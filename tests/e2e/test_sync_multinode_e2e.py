"""e2e multi-nodo del épico offline-desktop-sync (REQ-20, P5).

Reescrito por completo el 2026-08-29 tras la auditoría
`docs/auditorias/2026-08-29-p5-multinode-e2e-offline-sync.md` (score 1.40/5.00 🔴): la
versión original nunca corrió una sola vez (NameError inmediato), usaba una fixture que
apuntaba a la BD real de dev en el puerto del servidor de dev real, y sus aserciones
consultaban una BD fantasma desconectada del servidor bajo prueba.

Modela cada "nodo" como un emisor de eventos HTTP contra un único servidor central real
(subproceso uvicorn + SQLite temporal aislado, fixture `central_server`) — cada nodo es
simplemente un `DeviceToken` distinto haciendo `POST /sync/push`. El valor de REQ-20 es la
convergencia del servidor (idempotencia, conflicto de stock, deduplicación de cliente), que
es exactamente lo que este test ejercita contra el protocolo real, sin necesidad de levantar
procesos-nodo completos (la lógica de outbox/apply_pull del cliente ya está cubierta por los
unit de P3 y por tests/e2e/test_sync_e2e.py).
"""
from __future__ import annotations

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.migrations import _LATEST_SCHEMA_VERSION
from app.models import (
    AppliedSyncEvent, Business, Customer, DeviceToken, InventoryMovement,
    Location, Product, SyncConflict,
)

pytestmark = pytest.mark.e2e


def _session_for(db_path: str):
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    session = sessionmaker(bind=engine)()
    return session, engine


def _register_device(db, business_id: int, token: str, label: str) -> None:
    db.add(DeviceToken(token=token, business_id=business_id, label=label))
    db.commit()


def _push(base_url: str, token: str, events: list[dict]) -> dict:
    resp = httpx.post(
        f"{base_url}/sync/push",
        json={"app_schema_version": _LATEST_SCHEMA_VERSION, "events": events},
        headers={"X-Sync-Token": token},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


def _sale_event(event_id: str, seq: int, *, sku: str, quantity: float, location_code: str) -> dict:
    return {
        "event_id": event_id, "event_type": "sale", "seq": seq,
        "prev_hash": None, "event_hash": "", "location_id": None,
        "payload": {
            "sku": sku, "quantity": quantity, "unit_price": 10.0,
            "movement_date": None, "note": None, "location_code": location_code,
        },
    }


def _customer_event(event_id: str, seq: int, *, client_id: str, name: str, phone: str) -> dict:
    return {
        "event_id": event_id, "event_type": "customer", "seq": seq,
        "prev_hash": None, "event_hash": "", "location_id": None,
        "payload": {"client_id": client_id, "name": name, "address": "", "phone": phone, "email": ""},
    }


def test_sync_multinode_convergence(central_server: tuple[str, str]) -> None:
    """REQ-20: dos nodos convergen contra un servidor central real.

    - Nodo A vende E2E-SKU1 x2 (hay stock de sobra, sembrado por `_seed_stock`) y da de
      alta un cliente nuevo.
    - Nodo B intenta vender el mismo SKU x999 (sin stock -> conflicto `sale_stock`) y da
      de alta un cliente con el mismo teléfono que el de A (dedup: no crea un segundo
      Customer, pero el evento igual se marca aplicado).
    - Reenviar el evento de venta de A (mismo event_id) no lo vuelve a aplicar
      (idempotencia vía AppliedSyncEvent).
    """
    base_url, db_path = central_server
    db, engine = _session_for(db_path)
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        assert biz is not None, "business 'recambios' no encontrado (seed de startup)"
        central = db.scalar(
            select(Location).where(Location.business_id == biz.id, Location.role == "central")
        )
        assert central is not None, "location central no encontrada (seed de startup)"

        token_a, token_b = "e2e-node-a-token", "e2e-node-b-token"
        _register_device(db, biz.id, token_a, "Nodo A")
        _register_device(db, biz.id, token_b, "Nodo B")

        sale_a = _sale_event("evt-sale-a-1", 1, sku="E2E-SKU1", quantity=2, location_code=central.code)
        customer_a = _customer_event("evt-cust-a-1", 2, client_id="C-A", name="Cliente A", phone="600-111-222")
        resp_a = _push(base_url, token_a, [sale_a, customer_a])
        assert set(resp_a["applied_event_ids"]) == {"evt-sale-a-1", "evt-cust-a-1"}

        sale_b = _sale_event("evt-sale-b-1", 1, sku="E2E-SKU1", quantity=999, location_code=central.code)
        customer_b = _customer_event("evt-cust-b-1", 2, client_id="C-B", name="Cliente B", phone="600-111-222")
        resp_b = _push(base_url, token_b, [sale_b, customer_b])
        assert "evt-sale-b-1" not in resp_b["applied_event_ids"]  # sin stock -> conflicto, no se aplica
        assert "evt-cust-b-1" in resp_b["applied_event_ids"]  # dedup: se marca aplicado sin duplicar

        # Idempotencia: reenviar el evento ya aplicado de A no genera un segundo movimiento.
        resp_a_retry = _push(base_url, token_a, [sale_a])
        assert resp_a_retry["applied_event_ids"] == []

        db.expire_all()

        applied_ids = {
            a.event_id for a in db.scalars(
                select(AppliedSyncEvent).where(AppliedSyncEvent.business_id == biz.id)
            )
        }
        assert applied_ids == {"evt-sale-a-1", "evt-cust-a-1", "evt-cust-b-1"}

        product_id = db.scalar(select(Product.id).where(Product.sku == "E2E-SKU1"))
        sale_movements = list(db.scalars(
            select(InventoryMovement).where(
                InventoryMovement.business_id == biz.id,
                InventoryMovement.type == "sale",
                InventoryMovement.product_id == product_id,
            )
        ))
        assert len(sale_movements) == 1  # ni la de B (sin stock) ni el reenvío de A duplicaron
        assert float(sale_movements[0].quantity) == -2.0

        customers = list(db.scalars(
            select(Customer).where(Customer.business_id == biz.id, Customer.phone == "600-111-222")
        ))
        assert len(customers) == 1  # dedup por teléfono conservó el de A, no creó el de B
        assert customers[0].client_id == "C-A"

        conflicts = list(db.scalars(
            select(SyncConflict).where(SyncConflict.business_id == biz.id, SyncConflict.resolved == False)  # noqa: E712
        ))
        assert any(c.event_id == "evt-sale-b-1" and c.kind == "sale_stock" for c in conflicts)
    finally:
        db.close()
        engine.dispose()
