from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.auth import hash_password
from app.models import Business, InventoryLot, InventoryMovement, Location, Product, User

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _wait_ready(base_url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{base_url}/health", timeout=1).read()
            return
        except Exception:
            time.sleep(0.3)
    raise RuntimeError("La app no quedó lista en el tiempo esperado")


def _seed_stock(db_path: str) -> None:
    """Siembra 2 productos con stock en CENTRAL para el negocio 'recambios'."""
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        if biz is None:
            raise RuntimeError("business 'recambios' no fue creado en el seed de startup")
        central = db.scalar(
            select(Location).where(Location.business_id == biz.id, Location.role == "central")
        )
        if central is None:
            raise RuntimeError("location central no fue creada en el seed de startup")

        now = datetime.now(timezone.utc)
        for sku, name, qty in (
            ("E2E-SKU1", "Producto E2E Uno", 10.0),
            ("E2E-SKU2", "Producto E2E Dos", 5.0),
        ):
            p = db.scalar(select(Product).where(Product.sku == sku))
            if p is None:
                p = Product(
                    business_id=biz.id, sku=sku, name=name, min_stock=0,
                    unit_of_measure="ud", default_purchase_cost=1.0,
                    default_sale_price=2.0, lead_time_days=0,
                )
                db.add(p)
                db.flush()

            mv = InventoryMovement(
                business_id=biz.id, product_id=p.id, location_id=central.id,
                type="purchase", quantity=qty, unit_cost=1.0, unit_price=None,
                movement_date=now, note=None,
            )
            db.add(mv)
            db.flush()
            db.add(InventoryLot(
                business_id=biz.id, movement_id=mv.id, product_id=p.id,
                location_id=central.id, lot_code=f"{sku}-E2E", received_at=now,
                unit_cost=1.0, qty_received=qty, qty_remaining=qty,
            ))
        # Operador de prueba (cajero): puede vender, no gestionar inventario (RBAC Fase C).
        # Upsert con password conocida: el seed de startup puede haberlo creado con otra
        # password según la env del entorno (CI vs local) — forzamos 'operator'.
        op = db.scalar(select(User).where(User.username == "operator"))
        if op is None:
            op = User(username="operator", password_hash=hash_password("operator"),
                      role="operator", is_active=True, business_id=biz.id)
            db.add(op)
        else:
            op.password_hash = hash_password("operator")
            op.role = "operator"
            op.is_active = True
            op.business_id = biz.id
        # Owner de prueba: acceso multi-negocio, pero Coach queda admin-only (H-0xx).
        ow = db.scalar(select(User).where(User.username == "owner"))
        if ow is None:
            ow = User(username="owner", password_hash=hash_password("owner"),
                      role="owner", is_active=True, business_id=biz.id)
            db.add(ow)
        else:
            ow.password_hash = hash_password("owner")
            ow.role = "owner"
            ow.is_active = True
            ow.business_id = biz.id
        db.commit()
    finally:
        db.close()
        engine.dispose()


def _boot_temp_server(prefix: str, *, extra_env: dict | None = None) -> tuple[str, str, subprocess.Popen, str]:
    """Arranca uvicorn real contra un SQLite temporal aislado en un puerto libre dinámico.

    Compartido por `live_server` y `central_server` — NUNCA apuntar un fixture de e2e a
    `app.settings.DATABASE_URL` ni a un puerto fijo: eso apuntaría a la BD real de dev/prod
    y podría colisionar con el servidor real corriendo en ese puerto (H-002/H-005 de la
    auditoría 2026-08-29, `docs/auditorias/2026-08-29-p5-multinode-e2e-offline-sync.md`).
    """
    tmp = tempfile.mkdtemp(prefix=prefix)
    db_path = os.path.join(tmp, "e2e.db")
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    env = dict(os.environ)
    env.update({
        "DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        "SESSION_SECRET": "e2e-test-secret",
        "SESSION_HTTPS_ONLY": "0",
        "ADMIN_USERNAME": "admin",
        "ADMIN_PASSWORD": "admin",
    })
    if extra_env:
        env.update(extra_env)

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        env=env,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_ready(base_url)
    return base_url, db_path, proc, tmp


@pytest.fixture(scope="function")
def central_server() -> tuple[str, str]:
    """Servidor central real (subproceso uvicorn) para e2e multi-nodo. Devuelve (base_url, db_path).

    SQLite temporal aislado por test (mismo patrón que `live_server`), con stock ya sembrado
    vía `_seed_stock` — los "nodos" del e2e multi-nodo son emisores de eventos HTTP contra
    este servidor (POST /sync/push con su propio device token), no procesos separados: el
    valor de REQ-20 es la convergencia del servidor, ya cubierta por device tokens distintos.
    """
    base_url, db_path, proc, tmp = _boot_temp_server("inventario-e2e-central-")
    try:
        _seed_stock(db_path)
        yield base_url, db_path
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="function")
def live_server() -> str:
    """Levanta la app contra un SQLite temporal y siembra stock. Devuelve la URL base."""
    base_url, db_path, proc, tmp = _boot_temp_server("inventario-e2e-")
    try:
        _seed_stock(db_path)
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


# ── Fase 2 del release 0.5.0 (JS a módulos, CSP sin 'unsafe-inline') ─────────────────────
# Siembra para recorrer TODAS las pestañas y modales: cada modal necesita una fila que lo abra.
TOUR_PRODUCT_NAME = "Tour <b>uno</b> & 'co'"  # hostil: el tooltip del pastel debe mostrarlo como texto
TOUR_WIDGET_TITLE = "Widget <b>tour</b>"


def _seed_tour(db_path: str) -> dict:
    """Movimientos (compra, traspaso, ventas, ajuste), cliente, gasto, extracción, documento,
    widget de gráfico y un usuario con cambio de contraseña obligatorio. Fechas = ahora, para
    que los filtros por mes por defecto los muestren. Devuelve los ids que usan los e2e."""
    from app.agent.widgets import add_widget
    from app.models import (
        Customer, MoneyExtraction, OperatingExpense, SalesDocument, SalesDocumentItem,
    )
    from app.schemas import (
        AdjustmentCreate, PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate,
    )
    from app.services.inventory_service import InventoryService

    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    now = datetime.now(timezone.utc)
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        bid = int(biz.id)
        central = db.scalar(select(Location).where(Location.business_id == bid, Location.role == "central"))
        pos = db.scalar(
            select(Location).where(Location.business_id == bid, Location.id != central.id).order_by(Location.id)
        )
        # TOUR-3 sin movimientos: es el único borrable (casillas name="skus" de stock_table).
        for sku, name in (("TOUR-1", TOUR_PRODUCT_NAME), ("TOUR-2", "Tour dos"), ("TOUR-3", "Tour tres")):
            db.add(Product(business_id=bid, sku=sku, name=name, min_stock=50, unit_of_measure="ud",
                           default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        purchase = svc.purchase(PurchaseCreate(sku="TOUR-1", quantity=10, unit_cost=1.0, movement_date=now,
                                               lot_code="LOT-TOUR-1", location_code=central.code, note="n"))
        transfer = svc.transfer(TransferCreate(from_location_code=central.code, to_location_code=pos.code,
                                               lines=[TransferLineCreate(sku="TOUR-1", quantity=3)],
                                               movement_date=now, note="n"))
        # Una venta por cada origen de lote: compra, traspaso y ajuste (botones de Utilidades).
        svc.sale(SaleCreate(sku="TOUR-1", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=central.code, note="n"))
        svc.sale(SaleCreate(sku="TOUR-1", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=pos.code, note="n"))
        svc.adjustment(AdjustmentCreate(sku="TOUR-2", quantity_delta=2, unit_cost=1.0, movement_date=now,
                                        location_code=central.code, note="n"))
        svc.sale(SaleCreate(sku="TOUR-2", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=central.code, note="n"))
        customer = Customer(business_id=bid, client_id="C-TOUR", name="Cliente Tour", address="Calle 1",
                            phone="600000000", email="c@tour.test")
        db.add(customer)
        db.add(OperatingExpense(business_id=bid, expense_date=now, amount=12.5, concept="Luz"))
        db.add(MoneyExtraction(business_id=bid, extraction_date=now, party="Negocio", amount=20.0,
                               concept="Retiro"))
        db.flush()
        doc = SalesDocument(
            business_id=bid, location_id=central.id, customer_id=customer.id, doc_type="F",
            year_month=now.strftime("%Y%m"), seq=1, code=f"F{now:%Y%m}-0001", issue_date=now,
            issuer_name="Emisor", client_name=customer.name, client_id=customer.client_id,
            client_address=customer.address, notes="n", subtotal=4.0, total=4.0,
        )
        db.add(doc)
        db.flush()
        db.add(SalesDocumentItem(document_id=doc.id, line_no=1, sku="TOUR-1", description="Tour uno",
                                 unit_of_measure="ud", quantity=2, unit_price=2.0, line_total=4.0))
        db.add(User(username="mustchange", password_hash=hash_password("mustchange"), role="admin",
                    is_active=True, business_id=bid, must_change_password=True))
        db.commit()
        add_widget(db, bid, type="pie", title=TOUR_WIDGET_TITLE, tool="e2e", tool_args={},
                   payload={"labels": ["A", "B"], "values": [1, 2]}, unit=None, created_by=None)
        return {"purchase": purchase.movement.id, "transfer_ref": transfer.transfer_ref or ""}
    finally:
        db.close()
        engine.dispose()


@pytest.fixture(scope="function")
def tour_server() -> tuple[str, dict]:
    """Como `live_server`, más la siembra de `_seed_tour`. Devuelve (base_url, ids)."""
    base_url, db_path, proc, tmp = _boot_temp_server("inventario-e2e-tour-")
    try:
        _seed_stock(db_path)
        ids = _seed_tour(db_path)
        yield base_url, ids
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="function", autouse=True)
def js_errors(page, request) -> list[str]:
    """Errores de JS de la página: excepciones no capturadas y mensajes `console.error`
    (Chromium informa ahí las violaciones de CSP y los módulos que no cargan).
    Guarda automática en todos los e2e; los tests pueden asertar en el fixture o confiar
    en el teardown que falla si hay errores reales (no HTTP errores ni recursos 404)."""
    errors: list[str] = []
    page.on("pageerror", lambda exc: errors.append(f"pageerror: {exc}"))
    # Filtrar: permitir "Failed to load resource" (errores 4xx de HTTP) y violaciones CSP esperadas
    # Rechazar: violaciones CSP reales, módulos no encontrados, excepciones no capturadas
    page.on("console", lambda msg: errors.append(f"console.error: {msg.text}")
            if msg.type == "error" and "Failed to load resource" not in msg.text else None)
    yield errors
    # Fallar el test solo si hay errores reales (CSP, módulos no cargables, excepciones)
    # "Content Security Policy": cualquier violación de CSP (scripts y, desde la fase 3, estilos inline:
    # Chromium dice "Applying inline style violates the following Content Security Policy directive…").
    real_errors = [e for e in errors if "Refused to load the script" in e or "Refused to execute" in e
                   or "Content Security Policy" in e or "pageerror" in e]
    if real_errors:
        pytest.fail(f"JS errors reales durante {request.node.name}: {real_errors}")
