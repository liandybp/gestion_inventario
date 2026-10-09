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
