"""E2e del flujo de sync offline-desktop (P3, docs/auditorias/2026-08-29-p3-sync-client-offline-sync{,-reaudit}.md).

Cubre H-009 (cobertura e2e faltante): levanta DOS servidores reales
(`central_server` en modo server, `desktop_server` en modo desktop, cada uno
con su propia BD SQLite temporal) y valida el flujo completo de usuario:

    login (central) -> registrar dispositivo -> login (desktop) ->
    aprovisionar -> sincronizar (pull inicial) -> venta offline ->
    sincronizar (push) -> la venta aparece en el servidor central

De paso, verifica en vivo los 3 bugs críticos que motivaron esta suite
(H-001 orden FK, H-002 password_hash preservado, H-003 movement_date null):
- H-001/H-002 se ejercitan porque el pull inicial trae businesses/locations/
  products/users y el login post-sync en el nodo desktop demuestra que el
  password_hash local no se perdió.
- H-003 se ejercita porque la venta offline se hace sin fecha explícita (el
  campo se deja vacío) y el replay en el servidor central debe aceptarla.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

pytestmark = pytest.mark.e2e

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


def _boot_server(*, app_mode: str) -> tuple[str, str, subprocess.Popen]:
    """Levanta un proceso uvicorn real con su propia BD SQLite temporal.

    Devuelve (base_url, db_path, proceso). El caller es responsable de
    terminar el proceso y limpiar el directorio temporal.
    """
    tmp = tempfile.mkdtemp(prefix=f"inventario-e2e-sync-{app_mode}-")
    db_path = os.path.join(tmp, "e2e.db")
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    env = dict(os.environ)
    env.update({
        "DATABASE_URL": f"sqlite+pysqlite:///{db_path}",
        "SESSION_SECRET": "e2e-sync-test-secret",
        "SESSION_HTTPS_ONLY": "0",
        "ADMIN_USERNAME": "admin",
        "ADMIN_PASSWORD": "admin",
        "APP_MODE": app_mode,
    })

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        env=env,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_ready(base_url)
    return base_url, db_path, proc


def _seed_central_stock(db_path: str) -> int:
    """Siembra stock en CENTRAL del negocio 'recambios' del servidor central.

    Devuelve el business_id de 'recambios' para uso posterior del test.
    """
    from datetime import datetime, timezone
    from app.models import Business, InventoryLot, InventoryMovement, Location, Product

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
        p = db.scalar(select(Product).where(Product.sku == "SYNC-E2E-SKU"))
        if p is None:
            p = Product(
                business_id=biz.id, sku="SYNC-E2E-SKU", name="Producto Sync E2E", min_stock=0,
                unit_of_measure="ud", default_purchase_cost=1.0,
                default_sale_price=2.0, lead_time_days=0,
            )
            db.add(p)
            db.flush()

        mv = InventoryMovement(
            business_id=biz.id, product_id=p.id, location_id=central.id,
            type="purchase", quantity=10.0, unit_cost=1.0, unit_price=None,
            movement_date=now, note=None,
        )
        db.add(mv)
        db.flush()
        db.add(InventoryLot(
            business_id=biz.id, movement_id=mv.id, product_id=p.id,
            location_id=central.id, lot_code="SYNC-E2E-SKU-LOT", received_at=now,
            unit_cost=1.0, qty_received=10.0, qty_remaining=10.0,
        ))
        db.commit()
        return int(biz.id)
    finally:
        db.close()
        engine.dispose()


def _central_sale_movements(db_path: str, business_id: int) -> list:
    from app.models import InventoryMovement

    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        return list(db.scalars(
            select(InventoryMovement).where(
                InventoryMovement.business_id == business_id,
                InventoryMovement.type == "sale",
            )
        ))
    finally:
        db.close()
        engine.dispose()


@pytest.fixture(scope="function")
def central_server():
    base_url, db_path, proc = _boot_server(app_mode="server")
    business_id = _seed_central_stock(db_path)
    try:
        yield {"url": base_url, "db_path": db_path, "business_id": business_id}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)


@pytest.fixture(scope="function")
def desktop_server():
    base_url, db_path, proc = _boot_server(app_mode="desktop")
    try:
        yield {"url": base_url, "db_path": db_path}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(os.path.dirname(db_path), ignore_errors=True)


def _login(page: Page, base_url: str, *, username: str = "admin", password: str = "admin") -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', username)
    page.fill('input[name="password"]', password)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def test_offline_sale_syncs_to_central_server(
    central_server: dict, desktop_server: dict, page: Page,
) -> None:
    central_url = central_server["url"]
    desktop_url = desktop_server["url"]
    business_id = central_server["business_id"]

    # 1. Admin se loguea en el servidor central y registra el nodo desktop.
    _login(page, central_url)
    register_resp = page.request.post(
        f"{central_url}/sync/register",
        data={"business_id": business_id, "location_code": "CENTRAL", "label": "E2E desktop node"},
    )
    assert register_resp.ok, register_resp.text()
    device_token = register_resp.json()["token"]

    # 2. Admin se loguea en el nodo desktop (BD local, totalmente separada).
    _login(page, desktop_url)
    expect(page.locator("#sync-banner")).to_contain_text("Nodo no aprovisionado")
    # H-005: en modo desktop, Compras/Traspasos/Copiloto quedan ocultos.
    expect(page.locator("#tab-purchases")).to_have_count(0)
    expect(page.locator("#tab-transfers")).to_have_count(0)

    # 3. Aprovisiona el nodo pegando el token obtenido en el paso 1 (H-008).
    page.fill('.sync-provision-form input[name="server_url"]', central_url)
    page.fill('.sync-provision-form input[name="device_token"]', device_token)
    page.click('.sync-provision-form button[type="submit"]')
    expect(page.locator("#sync-banner")).to_contain_text("Nunca sincronizado")

    # 4. Primer sync: pull inicial trae business/location/product/stock/users
    #    desde el central (H-001: orden FK correcto en un pull real contra
    #    una BD SQLite con PRAGMA foreign_keys=ON).
    page.click('#sync-banner button:has-text("Sincronizar")')
    expect(page.locator("#sync-banner")).to_contain_text("Última sincronización")

    # 5. Vende offline el producto recién traído por el pull (sin fecha
    #    explícita — ejercita H-003 en el replay del paso 7).
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    page.select_option(
        '#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL"
    )
    page.fill('input[name="product"]', "SYNC-E2E-SKU")
    page.fill('input[name="quantity"]', "1")
    page.fill('input[name="unit_price"]', "2.00")
    page.click('#sale-panel form[hx-post="/ui/sale"] button[type="submit"]')
    expect(page.locator("#sale-panel").locator("text=SYNC-E2E-SKU").first).to_be_visible()

    # 6. Segundo sync: push del evento de venta pendiente. El banner vive fuera
    #    de #tab-content (dashboard.html lo incluye antes, en <main>), así que
    #    sigue visible sin volver a la pestaña Home.
    page.click('#sync-banner button:has-text("Sincronizar")')
    expect(page.locator("#sync-banner")).to_contain_text("Aplicados: 1")

    # 7. La venta offline se replicó de verdad en el servidor central
    #    (InventoryService.sale() corrió ahí, no solo localmente).
    sales = _central_sale_movements(central_server["db_path"], business_id)
    assert len(sales) == 1
    assert float(sales[0].quantity) == -1.0

    # 8. H-002: el password_hash local del admin del nodo desktop sobrevivió
    #    al pull (que trajo el `users` del central) — puede volver a loguear.
    page.click("a.sidebar-logout")
    page.wait_for_url(f"{desktop_url}/ui/login**")
    _login(page, desktop_url)
    expect(page).to_have_url(f"{desktop_url}/ui/dashboard")
