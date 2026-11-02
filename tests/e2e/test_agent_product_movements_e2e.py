from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.agent.tools import TOOL_REGISTRY
from app.agent.widgets import add_widget
from app.models import Business, Location, Product
from app.schemas import PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService

pytestmark = pytest.mark.e2e

SKU = "E2E-MOV"


def _seed_and_run_tool(db_path: str) -> int:
    """Siembra compra + traspaso + venta del SKU, ejecuta la tool real y guarda el
    resultado como widget tipo table. Devuelve el id del widget."""
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        bid = int(biz.id)
        central = db.scalar(
            select(Location).where(Location.business_id == bid, Location.role == "central")
        )
        pos = db.scalar(
            select(Location).where(Location.business_id == bid, Location.id != central.id)
            .order_by(Location.id)
        )
        db.add(Product(business_id=bid, sku=SKU, name="Movimientos E2E", min_stock=0,
                       unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                       lead_time_days=0))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        now = datetime.now(timezone.utc)
        svc.purchase(PurchaseCreate(sku=SKU, quantity=10, unit_cost=1.0, movement_date=now,
                                    lot_code="LOT-E2E-MOV", note=None))
        svc.transfer(TransferCreate(from_location_code=central.code, to_location_code=pos.code,
                                    lines=[TransferLineCreate(sku=SKU, quantity=4)],
                                    movement_date=now, note=None))
        svc.sale(SaleCreate(sku=SKU, quantity=1, unit_price=2.0, movement_date=now,
                            location_code=pos.code, note=None))
        payload = TOOL_REGISTRY["product_movements"]["fn"](svc, {"sku": SKU})
        widget = add_widget(db, bid, type="table", title="Movimientos E2E", tool="product_movements",
                            tool_args={"sku": SKU}, payload=payload, unit=None, created_by=None)
        return int(widget.id)
    finally:
        db.close()
        engine.dispose()


def test_product_movements_table_initial_transfer_and_final_balance(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_and_run_tool(db_path)

    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined'")
    page.locator("#tab-agent").click()

    table = page.locator(f"#widget-{wid} table.widget-table")
    table.wait_for(state="attached", timeout=10000)

    rows = table.locator("tbody tr")
    first_cells = rows.nth(0).locator("td").all_inner_texts()
    assert first_cells[1] == "Saldo inicial"

    all_cells = [c for i in range(rows.count()) for c in rows.nth(i).locator("td").all_inner_texts()]
    assert any("→" in c for c in all_cells)

    last_cells = rows.nth(rows.count() - 1).locator("td").all_inner_texts()
    assert last_cells[5] == "9.0"
