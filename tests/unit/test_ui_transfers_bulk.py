from __future__ import annotations

from sqlalchemy import select

from app.models import InventoryMovement, Location, Product
from app.routers.ui_transfers import _parse_transfer_lines
from app.schemas import PurchaseCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService


def test_parse_transfer_lines_basic():
    lines = _parse_transfer_lines(["SKU1", "SKU2"], ["2", "3.5"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU1", 2.0), ("SKU2", 3.5)]


def test_parse_transfer_lines_strips_sku_label():
    lines = _parse_transfer_lines(["SKU1 - Nombre producto"], ["4"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU1", 4.0)]


def test_parse_transfer_lines_skips_empty_and_nonpositive():
    lines = _parse_transfer_lines(["", "SKU2", "SKU3"], ["1", "0", "-2"])
    assert lines == []


def test_parse_transfer_lines_skips_invalid_quantity():
    lines = _parse_transfer_lines(["SKU1", "SKU2"], ["abc", "5"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU2", 5.0)]


def test_transfer_multiple_lines_in_one_operation(db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Central")
    pos = Location(business_id=business_id, code="POS1", name="Pos")
    db_session.add_all([central, pos])
    db_session.commit()

    for sku in ("SKU-A", "SKU-B"):
        db_session.add(Product(
            business_id=business_id, sku=sku, name=sku, min_stock=0,
            unit_of_measure="ud", default_purchase_cost=1.0,
            default_sale_price=2.0, lead_time_days=0,
        ))
    db_session.commit()

    service = InventoryService(db_session, business_id=business_id)
    service.purchase(PurchaseCreate(sku="SKU-A", quantity=10, unit_cost=1.0))
    service.purchase(PurchaseCreate(sku="SKU-B", quantity=5, unit_cost=1.0))

    result = service.transfer(TransferCreate(
        from_location_code="CENTRAL",
        to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-A", quantity=3), TransferLineCreate(sku="SKU-B", quantity=2)],
    ))

    assert len(result.lines) == 2
    outs = list(db_session.scalars(select(InventoryMovement).where(InventoryMovement.type == "transfer_out")))
    assert len(outs) == 2
    assert service.stock_for_location("SKU-A", "POS1") == 3
    assert service.stock_for_location("SKU-B", "POS1") == 2
