from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models import Business, InventoryLot, InventoryMovement, Location, Product
from app.schemas import PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService

COLUMNS = ["Fecha", "Tipo", "Ubicación", "Cantidad", "Costo/Precio", "Saldo", "Usuario"]


def _seed_locations(db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Almacén central")
    pos = Location(business_id=business_id, code="POS1", name="Playa")
    db_session.add_all([central, pos])
    db_session.flush()
    return central, pos


def _seed_product(db_session, business_id, sku):
    product = Product(
        business_id=business_id, sku=sku, name=f"Artículo {sku}", min_stock=0,
        unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
        lead_time_days=0,
    )
    db_session.add(product)
    db_session.commit()
    return product


def test_purchase_transfer_sale_order_and_balance(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M1")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M1", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M1", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M1", quantity=4)],
        movement_date=datetime(2026, 8, 10, tzinfo=timezone.utc), note=None,
    ))
    svc.sale(SaleCreate(sku="SKU-M1", quantity=1, unit_price=2.0,
                        movement_date=datetime(2026, 9, 2, tzinfo=timezone.utc),
                        location_code="POS1", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M1",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    assert result["columns"] == COLUMNS
    assert "note" not in result
    rows = result["rows"]
    assert rows[0] == ["", "Saldo inicial", "Todas", "", "", 0.0, ""]
    assert rows[1][1] == "Compra"
    assert rows[1][2] == "Almacén central"
    assert rows[1][3] == 10.0
    assert rows[1][4] == 1.0
    assert rows[1][5] == 10.0
    assert rows[2][1] == "Traspaso"
    assert rows[2][2] == "Almacén central → Playa"
    assert rows[2][3] == 4.0
    assert rows[2][4] == ""
    assert rows[2][5] == 10.0
    assert rows[3][1] == "Venta"
    assert rows[3][2] == "Playa"
    assert rows[3][3] == -1.0
    assert rows[3][4] == 2.0
    assert rows[3][5] == 9.0


def test_filtered_location_balance(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M1")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M1", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M1", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M1", quantity=4)],
        movement_date=datetime(2026, 8, 10, tzinfo=timezone.utc), note=None,
    ))
    svc.sale(SaleCreate(sku="SKU-M1", quantity=1, unit_price=2.0,
                        movement_date=datetime(2026, 9, 2, tzinfo=timezone.utc),
                        location_code="POS1", note=None))
    pos_id = db_session.query(Location.id).filter(
        Location.code == "POS1", Location.business_id == business_id).scalar()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M1",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
        location_id=pos_id,
    )
    rows = result["rows"]
    assert rows[0][2] == "Playa"
    assert rows[0][5] == 0.0
    assert rows[1][1] == "Traspaso"
    assert rows[1][2] == "← Almacén central"
    assert rows[1][3] == 4.0
    assert rows[1][5] == 4.0
    assert rows[2][1] == "Venta"
    assert rows[2][3] == -1.0
    assert rows[2][5] == 3.0


def test_transfer_with_two_entries_is_one_row(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M2")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M2", quantity=2, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
                                lot_code="L1", note=None))
    svc.purchase(PurchaseCreate(sku="SKU-M2", quantity=3, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 2, tzinfo=timezone.utc),
                                lot_code="L2", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M2", quantity=5)],
        movement_date=datetime(2026, 8, 3, tzinfo=timezone.utc), note=None,
    ))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M2",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert [r[1] for r in rows] == ["Saldo inicial", "Compra", "Compra", "Traspaso"]
    assert rows[3][3] == 5.0
    assert rows[3][5] == 5.0
