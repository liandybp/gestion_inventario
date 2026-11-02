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


def test_transfer_in_without_out_id_is_own_row(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M3")
    pos_id = db_session.query(Location.id).filter(
        Location.code == "POS1", Location.business_id == business_id).scalar()
    db_session.add(InventoryMovement(
        business_id=business_id, product_id=product.id, location_id=pos_id,
        type="transfer_in", quantity=3.0, unit_cost=1.0, unit_price=None,
        movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
        note="Transfer in from CENTRAL lot=L1",
    ))
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M3",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert rows[0][1] == "Saldo inicial"
    assert rows[1][1] == "Traspaso"
    assert rows[1][2] == "← Almacén central"
    assert rows[1][3] == 3.0


def test_movement_exactly_at_start_is_a_row_not_initial(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M4")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M4", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
                                lot_code="LOT-M4", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M4",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert rows[0][5] == 0.0
    assert rows[1][1] == "Compra"


def test_other_business_movements_invisible(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-ISO")
    other = Business(code="otro", name="Otro Negocio")
    db_session.add(other)
    db_session.commit()
    other_prod = Product(business_id=other.id, sku="SKU-OTRO", name="Compartido", min_stock=0,
                         unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                         lead_time_days=0)
    db_session.add(other_prod)
    db_session.flush()
    other_loc = Location(business_id=other.id, code="OTRO_CENTRAL", name="Central")
    db_session.add(other_loc)
    db_session.flush()
    db_session.add(InventoryMovement(
        business_id=other.id, product_id=other_prod.id, location_id=other_loc.id,
        type="purchase", quantity=50.0, unit_cost=1.0, unit_price=None,
        movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc), note=None,
    ))
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-ISO",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    assert result["rows"] == [["", "Saldo inicial", "Todas", "", "", 0.0, ""]]


def test_limit_keeps_first_rows_and_adds_note(db_session, business_id, monkeypatch):
    monkeypatch.setattr("app.services.reporting_service.PRODUCT_MOVEMENTS_MAX_ROWS", 2)
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M5")
    svc = InventoryService(db_session, business_id)
    for i, day in enumerate((1, 2, 3), start=1):
        svc.purchase(PurchaseCreate(sku="SKU-M5", quantity=1, unit_cost=1.0,
                                    movement_date=datetime(2026, 8, day, tzinfo=timezone.utc),
                                    lot_code=f"LOT-M5-{i}", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M5",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert len(rows) == 3  # saldo inicial + 2 compras (de 3)
    assert rows[0][1] == "Saldo inicial"
    assert result["note"] == "Se muestran los 2 primeros movimientos de 3."


def test_note_when_balance_differs_from_lots(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M6")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M6", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M6", note=None))
    lot = db_session.query(InventoryLot).filter(
        InventoryLot.product_id == product.id).one()
    lot.qty_remaining = 9.0
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M6",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime.now(timezone.utc) + timedelta(days=1),
    )
    assert result["note"] == "El saldo calculado (10.0) no coincide con el stock actual por lotes (9.0)."
