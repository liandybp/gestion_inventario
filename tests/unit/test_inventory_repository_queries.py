from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import AuditLog, InventoryLot, InventoryMovement, Location, MovementAllocation, Product
from app.repositories.inventory_repository import InventoryRepository


def _create_product(db: Session, business_id: int, sku: str, name: str) -> Product:
    p = Product(
        business_id=business_id,
        sku=sku,
        name=name,
        min_stock=1,
        unit_of_measure="ud",
        default_purchase_cost=1.0,
        default_sale_price=3.0,
        lead_time_days=2,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


def test_stock_list_recent_and_history(db_session: Session, business_id: int) -> None:
    repo = InventoryRepository(db_session, business_id=business_id)
    p = _create_product(db_session, business_id, "SKU-H1", "Camiseta Roja")

    t0 = datetime.now(timezone.utc)
    mv_purchase = InventoryMovement(
        business_id=business_id,
        product_id=p.id,
        location_id=None,
        type="purchase",
        quantity=4,
        unit_cost=2.5,
        unit_price=None,
        movement_date=t0,
        note="purchase",
    )
    mv_sale = InventoryMovement(
        business_id=business_id,
        product_id=p.id,
        location_id=None,
        type="sale",
        quantity=-1,
        unit_cost=None,
        unit_price=5,
        movement_date=t0 + timedelta(minutes=1),
        note="sale",
    )
    db_session.add_all([mv_purchase, mv_sale])
    db_session.flush()

    lot = InventoryLot(
        business_id=business_id,
        movement_id=mv_purchase.id,
        product_id=p.id,
        location_id=None,
        lot_code="LOT-A",
        received_at=t0,
        unit_cost=2.5,
        qty_received=4,
        qty_remaining=3,
    )
    db_session.add(lot)
    db_session.flush()

    alloc = MovementAllocation(movement_id=mv_sale.id, lot_id=lot.id, quantity=1, unit_cost=2.5)
    db_session.add(alloc)
    db_session.add(
        AuditLog(
            username="tester",
            action="sale_create",
            entity_type="movement",
            entity_id=str(mv_sale.id),
            detail="{}",
        )
    )
    db_session.commit()

    stock_rows = repo.stock_list(query="camiseta")
    purchase_rows = repo.recent_purchases(query="camiseta", limit=10)
    sales_rows = repo.recent_sales(query="camiseta", limit=10)
    history_rows = repo.movement_history(query="camiseta", limit=10)

    assert len(stock_rows) == 1
    assert stock_rows[0][0] == "SKU-H1"
    assert len(purchase_rows) == 1
    assert len(sales_rows) == 1
    assert len(history_rows) >= 2


def test_recent_filters_by_date(db_session: Session, business_id: int) -> None:
    repo = InventoryRepository(db_session, business_id=business_id)
    p = _create_product(db_session, business_id, "SKU-DATE", "Pantalon")

    old_dt = datetime(2026, 1, 10, tzinfo=timezone.utc)
    new_dt = datetime(2026, 2, 10, tzinfo=timezone.utc)

    db_session.add_all(
        [
            InventoryMovement(
                business_id=business_id,
                product_id=p.id,
                location_id=None,
                type="purchase",
                quantity=1,
                unit_cost=2,
                unit_price=None,
                movement_date=old_dt,
                note=None,
            ),
            InventoryMovement(
                business_id=business_id,
                product_id=p.id,
                location_id=None,
                type="purchase",
                quantity=1,
                unit_cost=3,
                unit_price=None,
                movement_date=new_dt,
                note=None,
            ),
        ]
    )
    db_session.commit()

    jan = repo.recent_purchases(month="1", year=2026, limit=10)
    feb = repo.recent_purchases(month="2", year=2026, limit=10)

    assert len(jan) == 1
    assert len(feb) == 1


def test_recent_sales_filters_by_location_id(db_session: Session, business_id: int) -> None:
    repo = InventoryRepository(db_session, business_id=business_id)
    p = _create_product(db_session, business_id, "SKU-LOC", "Zapato")
    central = Location(business_id=business_id, code="CENTRAL", name="Almacen central")
    pos1 = Location(business_id=business_id, code="POS1", name="Punto 1")
    db_session.add_all([central, pos1])
    db_session.flush()

    t0 = datetime.now(timezone.utc)
    db_session.add_all(
        [
            InventoryMovement(
                business_id=business_id,
                product_id=p.id,
                location_id=central.id,
                type="sale",
                quantity=-1,
                unit_cost=None,
                unit_price=10,
                movement_date=t0,
                note="sale central",
            ),
            InventoryMovement(
                business_id=business_id,
                product_id=p.id,
                location_id=pos1.id,
                type="sale",
                quantity=-1,
                unit_cost=None,
                unit_price=11,
                movement_date=t0 + timedelta(minutes=1),
                note="sale pos",
            ),
        ]
    )
    db_session.commit()

    central_sales = repo.recent_sales(limit=10, location_id=central.id)
    pos_sales = repo.recent_sales(limit=10, location_id=pos1.id)

    assert len(central_sales) == 1
    assert len(pos_sales) == 1
    assert central_sales[0][8] == "Almacen central"
    assert pos_sales[0][8] == "Punto 1"


