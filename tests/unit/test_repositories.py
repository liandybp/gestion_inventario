from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, Product
from app.repositories.inventory_repository import InventoryRepository, _query_match
from app.repositories.product_repository import ProductRepository


def _create_product(db: Session, business_id: int, sku: str, name: str) -> Product:
    p = Product(
        business_id=business_id,
        sku=sku,
        name=name,
        min_stock=0,
        unit_of_measure="ud",
        default_purchase_cost=1.0,
        default_sale_price=2.0,
        lead_time_days=0,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


def test_product_repository_search_is_case_and_accent_insensitive(db_session: Session, business_id: int) -> None:
    _create_product(db_session, business_id, "SKU1", "Camiseta Azul")
    _create_product(db_session, business_id, "SKU2", "Pantalon")

    repo = ProductRepository(db_session, business_id=business_id)

    rows = repo.search("camiseta", limit=10)
    assert len(rows) == 1
    assert rows[0].sku == "SKU1"


def test_inventory_repository_query_match_helper() -> None:
    assert _query_match("camiseta", "SKU1", "Camiseta Azul")
    assert not _query_match("zapato", "SKU1", "Camiseta Azul")


def test_inventory_repository_stock_and_fifo_order(db_session: Session, business_id: int) -> None:
    product = _create_product(db_session, business_id, "SKU3", "Calcetin")
    repo = InventoryRepository(db_session, business_id=business_id)

    dt = datetime.now(timezone.utc)
    mv1 = InventoryMovement(
        business_id=business_id,
        product_id=product.id,
        location_id=None,
        type="purchase",
        quantity=2,
        unit_cost=5,
        unit_price=None,
        movement_date=dt,
        note=None,
    )
    mv2 = InventoryMovement(
        business_id=business_id,
        product_id=product.id,
        location_id=None,
        type="purchase",
        quantity=3,
        unit_cost=6,
        unit_price=None,
        movement_date=dt + timedelta(minutes=1),
        note=None,
    )
    db_session.add_all([mv1, mv2])
    db_session.flush()

    lot1 = InventoryLot(
        business_id=business_id,
        movement_id=mv1.id,
        product_id=product.id,
        location_id=None,
        lot_code="L1",
        received_at=dt,
        unit_cost=5,
        qty_received=2,
        qty_remaining=2,
    )
    lot2 = InventoryLot(
        business_id=business_id,
        movement_id=mv2.id,
        product_id=product.id,
        location_id=None,
        lot_code="L2",
        received_at=dt + timedelta(minutes=1),
        unit_cost=6,
        qty_received=3,
        qty_remaining=3,
    )
    db_session.add_all([lot1, lot2])
    db_session.commit()

    assert repo.stock_for_product_id(product.id) == 5.0

    fifo = repo.fifo_lots_for_product_id(product.id)
    assert [lot.lot_code for lot in fifo] == ["L1", "L2"]
