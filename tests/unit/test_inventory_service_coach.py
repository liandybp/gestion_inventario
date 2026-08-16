from datetime import datetime, timedelta, timezone

import pytest

from app.models import Business, InventoryLot, InventoryMovement
from app.services.inventory_service import InventoryService


@pytest.fixture
def seeded_business(db_session) -> Business:
    biz = db_session.query(Business).filter(Business.code == "test").first()
    assert biz is not None
    return biz


def _add_purchase(db_session, business_id, product, qty, unit_cost, received_at, lot_code):
    mv = InventoryMovement(
        business_id=business_id, product_id=product.id, type="purchase",
        quantity=qty, unit_cost=unit_cost, unit_price=None, movement_date=received_at, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(InventoryLot(
        business_id=business_id, movement_id=mv.id, product_id=product.id,
        lot_code=lot_code, received_at=received_at, unit_cost=unit_cost,
        qty_received=qty, qty_remaining=qty,
    ))
    db_session.flush()


def test_turnover_days_none_when_no_cogs(db_session, seeded_business):
    svc = InventoryService(db_session, seeded_business.id)
    assert svc.inventory_turnover_days(0) is None


def test_turnover_days_value(db_session, seeded_business, sample_product):
    now = datetime.now(timezone.utc)
    _add_purchase(db_session, seeded_business.id, sample_product, 10, 2.0, now, "LOT-TURN-1")
    db_session.commit()
    svc = InventoryService(db_session, seeded_business.id)
    # inv_value = 10 * 2.0 = 20; cogs_daily = 60/30 = 2 → 20/2 = 10 días
    assert svc.inventory_turnover_days(60.0) == 10.0


def test_dead_stock_includes_old_unsold(db_session, seeded_business, sample_product):
    now = datetime.now(timezone.utc)
    _add_purchase(db_session, seeded_business.id, sample_product, 5, 10.0,
                  now - timedelta(days=120), "LOT-OLD-1")
    db_session.commit()
    svc = InventoryService(db_session, seeded_business.id)
    items = svc.dead_stock(days=90, now=now)
    assert len(items) == 1
    assert items[0]["sku"] == sample_product.sku
    assert items[0]["value"] == 50.0


def test_dead_stock_excludes_recently_sold(db_session, seeded_business, sample_product):
    """REQ-3: stock parado >90 días pero con venta reciente NO es stock muerto."""
    now = datetime.now(timezone.utc)
    _add_purchase(db_session, seeded_business.id, sample_product, 5, 10.0,
                  now - timedelta(days=120), "LOT-OLD-2")
    db_session.flush()
    sale = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-1, unit_cost=10.0, unit_price=20.0, movement_date=now - timedelta(days=10), note=None,
    )
    db_session.add(sale)
    db_session.commit()
    svc = InventoryService(db_session, seeded_business.id)
    items = svc.dead_stock(days=90, now=now)
    assert all(i["sku"] != sample_product.sku for i in items)