from __future__ import annotations

from datetime import datetime, timezone

from app.models import InventoryMovement, OperatingExpense, Product
from app.services.reporting_service import ReportingService


def test_expenses_by_month(db_session, business_id):
    now = datetime.now(timezone.utc)
    db_session.add(OperatingExpense(
        business_id=business_id, amount=30.0, concept="Luz", expense_date=now,
    ))
    db_session.commit()

    service = ReportingService(db_session, business_id)
    series = service.expenses_by_month(months=1)

    assert len(series) == 1
    assert series[0]["month"] == now.strftime("%Y-%m")
    assert series[0]["total"] == 30.0


def test_purchases_by_month(db_session, business_id):
    now = datetime.now(timezone.utc)
    p = Product(business_id=business_id, sku="RP-SKU", name="P", min_stock=0,
                unit_of_measure="ud", default_purchase_cost=1.0,
                default_sale_price=2.0, lead_time_days=0)
    db_session.add(p)
    db_session.commit()
    db_session.add(InventoryMovement(
        business_id=business_id, product_id=p.id, type="purchase",
        quantity=5, unit_cost=2.0, unit_price=None, movement_date=now,
    ))
    db_session.commit()

    service = ReportingService(db_session, business_id)
    series = service.purchases_by_month(months=1)

    assert len(series) == 1
    assert series[0]["month"] == now.strftime("%Y-%m")
    assert series[0]["total"] == 10.0
