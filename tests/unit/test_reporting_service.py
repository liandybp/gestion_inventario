from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models import Business, InventoryMovement, Location, OperatingExpense, Product
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


def _product(db, business_id, sku):
    p = Product(business_id=business_id, sku=sku, name=sku, min_stock=0, unit_of_measure="ud",
                default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db.add(p)
    db.commit()
    return p


def _sale(db, business_id, product_id, location_id, qty, price, when):
    db.add(InventoryMovement(business_id=business_id, product_id=product_id, location_id=location_id,
                             type="sale", quantity=-qty, unit_cost=1.0, unit_price=price, movement_date=when))


def test_sales_by_location_monthly(db_session, business_id):
    now = datetime.now(timezone.utc)
    p = _product(db_session, business_id, "RSLM-SKU")
    a = Location(business_id=business_id, code="RSLM-A", name="Tienda A", position=2)
    b = Location(business_id=business_id, code="RSLM-B", name="Tienda B", position=1)
    c = Location(business_id=business_id, code="RSLM-C", name="Tienda C", position=3)
    db_session.add_all([a, b, c])
    db_session.commit()
    _sale(db_session, business_id, p.id, a.id, 2, 10.0, now)
    _sale(db_session, business_id, p.id, b.id, 1, 5.0, now)
    _sale(db_session, business_id, p.id, a.id, 3, 10.0, now - timedelta(days=250))
    db_session.add(InventoryMovement(business_id=business_id, product_id=p.id, location_id=a.id,
                                     type="purchase", quantity=4, unit_cost=2.0, unit_price=None, movement_date=now))
    db_session.commit()

    result = ReportingService(db_session, business_id).sales_by_location_monthly(months=6)

    assert len(result["months"]) == 6
    assert result["months"][-1] == now.strftime("%Y-%m")
    assert [s["location"] for s in result["series"]] == ["Tienda B", "Tienda A"]
    assert result["series"][0]["values"] == [0.0] * 5 + [5.0]
    assert result["series"][1]["values"] == [0.0] * 5 + [20.0]


def test_sales_by_location_monthly_scoped_to_business(db_session, business_id):
    now = datetime.now(timezone.utc)
    other = Business(code="rslm-other", name="Other")
    db_session.add(other)
    db_session.commit()
    p = _product(db_session, other.id, "RSLM-OTHER")
    loc = Location(business_id=other.id, code="RSLM-O", name="Otra")
    db_session.add(loc)
    db_session.commit()
    _sale(db_session, other.id, p.id, loc.id, 1, 99.0, now)
    db_session.commit()

    result = ReportingService(db_session, business_id).sales_by_location_monthly(months=1)

    assert result == {"months": [now.strftime("%Y-%m")], "series": []}


def test_sales_by_location_monthly_same_name_locations_are_separate(db_session, business_id):
    now = datetime.now(timezone.utc)
    p = _product(db_session, business_id, "RSLM-DUP")
    x = Location(business_id=business_id, code="RSLM-X1", name="Tienda", position=1)
    y = Location(business_id=business_id, code="RSLM-X2", name="Tienda", position=2)
    db_session.add_all([x, y])
    db_session.commit()
    _sale(db_session, business_id, p.id, x.id, 1, 3.0, now)
    _sale(db_session, business_id, p.id, y.id, 1, 4.0, now)
    db_session.commit()

    series = ReportingService(db_session, business_id).sales_by_location_monthly(months=1)["series"]

    assert series == [{"location": "Tienda", "values": [3.0]}, {"location": "Tienda", "values": [4.0]}]


def test_sales_by_location_monthly_clamps_months(db_session, business_id):
    service = ReportingService(db_session, business_id)
    assert len(service.sales_by_location_monthly(months=0)["months"]) == 1
    assert len(service.sales_by_location_monthly(months=500)["months"]) == 24
