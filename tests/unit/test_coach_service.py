from datetime import datetime, timedelta, timezone

import pytest

from app.models import Business, CashBaseline, InventoryLot, InventoryMovement, MovementAllocation
from app.services.coach_service import CoachService


@pytest.fixture
def seeded_business(db_session) -> Business:
    biz = db_session.query(Business).filter(Business.code == "test").first()
    assert biz is not None
    return biz


def _purchase_with_lot(db_session, seeded_business, product, now, lot_code="LOT-1"):
    pv = InventoryMovement(
        business_id=seeded_business.id, product_id=product.id, type="purchase",
        quantity=10, unit_cost=1.0, unit_price=None, movement_date=now, note=None,
    )
    db_session.add(pv)
    db_session.flush()
    lot = InventoryLot(
        business_id=seeded_business.id, movement_id=pv.id, product_id=product.id,
        lot_code=lot_code, received_at=now, unit_cost=1.0, qty_received=10, qty_remaining=10,
    )
    db_session.add(lot)
    db_session.flush()
    return pv, lot


def test_financial_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    ind = svc._financial_indicators()
    assert "margin_pct" in ind
    assert "expense_ratio" in ind


def test_financial_indicators_with_sale(db_session, seeded_business, sample_product):
    """REQ-2: margin_pct coincide con gross_margin_pct del monthly_profit_report."""
    now = datetime.now(timezone.utc)
    _pv, lot = _purchase_with_lot(db_session, seeded_business, sample_product, now)
    mv = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-2, unit_cost=1.0, unit_price=4.0, movement_date=now, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=2, unit_cost=1.0))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    ind = svc._financial_indicators(now)
    # ventas = 2*4 = 8, cogs = 2*1 = 2, margen bruto = 6/8 = 75%
    assert ind["margin_pct"] == 75.0
    assert ind["expense_ratio"] == 0.0


def test_growth_and_extraction_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators()
    result = svc._add_growth_and_extraction(financial)
    assert "sales_growth_pct" in result
    assert "extraction_ratio" in result


def test_extraction_ratio_uses_extractions_sum(db_session, seeded_business, sample_product):
    from app.models import MoneyExtraction

    now = datetime.now(timezone.utc)
    _pv, lot = _purchase_with_lot(db_session, seeded_business, sample_product, now)
    mv = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-2, unit_cost=1.0, unit_price=4.0, movement_date=now, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=2, unit_cost=1.0))
    db_session.add(MoneyExtraction(business_id=seeded_business.id, party="Negocio",
                                   amount=3.0, concept="retiro", extraction_date=now))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators(now)
    result = svc._add_growth_and_extraction(financial, now)
    # net_total = ventas(8) - cogs(2) - gastos(0) = 6; extracciones 3 → ratio 0.5
    assert result["extraction_ratio"] == 0.5


def test_current_cash_relative_without_baseline(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    result = svc.current_cash()
    assert result["mode"] == "relative"
    assert result["amount"] is None


def test_current_cash_real_with_baseline(db_session, seeded_business):
    from app.models import CashBaseline

    db_session.add(CashBaseline(business_id=seeded_business.id,
                                baseline_date=datetime.now(timezone.utc), amount=500.0))
    db_session.commit()
    svc = CoachService(db_session, seeded_business.id)
    result = svc.current_cash()
    assert result["mode"] == "real"
    assert result["amount"] == 500.0


def test_operational_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators()
    op = svc._operational_indicators(financial)
    assert "dead_stock_items" in op
    assert isinstance(op["dead_stock_items"], list)
    assert "restock_risk_ratio" in op
    assert "turnover_days" in op


def test_customer_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    cust = svc._customer_indicators()
    assert "customer_concentration" in cust
    assert "top_customer_name" in cust
    assert isinstance(cust["churned_customers"], list)


def test_customer_indicators_without_customers_no_crash(db_session, seeded_business):
    """H-004: negocio sin ningún documento de venta -> sin división por cero."""
    svc = CoachService(db_session, seeded_business.id)
    cust = svc._customer_indicators()
    assert cust["customer_concentration"] is None
    assert cust["top_customer_name"] is None
    assert cust["recurring_customers_pct"] is None
    assert cust["churned_customers"] == []


def test_growth_pct_none_when_prior_months_have_no_sales(db_session, seeded_business):
    """H-004: sin ventas en los 3 meses previos (prior_avg=0) -> None, no división por cero."""
    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators()
    result = svc._add_growth_and_extraction(financial)
    assert result["sales_growth_pct"] is None


def test_customer_concentration_uses_sales_documents(db_session, seeded_business):
    from app.models import Customer, SalesDocument

    now = datetime.now(timezone.utc)
    issue = now - timedelta(minutes=5)
    client = Customer(business_id=seeded_business.id, name="Acme", client_id="ACME-1", address="",
                      phone="", email="")
    db_session.add(client)
    db_session.flush()
    now_str = issue.strftime("%Y%m")
    db_session.add_all([
        SalesDocument(business_id=seeded_business.id, doc_type="F", year_month=now_str, seq=i, code=f"F-{i}",
                      client_id=client.id, client_name="Acme", issuer_name="Test", issue_date=issue,
                      total=80.0, subtotal=80.0, created_at=issue)
        for i in range(1, 3)
    ])
    db_session.add(SalesDocument(business_id=seeded_business.id, doc_type="F", year_month=now_str, seq=100,
                                 code="F-0100", client_id="", client_name="Otro", issuer_name="Test",
                                 issue_date=issue, total=20.0, subtotal=20.0, created_at=issue))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    cust = svc._customer_indicators(now)
    # Acme = 160/180 = 88.9%
    assert cust["top_customer_name"] == "Acme"
    assert cust["customer_concentration"] == pytest.approx(160.0 / 180.0)


def test_health_score_unknown_without_sales(db_session, seeded_business):
    """REQ-13: negocio sin ventas este mes -> 'unknown', no un score numérico engañoso."""
    svc = CoachService(db_session, seeded_business.id)
    result = svc.health_score()
    assert result["score"] is None
    assert result["level"] == "unknown"
    assert result["message"]


def test_health_score_shape(db_session, seeded_business, sample_product):
    now = datetime.now(timezone.utc)
    _pv, lot = _purchase_with_lot(db_session, seeded_business, sample_product, now)
    mv = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-2, unit_cost=1.0, unit_price=4.0, movement_date=now, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=2, unit_cost=1.0))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    result = svc.health_score(now)
    assert 1.0 <= result["score"] <= 10.0
    assert result["level"] != "unknown"
    assert "financial" in result
    assert "operational" in result
    assert "cash" in result


def test_recommendations_return_rules(db_session, seeded_business, sample_product):
    now = datetime.now(timezone.utc)
    _pv, lot = _purchase_with_lot(db_session, seeded_business, sample_product, now)
    mv = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-2, unit_cost=1.0, unit_price=4.0, movement_date=now, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=2, unit_cost=1.0))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    recs = svc.recommendations(now)
    assert isinstance(recs, list)
    for r in recs:
        assert r["rule_key"]
        assert r["severity"] in ("yellow", "red")
        assert r["action"]


def test_recommendations_empty_without_sales(db_session, seeded_business):
    """REQ-13: sin ventas, recommendations() no evalúa reglas sobre ratios sin sentido."""
    svc = CoachService(db_session, seeded_business.id)
    assert svc.recommendations() == []


def test_health_score_and_recommendations_share_cached_indicators(db_session, seeded_business, sample_product):
    """F-004: una sola instancia no debe recalcular el pipeline dos veces."""
    now = datetime.now(timezone.utc)
    _pv, lot = _purchase_with_lot(db_session, seeded_business, sample_product, now)
    mv = InventoryMovement(
        business_id=seeded_business.id, product_id=sample_product.id, type="sale",
        quantity=-2, unit_cost=1.0, unit_price=4.0, movement_date=now, note=None,
    )
    db_session.add(mv)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=2, unit_cost=1.0))
    db_session.commit()

    svc = CoachService(db_session, seeded_business.id)
    report = svc.health_score(now)
    recs = svc.recommendations(now)
    assert svc._indicators_cache is not None
    assert report["financial"] is svc._indicators_cache["financial"]
    assert isinstance(recs, list)


def test_mark_action_suppresses(db_session, seeded_business):
    from app.models import User

    user = User(username="coach", password_hash="x", role="owner")
    db_session.add(user)
    db_session.commit()
    svc = CoachService(db_session, seeded_business.id)
    svc.mark_action("margin_drop", "dismissed", user_id=user.id)
    keys = svc._dismissed_or_done_keys()
    assert "margin_drop" in keys