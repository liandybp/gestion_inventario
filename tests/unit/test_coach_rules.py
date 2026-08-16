from app.services.coach_rules import evaluate


def test_margin_drop_yellow():
    recs = evaluate({"margin_pct": 15.0, "margin_avg_6m": 18.0})
    assert any(r["rule_key"] == "margin_drop" and r["severity"] == "yellow" for r in recs)


def test_margin_drop_red():
    recs = evaluate({"margin_pct": 12.0, "margin_avg_6m": 18.0})
    assert any(r["rule_key"] == "margin_drop" and r["severity"] == "red" for r in recs)


def test_evaluate_catches_errors():
    recs = evaluate(None)  # Error pero no alucina
    assert recs == []


def test_expense_ratio_yellow():
    recs = evaluate({"expense_ratio": 0.35})
    assert any(r["rule_key"] == "expense_ratio_high" and r["severity"] == "yellow" for r in recs)


def test_expense_ratio_red():
    recs = evaluate({"expense_ratio": 0.50})
    assert any(r["rule_key"] == "expense_ratio_high" and r["severity"] == "red" for r in recs)


def test_extraction_ratio_yellow():
    recs = evaluate({"extraction_ratio": 0.90})
    assert any(r["rule_key"] == "extraction_ratio_high" and r["severity"] == "yellow" for r in recs)


def test_extraction_ratio_red():
    recs = evaluate({"extraction_ratio": 1.2})
    assert any(r["rule_key"] == "extraction_ratio_high" and r["severity"] == "red" for r in recs)


def test_no_rule_without_crossing_threshold():
    recs = evaluate({"margin_pct": 17.5, "margin_avg_6m": 18.0})
    assert all(r["rule_key"] != "margin_drop" for r in recs)


def test_dead_stock_impact():
    recs = evaluate({"dead_stock_value": 1200.0, "dead_stock_items": [{"sku": "A", "name": "Bujía", "value": 1200.0}]})
    rec = next(r for r in recs if r["rule_key"] == "dead_stock")
    assert rec["impact_amount"] == 1200.0


def test_dead_stock_none_when_no_items():
    recs = evaluate({"dead_stock_value": 0.0, "dead_stock_items": []})
    assert all(r["rule_key"] != "dead_stock" for r in recs)


def test_customer_concentration():
    recs = evaluate({"customer_concentration": 0.40, "top_customer_name": "Acme"})
    assert any(r["rule_key"] == "customer_concentration" for r in recs)


def test_customer_concentration_red():
    recs = evaluate({"customer_concentration": 0.60, "top_customer_name": "Acme"})
    rec = next(r for r in recs if r["rule_key"] == "customer_concentration")
    assert rec["severity"] == "red"


def test_evaluate_empty_dict_no_crash():
    """H-004: dict vacío (todas las claves ausentes) no debe lanzar ni generar reglas falsas."""
    assert evaluate({}) == []


def test_margin_drop_none_when_avg_missing():
    """H-004: margin_pct presente pero sin histórico (avg6=None) -> no evalúa la regla."""
    recs = evaluate({"margin_pct": 10.0, "margin_avg_6m": None})
    assert all(r["rule_key"] != "margin_drop" for r in recs)


def test_expense_ratio_none_no_crash():
    recs = evaluate({"expense_ratio": None})
    assert all(r["rule_key"] != "expense_ratio_high" for r in recs)


def test_dead_stock_value_without_items_no_crash():
    """H-004: dead_stock_value>0 pero items=None (indicador parcialmente calculado) no debe lanzar KeyError."""
    recs = evaluate({"dead_stock_value": 500.0, "dead_stock_items": None})
    assert all(r["rule_key"] != "dead_stock" for r in recs)