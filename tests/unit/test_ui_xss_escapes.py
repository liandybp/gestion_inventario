"""Regresión H-029/H-034: campos de cliente y UOM escapados en HTMLResponse.

Un cliente/producto con HTML/JS en esos campos no debe inyectar markup en la
respuesta del panel de venta (XSS almacenado).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.models import Customer, Product
from app.routers.ui_products import product_defaults_sale
from app.routers.ui_sales_documents import sales_doc_customer_defaults


def _request() -> MagicMock:
    req = MagicMock()
    req.session = {}
    req.url.path = "/ui/sales"
    return req


def test_customer_defaults_escapes_client_id_and_address(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.ui_context.get_active_business_id",
        lambda _db, _request, **_kw: business_id,
    )
    payload = '<script>alert(1)</script>'
    customer = Customer(
        business_id=business_id,
        client_id=payload,
        name="Cliente XSS",
        address='"><img src=x onerror=alert(1)>',
    )
    db_session.add(customer)
    db_session.commit()

    response = sales_doc_customer_defaults(
        request=_request(),
        client_name="Cliente XSS",
        db=db_session,
    )

    body = response.body.decode("utf-8") if isinstance(response.body, bytes) else str(response.body)
    assert "<script>" not in body
    assert "&lt;script&gt;" in body
    assert "<img" not in body  # el address quedó escapado (no es una etiqueta viva)
    assert "&lt;img" in body


def test_product_defaults_sale_escapes_unit_of_measure(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(
        "app.routers.ui_context.get_active_business_id",
        lambda _db, _request, **_kw: business_id,
    )
    product = Product(
        business_id=business_id,
        sku="SKU_XSS_UOM",
        name="Producto XSS",
        unit_of_measure='"><script>alert(2)</script>',
        min_stock=0,
    )
    db_session.add(product)
    db_session.commit()

    response = product_defaults_sale(
        request=_request(),
        product="SKU_XSS_UOM",
        db=db_session,
    )

    body = response.body.decode("utf-8") if isinstance(response.body, bytes) else str(response.body)
    assert "<script>" not in body
    assert "&lt;script&gt;" in body


def test_home_charts_json_cannot_break_out_of_script_tag() -> None:
    """Un nombre de producto con "</script>" no debe cerrar el <script type="application/json">
    de los gráficos de Inicio (antes: json.dumps + |safe → XSS almacenado)."""
    import json
    import re
    from datetime import datetime, timezone

    from app.routers.ui_common import templates
    from app.routers.ui_home import _home_charts_context

    evil = '</script><img src=x onerror=alert(3)>'
    reporting = MagicMock()
    reporting.monthly_profit_report.return_value = (
        {},
        [{"sku": "SKU_XSS", "name": evil, "sales": 10.0, "qty": 1.0}],
    )
    reporting.daily_sales_series.return_value = [{"day": "2026-10-01", "sales": 10.0}]
    reporting.monthly_overview.return_value = [
        {"month": "2026-10", "sales": 10.0, "purchases": 5.0, "gross_profit": 5.0}
    ]
    reporting.sales_metrics_table.return_value = []

    ctx = _home_charts_context(reporting, datetime(2026, 10, 15, tzinfo=timezone.utc))
    html = templates.get_template("features/home/home_charts.html").render(ctx)

    assert "<img" not in html
    assert html.count("</script>") == 3  # solo los cierres legítimos de los 3 bloques JSON

    match = re.search(r'id="monthly-sales-pie-data">(.*?)</script>', html, re.S)
    assert match is not None
    assert json.loads(match.group(1))["labels"] == [f"SKU_XSS - {evil}"]


def test_widget_table_note_is_escaped() -> None:
    from app.routers.ui_common import templates

    evil = '<script>alert("pm")</script>'
    w = {"payload": {"columns": ["A"], "rows": [[1]], "note": evil}}
    html = templates.get_template("features/agent/widget_table.html").render(w=w)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
