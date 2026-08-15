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
        "app.routers.ui_sales_documents.require_active_business_id",
        lambda _db, _request: business_id,
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
        "app.routers.ui_products.require_active_business_id",
        lambda _db, _request: business_id,
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
