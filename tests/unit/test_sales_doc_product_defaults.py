"""H-711: autorrelleno de precio en Documentos con un SKU inexistente devuelve vacío, no 404."""

from __future__ import annotations

from unittest.mock import MagicMock

from app.models import Product
from app.routers import ui_context as _uc
from app.routers.ui_sales_documents import sales_doc_product_defaults


def _request() -> MagicMock:
    req = MagicMock()
    req.session = {}
    return req


def test_unknown_sku_returns_empty_fragment(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _req: business_id)

    response = sales_doc_product_defaults(request=_request(), product="NO-EXISTE", db=db_session)

    assert response.status_code == 200
    assert response.body == b""


def test_known_sku_fills_price(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _req: business_id)
    db_session.add(Product(business_id=business_id, sku="SD-1", name="Doc", min_stock=0, default_sale_price=3.5))
    db_session.commit()

    response = sales_doc_product_defaults(request=_request(), product="SD-1", db=db_session)

    body = response.body.decode("utf-8")
    assert 'id="sales-doc-unit-price"' in body and 'value="3.5"' in body and 'hx-swap-oob="true"' in body
