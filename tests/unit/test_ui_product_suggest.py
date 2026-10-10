"""REQ-1/REQ-2: GET /ui/product-suggest."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from app.models import Business, Product
from app.routers import ui_products


def _suggest(db: Session, business_id: int, monkeypatch, q: str, fmt: str = "label") -> str:
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    request = MagicMock()
    request.session = {}
    resp = ui_products.product_suggest(request=request, q=q, fmt=fmt, db=db)
    return resp.body.decode("utf-8")


def _add(db: Session, business_id: int, sku: str, name: str) -> None:
    db.add(Product(business_id=business_id, sku=sku, name=name, min_stock=0, lead_time_days=0))


def test_finds_product_beyond_first_200(db_session: Session, business_id: int, monkeypatch) -> None:
    for i in range(1, 206):
        _add(db_session, business_id, f"SUG{i:04d}", f"Producto {i:04d}")
    db_session.commit()

    body = _suggest(db_session, business_id, monkeypatch, "0205")

    assert 'value="SUG0205 - Producto 0205"' in body


def test_short_query_returns_empty(db_session: Session, business_id: int, monkeypatch) -> None:
    _add(db_session, business_id, "SUGA", "Alfa")
    db_session.commit()
    assert _suggest(db_session, business_id, monkeypatch, "a").strip() == ""


def test_other_business_not_returned_and_html_escaped(db_session: Session, business_id: int, monkeypatch) -> None:
    other = Business(code="otro", name="Otro negocio")
    db_session.add(other)
    db_session.commit()
    _add(db_session, other.id, "AJENO", "Producto Ajeno")
    _add(db_session, business_id, "XSS1", "<b>Producto</b>")
    db_session.commit()

    assert "Ajeno" not in _suggest(db_session, business_id, monkeypatch, "Producto")
    body = _suggest(db_session, business_id, monkeypatch, "Producto")
    assert "<b>" not in body
    assert "&lt;b&gt;Producto&lt;/b&gt;" in body


@pytest.mark.parametrize(
    ("fmt", "expected"),
    [
        ("sku", '<option value="FMT1">FMT1 - Formato</option>'),
        ("sale", 'value="FMT1 - Formato"'),  # formato retirado (Ventas filtra por stock) → label
        ("raro", 'value="FMT1 - Formato"'),
    ],
)
def test_formats(db_session: Session, business_id: int, monkeypatch, fmt: str, expected: str) -> None:
    _add(db_session, business_id, "FMT1", "Formato")
    db_session.commit()
    assert expected in _suggest(db_session, business_id, monkeypatch, "Formato", fmt=fmt)


@pytest.mark.parametrize(
    "template",
    ["partials/sale_panel.html", "partials/sale_edit_form.html", "partials/tab_transfers.html"],
)
def test_stock_filtered_lists_do_not_use_dynamic_suggest(template: str) -> None:
    """H-201/H-202: Ventas y Traspasos sugieren solo productos con stock en la ubicación."""
    from pathlib import Path

    assert "data-product-suggest" not in Path("app/templates", template).read_text(encoding="utf-8")
