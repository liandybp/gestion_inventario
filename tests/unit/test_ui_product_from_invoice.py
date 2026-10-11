"""Regresión H-102: "Importar productos desde factura" crea los productos en el negocio activo.

Antes construía Product(...) sin business_id (producto sin negocio, invisible y con el SKU
global bloqueado), buscaba existencia en todos los negocios y no hacía rollback por línea.
"""

from __future__ import annotations

import io
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from app.invoice_parsers import InvoiceLine, ParsedInvoice
from app.models import Business, Product
from app.routers import ui_products


def _patch_route(monkeypatch, business_id: int, lines: list[InvoiceLine]) -> None:
    monkeypatch.setattr(ui_products, "ensure_admin_or_owner", lambda _db, _req: None)
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(ui_products, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(
        ui_products,
        "get_active_business_config",
        lambda _db, _req: SimpleNamespace(purchase=SimpleNamespace(invoice_vat_rate=1.0, invoice_fx_rate=None)),
    )
    monkeypatch.setattr(
        ui_products,
        "parse_autodoc_pdf",
        lambda _f: ParsedInvoice(invoice_number="F-1", invoice_date=None, lines=lines),
    )


def _pdf() -> SimpleNamespace:
    return SimpleNamespace(filename="factura.pdf", content_type="application/pdf", file=io.BytesIO(b"%PDF"))


def test_product_from_invoice_uses_active_business_and_isolates_failed_lines(
    db_session: Session, business_id: int, monkeypatch
) -> None:
    other = Business(code="otro", name="Otro negocio")
    db_session.add(other)
    db_session.commit()
    db_session.add_all(
        [
            Product(business_id=business_id, sku="INV-EXIST", name="Ya existía", min_stock=0, lead_time_days=0),
            Product(business_id=other.id, sku="INV-OTRO", name="De otro negocio", min_stock=0, lead_time_days=0),
        ]
    )
    db_session.commit()

    _patch_route(
        monkeypatch,
        business_id,
        [
            InvoiceLine(sku="INV-OTRO", name="Choca con otro negocio", quantity=1, net_unit_price=1.0),
            InvoiceLine(sku="INV-NUEVO", name="Producto nuevo", quantity=1, net_unit_price=2.5),
            InvoiceLine(sku="INV-EXIST", name="Ya existía", quantity=1, net_unit_price=1.0),
        ],
    )
    request = MagicMock()
    request.session = {}

    response = ui_products.product_from_invoice(request=request, invoice_pdf=_pdf(), db=db_session)

    nuevo = db_session.query(Product).filter(Product.sku == "INV-NUEVO").one()
    assert nuevo.business_id == business_id
    assert nuevo.default_purchase_cost == 2.5
    assert nuevo.default_sale_price is None
    assert nuevo.unit_of_measure == "u"
    # La línea que choca con otro negocio falla sin envenenar la sesión para las siguientes.
    assert db_session.query(Product).filter(Product.sku == "INV-OTRO").count() == 1
    assert db_session.query(Product).filter(Product.business_id.is_(None)).count() == 0
    body = response.body.decode("utf-8")
    assert "1 productos creados" in body
    assert "1 productos ya existían" in body
    assert "INV-OTRO" in body  # reportado como error
