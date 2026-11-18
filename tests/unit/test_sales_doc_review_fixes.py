"""Regresión de los hallazgos de la revisión 5b del PAE pdf-documentos-venta-oferta."""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pdfplumber
import pytest

from app import migrations
from app.business_config import BusinessConfig, IssuerConfig, SalesDocumentsConfig, parse_validity_days
from app.models import Business
from app.routers import ui_sales_documents
from app.routers.ui_common import default_valid_until as form_default_valid_until
from app.sales_document_pdf import build_sales_document_pdf
from app.sales_documents_format import default_valid_until, resolve_valid_until

MM_PER_PT = 25.4 / 72


def _pdf_kwargs(**overrides) -> dict:
    base = dict(
        doc_type="P", doc_label="Presupuesto", code="P1", issue_date=datetime(2026, 11, 18, tzinfo=timezone.utc),
        valid_until=date(2026, 12, 3), currency_code="CUP", issuer_name="X", issuer_holder=None, issuer_tax_id=None,
        issuer_address=None, issuer_phone=None, issuer_email=None, bank_name=None, bank_branch=None,
        bank_account=None, bank_account_currency=None, client_name="C", client_id="1", client_address=None,
        items=[{"description": f"item {i}", "quantity": 1, "unit_price": 1, "line_total": 1} for i in range(40)],
        subtotal=40.0, total=40.0, notes="\n".join(f"nota linea {i}" for i in range(80)),
    )
    base.update(overrides)
    return base


def test_continuation_pages_never_draw_content_over_the_document_line() -> None:
    """Las notas que pasan de página empiezan bajo 'Documento: <código>', no encima."""
    with pdfplumber.open(io.BytesIO(build_sales_document_pdf(**_pdf_kwargs()))) as pdf:
        assert len(pdf.pages) >= 3
        for page in pdf.pages[1:]:
            words = page.extract_words()
            header = [w for w in words if w["text"] == "Documento:"]
            assert header, f"página {page.page_number} sin 'Documento:'"
            header_bottom = header[0]["bottom"]
            body = [w for w in words if w["text"] not in ("Documento:", "P1")]
            assert body, f"página {page.page_number} sin contenido"
            assert min(w["top"] for w in body) > header_bottom, (
                f"página {page.page_number}: contenido a {min(w['top'] for w in body) * MM_PER_PT:.1f} mm, "
                f"encima de 'Documento:' ({header_bottom * MM_PER_PT:.1f} mm)"
            )


@pytest.mark.parametrize(
    ("value", "fallback", "expected"),
    [(None, 15, 15), ("", 15, 15), ("abc", 15, 15), ("30", 15, 30), (30, 15, 30), ("-3", 15, 0), ("400", 15, 365),
     ("", 20, 20), ("x", 999, 365)],
)
def test_parse_validity_days_single_rule(value, fallback, expected) -> None:
    assert parse_validity_days(value, fallback) == expected


def test_form_default_and_server_default_share_the_same_rule() -> None:
    today = datetime.now(timezone.utc).date()
    assert form_default_valid_until(15) == default_valid_until(today, 15).isoformat()
    # Un plazo fuera de rango se acota igual en el formulario y en el servidor.
    assert form_default_valid_until(-5) == today.isoformat()
    issue = datetime.now(timezone.utc)
    assert resolve_valid_until("", doc_type="P", issue_date=issue, validity_days=-5) == issue.date()


def _config(days: int = 15) -> SimpleNamespace:
    return SimpleNamespace(sales_documents=SimpleNamespace(quote_validity_days=days))


def test_draft_does_not_freeze_the_default_due_date() -> None:
    default_iso = (datetime.now(timezone.utc).date() + timedelta(days=15)).isoformat()
    assert ui_sales_documents._draft_valid_until(default_iso, doc_type="P", config=_config(15)) is None
    assert ui_sales_documents._draft_valid_until("", doc_type="P", config=_config(15)) is None


def test_draft_keeps_a_date_chosen_by_the_user() -> None:
    assert ui_sales_documents._draft_valid_until("2099-12-31", doc_type="P", config=_config(15)) == "2099-12-31"
    # Un valor no válido también se conserva, para corregirlo tras el 422.
    assert ui_sales_documents._draft_valid_until("31/12/2099", doc_type="P", config=_config(15)) == "31/12/2099"


def test_draft_never_keeps_a_due_date_for_an_invoice() -> None:
    assert ui_sales_documents._draft_valid_until("2099-12-31", doc_type="F", config=_config(15)) is None


def test_config_backfill_copies_bank_fields_and_validity_days(db_session, monkeypatch) -> None:
    config = BusinessConfig(
        issuer=IssuerConfig(
            name="DyB Autopartes", bank_name="Banco Metropolitano", bank_branch="248",
            bank_account="0598770024085419", bank_account_currency="CUP",
        ),
        sales_documents=SalesDocumentsConfig(quote_validity_days=30),
    )
    monkeypatch.setattr(migrations, "get_session", lambda: db_session)
    monkeypatch.setattr(migrations, "_discover_business_codes_from_config_files", lambda: ["test"])
    monkeypatch.setattr(migrations, "load_business_config", lambda _code=None: config)
    monkeypatch.setattr(migrations, "_backfill_locations", lambda *_a, **_kw: None)

    migrations._backfill_business_config()

    business = db_session.query(Business).filter(Business.code == "test").one()
    assert business.issuer_bank_name == "Banco Metropolitano"
    assert business.issuer_bank_branch == "248"
    assert business.issuer_bank_account == "0598770024085419"
    assert business.issuer_bank_account_currency == "CUP"
    assert business.sales_quote_validity_days == 30


def test_valid_until_error_fragment_uses_the_message_constant(monkeypatch) -> None:
    monkeypatch.setattr(ui_sales_documents, "INVALID_VALID_UNTIL", "Mensaje de prueba")
    request = SimpleNamespace(scope={"type": "http"})
    captured = {}

    def fake_template_response(**kwargs):
        captured.update(kwargs)
        return kwargs

    monkeypatch.setattr(ui_sales_documents.templates, "TemplateResponse", fake_template_response)
    ui_sales_documents._valid_until_error_fragment(request, headers={"X-Modal-Keep": "1"})
    assert captured["context"] == {"message": "Mensaje de prueba"}
    assert captured["status_code"] == 422
    assert captured["headers"] == {"X-Modal-Keep": "1"}
