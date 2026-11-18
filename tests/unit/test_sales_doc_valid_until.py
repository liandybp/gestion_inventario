"""Fecha de vencimiento en los handlers de documentos de venta (REQ-3, REQ-4)."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select

from app.models import Business, Location, SalesDocument, SalesDocumentItem
from app.routers import ui_context as _uc
from app.routers.ui_sales_documents import (
    _get_draft,
    sales_doc_cart_add,
    sales_doc_cart_clear,
    sales_doc_cart_remove,
    sales_doc_cart_update,
    sales_doc_issue,
    sales_doc_preview,
    sales_doc_update,
)

ERROR = "Fecha de vencimiento inválida"
CART_LINE = {"sku": None, "description": "Pastillas", "unit_of_measure": "JGO", "quantity": 1.0, "unit_price": 10.0}


def _today() -> date:
    return datetime.now(timezone.utc).date()


@pytest.fixture(autouse=True)
def _active_business(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(_uc, "get_active_business_id", lambda _db, _req, **_kw: business_id)
    db_session.add(Location(business_id=business_id, code="TEST_CENTRAL", name="Central", role="central", is_default=True))
    db_session.commit()


def _request(cart: list[dict] | None = None, draft: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.session = {}
    if cart is not None:
        req.session["sales_doc_cart"] = cart
    if draft is not None:
        req.session["sales_doc_draft"] = draft
    return req


def _form(**over) -> dict:
    data = dict(
        doc_type="P",
        location_code="TEST_CENTRAL",
        client_name="Cliente A",
        client_id="C-1",
        client_address="",
        notes="",
        currency_code="USD",
        valid_until="",
    )
    data.update(over)
    return data


def _issue(db, req, **over):
    return sales_doc_issue(request=req, db=db, **_form(**over))


def _documents(db) -> list[SalesDocument]:
    return list(db.scalars(select(SalesDocument).order_by(SalesDocument.id)))


def _body(response) -> str:
    return response.body.decode("utf-8")


def test_issue_quote_with_chosen_due_date(db_session) -> None:
    chosen = _today() + timedelta(days=40)

    response = _issue(db_session, _request([dict(CART_LINE)]), valid_until=chosen.isoformat())

    assert response.status_code == 200
    (doc,) = _documents(db_session)
    assert doc.doc_type == "P"
    assert doc.valid_until == chosen


def test_issue_quote_with_empty_due_date_uses_issue_date_plus_validity(db_session, business_id) -> None:
    db_session.get(Business, business_id).sales_quote_validity_days = 20
    db_session.commit()

    _issue(db_session, _request([dict(CART_LINE)]), valid_until="")

    (doc,) = _documents(db_session)
    assert doc.valid_until == doc.issue_date.date() + timedelta(days=20)


def test_issue_invoice_never_stores_due_date(db_session) -> None:
    _issue(db_session, _request([dict(CART_LINE)]), doc_type="F", valid_until=(_today() + timedelta(days=40)).isoformat())

    (doc,) = _documents(db_session)
    assert doc.doc_type == "F"
    assert doc.valid_until is None


@pytest.mark.parametrize("bad", ["31/12/2026", "no-es-fecha", (_today() - timedelta(days=1)).isoformat()])
def test_issue_quote_with_invalid_due_date_returns_422_html_and_keeps_everything(db_session, bad) -> None:
    req = _request([dict(CART_LINE)])

    response = _issue(db_session, req, valid_until=bad, notes="Nota importante", client_name="Cliente B", client_id="C-2")

    assert response.status_code == 422
    assert response.media_type == "text/html"
    assert ERROR in _body(response)
    assert _documents(db_session) == []
    assert req.session["sales_doc_cart"] == [CART_LINE]
    draft = _get_draft(req)
    assert draft["valid_until"] == bad
    assert draft["client_name"] == "Cliente B"
    assert draft["notes"] == "Nota importante"
    assert db_session.scalar(select(func.count()).select_from(SalesDocument)) == 0


def test_issue_invoice_ignores_a_garbage_due_date(db_session) -> None:
    response = _issue(db_session, _request([dict(CART_LINE)]), doc_type="F", valid_until="31/12/2026")

    assert response.status_code == 200
    assert len(_documents(db_session)) == 1


def test_issue_form_shows_the_default_due_date_and_keeps_the_typed_one(db_session, business_id) -> None:
    db_session.get(Business, business_id).sales_quote_validity_days = 15
    db_session.commit()
    now = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    past_due_date = "2026-11-17"

    with patch("app.routers.ui_common.datetime", wraps=datetime) as default_clock, patch(
        "app.routers.ui_sales_documents.datetime", wraps=datetime
    ) as issue_clock:
        default_clock.now.return_value = now
        issue_clock.now.return_value = now
        rejected = _issue(db_session, _request([dict(CART_LINE)]), valid_until=past_due_date)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))

    assert rejected.status_code == 422
    assert f'name="valid_until" value="{past_due_date}"' in _body(rejected)
    assert 'name="valid_until" value="2026-12-03"' in _body(empty_draft)


def test_cart_handlers_keep_the_due_date_in_the_draft(db_session) -> None:
    chosen = "2099-12-31"

    req = _request()
    sales_doc_cart_add(
        request=req, db=db_session, product="", description="Pastillas", quantity="1", unit_price="10",
        **_form(valid_until=chosen),
    )
    assert _get_draft(req)["valid_until"] == chosen
    assert len(req.session["sales_doc_cart"]) == 1

    req.session["sales_doc_draft"] = {}
    sales_doc_cart_update(
        request=req, db=db_session, index=0, description="Pastillas", quantity="2", unit_price="10",
        **_form(valid_until=chosen),
    )
    assert _get_draft(req)["valid_until"] == chosen

    req.session["sales_doc_draft"] = {}
    sales_doc_cart_remove(request=req, db=db_session, index=0, **_form(valid_until=chosen))
    assert _get_draft(req)["valid_until"] == chosen
    assert req.session["sales_doc_cart"] == []

    req.session["sales_doc_draft"] = {}
    response = sales_doc_cart_clear(request=req, db=db_session, **_form(valid_until=chosen))
    assert _get_draft(req)["valid_until"] == chosen
    assert f'name="valid_until" value="{chosen}"' in _body(response)


def test_preview_quote_shows_due_date_and_carries_it_to_the_issue_form(db_session) -> None:
    response = sales_doc_preview(request=_request([dict(CART_LINE)]), db=db_session, **_form(valid_until="2099-12-31"))

    body = _body(response)
    assert response.status_code == 200
    assert "Vencimiento:" in body and "31/12/2099" in body
    assert 'name="valid_until" value="2099-12-31"' in body


def test_preview_invoice_has_no_due_date(db_session) -> None:
    response = sales_doc_preview(
        request=_request([dict(CART_LINE)]), db=db_session, **_form(doc_type="F", valid_until="2099-12-31")
    )

    assert response.status_code == 200
    assert "Vencimiento" not in _body(response)


def test_preview_with_invalid_due_date_returns_422_html(db_session) -> None:
    req = _request([dict(CART_LINE)])

    response = sales_doc_preview(request=req, db=db_session, **_form(valid_until="31/12/2026"))

    assert response.status_code == 422
    assert ERROR in _body(response)
    assert _get_draft(req)["valid_until"] == "31/12/2026"


def _seed_quote(db, business_id: int, *, valid_until: date | None, doc_type: str = "P") -> SalesDocument:
    doc = SalesDocument(
        business_id=business_id,
        doc_type=doc_type,
        year_month="202611",
        seq=1,
        code=f"{doc_type}2026111800001",
        issue_date=datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc),
        valid_until=valid_until,
        issuer_name="Emisor",
        client_name="A",
        client_id="C-A",
        currency_code="USD",
        currency_symbol="USD",
        subtotal=10.0,
        total=10.0,
    )
    db.add(doc)
    db.flush()
    db.add(SalesDocumentItem(document_id=doc.id, line_no=1, description="Linea 1", quantity=1, unit_price=4, line_total=4))
    db.add(SalesDocumentItem(document_id=doc.id, line_no=2, description="Linea 2", quantity=1, unit_price=6, line_total=6))
    db.commit()
    return doc


def _update(db, doc_id: int, **fields):
    req = _request()
    req.form = AsyncMock(return_value=fields)
    return asyncio.run(sales_doc_update(request=req, doc_id=doc_id, db=db))


def test_update_quote_changes_only_the_due_date(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3))
    items = list(db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)))
    fields = {"client_name": "A", "client_id": "C-A", "valid_until": "2026-12-20"}
    for it in items:
        fields[f"desc_{it.id}"] = it.description
        fields[f"qty_{it.id}"] = str(it.quantity)
        fields[f"price_{it.id}"] = str(it.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until == date(2026, 12, 20)
    assert float(doc.total) == 10.0
    assert db_session.scalar(select(func.count()).select_from(SalesDocumentItem)) == 2


def test_update_old_quote_with_empty_due_date_stays_null(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=None)
    items = list(db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)))
    fields = {"client_name": "A", "client_id": "C-A", "notes": "Solo notas", "valid_until": ""}
    for it in items:
        fields[f"desc_{it.id}"] = it.description
        fields[f"qty_{it.id}"] = str(it.quantity)
        fields[f"price_{it.id}"] = str(it.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
    assert doc.notes == "Solo notas"


def test_update_with_invalid_due_date_changes_nothing(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3))
    first = db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)).first()

    response = _update(
        db_session, doc.id,
        client_name="B", client_id="C-B", valid_until="31/12/2026", **{f"del_{first.id}": "on"},
    )

    assert response.status_code == 422
    assert response.media_type == "text/html"
    assert ERROR in _body(response)
    db_session.refresh(doc)
    assert doc.client_name == "A"
    assert doc.valid_until == date(2026, 12, 3)
    assert float(doc.total) == 10.0
    assert db_session.scalar(select(func.count()).select_from(SalesDocumentItem)) == 2


def test_update_invoice_forces_null_due_date(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3), doc_type="F")

    items_query = (
        select(SalesDocumentItem)
        .where(SalesDocumentItem.document_id == doc.id)
        .order_by(SalesDocumentItem.line_no)
    )
    fields = {"client_name": "A", "client_id": "C-A", "valid_until": "2099-12-31"}
    for item in db_session.scalars(items_query):
        fields[f"desc_{item.id}"] = item.description
        fields[f"qty_{item.id}"] = str(item.quantity)
        fields[f"price_{item.id}"] = str(item.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
    assert float(doc.total) == 10.0
    assert [float(item.line_total) for item in db_session.scalars(items_query)] == [4.0, 6.0]
