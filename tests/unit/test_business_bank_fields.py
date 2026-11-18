"""Datos bancarios y plazo de validez del negocio (REQ-1, REQ-2, REQ-9)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from app.models import Business, User
from app.repositories.business_repository import get_business_config
from app.routers.ui_businesses import business_create, business_edit_form, business_update


@pytest.fixture
def admin_request() -> MagicMock:
    req = MagicMock()
    req.session = {"username": "admin", "active_business_id": 1}
    req.url.path = "/ui/tabs/businesses"
    return req


@pytest.fixture
def admin_user(db_session) -> User:
    user = User(username="admin", password_hash="pbkdf2_sha256$...", role="admin", is_active=True, business_id=1)
    db_session.add(user)
    db_session.commit()
    return user


def _business(db_session, **fields) -> Business:
    biz = Business(code="dyb", name="DyB Autopartes", **fields)
    db_session.add(biz)
    db_session.commit()
    return biz


def test_create_business_saves_bank_data_and_validity(admin_request, db_session, admin_user) -> None:
    response = business_create(
        request=admin_request,
        code="nuevo",
        name="Nuevo",
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        sales_quote_validity_days="30",
        locations_pos="",
        locations_default_pos="",
        db=db_session,
    )

    assert response.status_code == 200
    biz = db_session.scalar(select(Business).where(Business.code == "nuevo"))
    config = get_business_config(db_session, biz.id)
    assert config.issuer.bank_name == "Banco Metropolitano"
    assert config.issuer.bank_branch == "248"
    assert config.issuer.bank_account == "0598770024085419"
    assert config.issuer.bank_account_currency == "CUP"
    assert config.sales_documents.quote_validity_days == 30


def test_create_business_defaults_validity_to_15(admin_request, db_session, admin_user) -> None:
    business_create(
        request=admin_request, code="def", name="Def", sales_quote_validity_days="abc",
        locations_pos="", locations_default_pos="", db=db_session,
    )

    biz = db_session.scalar(select(Business).where(Business.code == "def"))
    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == 15


def test_update_saves_bank_data_and_keeps_the_rest_of_the_issuer(admin_request, db_session, admin_user) -> None:
    biz = _business(
        db_session,
        issuer_holder="Randy Manuel Delgado",
        issuer_tax_id="01030573448",
        issuer_phone="54664732 / 50781349",
        dividends_partners="Liandy,Randy",
    )

    response = business_update(
        request=admin_request,
        business_id=biz.id,
        code="dyb",
        name="DyB Autopartes",
        issuer_holder="Randy Manuel Delgado",
        issuer_tax_id="01030573448",
        issuer_phone="54664732 / 50781349",
        dividends_partners="Liandy,Randy",
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        db=db_session,
    )

    assert response.status_code == 200
    config = get_business_config(db_session, biz.id)
    assert config.issuer.bank_account == "0598770024085419"
    assert config.issuer.holder == "Randy Manuel Delgado"
    assert config.issuer.tax_id == "01030573448"
    assert config.issuer.phone == "54664732 / 50781349"
    assert config.dividends.partners == ["Liandy", "Randy"]


@pytest.mark.parametrize(
    "sent,expected",
    [("30", 30), ("abc", 15), ("-3", 0), ("400", 365), ("0", 0), ("", 15)],
)
def test_update_validity_days_is_bounded_and_non_numeric_keeps_the_current_value(
    admin_request, db_session, admin_user, sent, expected
) -> None:
    biz = _business(db_session, sales_quote_validity_days=15)

    business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        sales_quote_validity_days=sent, db=db_session,
    )

    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == expected


def test_update_with_non_numeric_validity_keeps_a_custom_value(admin_request, db_session, admin_user) -> None:
    biz = _business(db_session, sales_quote_validity_days=45)

    business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        sales_quote_validity_days="abc", db=db_session,
    )

    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == 45


def test_update_trims_bank_fields_to_their_column_size(admin_request, db_session, admin_user) -> None:
    biz = _business(db_session)

    response = business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        issuer_bank_name="B" * 200, issuer_bank_branch="S" * 65, issuer_bank_account="9" * 70,
        issuer_bank_account_currency="CUPCUPCUP1", db=db_session,
    )

    assert response.status_code == 200
    db_session.refresh(biz)
    assert len(biz.issuer_bank_name) == 128
    assert len(biz.issuer_bank_branch) == 64
    assert len(biz.issuer_bank_account) == 64
    assert biz.issuer_bank_account_currency == "CUPCUPCU"


def test_edit_form_shows_the_saved_values_with_maxlength(admin_request, db_session, admin_user) -> None:
    biz = _business(
        db_session,
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        sales_quote_validity_days=30,
    )

    body = business_edit_form(request=admin_request, business_id=biz.id, db=db_session).body.decode("utf-8")

    assert 'name="issuer_bank_name" maxlength="128"' in body
    assert 'value="Banco Metropolitano"' in body
    assert 'value="248"' in body
    assert 'value="0598770024085419"' in body
    assert 'name="issuer_bank_account_currency" maxlength="8"' in body
    assert 'name="sales_quote_validity_days" min="0" max="365"' in body
    assert 'value="30"' in body
