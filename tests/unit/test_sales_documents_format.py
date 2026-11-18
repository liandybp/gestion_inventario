"""Funciones puras de formato y de fecha de vencimiento (REQ-3, REQ-5, REQ-6)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from app.sales_documents_format import (
    INVALID_VALID_UNTIL,
    currency_column_label,
    format_amount,
    format_doc_date,
    format_quantity,
    resolve_valid_until,
)

ISSUE = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "code,label",
    [
        ("GEN", "IMPORTE CUP"),
        (" gen ", "IMPORTE CUP"),
        ("USD", "IMPORTE USD"),
        ("eur", "IMPORTE EUR"),
        ("", "IMPORTE"),
        (None, "IMPORTE"),
    ],
)
def test_currency_column_label_import(code, label) -> None:
    assert currency_column_label("IMPORTE", code) == label


def test_currency_column_label_price() -> None:
    assert currency_column_label("PRECIO", "usd") == "PRECIO USD"
    assert currency_column_label("PRECIO", "GEN") == "PRECIO CUP"
    assert currency_column_label("PRECIO", None) == "PRECIO"


@pytest.mark.parametrize(
    "value,expected",
    [(303875, "303,875.00"), (490875.0, "490,875.00"), (1234.5, "1,234.50"), (0, "0.00"), (None, "0.00")],
)
def test_format_amount(value, expected) -> None:
    assert format_amount(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (1.0, "1"),
        (2.5, "2.5"),
        (0.0001, "0.0001"),
        (1234567.0, "1,234,567"),
        (10.0, "10"),
        (3.1, "3.1"),
        (0, "0"),
        (None, "0"),
    ],
)
def test_format_quantity(value, expected) -> None:
    assert format_quantity(value) == expected


def test_format_doc_date() -> None:
    assert format_doc_date(date(2026, 12, 3)) == "03/12/2026"
    assert format_doc_date(datetime(2026, 1, 2, 23, 59, tzinfo=timezone.utc)) == "02/01/2026"
    assert format_doc_date(None) == ""


def test_resolve_valid_until_default_is_issue_date_plus_validity_days() -> None:
    assert resolve_valid_until("", doc_type="P", issue_date=ISSUE, validity_days=15) == date(2026, 12, 3)
    assert resolve_valid_until(None, doc_type="P", issue_date=ISSUE, validity_days=0) == date(2026, 11, 18)


def test_resolve_valid_until_accepts_chosen_date() -> None:
    assert resolve_valid_until("2026-12-31", doc_type="P", issue_date=ISSUE, validity_days=15) == date(2026, 12, 31)
    assert resolve_valid_until("2026-11-18", doc_type="p", issue_date=ISSUE, validity_days=15) == date(2026, 11, 18)


@pytest.mark.parametrize("raw", ["2026-12-31", "garbage", "", None])
def test_resolve_valid_until_invoice_is_always_none(raw) -> None:
    assert resolve_valid_until(raw, doc_type="F", issue_date=ISSUE, validity_days=15) is None


@pytest.mark.parametrize("raw", ["31/12/2026", "2026-11-17", "2026-13-40", "20261231", "abc"])
def test_resolve_valid_until_rejects_invalid_values(raw) -> None:
    with pytest.raises(ValueError, match=INVALID_VALID_UNTIL):
        resolve_valid_until(raw, doc_type="P", issue_date=ISSUE, validity_days=15)


def test_resolve_valid_until_keep_mode_returns_current() -> None:
    assert resolve_valid_until(
        "", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=date(2026, 12, 3)
    ) == date(2026, 12, 3)
    assert resolve_valid_until("", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=None) is None
    assert resolve_valid_until(
        "2026-12-20", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=date(2026, 12, 3)
    ) == date(2026, 12, 20)
