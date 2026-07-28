from __future__ import annotations

from datetime import datetime, timezone

from app.utils import get_session_secret, month_range, normalize_text, query_match


def test_month_range_from_naive_datetime_uses_utc_boundaries() -> None:
    start, end = month_range(datetime(2026, 7, 28, 15, 12, 3))
    assert start == datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_month_range_december_rollover() -> None:
    start, end = month_range(datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc))
    assert start == datetime(2026, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert end == datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def test_normalize_text_removes_accents() -> None:
    assert normalize_text("Almacén") == "Almacen"
    assert normalize_text("Almacén Central") == "Almacen Central"
    assert normalize_text("  camiOn  ") == "camiOn"


def test_query_match_is_accent_and_case_insensitive() -> None:
    assert query_match("almacen", "Almacen Central")
    assert query_match("Camiseta", "camiseta azul")
    assert not query_match("zapato", "camiseta azul")
    assert query_match("", "anything")


def test_get_session_secret_prefers_env(monkeypatch) -> None:
    monkeypatch.setenv("SESSION_SECRET", "my-secret")
    assert get_session_secret() == "my-secret"


def test_get_session_secret_generates_when_missing(monkeypatch) -> None:
    monkeypatch.delenv("SESSION_SECRET", raising=False)
    secret = get_session_secret()
    assert isinstance(secret, str)
    assert len(secret) == 64


