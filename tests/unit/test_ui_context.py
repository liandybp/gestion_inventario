from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.routers import ui_context as uc


def _ctx(request=None, db=None):
    return uc.UIContext(request=request if request is not None else MagicMock(),
                        db=db if db is not None else MagicMock())


def test_user_resolved_once_and_cached(monkeypatch):
    calls = []
    fake_user = MagicMock()
    monkeypatch.setattr(uc, "get_current_user_from_session",
                        lambda _db, _req: calls.append(1) or fake_user)
    ctx = _ctx()
    assert ctx.user is fake_user
    assert ctx.user is fake_user
    assert len(calls) == 1


def test_user_caches_none(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: calls.append(1) or None)
    ctx = _ctx()
    assert ctx.user is None
    assert ctx.user is None
    assert len(calls) == 1


def test_business_id_resolved_once(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: calls.append(1) or 7)
    ctx = _ctx()
    assert ctx.business_id == 7
    assert ctx.business_id == 7
    assert len(calls) == 1


def test_business_id_409_every_read_without_re_resolving(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: calls.append(1) or None)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    with pytest.raises(HTTPException):
        ctx.business_id
    assert len(calls) == 1


def _user_with_role(role):
    u = MagicMock()
    u.role = role
    u.is_active = True
    return u


def test_ensure_admin_403_for_owner(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("owner"))
    with pytest.raises(HTTPException) as e:
        _ctx().ensure_admin()
    assert e.value.status_code == 403
    assert e.value.detail == "Admin required"


def test_ensure_admin_ok_for_admin(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("admin"))
    assert _ctx().ensure_admin() is None


def test_ensure_admin_or_owner_ok_for_owner(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("owner"))
    assert _ctx().ensure_admin_or_owner() is None


def test_ensure_admin_or_owner_403_for_operator(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("operator"))
    with pytest.raises(HTTPException) as e:
        _ctx().ensure_admin_or_owner()
    assert e.value.status_code == 403


def test_ctx_db_is_the_injected_db():
    db = MagicMock(spec=Session)
    ctx = _ctx(db=db)
    assert ctx.db is db


def test_business_id_raises_409_when_no_business(monkeypatch):
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    assert e.value.detail == "Active business_id is required"


def test_config_does_not_raise_before_business_id(monkeypatch):
    # config lee active_business_id (None → BusinessConfig()), no lanza el 409.
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    monkeypatch.setattr(uc, "get_business_config", lambda _db, _bid: "DEFAULT")
    ctx = _ctx()
    assert ctx.config == "DEFAULT"


def test_all_properties_resolve_user_once(monkeypatch):
    calls = []
    fake_user = MagicMock()
    monkeypatch.setattr(uc, "get_current_user_from_session",
                        lambda _db, _req: calls.append(1) or fake_user)
    monkeypatch.setattr(uc, "get_active_business_id",
                        lambda _db, _req, *, user: calls.append(1) or 7)
    monkeypatch.setattr(uc, "get_business_code", lambda _db, _bid: calls.append(1) or "TEST")
    monkeypatch.setattr(uc, "get_business_config", lambda _db, _bid: calls.append(1) or "CFG")
    ctx = _ctx()
    _ = ctx.user
    _ = ctx.active_business_id
    _ = ctx.business_id
    _ = ctx.business_code
    _ = ctx.config
    # una sola resolución del usuario (las otras 3 lecturas usan el id cacheado)
    assert len(calls) == 4


def test_business_code_none_when_no_business(monkeypatch):
    monkeypatch.setattr(uc, "get_active_business_id", lambda _db, _req, *, user: None)
    monkeypatch.setattr(uc, "get_business_code", lambda _db, _bid: None)
    ctx = _ctx()
    assert ctx.business_code is None
