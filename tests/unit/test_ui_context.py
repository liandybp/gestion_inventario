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
    monkeypatch.setattr(uc, "require_active_business_id", lambda _db, _req: calls.append(1) or 7)
    ctx = _ctx()
    assert ctx.business_id == 7
    assert ctx.business_id == 7
    assert len(calls) == 1


def test_business_id_does_not_cache_failure(monkeypatch):
    calls = []
    def _boom(_db, _req):
        calls.append(1)
        raise HTTPException(status_code=409, detail="Active business_id is required")
    monkeypatch.setattr(uc, "require_active_business_id", _boom)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    with pytest.raises(HTTPException):
        ctx.business_id
    assert len(calls) == 2


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
