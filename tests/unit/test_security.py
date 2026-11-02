from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import User
from app.security import _UNSET, get_active_business_id, require_admin_or_owner_api


class _Req:
    def __init__(self, username):
        self.session = {"username": username}


def test_require_admin_or_owner_allows_admin_and_owner(db_session):
    for role in ("admin", "owner"):
        u = User(username=f"{role}_t", password_hash="x", role=role, is_active=True, business_id=1)
        db_session.add(u)
        db_session.commit()
        got = require_admin_or_owner_api(_Req(f"{role}_t"), db_session)
        assert got.username == f"{role}_t"


def test_require_admin_or_owner_rejects_operator(db_session):
    u = User(username="op_t", password_hash="x", role="operator", is_active=True, business_id=1)
    db_session.add(u)
    db_session.commit()
    import pytest
    with pytest.raises(HTTPException) as exc:
        require_admin_or_owner_api(_Req("op_t"), db_session)
    assert exc.value.status_code == 403


def _add_user(db: Session, username: str, role: str, business_id: int) -> User:
    u = User(username=username, password_hash="x", role=role, is_active=True,
             business_id=business_id)
    db.add(u)
    db.commit()
    db.refresh(u)
    return u


def _request_with_session(business_id: int):
    req = MagicMock()
    req.session = {"username": "u", "active_business_id": business_id}
    return req


def test_get_active_business_id_uses_passed_user_without_users_query(db_session):
    u = _add_user(db_session, "owner1", "owner", 1)
    req = _request_with_session(3)
    # owner sin negocios asignados pero con user.business_id=1: con user= devuelve 1
    assert get_active_business_id(db_session, req, user=u) == 1


def test_get_active_business_id_without_user_resolves_session(db_session):
    _add_user(db_session, "op1", "operator", 1)
    req = MagicMock()
    req.session = {"username": "op1"}
    assert get_active_business_id(db_session, req) == 1


def test_get_active_business_id_user_none_returns_none(db_session):
    req = _request_with_session(7)
    assert get_active_business_id(db_session, req, user=None) is None


def test_get_active_business_id_user_sentinel_is_distinct_from_none():
    assert _UNSET is not None
