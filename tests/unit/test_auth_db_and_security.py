from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.auth import authenticate, get_user_by_username, hash_password
from app.models import Business, User
from app.security import (
    can_access_full_dashboard,
    can_change_business,
    can_manage_users,
    can_view_activity,
    get_active_business_code,
    get_active_business_id,
    get_current_user_from_session,
    is_admin,
    is_operator,
    is_owner,
    require_active_business_id,
    require_admin_api,
    require_user_api,
)


def _mk_user(db: Session, *, username: str, role: str, business_id: int | None, active: bool = True) -> User:
    user = User(
        username=username,
        password_hash=hash_password("secret"),
        role=role,
        business_id=business_id,
        is_active=active,
        must_change_password=False,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def _req(session: dict | None = None):
    return SimpleNamespace(session=session if session is not None else {})


def test_get_user_by_username_and_authenticate(db_session: Session, business_id: int) -> None:
    _mk_user(db_session, username="admin", role="admin", business_id=business_id)

    assert get_user_by_username(db_session, "admin") is not None
    assert authenticate(db_session, "admin", "secret") is not None
    assert authenticate(db_session, "admin", "bad") is None
    assert get_user_by_username(db_session, "") is None


def test_get_current_user_from_session_clears_inactive(db_session: Session, business_id: int) -> None:
    _mk_user(db_session, username="off", role="operator", business_id=business_id, active=False)
    request = _req({"username": "off", "active_business_id": 999})

    user = get_current_user_from_session(db_session, request)

    assert user is None
    assert request.session == {}


def test_get_active_business_id_for_owner_uses_user_business(db_session: Session, business_id: int) -> None:
    user = _mk_user(db_session, username="owner", role="owner", business_id=business_id)
    request = _req({"username": user.username, "active_business_id": 999})

    bid = get_active_business_id(db_session, request)

    assert bid == business_id
    assert "active_business_id" not in request.session


def test_get_active_business_id_for_admin_session_and_fallback(db_session: Session, business_id: int) -> None:
    user = _mk_user(db_session, username="admin2", role="admin", business_id=business_id)

    request_session = _req({"username": user.username, "active_business_id": business_id})
    assert get_active_business_id(db_session, request_session) == business_id

    request_user_fallback = _req({"username": user.username})
    assert get_active_business_id(db_session, request_user_fallback) == business_id


def test_get_active_business_id_admin_uses_first_business_when_user_has_none(db_session: Session) -> None:
    other = Business(code="b2", name="B2")
    db_session.add(other)
    db_session.commit()
    db_session.refresh(other)

    user = _mk_user(db_session, username="admin3", role="admin", business_id=None)
    request = _req({"username": user.username})

    bid = get_active_business_id(db_session, request)

    assert bid is not None
    assert request.session.get("active_business_id") == bid


def test_require_helpers_and_role_booleans(db_session: Session, business_id: int) -> None:
    admin = _mk_user(db_session, username="admin4", role="admin", business_id=business_id)
    owner = _mk_user(db_session, username="owner4", role="owner", business_id=business_id)
    operator = _mk_user(db_session, username="op4", role="operator", business_id=business_id)

    req_admin = _req({"username": admin.username})
    req_owner = _req({"username": owner.username})
    req_none = _req({})

    assert require_user_api(req_admin, db_session).username == admin.username
    assert require_admin_api(req_admin, db_session).username == admin.username

    with pytest.raises(HTTPException):
        require_admin_api(req_owner, db_session)
    with pytest.raises(HTTPException):
        require_user_api(req_none, db_session)

    assert is_admin(admin) and not is_admin(owner)
    assert is_owner(owner) and not is_owner(admin)
    assert is_operator(operator) and not is_operator(owner)
    assert can_manage_users(admin) and not can_manage_users(owner)
    assert can_change_business(admin) and not can_change_business(owner)
    assert can_view_activity(admin) and not can_view_activity(owner)
    assert can_access_full_dashboard(admin)
    assert can_access_full_dashboard(owner)
    assert not can_access_full_dashboard(operator)


def test_require_active_business_id_and_code(db_session: Session, business_id: int) -> None:
    user = _mk_user(db_session, username="codeuser", role="operator", business_id=business_id)
    req = _req({"username": user.username})

    assert require_active_business_id(db_session, req) == business_id
    code = get_active_business_code(db_session, req)
    assert code == "test"

    with pytest.raises(HTTPException):
        require_active_business_id(db_session, _req({}))
