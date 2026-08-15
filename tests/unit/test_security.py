from fastapi import HTTPException
from app.models import User
from app.security import require_admin_or_owner_api


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
