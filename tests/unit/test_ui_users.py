from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base
from app.models import AuditLog, Business, User
from app.routers.ui_users import user_delete


@pytest.fixture
def fk_db_session() -> Session:
    """In-memory SQLite with FK enforcement (mirrors prod/Postgres behavior)."""
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _fk_on(dbapi_con, _record) -> None:
        dbapi_con.execute("PRAGMA foreign_keys=ON")

    TestingSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


def _admin_request() -> MagicMock:
    req = MagicMock()
    req.session = {"username": "admin", "active_business_id": 1}
    req.url.path = "/ui/tabs/users"
    return req


def test_user_delete_clears_audit_fk(fk_db_session: Session) -> None:
    """Deleting a user with audit logs must not fail on the FK constraint."""
    from app.auth import hash_password

    biz = Business(code="t", name="T")
    fk_db_session.add(biz)
    fk_db_session.commit()

    admin = User(
        username="admin",
        password_hash=hash_password("x"),
        role="admin",
        is_active=True,
        business_id=biz.id,
    )
    target = User(
        username="pepito",
        password_hash=hash_password("x"),
        role="operator",
        is_active=True,
        business_id=biz.id,
    )
    fk_db_session.add_all([admin, target])
    fk_db_session.commit()
    fk_db_session.add(
        AuditLog(
            user_id=target.id,
            username=target.username,
            action="login",
            entity_type="auth",
            entity_id=target.username,
        )
    )
    fk_db_session.commit()

    result = user_delete(
        request=_admin_request(),
        user_id=target.id,
        db=fk_db_session,
    )

    assert result.status_code == 200
    # user is gone
    assert fk_db_session.get(User, target.id) is None
    # audit log preserved, FK nulled out
    audit = fk_db_session.scalar(
        select(AuditLog).where(AuditLog.username == "pepito")
    )
    assert audit is not None
    assert audit.user_id is None
