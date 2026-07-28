from __future__ import annotations

from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import AuditLog, User


def test_log_event_persists_audit_row(db_session: Session, business_id: int) -> None:
    user = User(
        username="auditor",
        password_hash="x",
        role="admin",
        business_id=business_id,
        is_active=True,
        must_change_password=False,
    )
    db_session.add(user)
    db_session.commit()
    db_session.refresh(user)

    log_event(
        db_session,
        user,
        action="product_create",
        entity_type="product",
        entity_id="SKU1",
        detail={"name": "Producto"},
    )

    row = db_session.query(AuditLog).filter(AuditLog.entity_id == "SKU1").one()
    assert row.username == "auditor"
    assert row.action == "product_create"


def test_session_dep_yields_and_closes(monkeypatch):
    class DummySession:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    dummy = DummySession()
    monkeypatch.setattr("app.deps.get_session", lambda: dummy)

    gen = session_dep()
    yielded = next(gen)
    assert yielded is dummy
    try:
        next(gen)
        assert False, "generator should stop"
    except StopIteration:
        pass

    assert dummy.closed is True

