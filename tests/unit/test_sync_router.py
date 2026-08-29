from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.deps import session_dep
from app.main import app
from app.models import Business, DeviceToken, Location, User, UserBusiness
from app.security import require_admin_or_owner_api


@pytest.fixture
def client_with_db(db_session):
    """TestClient con session_dep sobreescrito a la fixture db_session.

    El enfoque de seed vía get_session() + TestClient() no sirve con
    sqlite+pysqlite:///:memory:: ese DSN crea una BD NUEVA por conexión, así que
    lo que siembra el seed no lo ve el request. La fixture db_session usa StaticPool
    (misma conexión), de modo que sobreescribir session_dep es determinista.
    """
    def _override():
        yield db_session
    app.dependency_overrides[session_dep] = _override
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_push_without_token_returns_401(client_with_db):
    r = client_with_db.post("/sync/push", json={"app_schema_version": 6, "events": []})
    assert r.status_code == 401


def test_push_old_version_returns_409(client_with_db, db_session, business_id):
    db_session.add(DeviceToken(token="tok-abc", business_id=business_id, label="POS"))
    db_session.commit()
    r = client_with_db.post(
        "/sync/push",
        json={"app_schema_version": 1, "events": []},
        headers={"X-Sync-Token": "tok-abc"},
    )
    assert r.status_code == 409


def test_register_device_owner_rejected_for_other_business(client_with_db, db_session, business_id):
    # H-001: un owner sin acceso al negocio destino no puede aprovisionar un device ahí.
    other_biz = Business(code="other", name="Other Business")
    db_session.add(other_biz)
    db_session.flush()
    owner = User(username="owner1", password_hash="x", role="owner", is_active=True,
                 business_id=business_id)
    db_session.add(owner)
    db_session.flush()
    db_session.add(UserBusiness(user_id=owner.id, business_id=business_id))
    db_session.commit()

    app.dependency_overrides[require_admin_or_owner_api] = lambda: owner
    try:
        r = client_with_db.post(
            "/sync/register",
            json={"business_id": int(other_biz.id), "location_code": "CENTRAL", "label": "POS"},
        )
        assert r.status_code == 403
    finally:
        del app.dependency_overrides[require_admin_or_owner_api]


def test_register_device_owner_allowed_for_own_business(client_with_db, db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Central", role="central")
    db_session.add(central)
    db_session.flush()
    owner = User(username="owner2", password_hash="x", role="owner", is_active=True,
                 business_id=business_id)
    db_session.add(owner)
    db_session.flush()
    db_session.add(UserBusiness(user_id=owner.id, business_id=business_id))
    db_session.commit()

    app.dependency_overrides[require_admin_or_owner_api] = lambda: owner
    try:
        r = client_with_db.post(
            "/sync/register",
            json={"business_id": business_id, "location_code": "CENTRAL", "label": "POS"},
        )
        assert r.status_code == 200
        assert "token" in r.json()
    finally:
        del app.dependency_overrides[require_admin_or_owner_api]


def test_register_device_unknown_location_rejected(client_with_db, db_session, business_id):
    admin = User(username="admin1", password_hash="x", role="admin", is_active=True)
    db_session.add(admin)
    db_session.commit()

    app.dependency_overrides[require_admin_or_owner_api] = lambda: admin
    try:
        r = client_with_db.post(
            "/sync/register",
            json={"business_id": business_id, "location_code": "DOES-NOT-EXIST", "label": "POS"},
        )
        assert r.status_code == 409
    finally:
        del app.dependency_overrides[require_admin_or_owner_api]
