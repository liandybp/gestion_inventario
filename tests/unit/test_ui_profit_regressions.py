"""Regresiones de la fase 5b en caminos sin cobertura previa (filtro por ubicación y borrado en Utilidades)."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import hash_password
from app.db import Base
from app.deps import session_dep
from app.main import app
from app.models import Business, Location, User
from app.services.inventory_service import InventoryService


@pytest.fixture
def admin_client():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    biz = Business(code="test", name="Test Business")
    db.add(biz)
    db.flush()
    db.add(Location(business_id=biz.id, code="TEST_CENTRAL", name="Central", role="central", position=0))
    db.add(User(username="admin", password_hash=hash_password("admin"), role="admin",
                is_active=True, business_id=biz.id))
    db.commit()
    db.close()

    def _override():
        s = Session()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[session_dep] = _override
    client = TestClient(app)
    resp = client.post("/ui/login", data={"username": "admin", "password": "admin"}, follow_redirects=False)
    assert resp.status_code in (302, 303)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()
        engine.dispose()


def test_profit_tab_with_selected_location_renders(admin_client) -> None:
    resp = admin_client.get("/ui/tabs/profit", params={"location_code": "TEST_CENTRAL"})
    assert resp.status_code == 200


def test_profit_items_sale_delete_renders_tab(admin_client, monkeypatch) -> None:
    monkeypatch.setattr(InventoryService, "delete_sale_movement", lambda self, movement_id: None)
    resp = admin_client.post(
        "/ui/tabs/profit-items/sale/1/delete",
        data={"query": "", "start_date": "", "end_date": "", "location_code": ""},
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
