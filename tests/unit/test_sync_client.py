from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import hash_password, verify_password
from app.db import Base
from app.models import (
    AuditLog, Business, InventoryLot, InventoryMovement, Location, Product,
    SyncEvent, SyncState, User,
)
from app.sync.client import _pending_events, apply_pull, sync
from app.sync.outbox import append_event, verify_chain


def test_pending_events_serialized_in_seq_order(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"sku": "A", "quantity": 1})
    append_event(db_session, business_id=business_id, event_type="customer", payload={"client_id": "C1", "name": "Ana"})
    db_session.commit()
    evs = _pending_events(db_session)
    assert [e["seq"] for e in evs] == [1, 2]
    assert evs[0]["payload"]["sku"] == "A"


def test_sync_without_provisioning_returns_error(db_session):
    res = sync(db_session)
    assert res["ok"] is False


def test_apply_pull_replaces_read_tables(db_session, business_id):
    db_session.add(Product(business_id=business_id, sku="OLD", name="Old", min_stock=0,
                           default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0))
    db_session.commit()
    snapshot = {"products": [
        {"id": 999, "business_id": business_id, "sku": "NEW", "name": "New", "min_stock": 0,
         "default_purchase_cost": 1.0, "default_sale_price": 2.0, "lead_time_days": 0}
    ]}
    apply_pull(db_session, snapshot)
    db_session.commit()
    skus = {p.sku for p in db_session.query(Product).all()}
    assert skus == {"NEW"}


def test_apply_pull_does_not_touch_outbox(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"sku": "A", "quantity": 1})
    db_session.commit()
    apply_pull(db_session, {"products": []})
    db_session.commit()
    assert db_session.query(SyncEvent).count() == 1


@pytest.fixture
def fk_db_session() -> Session:
    """In-memory SQLite con FK enforcement (mirrors prod/Postgres behavior).

    Necesario para H-001: bajo la fixture db_session normal (sin PRAGMA
    foreign_keys=ON) el borrado en orden incorrecto no falla, ocultando el bug.
    """
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


def test_apply_pull_does_not_violate_fk_order_on_second_sync(fk_db_session):
    """H-001: apply_pull debe borrar/reinsertar en un orden que respete las FKs
    (SQLite con PRAGMA foreign_keys=ON, igual que el proyecto exige en producción).
    Antes del fix, `db.query(Business).delete()` corría antes que sus hijas y
    lanzaba IntegrityError en cualquier sync posterior al primero."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()
    loc = Location(business_id=biz.id, code="CENTRAL", name="Central")
    fk_db_session.add(loc)
    fk_db_session.commit()

    snapshot = {
        "businesses": [{"id": biz.id, "code": "t", "name": "Test"}],
        "locations": [{"id": loc.id, "business_id": biz.id, "code": "CENTRAL", "name": "Central"}],
    }
    apply_pull(fk_db_session, snapshot)  # primer sync: tablas ya vacías salvo lo creado arriba
    fk_db_session.commit()
    apply_pull(fk_db_session, snapshot)  # segundo sync: aquí fallaba antes del fix
    fk_db_session.commit()

    assert fk_db_session.query(Business).count() == 1
    assert fk_db_session.query(Location).count() == 1


def test_apply_pull_preserves_local_password_hash_for_existing_user(fk_db_session):
    """H-002: el pull nunca trae password_hash (P2 lo excluye a propósito); un
    reemplazo ciego de la tabla users la insertaba sin ese campo NOT NULL y
    hacía crashear apply_pull en cualquier sync real."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()
    user = User(username="admin", password_hash=hash_password("secret123"),
                business_id=biz.id, role="admin")
    fk_db_session.add(user)
    fk_db_session.commit()

    snapshot = {
        "businesses": [{"id": biz.id, "code": "t", "name": "Test"}],
        "users": [{"id": user.id, "business_id": biz.id, "username": "admin",
                   "role": "admin", "is_active": True, "must_change_password": False}],
    }
    apply_pull(fk_db_session, snapshot)
    fk_db_session.commit()

    reloaded = fk_db_session.query(User).filter_by(username="admin").one()
    assert verify_password("secret123", reloaded.password_hash)


def test_apply_pull_survives_local_audit_log_referencing_user(fk_db_session):
    """H-e2e-1: en un nodo real, cualquier login local escribe un audit_logs.user_id
    ANTES del primer sync. audit_logs no es una tabla sincronizada (no está en
    SYNC_TABLE_ORDER), pero su FK a users.id igual bloqueaba
    `db.query(User).delete()` bajo PRAGMA foreign_keys=ON — encontrado por el
    e2e real de sync (tests/e2e/test_sync_e2e.py), no por los tests unitarios
    anteriores porque ninguno creaba un AuditLog junto al User."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()
    user = User(username="admin", password_hash=hash_password("secret123"),
                business_id=biz.id, role="admin")
    fk_db_session.add(user)
    fk_db_session.commit()
    fk_db_session.add(AuditLog(user_id=user.id, username="admin", action="login",
                                entity_type="user", entity_id=str(user.id), detail="{}"))
    fk_db_session.commit()

    snapshot = {
        "businesses": [{"id": biz.id, "code": "t", "name": "Test"}],
        "users": [{"id": user.id, "business_id": biz.id, "username": "admin",
                   "role": "admin", "is_active": True, "must_change_password": False}],
    }
    apply_pull(fk_db_session, snapshot)  # antes del fix: IntegrityError acá
    fk_db_session.commit()

    reloaded = fk_db_session.query(User).filter_by(username="admin").one()
    assert verify_password("secret123", reloaded.password_hash)
    log = fk_db_session.query(AuditLog).one()
    assert log.user_id is None  # detach, no se pierde la fila de auditoría


def test_apply_pull_parses_json_datetime_strings(fk_db_session):
    """H-e2e-2: el snapshot real cruza HTTP (`httpx.post(json=...)` /
    `resp.json()`), así que sus valores datetime llegan como string ISO, no
    como objetos `datetime` de Python — `model(**data)` fallaba con
    `TypeError: SQLite DateTime type only accepts Python datetime and date
    objects`. Los tests anteriores de apply_pull nunca lo detectaron porque
    siempre pasaban dicts con datetimes nativos, no el snapshot ya
    serializado/deserializado que realmente viaja por la red (encontrado por
    tests/e2e/test_sync_e2e.py)."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()

    snapshot = {
        "businesses": [{
            "id": biz.id, "code": "t", "name": "Test",
            "created_at": "2026-08-29T14:52:36",
        }],
    }
    apply_pull(fk_db_session, snapshot)  # antes del fix: TypeError acá
    fk_db_session.commit()

    reloaded = fk_db_session.query(Business).one()
    assert isinstance(reloaded.created_at, datetime)


def test_apply_pull_inserts_fk_dependent_rows_in_same_pull(fk_db_session):
    """H-e2e-3: sin `relationship()` ORM declaradas entre InventoryMovement e
    InventoryLot, un único `db.flush()` al final de todo el loop no reordena
    los INSERT por dependencia FK entre tablas distintas — un lot referenciando
    un movement recién traído en el MISMO pull fallaba con IntegrityError pese
    a que _PULL_ORDER ya tenía el orden de tablas correcto (movements antes que
    lots). El fix flushea una vez por tabla, no una sola vez al final."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()
    loc = Location(business_id=biz.id, code="CENTRAL", name="Central")
    prod = Product(business_id=biz.id, sku="X", name="X", min_stock=0,
                    unit_of_measure="ud", default_purchase_cost=1.0,
                    default_sale_price=2.0, lead_time_days=0)
    fk_db_session.add_all([loc, prod])
    fk_db_session.commit()

    snapshot = {
        "businesses": [{"id": biz.id, "code": "t", "name": "Test"}],
        "locations": [{"id": loc.id, "business_id": biz.id, "code": "CENTRAL", "name": "Central"}],
        "products": [{"id": prod.id, "business_id": biz.id, "sku": "X", "name": "X",
                      "min_stock": 0, "unit_of_measure": "ud", "default_purchase_cost": 1.0,
                      "default_sale_price": 2.0, "lead_time_days": 0}],
        "inventory_movements": [{
            "id": 1, "business_id": biz.id, "product_id": prod.id, "location_id": loc.id,
            "type": "purchase", "quantity": 10.0, "unit_cost": 1.0, "unit_price": None,
            "movement_date": "2026-08-29T14:52:45.719646", "note": None,
        }],
        "inventory_lots": [{
            "id": 1, "business_id": biz.id, "movement_id": 1, "product_id": prod.id,
            "location_id": loc.id, "lot_code": "L1",
            "received_at": "2026-08-29T14:52:45.719646", "unit_cost": 1.0,
            "qty_received": 10.0, "qty_remaining": 10.0,
        }],
    }
    apply_pull(fk_db_session, snapshot)  # antes del fix: IntegrityError acá
    fk_db_session.commit()

    assert fk_db_session.query(InventoryMovement).count() == 1
    assert fk_db_session.query(InventoryLot).one().movement_id == 1


def test_apply_pull_assigns_usable_hash_to_brand_new_user(fk_db_session):
    """Un usuario nunca visto localmente (llega por primera vez en el pull) no
    debe romper el NOT NULL de password_hash; se le asigna un hash aleatorio
    (documentado: no podrá loguear en este nodo hasta que se le asigne uno)."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()

    snapshot = {
        "businesses": [{"id": biz.id, "code": "t", "name": "Test"}],
        "users": [{"id": 999, "business_id": biz.id, "username": "brand_new",
                   "role": "operator", "is_active": True, "must_change_password": True}],
    }
    apply_pull(fk_db_session, snapshot)
    fk_db_session.commit()

    reloaded = fk_db_session.query(User).filter_by(username="brand_new").one()
    assert reloaded.password_hash  # NOT NULL satisfecho, no crashea


def test_sync_success_path_marks_synced_and_keeps_hash_chain_valid(fk_db_session, monkeypatch):
    """H-004: tras un sync exitoso, verify_chain debe seguir reportando la
    cadena como íntegra (el bug marcaba `synced` sin recalcular event_hash)."""
    biz = Business(code="t", name="Test")
    fk_db_session.add(biz)
    fk_db_session.commit()
    ev = append_event(fk_db_session, business_id=biz.id, event_type="sale", payload={"sku": "A"})
    fk_db_session.commit()

    state = SyncState(id=1, server_url="http://fake", device_token="tok")
    fk_db_session.add(state)
    fk_db_session.commit()

    class _FakeResp:
        status_code = 200

        def json(self):
            return {"applied_event_ids": [ev.event_id], "conflicts": [], "pull": {"snapshot": {}}}

    import app.sync.client as client_mod
    monkeypatch.setattr(client_mod.httpx, "post", lambda *a, **k: _FakeResp())

    result = sync(fk_db_session)

    assert result["ok"] is True
    assert result["applied"] == 1
    assert verify_chain(fk_db_session, biz.id) is True
