"""Regresión H-003/H-007/H-009 (auditoría 2026-08-29, docs/auditorias/2026-08-29-p3-sync-client-offline-sync.md).

Cubre el camino que dejó pasar los bugs críticos de P3 sin que ningún test lo
notara: los hooks de outbox en ``ui_sales.py``/``ui_customers.py`` con
``is_desktop()==True``. `design.md` §2.1 prometía este archivo
(``tests/unit/test_offline_hooks.py``) y nunca se creó.
"""
from __future__ import annotations

import json

import pytest

from app import settings as app_settings
from app.models import Location, Product, SyncEvent, User
from app.routers import ui_customers, ui_sales
from app.schemas import PurchaseCreate
from app.services.inventory_service import InventoryService


class _FakeRequest:
    def __init__(self, username: str):
        self.session = {"username": username}


def _operator(db, business_id: int) -> User:
    user = User(username="opdesktop", password_hash="x", role="operator",
                is_active=True, business_id=business_id)
    db.add(user)
    db.commit()
    return user


def _stocked_product(db, business_id: int, sku: str = "SKU-OFFLINE") -> Product:
    db.add(Location(business_id=business_id, code="CENTRAL", name="Almacen central"))
    product = Product(
        business_id=business_id, sku=sku, name="Producto offline", min_stock=0,
        unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
        lead_time_days=0,
    )
    db.add(product)
    db.commit()
    InventoryService(db, business_id=business_id).purchase(
        PurchaseCreate(sku=sku, quantity=10, unit_cost=1.0)
    )
    return product


@pytest.fixture
def desktop_mode(monkeypatch):
    monkeypatch.setattr(app_settings, "APP_MODE", "desktop")
    yield
    monkeypatch.setattr(app_settings, "APP_MODE", "server")


def test_sale_in_desktop_mode_enqueues_outbox_event(db_session, business_id, desktop_mode, monkeypatch):
    _stocked_product(db_session, business_id)
    _operator(db_session, business_id)
    request = _FakeRequest("opdesktop")

    ui_sales.sale(
        request=request, product="SKU-OFFLINE", location_code="CENTRAL", quantity=1,
        unit_price="", movement_date=None, note=None, db=db_session,
    )

    events = db_session.query(SyncEvent).filter(SyncEvent.business_id == business_id).all()
    assert len(events) == 1
    assert events[0].event_type == "sale"
    payload = json.loads(events[0].payload)
    assert payload["sku"] == "SKU-OFFLINE"
    # H-003: movement_date omitido debe serializar a null, no al string "None".
    assert payload["movement_date"] is None


def test_sale_in_server_mode_does_not_enqueue(db_session, business_id, monkeypatch):
    monkeypatch.setattr(app_settings, "APP_MODE", "server")
    _stocked_product(db_session, business_id)
    _operator(db_session, business_id)
    request = _FakeRequest("opdesktop")

    ui_sales.sale(
        request=request, product="SKU-OFFLINE", location_code="CENTRAL", quantity=1,
        unit_price="", movement_date=None, note=None, db=db_session,
    )

    assert db_session.query(SyncEvent).count() == 0


def test_sale_offline_hook_failure_does_not_break_the_response(
    db_session, business_id, desktop_mode, monkeypatch
):
    """H-007: si append_event falla, la venta (ya commiteada) sigue devolviendo
    una respuesta 200 en vez de un 500 crudo que sugiere que la venta falló."""

    def _boom(*_a, **_k):
        raise RuntimeError("outbox down")

    monkeypatch.setattr(ui_sales, "append_event", _boom)
    _stocked_product(db_session, business_id)
    _operator(db_session, business_id)
    request = _FakeRequest("opdesktop")

    response = ui_sales.sale(
        request=request, product="SKU-OFFLINE", location_code="CENTRAL", quantity=1,
        unit_price="", movement_date=None, note=None, db=db_session,
    )
    assert response.status_code == 200
    # La venta al inventario ya había commiteado; no debe quedar ningún evento a medias.
    assert db_session.query(SyncEvent).count() == 0


def test_customer_create_in_desktop_mode_enqueues_outbox_event(db_session, business_id, desktop_mode):
    request = _FakeRequest("opdesktop")
    _operator(db_session, business_id)

    ui_customers.customer_create(
        request=request, client_id="C-OFFLINE", name="Cliente Offline",
        address="", phone="600111222", email="", db=db_session,
    )

    events = db_session.query(SyncEvent).filter(SyncEvent.business_id == business_id).all()
    assert len(events) == 1
    assert events[0].event_type == "customer"
    payload = json.loads(events[0].payload)
    assert payload["client_id"] == "C-OFFLINE"


def test_customer_create_in_server_mode_does_not_enqueue(db_session, business_id, monkeypatch):
    monkeypatch.setattr(app_settings, "APP_MODE", "server")
    request = _FakeRequest("opdesktop")
    _operator(db_session, business_id)

    ui_customers.customer_create(
        request=request, client_id="C-ONLINE", name="Cliente Online",
        address="", phone="600333444", email="", db=db_session,
    )

    assert db_session.query(SyncEvent).count() == 0
