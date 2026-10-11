"""H-204/H-209: errores al guardar en el modal de edición de producto."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from app.models import Product
from app.routers import ui_products


@pytest.fixture
def _route(monkeypatch, business_id: int):
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(ui_products, "ensure_admin_or_owner", lambda _db, _req: None)
    monkeypatch.setattr(ui_products, "get_current_user_from_session", lambda _db, _req: None)
    saved: list[str] = []
    deleted: list = []
    monkeypatch.setattr(ui_products, "save_product_image", lambda _f: saved.append("/static/uploads/new.png") or saved[-1])
    monkeypatch.setattr(ui_products, "delete_uploaded_image", lambda url: deleted.append(url))
    return saved, deleted


def _add(db: Session, business_id: int, sku: str) -> None:
    db.add(Product(business_id=business_id, sku=sku, name=sku, min_stock=0, lead_time_days=0, default_sale_price=5.0))
    db.commit()


def _update(db: Session, sku: str, **form):
    request = MagicMock()
    request.session = {}
    fields = dict(new_sku="", name="Nombre", unit_of_measure="", image_file=None, category=None, min_stock=0,
                  default_purchase_cost="", default_sale_price="", lead_time_days=None)
    fields.update(form)
    return ui_products.product_update(request=request, sku=sku, db=db, **fields)


def test_typed_nan_sale_price_is_422_and_price_kept(db_session: Session, business_id: int, _route) -> None:
    _add(db_session, business_id, "PU-1")

    resp = _update(db_session, "PU-1", default_sale_price="nan")

    assert resp.status_code == 422
    assert "El precio de venta debe ser un número" in resp.body.decode()
    db_session.expire_all()
    assert db_session.query(Product).filter_by(sku="PU-1").one().default_sale_price == 5.0


def test_failed_update_deletes_new_image(db_session: Session, business_id: int, _route) -> None:
    saved, deleted = _route
    _add(db_session, business_id, "PU-2")
    _add(db_session, business_id, "PU-3")

    resp = _update(db_session, "PU-2", new_sku="PU-3", image_file=MagicMock())

    assert resp.status_code == 409
    assert saved == ["/static/uploads/new.png"]
    assert deleted == ["/static/uploads/new.png"]


def test_failed_update_without_new_image_deletes_nothing(db_session: Session, business_id: int, _route) -> None:
    _, deleted = _route
    _add(db_session, business_id, "PU-4")
    _add(db_session, business_id, "PU-5")

    resp = _update(db_session, "PU-4", new_sku="PU-5")

    assert resp.status_code == 409
    assert deleted == [None]


def _update_inventory(db: Session, sku: str, **form):
    request = MagicMock()
    request.session = {}
    fields = dict(new_sku="", name="Nombre", unit_of_measure="", image_file=MagicMock(), category=None, min_stock=0,
                  default_purchase_cost="", default_sale_price="", lead_time_days=None, desired_stock=None,
                  initial_inventory=False, location_code="")
    fields.update(form)
    return ui_products.product_update_inventory(request=request, sku=sku, db=db, **fields)


def test_edit_inventory_failed_update_deletes_new_image(db_session: Session, business_id: int, _route, monkeypatch) -> None:
    _, deleted = _route
    monkeypatch.setattr(ui_products, "get_active_business_config", lambda _db, _req: MagicMock(locations=MagicMock(pos=[])))
    _add(db_session, business_id, "PI-1")
    _add(db_session, business_id, "PI-2")

    resp = _update_inventory(db_session, "PI-1", new_sku="PI-2")

    assert resp.status_code == 409
    assert deleted == ["/static/uploads/new.png"]


def test_edit_inventory_failure_after_update_keeps_image(db_session: Session, business_id: int, _route, monkeypatch) -> None:
    """El producto (con la imagen) ya se guardó: un fallo posterior del ajuste no la borra."""
    _, deleted = _route
    monkeypatch.setattr(ui_products, "get_active_business_config", lambda _db, _req: MagicMock(locations=MagicMock(pos=[])))
    _add(db_session, business_id, "PI-3")

    resp = _update_inventory(db_session, "PI-3", desired_stock=-1)

    assert resp.status_code == 422
    assert deleted == [None]
    db_session.expire_all()
    assert db_session.query(Product).filter_by(sku="PI-3").one().image_url == "/static/uploads/new.png"
