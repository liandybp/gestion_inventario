from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import InventoryMovement, Product
from app.schemas import ProductCreate, ProductUpdate
from app.services.product_service import ProductService


def _payload(**overrides):
    data = {
        "sku": None,
        "name": "Zapato",
        "category": "Calzado",
        "min_stock": 1,
        "unit_of_measure": "par",
        "default_purchase_cost": 10.0,
        "default_sale_price": 15.0,
        "lead_time_days": 5,
        "image_url": None,
    }
    data.update(overrides)
    return ProductCreate(**data)


def test_create_product_auto_sku_increments(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)

    p1 = service.create(_payload(name="A"))
    p2 = service.create(_payload(name="B"))

    assert p1.sku == "SKU000001"
    assert p2.sku == "SKU000002"


def test_create_product_validates_name(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)

    with pytest.raises(HTTPException) as exc:
        service.create(_payload(name="   "))

    assert exc.value.status_code == 422


def test_update_product_changes_values(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    created = service.create(_payload(sku="SKU-X"))

    updated = service.update(
        "SKU-X",
        ProductUpdate(
            sku="SKU-Y",
            name="Zapato premium",
            category="Calzado",
            min_stock=2,
            unit_of_measure="par",
            default_purchase_cost=11.5,
            default_sale_price=17.0,
            lead_time_days=7,
            image_url="http://img",
        ),
    )

    assert updated.id == created.id
    assert updated.sku == "SKU-Y"
    assert updated.default_sale_price == 17.0


def test_delete_product_without_movements_succeeds(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    created = service.create(_payload(sku="SKU-DEL"))

    service.delete("SKU-DEL")

    found = db_session.query(Product).filter(Product.id == created.id).first()
    assert found is None


def test_delete_product_with_movements_raises_conflict(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    created = service.create(_payload(sku="SKU-MV"))

    mv = InventoryMovement(
        business_id=business_id,
        product_id=created.id,
        location_id=None,
        type="purchase",
        quantity=1,
        unit_cost=10,
        unit_price=None,
        movement_date=datetime.now(timezone.utc),
        note="test",
    )
    db_session.add(mv)
    db_session.commit()

    with pytest.raises(HTTPException) as exc:
        service.delete("SKU-MV")

    assert exc.value.status_code == 409

