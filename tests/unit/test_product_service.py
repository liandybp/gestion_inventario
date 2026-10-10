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

    assert p1.sku == "SKU010001"
    assert p2.sku == "SKU010002"


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


# --- create_for_purchase (creación de producto desde "Registrar compra") ---


def test_create_for_purchase_auto_sku_and_default_sale_price(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)

    p = service.create_for_purchase(name="Tornillo", sku=None, purchase_cost=1.5, sale_price=None)

    assert p.sku == "SKU010001"
    assert p.name == "Tornillo"
    assert p.default_purchase_cost == 1.5
    assert p.default_sale_price == 0


def test_create_for_purchase_uses_sale_price_and_explicit_sku(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)

    p = service.create_for_purchase(name="", sku="ABC-1", purchase_cost=2.0, sale_price=7.5)

    assert p.sku == "ABC-1"
    assert p.name == "ABC-1"  # sin nombre, se usa el SKU (comportamiento previo)
    assert p.default_sale_price == 7.5


@pytest.mark.parametrize(
    ("kwargs", "detail_part"),
    [
        ({"name": "", "sku": None, "purchase_cost": 1.0, "sale_price": None}, "Nombre (si es nuevo)"),
        ({"name": "X", "sku": None, "purchase_cost": None, "sale_price": None}, "costo unitario"),
        ({"name": "X", "sku": None, "purchase_cost": float("nan"), "sale_price": None}, "costo unitario"),
        ({"name": "X", "sku": None, "purchase_cost": -1.0, "sale_price": None}, "costo unitario"),
        ({"name": "X", "sku": None, "purchase_cost": 1.0, "sale_price": -1.0}, "precio de venta"),
        ({"name": "X", "sku": None, "purchase_cost": 1.0, "sale_price": float("nan")}, "precio de venta"),
        ({"name": "X", "sku": None, "purchase_cost": 1.0, "sale_price": float("inf")}, "precio de venta"),
    ],
)
def test_create_for_purchase_validates_input(db_session: Session, business_id: int, kwargs, detail_part) -> None:
    service = ProductService(db_session, business_id=business_id)

    with pytest.raises(HTTPException) as exc:
        service.create_for_purchase(**kwargs)

    assert exc.value.status_code == 422
    assert detail_part in str(exc.value.detail)
    assert db_session.query(Product).filter(Product.name == "X").count() == 0


def test_create_for_purchase_flushes_without_commit(db_session: Session, business_id: int) -> None:
    """Producto y compra se confirman juntos: un rollback del llamador no deja huérfano."""
    service = ProductService(db_session, business_id=business_id)

    p = service.create_for_purchase(name="Huérfano", sku=None, purchase_cost=1.0, sale_price=None)
    assert p.id is not None  # flush asignó id

    db_session.rollback()

    assert db_session.query(Product).filter(Product.name == "Huérfano").count() == 0


def test_create_for_purchase_existing_sku_returns_409(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    service.create(_payload(sku="DUP-1", name="Original"))

    with pytest.raises(HTTPException) as exc:
        service.create_for_purchase(name="Otro", sku="DUP-1", purchase_cost=1.0, sale_price=None)

    assert exc.value.status_code == 409
