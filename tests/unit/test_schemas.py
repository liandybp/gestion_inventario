from __future__ import annotations

from pydantic import ValidationError

from app.schemas import PurchaseCreate, SaleCreate, SupplierReturnLotCreate


def test_purchase_create_requires_positive_quantity() -> None:
    try:
        PurchaseCreate(sku="A1", quantity=0)
        assert False, "Expected ValidationError"
    except ValidationError as exc:
        assert "quantity must be greater than 0" in str(exc)


def test_sale_create_requires_positive_quantity() -> None:
    try:
        SaleCreate(sku="A1", quantity=-1)
        assert False, "Expected ValidationError"
    except ValidationError as exc:
        assert "quantity must be greater than 0" in str(exc)


def test_supplier_return_requires_positive_quantity() -> None:
    try:
        SupplierReturnLotCreate(lot_id=1, quantity=0)
        assert False, "Expected ValidationError"
    except ValidationError as exc:
        assert "quantity must be greater than 0" in str(exc)
