from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import InventoryMovement, Product
from app.repositories.inventory_repository import InventoryRepository
from app.repositories.product_repository import ProductRepository
from app.schemas import (
    AdjustmentCreate,
    MovementRead,
    MovementResult,
    PurchaseCreate,
    SaleCreate,
    StockRead,
)


class InventoryService:
    def __init__(self, db: Session):
        self._db = db
        self._products = ProductRepository(db)
        self._inventory = InventoryRepository(db)

    def _get_product(self, sku: str) -> Product:
        product = self._products.get_by_sku(sku)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

    def purchase(self, payload: PurchaseCreate) -> MovementResult:
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if payload.unit_cost < 0:
            raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

        product = self._get_product(payload.sku)
        movement = InventoryMovement(
            product_id=product.id,
            type="purchase",
            quantity=payload.quantity,
            unit_cost=payload.unit_cost,
            unit_price=None,
            note=payload.note,
        )
        self._inventory.add_movement(movement)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = "Stock negative" if stock_after < 0 else None
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def sale(self, payload: SaleCreate) -> MovementResult:
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if payload.unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")

        product = self._get_product(payload.sku)
        movement = InventoryMovement(
            product_id=product.id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=payload.unit_price,
            note=payload.note,
        )
        self._inventory.add_movement(movement)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = "Stock negative" if stock_after < 0 else None
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def adjustment(self, payload: AdjustmentCreate) -> MovementResult:
        if payload.quantity_delta == 0:
            raise HTTPException(status_code=422, detail="quantity_delta must be != 0")

        product = self._get_product(payload.sku)
        movement = InventoryMovement(
            product_id=product.id,
            type="adjustment",
            quantity=payload.quantity_delta,
            unit_cost=None,
            unit_price=None,
            note=payload.note,
        )
        self._inventory.add_movement(movement)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = "Stock negative" if stock_after < 0 else None
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def stock(self, sku: str) -> StockRead:
        product = self._get_product(sku)
        qty = self._inventory.stock_for_product_id(product.id)
        return StockRead(sku=product.sku, quantity=qty, is_negative=qty < 0)

    def stock_list(self) -> list[StockRead]:
        return [
            StockRead(sku=sku, quantity=qty, is_negative=qty < 0)
            for sku, qty in self._inventory.stock_list()
        ]
