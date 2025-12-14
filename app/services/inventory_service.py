from __future__ import annotations

from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, MovementAllocation, Product
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

    def _movement_datetime(self, provided: datetime | None) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _warning_if_restock_needed(self, product: Product, stock_after: float) -> str | None:
        min_stock = float(product.min_stock or 0)
        if min_stock > 0 and stock_after < min_stock:
            return "Needs restock"
        return None

    def _consume_fifo(self, product_id: int, movement_id: int, quantity: float) -> None:
        if quantity <= 0:
            return

        stock = self._inventory.stock_for_product_id(product_id)
        if stock < quantity:
            raise HTTPException(status_code=409, detail="Insufficient stock")

        remaining = quantity
        lots = self._inventory.fifo_lots_for_product_id(product_id)
        for lot in lots:
            if remaining <= 0:
                break

            take = min(float(lot.qty_remaining), remaining)
            lot.qty_remaining = float(lot.qty_remaining) - take
            allocation = MovementAllocation(
                movement_id=movement_id,
                lot_id=lot.id,
                quantity=take,
                unit_cost=float(lot.unit_cost),
            )
            self._inventory.add_allocation(allocation)
            remaining -= take

        if remaining > 0:
            raise HTTPException(status_code=409, detail="Insufficient stock")

    def purchase(self, payload: PurchaseCreate) -> MovementResult:
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if payload.unit_cost < 0:
            raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

        product = self._get_product(payload.sku)
        movement_dt = self._movement_datetime(payload.movement_date)
        lot_code = payload.lot_code or f"{product.sku}-{movement_dt:%Y%m%d%H%M%S%f}"

        movement = InventoryMovement(
            product_id=product.id,
            type="purchase",
            quantity=payload.quantity,
            unit_cost=payload.unit_cost,
            unit_price=None,
            movement_date=movement_dt,
            note=payload.note,
        )
        lot = InventoryLot(
            product_id=product.id,
            lot_code=lot_code,
            received_at=movement_dt,
            unit_cost=payload.unit_cost,
            qty_received=payload.quantity,
            qty_remaining=payload.quantity,
        )
        self._inventory.add_movement(movement)
        self._inventory.add_lot(lot)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = self._warning_if_restock_needed(product, stock_after)
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
        movement_dt = self._movement_datetime(payload.movement_date)

        stock_before = self._inventory.stock_for_product_id(product.id)
        if stock_before < payload.quantity:
            raise HTTPException(status_code=409, detail="Insufficient stock")

        movement = InventoryMovement(
            product_id=product.id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=payload.unit_price,
            movement_date=movement_dt,
            note=payload.note,
        )
        self._inventory.add_movement(movement)

        self._db.flush()
        self._consume_fifo(product.id, movement.id, payload.quantity)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def adjustment(self, payload: AdjustmentCreate) -> MovementResult:
        if payload.quantity_delta == 0:
            raise HTTPException(status_code=422, detail="quantity_delta must be != 0")

        product = self._get_product(payload.sku)
        movement_dt = self._movement_datetime(payload.movement_date)

        if payload.quantity_delta > 0:
            if payload.unit_cost is None:
                raise HTTPException(
                    status_code=422, detail="unit_cost is required for positive adjustment"
                )
            if payload.unit_cost < 0:
                raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

            lot_code = f"ADJ-{product.sku}-{movement_dt:%Y%m%d%H%M%S%f}"
            movement = InventoryMovement(
                product_id=product.id,
                type="adjustment",
                quantity=payload.quantity_delta,
                unit_cost=payload.unit_cost,
                unit_price=None,
                movement_date=movement_dt,
                note=payload.note,
            )
            lot = InventoryLot(
                product_id=product.id,
                lot_code=lot_code,
                received_at=movement_dt,
                unit_cost=payload.unit_cost,
                qty_received=payload.quantity_delta,
                qty_remaining=payload.quantity_delta,
            )
            self._inventory.add_movement(movement)
            self._inventory.add_lot(lot)
            self._db.commit()
            self._db.refresh(movement)
        else:
            qty_to_remove = -payload.quantity_delta
            stock_before = self._inventory.stock_for_product_id(product.id)
            if stock_before < qty_to_remove:
                raise HTTPException(status_code=409, detail="Insufficient stock")

            movement = InventoryMovement(
                product_id=product.id,
                type="adjustment",
                quantity=payload.quantity_delta,
                unit_cost=None,
                unit_price=None,
                movement_date=movement_dt,
                note=payload.note,
            )
            self._inventory.add_movement(movement)
            self._db.flush()
            self._consume_fifo(product.id, movement.id, qty_to_remove)
            self._db.commit()
            self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def stock(self, sku: str) -> StockRead:
        product = self._get_product(sku)
        qty = self._inventory.stock_for_product_id(product.id)
        min_stock = float(product.min_stock or 0)
        return StockRead(
            sku=product.sku,
            quantity=qty,
            min_stock=min_stock,
            needs_restock=min_stock > 0 and qty < min_stock,
        )

    def stock_list(self) -> list[StockRead]:
        return [
            StockRead(
                sku=sku,
                quantity=qty,
                min_stock=min_stock,
                needs_restock=min_stock > 0 and qty < min_stock,
            )
            for sku, qty, min_stock in self._inventory.stock_list()
        ]
