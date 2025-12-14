from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import delete, select
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

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _warning_if_restock_needed(self, product: Product, stock_after: float) -> Optional[str]:
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

    def _rebuild_product_fifo(self, product: Product, lot_code_overrides: Optional[dict[int, str]] = None) -> None:
        overrides = lot_code_overrides or {}

        existing_codes = {
            lot.movement_id: lot.lot_code
            for lot in self._db.scalars(
                select(InventoryLot).where(InventoryLot.product_id == product.id)
            )
        }
        existing_codes.update(overrides)

        self._db.execute(
            delete(MovementAllocation).where(
                MovementAllocation.movement_id.in_(
                    select(InventoryMovement.id).where(InventoryMovement.product_id == product.id)
                )
            )
        )
        self._db.execute(delete(InventoryLot).where(InventoryLot.product_id == product.id))
        self._db.flush()

        movements = list(
            self._db.scalars(
                select(InventoryMovement)
                .where(InventoryMovement.product_id == product.id)
                .order_by(InventoryMovement.movement_date, InventoryMovement.id)
            )
        )

        fifo_lots: list[InventoryLot] = []
        for mv in movements:
            qty = float(mv.quantity)
            if mv.type in ("purchase", "adjustment") and qty > 0:
                code = existing_codes.get(mv.id)
                if not code:
                    prefix = "ADJ" if mv.type == "adjustment" else product.sku
                    code = f"{prefix}-{mv.movement_date:%Y%m%d%H%M%S%f}"

                lot = InventoryLot(
                    movement_id=mv.id,
                    product_id=product.id,
                    lot_code=code,
                    received_at=mv.movement_date,
                    unit_cost=float(mv.unit_cost or 0),
                    qty_received=qty,
                    qty_remaining=qty,
                )
                self._db.add(lot)
                self._db.flush()
                fifo_lots.append(lot)
                continue

            consume_qty = 0.0
            if mv.type == "sale":
                consume_qty = abs(qty)
            elif mv.type == "adjustment" and qty < 0:
                consume_qty = abs(qty)

            if consume_qty <= 0:
                continue

            remaining = consume_qty
            for lot in fifo_lots:
                if remaining <= 0:
                    break
                take = min(float(lot.qty_remaining), remaining)
                if take <= 0:
                    continue
                lot.qty_remaining = float(lot.qty_remaining) - take
                alloc = MovementAllocation(
                    movement_id=mv.id,
                    lot_id=lot.id,
                    quantity=take,
                    unit_cost=float(lot.unit_cost),
                )
                self._db.add(alloc)
                remaining -= take

            if remaining > 0:
                raise HTTPException(status_code=409, detail="Insufficient stock")

    def reset_purchases_and_sales(self) -> None:
        movement_ids = select(InventoryMovement.id).where(
            InventoryMovement.type.in_(("purchase", "sale"))
        )
        purchase_ids = select(InventoryMovement.id).where(InventoryMovement.type == "purchase")
        purchase_lot_ids = select(InventoryLot.id).where(InventoryLot.movement_id.in_(purchase_ids))

        self._db.execute(
            delete(MovementAllocation).where(
                (MovementAllocation.movement_id.in_(movement_ids))
                | (MovementAllocation.lot_id.in_(purchase_lot_ids))
            )
        )
        self._db.execute(delete(InventoryLot).where(InventoryLot.id.in_(purchase_lot_ids)))
        self._db.execute(delete(InventoryMovement).where(InventoryMovement.id.in_(movement_ids)))
        self._db.commit()

    def update_purchase(
        self,
        movement_id: int,
        sku: str,
        quantity: float,
        unit_cost: float,
        movement_date: Optional[datetime],
        lot_code: Optional[str],
        note: Optional[str],
    ) -> MovementResult:
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "purchase":
            raise HTTPException(status_code=404, detail="Purchase movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if unit_cost < 0:
            raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

        old_product_id = mv.product_id
        product = self._get_product(sku)
        mv.product_id = product.id
        mv.quantity = quantity
        mv.unit_cost = unit_cost
        mv.movement_date = self._movement_datetime(movement_date)
        mv.note = note

        self._db.flush()

        overrides: dict[int, str] = {}
        if lot_code:
            overrides[mv.id] = lot_code

        affected_ids = {old_product_id, product.id}
        try:
            for pid in affected_ids:
                prod = self._db.get(Product, pid)
                if prod is None:
                    continue
                self._rebuild_product_fifo(prod, lot_code_overrides=overrides if pid == product.id else None)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv)
        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv),
            stock_after=stock_after,
            warning=warning,
        )

    def update_sale(
        self,
        movement_id: int,
        sku: str,
        quantity: float,
        unit_price: float,
        movement_date: Optional[datetime],
        note: Optional[str],
    ) -> MovementResult:
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "sale":
            raise HTTPException(status_code=404, detail="Sale movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")

        old_product_id = mv.product_id
        product = self._get_product(sku)
        mv.product_id = product.id
        mv.quantity = -quantity
        mv.unit_price = unit_price
        mv.movement_date = self._movement_datetime(movement_date)
        mv.note = note

        self._db.flush()

        affected_ids = {old_product_id, product.id}
        try:
            for pid in affected_ids:
                prod = self._db.get(Product, pid)
                if prod is None:
                    continue
                self._rebuild_product_fifo(prod)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv)
        stock_after = self._inventory.stock_for_product_id(product.id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv),
            stock_after=stock_after,
            warning=warning,
        )

    def purchase(self, payload: PurchaseCreate) -> MovementResult:
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        product = self._get_product(payload.sku)
        unit_cost = payload.unit_cost
        if unit_cost is None:
            unit_cost = product.default_purchase_cost
        if unit_cost is None:
            raise HTTPException(status_code=422, detail="unit_cost is required")
        if unit_cost < 0:
            raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

        movement_dt = self._movement_datetime(payload.movement_date)
        lot_code = payload.lot_code or f"{product.sku}-{movement_dt:%Y%m%d%H%M%S%f}"

        movement = InventoryMovement(
            product_id=product.id,
            type="purchase",
            quantity=payload.quantity,
            unit_cost=unit_cost,
            unit_price=None,
            movement_date=movement_dt,
            note=payload.note,
        )
        self._inventory.add_movement(movement)

        self._db.flush()
        lot = InventoryLot(
            movement_id=movement.id,
            product_id=product.id,
            lot_code=lot_code,
            received_at=movement_dt,
            unit_cost=unit_cost,
            qty_received=payload.quantity,
            qty_remaining=payload.quantity,
        )
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

        product = self._get_product(payload.sku)
        unit_price = payload.unit_price
        if unit_price is None:
            unit_price = product.default_sale_price
        if unit_price is None:
            raise HTTPException(status_code=422, detail="unit_price is required")
        if unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")

        movement_dt = self._movement_datetime(payload.movement_date)

        stock_before = self._inventory.stock_for_product_id(product.id)
        if stock_before < payload.quantity:
            raise HTTPException(status_code=409, detail="Insufficient stock")

        movement = InventoryMovement(
            product_id=product.id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=unit_price,
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
            self._inventory.add_movement(movement)

            self._db.flush()
            lot = InventoryLot(
                movement_id=movement.id,
                product_id=product.id,
                lot_code=lot_code,
                received_at=movement_dt,
                unit_cost=payload.unit_cost,
                qty_received=payload.quantity_delta,
                qty_remaining=payload.quantity_delta,
            )
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
            name=product.name,
            unit_of_measure=product.unit_of_measure,
            quantity=qty,
            min_stock=min_stock,
            needs_restock=min_stock > 0 and qty < min_stock,
        )

    def stock_list(self, query: str = "") -> list[StockRead]:
        return [
            StockRead(
                sku=sku,
                name=name,
                unit_of_measure=uom or None,
                quantity=qty,
                min_stock=min_stock,
                needs_restock=min_stock > 0 and qty < min_stock,
            )
            for sku, name, uom, qty, min_stock in self._inventory.stock_list(query=query)
        ]

    def recent_purchases(self, query: str = "", limit: int = 20) -> list[tuple]:
        return self._inventory.recent_purchases(query=query, limit=limit)

    def recent_sales(self, query: str = "", limit: int = 20) -> list[tuple]:
        return self._inventory.recent_sales(query=query, limit=limit)
