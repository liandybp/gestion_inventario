from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, MovementAllocation, Product


class InventoryRepository:
    def __init__(self, db: Session):
        self._db = db

    def add_movement(self, movement: InventoryMovement) -> None:
        self._db.add(movement)

    def add_lot(self, lot: InventoryLot) -> None:
        self._db.add(lot)

    def add_allocation(self, allocation: MovementAllocation) -> None:
        self._db.add(allocation)

    def stock_for_product_id(self, product_id: int) -> float:
        total = self._db.scalar(
            select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0)).where(
                InventoryLot.product_id == product_id
            )
        )
        return float(total or 0)

    def fifo_lots_for_product_id(self, product_id: int) -> list[InventoryLot]:
        return list(
            self._db.scalars(
                select(InventoryLot)
                .where(
                    InventoryLot.product_id == product_id,
                    InventoryLot.qty_remaining > 0,
                )
                .order_by(InventoryLot.received_at, InventoryLot.id)
            )
        )

    def stock_list(self) -> list[tuple[str, float, float]]:
        rows = self._db.execute(
            select(
                Product.sku,
                func.coalesce(func.sum(InventoryLot.qty_remaining), 0).label("qty"),
                Product.min_stock,
            )
            .select_from(Product)
            .outerjoin(InventoryLot, InventoryLot.product_id == Product.id)
            .group_by(Product.id)
            .order_by(Product.sku)
        ).all()
        return [(sku, float(qty or 0), float(min_stock or 0)) for sku, qty, min_stock in rows]
