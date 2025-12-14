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

    def stock_list(self, query: str = "") -> list[tuple[str, str, float, float]]:
        q = query.strip()
        stmt = (
            select(
                Product.sku,
                Product.name,
                func.coalesce(func.sum(InventoryLot.qty_remaining), 0).label("qty"),
                Product.min_stock,
            )
            .select_from(Product)
            .outerjoin(InventoryLot, InventoryLot.product_id == Product.id)
        )
        if q:
            like = f"%{q}%"
            stmt = stmt.where((Product.sku.like(like)) | (Product.name.like(like)))

        rows = self._db.execute(
            stmt.group_by(Product.id).order_by(Product.name)
        ).all()
        return [
            (sku, name, float(qty or 0), float(min_stock or 0))
            for sku, name, qty, min_stock in rows
        ]

    def recent_purchases(self, query: str = "", limit: int = 20) -> list[tuple]:
        q = query.strip()
        stmt = (
            select(
                InventoryMovement.movement_date,
                Product.sku,
                Product.name,
                InventoryMovement.quantity,
                InventoryMovement.unit_cost,
                InventoryLot.lot_code,
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .outerjoin(InventoryLot, InventoryLot.movement_id == InventoryMovement.id)
            .where(InventoryMovement.type == "purchase")
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
            .limit(limit)
        )
        if q:
            like = f"%{q}%"
            stmt = stmt.where((Product.sku.like(like)) | (Product.name.like(like)))
        return list(self._db.execute(stmt).all())

    def recent_sales(self, query: str = "", limit: int = 20) -> list[tuple]:
        q = query.strip()
        stmt = (
            select(
                InventoryMovement.movement_date,
                Product.sku,
                Product.name,
                func.abs(InventoryMovement.quantity),
                InventoryMovement.unit_price,
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(InventoryMovement.type == "sale")
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
            .limit(limit)
        )
        if q:
            like = f"%{q}%"
            stmt = stmt.where((Product.sku.like(like)) | (Product.name.like(like)))
        return list(self._db.execute(stmt).all())
