from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import InventoryMovement, Product


class InventoryRepository:
    def __init__(self, db: Session):
        self._db = db

    def add_movement(self, movement: InventoryMovement) -> None:
        self._db.add(movement)

    def stock_for_product_id(self, product_id: int) -> float:
        total = self._db.scalar(
            select(func.coalesce(func.sum(InventoryMovement.quantity), 0)).where(
                InventoryMovement.product_id == product_id
            )
        )
        return float(total or 0)

    def stock_list(self) -> list[tuple[str, float]]:
        rows = self._db.execute(
            select(
                Product.sku,
                func.coalesce(func.sum(InventoryMovement.quantity), 0).label("qty"),
            )
            .select_from(Product)
            .outerjoin(InventoryMovement, InventoryMovement.product_id == Product.id)
            .group_by(Product.id)
            .order_by(Product.sku)
        ).all()
        return [(sku, float(qty or 0)) for sku, qty in rows]
