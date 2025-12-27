from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import String, cast, func, select
from sqlalchemy.orm import Session

from app.models import AuditLog, InventoryLot, InventoryMovement, MovementAllocation, Product


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

    def stock_list(self, query: str = "") -> list[tuple[str, str, str, float, float]]:
        q = query.strip()
        stmt = (
            select(
                Product.sku,
                Product.name,
                Product.unit_of_measure,
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
            (sku, name, uom or "", float(qty or 0), float(min_stock or 0))
            for sku, name, uom, qty, min_stock in rows
        ]

    def recent_purchases(self, query: str = "", limit: int = 20) -> list[tuple]:
        q = query.strip()
        stmt = (
            select(
                InventoryMovement.id,
                InventoryMovement.movement_date,
                Product.sku,
                Product.name,
                Product.unit_of_measure,
                Product.image_url,
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

        if self._db.get_bind().dialect.name == "postgresql":
            lot_codes_expr = func.string_agg(InventoryLot.lot_code, ",").label("lot_codes")
        else:
            lot_codes_expr = func.group_concat(InventoryLot.lot_code, ",").label("lot_codes")

        stmt = (
            select(
                InventoryMovement.id,
                InventoryMovement.movement_date,
                Product.sku,
                Product.name,
                Product.unit_of_measure,
                Product.image_url,
                func.abs(InventoryMovement.quantity),
                InventoryMovement.unit_price,
                lot_codes_expr,
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .outerjoin(MovementAllocation, MovementAllocation.movement_id == InventoryMovement.id)
            .outerjoin(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
            .where(InventoryMovement.type == "sale")
            .group_by(InventoryMovement.id, Product.id)
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
            .limit(limit)
        )
        if q:
            like = f"%{q}%"
            stmt = stmt.where((Product.sku.like(like)) | (Product.name.like(like)))
        return list(self._db.execute(stmt).all())

    def movement_history(
        self,
        sku: Optional[str] = None,
        movement_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[tuple]:
        username_sq = (
            select(AuditLog.username)
            .where(
                AuditLog.entity_type == "movement",
                AuditLog.entity_id == cast(InventoryMovement.id, String),
                AuditLog.action.in_([
                    "purchase_create",
                    "sale_create",
                    "adjustment_create",
                ]),
            )
            .order_by(AuditLog.created_at.desc(), AuditLog.id.desc())
            .limit(1)
            .scalar_subquery()
        )

        stmt = (
            select(
                InventoryMovement.id,
                InventoryMovement.movement_date,
                InventoryMovement.type,
                Product.sku,
                Product.name,
                Product.unit_of_measure,
                InventoryMovement.quantity,
                InventoryMovement.unit_cost,
                InventoryMovement.unit_price,
                InventoryMovement.note,
                username_sq.label("username"),
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
        )

        if sku:
            stmt = stmt.where(Product.sku == sku)

        if movement_type:
            stmt = stmt.where(InventoryMovement.type == movement_type)

        if start_date:
            stmt = stmt.where(InventoryMovement.movement_date >= start_date)

        if end_date:
            stmt = stmt.where(InventoryMovement.movement_date < end_date)

        stmt = stmt.limit(limit)
        return list(self._db.execute(stmt).all())
