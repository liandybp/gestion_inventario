from __future__ import annotations

from datetime import datetime
import unicodedata
from typing import Optional

from sqlalchemy import String, case, cast, func, select
from sqlalchemy.orm import Session

from app.models import AuditLog, InventoryLot, InventoryMovement, MovementAllocation, Product


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    nfd = unicodedata.normalize("NFD", text)
    return "".join(char for char in nfd if unicodedata.category(char) != "Mn")


def _query_match(query: str, sku: str, name: str) -> bool:
    q = _normalize_text(query).lower().strip()
    if not q:
        return True
    sku_n = _normalize_text(sku or "").lower()
    name_n = _normalize_text(name or "").lower()
    return (q in sku_n) or (q in name_n)


class InventoryRepository:
    def __init__(self, db: Session):
        self._db = db

    def add_movement(self, movement: InventoryMovement) -> None:
        self._db.add(movement)

    def add_lot(self, lot: InventoryLot) -> None:
        self._db.add(lot)

    def add_allocation(self, allocation: MovementAllocation) -> None:
        self._db.add(allocation)

    def stock_for_product_id(self, product_id: int, location_id: Optional[int] = None) -> float:
        stmt = select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0)).where(
            InventoryLot.product_id == product_id
        )
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        total = self._db.scalar(stmt)
        return float(total or 0)

    def fifo_lots_for_product_id(self, product_id: int, location_id: Optional[int] = None) -> list[InventoryLot]:
        stmt = (
            select(InventoryLot)
            .where(
                InventoryLot.product_id == product_id,
                InventoryLot.qty_remaining > 0,
            )
        )
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        return list(
            self._db.scalars(
                stmt.order_by(InventoryLot.received_at, InventoryLot.id)
            )
        )

    def stock_list(
        self, query: str = "", location_id: Optional[int] = None
    ) -> list[tuple[str, str, str, float, float, int, Optional[float], Optional[float]]]:
        q = query.strip()

        qty_subq = (
            select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0))
            .where(InventoryLot.product_id == Product.id)
            .correlate(Product)
            .scalar_subquery()
        )
        if location_id is not None:
            qty_subq = (
                select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0))
                .where(
                    InventoryLot.product_id == Product.id,
                    InventoryLot.location_id == location_id,
                )
                .correlate(Product)
                .scalar_subquery()
            )
        min_purchase_cost_subq = (
            select(func.min(InventoryMovement.unit_cost))
            .where(
                InventoryMovement.product_id == Product.id,
                InventoryMovement.type == "purchase",
            )
            .correlate(Product)
            .scalar_subquery()
        )

        stmt = (
            select(
                Product.sku,
                Product.name,
                Product.unit_of_measure,
                qty_subq.label("qty"),
                Product.min_stock,
                Product.lead_time_days,
                min_purchase_cost_subq.label("min_purchase_cost"),
                Product.default_sale_price,
            )
            .select_from(Product)
        )
        rows = self._db.execute(stmt.order_by(Product.name)).all()
        if q:
            rows = [
                (sku, name, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_sale_price)
                for sku, name, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_sale_price in rows
                if _query_match(q, str(sku or ""), str(name or ""))
            ]
        return [
            (
                sku,
                name,
                uom or "",
                float(qty or 0),
                float(min_stock or 0),
                int(lead_time_days or 0),
                float(min_purchase_cost) if min_purchase_cost is not None else None,
                float(default_sale_price) if default_sale_price is not None else None,
            )
            for sku, name, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_sale_price in rows
        ]

    def recent_purchases(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[tuple]:
        q = query.strip()
        prefetch_limit = max(int(limit or 0) * 10, 500) if q else int(limit or 0)
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
            .limit(prefetch_limit)
        )
        if location_id is not None:
            stmt = stmt.where(InventoryMovement.location_id == location_id)
        if start_date:
            stmt = stmt.where(InventoryMovement.movement_date >= start_date)
        if end_date:
            stmt = stmt.where(InventoryMovement.movement_date < end_date)
        if month and year:
            stmt = stmt.where(
                func.extract('year', InventoryMovement.movement_date) == year,
                func.extract('month', InventoryMovement.movement_date) == int(month)
            )
        rows = list(self._db.execute(stmt).all())
        if q:
            rows = [r for r in rows if _query_match(q, str(r[2] or ""), str(r[3] or ""))]
        return rows[: int(limit or 0)]

    def recent_sales(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[tuple]:
        q = query.strip()
        prefetch_limit = max(int(limit or 0) * 10, 500) if q else int(limit or 0)

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
            .limit(prefetch_limit)
        )
        if location_id is not None:
            stmt = stmt.where(InventoryMovement.location_id == location_id)
        if start_date:
            stmt = stmt.where(InventoryMovement.movement_date >= start_date)
        if end_date:
            stmt = stmt.where(InventoryMovement.movement_date < end_date)
        if month and year:
            stmt = stmt.where(
                func.extract('year', InventoryMovement.movement_date) == year,
                func.extract('month', InventoryMovement.movement_date) == int(month)
            )
        rows = list(self._db.execute(stmt).all())
        if q:
            rows = [r for r in rows if _query_match(q, str(r[2] or ""), str(r[3] or ""))]
        return rows[: int(limit or 0)]

    def movement_history(
        self,
        sku: Optional[str] = None,
        query: str = "",
        movement_type: Optional[str] = None,
        location_id: Optional[int] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[tuple]:
        q = (query or "").strip()
        prefetch_limit = max(int(limit or 0) * 10, 500) if q else int(limit or 0)
        username_sq = (
            select(AuditLog.username)
            .where(
                AuditLog.entity_type == "movement",
                AuditLog.entity_id == cast(InventoryMovement.id, String),
                AuditLog.action.in_([
                    "purchase_create",
                    "sale_create",
                    "adjustment_create",
                    "transfer_create",
                    "supplier_return_create",
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
                InventoryMovement.location_id,
                Product.sku,
                Product.name,
                Product.unit_of_measure,
                InventoryMovement.quantity,
                InventoryMovement.unit_cost,
                case(
                    (InventoryMovement.type == "purchase", Product.default_sale_price),
                    else_=InventoryMovement.unit_price,
                ).label("unit_price"),
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

        if location_id is not None:
            stmt = stmt.where(InventoryMovement.location_id == location_id)

        if start_date:
            stmt = stmt.where(InventoryMovement.movement_date >= start_date)

        if end_date:
            stmt = stmt.where(InventoryMovement.movement_date < end_date)

        stmt = stmt.limit(prefetch_limit)
        rows = list(self._db.execute(stmt).all())
        if q:
            rows = [r for r in rows if _query_match(q, str(r[4] or ""), str(r[5] or ""))]
        return rows[: int(limit or 0)]
