from __future__ import annotations

from datetime import datetime
import json
import re
import unicodedata
from typing import NamedTuple, Optional

from sqlalchemy import String, case, cast, func, select
from sqlalchemy.orm import Session

from app.models import AuditLog, InventoryLot, InventoryMovement, Location, MovementAllocation, Product

_TRANSFER_REF_RE = re.compile(r"\bref=(TP-[A-Za-z0-9_-]+)")


def _json_object(raw) -> dict:
    if not raw:
        return {}
    try:
        val = json.loads(raw)
        return val if isinstance(val, dict) else {}
    except Exception:
        return {}


class MovementHistoryRow(NamedTuple):
    id: int
    movement_date: Optional[datetime]
    type: Optional[str]
    location_id: Optional[int]
    sku: Optional[str]
    name: Optional[str]
    unit_of_measure: Optional[str]
    quantity: Optional[float]
    unit_cost: Optional[float]
    unit_price: Optional[float]
    note: Optional[str]
    username: Optional[str]



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
    def __init__(self, db: Session, business_id: Optional[int] = None):
        self._db = db
        if business_id is None:
            raise ValueError("business_id is required")
        self._business_id = int(business_id)

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
        stmt = stmt.where(InventoryLot.business_id == self._business_id)
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
        stmt = stmt.where(InventoryLot.business_id == self._business_id)
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        return list(
            self._db.scalars(
                stmt.order_by(InventoryLot.received_at, InventoryLot.id)
            )
        )

    def stock_list(
        self, query: str = "", location_id: Optional[int] = None
    ) -> list[tuple[str, str, Optional[str], str, float, float, int, Optional[float], Optional[float], Optional[float]]]:
        q = query.strip()

        qty_subq = (
            select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0))
            .where(
                InventoryLot.product_id == Product.id,
                InventoryLot.business_id == self._business_id,
            )
            .correlate(Product)
            .scalar_subquery()
        )
        if location_id is not None:
            qty_subq = (
                select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0))
                .where(
                    InventoryLot.product_id == Product.id,
                    InventoryLot.location_id == location_id,
                    InventoryLot.business_id == self._business_id,
                )
                .correlate(Product)
                .scalar_subquery()
            )
        min_purchase_cost_subq = (
            select(func.min(InventoryMovement.unit_cost))
            .where(
                InventoryMovement.product_id == Product.id,
                InventoryMovement.type == "purchase",
                InventoryMovement.business_id == self._business_id,
            )
            .correlate(Product)
            .scalar_subquery()
        )

        stmt = (
            select(
                Product.sku,
                Product.name,
                Product.category,
                Product.unit_of_measure,
                qty_subq.label("qty"),
                Product.min_stock,
                Product.lead_time_days,
                min_purchase_cost_subq.label("min_purchase_cost"),
                Product.default_purchase_cost,
                Product.default_sale_price,
            )
            .select_from(Product)
        )
        stmt = stmt.where(Product.business_id == self._business_id)
        rows = self._db.execute(stmt.order_by(Product.name)).all()
        if q:
            rows = [
                (sku, name, category, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_purchase_cost, default_sale_price)
                for sku, name, category, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_purchase_cost, default_sale_price in rows
                if _query_match(q, str(sku or ""), str(name or ""))
            ]
        return [
            (
                sku,
                name,
                category,
                uom or "",
                float(qty or 0),
                float(min_stock or 0),
                int(lead_time_days or 0),
                float(min_purchase_cost) if min_purchase_cost is not None else None,
                float(default_purchase_cost) if default_purchase_cost is not None else None,
                float(default_sale_price) if default_sale_price is not None else None,
            )
            for sku, name, category, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_purchase_cost, default_sale_price in rows
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
        stmt = stmt.where(InventoryMovement.business_id == self._business_id)
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
        payment_method: Optional[str] = None,
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
                Location.name,
                lot_codes_expr,
                InventoryMovement.paid_by_transfer,
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .outerjoin(Location, Location.id == InventoryMovement.location_id)
            .outerjoin(MovementAllocation, MovementAllocation.movement_id == InventoryMovement.id)
            .outerjoin(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
            .where(InventoryMovement.type == "sale")
            .group_by(InventoryMovement.id, Product.id, Location.id)
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
            .limit(prefetch_limit)
        )
        stmt = stmt.where(InventoryMovement.business_id == self._business_id)
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
        if payment_method == "transfer":
            stmt = stmt.where(InventoryMovement.paid_by_transfer.is_(True))
        elif payment_method == "cash":
            stmt = stmt.where(InventoryMovement.paid_by_transfer.is_(False))
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
        limit: Optional[int] = 100,
    ) -> list[tuple]:
        q = (query or "").strip()
        prefetch_limit = None if limit is None else (max(int(limit or 0) * 10, 500) if q else int(limit or 0))
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

        stmt = stmt.where(InventoryMovement.business_id == self._business_id)

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

        if prefetch_limit is not None:
            stmt = stmt.limit(prefetch_limit)
        rows = [MovementHistoryRow(*r) for r in self._db.execute(stmt).all()]
        if q:
            rows = [r for r in rows if _query_match(q, str(r[4] or ""), str(r[5] or ""))]
        rows = self._fill_reordered_audit_usernames(rows)
        if limit is None:
            return rows
        return rows[: int(limit or 0)]

    def _fill_reordered_audit_usernames(self, rows: list[MovementHistoryRow]) -> list[MovementHistoryRow]:
        """Resuelve el username de ventas/traspasos cuyo audit se emite ANTES de la
        operación (H-041, entity_id NULL) correlacionando por sku+cantidad (salidas)
        y por `ref=` de la nota del movimiento (transfer_in)."""
        missing = [
            r for r in rows
            if not (r.username or "").strip() and r.type in ("sale", "transfer_out", "transfer_in")
        ]
        if not missing:
            return rows
        audits = list(
            self._db.scalars(
                select(AuditLog)
                .where(
                    AuditLog.entity_type == "movement",
                    AuditLog.entity_id.is_(None),
                    AuditLog.action.in_(["sale_create", "transfer_create"]),
                )
                .order_by(AuditLog.created_at.asc(), AuditLog.id.asc())
            )
        )
        if not audits:
            return rows

        transfer_by_sku_qty: dict[tuple[str, float], str] = {}
        sale_by_sku_qty: dict[tuple[str, float], str] = {}
        for a in audits:
            detail = _json_object(a.detail)
            sku = str(detail.get("sku") or "").strip()
            try:
                qty = float(detail.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if not sku:
                continue
            if a.action == "transfer_create":
                transfer_by_sku_qty[(sku, qty)] = a.username or ""
            elif a.action == "sale_create":
                sale_by_sku_qty[(sku, qty)] = a.username or ""
        if not transfer_by_sku_qty and not sale_by_sku_qty:
            return rows

        ref_to_user: dict[str, str] = {}
        for r in rows:
            if r.type != "transfer_out":
                continue
            user = transfer_by_sku_qty.get((str(r.sku or "").strip(), abs(float(r.quantity or 0))), "")
            if not user:
                continue
            m = _TRANSFER_REF_RE.search(r.note or "")
            if m:
                ref_to_user[m.group(1)] = user

        out: list[MovementHistoryRow] = []
        for r in rows:
            username = r.username or ""
            if not username:
                if r.type in ("transfer_out", "transfer_in"):
                    m = _TRANSFER_REF_RE.search(r.note or "")
                    if m and m.group(1) in ref_to_user:
                        username = ref_to_user[m.group(1)]
                elif r.type == "sale":
                    key = (str(r.sku or "").strip(), abs(float(r.quantity or 0)))
                    username = sale_by_sku_qty.get(key, "")
            out.append(r if username == (r.username or "") else r._replace(username=username or None))
        return out
