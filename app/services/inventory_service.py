from __future__ import annotations
from app.repositories.business_repository import get_business_config

import hashlib
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import and_, case, delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased

from app.models import (
    Business,
    InventoryLot,
    InventoryMovement,
    Location,
    MoneyExtraction,
    MovementAllocation,
    OperatingExpense,
    Product,
)
from app.repositories.inventory_repository import InventoryRepository
from app.repositories.location_repository import LocationRepository
from app.repositories.product_repository import ProductRepository
from app.schemas import (
    AdjustmentCreate,
    MovementRead,
    MovementResult,
    PurchaseCreate,
    SaleCreate,
    StockRead,
    SupplierReturnLotCreate,
    TransferCreate,
    TransferLineResult,
    TransferResult,
)
from app.utils import month_range as utils_month_range

# Cache de métricas de ventas (SS/ROP) — H-033: evitar recalcular por request
_metrics_cache: dict[tuple, tuple] = {}


class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None):
        self._db = db
        if business_id is None:
            raise HTTPException(status_code=409, detail="business_id is required")
        self._business_id = int(business_id)
        self._business_code: Optional[str] = None
        self._products = ProductRepository(db, business_id=self._business_id)
        self._inventory = InventoryRepository(db, business_id=self._business_id)
        self._locations = LocationRepository(db, self._business_id, self._business_code)

    @property
    def db(self) -> Session:
        return self._db

    @property
    def business_id(self) -> int:
        return self._business_id

    def _invalidate_metrics_cache(self) -> None:
        _metrics_cache.clear()

    def _cached_stock_metrics(self, loc_id, skus):
        import time as _time
        import app.settings as _settings
        key = (self._business_id, loc_id, tuple(sorted(str(s) for s in skus)))
        hit = _metrics_cache.get(key)
        if hit is not None and (_time.monotonic() - hit[0]) < _settings.INVENTORY_METRICS_TTL:
            return hit[1]
        result = self._compute_stock_metrics(loc_id, skus)
        _metrics_cache[key] = (_time.monotonic(), result)
        return result

    def _compute_stock_metrics(self, loc_id, skus):
        """Métricas semanales/diarias por SKU usadas para SS/ROP (H-033)."""
        avg_daily_by_sku: dict[str, float] = {}
        weekly_stats_by_sku: dict[str, tuple[float, float, int]] = {}
        if skus:
            now_dt = datetime.now(timezone.utc)

            # Rolling window: use all sales history until it reaches 1 year; after that, last 12 months.
            one_year_ago = now_dt - timedelta(days=365)
            earliest_sale_filters = [
                InventoryMovement.type == "sale",
                Product.sku.in_(skus),
                True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
            ]
            if loc_id is not None:
                earliest_sale_filters.append(InventoryMovement.location_id == loc_id)
            earliest_sale = self._db.scalar(
                select(func.min(InventoryMovement.movement_date))
                .select_from(InventoryMovement)
                .join(Product, Product.id == InventoryMovement.product_id)
                .where(and_(*earliest_sale_filters))
            )
            if earliest_sale is not None:
                try:
                    earliest_sale = earliest_sale.astimezone(timezone.utc)
                except Exception:
                    pass
            start_window = earliest_sale if earliest_sale and earliest_sale > one_year_ago else one_year_ago

            # Aggregate daily sales and bucket to weeks in Python (dialect-safe for sqlite/postgres).
            daily_rows = self._db.execute(
                select(
                    Product.sku,
                    func.date(InventoryMovement.movement_date).label("day"),
                    func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                )
                .select_from(InventoryMovement)
                .join(Product, Product.id == InventoryMovement.product_id)
                .where(
                    and_(
                        InventoryMovement.type == "sale",
                        InventoryMovement.movement_date >= start_window,
                        InventoryMovement.movement_date < now_dt,
                        Product.sku.in_(skus),
                        True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                        True if loc_id is None else (InventoryMovement.location_id == loc_id),
                    )
                )
                .group_by(Product.sku, func.date(InventoryMovement.movement_date))
            ).all()

            weekly_by_sku: dict[str, dict[datetime, float]] = {}
            for sku, day_raw, qty in daily_rows:
                sku_s = str(sku)
                try:
                    # day_raw might be a string (sqlite) or date (postgres). Normalize to date.
                    if hasattr(day_raw, "year"):
                        day_d = day_raw
                    else:
                        day_d = datetime.fromisoformat(str(day_raw)).date()
                except Exception:
                    continue
                week_start_date = day_d - timedelta(days=int(day_d.weekday()))
                week_start_dt = datetime(
                    week_start_date.year,
                    week_start_date.month,
                    week_start_date.day,
                    tzinfo=timezone.utc,
                )
                m = weekly_by_sku.setdefault(sku_s, {})
                m[week_start_dt] = float(m.get(week_start_dt, 0.0)) + float(qty or 0)

            # Build weekly series (including zero weeks) and compute mean/std per SKU.
            try:
                start_date = start_window.date()
            except Exception:
                start_date = one_year_ago.date()
            start_week_date = start_date - timedelta(days=int(start_date.weekday()))
            end_date = (now_dt - timedelta(days=1)).date() if now_dt else start_date
            end_week_date = end_date - timedelta(days=int(end_date.weekday()))

            week_starts: list[datetime] = []
            cur = datetime(start_week_date.year, start_week_date.month, start_week_date.day, tzinfo=timezone.utc)
            end_dt_week = datetime(end_week_date.year, end_week_date.month, end_week_date.day, tzinfo=timezone.utc)
            while cur <= end_dt_week:
                week_starts.append(cur)
                cur = cur + timedelta(days=7)

            for sku_s in skus:
                wmap = weekly_by_sku.get(str(sku_s), {})
                series = [float(wmap.get(ws, 0.0)) for ws in week_starts]
                n_weeks = len(series)
                if n_weeks <= 0:
                    weekly_stats_by_sku[str(sku_s)] = (0.0, 0.0, 0)
                    continue
                mean_w = float(sum(series)) / float(n_weeks)
                if n_weeks <= 1:
                    std_w = 0.0
                else:
                    var = float(sum((x - mean_w) ** 2 for x in series)) / float(n_weeks)
                    std_w = float(math.sqrt(var))
                weekly_stats_by_sku[str(sku_s)] = (mean_w, std_w, int(n_weeks))

            start = now_dt - timedelta(days=30)
            where_parts = [
                InventoryMovement.type == "sale",
                InventoryMovement.movement_date >= start,
                Product.sku.in_(skus),
            ]
            if self._business_id is not None:
                where_parts.append(InventoryMovement.business_id == self._business_id)
            if loc_id is not None:
                where_parts.append(InventoryMovement.location_id == loc_id)

            rows = self._db.execute(
                select(
                    Product.sku,
                    func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                )
                .select_from(InventoryMovement)
                .join(Product, Product.id == InventoryMovement.product_id)
                .where(
                    and_(*where_parts)
                )
                .group_by(Product.sku)
            ).all()
            for sku, qty_sold in rows:
                avg_daily_by_sku[str(sku)] = float(qty_sold or 0) / 30.0

        return avg_daily_by_sku, weekly_stats_by_sku

    def _get_product(self, sku: str) -> Product:
        product = self._products.get_by_sku(sku)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _location_id_for_code(self, code: str) -> int:
        return self._locations.location_id_for_code(code, auto_create=True)
    def _config(self):
        return get_business_config(self._db, self._business_id)

    def _central_location_id(self) -> int:
        cfg = self._config()
        return self._location_id_for_code(cfg.locations.central.code)

    def _default_pos_location_id(self) -> int:
        cfg = self._config()
        return self._location_id_for_code(cfg.locations.default_sale_code())

    def _month_range(self, now: datetime) -> tuple[datetime, datetime]:
        return utils_month_range(now)

    def _warning_if_restock_needed(self, product: Product, stock_after: float) -> Optional[str]:
        min_stock = float(product.min_stock or 0)
        if min_stock > 0 and stock_after < min_stock:
            return "Needs restock"
        return None

    def _sync_default_purchase_cost_from_history(self, product: Product) -> None:
        min_cost = self._db.scalar(
            select(func.min(InventoryMovement.unit_cost)).where(
                and_(
                    InventoryMovement.type == "purchase",
                    InventoryMovement.product_id == product.id,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                )
            )
        )
        if min_cost is None:
            product.default_purchase_cost = None
            return

        val = float(min_cost or 0)
        uom = (str(getattr(product, "unit_of_measure", "") or "").strip().lower())
        if uom == "par":
            val = val * 2.0
        product.default_purchase_cost = float(val)

    def _unique_lot_code(self, base_code: str, *, max_len: int = 64) -> str:
        base = self._compact_lot_code(base_code, max_len=int(max_len))
        candidate = base
        i = 0
        while self._db.scalar(select(InventoryLot.id).where(InventoryLot.lot_code == candidate)) is not None:
            suffix = chr(ord("A") + (i % 26))
            n = i // 26
            if n > 0:
                suffix = (chr(ord("A") + ((n - 1) % 26))) + suffix
            candidate = f"{base}-{suffix}"
            candidate = self._compact_lot_code(candidate, max_len=int(max_len))
            i += 1
        return candidate

    def _compact_lot_code(self, base: str, *, max_len: int = 64) -> str:
        b = (base or "").strip()
        if len(b) <= int(max_len):
            return b
        h = hashlib.sha1(b.encode("utf-8")).hexdigest()[:8]
        cut = max(1, int(max_len) - 9)
        return f"{b[:cut]}-{h}"

    def _consume_fifo(self, product_id: int, location_id: int, movement_id: int, quantity: float) -> None:
        if quantity <= 0:
            return

        stock = self._inventory.stock_for_product_id(product_id, location_id=location_id)
        if stock < quantity:
            raise HTTPException(status_code=409, detail="Stock insuficiente")

        remaining = quantity
        lots = self._inventory.fifo_lots_for_product_id(product_id, location_id=location_id)
        for lot in lots:
            if remaining <= 1e-9:
                break

            take = min(float(lot.qty_remaining), remaining)
            if take <= 1e-9:
                continue
            lot.qty_remaining = float(lot.qty_remaining) - take
            allocation = MovementAllocation(
                movement_id=movement_id,
                lot_id=lot.id,
                quantity=take,
                unit_cost=float(lot.unit_cost),
            )
            self._inventory.add_allocation(allocation)
            remaining -= take

        if remaining > 1e-9:
            raise HTTPException(status_code=409, detail="Stock insuficiente")

    def _rebuild_product_fifo(self, product: Product, lot_code_overrides: Optional[dict[int, str]] = None) -> None:
        overrides = lot_code_overrides or {}

        central_loc_id = self._central_location_id()

        lot_stmt = select(InventoryLot).where(InventoryLot.product_id == product.id)
        if self._business_id is not None:
            lot_stmt = lot_stmt.where(InventoryLot.business_id == self._business_id)

        existing_codes = {
            lot.movement_id: lot.lot_code
            for lot in self._db.scalars(
                lot_stmt
            )
        }
        existing_codes.update(overrides)

        mv_id_stmt = select(InventoryMovement.id).where(InventoryMovement.product_id == product.id)
        if self._business_id is not None:
            mv_id_stmt = mv_id_stmt.where(InventoryMovement.business_id == self._business_id)

        self._db.execute(
            delete(MovementAllocation).where(
                (MovementAllocation.movement_id.in_(mv_id_stmt))
                | (MovementAllocation.lot_id.in_(select(InventoryLot.id).where(InventoryLot.product_id == product.id)))
            )
        )
        del_lot_stmt = delete(InventoryLot).where(InventoryLot.product_id == product.id)
        if self._business_id is not None:
            del_lot_stmt = del_lot_stmt.where(InventoryLot.business_id == self._business_id)
        self._db.execute(del_lot_stmt)
        self._db.flush()

        mv_stmt = (
            select(InventoryMovement)
            .where(InventoryMovement.product_id == product.id)
            .order_by(InventoryMovement.movement_date, InventoryMovement.id)
        )
        if self._business_id is not None:
            mv_stmt = mv_stmt.where(InventoryMovement.business_id == self._business_id)

        movements = list(
            self._db.scalars(mv_stmt)
        )

        fifo_lots_by_loc: dict[int, list[InventoryLot]] = {}
        used_codes: set[str] = set(existing_codes.values())
        for mv in movements:
            qty = float(mv.quantity)
            mv_loc_id = int(getattr(mv, "location_id", None) or 0) or central_loc_id
            fifo_lots = fifo_lots_by_loc.setdefault(mv_loc_id, [])
            if mv.type in ("purchase", "adjustment", "transfer_in") and qty > 0:
                code = existing_codes.get(mv.id)
                if not code:
                    prefix = "ADJ" if mv.type == "adjustment" else product.sku
                    base = f"{prefix}-{mv.movement_date:%y%m%d%H%M}"
                    code = base
                    i = 0
                    while code in used_codes:
                        suffix = chr(ord("A") + (i % 26))
                        n = i // 26
                        if n > 0:
                            suffix = (chr(ord("A") + ((n - 1) % 26))) + suffix
                        code = f"{base}-{suffix}"
                        i += 1
                    used_codes.add(code)

                received_at_dt = mv.movement_date
                if mv.type == "transfer_in":
                    raw = str(mv.note or "")
                    m = re.search(r"received_at=([^ ;]+)", raw)
                    if m:
                        val = m.group(1).strip()
                        if val.endswith("Z"):
                            val = val[:-1] + "+00:00"
                        try:
                            received_at_dt = datetime.fromisoformat(val)
                        except Exception:
                            received_at_dt = mv.movement_date
                if mv.type == "adjustment" and (mv.note or "").startswith("Inventario inicial"):
                    received_at_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
                lot = InventoryLot(
                    business_id=self._business_id,
                    movement_id=mv.id,
                    product_id=product.id,
                    location_id=mv_loc_id,
                    lot_code=code,
                    received_at=received_at_dt,
                    unit_cost=float(mv.unit_cost or 0),
                    qty_received=qty,
                    qty_remaining=qty,
                )
                self._db.add(lot)
                try:
                    self._db.flush()
                except IntegrityError as e:
                    raise HTTPException(status_code=409, detail="Lote ya existe") from e
                fifo_lots.append(lot)
                continue

            if mv.type == "return_supplier":
                raw = str(mv.note or "")
                m = re.search(r"lot_code=([^ ;]+)", raw)
                if m:
                    target_code = m.group(1).strip()
                    target_lot = None
                    for lot in fifo_lots:
                        if str(getattr(lot, "lot_code", "")) == target_code:
                            target_lot = lot
                            break
                    if target_lot is None:
                        raise HTTPException(status_code=409, detail="Lot not found for supplier return")

                    remaining = abs(qty)
                    take = min(float(target_lot.qty_remaining), remaining)
                    if take <= 0:
                        raise HTTPException(status_code=409, detail="Insufficient stock")
                    target_lot.qty_remaining = float(target_lot.qty_remaining) - take
                    alloc = MovementAllocation(
                        movement_id=mv.id,
                        lot_id=target_lot.id,
                        quantity=take,
                        unit_cost=float(target_lot.unit_cost),
                    )
                    self._db.add(alloc)
                    remaining -= take
                    if remaining > 0:
                        raise HTTPException(status_code=409, detail="Insufficient stock")
                    continue

            consume_qty = 0.0
            if mv.type in ("sale", "transfer_out", "return_supplier"):
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
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Insufficient stock while rebuilding FIFO "
                        f"(sku={product.sku}, movement_id={mv.id}, type={mv.type}, "
                        f"location_id={mv_loc_id}, date={mv.movement_date.isoformat()}, "
                        f"missing={remaining})"
                    ),
                )

    def reset_purchases_and_sales(self) -> None:
        movement_ids = select(InventoryMovement.id).where(
            InventoryMovement.type.in_(("purchase", "sale"))
        )
        purchase_ids = select(InventoryMovement.id).where(InventoryMovement.type == "purchase")
        if self._business_id is not None:
            movement_ids = movement_ids.where(InventoryMovement.business_id == self._business_id)
            purchase_ids = purchase_ids.where(InventoryMovement.business_id == self._business_id)
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
    def delete_purchase_movement(self, movement_id: int) -> None:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "purchase":
            raise HTTPException(status_code=404, detail="Purchase movement not found")
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
            raise HTTPException(status_code=404, detail="Purchase movement not found")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        lot_ids = [
            int(lid)
            for (lid,) in self._db.execute(
                select(InventoryLot.id).where(InventoryLot.movement_id == mv.id)
            ).all()
            if int(lid or 0) > 0
        ]

        if lot_ids:
            blockers = self._db.execute(
                select(
                    InventoryMovement.id,
                    InventoryMovement.type,
                    InventoryMovement.movement_date,
                    InventoryMovement.note,
                )
                .select_from(MovementAllocation)
                .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
                .where(
                    and_(
                        MovementAllocation.lot_id.in_(lot_ids),
                        InventoryMovement.id != mv.id,
                        True
                        if self._business_id is None
                        else (InventoryMovement.business_id == self._business_id),
                    )
                )
                .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
                .limit(5)
            ).all()
            if blockers:
                detail_parts: list[str] = []
                for mid, mtype, mdate, note in blockers:
                    dt_s = ""
                    try:
                        dt_s = mdate.isoformat() if mdate else ""
                    except Exception:
                        dt_s = str(mdate or "")
                    note_s = (str(note or "").strip() or "-")
                    detail_parts.append(f"{int(mid)} ({str(mtype)} {dt_s}) note={note_s}")

                raise HTTPException(
                    status_code=409,
                    detail=(
                        "No se puede eliminar la compra porque su lote fue consumido por otros movimientos "
                        "(por ejemplo: ajuste/venta/traspaso). Elimina primero esos movimientos. "
                        "Dependencias: "
                        + "; ".join(detail_parts)
                    ),
                )

        try:
            if lot_ids:
                self._db.execute(delete(MovementAllocation).where(MovementAllocation.lot_id.in_(lot_ids)))
                self._db.execute(delete(InventoryLot).where(InventoryLot.id.in_(lot_ids)))

            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv.id))
            self._db.execute(delete(InventoryLot).where(InventoryLot.movement_id == mv.id))
            self._db.execute(delete(InventoryMovement).where(InventoryMovement.id == mv.id))
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._sync_default_purchase_cost_from_history(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise
        except IntegrityError as e:
            self._db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "No se pudo eliminar la compra porque está relacionada con otros movimientos/stock. "
                    "Revisa si existe un ajuste o una venta posterior para este producto y elimínalo primero."
                ),
            ) from e
        except Exception:
            self._db.rollback()
            raise

    def update_transfer_movement(
        self,
        movement_id: int,
        quantity: float,
        unit_cost: Optional[float],
        movement_date: Optional[datetime],
        note: Optional[str],
    ) -> MovementResult:
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type not in ("transfer_in", "transfer_out"):
            raise HTTPException(status_code=404, detail="Transfer movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        mv.movement_date = self._movement_datetime(movement_date)
        mv.note = note

        if mv.type == "transfer_out":
            mv.quantity = -float(quantity)
            mv.unit_cost = None
            mv.unit_price = None
        else:
            if unit_cost is None:
                unit_cost = float(mv.unit_cost or 0)
            if float(unit_cost) < 0:
                raise HTTPException(status_code=422, detail="unit_cost must be >= 0")
            mv.quantity = float(quantity)
            mv.unit_cost = float(unit_cost)
            mv.unit_price = None

        try:
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv)
        loc_id = int(getattr(mv, "location_id", None) or 0) or self._central_location_id()
        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv),
            stock_after=stock_after,
            warning=warning,
        )

    def update_adjustment_movement(
        self,
        movement_id: int,
        unit_cost: float,
    ) -> MovementResult:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "adjustment":
            raise HTTPException(status_code=404, detail="Adjustment movement not found")
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
            raise HTTPException(status_code=404, detail="Adjustment movement not found")

        if float(mv.quantity or 0) <= 0:
            raise HTTPException(status_code=409, detail="Solo se puede editar el costo de un ajuste positivo")
        if float(unit_cost) < 0:
            raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        mv.unit_cost = float(unit_cost)

        try:
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv)
        loc_id = int(getattr(mv, "location_id", None) or 0) or self._central_location_id()
        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv),
            stock_after=stock_after,
            warning=warning,
        )

    def _transfer_out_id_for_movement_id(self, movement_id: int) -> int:
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type not in ("transfer_in", "transfer_out"):
            raise HTTPException(status_code=404, detail="Transfer movement not found")
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
            raise HTTPException(status_code=404, detail="Transfer movement not found")
        if mv.type == "transfer_out":
            return int(mv.id)
        raw = str(mv.note or "")
        m = re.search(r"out_id=(\d+)", raw)
        if not m:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Este traspaso fue creado antes de habilitar la edición. "
                    "Vuelve a registrarlo o elimínalo y créalo de nuevo."
                ),
            )
        return int(m.group(1))

    def _transfer_to_code_from_out_note(self, note: str) -> str:
        _from_code, to_code, _ref = self._transfer_codes_from_out_note(note)
        return to_code

    def _transfer_codes_from_out_note(self, note: str) -> tuple[str, str, Optional[str]]:
        raw = str(note or "")
        m = re.search(r"Transfer\s+([^\s:;]+)->([^\s:;]+)", raw)
        if not m:
            raise HTTPException(
                status_code=409,
                detail="No se puede editar este traspaso porque falta el origen/destino en la nota.",
            )
        from_code = (m.group(1) or "").strip()
        to_code = (m.group(2) or "").strip()
        if not from_code or not to_code:
            raise HTTPException(
                status_code=409,
                detail="No se puede editar este traspaso porque falta el origen/destino en la nota.",
            )
        ref_m = re.search(r"\bref=([^\s:;]+)", raw)
        ref = (ref_m.group(1).strip() if ref_m else None) or None
        return from_code, to_code, ref

    def _legacy_transfer_in_ids_for_out(
        self,
        mv_out: InventoryMovement,
        to_loc_id: int,
        mv_dt: datetime,
    ) -> list[int]:
        alloc_rows = list(
            self._db.execute(
                select(
                    MovementAllocation.quantity,
                    MovementAllocation.unit_cost,
                    InventoryLot.lot_code,
                    InventoryLot.received_at,
                )
                .select_from(MovementAllocation)
                .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
                .where(MovementAllocation.movement_id == mv_out.id)
                .order_by(InventoryLot.received_at, InventoryLot.id)
            ).all()
        )

        if not alloc_rows:
            return []

        note_hint = ""
        try:
            raw = str(mv_out.note or "")
            if ":" in raw:
                note_hint = raw.split(":", 1)[1].strip()
        except Exception:
            note_hint = ""

        in_ids: list[int] = []
        for a_qty, a_unit_cost, src_lot_code, src_received_at in alloc_rows:
            qty_val = float(a_qty or 0)
            if qty_val <= 1e-9:
                continue
            cost_val = float(a_unit_cost or 0)
            recv_dt = src_received_at
            if recv_dt is None:
                recv_dt = mv_dt

            dt_start = mv_dt - timedelta(hours=12)
            dt_end = mv_dt + timedelta(hours=12)

            recv_start = recv_dt - timedelta(hours=12)
            recv_end = recv_dt + timedelta(hours=12)

            eps = 0.0001
            src_code = (str(src_lot_code or "").strip() or None)
            lot_filter = None
            if src_code:
                lot_filter = (
                    InventoryLot.lot_code.ilike(f"TR-{src_code}-%")
                    | InventoryMovement.note.ilike(f"%lot={src_code}%")
                )
            note_filter = InventoryMovement.note.ilike(f"%{note_hint}%") if note_hint else None

            def _candidates_lot(
                *,
                include_received_at: bool,
                include_note: bool,
                include_lot_hint: bool,
            ) -> list[tuple[int, Optional[datetime]]]:
                where_filters = [
                    InventoryMovement.type == "transfer_in",
                    InventoryMovement.product_id == mv_out.product_id,
                    InventoryMovement.location_id == to_loc_id,
                    InventoryMovement.movement_date >= dt_start,
                    InventoryMovement.movement_date <= dt_end,
                    InventoryLot.product_id == mv_out.product_id,
                    InventoryLot.location_id == to_loc_id,
                    func.abs(func.coalesce(InventoryLot.qty_received, 0) - qty_val) <= eps,
                    func.abs(func.coalesce(InventoryLot.unit_cost, 0) - cost_val) <= eps,
                ]
                if self._business_id is not None:
                    where_filters.extend(
                        [
                            InventoryMovement.business_id == self._business_id,
                            InventoryLot.business_id == self._business_id,
                        ]
                    )
                if include_received_at:
                    where_filters.extend(
                        [
                            InventoryLot.received_at >= recv_start,
                            InventoryLot.received_at <= recv_end,
                        ]
                    )
                if include_lot_hint and lot_filter is not None:
                    where_filters.append(lot_filter)
                if include_note and note_filter is not None:
                    where_filters.append(note_filter)
                return list(
                    self._db.execute(
                        select(InventoryMovement.id, InventoryMovement.movement_date)
                        .select_from(InventoryLot)
                        .join(InventoryMovement, InventoryMovement.id == InventoryLot.movement_id)
                        .where(*where_filters)
                    ).all()
                )

            def _candidates_mv(
                *,
                include_note: bool,
                include_lot_hint: bool,
            ) -> list[tuple[int, Optional[datetime]]]:
                where_filters = [
                    InventoryMovement.type == "transfer_in",
                    InventoryMovement.product_id == mv_out.product_id,
                    InventoryMovement.location_id == to_loc_id,
                    InventoryMovement.movement_date >= dt_start,
                    InventoryMovement.movement_date <= dt_end,
                    func.abs(func.coalesce(InventoryMovement.quantity, 0) - qty_val) <= eps,
                    func.abs(func.coalesce(InventoryMovement.unit_cost, 0) - cost_val) <= eps,
                ]
                if self._business_id is not None:
                    where_filters.append(InventoryMovement.business_id == self._business_id)
                if include_lot_hint and src_code:
                    where_filters.append(InventoryMovement.note.ilike(f"%lot={src_code}%"))
                if include_note and note_filter is not None:
                    where_filters.append(note_filter)
                return list(
                    self._db.execute(
                        select(InventoryMovement.id, InventoryMovement.movement_date)
                        .select_from(InventoryMovement)
                        .where(*where_filters)
                    ).all()
                )

            # Matching passes from most strict to most tolerant.
            passes = [
                ("lot_strict", lambda: _candidates_lot(include_received_at=True, include_note=True, include_lot_hint=True)),
                ("lot_base", lambda: _candidates_lot(include_received_at=False, include_note=True, include_lot_hint=True)),
                ("lot_no_note", lambda: _candidates_lot(include_received_at=False, include_note=False, include_lot_hint=True)),
                ("mv_note", lambda: _candidates_mv(include_note=True, include_lot_hint=True)),
                ("mv_no_note", lambda: _candidates_mv(include_note=False, include_lot_hint=True)),
                ("mv_last", lambda: _candidates_mv(include_note=False, include_lot_hint=False)),
            ]

            chosen_from = None
            candidates: list[tuple[int, Optional[datetime]]] = []
            for name, fn in passes:
                try:
                    candidates = fn() or []
                except Exception:
                    candidates = []
                if candidates:
                    chosen_from = name
                    break

            if not candidates:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "No se pudo identificar las entradas del POS para este traspaso (traspaso antiguo). "
                        f"out_id={int(mv_out.id)} product_id={int(mv_out.product_id)} to_loc_id={int(to_loc_id)} "
                        f"mv_dt={mv_dt.isoformat()} qty={qty_val:g} cost={cost_val:g} src_lot={src_code or ''} "
                        f"note_hint={note_hint or ''}"
                    ),
                )

            unused = [c for c in candidates if int(c[0]) not in in_ids]
            pool = unused if unused else candidates
            pool_sorted = sorted(
                pool,
                key=lambda r: (
                    abs((r[1] or mv_dt) - mv_dt),
                    int(r[0]),
                ),
            )
            in_ids.append(int(pool_sorted[0][0]))

        return sorted({int(x) for x in in_ids})

    def update_transfer_shipment(
        self,
        movement_id: int,
        quantity: float,
        movement_date: Optional[datetime],
        note: Optional[str],
    ) -> MovementResult:
        self._invalidate_metrics_cache()
        out_id = self._transfer_out_id_for_movement_id(movement_id)
        mv_out = self._db.get(InventoryMovement, out_id)
        if mv_out is None or mv_out.type != "transfer_out":
            raise HTTPException(status_code=404, detail="Transfer movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        product = self._db.get(Product, mv_out.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        from_code, to_code, ref = self._transfer_codes_from_out_note(str(mv_out.note or ""))
        from_loc_id = self._location_id_for_code(from_code)
        to_loc_id = self._location_id_for_code(to_code)
        mv_dt = self._movement_datetime(movement_date)

        in_ids = list(
            self._db.scalars(
                select(InventoryMovement.id).where(
                    InventoryMovement.type == "transfer_in",
                    InventoryMovement.note.ilike(f"%out_id={out_id}%"),
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                )
            )
        )
        if not in_ids:
            current_dt = self._movement_datetime(mv_out.movement_date)
            in_ids = self._legacy_transfer_in_ids_for_out(
                mv_out=mv_out,
                to_loc_id=to_loc_id,
                mv_dt=current_dt,
            )
        if not in_ids:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No se pudo identificar las entradas del POS para este traspaso. "
                    "Recomendación: vuelve a registrarlo o elimínalo y créalo de nuevo."
                ),
            )

        effective_ref = (ref or "").strip() or f"TP-{from_code}-{to_code}-{mv_dt:%y%m%d%H%M%S}"  # stable grouping key

        clean_note = (note or "").strip()
        base_note = f"Transfer {from_code}->{to_code} ref={effective_ref}"
        mv_out.note = f"{base_note}: {clean_note}" if clean_note else base_note
        mv_out.quantity = -float(quantity)
        mv_out.movement_date = mv_dt

        try:
            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv_out.id))
            if in_ids:
                self._db.execute(delete(InventoryLot).where(InventoryLot.movement_id.in_(in_ids)))
                self._db.execute(delete(InventoryMovement).where(InventoryMovement.id.in_(in_ids)))
            self._db.flush()

            self._consume_fifo(product.id, from_loc_id, mv_out.id, float(quantity))
            self._db.flush()

            alloc_rows = list(
                self._db.execute(
                    select(
                        MovementAllocation.quantity,
                        MovementAllocation.unit_cost,
                        InventoryLot.lot_code,
                        InventoryLot.received_at,
                    )
                    .select_from(MovementAllocation)
                    .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
                    .where(MovementAllocation.movement_id == mv_out.id)
                    .order_by(InventoryLot.received_at, InventoryLot.id)
                ).all()
            )

            for a_qty, a_unit_cost, src_lot_code, src_received_at in alloc_rows:
                if float(a_qty or 0) <= 1e-9:
                    continue
                src_recv_dt = src_received_at
                if src_recv_dt is None:
                    src_recv_dt = mv_dt

                recv_iso = None
                try:
                    recv_iso = src_recv_dt.isoformat()
                except Exception:
                    recv_iso = None

                mv_in_note = f"Transfer in from {from_code} out_id={mv_out.id} ref={effective_ref}"
                if src_lot_code:
                    mv_in_note = mv_in_note + f" lot={src_lot_code}"
                if recv_iso:
                    mv_in_note = mv_in_note + f" received_at={recv_iso}"
                if clean_note:
                    mv_in_note = mv_in_note + f"; {clean_note}"

                mv_in = InventoryMovement(
                    business_id=self._business_id,
                    product_id=product.id,
                    location_id=to_loc_id,
                    type="transfer_in",
                    quantity=float(a_qty or 0),
                    unit_cost=float(a_unit_cost or 0),
                    unit_price=None,
                    movement_date=mv_dt,
                    note=mv_in_note,
                )
                self._inventory.add_movement(mv_in)
                self._db.flush()

                base_code = f"TR-{src_lot_code or product.sku}-{to_code}-{mv_dt:%y%m%d%H%M%S}-{mv_in.id}"
                lot_code = self._unique_lot_code(self._compact_lot_code(base_code))
                lot = InventoryLot(
                    business_id=self._business_id,
                    movement_id=mv_in.id,
                    product_id=product.id,
                    location_id=to_loc_id,
                    lot_code=lot_code,
                    received_at=src_recv_dt,
                    unit_cost=float(a_unit_cost or 0),
                    qty_received=float(a_qty or 0),
                    qty_remaining=float(a_qty or 0),
                )
                self._inventory.add_lot(lot)

            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv_out)
        stock_after = self._inventory.stock_for_product_id(product.id, location_id=from_loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv_out),
            stock_after=stock_after,
            warning=warning,
        )

    def delete_transfer_shipment(self, movement_id: int) -> None:
        out_id = self._transfer_out_id_for_movement_id(movement_id)
        mv_out = self._db.get(InventoryMovement, out_id)
        if mv_out is None or mv_out.type != "transfer_out":
            raise HTTPException(status_code=404, detail="Transfer movement not found")

        product = self._db.get(Product, mv_out.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        _from_code, to_code, _ref = self._transfer_codes_from_out_note(str(mv_out.note or ""))
        mv_dt = self._movement_datetime(mv_out.movement_date)
        to_loc_id = self._location_id_for_code(to_code)

        in_ids = list(
            self._db.scalars(
                select(InventoryMovement.id).where(
                    InventoryMovement.type == "transfer_in",
                    InventoryMovement.note.ilike(f"%out_id={out_id}%"),
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                )
            )
        )
        if not in_ids:
            in_ids = self._legacy_transfer_in_ids_for_out(mv_out=mv_out, to_loc_id=to_loc_id, mv_dt=mv_dt)
        if not in_ids:
            raise HTTPException(
                status_code=409,
                detail=(
                    "No se pudo identificar las entradas del POS para este traspaso. "
                    "Recomendación: vuelve a registrarlo o elimínalo y créalo de nuevo."
                ),
            )

        try:
            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv_out.id))
            if in_ids:
                self._db.execute(delete(InventoryLot).where(InventoryLot.movement_id.in_(in_ids)))
                self._db.execute(delete(InventoryMovement).where(InventoryMovement.id.in_(in_ids)))
            self._db.execute(delete(InventoryMovement).where(InventoryMovement.id == mv_out.id))
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

    def delete_transfer_movement(self, movement_id: int) -> None:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type not in ("transfer_in", "transfer_out"):
            raise HTTPException(status_code=404, detail="Transfer movement not found")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        try:
            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv.id))
            self._db.execute(delete(InventoryLot).where(InventoryLot.movement_id == mv.id))
            self._db.execute(delete(InventoryMovement).where(InventoryMovement.id == mv.id))
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

    def delete_sale_movement(self, movement_id: int) -> None:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "sale":
            raise HTTPException(status_code=404, detail="Sale movement not found")
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
            raise HTTPException(status_code=404, detail="Sale movement not found")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        try:
            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv.id))
            self._db.execute(delete(InventoryMovement).where(InventoryMovement.id == mv.id))
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

    def delete_adjustment_movement(self, movement_id: int) -> None:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "adjustment":
            raise HTTPException(status_code=404, detail="Adjustment movement not found")
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
            raise HTTPException(status_code=404, detail="Adjustment movement not found")

        product = self._db.get(Product, mv.product_id)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        lot_ids = [
            int(lid)
            for (lid,) in self._db.execute(
                select(InventoryLot.id).where(InventoryLot.movement_id == mv.id)
            ).all()
            if int(lid or 0) > 0
        ]

        if lot_ids:
            blockers = self._db.execute(
                select(
                    InventoryMovement.id,
                    InventoryMovement.type,
                    InventoryMovement.movement_date,
                    InventoryMovement.note,
                )
                .select_from(MovementAllocation)
                .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
                .where(
                    and_(
                        MovementAllocation.lot_id.in_(lot_ids),
                        InventoryMovement.id != mv.id,
                        True
                        if self._business_id is None
                        else (InventoryMovement.business_id == self._business_id),
                    )
                )
                .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
                .limit(5)
            ).all()
            if blockers:
                detail_parts: list[str] = []
                for mid, mtype, mdate, note in blockers:
                    dt_s = ""
                    try:
                        dt_s = mdate.isoformat() if mdate else ""
                    except Exception:
                        dt_s = str(mdate or "")
                    note_s = (str(note or "").strip() or "-")
                    detail_parts.append(f"{int(mid)} ({str(mtype)} {dt_s}) note={note_s}")

                raise HTTPException(
                    status_code=409,
                    detail=(
                        "No se puede eliminar el ajuste porque su lote fue consumido por otros movimientos. "
                        "Elimina primero esos movimientos. Dependencias: "
                        + "; ".join(detail_parts)
                    ),
                )

        try:
            self._db.execute(delete(MovementAllocation).where(MovementAllocation.movement_id == mv.id))
            if lot_ids:
                self._db.execute(delete(MovementAllocation).where(MovementAllocation.lot_id.in_(lot_ids)))
                self._db.execute(delete(InventoryLot).where(InventoryLot.id.in_(lot_ids)))
            self._db.execute(delete(InventoryLot).where(InventoryLot.movement_id == mv.id))
            self._db.execute(delete(InventoryMovement).where(InventoryMovement.id == mv.id))
            self._db.flush()
            self._rebuild_product_fifo(product)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise
        except IntegrityError as e:
            self._db.rollback()
            raise HTTPException(
                status_code=409,
                detail=(
                    "No se pudo eliminar el ajuste porque está relacionado con otros movimientos/stock. "
                    "Revisa si existe una venta/ajuste posterior para este producto y elimínalo primero."
                ),
            ) from e
        except Exception:
            self._db.rollback()
            raise
    def inventory_value_total(self, location_id: Optional[int] = None) -> float:
        stmt = (
            select(
                func.coalesce(
                    func.sum(
                        func.coalesce(InventoryLot.qty_remaining, 0)
                        * func.coalesce(InventoryLot.unit_cost, 0)
                    ),
                    0,
                )
            )
            .select_from(InventoryLot)
        )
        if self._business_id is not None:
            stmt = stmt.where(InventoryLot.business_id == self._business_id)
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        total = self._db.scalar(stmt)
        return float(total or 0)

    def inventory_sale_value_total(self, location_id: Optional[int] = None) -> float:
        stmt = (
            select(
                func.coalesce(
                    func.sum(
                        func.coalesce(InventoryLot.qty_remaining, 0)
                        * func.coalesce(Product.default_sale_price, 0)
                    ),
                    0,
                )
            )
            .select_from(InventoryLot)
            .join(Product, Product.id == InventoryLot.product_id)
        )
        if self._business_id is not None:
            stmt = stmt.where(InventoryLot.business_id == self._business_id)
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        total = self._db.scalar(stmt)
        return float(total or 0)

    def dead_stock(self, days: int = 90, now: Optional[datetime] = None) -> list[dict]:
        from app.models import InventoryLot, Product, InventoryMovement
        now_dt = now or datetime.now(timezone.utc)
        cutoff = now_dt - timedelta(days=days)
        lot_rows = self._db.execute(
            select(InventoryLot.product_id, Product.sku, Product.name,
                   func.sum(InventoryLot.qty_remaining * InventoryLot.unit_cost).label("value"))
            .join(Product, Product.id == InventoryLot.product_id)
            .where(and_(
                InventoryLot.qty_remaining > 0, InventoryLot.received_at < cutoff,
                True if self._business_id is None else (InventoryLot.business_id == self._business_id),
            ))
            .group_by(InventoryLot.product_id, Product.sku, Product.name)
        ).all()
        sold_ids = {pid for (pid,) in self._db.execute(
            select(InventoryMovement.product_id).distinct().where(and_(
                InventoryMovement.type == "sale", InventoryMovement.movement_date >= cutoff,
                True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
            ))
        ).all()}
        return [{"sku": sku, "name": name, "value": float(value or 0)}
                for pid, sku, name, value in lot_rows if pid not in sold_ids]

    def inventory_turnover_days(self, monthly_cogs: float) -> Optional[float]:
        if monthly_cogs <= 0:
            return None
        inv_value = self.inventory_value_total()
        cogs_daily = monthly_cogs / 30.0
        return inv_value / cogs_daily if cogs_daily > 0 else None

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
        self._invalidate_metrics_cache()
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
                self._sync_default_purchase_cost_from_history(prod)
            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise

        self._db.refresh(mv)
        loc_id = int(getattr(mv, "location_id", None) or 0) or self._central_location_id()
        if mv.location_id is None:
            mv.location_id = loc_id
            self._db.commit()
        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
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
        self._invalidate_metrics_cache()
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
        loc_id = int(getattr(mv, "location_id", None) or 0) or self._default_pos_location_id()
        if mv.location_id is None:
            mv.location_id = loc_id
            self._db.commit()
        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(mv),
            stock_after=stock_after,
            warning=warning,
        )

    def purchase(self, payload: PurchaseCreate) -> MovementResult:
        self._invalidate_metrics_cache()
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

        central_loc_id = self._central_location_id()

        movement = InventoryMovement(
            business_id=self._business_id,
            product_id=product.id,
            location_id=central_loc_id,
            type="purchase",
            quantity=payload.quantity,
            unit_cost=unit_cost,
            unit_price=None,
            movement_date=movement_dt,
            note=payload.note,
        )
        self._inventory.add_movement(movement)

        self._db.flush()

        if payload.lot_code:
            lot_code = payload.lot_code
        else:
            base = f"{product.sku}-{movement_dt:%y%m%d%H%M}"
            lot_code = self._unique_lot_code(base)
        lot = InventoryLot(
            business_id=self._business_id,
            movement_id=movement.id,
            product_id=product.id,
            location_id=central_loc_id,
            lot_code=lot_code,
            received_at=movement_dt,
            unit_cost=unit_cost,
            qty_received=payload.quantity,
            qty_remaining=payload.quantity,
        )
        self._inventory.add_lot(lot)

        self._sync_default_purchase_cost_from_history(product)
        try:
            self._db.commit()
        except IntegrityError as e:
            self._db.rollback()
            raise HTTPException(status_code=409, detail="Lote ya existe") from e
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id, location_id=central_loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def supplier_return_by_lot(self, payload: SupplierReturnLotCreate) -> MovementResult:
        self._invalidate_metrics_cache()
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        lot = self._db.get(InventoryLot, int(payload.lot_id))
        if lot is None:
            raise HTTPException(status_code=404, detail="Lot not found")

        product = self._db.get(Product, int(lot.product_id))
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")

        lot_loc_id = int(getattr(lot, "location_id", None) or 0)
        if payload.location_code:
            expected_loc_id = self._location_id_for_code(payload.location_code)
            if int(expected_loc_id) != int(lot_loc_id):
                raise HTTPException(status_code=409, detail="Lot does not belong to selected location")

        if float(lot.qty_remaining or 0) < float(payload.quantity):
            raise HTTPException(status_code=409, detail="Insufficient stock in selected lot")

        movement_dt = self._movement_datetime(payload.movement_date)
        raw_note = (payload.note or "").strip()
        base_note = raw_note or "Devolución a proveedor"
        note = f"{base_note} lot_code={lot.lot_code}"

        movement = InventoryMovement(
            business_id=self._business_id,
            product_id=int(lot.product_id),
            location_id=lot_loc_id,
            type="return_supplier",
            quantity=-float(payload.quantity),
            unit_cost=float(lot.unit_cost or 0),
            unit_price=None,
            movement_date=movement_dt,
            note=note,
        )
        self._inventory.add_movement(movement)
        self._db.flush()

        lot.qty_remaining = float(lot.qty_remaining) - float(payload.quantity)
        alloc = MovementAllocation(
            movement_id=int(movement.id),
            lot_id=int(lot.id),
            quantity=float(payload.quantity),
            unit_cost=float(lot.unit_cost or 0),
        )
        self._inventory.add_allocation(alloc)

        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id, location_id=lot_loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def available_lots(self, sku: str, *, location_code: Optional[str] = None) -> list[InventoryLot]:
        product = self._get_product(sku)
        loc_id: Optional[int] = None
        if location_code:
            loc_id = self._location_id_for_code(location_code)
        return self._inventory.fifo_lots_for_product_id(product.id, location_id=loc_id)

    def transfer(self, payload: TransferCreate) -> TransferResult:
        self._invalidate_metrics_cache()
        if not payload.lines:
            raise HTTPException(status_code=422, detail="lines is required")

        from_code = (payload.from_location_code or "").strip()
        if not from_code:
            from_code = self._config().locations.central.code

        to_code = (payload.to_location_code or "").strip()
        if not to_code:
            raise HTTPException(status_code=422, detail="to_location_code is required")

        if from_code == to_code:
            raise HTTPException(status_code=422, detail="from_location_code must be different from to_location_code")

        movement_dt = self._movement_datetime(payload.movement_date)
        from_loc_id = self._location_id_for_code(from_code)
        to_loc_id = self._location_id_for_code(to_code)

        transfer_ref = f"TP-{from_code}-{to_code}-{movement_dt:%y%m%d%H%M%S}"  # stable grouping key

        results: list[TransferLineResult] = []

        try:
            for line in payload.lines:
                sku = (line.sku or "").strip()
                if not sku:
                    continue
                qty = float(line.quantity or 0)
                if qty <= 0:
                    raise HTTPException(status_code=422, detail=f"quantity must be > 0 for SKU {sku}")

                product = self._get_product(sku)

                stock_before = self._inventory.stock_for_product_id(product.id, location_id=from_loc_id)
                if stock_before < qty:
                    if float(stock_before or 0) <= 0:
                        raise HTTPException(
                            status_code=409,
                            detail=f"No hay stock en {from_code} para {sku}.",
                        )
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"Stock insuficiente en {from_code} para {sku}. "
                            f"Disponible: {float(stock_before):g}. Solicitado: {float(qty):g}."
                        ),
                    )

                note = payload.note
                if note:
                    note = f"Transfer {from_code}->{to_code} ref={transfer_ref}: {note}"
                else:
                    note = f"Transfer {from_code}->{to_code} ref={transfer_ref}"

                mv_out = InventoryMovement(
                    business_id=self._business_id,
                    product_id=product.id,
                    location_id=from_loc_id,
                    type="transfer_out",
                    quantity=-qty,
                    unit_cost=None,
                    unit_price=None,
                    movement_date=movement_dt,
                    note=note,
                )
                self._inventory.add_movement(mv_out)
                self._db.flush()

                self._consume_fifo(product.id, from_loc_id, mv_out.id, qty)
                self._db.flush()

                alloc_rows = list(
                    self._db.execute(
                        select(
                            MovementAllocation.quantity,
                            MovementAllocation.unit_cost,
                            InventoryLot.lot_code,
                            InventoryLot.received_at,
                        )
                        .select_from(MovementAllocation)
                        .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
                        .where(MovementAllocation.movement_id == mv_out.id)
                        .order_by(InventoryLot.received_at, InventoryLot.id)
                    ).all()
                )

                in_ids: list[int] = []
                for a_qty, a_unit_cost, src_lot_code, src_received_at in alloc_rows:
                    if float(a_qty or 0) <= 1e-9:
                        continue
                    src_recv_dt = src_received_at
                    if src_recv_dt is None:
                        src_recv_dt = movement_dt

                    recv_iso = None
                    try:
                        recv_iso = src_recv_dt.isoformat()
                    except Exception:
                        recv_iso = None

                    mv_in_note = f"Transfer in from {from_code} out_id={mv_out.id} ref={transfer_ref}"
                    if src_lot_code:
                        mv_in_note = mv_in_note + f" lot={src_lot_code}"
                    if recv_iso:
                        mv_in_note = mv_in_note + f" received_at={recv_iso}"
                    if payload.note:
                        mv_in_note = mv_in_note + f"; {payload.note}"

                    mv_in = InventoryMovement(
                        business_id=self._business_id,
                        product_id=product.id,
                        location_id=to_loc_id,
                        type="transfer_in",
                        quantity=float(a_qty or 0),
                        unit_cost=float(a_unit_cost or 0),
                        unit_price=None,
                        movement_date=movement_dt,
                        note=mv_in_note,
                    )
                    self._inventory.add_movement(mv_in)
                    self._db.flush()

                    base_code = f"TR-{src_lot_code or product.sku}-{to_code}-{movement_dt:%y%m%d%H%M%S}-{mv_in.id}"
                    lot_code = self._unique_lot_code(self._compact_lot_code(base_code))
                    lot = InventoryLot(
                        business_id=self._business_id,
                        movement_id=mv_in.id,
                        product_id=product.id,
                        location_id=to_loc_id,
                        lot_code=lot_code,
                        received_at=src_recv_dt,
                        unit_cost=float(a_unit_cost or 0),
                        qty_received=float(a_qty or 0),
                        qty_remaining=float(a_qty or 0),
                    )
                    self._inventory.add_lot(lot)
                    in_ids.append(int(mv_in.id))

                results.append(
                    TransferLineResult(
                        sku=product.sku,
                        quantity=qty,
                        movements_out=[int(mv_out.id)],
                        movements_in=in_ids,
                    )
                )

            self._db.commit()
        except HTTPException:
            self._db.rollback()
            raise
        except Exception as e:
            self._db.rollback()
            raise HTTPException(status_code=400, detail=str(e)) from e

        return TransferResult(from_location_code=from_code, to_location_code=to_code, lines=results, transfer_ref=transfer_ref)

    def sale(self, payload: SaleCreate) -> MovementResult:
        self._invalidate_metrics_cache()
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

        cfg = self._config()
        selected_code = str(payload.location_code or "").strip() or cfg.locations.default_sale_code()
        loc_id = self._location_id_for_code(selected_code)

        stock_before = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        if stock_before < payload.quantity:
            if float(stock_before or 0) <= 0:
                raise HTTPException(
                    status_code=409,
                    detail=f"No hay stock en {selected_code} para {product.sku}.",
                )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Stock insuficiente en {selected_code} para {product.sku}. "
                    f"Disponible: {float(stock_before):g}. Solicitado: {float(payload.quantity):g}."
                ),
            )

        movement = InventoryMovement(
            business_id=self._business_id,
            product_id=product.id,
            location_id=loc_id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=unit_price,
            movement_date=movement_dt,
            note=payload.note,
        )
        self._inventory.add_movement(movement)

        self._db.flush()
        self._consume_fifo(product.id, loc_id, movement.id, payload.quantity)
        self._db.commit()
        self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def adjustment(self, payload: AdjustmentCreate) -> MovementResult:
        self._invalidate_metrics_cache()
        if payload.quantity_delta == 0:
            raise HTTPException(status_code=422, detail="quantity_delta must be != 0")

        product = self._get_product(payload.sku)
        movement_dt = self._movement_datetime(payload.movement_date)
        is_initial_inventory = bool(
            payload.note and str(payload.note).startswith("Inventario inicial")
        )

        loc_id = self._central_location_id()
        if payload.location_code:
            loc_id = self._location_id_for_code(payload.location_code)

        if payload.quantity_delta > 0:
            if payload.unit_cost is None:
                raise HTTPException(
                    status_code=422, detail="unit_cost is required for positive adjustment"
                )
            if payload.unit_cost < 0:
                raise HTTPException(status_code=422, detail="unit_cost must be >= 0")

            movement = InventoryMovement(
                business_id=self._business_id,
                product_id=product.id,
                location_id=loc_id,
                type="adjustment",
                quantity=payload.quantity_delta,
                unit_cost=payload.unit_cost,
                unit_price=None,
                movement_date=movement_dt,
                note=payload.note,
            )
            self._inventory.add_movement(movement)

            self._db.flush()

            lot_code = f"ADJ-{product.sku}-{movement_dt:%y%m%d%H%M%S}-{movement.id}"
            received_at_dt = movement_dt
            if is_initial_inventory:
                received_at_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            lot = InventoryLot(
                business_id=self._business_id,
                movement_id=movement.id,
                product_id=product.id,
                location_id=loc_id,
                lot_code=lot_code,
                received_at=received_at_dt,
                unit_cost=payload.unit_cost,
                qty_received=payload.quantity_delta,
                qty_remaining=payload.quantity_delta,
            )
            self._inventory.add_lot(lot)
            try:
                self._db.commit()
            except IntegrityError as e:
                self._db.rollback()
                raise HTTPException(status_code=409, detail="Lote ya existe") from e
            self._db.refresh(movement)
        else:
            qty_to_remove = -payload.quantity_delta
            stock_before = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
            if stock_before < qty_to_remove:
                raise HTTPException(status_code=409, detail="Insufficient stock")

            movement = InventoryMovement(
                business_id=self._business_id,
                product_id=product.id,
                location_id=loc_id,
                type="adjustment",
                quantity=payload.quantity_delta,
                unit_cost=None,
                unit_price=None,
                movement_date=movement_dt,
                note=payload.note,
            )
            self._inventory.add_movement(movement)
            self._db.flush()
            self._consume_fifo(product.id, loc_id, movement.id, qty_to_remove)
            self._db.commit()
            self._db.refresh(movement)

        stock_after = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        warning = self._warning_if_restock_needed(product, stock_after)
        return MovementResult(
            movement=MovementRead.model_validate(movement),
            stock_after=stock_after,
            warning=warning,
        )

    def stock(self, sku: str, location_code: Optional[str] = None) -> StockRead:
        product = self._get_product(sku)
        loc_id = self._central_location_id() if not location_code else self._location_id_for_code(location_code)
        qty = self._inventory.stock_for_product_id(product.id, location_id=loc_id)
        min_stock = float(product.min_stock or 0)
        return StockRead(
            sku=product.sku,
            name=product.name,
            unit_of_measure=product.unit_of_measure,
            quantity=qty,
            min_stock=min_stock,
            needs_restock=min_stock > 0 and qty < min_stock,
            lead_time_days=int(getattr(product, "lead_time_days", 0) or 0),
        )

    def stock_for_location(self, sku: str, location_code: str) -> float:
        product = self._get_product(sku)
        loc_id = self._location_id_for_code(location_code)
        return self._inventory.stock_for_product_id(product.id, location_id=loc_id)

    def stock_list(
        self,
        query: str = "",
        location_code: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> list[StockRead]:
        loc_id = None
        if location_code:
            loc_id = self._location_id_for_code(location_code)
        base_rows = list(self._inventory.stock_list(query=query, location_id=loc_id))
        skus = [sku for sku, *_ in base_rows if sku]

        # Safety stock / reorder point configuration.
        service_z = 1.2816  # 90%
        cfg = self._config()
        lead_time_days_cfg = float(getattr(getattr(cfg, "inventory", None), "replenishment_lead_time_days", 25.0) or 25.0)
        lead_time_weeks = float(lead_time_days_cfg) / 7.0

        avg_daily_by_sku: dict[str, float] = {}
        weekly_stats_by_sku: dict[str, tuple[float, float, int]] = {}
        if skus:
            avg_daily_by_sku, weekly_stats_by_sku = self._cached_stock_metrics(loc_id, skus)

        out: list[StockRead] = []
        for sku, name, category, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_purchase_cost, default_sale_price in base_rows:
            avg_daily = float(avg_daily_by_sku.get(str(sku), 0.0))
            reorder_in_days: Optional[int] = None
            if avg_daily > 0:
                days_cover = float(qty or 0) / avg_daily
                reorder_in_days = max(0, int(days_cover - float(lead_time_days or 0)))

            mean_w, std_w, n_weeks = weekly_stats_by_sku.get(str(sku), (0.0, 0.0, 0))
            safety_stock = 0.0
            reorder_point = 0.0
            reorder_shortage = 0.0
            if mean_w > 0:
                std_eff = float(std_w)
                if int(n_weeks) < 4:
                    std_eff = float(max(std_eff, float(mean_w)))
                safety_stock_calc = float(service_z) * float(math.sqrt(lead_time_weeks)) * float(std_eff)
                safety_stock = float(max(1.0, float(math.ceil(safety_stock_calc))))
                reorder_point = float((float(mean_w) * float(lead_time_weeks)) + float(safety_stock))

            min_stock_f = float(min_stock or 0)
            reorder_point = float(max(float(reorder_point or 0.0), float(min_stock_f or 0.0)))
            reorder_shortage = float(max(0.0, float(reorder_point) - float(qty or 0)))

            out.append(
                StockRead(
                    sku=sku,
                    name=name,
                    category=category,
                    unit_of_measure=uom or None,
                    quantity=qty,
                    min_stock=min_stock,
                    needs_restock=reorder_shortage > 0,
                    lead_time_days=int(lead_time_days or 0),
                    avg_daily_sales=avg_daily,
                    reorder_in_days=reorder_in_days,
                    avg_weekly_sales=float(mean_w),
                    std_weekly_sales=float(std_w),
                    safety_stock=float(safety_stock),
                    reorder_point=float(reorder_point),
                    reorder_shortage=float(reorder_shortage),
                    min_purchase_cost=min_purchase_cost,
                    default_purchase_cost=default_purchase_cost,
                    default_sale_price=default_sale_price,
                )
            )

        if limit is not None:
            out = out[offset : offset + limit]

        return out

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
        return self._inventory.recent_purchases(
            query=query,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            month=month,
            year=year,
            location_id=location_id,
        )

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
        return self._inventory.recent_sales(
            query=query,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            month=month,
            year=year,
            location_id=location_id,
        )

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
        return self._inventory.movement_history(
            sku=sku,
            query=query,
            movement_type=movement_type,
            location_id=location_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
