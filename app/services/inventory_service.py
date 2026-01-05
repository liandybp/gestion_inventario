from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import and_, case, delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    InventoryLot,
    InventoryMovement,
    MoneyExtraction,
    MovementAllocation,
    OperatingExpense,
    Product,
)
from app.business_config import load_business_config
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
from app.utils import month_range as utils_month_range


class InventoryService:
    def __init__(self, db: Session):
        self._db = db
        self._products = ProductRepository(db)
        self._inventory = InventoryRepository(db)

    @property
    def db(self) -> Session:
        return self._db

    def _get_product(self, sku: str) -> Product:
        product = self._products.get_by_sku(sku)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _month_range(self, now: datetime) -> tuple[datetime, datetime]:
        return utils_month_range(now)

    def _warning_if_restock_needed(self, product: Product, stock_after: float) -> Optional[str]:
        min_stock = float(product.min_stock or 0)
        if min_stock > 0 and stock_after < min_stock:
            return "Needs restock"
        return None

    def _unique_lot_code(self, base_code: str) -> str:
        candidate = base_code
        i = 0
        while (
            self._db.scalar(select(InventoryLot.id).where(InventoryLot.lot_code == candidate))
            is not None
        ):
            suffix = chr(ord("A") + (i % 26))
            n = i // 26
            if n > 0:
                suffix = (chr(ord("A") + ((n - 1) % 26))) + suffix
            candidate = f"{base_code}-{suffix}"
            i += 1
        return candidate

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
        used_codes: set[str] = set(existing_codes.values())
        for mv in movements:
            qty = float(mv.quantity)
            if mv.type in ("purchase", "adjustment") and qty > 0:
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
                try:
                    self._db.flush()
                except IntegrityError as e:
                    raise HTTPException(status_code=409, detail="Lote ya existe") from e
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

    def create_expense(self, amount: float, concept: str, expense_date: Optional[datetime]) -> None:
        exp = OperatingExpense(
            amount=float(amount),
            concept=concept.strip(),
            expense_date=self._movement_datetime(expense_date),
        )
        self._db.add(exp)
        self._db.commit()

    def create_extraction(
        self,
        party: str,
        amount: float,
        concept: str,
        extraction_date: Optional[datetime],
    ) -> None:
        row = MoneyExtraction(
            party=(party or "").strip(),
            amount=float(amount),
            concept=concept.strip(),
            extraction_date=self._movement_datetime(extraction_date),
        )
        self._db.add(row)
        self._db.commit()

    def get_extraction(self, extraction_id: int) -> MoneyExtraction:
        row = self._db.get(MoneyExtraction, extraction_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Extraction not found")
        return row

    def update_extraction(
        self,
        extraction_id: int,
        party: str,
        amount: float,
        concept: str,
        extraction_date: Optional[datetime],
    ) -> None:
        row = self.get_extraction(extraction_id)
        row.party = (party or "").strip()
        row.amount = float(amount)
        row.concept = concept.strip()
        row.extraction_date = self._movement_datetime(extraction_date)
        self._db.commit()

    def delete_extraction(self, extraction_id: int) -> None:
        row = self.get_extraction(extraction_id)
        self._db.delete(row)
        self._db.commit()

    def list_extractions(self, start: datetime, end: datetime, limit: int = 200) -> list[MoneyExtraction]:
        return list(
            self._db.scalars(
                select(MoneyExtraction)
                .where(and_(MoneyExtraction.extraction_date >= start, MoneyExtraction.extraction_date < end))
                .order_by(MoneyExtraction.extraction_date.desc(), MoneyExtraction.id.desc())
                .limit(limit)
            )
        )

    def total_extractions_by_party(self, start: datetime, end: datetime) -> dict[str, float]:
        rows = self._db.execute(
            select(
                MoneyExtraction.party,
                func.coalesce(func.sum(MoneyExtraction.amount), 0).label("total"),
            )
            .where(and_(MoneyExtraction.extraction_date >= start, MoneyExtraction.extraction_date < end))
            .group_by(MoneyExtraction.party)
        ).all()
        return {(party or ""): float(total or 0) for party, total in rows}

    def monthly_dividends_report(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        start, end = self._month_range(now_dt)
        summary, _items = self.monthly_profit_report(now=now_dt)
        extraction_totals = self.total_extractions_by_party(start=start, end=end)

        config = load_business_config()
        business_label = (config.dividends.business_label or "Negocio").strip() or "Negocio"
        partners = [p.strip() for p in (config.dividends.partners or []) if (p or "").strip()]

        cogs_total = float(summary.get("cogs_total", 0) or 0)
        expenses_total = float(summary.get("expenses_total", 0) or 0)
        net_total = float(summary.get("net_total", 0) or 0)
        share_each = (net_total / float(len(partners))) if partners else 0.0

        business_ext = float(extraction_totals.get(business_label, 0) or 0)
        extractions: dict[str, float] = {business_label: business_ext}
        pending: dict[str, float] = {business_label: (cogs_total + expenses_total) - business_ext}

        for p in partners:
            p_ext = float(extraction_totals.get(p, 0) or 0)
            extractions[p] = p_ext
            pending[p] = share_each - p_ext

        opening = getattr(config.dividends, "opening_pending", None) or {}
        if isinstance(opening, dict):
            for k, v in opening.items():
                key = (str(k) or "").strip()
                if not key:
                    continue
                try:
                    add = float(v)
                except Exception:
                    continue
                pending[key] = float(pending.get(key, 0) or 0) + add

        return {
            "month_start": start,
            "month_end": end,
            "cogs_total": cogs_total,
            "expenses_total": expenses_total,
            "net_total": net_total,
            "share_each": share_each,
            "extractions": extractions,
            "pending": pending,
        }

    def get_expense(self, expense_id: int) -> OperatingExpense:
        exp = self._db.get(OperatingExpense, expense_id)
        if exp is None:
            raise HTTPException(status_code=404, detail="Expense not found")
        return exp

    def update_expense(
        self,
        expense_id: int,
        amount: float,
        concept: str,
        expense_date: Optional[datetime],
    ) -> None:
        exp = self.get_expense(expense_id)
        exp.amount = float(amount)
        exp.concept = concept.strip()
        exp.expense_date = self._movement_datetime(expense_date)
        self._db.commit()

    def delete_expense(self, expense_id: int) -> None:
        exp = self.get_expense(expense_id)
        self._db.delete(exp)
        self._db.commit()

    def delete_purchase_movement(self, movement_id: int) -> None:
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "purchase":
            raise HTTPException(status_code=404, detail="Purchase movement not found")

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
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "sale":
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

    def list_expenses(self, start: datetime, end: datetime, limit: int = 100) -> list[OperatingExpense]:
        return list(
            self._db.scalars(
                select(OperatingExpense)
                .where(and_(OperatingExpense.expense_date >= start, OperatingExpense.expense_date < end))
                .order_by(OperatingExpense.expense_date.desc(), OperatingExpense.id.desc())
                .limit(limit)
            )
        )

    def total_expenses(self, start: datetime, end: datetime) -> float:
        total = self._db.scalar(
            select(func.coalesce(func.sum(OperatingExpense.amount), 0)).where(
                and_(OperatingExpense.expense_date >= start, OperatingExpense.expense_date < end)
            )
        )
        return float(total or 0)

    def inventory_value_total(self) -> float:
        total = self._db.scalar(
            select(
                func.coalesce(
                    func.sum(
                        func.coalesce(InventoryLot.qty_remaining, 0)
                        * func.coalesce(InventoryLot.unit_cost, 0)
                    ),
                    0,
                )
            )
        )
        return float(total or 0)

    def inventory_sale_value_total(self) -> float:
        total = self._db.scalar(
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
        return float(total or 0)

    def sales_by_product(self, start: datetime, end: datetime) -> tuple[float, list[dict]]:
        rows = self._db.execute(
            select(
                Product.sku,
                Product.name,
                func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                func.coalesce(
                    func.sum(
                        func.abs(InventoryMovement.quantity)
                        * func.coalesce(InventoryMovement.unit_price, 0)
                    ),
                    0,
                ).label("sales"),
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                )
            )
            .group_by(Product.id)
            .order_by(func.coalesce(func.sum(func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0)), 0).desc())
        ).all()

        items: list[dict] = []
        total_sales = 0.0
        for sku, name, qty, sales in rows:
            sales_f = float(sales or 0)
            total_sales += sales_f
            items.append(
                {
                    "sku": sku,
                    "name": name,
                    "qty": float(qty or 0),
                    "sales": sales_f,
                }
            )
        return total_sales, items

    def sales_metrics_table(self, now: datetime, months: int = 12) -> list[dict]:
        now_dt = now
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        month_start, month_end = self._month_range(now_dt)

        range_start = month_start
        for _ in range(max(months - 1, 0)):
            if range_start.month == 1:
                range_start = range_start.replace(year=range_start.year - 1, month=12)
            else:
                range_start = range_start.replace(month=range_start.month - 1)

        range_end = month_end
        range_days = max(1, int((range_end - range_start).days))

        qty_month_expr = func.sum(
            case(
                (
                    and_(InventoryMovement.movement_date >= month_start, InventoryMovement.movement_date < month_end),
                    func.abs(InventoryMovement.quantity),
                ),
                else_=0,
            )
        )

        sales_month_expr = func.sum(
            case(
                (
                    and_(InventoryMovement.movement_date >= month_start, InventoryMovement.movement_date < month_end),
                    func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0),
                ),
                else_=0,
            )
        )

        qty_range_expr = func.sum(func.abs(InventoryMovement.quantity))
        sales_range_expr = func.sum(func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0))

        sale_days_expr = func.count(func.distinct(func.date(InventoryMovement.movement_date)))

        stock_sq = (
            select(
                InventoryLot.product_id.label("product_id"),
                func.coalesce(func.sum(func.coalesce(InventoryLot.qty_remaining, 0)), 0).label("stock_qty"),
            )
            .select_from(InventoryLot)
            .group_by(InventoryLot.product_id)
            .subquery()
        )

        rows = self._db.execute(
            select(
                Product.sku,
                Product.name,
                func.coalesce(qty_month_expr, 0).label("qty_month"),
                func.coalesce(sales_month_expr, 0).label("sales_month"),
                func.coalesce(qty_range_expr, 0).label("qty_range"),
                func.coalesce(sales_range_expr, 0).label("sales_range"),
                func.coalesce(sale_days_expr, 0).label("sale_days"),
                func.coalesce(Product.min_stock, 0).label("min_stock"),
                func.coalesce(Product.lead_time_days, 0).label("lead_time_days"),
                func.coalesce(stock_sq.c.stock_qty, 0).label("stock_qty"),
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .outerjoin(stock_sq, stock_sq.c.product_id == Product.id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= range_start,
                    InventoryMovement.movement_date < range_end,
                )
            )
            .group_by(
                Product.id,
                Product.sku,
                Product.name,
                Product.min_stock,
                Product.lead_time_days,
                stock_sq.c.stock_qty,
            )
            .order_by(func.coalesce(sales_month_expr, 0).desc())
        ).all()

        out: list[dict] = []
        for sku, name, qty_month, sales_month, qty_range, sales_range, sale_days, min_stock, lead_time_days, stock_qty in rows:
            qty_m = float(qty_month or 0)
            sales_m = float(sales_month or 0)
            qty_r = float(qty_range or 0)
            sales_r = float(sales_range or 0)
            sale_days_i = int(sale_days or 0)

            stock_qty_f = float(stock_qty or 0)
            min_stock_f = float(min_stock or 0)

            avg_month_units = qty_r / float(max(months, 1))
            freq_days = (float(range_days) / float(sale_days_i)) if sale_days_i > 0 else None

            lead_time_i = int(lead_time_days or 0)
            min_replenishment_days = max(30, lead_time_i)

            avg_daily_units = float(avg_month_units) / 30.0
            target_days = max(0, lead_time_i + 15)
            target_stock = max(min_stock_f, avg_daily_units * float(target_days))
            qty_to_order = max(0.0, float(target_stock) - stock_qty_f)

            out.append(
                {
                    "sku": str(sku),
                    "name": str(name or ""),
                    "qty_month": qty_m,
                    "sales_month": sales_m,
                    "qty_range": qty_r,
                    "sales_range": sales_r,
                    "avg_month_units": float(avg_month_units),
                    "freq_days": float(freq_days) if freq_days is not None else None,
                    "stock_qty": float(stock_qty_f),
                    "min_stock": float(min_stock_f),
                    "min_replenishment_days": int(min_replenishment_days),
                    "target_days": int(target_days),
                    "target_stock": float(target_stock),
                    "qty_to_order": float(qty_to_order),
                }
            )

        return out

    def daily_sales_series(self, start: datetime, end: datetime) -> list[dict]:
        rows = self._db.execute(
            select(
                func.date(InventoryMovement.movement_date).label("day"),
                func.coalesce(
                    func.sum(
                        func.abs(InventoryMovement.quantity)
                        * func.coalesce(InventoryMovement.unit_price, 0)
                    ),
                    0,
                ).label("sales"),
            )
            .select_from(InventoryMovement)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                )
            )
            .group_by(func.date(InventoryMovement.movement_date))
            .order_by(func.date(InventoryMovement.movement_date))
        ).all()

        by_day: dict[str, float] = {}
        for day, sales in rows:
            by_day[str(day)] = float(sales or 0)

        out: list[dict] = []
        cur = start.date()
        end_date = end.date()
        while cur < end_date:
            key = cur.isoformat()
            out.append({"day": key, "sales": float(by_day.get(key, 0.0))})
            cur = cur + timedelta(days=1)

        return out

    def top_expense_concept(self, start: datetime, end: datetime) -> Optional[dict]:
        row = self._db.execute(
            select(
                OperatingExpense.concept,
                func.coalesce(func.sum(OperatingExpense.amount), 0).label("total"),
            )
            .where(and_(OperatingExpense.expense_date >= start, OperatingExpense.expense_date < end))
            .group_by(OperatingExpense.concept)
            .order_by(func.coalesce(func.sum(OperatingExpense.amount), 0).desc())
            .limit(1)
        ).first()

        if not row:
            return None

        concept, total = row
        return {"concept": concept, "total": float(total or 0)}

    def monthly_profit_report(self, now: Optional[datetime] = None) -> tuple[dict, list[dict]]:
        now_dt = now or datetime.now(timezone.utc)
        start, end = self._month_range(now_dt)

        sales_rows = self._db.execute(
            select(
                Product.id,
                Product.sku,
                Product.name,
                func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                func.coalesce(
                    func.sum(
                        func.abs(InventoryMovement.quantity)
                        * func.coalesce(InventoryMovement.unit_price, 0)
                    ),
                    0,
                ).label("sales"),
            )
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                )
            )
            .group_by(Product.id)
        ).all()

        cogs_rows = self._db.execute(
            select(
                Product.id,
                func.coalesce(func.sum(MovementAllocation.quantity * MovementAllocation.unit_cost), 0).label(
                    "cogs"
                ),
            )
            .select_from(MovementAllocation)
            .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                )
            )
            .group_by(Product.id)
        ).all()

        cogs_by_product = {int(pid): float(cogs or 0) for pid, cogs in cogs_rows}

        items: list[dict] = []
        sales_total = 0.0
        cogs_total = 0.0
        for pid, sku, name, qty, sales in sales_rows:
            sales_f = float(sales or 0)
            cogs_f = float(cogs_by_product.get(int(pid), 0))
            gross = sales_f - cogs_f
            cost_pct = (cogs_f / sales_f * 100.0) if sales_f else 0.0
            gross_pct = (gross / sales_f * 100.0) if sales_f else 0.0
            items.append(
                {
                    "sku": sku,
                    "name": name,
                    "qty": float(qty or 0),
                    "sales": sales_f,
                    "cogs": cogs_f,
                    "gross": gross,
                    "cost_pct": cost_pct,
                    "gross_pct": gross_pct,
                }
            )
            sales_total += sales_f
            cogs_total += cogs_f

        items.sort(key=lambda r: r["sales"], reverse=True)

        gross_total = sales_total - cogs_total
        expenses_total = self.total_expenses(start=start, end=end)
        net_total = gross_total - expenses_total

        summary = {
            "month_start": start,
            "month_end": end,
            "sales_total": sales_total,
            "cogs_total": cogs_total,
            "gross_total": gross_total,
            "expenses_total": expenses_total,
            "net_total": net_total,
            "cogs_pct": (cogs_total / sales_total * 100.0) if sales_total else 0.0,
            "gross_margin_pct": (gross_total / sales_total * 100.0) if sales_total else 0.0,
            "net_margin_pct": (net_total / sales_total * 100.0) if sales_total else 0.0,
            "expenses_pct": (expenses_total / sales_total * 100.0) if sales_total else 0.0,
        }

        return summary, items

    def monthly_profit_items_report(self, now: Optional[datetime] = None) -> tuple[dict, list[dict]]:
        now_dt = now or datetime.now(timezone.utc)
        start, end = self._month_range(now_dt)

        rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                Product.sku,
                Product.name,
                Product.category,
                InventoryLot.lot_code,
                MovementAllocation.unit_cost,
                InventoryMovement.unit_price,
                MovementAllocation.quantity,
            )
            .select_from(MovementAllocation)
            .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
            .join(Product, Product.id == InventoryMovement.product_id)
            .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                )
            )
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
        ).all()

        items: list[dict] = []
        qty_total = 0.0
        sales_total = 0.0
        cogs_total = 0.0
        profit_total = 0.0

        for movement_date, sku, name, category, lot_code, unit_cost, unit_price, qty in rows:
            qty_f = float(qty or 0)
            unit_price_f = float(unit_price or 0)
            unit_cost_f = float(unit_cost or 0)
            sales = qty_f * unit_price_f
            cogs = qty_f * unit_cost_f
            profit = sales - cogs
            margin_pct = (profit / sales * 100.0) if sales else 0.0

            items.append(
                {
                    "movement_date": movement_date,
                    "sku": sku,
                    "name": name,
                    "category": category,
                    "lot_code": lot_code,
                    "unit_cost": unit_cost_f,
                    "unit_price": unit_price_f,
                    "qty": qty_f,
                    "profit": profit,
                    "margin_pct": margin_pct,
                }
            )
            qty_total += qty_f
            sales_total += sales
            cogs_total += cogs
            profit_total += profit

        summary = {
            "month_start": start,
            "month_end": end,
            "qty_total": qty_total,
            "sales_total": sales_total,
            "cogs_total": cogs_total,
            "profit_total": profit_total,
            "margin_pct": (profit_total / sales_total * 100.0) if sales_total else 0.0,
        }

        return summary, items

    def monthly_overview(self, months: int = 12, now: Optional[datetime] = None) -> list[dict]:
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        month_start, _ = self._month_range(now_dt)
        start = month_start
        for _ in range(max(months - 1, 0)):
            if start.month == 1:
                start = start.replace(year=start.year - 1, month=12)
            else:
                start = start.replace(month=start.month - 1)

        purchases_by: dict[str, float] = {}
        sales_by: dict[str, float] = {}
        cogs_by: dict[str, float] = {}

        purchase_rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                InventoryMovement.quantity,
                InventoryMovement.unit_cost,
            ).where(
                and_(
                    InventoryMovement.type == "purchase",
                    InventoryMovement.movement_date >= start,
                )
            )
        ).all()
        for movement_date, qty, unit_cost in purchase_rows:
            dt = movement_date
            if dt is None:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            key = dt.astimezone(timezone.utc).strftime("%Y-%m")
            purchases_by[key] = purchases_by.get(key, 0.0) + float(qty or 0) * float(unit_cost or 0)

        sale_rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                InventoryMovement.quantity,
                InventoryMovement.unit_price,
            ).where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                )
            )
        ).all()
        for movement_date, qty, unit_price in sale_rows:
            dt = movement_date
            if dt is None:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            key = dt.astimezone(timezone.utc).strftime("%Y-%m")
            sales_by[key] = sales_by.get(key, 0.0) + abs(float(qty or 0)) * float(unit_price or 0)

        cogs_rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                MovementAllocation.quantity,
                MovementAllocation.unit_cost,
            )
            .select_from(MovementAllocation)
            .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                )
            )
        ).all()
        for movement_date, qty, unit_cost in cogs_rows:
            dt = movement_date
            if dt is None:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            key = dt.astimezone(timezone.utc).strftime("%Y-%m")
            cogs_by[key] = cogs_by.get(key, 0.0) + float(qty or 0) * float(unit_cost or 0)

        series: list[dict] = []
        cursor = start
        for _ in range(months):
            key = cursor.strftime("%Y-%m")
            sales = float(sales_by.get(key, 0))
            purchases = float(purchases_by.get(key, 0))
            cogs = float(cogs_by.get(key, 0))
            gross = sales - cogs
            series.append(
                {
                    "month": key,
                    "sales": sales,
                    "purchases": purchases,
                    "gross_profit": gross,
                }
            )
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)

        return series

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

        if payload.lot_code:
            lot_code = payload.lot_code
        else:
            base = f"{product.sku}-{movement_dt:%y%m%d%H%M}"
            lot_code = self._unique_lot_code(base)
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
        try:
            self._db.commit()
        except IntegrityError as e:
            self._db.rollback()
            raise HTTPException(status_code=409, detail="Lote ya existe") from e
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

            lot_code = f"ADJ-{product.sku}-{movement_dt:%y%m%d%H%M%S}-{movement.id}"
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
            try:
                self._db.commit()
            except IntegrityError as e:
                self._db.rollback()
                raise HTTPException(status_code=409, detail="Lote ya existe") from e
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
            lead_time_days=int(getattr(product, "lead_time_days", 0) or 0),
        )

    def stock_list(self, query: str = "") -> list[StockRead]:
        base_rows = list(self._inventory.stock_list(query=query))
        skus = [sku for sku, *_ in base_rows if sku]

        avg_daily_by_sku: dict[str, float] = {}
        if skus:
            now_dt = datetime.now(timezone.utc)
            start = now_dt - timedelta(days=30)
            rows = self._db.execute(
                select(
                    Product.sku,
                    func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
                )
                .select_from(InventoryMovement)
                .join(Product, Product.id == InventoryMovement.product_id)
                .where(
                    and_(
                        InventoryMovement.type == "sale",
                        InventoryMovement.movement_date >= start,
                        Product.sku.in_(skus),
                    )
                )
                .group_by(Product.sku)
            ).all()
            for sku, qty_sold in rows:
                avg_daily_by_sku[str(sku)] = float(qty_sold or 0) / 30.0

        out: list[StockRead] = []
        for sku, name, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_sale_price in base_rows:
            avg_daily = float(avg_daily_by_sku.get(str(sku), 0.0))
            reorder_in_days: Optional[int] = None
            if avg_daily > 0:
                days_cover = float(qty or 0) / avg_daily
                reorder_in_days = max(0, int(days_cover - float(lead_time_days or 0)))

            out.append(
                StockRead(
                    sku=sku,
                    name=name,
                    unit_of_measure=uom or None,
                    quantity=qty,
                    min_stock=min_stock,
                    needs_restock=min_stock > 0 and qty < min_stock,
                    lead_time_days=int(lead_time_days or 0),
                    avg_daily_sales=avg_daily,
                    reorder_in_days=reorder_in_days,
                    min_purchase_cost=min_purchase_cost,
                    default_sale_price=default_sale_price,
                )
            )

        return out

    def recent_purchases(self, query: str = "", limit: int = 20, month: Optional[str] = None, year: Optional[int] = None) -> list[tuple]:
        return self._inventory.recent_purchases(query=query, limit=limit, month=month, year=year)

    def recent_sales(self, query: str = "", limit: int = 20, month: Optional[str] = None, year: Optional[int] = None) -> list[tuple]:
        return self._inventory.recent_sales(query=query, limit=limit, month=month, year=year)

    def movement_history(
        self,
        sku: Optional[str] = None,
        movement_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[tuple]:
        return self._inventory.movement_history(
            sku=sku,
            movement_type=movement_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
