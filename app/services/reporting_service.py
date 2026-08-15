from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, case, func, select
from sqlalchemy.orm import Session, aliased

from app.models import (
    InventoryLot,
    Location,
    InventoryMovement,
    MovementAllocation,
    OperatingExpense,
    Product,
)
from app.repositories.business_repository import get_business_config
from app.utils import month_range as utils_month_range


class ReportingService:
    """Series temporales mensuales para reportes (gastos y compras por mes)."""

    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def _finance(self):
        from app.services.finance_service import FinanceService
        return FinanceService(self._db, self._business_id)

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _config(self):
        return get_business_config(self._db, self._business_id)

    def _month_range(self, now: datetime) -> tuple[datetime, datetime]:
        return utils_month_range(now)

    def _month_series(self, months: int) -> tuple[datetime, list[str]]:
        """Devuelve (inicio del mes más antiguo, [claves 'YYYY-MM'] de los N meses)."""
        now = datetime.now(timezone.utc)
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        for _ in range(max(int(months) - 1, 0)):
            if start.month == 1:
                start = start.replace(year=start.year - 1, month=12)
            else:
                start = start.replace(month=start.month - 1)
        keys: list[str] = []
        cursor = start
        for _ in range(int(months)):
            keys.append(cursor.strftime("%Y-%m"))
            if cursor.month == 12:
                cursor = cursor.replace(year=cursor.year + 1, month=1)
            else:
                cursor = cursor.replace(month=cursor.month + 1)
        return start, keys

    def expenses_by_month(self, months: int = 12) -> list[dict]:
        start, keys = self._month_series(months)
        rows = self._db.execute(
            select(OperatingExpense.expense_date, OperatingExpense.amount).where(
                OperatingExpense.business_id == self._business_id,
                OperatingExpense.expense_date >= start,
            )
        ).all()
        by_month: dict[str, float] = {}
        for dt, amount in rows:
            key = dt.strftime("%Y-%m")
            by_month[key] = by_month.get(key, 0.0) + float(amount or 0)
        return [{"month": k, "total": by_month.get(k, 0.0)} for k in keys]

    def purchases_by_month(self, months: int = 12) -> list[dict]:
        start, keys = self._month_series(months)
        rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                InventoryMovement.quantity,
                InventoryMovement.unit_cost,
            ).where(
                InventoryMovement.business_id == self._business_id,
                InventoryMovement.type == "purchase",
                InventoryMovement.movement_date >= start,
            )
        ).all()
        by_month: dict[str, float] = {}
        for dt, qty, cost in rows:
            key = dt.strftime("%Y-%m")
            by_month[key] = by_month.get(key, 0.0) + float(qty or 0) * float(cost or 0)
        return [{"month": k, "total": by_month.get(k, 0.0)} for k in keys]

    def sales_by_product(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> tuple[float, list[dict]]:
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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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

    def sales_metrics_table(
        self,
        now: datetime,
        months: int = 12,
        location_id: Optional[int] = None,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> list[dict]:
        now_dt = now
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        # Planning window (e.g. last 12 months) is anchored to end_dt (if provided) or `now`.
        anchor_dt = (end_dt - timedelta(seconds=1)) if end_dt is not None else now_dt
        month_start, month_end = self._month_range(anchor_dt)

        range_start = month_start
        for _ in range(max(months - 1, 0)):
            if range_start.month == 1:
                range_start = range_start.replace(year=range_start.year - 1, month=12)
            else:
                range_start = range_start.replace(month=range_start.month - 1)

        range_end = month_end
        range_days = max(1, int((range_end - range_start).days))

        # Period totals (cumulative by default).
        period_conds = []
        if start_dt is not None:
            period_conds.append(InventoryMovement.movement_date >= start_dt)
        if end_dt is not None:
            period_conds.append(InventoryMovement.movement_date < end_dt)
        period_cond = and_(*period_conds) if period_conds else None

        if period_cond is None:
            qty_period_expr = func.sum(func.abs(InventoryMovement.quantity))
            sales_period_expr = func.sum(func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0))
        else:
            qty_period_expr = func.sum(
                case(
                    (
                        period_cond,
                        func.abs(InventoryMovement.quantity),
                    ),
                    else_=0,
                )
            )
            sales_period_expr = func.sum(
                case(
                    (
                        period_cond,
                        func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0),
                    ),
                    else_=0,
                )
            )

        # Rolling window totals for planning (e.g. last 12 months).
        window_cond = and_(InventoryMovement.movement_date >= range_start, InventoryMovement.movement_date < range_end)
        qty_range_expr = func.sum(
            case(
                (
                    window_cond,
                    func.abs(InventoryMovement.quantity),
                ),
                else_=0,
            )
        )
        sales_range_expr = func.sum(
            case(
                (
                    window_cond,
                    func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0),
                ),
                else_=0,
            )
        )
        sale_days_expr = func.count(func.distinct(case((window_cond, func.date(InventoryMovement.movement_date)), else_=None)))

        stock_sq = (
            select(
                InventoryLot.product_id.label("product_id"),
                func.coalesce(func.sum(func.coalesce(InventoryLot.qty_remaining, 0)), 0).label("stock_qty"),
            )
            .select_from(InventoryLot)
            .where(
                and_(
                    True if self._business_id is None else (InventoryLot.business_id == self._business_id),
                    True if location_id is None else (InventoryLot.location_id == location_id),
                )
            )
            .group_by(InventoryLot.product_id)
            .subquery()
        )

        # For performance, narrow scan when a period filter is provided.
        date_filters = []
        if period_cond is not None:
            min_start = range_start
            if start_dt is not None and start_dt < min_start:
                min_start = start_dt
            max_end = range_end
            if end_dt is not None and end_dt > max_end:
                max_end = end_dt
            date_filters = [InventoryMovement.movement_date >= min_start, InventoryMovement.movement_date < max_end]

        rows = self._db.execute(
            select(
                Product.sku,
                Product.name,
                func.coalesce(qty_period_expr, 0).label("qty_period"),
                func.coalesce(sales_period_expr, 0).label("sales_period"),
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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
                    *date_filters,
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
            .order_by(func.coalesce(sales_period_expr, 0).desc())
        ).all()

        # Weekly demand stats + safety stock / reorder point.
        service_z = 1.2816  # 90%
        cfg = self._config()
        lead_time_days_cfg = float(getattr(getattr(cfg, "inventory", None), "replenishment_lead_time_days", 25.0) or 25.0)
        lead_time_weeks = float(lead_time_days_cfg) / 7.0

        skus = [str(sku) for sku, *_ in rows if sku]
        weekly_stats_by_sku: dict[str, tuple[float, float, int]] = {}
        if skus:
            one_year_ago = anchor_dt - timedelta(days=365)
            earliest_sale = self._db.scalar(
                select(func.min(InventoryMovement.movement_date))
                .select_from(InventoryMovement)
                .where(
                    and_(
                        InventoryMovement.type == "sale",
                        True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                        True if location_id is None else (InventoryMovement.location_id == location_id),
                    )
                )
            )
            if earliest_sale is not None:
                try:
                    earliest_sale = earliest_sale.astimezone(timezone.utc)
                except Exception:
                    pass
            start_window = earliest_sale if earliest_sale and earliest_sale > one_year_ago else one_year_ago

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
                        InventoryMovement.movement_date < anchor_dt,
                        Product.sku.in_(skus),
                        True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                        True if location_id is None else (InventoryMovement.location_id == location_id),
                    )
                )
                .group_by(Product.sku, func.date(InventoryMovement.movement_date))
            ).all()

            weekly_by_sku: dict[str, dict[datetime, float]] = {}
            for sku, day_raw, qty in daily_rows:
                sku_s = str(sku)
                try:
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

            try:
                start_date = start_window.date()
            except Exception:
                start_date = one_year_ago.date()
            start_week_date = start_date - timedelta(days=int(start_date.weekday()))
            end_date = (anchor_dt - timedelta(days=1)).date() if anchor_dt else start_date
            end_week_date = end_date - timedelta(days=int(end_date.weekday()))

            week_starts: list[datetime] = []
            cur = datetime(start_week_date.year, start_week_date.month, start_week_date.day, tzinfo=timezone.utc)
            end_dt_week = datetime(end_week_date.year, end_week_date.month, end_week_date.day, tzinfo=timezone.utc)
            while cur <= end_dt_week:
                week_starts.append(cur)
                cur = cur + timedelta(days=7)

            for sku_s in skus:
                wmap = weekly_by_sku.get(sku_s, {})
                series = [float(wmap.get(ws, 0.0)) for ws in week_starts]
                n_weeks = len(series)
                if n_weeks <= 0:
                    weekly_stats_by_sku[sku_s] = (0.0, 0.0, 0)
                    continue
                mean_w = float(sum(series)) / float(n_weeks)
                if n_weeks <= 1:
                    std_w = 0.0
                else:
                    var = float(sum((x - mean_w) ** 2 for x in series)) / float(n_weeks)
                    std_w = float(math.sqrt(var))
                weekly_stats_by_sku[sku_s] = (mean_w, std_w, int(n_weeks))

        out: list[dict] = []
        for sku, name, qty_period, sales_period, qty_range, sales_range, sale_days, min_stock, lead_time_days, stock_qty in rows:
            qty_p = float(qty_period or 0)
            sales_p = float(sales_period or 0)
            qty_r = float(qty_range or 0)
            sales_r = float(sales_range or 0)
            sale_days_i = int(sale_days or 0)

            stock_qty_f = float(stock_qty or 0)
            min_stock_f = float(min_stock or 0)

            sku_s = str(sku)
            mean_w, std_w, n_weeks = weekly_stats_by_sku.get(sku_s, (0.0, 0.0, 0))
            safety_stock = 0.0
            reorder_point = 0.0
            reorder_shortage = 0.0
            if float(mean_w) > 0:
                std_eff = float(std_w)
                if int(n_weeks) < 4:
                    std_eff = float(max(std_eff, float(mean_w)))
                safety_stock_calc = float(service_z) * float(math.sqrt(lead_time_weeks)) * float(std_eff)
                safety_stock = float(max(1.0, float(math.ceil(safety_stock_calc))))
                reorder_point = float((float(mean_w) * float(lead_time_weeks)) + float(safety_stock))

            # Effective ROP uses product.min_stock as a floor.
            reorder_point = float(max(float(reorder_point or 0.0), float(min_stock_f or 0.0)))
            reorder_shortage = float(max(0.0, float(reorder_point) - float(stock_qty_f)))

            out.append(
                {
                    "sku": str(sku),
                    "name": str(name or ""),
                    "qty_period": qty_p,
                    "sales_period": sales_p,
                    "stock_qty": float(stock_qty_f),
                    "min_stock": float(min_stock_f),
                    "avg_weekly_sales": float(mean_w),
                    "std_weekly_sales": float(std_w),
                    "safety_stock": float(safety_stock),
                    "reorder_point": float(reorder_point),
                    "reorder_shortage": float(reorder_shortage),
                }
            )

        return out

    def daily_sales_series(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> list[dict]:
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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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

    def monthly_profit_report(
        self,
        now: Optional[datetime] = None,
        location_id: Optional[int] = None,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> tuple[dict, list[dict]]:
        now_dt = now or datetime.now(timezone.utc)
        if start is None or end is None:
            month_start, month_end = self._month_range(now_dt)
            start = month_start if start is None else start
            end = month_end if end is None else end

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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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
        expenses_total = self._finance().total_expenses(start=start, end=end)
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

    def monthly_profit_items_report(
        self,
        now: Optional[datetime] = None,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        location_id: Optional[int] = None,
    ) -> tuple[dict, list[dict]]:
        now_dt = now or datetime.now(timezone.utc)
        if start is None or end is None:
            month_start, month_end = self._month_range(now_dt)
            start = month_start if start is None else start
            end = month_end if end is None else end

        src_mv = aliased(InventoryMovement)
        loc = aliased(Location)

        rows = self._db.execute(
            select(
                InventoryMovement.id.label("sale_movement_id"),
                InventoryMovement.movement_date,
                loc.code.label("location_code"),
                loc.name.label("location_name"),
                Product.sku,
                Product.name,
                Product.category,
                InventoryLot.id.label("lot_id"),
                InventoryLot.movement_id.label("source_movement_id"),
                src_mv.type.label("source_movement_type"),
                InventoryLot.lot_code,
                MovementAllocation.unit_cost,
                InventoryMovement.unit_price,
                MovementAllocation.quantity,
            )
            .select_from(MovementAllocation)
            .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
            .outerjoin(loc, loc.id == InventoryMovement.location_id)
            .join(Product, Product.id == InventoryMovement.product_id)
            .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
            .join(src_mv, src_mv.id == InventoryLot.movement_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
                )
            )
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
        ).all()

        items: list[dict] = []
        qty_total = 0.0
        sales_total = 0.0
        cogs_total = 0.0
        profit_total = 0.0

        for (
            sale_movement_id,
            movement_date,
            location_code,
            location_name,
            sku,
            name,
            category,
            lot_id,
            source_movement_id,
            source_movement_type,
            lot_code,
            unit_cost,
            unit_price,
            qty,
        ) in rows:
            qty_f = float(qty or 0)
            unit_price_f = float(unit_price or 0)
            unit_cost_f = float(unit_cost or 0)
            sales = qty_f * unit_price_f
            cogs = qty_f * unit_cost_f
            profit = sales - cogs
            margin_pct = (profit / sales * 100.0) if sales else 0.0

            items.append(
                {
                    "sale_movement_id": int(sale_movement_id),
                    "movement_date": movement_date,
                    "location_code": (str(location_code or "").strip() or ""),
                    "location_name": (str(location_name or "").strip() or ""),
                    "sku": sku,
                    "name": name,
                    "category": category,
                    "lot_id": int(lot_id) if lot_id is not None else None,
                    "source_movement_id": int(source_movement_id) if source_movement_id is not None else None,
                    "source_movement_type": str(source_movement_type or ""),
                    "lot_code": lot_code,
                    "unit_cost": unit_cost_f,
                    "unit_price": unit_price_f,
                    "qty": qty_f,
                    "cogs": cogs,
                    "sales": sales,
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

    def monthly_overview(
        self,
        months: int = 12,
        now: Optional[datetime] = None,
        location_id: Optional[int] = None,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
    ) -> list[dict]:
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        month_start, _ = self._month_range(now_dt)

        range_start = month_start
        for _ in range(max(months - 1, 0)):
            if range_start.month == 1:
                range_start = range_start.replace(year=range_start.year - 1, month=12)
            else:
                range_start = range_start.replace(month=range_start.month - 1)

        _month_start, month_end = self._month_range(now_dt)
        range_end = month_end
        _range_days = max(1, int((range_end - range_start).days))

        date_start = start_dt if start_dt is not None else range_start
        date_end = end_dt if end_dt is not None else range_end

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
                    InventoryMovement.movement_date >= date_start,
                    InventoryMovement.movement_date < date_end,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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
                    InventoryMovement.movement_date >= date_start,
                    InventoryMovement.movement_date < date_end,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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
                    InventoryMovement.movement_date >= date_start,
                    InventoryMovement.movement_date < date_end,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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
        cursor = range_start
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

