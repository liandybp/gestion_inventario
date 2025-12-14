from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import and_, delete, func, select
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, MovementAllocation, OperatingExpense, Product
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

    def _month_range(self, now: datetime) -> tuple[datetime, datetime]:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now = now.astimezone(timezone.utc)
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        return start, end

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
                    code = f"{prefix}-{mv.movement_date:%y%m%d%H%M}-{mv.id}"

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

    def create_expense(self, amount: float, concept: str, expense_date: Optional[datetime]) -> None:
        exp = OperatingExpense(
            amount=float(amount),
            concept=concept.strip(),
            expense_date=self._movement_datetime(expense_date),
        )
        self._db.add(exp)
        self._db.commit()

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

        month_key = func.strftime("%Y-%m", InventoryMovement.movement_date)

        purchase_rows = self._db.execute(
            select(
                month_key.label("m"),
                func.coalesce(
                    func.sum(InventoryMovement.quantity * func.coalesce(InventoryMovement.unit_cost, 0)),
                    0,
                ).label("purchases"),
            )
            .where(
                and_(
                    InventoryMovement.type == "purchase",
                    InventoryMovement.movement_date >= start,
                )
            )
            .group_by("m")
        ).all()

        sales_rows = self._db.execute(
            select(
                month_key.label("m"),
                func.coalesce(
                    func.sum(
                        func.abs(InventoryMovement.quantity)
                        * func.coalesce(InventoryMovement.unit_price, 0)
                    ),
                    0,
                ).label("sales"),
            )
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                )
            )
            .group_by("m")
        ).all()

        cogs_rows = self._db.execute(
            select(
                month_key.label("m"),
                func.coalesce(func.sum(MovementAllocation.quantity * MovementAllocation.unit_cost), 0).label(
                    "cogs"
                ),
            )
            .select_from(MovementAllocation)
            .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                )
            )
            .group_by("m")
        ).all()

        purchases_by = {m: float(v or 0) for m, v in purchase_rows}
        sales_by = {m: float(v or 0) for m, v in sales_rows}
        cogs_by = {m: float(v or 0) for m, v in cogs_rows}

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

        lot_code = payload.lot_code or f"{product.sku}-{movement_dt:%y%m%d%H%M}-{movement.id}"
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

            lot_code = f"ADJ-{product.sku}-{movement_dt:%y%m%d%H%M}-{movement.id}"
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
