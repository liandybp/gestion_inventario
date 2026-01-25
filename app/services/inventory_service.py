from __future__ import annotations

import hashlib
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
    SupplierReturnLotCreate,
    TransferCreate,
    TransferLineResult,
    TransferResult,
)
from app.utils import month_range as utils_month_range


class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None):
        self._db = db
        if business_id is None:
            raise HTTPException(status_code=409, detail="business_id is required")
        self._business_id = int(business_id)
        self._business_code: Optional[str] = None
        self._products = ProductRepository(db, business_id=self._business_id)
        self._inventory = InventoryRepository(db, business_id=self._business_id)

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

    def _location_id_for_code(self, code: str) -> int:
        c = (code or "").strip()
        if not c:
            raise HTTPException(status_code=409, detail="location_code is required")
        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == c,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is not None:
            return int(loc_id)

        cfg = self._config()
        business_code = (self._business_code or "").strip()
        prefix = "".join(ch if ch.isalnum() else "_" for ch in (business_code.upper() or "BUSINESS"))
        alt_code = c if c.startswith(f"{prefix}_") else f"{prefix}_{c}"

        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == alt_code,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is not None:
            return int(loc_id)

        name = c
        try:
            cfg_central = str(getattr(cfg.locations.central, "code", "") or "").strip()
            if c == cfg_central or c.upper() == "CENTRAL":
                name = str(getattr(cfg.locations.central, "name", "") or "").strip() or c
            else:
                for p in (getattr(cfg.locations, "pos", None) or []):
                    p_code = str(getattr(p, "code", "") or "").strip()
                    if p_code == c:
                        name = str(getattr(p, "name", "") or "").strip() or c
                        break

            self._db.add(Location(business_id=int(self._business_id), code=alt_code, name=name))
            self._db.commit()
        except IntegrityError:
            self._db.rollback()

        loc_id = self._db.scalar(
            select(Location.id).where(
                and_(
                    Location.code == alt_code,
                    Location.business_id == self._business_id,
                )
            )
        )
        if loc_id is None:
            raise HTTPException(status_code=409, detail=f"Unknown location_code for this business: {c}")
        return int(loc_id)

    def _config(self):
        if self._business_code is None and self._business_id is not None:
            code = self._db.scalar(select(Business.code).where(Business.id == int(self._business_id)))
            self._business_code = (str(code).strip() if code is not None else None) or ""
        return load_business_config(self._business_code or None)

    def _central_location_id(self) -> int:
        cfg = self._config()
        return self._location_id_for_code(cfg.locations.central.code)

    def _default_pos_location_id(self) -> int:
        cfg = self._config()
        return self._location_id_for_code(cfg.locations.default_pos)

    def _month_range(self, now: datetime) -> tuple[datetime, datetime]:
        return utils_month_range(now)

    def _warning_if_restock_needed(self, product: Product, stock_after: float) -> Optional[str]:
        min_stock = float(product.min_stock or 0)
        if min_stock > 0 and stock_after < min_stock:
            return "Needs restock"
        return None

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

    def create_expense(self, amount: float, concept: str, expense_date: Optional[datetime]) -> None:
        exp = OperatingExpense(
            business_id=self._business_id,
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
            business_id=self._business_id,
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
        if self._business_id is not None and int(getattr(row, "business_id", 0) or 0) != int(self._business_id):
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

    def list_extractions(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 200,
    ) -> list[MoneyExtraction]:
        stmt = select(MoneyExtraction)
        if self._business_id is not None:
            stmt = stmt.where(MoneyExtraction.business_id == self._business_id)
        if start is not None:
            stmt = stmt.where(MoneyExtraction.extraction_date >= start)
        if end is not None:
            stmt = stmt.where(MoneyExtraction.extraction_date < end)
        stmt = stmt.order_by(MoneyExtraction.extraction_date.desc(), MoneyExtraction.id.desc()).limit(limit)
        return list(self._db.scalars(stmt))

    def total_extractions_by_party(self, start: datetime, end: datetime) -> dict[str, float]:
        stmt = (
            select(
                MoneyExtraction.party,
                func.coalesce(func.sum(MoneyExtraction.amount), 0).label("total"),
            )
            .where(and_(MoneyExtraction.extraction_date >= start, MoneyExtraction.extraction_date < end))
            .group_by(MoneyExtraction.party)
        )
        if self._business_id is not None:
            stmt = stmt.where(MoneyExtraction.business_id == self._business_id)
        rows = self._db.execute(stmt).all()
        return {(party or ""): float(total or 0) for party, total in rows}

    def monthly_dividends_report(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        start, end = self._month_range(now_dt)
        summary, _items = self.monthly_profit_report(now=now_dt)
        extraction_totals = self.total_extractions_by_party(start=start, end=end)

        config = self._config()
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
        if self._business_id is not None and int(getattr(exp, "business_id", 0) or 0) != int(self._business_id):
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
        if self._business_id is not None and int(getattr(mv, "business_id", 0) or 0) != int(self._business_id):
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

    def list_expenses(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[OperatingExpense]:
        stmt = select(OperatingExpense)
        if self._business_id is not None:
            stmt = stmt.where(OperatingExpense.business_id == self._business_id)
        if start is not None:
            stmt = stmt.where(OperatingExpense.expense_date >= start)
        if end is not None:
            stmt = stmt.where(OperatingExpense.expense_date < end)
        stmt = stmt.order_by(OperatingExpense.expense_date.desc(), OperatingExpense.id.desc()).limit(limit)
        return list(self._db.scalars(stmt))

    def total_expenses(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> float:
        stmt = select(func.coalesce(func.sum(OperatingExpense.amount), 0))
        if self._business_id is not None:
            stmt = stmt.where(OperatingExpense.business_id == self._business_id)
        if start is not None:
            stmt = stmt.where(OperatingExpense.expense_date >= start)
        if end is not None:
            stmt = stmt.where(OperatingExpense.expense_date < end)
        return float(self._db.scalar(stmt) or 0)

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

    def sales_metrics_table(self, now: datetime, months: int = 12, location_id: Optional[int] = None) -> list[dict]:
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
                    and_(InventoryMovement.movement_date >= month_start, InventoryMovement.movement_date < range_end),
                    func.abs(InventoryMovement.quantity),
                ),
                else_=0,
            )
        )

        sales_month_expr = func.sum(
            case(
                (
                    and_(InventoryMovement.movement_date >= month_start, InventoryMovement.movement_date < range_end),
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
            .where(
                and_(
                    True if self._business_id is None else (InventoryLot.business_id == self._business_id),
                    True if location_id is None else (InventoryLot.location_id == location_id),
                )
            )
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
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    True if location_id is None else (InventoryMovement.location_id == location_id),
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

    def top_expense_concept(self, start: datetime, end: datetime) -> Optional[dict]:
        row = self._db.execute(
            select(
                OperatingExpense.concept,
                func.coalesce(func.sum(OperatingExpense.amount), 0).label("total"),
            )
            .where(
                and_(
                    OperatingExpense.expense_date >= start,
                    OperatingExpense.expense_date < end,
                    True if self._business_id is None else (OperatingExpense.business_id == self._business_id),
                )
            )
            .group_by(OperatingExpense.concept)
            .order_by(func.coalesce(func.sum(OperatingExpense.amount), 0).desc())
            .limit(1)
        ).first()

        if not row:
            return None

        concept, total = row
        return {"concept": concept, "total": float(total or 0)}

    def monthly_profit_report(self, now: Optional[datetime] = None, location_id: Optional[int] = None) -> tuple[dict, list[dict]]:
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

    def monthly_profit_items_report(
        self,
        now: Optional[datetime] = None,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> tuple[dict, list[dict]]:
        now_dt = now or datetime.now(timezone.utc)
        if start is None or end is None:
            month_start, month_end = self._month_range(now_dt)
            start = month_start if start is None else start
            end = month_end if end is None else end

        src_mv = aliased(InventoryMovement)

        rows = self._db.execute(
            select(
                InventoryMovement.id.label("sale_movement_id"),
                InventoryMovement.movement_date,
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
            .join(Product, Product.id == InventoryMovement.product_id)
            .join(InventoryLot, InventoryLot.id == MovementAllocation.lot_id)
            .join(src_mv, src_mv.id == InventoryLot.movement_id)
            .where(
                and_(
                    InventoryMovement.type == "sale",
                    InventoryMovement.movement_date >= start,
                    InventoryMovement.movement_date < end,
                    True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                )
            )
            .order_by(InventoryMovement.movement_date.desc(), InventoryMovement.id.desc())
        ).all()

        items: list[dict] = []
        qty_total = 0.0
        sales_total = 0.0
        cogs_total = 0.0
        profit_total = 0.0

        for sale_movement_id, movement_date, sku, name, category, lot_id, source_movement_id, source_movement_type, lot_code, unit_cost, unit_price, qty in rows:
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

    def monthly_overview(self, months: int = 12, now: Optional[datetime] = None, location_id: Optional[int] = None) -> list[dict]:
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

        range_end = month_start
        range_days = max(1, int((range_end - range_start).days))

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
                    InventoryMovement.movement_date >= range_start,
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
                    InventoryMovement.movement_date >= range_start,
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
                    InventoryMovement.movement_date >= range_start,
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
        selected_code = str(payload.location_code or "").strip() or str(cfg.locations.default_pos)
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

    def stock_list(self, query: str = "", location_code: Optional[str] = None) -> list[StockRead]:
        loc_id = None
        if location_code:
            loc_id = self._location_id_for_code(location_code)
        base_rows = list(self._inventory.stock_list(query=query, location_id=loc_id))
        skus = [sku for sku, *_ in base_rows if sku]

        avg_daily_by_sku: dict[str, float] = {}
        if skus:
            now_dt = datetime.now(timezone.utc)
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

        out: list[StockRead] = []
        for sku, name, category, uom, qty, min_stock, lead_time_days, min_purchase_cost, default_purchase_cost, default_sale_price in base_rows:
            avg_daily = float(avg_daily_by_sku.get(str(sku), 0.0))
            reorder_in_days: Optional[int] = None
            if avg_daily > 0:
                days_cover = float(qty or 0) / avg_daily
                reorder_in_days = max(0, int(days_cover - float(lead_time_days or 0)))

            out.append(
                StockRead(
                    sku=sku,
                    name=name,
                    category=category,
                    unit_of_measure=uom or None,
                    quantity=qty,
                    min_stock=min_stock,
                    needs_restock=min_stock > 0 and qty < min_stock,
                    lead_time_days=int(lead_time_days or 0),
                    avg_daily_sales=avg_daily,
                    reorder_in_days=reorder_in_days,
                    min_purchase_cost=min_purchase_cost,
                    default_purchase_cost=default_purchase_cost,
                    default_sale_price=default_sale_price,
                )
            )

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
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[tuple]:
        return self._inventory.movement_history(
            sku=sku,
            query=query,
            movement_type=movement_type,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
