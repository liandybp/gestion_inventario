"""Finanzas: gastos, extracciones y dividendos (extraído de InventoryService, H-023)."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models import (
    InventoryMovement,
    MoneyExtraction,
    MovementAllocation,
    OperatingExpense,
)
from app.repositories.business_repository import get_business_config
from app.utils import month_range as utils_month_range
from fastapi import HTTPException


class FinanceService:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _month_range(self, now: datetime):
        return utils_month_range(now)

    def _config(self):
        return get_business_config(self._db, self._business_id)


    def create_expense(self, amount: float, concept: str, expense_date: Optional[datetime]) -> None:
        exp = OperatingExpense(
            business_id=self._business_id,
            amount=float(amount),
            concept=concept.strip(),
            expense_date=self._movement_datetime(expense_date),
        )
        self._db.add(exp)
        self._db.commit()


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


    def total_extractions_by_party(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> dict[str, float]:
        stmt = select(
            MoneyExtraction.party,
            func.coalesce(func.sum(MoneyExtraction.amount), 0).label("total"),
        ).group_by(MoneyExtraction.party)
        if self._business_id is not None:
            stmt = stmt.where(MoneyExtraction.business_id == self._business_id)
        if start is not None:
            stmt = stmt.where(MoneyExtraction.extraction_date >= start)
        if end is not None:
            stmt = stmt.where(MoneyExtraction.extraction_date < end)
        rows = self._db.execute(stmt).all()
        return {(party or ""): float(total or 0) for party, total in rows}


    def monthly_dividends_report(
        self,
        now: Optional[datetime] = None,
        *,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)
        period_start: datetime
        period_end: datetime
        if start is None or end is None:
            month_start, month_end = self._month_range(now_dt)
            period_start = month_start if start is None else start
            period_end = month_end if end is None else end
        else:
            period_start = start
            period_end = end

        # The accumulated pending should be calculated as-of the period end when
        # the user is looking at a past month/range; for the current month/range
        # it should reflect up to "now".
        as_of_dt = period_end
        if as_of_dt.tzinfo is None:
            as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)
        as_of_dt = as_of_dt.astimezone(timezone.utc)
        if as_of_dt > now_dt:
            as_of_dt = now_dt

        config = self._config()
        business_label = (config.dividends.business_label or "Negocio").strip() or "Negocio"
        partners = [p.strip() for p in (config.dividends.partners or []) if (p or "").strip()]

        opening_as_of_raw = str(getattr(config.dividends, "opening_pending_as_of", "") or "").strip()
        opening_as_of_dt: Optional[datetime] = None
        if opening_as_of_raw:
            try:
                opening_as_of_dt = datetime.fromisoformat(opening_as_of_raw)
                if opening_as_of_dt.tzinfo is None:
                    opening_as_of_dt = opening_as_of_dt.replace(tzinfo=timezone.utc)
                opening_as_of_dt = opening_as_of_dt.astimezone(timezone.utc)
            except Exception:
                opening_as_of_dt = None

        # Period metrics (month by default, or user-provided range).
        sales_total = float(
            self._db.scalar(
                select(
                    func.coalesce(
                        func.sum(
                            func.abs(InventoryMovement.quantity)
                            * func.coalesce(InventoryMovement.unit_price, 0)
                        ),
                        0,
                    )
                ).where(
                    and_(
                        InventoryMovement.type == "sale",
                        InventoryMovement.movement_date >= period_start,
                        InventoryMovement.movement_date < period_end,
                        True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    )
                )
            )
            or 0
        )
        cogs_total = float(
            self._db.scalar(
                select(func.coalesce(func.sum(MovementAllocation.quantity * MovementAllocation.unit_cost), 0))
                .select_from(MovementAllocation)
                .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
                .where(
                    and_(
                        InventoryMovement.type == "sale",
                        InventoryMovement.movement_date >= period_start,
                        InventoryMovement.movement_date < period_end,
                        True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
                    )
                )
            )
            or 0
        )
        expenses_total = float(self.total_expenses(start=period_start, end=period_end) or 0)
        gross_total = float(sales_total - cogs_total)
        net_total = float(gross_total - expenses_total)
        share_each = (net_total / float(len(partners))) if partners else 0.0

        extraction_totals_period = self.total_extractions_by_party(start=period_start, end=period_end)

        # Accumulated pending: opening_pending + totals since opening_pending_as_of (or all time if not set).
        cum_filters = [
            InventoryMovement.type == "sale",
            InventoryMovement.movement_date < as_of_dt,
            True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
        ]
        if opening_as_of_dt is not None:
            cum_filters.append(InventoryMovement.movement_date >= opening_as_of_dt)

        cum_sales_total = float(
            self._db.scalar(
                select(
                    func.coalesce(
                        func.sum(
                            func.abs(InventoryMovement.quantity)
                            * func.coalesce(InventoryMovement.unit_price, 0)
                        ),
                        0,
                    )
                ).where(and_(*cum_filters))
            )
            or 0
        )

        cum_cogs_filters = [
            InventoryMovement.type == "sale",
            InventoryMovement.movement_date < as_of_dt,
            True if self._business_id is None else (InventoryMovement.business_id == self._business_id),
        ]
        if opening_as_of_dt is not None:
            cum_cogs_filters.append(InventoryMovement.movement_date >= opening_as_of_dt)

        cum_cogs_total = float(
            self._db.scalar(
                select(func.coalesce(func.sum(MovementAllocation.quantity * MovementAllocation.unit_cost), 0))
                .select_from(MovementAllocation)
                .join(InventoryMovement, InventoryMovement.id == MovementAllocation.movement_id)
                .where(
                    and_(*cum_cogs_filters)
                )
            )
            or 0
        )

        cum_expenses_start = opening_as_of_dt
        cum_expenses_total = float(self.total_expenses(start=cum_expenses_start, end=as_of_dt) or 0)
        cum_net_total = float((cum_sales_total - cum_cogs_total) - cum_expenses_total)
        cum_share_each = (cum_net_total / float(len(partners))) if partners else 0.0

        extraction_totals_all = self.total_extractions_by_party(start=opening_as_of_dt, end=as_of_dt)

        opening_pending = getattr(config.dividends, "opening_pending", None) or {}
        opening_map: dict[str, float] = {}
        if isinstance(opening_pending, dict):
            for k, v in opening_pending.items():
                key = (str(k) or "").strip()
                if not key:
                    continue
                try:
                    opening_map[key] = float(v)
                except Exception:
                    continue

        extractions: dict[str, float] = {business_label: float(extraction_totals_period.get(business_label, 0) or 0)}
        pending: dict[str, float] = {}
        business_ext_all = float(extraction_totals_all.get(business_label, 0) or 0)
        pending[business_label] = float(opening_map.get(business_label, 0) or 0) + float(
            (cum_cogs_total + cum_expenses_total) - business_ext_all
        )

        for p in partners:
            extractions[p] = float(extraction_totals_period.get(p, 0) or 0)
            p_ext_all = float(extraction_totals_all.get(p, 0) or 0)
            pending[p] = float(opening_map.get(p, 0) or 0) + float(cum_share_each - p_ext_all)

        return {
            "month_start": period_start,
            "month_end": period_end,
            "cogs_total": cogs_total,
            "expenses_total": expenses_total,
            "net_total": net_total,
            "share_each": share_each,
            "extractions": extractions,
            "pending": pending,
        }

