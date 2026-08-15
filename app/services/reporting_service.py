from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import InventoryMovement, OperatingExpense


class ReportingService:
    """Series temporales mensuales para reportes (gastos y compras por mes)."""

    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

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
