from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.models import CashBaseline, InventoryMovement, MoneyExtraction, OperatingExpense
from app.services.coach_rules import evaluate
from app.services.finance_service import FinanceService
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService
from app.services.sales_document_service import SalesDocumentService

_log = logging.getLogger(__name__)


class CoachService:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)
        self._reporting = ReportingService(db, self._business_id)
        self._finance = FinanceService(db, self._business_id)
        self._inventory = InventoryService(db, self._business_id)
        self._sales_documents = SalesDocumentService(db, self._business_id)
        self._indicators_cache: Optional[dict] = None

    def _financial_indicators(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            summary, _items = self._reporting.monthly_profit_report(now_dt)
            overview = self._reporting.monthly_overview(months=6, now=now_dt)
            margin_values = [
                (row["gross_profit"] / row["sales"] * 100.0)
                for row in overview if row.get("sales", 0) > 0
            ]
            margin_avg_6m = sum(margin_values) / len(margin_values) if margin_values else None

            return {
                "margin_pct": summary["gross_margin_pct"],
                "margin_avg_6m": margin_avg_6m,
                "expense_ratio": (summary["expenses_total"] / summary["sales_total"]) if summary["sales_total"] > 0 else None,
                "_summary": summary,
            }
        except Exception as e:
            _log.error(f"Error en _financial_indicators: {e}", exc_info=True)
            return {"margin_pct": None, "margin_avg_6m": None, "expense_ratio": None, "_summary": {}}

    def _add_growth_and_extraction(self, financial: dict, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            overview = self._reporting.monthly_overview(months=4, now=now_dt)
            if len(overview) >= 4:
                recent = overview[-1]["sales"]
                prior_avg = sum(r["sales"] for r in overview[-4:-1]) / 3
                growth_pct = ((recent - prior_avg) / prior_avg * 100.0) if prior_avg > 0 else None
            else:
                growth_pct = None

            dividends = self._finance.monthly_dividends_report(now_dt)
            extractions_total = sum(
                float(v or 0) for v in (dividends.get("extractions") or {}).values()
            )
            net_total = financial["_summary"].get("net_total", 0)
            extraction_ratio = (extractions_total / net_total) if net_total > 0 else None

            financial.update({"sales_growth_pct": growth_pct, "extraction_ratio": extraction_ratio})
            return financial
        except Exception as e:
            _log.error(f"Error en _add_growth_and_extraction: {e}", exc_info=True)
            return financial

    def current_cash(self, now: Optional[datetime] = None) -> dict:
        try:
            baseline = self._db.execute(
                select(CashBaseline).where(CashBaseline.business_id == self._business_id)
                .order_by(CashBaseline.baseline_date.desc()).limit(1)
            ).scalar_one_or_none()
            if baseline is None:
                return {"amount": None, "mode": "relative"}

            since = baseline.baseline_date
            # 4 subqueries escalares en un único SELECT -> 1 round-trip a BD en vez de 4
            # (H-003: antes eran 4 llamadas .scalar() separadas).
            sales_subq = (
                select(func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_price), 0))
                .where(and_(InventoryMovement.business_id == self._business_id,
                            InventoryMovement.type == "sale", InventoryMovement.movement_date >= since))
                .scalar_subquery()
            )
            purchases_subq = (
                select(func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_cost), 0))
                .where(and_(InventoryMovement.business_id == self._business_id,
                            InventoryMovement.type == "purchase", InventoryMovement.movement_date >= since))
                .scalar_subquery()
            )
            expenses_subq = (
                select(func.coalesce(func.sum(OperatingExpense.amount), 0))
                .where(and_(OperatingExpense.business_id == self._business_id,
                            OperatingExpense.expense_date >= since))
                .scalar_subquery()
            )
            extractions_subq = (
                select(func.coalesce(func.sum(MoneyExtraction.amount), 0))
                .where(and_(MoneyExtraction.business_id == self._business_id,
                            MoneyExtraction.extraction_date >= since))
                .scalar_subquery()
            )
            sales_in, purchases_out, expenses_out, extractions_out = self._db.execute(
                select(sales_subq, purchases_subq, expenses_subq, extractions_subq)
            ).one()

            amount = float(baseline.amount) + float(sales_in) - float(purchases_out) - float(expenses_out) - float(extractions_out)
            return {"amount": amount, "mode": "real"}
        except Exception as e:
            _log.error(f"Error en current_cash: {e}", exc_info=True)
            return {"amount": None, "mode": "relative"}

    def _operational_indicators(self, financial: dict, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            metrics = self._reporting.sales_metrics_table(now_dt)
            total_products = len(metrics)
            under_rop = sum(1 for m in metrics if float(m.get("reorder_shortage") or 0) > 0)
            restock_risk_ratio = (under_rop / total_products) if total_products > 0 else None

            cogs_total = financial["_summary"].get("cogs_total", 0)
            turnover_days = self._inventory.inventory_turnover_days(cogs_total)

            dead_items = self._inventory.dead_stock(days=90, now=now_dt)
            dead_value = sum(i["value"] for i in dead_items)

            return {"restock_risk_ratio": restock_risk_ratio, "turnover_days": turnover_days,
                    "dead_stock_value": dead_value, "dead_stock_items": dead_items}
        except Exception as e:
            _log.error(f"Error en _operational_indicators: {e}", exc_info=True)
            return {"restock_risk_ratio": None, "turnover_days": None, "dead_stock_value": 0, "dead_stock_items": []}

    def _customer_indicators(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            # Concentración: numerador y denominador usan la MISMA ventana (mes actual).
            # Mezclar "top cliente del mes" / "total histórico" deflacta el ratio y la
            # alerta de concentración nunca dispara para negocios con historial largo.
            month_customers = self._sales_documents.sales_by_customer(start=month_start, end=now_dt, limit=10_000)
            all_customers = self._sales_documents.sales_by_customer(start=None, end=now_dt, limit=10_000)

            month_total = sum(r.get("total", 0) for r in month_customers)
            top = month_customers[0] if month_customers else None
            concentration = (top.get("total", 0) / month_total) if (top and month_total > 0) else None

            recurring = sum(1 for c in all_customers if c.get("doc_count", 0) >= 2)
            recurring_pct = (recurring / len(all_customers) * 100.0) if all_customers else None

            churn_cutoff = now_dt - timedelta(days=60)
            recent_customers = {
                c.get("client_name") for c in
                self._sales_documents.sales_by_customer(start=churn_cutoff, end=now_dt, limit=10_000)
            }
            churned = [c.get("client_name") for c in all_customers
                       if c.get("doc_count", 0) >= 2 and c.get("client_name") not in recent_customers]

            return {"customer_concentration": concentration, "top_customer_name": top.get("client_name") if top else None,
                    "recurring_customers_pct": recurring_pct, "churned_customers": churned[:10]}
        except Exception as e:
            _log.error(f"Error en _customer_indicators: {e}", exc_info=True)
            return {"customer_concentration": None, "top_customer_name": None, "recurring_customers_pct": None, "churned_customers": []}

    def _compute_indicators(self, now: Optional[datetime] = None) -> dict:
        """Calcula financial/operational/customers/cash una sola vez por instancia.

        CoachService se instancia por request (ver ui_coach.py); cachear aquí evita
        recalcular el mismo pipeline dos veces cuando health_score() y recommendations()
        se llaman en la misma petición (antes eran ~10-12 queries duplicadas).
        """
        if self._indicators_cache is not None:
            return self._indicators_cache

        now_dt = now or datetime.now(timezone.utc)
        financial = self._financial_indicators(now_dt)
        cash = self.current_cash(now_dt)
        no_data = financial.get("_summary", {}).get("sales_total", 0) <= 0
        if no_data:
            computed = {"_no_data": True, "financial": financial, "operational": {}, "customers": {}, "cash": cash}
            self._indicators_cache = computed
            return computed

        financial = self._add_growth_and_extraction(financial, now_dt)
        operational = self._operational_indicators(financial, now_dt)
        customers = self._customer_indicators(now_dt)
        computed = {"_no_data": False, "financial": financial, "operational": operational,
                    "customers": customers, "cash": cash}
        self._indicators_cache = computed
        return computed

    def health_score(self, now: Optional[datetime] = None) -> dict:
        computed = self._compute_indicators(now)
        financial, operational = computed["financial"], computed["operational"]
        customers, cash = computed["customers"], computed["cash"]

        if computed["_no_data"]:
            return {"score": None, "level": "unknown", "message": "Sin datos suficientes este mes",
                    "whats_good": [], "financial": financial, "operational": operational,
                    "customers": customers, "cash": cash}

        score = 5.0
        score += 1.0 if (financial.get("margin_pct") is not None and financial["margin_pct"] > 0) else -1.0
        growth = financial.get("sales_growth_pct")
        score += 1.0 if growth is not None and growth > 0 else (-1.0 if growth is not None and growth < -10 else 0.0)
        extractions = financial.get("extraction_ratio")
        if extractions is not None:
            score -= min(2.0, extractions / 0.25)
        dead_value = operational.get("dead_stock_value", 0)
        score -= min(1.0, dead_value / 1000.0)
        score = max(1.0, min(10.0, score))

        whats_good = []
        if financial.get("margin_pct") is not None and financial["margin_pct"] > 0:
            whats_good.append(f"Margen bruto positivo ({financial['margin_pct']:.1f}%)")
        if growth is not None and growth > 0:
            whats_good.append(f"Ventas creciendo ({growth:+.1f}%)")
        if operational.get("dead_stock_value", 0) == 0:
            whats_good.append("Sin stock muerto a 90 días")
        if not whats_good:
            whats_good = ["Datos cargados correctamente"]

        level = "green" if score >= 7.0 else ("yellow" if score >= 4.5 else "red")

        return {
            "score": round(score, 1),
            "level": level,
            "whats_good": whats_good,
            "financial": financial,
            "operational": operational,
            "customers": customers,
            "cash": cash,
        }

    def recommendations(self, now: Optional[datetime] = None) -> list[dict]:
        computed = self._compute_indicators(now)
        if computed["_no_data"]:
            return []

        financial, operational = computed["financial"], computed["operational"]
        customers, cash = computed["customers"], computed["cash"]
        indicators = {**financial, **operational, **customers, **cash}
        recommendations = evaluate(indicators)
        suppressed = self._dismissed_or_done_keys()
        return [r for r in recommendations if r["rule_key"] not in suppressed]

    def mark_action(self, rule_key: str, status: str, user_id: int) -> None:
        from app.models import CoachActionLog
        try:
            row = CoachActionLog(business_id=self._business_id, rule_key=rule_key,
                                 status=status, marked_by=user_id, marked_at=datetime.now(timezone.utc))
            self._db.add(row)
            self._db.commit()
        except Exception as e:
            _log.error(f"Error en mark_action: {e}", exc_info=True)
            self._db.rollback()

    def _dismissed_or_done_keys(self) -> set[str]:
        from app.models import CoachActionLog
        rows = self._db.execute(
            select(CoachActionLog.rule_key).where(
                and_(CoachActionLog.business_id == self._business_id,
                     CoachActionLog.status.in_(("dismissed", "done"))))
        ).all()
        return {r[0] for r in rows}
