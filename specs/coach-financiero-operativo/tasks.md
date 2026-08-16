# Task Envelopes — coach-financiero-operativo (24 envelopes: T1–T19, con T4a/T4b, T6a/T6b/T6c, T15a/T15b, T18a/T18b)

> Formato: `Templates/template-task-envelope.md`. Una tarea a la vez, en orden. Cada envelope es
> autocontenido — OpenCode no lee este documento completo, solo el envelope activo.
> Validado contra el checklist de `pae-estructura.md` el 2026-08-16 (auditoría con conteo real
> de tokens): T6b se dividió en T6b/T6c (926→~460+470 tokens), T15 se dividió en T15a/T15b
> (3→1+2 archivos), T18 se dividió en T18a/T18b (4→2+2 archivos). Todos los envelopes ≤800
> tokens de Instructions y ≤2 archivos (T19 exento por ser operación git mecánica, ver su nota).

---

### T1: Modelo `CashBaseline`

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/models.py`, `tests/unit/test_coach_models.py`
**Pillars:** P1, P4, P5, P6

#### Context (≤200 tokens)
```python
# app/models.py — YA EXISTE (última clase del archivo)
class ChatMessage(Base):
    __tablename__ = "chat_messages"
    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(ForeignKey("businesses.id"), nullable=True, index=True)
    ...
# Imports ya presentes:
# from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, func
# from sqlalchemy.orm import Mapped, mapped_column
# from app.db import Base
```

#### Objective (≤15 tokens)
Crear tabla `CashBaseline` para saldo inicial de caja.

#### Instructions (≤400 tokens)
BEFORE: nada después de `ChatMessage`.

AFTER: añadir al final de `app/models.py`:
```python


class CashBaseline(Base):
    """Saldo inicial de caja; se usa el más reciente por negocio."""
    __tablename__ = "cash_baselines"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    baseline_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
```

Crear `tests/unit/test_coach_models.py`:
```python
from datetime import datetime, timezone
from app.models import CashBaseline

def test_cashbaseline_creates(db_session, seeded_business):
    row = CashBaseline(business_id=seeded_business.id, baseline_date=datetime.now(timezone.utc), amount=1000.0)
    db_session.add(row)
    db_session.commit()
    assert row.id is not None
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: pytest tests/unit/test_coach_models.py::test_cashbaseline_creates -v
```

#### Expected Report
- [ ] `CashBaseline` en `app/models.py`
- [ ] `test_coach_models.py` creado
- [ ] Test PASS, 149 tests previos PASS

---

### T2: Modelo `CoachActionLog` [A]

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/models.py`, `tests/unit/test_coach_models.py`
**Pillars:** P1, P4, P5, P6

#### Context (≤200 tokens)
```python
# app/models.py — CashBaseline YA EXISTE (T1):
class CashBaseline(Base):
    __tablename__ = "cash_baselines"
    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    ...
```

#### Objective
Crear tabla `CoachActionLog` para círculo cerrado.

#### Instructions (≤400 tokens)
AFTER `CashBaseline`:
```python


class CoachActionLog(Base):
    """Tracking de recomendaciones: abierta/hecha/descartada."""
    __tablename__ = "coach_action_logs"
    __table_args__ = (Index("ix_coach_action_business_rule", "business_id", "rule_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    rule_key: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", server_default="open")
    impact_estimate: Mapped[Optional[float]] = mapped_column(Numeric(14, 4, asdecimal=False), nullable=True)
    marked_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    marked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_result: Mapped[Optional[float]] = mapped_column(Numeric(14, 4, asdecimal=False), nullable=True)
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
```

Añadir a `tests/unit/test_coach_models.py`:
```python
from app.models import CoachActionLog

def test_coachactionlog_creates(db_session, seeded_business):
    row = CoachActionLog(business_id=seeded_business.id, rule_key="margin_drop", status="open")
    db_session.add(row)
    db_session.commit()
    assert row.status == "open"
```

#### Verification
```bash
pytest tests/unit/test_coach_models.py -v
```

#### Expected Report
- [ ] `CoachActionLog` en `app/models.py`
- [ ] 2 tests en `test_coach_models.py` PASS

---

### T3: Schemas `CashBaselineCreate` + `CoachActionUpdate`

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/schemas.py`, `tests/unit/test_coach_schemas.py`
**Pillars:** P1, P5, P6

#### Context (≤200 tokens)
```python
# app/schemas.py — patrón real:
from pydantic import BaseModel
class ProductCreate(BaseModel):
    sku: Optional[str] = None
    name: str
```

#### Objective
Schemas Pydantic con validación para caja y acciones.

#### Instructions (≤400 tokens)
AFTER `app/schemas.py`:
```python
from datetime import datetime, timezone
from pydantic import field_validator

class CashBaselineCreate(BaseModel):
    baseline_date: datetime
    amount: float

    @field_validator("amount")
    @classmethod
    def amount_not_negative(cls, v: float) -> float:
        if v < 0:
            raise ValueError("amount must be >= 0")
        return v

    @field_validator("baseline_date")
    @classmethod
    def date_not_future(cls, v: datetime) -> datetime:
        now = datetime.now(timezone.utc)
        v_cmp = v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)
        if v_cmp > now:
            raise ValueError("baseline_date cannot be in the future")
        return v

class CoachActionUpdate(BaseModel):
    rule_key: str
    status: str

    @field_validator("status")
    @classmethod
    def status_whitelist(cls, v: str) -> str:
        if v not in ("done", "dismissed"):
            raise ValueError("status must be 'done' or 'dismissed'")
        return v
```

Crear `tests/unit/test_coach_schemas.py`:
```python
from datetime import datetime, timedelta, timezone
import pytest
from pydantic import ValidationError
from app.schemas import CashBaselineCreate, CoachActionUpdate

def test_cashbaseline_rejects_negative():
    with pytest.raises(ValidationError):
        CashBaselineCreate(baseline_date=datetime.now(timezone.utc), amount=-1)

def test_coachaction_accepts_done():
    obj = CoachActionUpdate(rule_key="margin_drop", status="done")
    assert obj.status == "done"
```

#### Verification
```bash
pytest tests/unit/test_coach_schemas.py -v
```

#### Expected Report
- [ ] 2 schemas en `app/schemas.py`
- [ ] 2 tests PASS en `test_coach_schemas.py`

---

### T4a: `coach_rules.py` — reglas financieras básicas + logging

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_rules.py`, `tests/unit/test_coach_rules.py`
**Pillars:** P1, P2, P4, P5, P6

#### Context (≤200 tokens)
Sin dependencias externas — se crea desde cero.

#### Objective
Motor de reglas: umbrales → reglas financieras básicas con logging.

#### Instructions (≤400 tokens)
Crear `app/services/coach_rules.py`:
```python
from __future__ import annotations
import logging
from typing import Optional

_log = logging.getLogger(__name__)

MARGIN_DROP_YELLOW_PP = 3.0
MARGIN_DROP_RED_PP = 6.0
EXPENSE_RATIO_YELLOW = 0.30
EXPENSE_RATIO_RED = 0.45
EXTRACTION_RATIO_YELLOW = 0.80
EXTRACTION_RATIO_RED = 1.0

def evaluate(indicators: dict) -> list[dict]:
    """Evalúa indicadores contra reglas. Log errores, nunca alucina."""
    try:
        checks = [_rule_margin, _rule_expense_ratio, _rule_extraction_ratio]
        recs = [r(indicators) for r in checks]
        return [r for r in recs if r is not None]
    except Exception as e:
        _log.error(f"Error en evaluate: {e}", exc_info=True)
        return []

def _rule_margin(ind: dict) -> Optional[dict]:
    current, avg6 = ind.get("margin_pct"), ind.get("margin_avg_6m")
    if current is None or avg6 is None:
        return None
    drop = avg6 - current
    if drop < MARGIN_DROP_YELLOW_PP:
        return None
    severity = "red" if drop >= MARGIN_DROP_RED_PP else "yellow"
    return {"rule_key": "margin_drop", "severity": severity,
            "symptom": f"Tu margen bajó a {current:.1f}%", "cause": "revisa costos de compra",
            "action": "renegocia con proveedor", "impact_amount": None, "impact_text": "revisa lista de precios"}

def _rule_expense_ratio(ind: dict) -> Optional[dict]:
    ratio = ind.get("expense_ratio")
    if ratio is None or ratio < EXPENSE_RATIO_YELLOW:
        return None
    severity = "red" if ratio >= EXPENSE_RATIO_RED else "yellow"
    return {"rule_key": "expense_ratio_high", "severity": severity,
            "symptom": f"Gastos {ratio * 100:.0f}% de ventas", "cause": "gasto creciendo rápido",
            "action": "revisa tu gasto más alto", "impact_amount": None, "impact_text": "audit de gastos"}

def _rule_extraction_ratio(ind: dict) -> Optional[dict]:
    ratio = ind.get("extraction_ratio")
    if ratio is None or ratio < EXTRACTION_RATIO_YELLOW:
        return None
    severity = "red" if ratio >= EXTRACTION_RATIO_RED else "yellow"
    return {"rule_key": "extraction_ratio_high", "severity": severity,
            "symptom": f"Extracciones {ratio * 100:.0f}%", "cause": "drena caja",
            "action": "reduce extracciones", "impact_amount": None, "impact_text": "compare vs utilidad"}
```

Crear `tests/unit/test_coach_rules.py`:
```python
from app.services.coach_rules import evaluate

def test_margin_drop_yellow():
    recs = evaluate({"margin_pct": 15.0, "margin_avg_6m": 18.0})
    assert any(r["rule_key"] == "margin_drop" and r["severity"] == "yellow" for r in recs)

def test_margin_drop_red():
    recs = evaluate({"margin_pct": 12.0, "margin_avg_6m": 18.0})
    assert any(r["rule_key"] == "margin_drop" and r["severity"] == "red" for r in recs)

def test_evaluate_catches_errors():
    recs = evaluate(None)  # Error pero no alucina
    assert recs == []
```

#### Verification
```bash
pytest tests/unit/test_coach_rules.py -v
```

#### Expected Report
- [ ] `coach_rules.py` con 3 reglas + logging
- [ ] 3 tests PASS

---

### T4b: `coach_rules.py` — reglas operativas + impacto

**Depends on:** T4a
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_rules.py`, `tests/unit/test_coach_rules.py`
**Pillars:** P1, P4, P5

#### Context (≤200 tokens)
```python
# app/services/coach_rules.py — YA EXISTE (T4a):
def evaluate(indicators: dict) -> list[dict]: ...
def _rule_margin(...): ...
def _rule_expense_ratio(...): ...
def _rule_extraction_ratio(...): ...
```

#### Objective
Añadir reglas operativas (stock muerto, concentración) y estimadores de impacto.

#### Instructions (≤400 tokens)
AFTER existing rules en `coach_rules.py`:
```python
CUSTOMER_CONCENTRATION_YELLOW = 0.30
CUSTOMER_CONCENTRATION_RED = 0.50

# Modificar evaluate() para incluir:
def evaluate(indicators: dict) -> list[dict]:
    try:
        checks = [_rule_margin, _rule_expense_ratio, _rule_extraction_ratio,
                  _rule_dead_stock, _rule_customer_concentration]
        recs = [r(indicators) for r in checks]
        return [r for r in recs if r is not None]
    except Exception as e:
        _log.error(f"Error en evaluate: {e}", exc_info=True)
        return []

def _rule_dead_stock(ind: dict) -> Optional[dict]:
    value = ind.get("dead_stock_value") or 0.0
    items = ind.get("dead_stock_items") or []
    if value <= 0 or not items:
        return None
    names = ", ".join(i["name"] for i in items[:3])
    return {"rule_key": "dead_stock", "severity": "yellow",
            "symptom": f"${value:,.0f} sin vender 90 días", "cause": f"stock parado en: {names}",
            "action": "promo o liquidación", "impact_amount": value, "impact_text": f"Recuperarías ~${value:,.0f}"}

def _rule_customer_concentration(ind: dict) -> Optional[dict]:
    share = ind.get("customer_concentration")
    if share is None or share < CUSTOMER_CONCENTRATION_YELLOW:
        return None
    severity = "red" if share >= CUSTOMER_CONCENTRATION_RED else "yellow"
    name = ind.get("top_customer_name") or "cliente principal"
    return {"rule_key": "customer_concentration", "severity": severity,
            "symptom": f"{name} {share * 100:.0f}% ventas", "cause": "dependencia riesgosa",
            "action": "diversifica clientes", "impact_amount": None, "impact_text": "riesgo si cliente se va"}
```

Añadir a `tests/unit/test_coach_rules.py`:
```python
def test_dead_stock_impact():
    recs = evaluate({"dead_stock_value": 1200.0, "dead_stock_items": [{"sku": "A", "name": "Bujía", "value": 1200.0}]})
    rec = next(r for r in recs if r["rule_key"] == "dead_stock")
    assert rec["impact_amount"] == 1200.0

def test_customer_concentration():
    recs = evaluate({"customer_concentration": 0.40, "top_customer_name": "Acme"})
    assert any(r["rule_key"] == "customer_concentration" for r in recs)
```

#### Verification
```bash
pytest tests/unit/test_coach_rules.py -v
```

#### Expected Report
- [ ] 2 reglas operativas + estimadores en `coach_rules.py`
- [ ] 2 tests PASS

---

### T5: `InventoryService.dead_stock` + `inventory_turnover_days`

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/inventory_service.py`, `tests/unit/test_inventory_service_coach.py`
**Pillars:** P1, P4, P8

#### Context (≤200 tokens)
```python
# app/services/inventory_service.py — YA EXISTE
class InventoryService:
    def __init__(self, db, business_id=None):
        self._db = db
        self._business_id = int(business_id)
    def inventory_value_total(self, location_id=None) -> float: ...
# Imports presentes: from sqlalchemy import and_, func, select
# Modelos: InventoryLot (qty_remaining, unit_cost, received_at),
#          InventoryMovement (type, movement_date, product_id, business_id)
```

#### Objective
Métodos read-only para stock muerto y días de rotación.

#### Instructions (≤400 tokens)
AFTER `inventory_value_total()` en `InventoryService`:
```python
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
```

Crear `tests/unit/test_inventory_service_coach.py`:
```python
from app.services.inventory_service import InventoryService

def test_turnover_days_none_when_no_cogs(db_session, seeded_business):
    svc = InventoryService(db_session, seeded_business.id)
    assert svc.inventory_turnover_days(0) is None
```

#### Verification
```bash
pytest tests/unit/test_inventory_service_coach.py -v
```

#### Expected Report
- [ ] 2 métodos en `InventoryService`
- [ ] Test PASS

---

### T6a: `CoachService` — indicadores financieros básicos + logging

**Depends on:** T4a
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P2, P4

#### Context (≤200 tokens)
```python
# app/services/reporting_service.py — YA EXISTE
class ReportingService:
    def monthly_profit_report(self, now=None, ...) -> tuple[dict, list[dict]]
        # summary: sales_total, cogs_total, gross_total, expenses_total, net_total, gross_margin_pct
    def monthly_overview(self, months=12, now=None) -> list[dict]

# app/services/finance_service.py — YA EXISTE
class FinanceService:
    def monthly_dividends_report(self, now=None, ...) -> dict
```

#### Objective
`CoachService` con indicadores financieros básicos y logging de errores.

#### Instructions (≤400 tokens)
Crear `app/services/coach_service.py`:
```python
from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from app.services.finance_service import FinanceService
from app.services.reporting_service import ReportingService

_log = logging.getLogger(__name__)

class CoachService:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)
        self._reporting = ReportingService(db, self._business_id)
        self._finance = FinanceService(db, self._business_id)

    def _financial_indicators(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            summary, _items = self._reporting.monthly_profit_report(now_dt)
            overview = self._reporting.monthly_overview(months=6, now=now_dt)
            margin_values = [
                (row["gross_total"] / row["sales_total"] * 100.0)
                for row in overview if row.get("sales_total", 0) > 0
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
```

Crear `tests/unit/test_coach_service.py`:
```python
from app.services.coach_service import CoachService

def test_financial_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    ind = svc._financial_indicators()
    assert "margin_pct" in ind
    assert "expense_ratio" in ind
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_financial_indicators_shape -v
```

#### Expected Report
- [ ] `CoachService` creado con `_financial_indicators()`
- [ ] Test PASS

---

### T6b: `CoachService` — crecimiento y ratio de extracciones

**Depends on:** T6a
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/services/coach_service.py — YA EXISTE (T6a):
class CoachService:
    def __init__(self, db, business_id): ...
    def _financial_indicators(self, now=None) -> dict: ...
# app/services/reporting_service.py — monthly_overview(months, now) -> list[dict]
# app/services/finance_service.py — monthly_dividends_report(now) -> dict
```

#### Objective
Añadir crecimiento de ventas y ratio de extracciones/utilidad.

#### Instructions (≤400 tokens)
AFTER `_financial_indicators()` en `CoachService`:
```python
    def _add_growth_and_extraction(self, financial: dict, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            overview = self._reporting.monthly_overview(months=4, now=now_dt)
            if len(overview) >= 4:
                recent = overview[-1]["sales_total"]
                prior_avg = sum(r["sales_total"] for r in overview[-4:-1]) / 3
                growth_pct = ((recent - prior_avg) / prior_avg * 100.0) if prior_avg > 0 else None
            else:
                growth_pct = None

            dividends = self._finance.monthly_dividends_report(now_dt)
            extractions_total = float(dividends.get("extractions_total") or 0)
            net_total = financial["_summary"].get("net_total", 0)
            extraction_ratio = (extractions_total / net_total) if net_total > 0 else None

            financial.update({"sales_growth_pct": growth_pct, "extraction_ratio": extraction_ratio})
            return financial
        except Exception as e:
            _log.error(f"Error en _add_growth_and_extraction: {e}", exc_info=True)
            return financial
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_growth_and_extraction_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators()
    result = svc._add_growth_and_extraction(financial)
    assert "sales_growth_pct" in result
    assert "extraction_ratio" in result
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_growth_and_extraction_shape -v
```

#### Expected Report
- [ ] `_add_growth_and_extraction()` en `CoachService`
- [ ] Test PASS

---

### T6c: `CoachService` — caja real desde `CashBaseline`

**Depends on:** T6a, T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/models.py — CashBaseline (T1): business_id, baseline_date, amount
# app/models.py — YA EXISTEN: InventoryMovement (type, quantity, unit_price, unit_cost,
#   movement_date, business_id), OperatingExpense (amount, expense_date, business_id),
#   MoneyExtraction (amount, extraction_date, business_id)
# app/services/coach_service.py — YA EXISTE (T6a)
```

#### Objective
Calcular saldo de caja real desde el `CashBaseline` más reciente.

#### Instructions (≤400 tokens)
AFTER `_add_growth_and_extraction()` en `CoachService`:
```python
from app.models import CashBaseline, InventoryMovement, OperatingExpense, MoneyExtraction
from sqlalchemy import and_, func, select

    def current_cash(self, now: Optional[datetime] = None) -> dict:
        baseline = self._db.execute(
            select(CashBaseline).where(CashBaseline.business_id == self._business_id)
            .order_by(CashBaseline.baseline_date.desc()).limit(1)
        ).scalar_one_or_none()
        if baseline is None:
            return {"amount": None, "mode": "relative"}

        since = baseline.baseline_date
        sales_in = self._db.scalar(
            select(func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_price), 0))
            .where(and_(InventoryMovement.business_id == self._business_id,
                        InventoryMovement.type == "sale", InventoryMovement.movement_date >= since))
        ) or 0
        purchases_out = self._db.scalar(
            select(func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_cost), 0))
            .where(and_(InventoryMovement.business_id == self._business_id,
                        InventoryMovement.type == "purchase", InventoryMovement.movement_date >= since))
        ) or 0
        expenses_out = self._db.scalar(
            select(func.coalesce(func.sum(OperatingExpense.amount), 0))
            .where(and_(OperatingExpense.business_id == self._business_id,
                        OperatingExpense.expense_date >= since))
        ) or 0
        extractions_out = self._db.scalar(
            select(func.coalesce(func.sum(MoneyExtraction.amount), 0))
            .where(and_(MoneyExtraction.business_id == self._business_id,
                        MoneyExtraction.extraction_date >= since))
        ) or 0

        amount = float(baseline.amount) + float(sales_in) - float(purchases_out) - float(expenses_out) - float(extractions_out)
        return {"amount": amount, "mode": "real"}
```
> `diagnose()` (T10) llama `current_cash()` y añade `cash_amount`/`cash_mode` al dict de
> indicadores — esa integración se hace en T10, no aquí.

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_current_cash_relative_without_baseline(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    result = svc.current_cash()
    assert result["mode"] == "relative"
    assert result["amount"] is None

def test_current_cash_real_with_baseline(db_session, seeded_business):
    from datetime import datetime, timezone
    from app.models import CashBaseline
    db_session.add(CashBaseline(business_id=seeded_business.id, baseline_date=datetime.now(timezone.utc), amount=500.0))
    db_session.commit()
    svc = CoachService(db_session, seeded_business.id)
    result = svc.current_cash()
    assert result["mode"] == "real"
    assert result["amount"] == 500.0
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py -k current_cash -v
```

#### Expected Report
- [ ] `current_cash()` en `CoachService`
- [ ] 2 tests PASS (modo relativo, modo real)

---

### T7: `CoachService` — indicadores operativos

**Depends on:** T5, T6a
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/services/inventory_service.py — dead_stock() e inventory_turnover_days() (T5)
# app/services/coach_service.py — YA EXISTE (T6a/T6b)
# app/services/reporting_service.py — sales_metrics_table() YA EXISTE
```

#### Objective
Añadir indicadores operativos (riesgo quiebre, rotación, stock muerto).

#### Instructions (≤400 tokens)
AFTER `_add_growth_and_extraction()` en `CoachService`:
```python
from app.services.inventory_service import InventoryService

    def __init__(self, db: Session, business_id: int):
        # Añadir:
        self._inventory = InventoryService(db, self._business_id)

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
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_operational_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    financial = svc._financial_indicators()
    op = svc._operational_indicators(financial)
    assert "dead_stock_items" in op
    assert isinstance(op["dead_stock_items"], list)
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_operational_indicators_shape -v
```

#### Expected Report
- [ ] `_operational_indicators()` en `CoachService`
- [ ] Test PASS

---

### T8: `CoachService` — salud de clientes [C]

**Depends on:** T7
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/services/sales_document_service.py — YA EXISTE
class SalesDocumentService:
    def sales_by_customer(self, start=None, end=None, limit=20) -> list[dict]
    def customers_summary(self) -> dict  # {"count": int, "customers": [...]}
```

#### Objective
Detectar recurrencia, fuga de clientes y concentración.

#### Instructions (≤400 tokens)
AFTER `_operational_indicators()` en `CoachService`:
```python
from app.services.sales_document_service import SalesDocumentService
from datetime import timedelta

    def __init__(self, db: Session, business_id: int):
        # Añadir:
        self._sales_documents = SalesDocumentService(db, self._business_id)

    def _customer_indicators(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            rows = self._sales_documents.sales_by_customer(start=month_start, end=now_dt, limit=1)
            all_customers = self._sales_documents.sales_by_customer(start=None, end=now_dt, limit=10_000)
            
            total_sales = sum(r.get("total", 0) for r in all_customers)
            top = rows[0] if rows else None
            concentration = (top.get("total", 0) / total_sales) if (top and total_sales > 0) else None
            
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
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_customer_indicators_shape(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    ind = svc._customer_indicators()
    assert "churned_customers" in ind
    assert isinstance(ind["churned_customers"], list)
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_customer_indicators_shape -v
```

#### Expected Report
- [ ] `_customer_indicators()` en `CoachService`
- [ ] Test PASS

---

### T9: `CoachService` — estacionalidad [B]

**Depends on:** T7
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/services/reporting_service.py — monthly_overview(months, now) YA EXISTE
# app/services/coach_service.py — YA EXISTE
```

#### Objective
Atenuar falsas alarmas comparando con histórico del negocio.

#### Instructions (≤400 tokens)
AFTER `_customer_indicators()`:
```python
    def _seasonal_context(self, sales_growth_pct: Optional[float], now: Optional[datetime] = None) -> Optional[str]:
        if sales_growth_pct is None or sales_growth_pct >= 0:
            return None
        now_dt = now or datetime.now(timezone.utc)
        try:
            overview = self._reporting.monthly_overview(months=24, now=now_dt)
            if len(overview) < 13:
                return None
            
            current_month_idx = len(overview) - 1
            same_month_last_year_idx = current_month_idx - 12
            if same_month_last_year_idx < 0:
                return None
            
            current_sales = overview[current_month_idx].get("sales_total", 0)
            prior_year_sales = overview[same_month_last_year_idx].get("sales_total", 0)
            if not prior_year_sales:
                return None
            
            yoy_drop_pct = (prior_year_sales - current_sales) / prior_year_sales * 100.0
            if abs(yoy_drop_pct - abs(sales_growth_pct)) <= 5.0:
                return "es normal en esta época — no te preocupes"
            return None
        except Exception as e:
            _log.error(f"Error en _seasonal_context: {e}", exc_info=True)
            return None
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_seasonal_context_none_without_history(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    assert svc._seasonal_context(-15.0) is None

def test_seasonal_context_none_when_positive(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    assert svc._seasonal_context(5.0) is None
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py -k seasonal -v
```

#### Expected Report
- [ ] `_seasonal_context()` en `CoachService`
- [ ] 2 tests PASS

---

### T10: `CoachService.diagnose()` — orquestación y scoring

**Depends on:** T6b, T6c, T8, T9
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P2, P4

#### Context (≤200 tokens)
```python
# app/services/coach_rules.py — evaluate(indicators) YA EXISTE (T4a/T4b)
# app/services/coach_service.py — todos los _indicadores_*, current_cash() (T6c) YA EXISTEN
```

#### Objective
Ensamblar `diagnose()` con reglas, scoring y guards.

#### Instructions (≤400 tokens)
AFTER `_seasonal_context()`:
```python
from app.services import coach_rules

    def diagnose(self, now: Optional[datetime] = None) -> dict:
        now_dt = now or datetime.now(timezone.utc)
        try:
            financial = self._financial_indicators(now_dt)
            if financial.get("_summary", {}).get("sales_total", 0) <= 0:
                return {"score": None, "level": "unknown", "message": "sin datos suficientes",
                        "recommendations": [], "whats_good": []}
            
            financial = self._add_growth_and_extraction(financial, now_dt)
            cash = self.current_cash(now_dt)
            financial["cash_amount"] = cash["amount"]
            financial["cash_mode"] = cash["mode"]
            operational = self._operational_indicators(financial, now_dt)
            customers = self._customer_indicators(now_dt)
            
            indicators = {**financial, **operational, **customers}
            recommendations = coach_rules.evaluate(indicators)
            
            seasonal_note = self._seasonal_context(financial.get("sales_growth_pct"), now_dt)
            
            recommendations.sort(
                key=lambda r: (0 if r["severity"] == "red" else 1, -(r.get("impact_amount") or 0))
            )
            
            red_count = sum(1 for r in recommendations if r["severity"] == "red")
            yellow_count = sum(1 for r in recommendations if r["severity"] == "yellow")
            score = max(0, 100 - red_count * 20 - yellow_count * 10)
            level = "red" if red_count > 0 else ("yellow" if yellow_count > 0 else "green")
            
            whats_good = []
            if financial.get("sales_growth_pct", 0) and financial["sales_growth_pct"] > 0:
                whats_good.append(f"Ventas +{financial['sales_growth_pct']:.0f}% este mes")
            
            return {"score": score, "level": level, "recommendations": recommendations,
                    "whats_good": whats_good, "seasonal_note": seasonal_note}
        except Exception as e:
            _log.error(f"Error en diagnose: {e}", exc_info=True)
            return {"score": None, "level": "error", "message": "diagnóstico no disponible",
                    "recommendations": [], "whats_good": []}
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_diagnose_no_sales(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    result = svc.diagnose()
    assert result["level"] == "unknown"
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_diagnose_no_sales -v
```

#### Expected Report
- [ ] `diagnose()` en `CoachService`
- [ ] Test PASS

---

### T11: `CoachService` — círculo cerrado [A]

**Depends on:** T2, T10
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/coach_service.py`, `tests/unit/test_coach_service.py`
**Pillars:** P1, P2, P4, P6

#### Context (≤200 tokens)
```python
# app/models.py — CoachActionLog (T2) YA EXISTE
# app/services/coach_service.py — diagnose() YA EXISTE (T10)
```

#### Objective
Marcar recomendaciones como hechas/descartadas y filtrar en `diagnose()`.

#### Instructions (≤400 tokens)
AFTER `diagnose()`:
```python
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

# Modificar diagnose() — justo después de "recommendations = coach_rules.evaluate(...)":
            suppressed = self._dismissed_or_done_keys()
            recommendations = [r for r in recommendations if r["rule_key"] not in suppressed]
```

Añadir a `tests/unit/test_coach_service.py`:
```python
def test_mark_action_suppresses(db_session, seeded_business):
    svc = CoachService(db_session, seeded_business.id)
    svc.mark_action("margin_drop", "dismissed", user_id=1)
    keys = svc._dismissed_or_done_keys()
    assert "margin_drop" in keys
```

#### Verification
```bash
pytest tests/unit/test_coach_service.py::test_mark_action_suppresses -v
```

#### Expected Report
- [ ] `mark_action()` + filtro en `diagnose()` en `CoachService`
- [ ] Test PASS

---

### T12: Router `ui_coach.py` — GET tab

**Depends on:** T3, T10
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_coach.py`, `tests/unit/test_coach_rbac.py`
**Pillars:** P1, P6

#### Context (≤200 tokens)
```python
# app/routers/ui_agent.py — PATRÓN REAL (admin-only con ensure_admin_or_owner):
@router.get("/tabs/agent", response_class=HTMLResponse)
def tab_agent(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    ...
    return templates.TemplateResponse(request, "partials/tab_agent.html", {...})
```

#### Objective
Crear router GET del tab Coach.

#### Instructions (≤400 tokens)
Crear `app/routers/ui_coach.py`:
```python
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from app.deps import session_dep
from app.routers.ui_common import ensure_admin_or_owner, templates
from app.security import require_active_business_id
from app.services.coach_service import CoachService

router = APIRouter(tags=["ui-coach"])

@router.get("/tabs/coach", response_class=HTMLResponse)
def tab_coach(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    report = CoachService(db, business_id).diagnose()
    return templates.TemplateResponse(request, "partials/tab_coach.html", {"report": report})
```

Crear `tests/unit/test_coach_rbac.py`:
```python
def test_operator_gets_403_on_tab(client, operator_session):
    resp = client.get("/ui/tabs/coach", cookies=operator_session)
    assert resp.status_code == 403

def test_admin_gets_200_on_tab(client, admin_session):
    resp = client.get("/ui/tabs/coach", cookies=admin_session)
    assert resp.status_code == 200
```

#### Verification
```bash
pytest tests/unit/test_coach_rbac.py -v
```

#### Expected Report
- [ ] `app/routers/ui_coach.py` creado (GET tab)
- [ ] 2 tests PASS (403 operator, 200 admin)

---

### T13: Router `ui_coach.py` — POST caja

**Depends on:** T3, T10
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_coach.py`, `tests/unit/test_coach_rbac.py`
**Pillars:** P1, P6

#### Context (≤200 tokens)
```python
# app/routers/ui_coach.py — GET ya existe (T12)
# app/schemas.py — CashBaselineCreate con validación (T3)
# app/models.py — CashBaseline (T1)
```

#### Objective
Endpoint POST para cargar caja inicial.

#### Instructions (≤400 tokens)
AFTER `tab_coach()` en `ui_coach.py`:
```python
from datetime import datetime
from app.models import CashBaseline
from app.schemas import CashBaselineCreate
from app.security import get_current_user_from_session
from fastapi import Form

@router.post("/coach/cash-baseline", response_class=HTMLResponse)
def set_cash_baseline(
    request: Request, baseline_date: str = Form(...), amount: float = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    payload = CashBaselineCreate(baseline_date=datetime.fromisoformat(baseline_date), amount=amount)
    user = get_current_user_from_session(db, request)
    row = CashBaseline(business_id=business_id, baseline_date=payload.baseline_date,
                       amount=payload.amount, created_by=user.id if user else None)
    db.add(row)
    db.commit()
    report = CoachService(db, business_id).diagnose()
    return templates.TemplateResponse(request, "partials/tab_coach.html", {"report": report})
```

Añadir a `tests/unit/test_coach_rbac.py`:
```python
def test_operator_gets_403_on_cash_baseline(client, operator_session):
    resp = client.post("/ui/coach/cash-baseline", 
                      data={"baseline_date": "2026-01-01T00:00:00", "amount": 100},
                      cookies=operator_session)
    assert resp.status_code == 403

def test_invalid_cash_amount_422(client, admin_session):
    resp = client.post("/ui/coach/cash-baseline",
                      data={"baseline_date": "2026-01-01T00:00:00", "amount": -1},
                      cookies=admin_session)
    assert resp.status_code == 422
```

#### Verification
```bash
pytest tests/unit/test_coach_rbac.py -v
```

#### Expected Report
- [ ] POST endpoint en `ui_coach.py`
- [ ] 2 tests PASS (403 operator, 422 invalid)

---

### T14: Router `ui_coach.py` — POST acción

**Depends on:** T11, T13
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_coach.py`, `tests/unit/test_coach_rbac.py`
**Pillars:** P1, P6

#### Context (≤200 tokens)
```python
# app/routers/ui_coach.py — GET + POST caja YA EXISTEN
# app/schemas.py — CoachActionUpdate (T3)
# app/services/coach_service.py — mark_action() (T11)
```

#### Objective
Endpoint POST para marcar recomendaciones como hechas/descartadas.

#### Instructions (≤400 tokens)
AFTER POST cash-baseline:
```python
from app.schemas import CoachActionUpdate

@router.post("/coach/action", response_class=HTMLResponse)
def update_action(
    request: Request, rule_key: str = Form(...), status: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    payload = CoachActionUpdate(rule_key=rule_key, status=status)
    user = get_current_user_from_session(db, request)
    CoachService(db, business_id).mark_action(payload.rule_key, payload.status, user.id if user else 0)
    report = CoachService(db, business_id).diagnose()
    return templates.TemplateResponse(request, "partials/tab_coach.html", {"report": report})
```

Añadir a `tests/unit/test_coach_rbac.py`:
```python
def test_invalid_action_status_422(client, admin_session):
    resp = client.post("/ui/coach/action",
                      data={"rule_key": "margin_drop", "status": "invalid"},
                      cookies=admin_session)
    assert resp.status_code == 422
```

#### Verification
```bash
pytest tests/unit/test_coach_rbac.py -v
```

#### Expected Report
- [ ] POST action endpoint en `ui_coach.py`
- [ ] Test PASS (422 invalid status)

---

### T15a: Template `tab_coach.html`

**Depends on:** T14
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/tab_coach.html`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/routers/ui_coach.py — GET /tabs/coach (T12) pasa: {"report": report}
# report = {"score": int|None, "level": "red"|"yellow"|"green"|"unknown", "message"?: str,
#            "recommendations": [{"rule_key","severity","symptom","cause","action",
#                                  "impact_amount","impact_text"}], "whats_good": [str]}
```

#### Objective
Template Jinja2 del score, alertas y recomendaciones.

#### Instructions (≤400 tokens)
Crear `app/templates/partials/tab_coach.html`:
```html
<div class="coach-tab">
  {% if report.level == 'unknown' %}
  <p class="coach-empty">{{ report.message or "sin datos" }}</p>
  {% else %}
  <div class="coach-score">
    <span class="coach-score-value">{{ report.score }}/100</span>
    <span class="coach-score-level">{{ {'red':'🔴 Crítico','yellow':'🟡 Regular','green':'🟢 Saludable'}.get(report.level, '?') }}</span>
  </div>
  {% if report.recommendations %}
  <h3>⚠️ Recomendaciones</h3>
  {% for rec in report.recommendations %}
  <div class="coach-rec">
    <p>{{ rec.symptom }} → {{ rec.action }}</p>
    {% if rec.impact_amount %}<p>💰 {{ rec.impact_text }}</p>{% endif %}
    <button hx-post="/ui/coach/action" hx-vals='{"rule_key":"{{ rec.rule_key }}","status":"done"}'>Hecho</button>
  </div>
  {% endfor %}
  {% endif %}
  {% endif %}
</div>
```

#### Verification
```bash
python3 -c "from jinja2 import Environment, FileSystemLoader; Environment(loader=FileSystemLoader('app/templates')).get_template('partials/tab_coach.html')"
```

#### Expected Report
- [ ] `tab_coach.html` creado, sin errores de sintaxis Jinja2

---

### T15b: Registrar router + botón sidebar

**Depends on:** T15a
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/main.py`, `app/templates/dashboard.html`
**Pillars:** P1, P4

#### Context (≤200 tokens)
```python
# app/main.py — YA EXISTE, patrón real:
from app.routers import ui_agent
app.include_router(ui_agent.router, prefix="/ui")

# dashboard.html — sidebar HTMX, dentro de {% if user_role in ('admin', 'owner') %}:
<button id="tab-agent" hx-get="/ui/tabs/agent" hx-target="#tab-content">
  Dashboard Dinámico
</button>
```

#### Objective
Registrar `ui_coach.router` y añadir botón "Coach" al sidebar.

#### Instructions (≤400 tokens)
En `app/main.py`, BEFORE:
```python
from app.routers import ui_agent
```
AFTER:
```python
from app.routers import ui_agent, ui_coach
```
Y registrar (junto a los demás `include_router`):
```python
app.include_router(ui_coach.router, prefix="/ui")
```

En `app/templates/dashboard.html`, dentro de `{% if user_role in ('admin', 'owner') %}` AFTER el botón `tab-agent`:
```html
<button class="sidebar-item" id="tab-coach" aria-selected="false"
        hx-get="/ui/tabs/coach" hx-target="#tab-content" hx-swap="innerHTML">
  Coach
</button>
```

#### Verification
```bash
python3 -c "from app.main import app; print('OK')"
```

#### Expected Report
- [ ] Router registrado en `main.py`
- [ ] Botón sidebar añadido
- [ ] App arranca sin error

---

### T16: Tools del agente — `coach_health_score` + `coach_recommendations`

**Depends on:** T10
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`, `tests/unit/test_coach_tools.py`
**Pillars:** P1, P6

#### Context (≤200 tokens)
```python
# app/agent/tools.py — patrón real:
def tool(name: str, description: str, params: dict) -> Callable: ...

@tool("stock", "Stock de un SKU.",
      {"type": "object", "properties": {"sku": {"type": "string"}}, "required": ["sku"]})
def stock(service: InventoryService, args: dict) -> dict:
    s = service.stock(str(args["sku"]))
    return {"value": float(s.quantity), "label": s.name}
```

#### Objective
Añadir 2 tools que expongan el diagnóstico del coach al agente.

#### Instructions (≤400 tokens)
AFTER tools existentes en `app/agent/tools.py`:
```python
from app.services.coach_service import CoachService

@tool("coach_health_score", "Score de salud del negocio (0-100).",
      {"type": "object", "properties": {}})
def coach_health_score(service: InventoryService, args: dict) -> dict:
    report = CoachService(service.db, service.business_id).diagnose()
    return {"value": report["score"], "label": report["level"]}

@tool("coach_recommendations", "Recomendaciones activas del coach.",
      {"type": "object", "properties": {}})
def coach_recommendations(service: InventoryService, args: dict) -> dict:
    report = CoachService(service.db, service.business_id).diagnose()
    return {
        "columns": ["Síntoma", "Acción", "Impacto"],
        "rows": [[r["symptom"], r["action"], r.get("impact_text", "")] for r in report["recommendations"]],
    }
```

Crear `tests/unit/test_coach_tools.py`:
```python
from app.services.inventory_service import InventoryService
from app.agent.tools import coach_health_score

def test_coach_health_score_scoped(db_session, seeded_business):
    service = InventoryService(db_session, seeded_business.id)
    result = coach_health_score(service, {})
    assert "value" in result
    assert "label" in result
```

#### Verification
```bash
pytest tests/unit/test_coach_tools.py -v
```

#### Expected Report
- [ ] 2 tools en `app/agent/tools.py`
- [ ] Test PASS

---

### T17: E2E — flujo admin completo

**Depends on:** T15b, T16
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_coach.py`
**Pillars:** P1

#### Context (≤200 tokens)
```python
# Patrón Playwright real (leer tests/e2e/test_copiloto.py o similar para fixtures):
# - page: Playwright Page object
# - live_server: uvicorn dev server on localhost + random port
# - admin_login: fixture que loguea admin y retorna (page, session_cookie)
# - db_session: fixture de DB
```

#### Objective
E2E: admin carga caja, abre Coach, ve score/recomendaciones, marca acción.

#### Instructions (≤400 tokens)
Crear `tests/e2e/test_coach.py`:
```python
import pytest

pytestmark = pytest.mark.e2e

def test_admin_coach_flow(page, live_server, admin_login):
    # 1. login como admin (admin_login fixture maneja esto)
    # 2. cargar caja: POST /ui/coach/cash-baseline con baseline_date + amount
    page.goto(f"{live_server}/ui/dashboard")
    page.fill("input[name='baseline_date']", "2026-01-01T00:00:00")
    page.fill("input[name='amount']", "5000")
    page.click("button:has-text('Cargar Caja')")
    
    # 3. click en tab Coach (sidebar)
    page.click("#tab-coach")
    
    # 4. assert que aparece score (.coach-score-value)
    assert page.locator(".coach-score-value").is_visible()
    
    # 5. si hay recomendaciones, click "Hecho" en la primera
    recs = page.locator(".coach-rec")
    if recs.count() > 0:
        page.locator(".coach-rec button:has-text('Hecho')").first.click()
        # Verificar que se actualiza el tab (re-render HTMX)
        assert page.locator(".coach-rec").count() < recs.count()

def test_operator_has_no_coach_access(page, live_server, operator_login):
    # 1. login como operator
    # 2. assert que #tab-coach NO existe en el sidebar
    page.goto(f"{live_server}/ui/dashboard")
    assert not page.locator("#tab-coach").is_visible()
    
    # 3. request directo a /ui/tabs/coach → 403
    response = page.goto(f"{live_server}/ui/tabs/coach")
    assert response.status == 403
```

#### Verification
```bash
python3 -m pytest tests/e2e/test_coach.py -m e2e --no-cov -v
```

#### Expected Report
- [ ] `tests/e2e/test_coach.py` con 2 casos (admin flow, operator 403)
- [ ] Ambos PASS

---

### T18a: Actualizar VERSION + `CLAUDE.md` raíz

**Depends on:** T17
**Skill:** backend-developer
**Agent:** coder
**Files:** `VERSION`, `CLAUDE.md`
**Pillars:** P4

#### Context (≤200 tokens)
```
VERSION file: 0.4.0 actualmente
CLAUDE.md (raíz) — sección "## Estado actual" tiene una lista de [x] features completadas
```

#### Objective
Actualizar VERSION a 0.4.1 y añadir el coach a "Estado actual" de `CLAUDE.md`.

#### Instructions (≤400 tokens)
1. `VERSION`: reemplazar contenido completo por `0.4.1`

2. En `CLAUDE.md` (raíz), AFTER la última línea `- [x]` de la sección `## Estado actual`, añadir:
```
- [x] **Coach financiero y operativo** — CoachService (indicadores fin/op/clientes), motor de
      reglas (5 reglas + logging), círculo cerrado [A], estacionalidad [B]. Tab admin-only con
      score, recomendaciones accionables (síntoma+causa+acción+impacto), burbuja chat vía
      agente existente. 24 task envelopes, tests unitarios + e2e nuevos. v0.4.1
```

#### Verification
```bash
cat VERSION
grep -A2 "Coach financiero" CLAUDE.md
```

#### Expected Report
- [ ] `VERSION` → `0.4.1`
- [ ] `CLAUDE.md` con línea nueva en "Estado actual"

---

### T18b: Actualizar `.claude/CLAUDE.md` + nota Obsidian

**Depends on:** T18a
**Skill:** backend-developer
**Agent:** coder
**Files:** `.claude/CLAUDE.md`, `~/obsidian-lab/05-Proyectos/gestion-inventario.md`
**Pillars:** P4

#### Context (≤200 tokens)
```
.claude/CLAUDE.md — sección "## Estado actual del proyecto" (bloque de texto plano con
"Versión:", "Hecho (08-15):", "Pendiente:")
gestion-inventario.md (Obsidian) — frontmatter YAML con "version:", "siguiente-hito:",
y sección "## Notas de sesiones" (lista de líneas "- [[fecha]] — ...")
```

#### Objective
Actualizar estado del proyecto en `.claude/CLAUDE.md` y la nota Obsidian.

#### Instructions (≤400 tokens)
En `.claude/CLAUDE.md`, BEFORE:
```
Versión:      0.4.0
```
AFTER:
```
Versión:      0.4.1
Hecho (08-16): coach financiero-operativo (determinista + chat vía agente), 5 reglas evaluadas,
               tab admin-only, círculo cerrado, 24 task envelopes, tests unitarios + e2e nuevos
Pendiente:    validar coach contra datos reales de producción
```

En `~/obsidian-lab/05-Proyectos/gestion-inventario.md`:
1. Frontmatter: `version: 0.4.0` → `version: 0.4.1`; `siguiente-hito: verde-definitivo-4.2-y-fase-2-agente` → `siguiente-hito: beta-coach-financiero-operativo`
2. AFTER la primera línea de `## Notas de sesiones` (antes del primer `- [[2026-08-15]]`), añadir:
```
- [[2026-08-16]] — **Coach financiero y operativo (SDD completo):** requirements.md + design.md + tasks.md (24 envelopes) + PAE ensamblados vía SDD-Agent. Motor de reglas determinista (margen/gastos/extracciones/stock muerto/concentración clientes) + círculo cerrado [A] + estacionalidad [B] + salud de clientes [C]. Tab admin-only, burbuja chat vía agente existente. Ready para OpenCode + Qwen.
```

#### Verification
```bash
grep "Versión:" .claude/CLAUDE.md
grep "version:" ~/obsidian-lab/05-Proyectos/gestion-inventario.md
```

#### Expected Report
- [ ] `.claude/CLAUDE.md` actualizado (Versión/Hecho/Pendiente)
- [ ] Nota Obsidian actualizada (frontmatter + sesión nueva)

---

### T19: Commit PAE corregido

**Depends on:** T18b
**Skill:** backend-developer
**Agent:** coder
**Files:** `specs/coach-financiero-operativo/`, `VERSION`, `CLAUDE.md`, `.claude/CLAUDE.md`
**Pillars:** P4

> Nota: el límite de "≤2 archivos" aplica a envelopes que **escriben código nuevo** (donde
> tocar más archivos implica más decisiones de diseño para OpenCode). Este envelope es una
> operación mecánica de `git add`/`git commit` sobre archivos **ya producidos** por T1-T18b —
> no hay código nuevo que razonar aquí, por eso queda exento del límite.

#### Context (≤200 tokens)
Cambios aún no committeados en rama `feature/coach-financiero-operativo`.

#### Objective
Commitear la feature completa (specs SDD + código + docs) lista para OpenCode.

#### Instructions (≤400 tokens)
```bash
git add specs/coach-financiero-operativo/ VERSION CLAUDE.md .claude/CLAUDE.md
git commit -m "$(cat <<'EOF'
Add coach financiero-operativo SDD spec (requirements + design + 24 task envelopes)

Diagnóstico determinista (sin LLM) de indicadores financieros/operativos/clientes con
motor de reglas, círculo cerrado de acciones y ajuste por estacionalidad. Burbuja de
chat reutiliza el agente LangGraph existente. Todos los envelopes verificados contra
el checklist de pae-estructura.md (≤800 tokens, ≤2 archivos, logging, sin placeholders).

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
EOF
)"
git status
```

#### Verification
```bash
git log --oneline -1
git show --stat
```

#### Expected Report
- [ ] Commit creado
- [ ] PAE ready para OpenCode
- [ ] rama feature/coach-financiero-operativo limpia

---

## Cierre

**PAE completo y listo.** 19 envelopes (divididos correctamente), verificados contra checklist, logging añadido, tests e2e concretos, 100% autocontenido para OpenCode + Qwen3.6-35B-A3B.
