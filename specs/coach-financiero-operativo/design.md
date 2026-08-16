# Phase 2 — Design: Coach financiero y operativo

- **Slug:** `coach-financiero-operativo`
- **Fecha:** 2026-08-16
- **Rama base:** `develop`

---

## 2.1 Codebase Grounding

Firmas reales verificadas en el repo (no asumidas):

```python
# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR (solo lectura)
class ReportingService:
    def __init__(self, db: Session, business_id: int): ...
    def monthly_profit_report(self, now=None, location_id=None, *, start=None, end=None) -> tuple[dict, list[dict]]
        # summary: sales_total, cogs_total, gross_total, expenses_total, net_total,
        #          cogs_pct, gross_margin_pct, net_margin_pct, expenses_pct
    def monthly_overview(self, months=12, now=None, location_id=None, start_dt=None, end_dt=None) -> list[dict]
    def sales_metrics_table(self, now, months=12, location_id=None, start_dt=None, end_dt=None) -> list[dict]
        # cada fila: name, stock_qty, min_stock, reorder_point, reorder_shortage

# app/services/finance_service.py — YA EXISTE
class FinanceService:
    def __init__(self, db: Session, business_id: int): ...
    def monthly_dividends_report(self, now=None, *, start=None, end=None) -> dict

# app/services/inventory_service.py — YA EXISTE
class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None): ...  # None -> HTTP 409
    def inventory_value_total(self, location_id=None) -> float

# app/services/sales_document_service.py — YA EXISTE
class SalesDocumentService:
    def __init__(self, db: Session, business_id: int): ...
    def sales_by_customer(self, start=None, end=None, limit=20) -> list[dict]
        # cada fila: client_name, doc_count, total

# app/security.py — YA EXISTE
def require_active_business_id(db, request) -> int   # 409 si no hay negocio activo
def get_current_user_from_session(db, request) -> Optional[User]

# app/routers/ui_common.py — YA EXISTE
def ensure_admin_or_owner(db, request) -> None   # HTTPException 403 si no es admin/owner
templates = Jinja2Templates(directory="app/templates")

# app/deps.py — YA EXISTE
def session_dep() -> Generator[Session, None, None]

# app/agent/tools.py — YA EXISTE, patrón de tool
def tool(name: str, description: str, params: dict) -> Callable: ...
# 27 tools existentes reciben (service: InventoryService, args: dict); usan service.db / service.business_id

# app/models.py — patrón: toda tabla tiene business_id FK a businesses.id
```

**Inventario de archivos:**

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/models.py` | MODIFY | añadir `CashBaseline`, `CoachActionLog` |
| `app/services/coach_rules.py` | CREATE | umbrales, textos, estimadores de impacto (declarativo, con logging) |
| `app/services/coach_service.py` | CREATE | `CoachService`: indicadores + diagnose() + mark_action() |
| `app/services/inventory_service.py` | MODIFY | +`dead_stock()`, +`inventory_turnover_days()` (aditivo, solo lectura) |
| `app/schemas.py` | MODIFY | +`CashBaselineCreate`, +`CoachActionUpdate` |
| `app/routers/ui_coach.py` | CREATE | GET tab, POST caja, POST estado de acción |
| `app/templates/partials/tab_coach.html` | CREATE | score + alertas + recomendaciones |
| `app/templates/dashboard.html` | MODIFY | botón sidebar "Coach" (dentro del bloque admin/owner ya existente) |
| `app/main.py` | MODIFY | registrar `ui_coach.router` |
| `app/agent/tools.py` | MODIFY | +`coach_health_score`, +`coach_recommendations` (read-only) |
| `ReportingService`, `FinanceService`, `SalesDocumentService`, `security.py`, `ui_common.py`, `db.py` | REFERENCE | solo lectura, sin modificar |

---

## 2.2 Architecture Approach

**Núcleo determinista (`CoachService` + `coach_rules.py`):** vive en el mismo proceso que la
API, sin LLM. Llama directamente a `ReportingService`/`FinanceService`/`InventoryService`/
`SalesDocumentService` (todos ya scoped por `business_id`). `coach_rules.evaluate(indicators)`
es una función pura: recibe un `dict` de indicadores, devuelve una lista de recomendaciones —
sin efectos secundarios, 100% testeable sin BD.

**Flujo de `GET /ui/tabs/coach`:**
```
GET /ui/tabs/coach
  → ensure_admin_or_owner + require_active_business_id
  → CoachService(db, business_id).diagnose()
       financial = _financial_indicators()          # margen, gastos/ingresos
       financial = _add_growth_and_extraction(...)   # crecimiento, extracciones, caja
       operational = _operational_indicators(...)    # rotación, stock muerto, quiebre
       customers = _customer_indicators(...)         # concentración, recurrencia, fuga
       indicators = {**financial, **operational, **customers}
       recommendations = coach_rules.evaluate(indicators)
       suppressed = _dismissed_or_done_keys()         # círculo cerrado [A]
       recommendations = [r for r in recommendations if r.rule_key not in suppressed]
       seasonal_note = _seasonal_context(...)          # atenúa falsas alarmas [B]
       score, level = <calculado de severidades>
  → render partials/tab_coach.html
```

**Burbuja de chat:** reutiliza `/ui/agent/chat` (ya existente, sin modificar) con 2 tools
nuevas (`coach_health_score`, `coach_recommendations`) que exponen el mismo `diagnose()` al
LLM — el LLM narra el diagnóstico ya calculado, nunca inventa cifras propias.

**Invariantes que se preservan:**
- `business_id` en toda tabla nueva y en todo query (aislamiento multi-negocio).
- `ensure_admin_or_owner` como único control de acceso (igual que el copiloto) — operator sin
  acceso alguno (más estricto que el copiloto, donde operator sí puede ver).
- `Base` de `app/db.py` como única base declarativa.
- Servicios existentes (`ReportingService`, etc.) no se modifican, solo se leen o se extienden
  de forma aditiva (`InventoryService.dead_stock`).
- Ninguna tabla existente sufre `ALTER` — solo tablas nuevas.

---

## 2.3 Data Model

### Entity: CashBaseline (NUEVA)

| Field | Type | Change | Default | Notes |
|-------|------|--------|---------|-------|
| id | Integer PK | NEW | auto | |
| business_id | FK businesses.id | NEW | — | NOT NULL, aislamiento multi-negocio |
| baseline_date | DateTime(tz) | NEW | — | fecha del saldo declarado |
| amount | Numeric(14,4) | NEW | — | validado ≥0 en `CashBaselineCreate` |
| created_by | FK users.id | NEW | NULL | |
| created_at | DateTime(tz) | NEW | server_default now | |

```python
class CashBaseline(Base):
    __tablename__ = "cash_baselines"
    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    baseline_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
```

**Migración:** `Base.metadata.create_all(bind=engine)` en `run_startup_tasks()` crea la tabla en
startup — sin `ALTER`, sin bump de `schema_version`. Sin backfill (tabla nueva, vacía).
**Rollback:** eliminar la clase + `DROP TABLE cash_baselines;` — sin pérdida de datos de negocio.
**Índices:** `business_id` y `baseline_date` — necesarios para `ORDER BY baseline_date DESC LIMIT 1`.

### Entity: CoachActionLog (NUEVA)

| Field | Type | Change | Default | Notes |
|-------|------|--------|---------|-------|
| id | Integer PK | NEW | auto | |
| business_id | FK businesses.id | NEW | — | |
| rule_key | String(128) | NEW | — | identidad estable de la regla (ej. `margin_drop`) |
| status | String(16) | NEW | `"open"` | `open` \| `done` \| `dismissed` (validado en schema) |
| impact_estimate | Numeric(14,4) | NEW | NULL | |
| marked_by | FK users.id | NEW | NULL | |
| marked_at | DateTime(tz) | NEW | NULL | |
| verified_result | Numeric(14,4) | NEW | NULL | reservado para verificación futura del impacto real |
| verified_at | DateTime(tz) | NEW | NULL | |

```python
class CoachActionLog(Base):
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

**Migración/Rollback:** igual patrón que `CashBaseline` — `create_all` automático, `DROP TABLE`
para rollback. **Índice compuesto** `(business_id, rule_key)` — necesario para el lookup de
`_dismissed_or_done_keys()` en cada `diagnose()`.

---

## 2.4 API Contracts

### GET /ui/tabs/coach
**Auth:** admin/owner (`ensure_admin_or_owner`); operator → 403.
**Response 200:** `partials/tab_coach.html` (score, alertas, recomendaciones, "lo que va bien").
**Response 403:** operator.
**Response 409:** sin negocio activo (`require_active_business_id`).

### POST /ui/coach/cash-baseline
**Auth:** admin/owner.
**Request (form):** `baseline_date: str` (ISO), `amount: float`.
**Response 200 (HTMX):** `partials/tab_coach.html` re-renderizado con `diagnose()` actualizado.
**Response 403:** operator.
**Response 422:** `amount < 0` o `baseline_date` futura (validado por `CashBaselineCreate`).
**Side effect:** persiste una fila nueva en `cash_baselines` (no sobreescribe la anterior — se
usa la más reciente por `baseline_date`).

### POST /ui/coach/action
**Auth:** admin/owner.
**Request (form):** `rule_key: str`, `status: str` (`done` | `dismissed`).
**Response 200 (HTMX):** `partials/tab_coach.html` re-renderizado sin esa recomendación.
**Response 403:** operator.
**Response 422:** `status` fuera de whitelist (validado por `CoachActionUpdate`).
**Side effect:** persiste en `coach_action_logs`; `diagnose()` posterior suprime esa `rule_key`.

---

## 2.5 Security Review

| Check | Answer |
|-------|--------|
| **Nuevo límite de auth?** | Sí — admin/owner en los 3 endpoints; **más estricto que el copiloto**, operator no ve nada del coach (ni siquiera lectura) |
| **Nuevos datos sensibles?** | No — caja/utilidad ya son datos del sistema; sin PII nueva |
| **Input validation?** | `CashBaselineCreate` (amount≥0, fecha no futura) y `CoachActionUpdate` (status whitelist) vía Pydantic `field_validator` |
| **Nueva superficie de ataque?** | Mínima — 3 endpoints nuevos con el mismo patrón de auth que `ui_agent.py`; tools del agente son read-only |
| **Secrets?** | Ninguno nuevo |
| **OWASP?** | Authorization (roles), IDOR (todo scoped por `business_id` inyectado por el router, nunca por el cliente), DoS (cálculo on-demand acotado, sin recursión) |

---

## 2.6 Trade-offs

| Decision | Elegido | Alternativa | Por qué no la alternativa |
|----------|---------|-------------|---------------------------|
| Motor de recomendaciones | Determinista (reglas + umbrales) | Todo vía LLM | Una recomendación financiera que el usuario sigue a ciegas no puede depender de que el LLM "adivine bien"; debe ser reproducible y testeable |
| Chat | Reutiliza agente LangGraph existente + 2 tools nuevas | Chat separado / embebido nuevo | Evita duplicar infraestructura ya auditada (circuit breaker, rate limit, retry) |
| Caja real | `CashBaseline` (saldo puntual) + cálculo en vivo | Pedir caja/deuda completas | El modelo no tiene balance; un baseline puntual desbloquea saldo real sin pedir datos que el sistema no mantiene actualizados |
| Deuda/pasivos | Fuera de alcance | Formulario de deuda manual | Sin ningún dato de origen en el sistema — pedirlo sería fricción sin garantía de mantenerse actualizado |
| Frecuencia | On-demand al abrir el tab | Job programado + snapshot diario | Sin scheduler nuevo; datos siempre frescos; volumen actual no lo justifica |
| RBAC | Solo admin/owner (operator sin acceso) | Operator solo-lectura (como el copiloto) | El coach expone diagnóstico financiero más sensible que el copiloto; decisión explícita del usuario |
| Observabilidad | `try/except` + `logging.error(exc_info=True)` en cada método público de `CoachService`/`coach_rules` | Dejar que las excepciones propaguen | Un error de cálculo no debe tumbar el tab (REQ-13); se loggea para diagnóstico posterior |

---

## 2.7 Migration & Rollback Strategy

```
Migración:
1. Añadir CashBaseline y CoachActionLog a models.py (create_all crea ambas tablas en startup).
2. Desplegar código (develop → dev → prod).
3. Sin backfill (tablas nuevas, vacías).

Rollback:
1. Revertir el registro de ui_coach.router y el botón del sidebar.
2. DROP TABLE cash_baselines; DROP TABLE coach_action_logs;  (sin pérdida de datos de negocio).
3. Revertir las 2 tools nuevas en app/agent/tools.py (no afecta las 27 existentes).
```
