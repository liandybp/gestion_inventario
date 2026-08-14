# Phase 2 — Design: Copiloto de negocio (dashboard dinámico con IA)

- **Slug:** `copiloto-dashboard-ia`
- **Fecha:** 2026-08-14
- **Rama base:** `develop`

---

## 2.1 Codebase Grounding

Firmas reales verificadas en el repo (no asumidas):

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None):
        # si business_id es None -> HTTPException(409, "business_id is required")
        self._business_id = int(business_id)

    def sales_metrics_table(self, now, months=12, location_id=None, start_dt=None, end_dt=None) -> list[dict]
    def sales_by_product(self, start, end, location_id=None) -> tuple[float, list[dict]]
    def daily_sales_series(self, start, end, location_id=None) -> list[dict]
    def monthly_profit_report(self, now=None, location_id=None, *, start=None, end=None) -> tuple[dict, list[dict]]
    def monthly_overview(self, months=12, now=None, location_id=None, start_dt=None, end_dt=None) -> list[dict]
    def total_expenses(self, start=None, end=None) -> float
    def top_expense_concept(self, start, end) -> Optional[dict]
    def inventory_value_total(self, location_id=None) -> float
    def inventory_sale_value_total(self, location_id=None) -> float
    def stock_list(self, query="", location_code=None) -> list[StockRead]
    def stock(self, sku, location_code=None) -> StockRead
    def available_lots(self, sku, *, location_code=None) -> list[InventoryLot]
    def recent_sales(self, query="", limit=20, start_date=None, end_date=None, month=None, year=None, location_id=None) -> list[tuple]
    def recent_purchases(self, query="", limit=20, start_date=None, end_date=None, month=None, year=None, location_id=None) -> list[tuple]
    def movement_history(self, sku=None, query="", movement_type=None, location_id=None, start_date=None, end_date=None, limit=100) -> list[tuple]

# app/security.py — YA EXISTE
def get_current_user_from_session(db, request) -> Optional[User]
def require_active_business_id(db, request) -> int   # 409 si no hay negocio activo
def get_active_business_id(db, request) -> Optional[int]

# app/routers/ui_common.py — YA EXISTE
def ensure_admin_or_owner(db, request) -> None   # HTTPException 403 si no es admin/owner
templates = Jinja2Templates(directory="app/templates")

# app/deps.py — YA EXISTE
def session_dep() -> Generator[Session, None, None]

# app/models.py — patrón de modelo (todas las tablas tienen business_id FK a businesses.id)
class Business(Base): __tablename__ = "businesses"; id: Mapped[int]; code: Mapped[str]; name: Mapped[str]
```

**Inventario de archivos:**

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/agent/__init__.py` | CREATE | paquete |
| `app/agent/config.py` | CREATE | env vars (`AGENT_LLM_*`), cliente config |
| `app/agent/llm.py` | CREATE | `ChatOpenAI` con `base_url` + timeout |
| `app/agent/prompts.py` | CREATE | system prompt + descripción de tools |
| `app/agent/schemas.py` | CREATE | schema pydantic de widget (validación) |
| `app/agent/tools.py` | CREATE | 9 tools read-only (payload canónico) que envuelven `InventoryService` |
| `app/agent/graph.py` | CREATE | grafo LangGraph (plan→tools→validate→build→narrate) |
| `app/agent/runner.py` | CREATE | threadpool + semáforo + timeout + retry |
| `app/agent/widgets.py` | CREATE | CRUD `DashboardWidget` + resolución spec→tool para render |
| `app/routers/ui_agent.py` | CREATE | endpoints HTMX del tab + chat + delete |
| `app/templates/partials/tab_agent.html` | CREATE | chat + grid |
| `app/templates/partials/widget_kpi.html` | CREATE | render KPI |
| `app/templates/partials/widget_chart.html` | CREATE | render line/bar (Chart.js) |
| `app/templates/partials/widget_table.html` | CREATE | render table |
| `app/templates/partials/widget_list.html` | CREATE | render list |
| `app/models.py` | MODIFY | añadir `DashboardWidget` |
| `app/main.py` | MODIFY | registrar `ui_agent.router` |
| `app/templates/dashboard.html` | MODIFY | botón sidebar "Copiloto" |
| `requirements.txt` | MODIFY | añadir langgraph, langchain-openai, httpx (aprobación) |
| `app/services/inventory_service.py` | REFERENCE | solo lectura |
| `app/security.py`, `app/routers/ui_common.py`, `app/db.py`, `app/schemas.py` | REFERENCE | solo lectura |

---

## 2.2 Architecture Approach

**Componentes nuevos (`app/agent/`):** un módulo autocontenido con el grafo LangGraph, el
cliente LLM y las tools. El agente vive en el **mismo proceso** que la API, por lo que llama
a `InventoryService` **directamente** (sin MCP, sin red interna).

**Flujo de una petición:**

```
POST /ui/agent/chat (HTMX, message=Form(...))
  → runner.run_agent(message, business_id)   [en threadpool, con semáforo + timeout]
     → graph.invoke({message, business_id, history})
        plan → (tools → validate ↺ retry ≤2) → build → narrate
     → widgets[] (specs) + reply (texto)
  → persistir widgets en dashboard_widgets
  → renderizar partials HTMX (narración + grid)
```

**Inyección de `business_id`:** el router obtiene `business_id = require_active_business_id(db, request)`
y lo pasa al runner. Cada tool recibe un `InventoryService(db, business_id)` ya scoped. El LLM
nunca ve ni decide el `business_id`.

**Persistencia + render:** el grafo ejecuta la tool y **normaliza** el resultado a un **payload
canónico** (`{value,label}` KPI · `{labels,values}` chart · `{columns,rows}` tabla). Ese payload se
guarda en `params` del widget y las plantillas lo renderizan directo (sin Python en el template).
Trade-off documentado en 2.6 (D4).

**Invariantes que se preservan:**
- `business_id` en toda tabla (multi-negocio) — `dashboard_widgets` lo incluye.
- Roles admin/owner/operator — reutiliza `ensure_admin_or_owner`.
- `Base` de `app/db.py` como única base (invariante del proyecto).
- `deps.py` / `security.py` no se modifican (control de acceso centralizado).

---

## 2.3 Data Model

### Entity: DashboardWidget (NUEVA)

| Field | Type | Change | Default | Notes |
|-------|------|--------|---------|-------|
| id | Integer PK | NEW | auto | |
| business_id | FK businesses.id | NEW | NULL | aislamiento multi-negocio |
| type | String(16) | NEW | — | `kpi` \| `line` \| `bar` \| `table` \| `list` |
| title | String(255) | NEW | — | |
| params | Text | NEW | NULL | JSON: `{"tool","tool_args","chart_type","unit"}` |
| position | Integer | NEW | 0 | orden en el grid |
| created_by | FK users.id | NEW | NULL | |
| created_at | DateTime(tz) | NEW | server_default now | |

```python
class DashboardWidget(Base):
    __tablename__ = "dashboard_widgets"
    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(ForeignKey("businesses.id"), nullable=True, index=True)
    type: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(255))
    params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    position: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
```

**Migración:** `Base.metadata.create_all(bind=engine)` en `run_startup_tasks()` crea la tabla
nueva automáticamente (no requiere `ALTER` ni bump de `schema_version`). No hay backfill.
**Rollback:** eliminar la clase + `DROP TABLE dashboard_widgets;` — sin pérdida de datos de negocio.
**Índices:** `business_id` (para filtrar el grid) y `created_at` (orden) — declarados en el modelo.

---

## 2.4 API Contracts

### GET /ui/tabs/agent
**Auth:** cualquier rol autenticado.
**Response 200:** `partials/tab_agent.html` (chat + grid de widgets del negocio activo).

### POST /ui/agent/chat
**Auth:** admin/owner (`ensure_admin_or_owner`); operator → 403.
**Request (form):** `message: str = Form(...)`.
**Response 200 (HTMX):** partial con la narración (bubble) + grid re-renderizado (`hx-swap`/oob).
**Response 403:** `{"detail": "Admin required"}`.
**Response 503:** narración "copiloto no disponible" si el LLM falla.
**Side effect:** persiste `DashboardWidget`(s).

### POST /ui/agent/widget/{widget_id}/delete
**Auth:** admin/owner.
**Response 200 (HTMX):** grid re-renderizado sin el widget.
**Response 403:** operator.
**Response 404:** widget no existe o no pertenece al negocio activo.

### GET /ui/agent/widgets
**Auth:** cualquier rol.
**Response 200 (HTMX):** `partials/widget_grid.html` con los widgets del negocio activo.

---

## 2.5 Security Review

| Check | Answer |
|-------|--------|
| **Nuevo límite de auth?** | Sí — admin/owner construyen (`ensure_admin_or_owner`), operator solo ve |
| **Nuevos datos sensibles?** | No — widgets contienen specs de consulta, no PII |
| **Input validation?** | `message` validado por FastAPI `Form`; widget schema validado por pydantic en el grafo |
| **Nueva superficie de ataque?** | Prompt injection → mitigado: el LLM solo puede llamar tools read-only scoped por `business_id`; nunca SQL; el resultado se trata como texto |
| **Secrets?** | `AGENT_LLM_API_KEY` en env var, nunca en código ni en `params` |
| **OWASP?** | Authorization (roles), IDOR (delete valida `business_id` del widget), SSRF (el `base_url` del LLM es fijo vía env var, no user-controlled), DoS (rate limit + semáforo + timeout) |

---

## 2.6 Trade-offs

| Decision | Elegido | Alternativa | Por qué no la alternativa |
|----------|---------|-------------|---------------------------|
| Orquestación | LangGraph (grafo + retry) | single-shot sin grafo | usuario eligió agente completo; el retry acotado compensa la fiabilidad del LLM |
| Render | guardar **payload normalizado** en `params` (el grafo normaliza la salida de la tool) | re-ejecutar la tool en cada carga | payload plano renderizable sin lógica en template; datos frescos menos críticos a esta escala (se re-consultan al volver a preguntar) |
| Cliente LLM | `langchain-openai` `ChatOpenAI` con `base_url` | `openai` crudo + tool-calls manuales | `ChatOpenAI` da tool-calling integrado y un solo camino para Gemini/DeepSeek/local |
| Aislamiento | `business_id` inyectado por el router | el LLM lo infiere | el LLM no es de confianza para scoping multi-negocio |
| Concurrencia | threadpool (`anyio.to_thread`) + semáforo | endpoints async nativos | el grafo y `InventoryService` son síncronos; threadpool los aísla del event loop |
| Persistencia | tabla `dashboard_widgets` | in-memory/session | el usuario pidió persistencia por negocio |

---

## 2.7 Migration & Rollback Strategy

```
Migración:
1. Añadir DashboardWidget a models.py (create_all crea la tabla en startup, dev y prod).
2. Desplegar código (develop → dev → prod).
3. No hay backfill (tabla nueva, vacía).

Rollback:
1. Revertir el registro del router y la clase.
2. DROP TABLE dashboard_widgets;  (sin pérdida de datos de negocio).
3. Revertir requirements.txt.
```
