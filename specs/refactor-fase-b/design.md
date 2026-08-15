# Fase B — Design (SDD Phase 2)

> Refactor estructural para subir el score de la auditoría: SoC (dim1 2→4) y
> Capas (dim3 2→4). Hallazgos: H-026 (god router `ui_tabs.py`), H-023 (god
> service `inventory_service.py`), H-028 (13/18 routers con ORM directo), H-049
> (triple `_location_id_for_code`).

## D1 — Dividir `ui_tabs.py` (2437 ln) en routers por dominio

Rutas se conservan EXACTAS (cliente/HTMX no cambian). Los routers nuevos se
registran en `app/routers/ui.py` (prefijo `/ui`).

### Mapa de movimiento (rutas → router destino)

| Handler(s) | Ruta(s) | Mueve a |
|------------|---------|---------|
| `dashboard`, `/` (redirect), `tab_home`, `/home-charts` | `/`, `/dashboard`, `/tabs/home`, `/home-charts` | **`ui_home.py` (nuevo)** |
| `tab_customers` | `/tabs/customers` | `ui_customers.py` |
| `tab_inventory` | `/tabs/inventory` | `ui_products.py` |
| `tab_purchases` | `/tabs/purchases` | `ui_purchases.py` |
| `tab_sales` | `/tabs/sales` | `ui_sales.py` |
| `tab_documents` | `/tabs/documents` | `ui_sales_documents.py` |
| `tab_expenses` | `/tabs/expenses` | `ui_expenses.py` |
| `tab_transfers` | `/tabs/transfers` | `ui_transfers.py` |
| `tab_dividends` | `/tabs/dividends` | **`ui_dividends.py` (nuevo)** |
| `tab_profit` | `/tabs/profit` | **`ui_profit.py` (nuevo)** |
| `tab_profit_items` + sub-rutas | `/tabs/profit-items*` | **`ui_profit_items.py` (nuevo)** |
| `tab_history` + `history/adjustments/delete-selected` | `/tabs/history*` | **`ui_history.py` (nuevo)** |
| `tab_activity` | `/tabs/activity` | **`ui_activity.py` (nuevo)** |
| `tab_users` | `/tabs/users` | `ui_users.py` |
| `tab_businesses` | `/tabs/businesses` | `ui_businesses.py` |
| stock/restock (`/stock-table`, `/stock/{sku}/delete`, `/stock/delete-selected`, `/movements/return-supplier`, `/return-supplier/lots`, `/restock-table`, `/restock-print`) | `/stock-table`, `/stock/*`, `/movements/*`, `/restock-*` | **`ui_stock.py` (nuevo)** |
| Helpers compartidos | — | **`ui_tabs_common.py` (nuevo)** |

### Helpers compartidos → `ui_tabs_common.py`

`_location_id_for_code`, `_location_name_for_code`, `_deletable_skus`,
`_normalize_restock_scope`, `_restock_items_with_location_history`,
`_month_label_es`. Cada router que los use importa de `ui_tabs_common`.

### `ui_tabs.py` resultante

Queda mínimo: ~150 ln con `router = APIRouter()` + re-export de nada; se ELIMINA
después de B1 (los routers nuevos quedan registrados en ui.py). **Verificación:**
`git diff --stat` muestra `ui_tabs.py` borrado o < 200 ln.

## D2 — Extraer `FinanceService` y `ReportingService` de `InventoryService` (3066 ln)

Verificado: NINGÚN método de finance/reporting usa `self._inventory` ni
`self._consume_fifo` (FIFO core). Dependencias internas: `_movement_datetime`,
`_config`, `_month_range` (triviales, se replican o importan de `utils`).

| Nuevo service | Archivo | Métodos que recibe (desde InventoryService) |
|---------------|---------|----------------------------------------------|
| `FinanceService(db, business_id)` | `app/services/finance_service.py` | `create_expense`, `get_expense`, `update_expense`, `delete_expense`, `list_expenses`, `total_expenses`, `top_expense_concept`, `create_extraction`, `get_extraction`, `update_extraction`, `delete_extraction`, `list_extractions`, `total_extractions_by_party`, `monthly_dividends_report` |
| `ReportingService(db, business_id)` | `app/services/reporting_service.py` | `sales_by_product`, `sales_metrics_table`, `daily_sales_series`, `monthly_profit_report`, `monthly_profit_items_report`, `monthly_overview` |

`InventoryService` conserva: FIFO core, stock, purchase/sale/transfer/adjustment,
update/delete de movimientos, `recent_*`, `movement_history`,
`inventory_value_total`, `inventory_sale_value_total`, `reset_purchases_and_sales`,
helpers privados.

### Callers a actualizar

| Caller | Uso actual | Nuevo |
|--------|-----------|-------|
| `ui_expenses.py` | `service = InventoryService(db, bid)` → expense methods | `FinanceService(db, bid)` |
| `ui_extractions.py` | `service` → extraction + dividends | `FinanceService(db, bid)` |
| `ui_dividends.py` (nuevo, desde ui_tabs) | `service.monthly_dividends_report` | `FinanceService(db, bid)` |
| `ui_profit.py` / `ui_profit_items.py` (nuevos) | reporting methods | `ReportingService(db, bid)` |
| `app/agent/tools.py` | `service.<reporting|finance>` sobre InventoryService | construir `ReportingService`/`FinanceService` con `service.db` + `service.business_id` (exponer `business_id` público) |

> `InventoryService` expone `@property business_id` (además de `db`) para que las
> tools construyan los sub-services.

## D3 — Capas: `LocationRepository` + mover resolución de ubicación (H-049)

- Nuevo `app/repositories/location_repository.py` con `location_id_for_code(db, business_id, code, *, auto_create=False)` y `location_name_for_code(db, request, code)`.
- Migrar los 3 callers de `_location_id_for_code` (inventory_service, ui_tabs_common, ui_sales_documents) a este repo.
- `inventory_service._location_id_for_code` delega en el repo (mantiene la firma pública `(code) -> int`).
- Beneficio: una sola fuente de verdad para "code → id (con prefijo business + auto-create)" → elimina la divergencia (auto-create vs None) que causó el NameError de prod.

## D4 — Fuera de scope de la Fase B

- Migración COMPLETA de todo el ORM de los 13 routers (H-028 total) → Fase B2 (los routers destino de B1 conservan su ORM; solo se centraliza la resolución de ubicaciones).
- `docker-compose*.yml`, `app/auth.py`, `app/deps.py` — invariantes, no se tocan.
- Caché/paginación (H-033), e2e en CI — otras fases.

## D5 — Estrategia de verificación e2e post-refactor (REQ-8/9/10)

Un refactor estructural NO se valida solo con unit tests (los unit tests llaman
funciones directamente; un router mal registrado o un import roto les pasa por
alto si no se importa el router). Por eso la Fase B añade 2 e2e:

### D5.1 — Smoke de tabs (`tests/e2e/test_tabs_smoke_e2e.py`)

Login admin → para CADA tab del sidebar (`#tab-home`, `#tab-inventory`,
`#tab-purchases`, `#tab-transfers`, `#tab-sales`, `#tab-documents`,
`#tab-customers`, `#tab-expenses`, `#tab-dividends`, `#tab-profit`,
`#tab-profit-items`, `#tab-history`, `#tab-activity`, `#tab-users`,
`#tab-businesses`, `#tab-agent`) clickear y asertar que `#tab-content` se
rellena sin error (sin "Internal Server Error", contenido no vacío, no 404).

- Caza: rutas rotas/404 del split B1, templates con variables eliminadas,
  imports circulares en el arranque del router.
- Nota: algunos tabs requieren datos (p.ej. profit-items) — la aserción es
  "renderiza sin error", no "tiene filas".

### D5.2 — Flujos críticos post-B2 (`tests/e2e/test_flows_e2e.py`)

Ejercita los métodos MOVIDOS a través de la UI (con stock sembrado por el
conftest en CENTRAL):

| Flujo | UI | Método movido que valida |
|-------|----|--------------------------|
| Crear gasto → aparece en la lista | tab Expenses | `FinanceService.create_expense`/`list_expenses` |
| Crear extracción → summary de dividendos | tab Dividends/Extracciones | `FinanceService.create_extraction`/`monthly_dividends_report` |
| Crear venta → stock decrece + aparece en recientes | tab Sales | `InventoryService.sale` (core intacto) |
| Crear compra → stock crece | tab Purchases | `InventoryService.purchase` (core intacto) |
| Abrir profit-items → tabla se renderiza | tab Profit-items | `ReportingService.monthly_profit_items_report` |

Los flujos de sale/purchase validan que el core NO se rompió (el InventoryService
sigue funcionando tras perder finance/reporting). Los de expense/extraction/profit
validan los services extraídos.

## 4. Firmas reales del repo (NO modificar salvo la tarea que lo indica)

```python
# app/routers/ui.py — EXISTE (registro de routers)
router = APIRouter(prefix="/ui", tags=["ui"])
router.include_router(tabs_router)   # ← se añaden los routers nuevos

# app/routers/ui_tabs.py — EXISTE (a dividir)
router = APIRouter()  # sin prefix; montado bajo /ui
@router.get("/tabs/home") ... def tab_home(request, db=Depends(session_dep)): -> HTMLResponse
@router.get("/dashboard") ... def dashboard(request, db=Depends(session_dep)): -> HTMLResponse
@router.get("/stock-table") ... (1726)
@router.get("/tabs/profit-items") ... (1210)

# app/services/inventory_service.py — EXISTE (a extraer)
class InventoryService:
    def __init__(self, db, business_id=None): self._db=db; self._business_id=int(business_id); self._products=...; self._inventory=...
    @property def db(self): return self._db
    def create_expense(self, amount, concept, expense_date=None) -> None: ...
    def list_expenses(self, start=None, end=None, limit=200): ...
    def monthly_dividends_report(self, now=None): ...
    def monthly_profit_report(self, ...): ...
    # (métodos a mover: ver D2)

# app/agent/tools.py — EXISTE (caller)
#   Las tool functions reciben `service` (InventoryService). Ej:
#   def fn(service, args): rep = service.monthly_profit_report(...)

# app/services/reporting_service.py — NO EXISTE (a crear)
# app/services/finance_service.py — NO EXISTE (a crear)
# app/repositories/location_repository.py — NO EXISTE (a crear)

# app/utils.py — EXISTE
def month_range(now) -> tuple[datetime, datetime]: ...
```

## 5. Inventario de archivos (scope duro)

### CREATE
- `app/routers/ui_home.py`, `ui_stock.py`, `ui_profit_items.py`, `ui_history.py`,
  `ui_activity.py`, `ui_dividends.py`, `ui_profit.py`, `ui_tabs_common.py`
- `app/services/finance_service.py`, `app/services/reporting_service.py`
- `app/repositories/location_repository.py`
- `tests/e2e/test_tabs_smoke_e2e.py` (D5.1 — smoke de 16 tabs)
- `tests/e2e/test_flows_e2e.py` (D5.2 — flujos críticos post-B2)

### MODIFY
- `app/routers/ui.py` (registrar routers nuevos)
- `app/routers/ui_tabs.py` (se vacía → DELETE tras B1)
- `app/routers/ui_customers.py`, `ui_products.py`, `ui_purchases.py`, `ui_sales.py`,
  `ui_sales_documents.py`, `ui_expenses.py`, `ui_transfers.py`, `ui_users.py`,
  `ui_businesses.py` (reciben su `tab_*` + imports)
- `app/services/inventory_service.py` (pierde finance/reporting; delega en LocationRepository)
- `app/routers/ui_extractions.py` (usa FinanceService)
- `app/agent/tools.py` (construye FinanceService/ReportingService)
- `app/routers/ui_sales_documents.py` (usa LocationRepository)
- `app/routers/ui_common.py` (si aplica, helpers de location)

### NO SE TOCAN
`docker-compose*.yml`, `app/auth.py`, `app/deps.py`, `app/db.py`, `app/models.py`,
`app/schemas.py`, `app/templates/*`, `app/agent/graph.py`, `app/agent/runner.py`,
`app/agent/llm.py`, `app/agent/widgets.py`, `app/routers/ui_auth.py`,
`app/routers/ui_agent.py`.
