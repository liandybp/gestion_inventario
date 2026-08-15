# Phase 3 — Task Envelopes: Refactor Fase B

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T18.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`
> **Regla dura:** mover el código VERBATIM (sin refactorizar durante el movimiento).
> Los números de línea son referencia de partida (pueden variar tras tareas previas — usar `grep -n "def <fn>"` para localizar).

---

## B1 — Dividir ui_tabs.py

### T1: `ui_tabs_common.py` (helpers compartidos)

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_tabs_common.py` (CREATE)

#### Context

```python
# app/routers/ui_tabs.py — helpers a extraer (buscar por nombre, no por línea)
def _deletable_skus(db, skus, *, business_id) -> set[str]        # l.54
def _month_label_es(dt) -> str                                    # l.92
def _location_id_for_code(...) -> int|None                        # l.243
def _location_name_for_code(location_code, db, request) -> str    # l.279
def _normalize_restock_scope(restock_scope) -> str                # l.289
def _restock_items_with_location_history(...)                     # l.293
```

#### Objective

Crear `ui_tabs_common.py` moviendo VERBATIM estos 6 helpers + sus imports necesarios.

#### Instructions

**Step 1:** Crear `app/routers/ui_tabs_common.py` con `from __future__ import annotations`, los imports que los helpers requieren (copiar del bloque de imports de ui_tabs.py los que usen: `sqlalchemy select`, `Session`, modelos que referencien, `datetime`, `get_logger`, `ui_common` items, etc.), y los 6 helpers copiados VERBATIM.
**Step 2:** En `ui_tabs.py`, ELIMINAR los 6 helpers y re-importarlos al inicio:

```python
from .ui_tabs_common import (
    _deletable_skus, _month_label_es, _location_id_for_code,
    _location_name_for_code, _normalize_restock_scope, _restock_items_with_location_history,
)
```

**⚠️ No mover:** `_home_charts_context` (l.110) y `_home_locations_context` (l.229) — van a `ui_home.py` (T2).

#### Verification

```bash
python3 -c "import ast; ast.parse(open('app/routers/ui_tabs_common.py').read())" && echo "SYNTAX OK"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan (ui_tabs sigue funcionando con los imports re-exportados)
```

#### Expected Report

- [ ] `ui_tabs_common.py` con 6 helpers verbatim
- [ ] `ui_tabs.py` importa de ui_tabs_common (sin helpers duplicados)
- [ ] Suite unit — PASS

---

### T2: `ui_home.py` (/, /dashboard, /tabs/home, /home-charts)

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_home.py` (CREATE), `app/routers/ui.py` (MODIFY)

#### Context

```python
# app/routers/ui_tabs.py — mover VERBATIM
@router.get("/")                                       # l.329
@router.get("/dashboard") def dashboard(...) -> HTMLResponse   # l.334
def _home_charts_context(...)                          # l.110
def _home_locations_context(...)                       # l.229
@router.get("/tabs/home") def tab_home(...)            # l.390
@router.get("/home-charts") def home_charts(...)       # l.497
```

#### Objective

Router `ui_home.py` con las 4 rutas de home/dashboard + sus 2 helpers.

#### Instructions

**Step 1:** Crear `app/routers/ui_home.py` con `router = APIRouter()` y copiar VERBATIM: `_home_charts_context`, `_home_locations_context`, la ruta `/` (redirect a /dashboard), `dashboard`, `tab_home`, `home_charts`. Copiar los imports necesarios del bloque de ui_tabs.py (TemplateResponse, get_active_business_config, InventoryService, ui_common items, datetime, etc.).
**Step 2:** En `ui_tabs.py`, ELIMINAR estas funciones.
**Step 3:** En `ui.py`, añadir `from .ui_home import router as home_router` y `router.include_router(home_router)`.

#### Verification

```bash
python3 -c "from app.routers import ui_home; print('import OK')"
python3 -c "from app.main import app; routes=[r.path for r in app.routes]; assert '/ui/dashboard' in routes and '/ui/tabs/home' in routes and '/ui/home-charts' in routes; print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `ui_home.py` creado, rutas registradas en ui.py
- [ ] `/ui/dashboard`, `/ui/tabs/home`, `/ui/home-charts`, `/ui/` responden
- [ ] Suite unit — PASS

---

### T3: `ui_stock.py` (stock/restock/return-supplier)

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_stock.py` (CREATE), `app/routers/ui.py` (MODIFY)

#### Context

```python
# app/routers/ui_tabs.py — mover VERBATIM (localizar por @router.get/post)
@router.get("/stock-table")                            # l.1726
@router.post("/stock/{sku}/delete")                    # l.1773
@router.post("/stock/delete-selected")                 # l.1840
@router.post("/movements/return-supplier")             # l.1923
@router.get("/return-supplier/lots")                   # l.2025
@router.get("/restock-table")                          # l.2066
@router.get("/restock-print")                          # l.2110
```

#### Objective

Router `ui_stock.py` con las 7 rutas de stock/restock/return-supplier.

#### Instructions

**Step 1:** Crear `app/routers/ui_stock.py` con `router = APIRouter()` y copiar VERBATIM las 7 rutas + helpers privados que usen (definidos entre ellas). Importar de `ui_tabs_common`: `_deletable_skus`, `_location_id_for_code`, `_location_name_for_code`, `_normalize_restock_scope`, `_restock_items_with_location_history`, `_month_label_es`. Importar `SupplierReturnLotCreate` de app.schemas.
**Step 2:** Eliminar de `ui_tabs.py`. Registrar en `ui.py`.

#### Verification

```bash
python3 -c "from app.main import app; routes=[r.path for r in app.routes]; assert '/ui/stock-table' in routes and '/ui/restock-table' in routes and '/ui/movements/return-supplier' in routes; print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `ui_stock.py` creado, 7 rutas registradas
- [ ] Suite unit — PASS

---

### T4: `ui_profit_items.py` (profit-items + sub-rutas)

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit_items.py` (CREATE), `app/routers/ui.py` (MODIFY)

#### Context

```python
# app/routers/ui_tabs.py — mover VERBATIM
@router.get("/tabs/profit-items")                       # l.1210
@router.get("/tabs/profit-items/adjustment/{movement_id}/edit")     # l.1230
@router.post("/tabs/profit-items/adjustment/{movement_id}/update")  # l.1263
@router.get("/tabs/profit-items/transfer-in/{movement_id}/edit")    # l.1300
@router.post("/tabs/profit-items/transfer-in/{movement_id}/update") # l.1333
def _render_profit_items_tab(...)                       # l.1381
@router.get("/tabs/profit-items/sale/{movement_id}/edit")           # l.1453
@router.post("/tabs/profit-items/sale/{movement_id}/update")        # l.1488
@router.post("/tabs/profit-items/sale/{movement_id}/delete")        # l.1535
@router.post("/tabs/profit-items/sale/delete-selected")             # l.1562
@router.get("/tabs/profit-items/purchase/{movement_id}/edit")       # l.1640
@router.post("/tabs/profit-items/purchase/{movement_id}/update")    # l.1677
```

#### Objective

Router `ui_profit_items.py` con tab_profit_items + sus 10 sub-rutas + `_render_profit_items_tab`.

#### Instructions

**Step 1:** Crear `app/routers/ui_profit_items.py` (router = APIRouter()) y copiar VERBATIM los 12 handlers + helpers privados que usen. Importar de `ui_tabs_common` lo que necesiten.
**Step 2:** Eliminar de `ui_tabs.py`. Registrar en `ui.py`.

#### Verification

```bash
python3 -c "from app.main import app; assert any(r.path.endswith('/tabs/profit-items') for r in app.routes); print('route OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `ui_profit_items.py` creado (12 handlers)
- [ ] Suite unit — PASS

---

### T5: `ui_history.py` + `ui_activity.py`

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_history.py`, `app/routers/ui_activity.py` (CREATE), `app/routers/ui.py` (MODIFY)

#### Context

```python
# ui_tabs.py — mover VERBATIM
@router.get("/tabs/history") def tab_history(...)                # l.2154
@router.post("/tabs/history/adjustments/delete-selected")        # l.2206
@router.get("/tabs/activity") def tab_activity(...)              # l.2300
```

#### Objective

Routers `ui_history.py` (tab_history + delete-selected) y `ui_activity.py` (tab_activity).

#### Instructions

**Step 1:** Crear ambos archivos (`router = APIRouter()`), copiar VERBATIM, importar de `ui_tabs_common`/`ui_common` lo necesario (p.ej. `tab_activity` consulta `AuditLog` — importar `AuditLog` de app.models).
**Step 2:** Eliminar de ui_tabs.py. Registrar ambos en ui.py.

#### Verification

```bash
python3 -c "from app.main import app; assert any(r.path=='/ui/tabs/history' for r in app.routes) and any(r.path=='/ui/tabs/activity' for r in app.routes); print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `ui_history.py`, `ui_activity.py` creados y registrados
- [ ] Suite unit — PASS

---

### T6: `ui_dividends.py` + `ui_profit.py`

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_dividends.py`, `app/routers/ui_profit.py` (CREATE), `app/routers/ui.py` (MODIFY)

#### Context

```python
# ui_tabs.py — mover VERBATIM
@router.get("/tabs/dividends") def tab_dividends(...)  # l.965
@router.get("/tabs/profit") def tab_profit(...)        # l.1157
```

#### Objective

Routers `ui_dividends.py` (tab_dividends) y `ui_profit.py` (tab_profit).

#### Instructions

**Step 1:** Crear ambos, copiar VERBATIM, importar de `ui_tabs_common` lo necesario (`_month_label_es` para tab_dividends/profit). `tab_dividends` usa `service.monthly_dividends_report` — tras B2 (T15) pasará a `FinanceService`; mientras tanto mantener `InventoryService` (el envelope T15 actualiza).
**Step 2:** Eliminar de ui_tabs.py. Registrar en ui.py.

#### Verification

```bash
python3 -c "from app.main import app; assert any(r.path=='/ui/tabs/dividends' for r in app.routes) and any(r.path=='/ui/tabs/profit' for r in app.routes); print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `ui_dividends.py`, `ui_profit.py` creados y registrados
- [ ] Suite unit — PASS

---

### T7: tab_customers/tab_inventory/tab_purchases/tab_sales/tab_documents → routers existentes

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_customers.py`, `ui_products.py`, `ui_purchases.py`, `ui_sales.py`, `ui_sales_documents.py`, `app/routers/ui.py` (MODIFY)

#### Context

```python
# ui_tabs.py — mover VERBATIM
@router.get("/tabs/customers") def tab_customers(...)   # l.368  → ui_customers.py
@router.get("/tabs/inventory") def tab_inventory(...)   # l.541  → ui_products.py
@router.get("/tabs/purchases") def tab_purchases(...)   # l.590  → ui_purchases.py
@router.get("/tabs/sales") def tab_sales(...)           # l.677  → ui_sales.py
@router.get("/tabs/documents") def tab_documents(...)   # l.828  → ui_sales_documents.py
```

#### Objective

Mover los 5 tab renders a sus routers de dominio existentes.

#### Instructions

**Step 1:** Para cada destino: pegar la función VERBATIM (con su decorador `@router.get("/tabs/...")`), añadir los imports que necesite (copiar del bloque de ui_tabs.py). Mantener el `router` propio del destino (todos usan `router = APIRouter()`).
**Step 2:** Eliminar las 5 funciones de ui_tabs.py.
**Step 3:** `ui.py` ya registra estos routers — NO se añade nada nuevo (las rutas /tabs/* viajan con el router de cada dominio).

#### Verification

```bash
python3 -c "from app.main import app; ps=[r.path for r in app.routes]; assert all(p in ps for p in ['/ui/tabs/customers','/ui/tabs/inventory','/ui/tabs/purchases','/ui/tabs/sales','/ui/tabs/documents']); print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] 5 tab renders movidos a sus routers de dominio
- [ ] Rutas /ui/tabs/* OK
- [ ] Suite unit — PASS

---

### T8: tab_expenses/tab_transfers/tab_users/tab_businesses → routers existentes

**Depends on:** T7
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_expenses.py`, `ui_transfers.py`, `ui_users.py`, `ui_businesses.py` (MODIFY)

#### Context

```python
# ui_tabs.py — mover VERBATIM
@router.get("/tabs/expenses") def tab_expenses(...)      # l.897  → ui_expenses.py
@router.get("/tabs/transfers") def tab_transfers(...)    # l.1018 → ui_transfers.py
@router.get("/tabs/users") def tab_users(...)            # l.2421 → ui_users.py
@router.get("/tabs/businesses") def tab_businesses(...)  # l.2438 → ui_businesses.py
```

#### Objective

Mover los 4 tab renders restantes a sus routers de dominio.

#### Instructions

**Step 1:** Pegar cada función VERBATIM en su destino (imports necesarios). `tab_transfers` usa `_restock_items_with_location_history`? No — usa helpers de transfers; importar de `ui_tabs_common` lo que use (verificar con `grep` dentro de la función).
**Step 2:** Eliminar de ui_tabs.py.

#### Verification

```bash
python3 -c "from app.main import app; ps=[r.path for r in app.routes]; assert all(p in ps for p in ['/ui/tabs/expenses','/ui/tabs/transfers','/ui/tabs/users','/ui/tabs/businesses']); print('routes OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] 4 tab renders movidos
- [ ] Suite unit — PASS

---

### T9: Registrar routers + eliminar `ui_tabs.py`

**Depends on:** T2-T8
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui.py` (MODIFY), `app/routers/ui_tabs.py` (DELETE)

#### Context

```python
# app/routers/ui.py — añadir imports/include_router de los nuevos
from .ui_tabs import router as tabs_router   # ← se elimina
```

#### Objective

`ui.py` registra todos los routers nuevos; `ui_tabs.py` se elimina.

#### Instructions

**Step 1:** En `ui.py`, eliminar `from .ui_tabs import router as tabs_router` y `router.include_router(tabs_router)`. Añadir imports + `include_router` para: `home_router`, `stock_router`, `profit_items_router`, `history_router`, `activity_router`, `dividends_router`, `profit_router`.
**Step 2:** Confirmar que `ui_tabs.py` no tiene más rutas (debe estar vacío o solo con `router = APIRouter()` sin handlers). Si quedó vacío: `git rm app/routers/ui_tabs.py`.
**Step 3:** Verificar que `_log = get_logger(__name__)` y helpers no referencien nada eliminado (grep de imports rotos).

#### Verification

```bash
python3 -c "from app.main import app; print('app imports OK,', len([r for r in app.routes if r.path.startswith('/ui/tabs') or r.path.startswith('/ui/stock')]), 'routes')"
# Expected: app importa sin error y las rutas existen
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -3
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] ui.py registra los 7 routers nuevos; ui_tabs.py eliminado
- [ ] Suite unit + e2e — PASS

---

### T-e2e-1: Smoke e2e de los 16 tabs (valida B1)

**Depends on:** T9
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_tabs_smoke_e2e.py` (CREATE)

#### Context

```python
# tests/e2e/conftest.py — EXISTE: fixture live_server (SQLite temporal + uvicorn + seed stock)
# tests/e2e/test_transfer_bulk_e2e.py — patrón de login: page.goto /ui/login, fill username/password, click submit
# app/templates/dashboard.html — sidebar con id="tab-<slug>" para cada tab
```

#### Objective

E2E que navega los 16 tabs y aserta que cada uno renderiza sin error (caza 404/rutas rotas del split B1).

#### Instructions

**Step 1:** Crear `tests/e2e/test_tabs_smoke_e2e.py`:

```python
from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

# Los 16 tabs del sidebar (dashboard.html). Los de rol admin se prueban con admin.
TABS = [
    "home", "agent", "inventory", "purchases", "transfers", "sales",
    "documents", "customers", "expenses", "dividends", "profit",
    "profit-items", "history", "activity", "users", "businesses",
]


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")


def test_all_tabs_render_without_error(live_server: str, page: Page) -> None:
    base_url = live_server
    _login(page, base_url)

    for slug in TABS:
        tab = page.locator(f"#tab-{slug}")
        expect(tab).to_be_visible(timeout=5000)
        tab.click()
        # La respuesta HTMX rellena #tab-content; asertar que NO es un error
        content = page.locator("#tab-content")
        expect(content).not_to_contain_text("Internal Server Error")
        expect(content).not_to_contain_text("Traceback")
        # Contenido no vacío (el partial se renderizó)
        html = content.inner_html()
        assert len(html.strip()) > 50, f"tab '{slug}' quedó vacío: {html[:120]!r}"
```

**Step 2:** Si algún tab es solo-owner/operator (p.ej. `users`/`businesses` admin-only) y el admin los ve, OK. Si `agent` requiere panel especial, incluirlo igual (debe renderizar el panel).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_tabs_smoke_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: 1 passed (o ajustar TABS si un tab legítimamente devuelve vacío para admin)
```

#### Expected Report

- [ ] `test_tabs_smoke_e2e.py` creado, 16 tabs navegados sin error
- [ ] Smoke — PASS

---

### T10: Hito B1 — verificación completa

**Depends on:** T-e2e-1
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** (verificación)

#### Instructions

```bash
wc -l app/routers/ui_tabs.py  # Expected: no existe o < 200
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
git diff --name-only develop
```

Si `ui_tabs.py` ya no existe, todo pasa y el smoke navega los 16 tabs → **B1 OK**. Reportar el diff de scope.

---

## B2 — Extraer FinanceService + ReportingService

### T11: `FinanceService`

**Depends on:** T10
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/finance_service.py` (CREATE)

#### Context

```python
# app/services/inventory_service.py — métodos a mover (verificar con grep)
create_expense, get_expense, update_expense, delete_expense, list_expenses,
total_expenses, top_expense_concept, create_extraction, get_extraction,
update_extraction, delete_extraction, list_extractions,
total_extractions_by_party, monthly_dividends_report
# Dependencias internas usadas: self._db, self._business_id, self._movement_datetime,
# self._config, self._month_range
```

#### Objective

`FinanceService(db, business_id)` con los 14 métodos movidos VERBATIM.

#### Instructions

**Step 1:** Crear `app/services/finance_service.py`:

```python
"""Finanzas: gastos, extracciones y dividendos (extraído de InventoryService, H-023)."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy.orm import Session
from app.utils import month_range
from app.repositories.business_repository import get_business_config

class FinanceService:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def _movement_datetime(self, provided: Optional[datetime]) -> datetime:
        return provided or datetime.now(timezone.utc)

    def _month_range(self, now: datetime):
        return month_range(now)

    def _config(self):
        return get_business_config(self._db, self._business_id)

    # ── Copiar VERBATIM los 14 métodos desde InventoryService ──
    def create_expense(self, amount, concept, expense_date=None) -> None: ...
    ...
```

**Step 2:** Copiar los 14 métodos VERBATIM (cuerpo sin cambios). `self._business_id` se usa igual.

#### Verification

```bash
python3 -c "from app.services.finance_service import FinanceService; print('import OK')"
# suite aún debe pasar: InventoryService conserva los métodos hasta T13
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `finance_service.py` creado con 14 métodos verbatim
- [ ] Suite unit — PASS (sin cambios en callers aún)

---

### T12: `ReportingService`

**Depends on:** T11
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/reporting_service.py` (CREATE)

#### Context

```python
# inventory_service.py — métodos a mover
sales_by_product, sales_metrics_table, daily_sales_series,
monthly_profit_report, monthly_profit_items_report, monthly_overview
```

#### Objective

`ReportingService(db, business_id)` con los 6 métodos movidos VERBATIM.

#### Instructions

**Step 1:** Crear `app/services/reporting_service.py` con la misma estructura de T11 (constructor + `_month_range` + `_config` + los 6 métodos verbatim). Importar lo que usen (p.ej. `InventoryMovement`, `InventoryLot` de app.models, `get_business_config`, `month_range`).

#### Verification

```bash
python3 -c "from app.services.reporting_service import ReportingService; print('import OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `reporting_service.py` creado con 6 métodos verbatim
- [ ] Suite unit — PASS

---

### T13: InventoryService — eliminar métodos movidos + `@property business_id`

**Depends on:** T12
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/inventory_service.py` (MODIFY)

#### Objective

Eliminar los 20 métodos movidos de `InventoryService`; exponer `business_id`.

#### Instructions

**Step 1:** Eliminar de `InventoryService` los métodos que moviste en T11/T12 (los 20).
**Step 2:** Añadir propiedad pública:

```python
    @property
    def business_id(self) -> int:
        return self._business_id
```

**Step 3:** `git diff` para confirmar que NO se eliminó ningún método de inventario core (purchase/sale/transfer/adjustment/stock/fifo/update/delete/recent/movement_history/inventory_value*).

#### Verification

```bash
wc -l app/services/inventory_service.py  # Expected: < 2300
python3 -c "from app.services.inventory_service import InventoryService; print('import OK')"
# suite: los callers aún usan InventoryService para estos métodos → deben fallar hasta T14/T15
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# ⚠️ Pueden fallar tests de expenses/extractions/agent — NORMAL, T14/T15 los arregla.
```

#### Expected Report

- [ ] InventoryService sin los 20 métodos; `business_id` property
- [ ] `wc -l` < 2300

---

### T14: Callers UI → FinanceService/ReportingService

**Depends on:** T13
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_expenses.py`, `ui_extractions.py`, `ui_dividends.py`, `ui_profit.py`, `ui_profit_items.py` (MODIFY)

#### Objective

Los routers de expense/extraction/dividends/profit usan `FinanceService`/`ReportingService`.

#### Instructions

**Step 1 — `ui_expenses.py`:** `service = InventoryService(db, bid)` → `service = FinanceService(db, bid)`. Añadir import `from app.services.finance_service import FinanceService`.
**Step 2 — `ui_extractions.py`:** idem → `FinanceService(db, bid)`.
**Step 3 — `ui_dividends.py`:** `service = InventoryService(...)` → `FinanceService(db, bid)` (usa `monthly_dividends_report`).
**Step 4 — `ui_profit.py` y `ui_profit_items.py`:** `InventoryService(...)` → `ReportingService(db, bid)` (import de `app.services.reporting_service`). ⚠️ Si algún handler de profit_items usa métodos de inventario core (stock/transfer), mantener `InventoryService` y construir `ReportingService` ADICIONAL para los de reporting.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q tests/unit/test_inventory_service_unit.py --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Expected: todos pasan (los callers apuntan a los services correctos)
```

#### Expected Report

- [ ] ui_expenses/ui_extractions/ui_dividends → FinanceService
- [ ] ui_profit/ui_profit_items → ReportingService
- [ ] Suite unit — PASS

---

### T15: agent/tools.py + hito B2

**Depends on:** T14
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/agent/tools.py` (MODIFY)

#### Context

```python
# app/agent/tools.py — tool functions reciben `service` (InventoryService)
# y llaman a métodos que ahora viven en FinanceService/ReportingService
def fn(service, args):
    rep = service.monthly_profit_report(...)   # ← ReportingService
    rows = service.list_expenses(...)          # ← FinanceService
```

#### Objective

Las tools construyen `FinanceService`/`ReportingService` desde `service`.

#### Instructions

**Step 1:** En `app/agent/tools.py`, añadir imports de `finance_service`/`reporting_service`. En cada tool que use un método de finance/reporting, construir el sub-service:

```python
from app.services.finance_service import FinanceService
from app.services.reporting_service import ReportingService

# dentro de la tool function, cuando llame a un método de reporting/finance:
rep = ReportingService(service.db, service.business_id).monthly_profit_report(...)
# o reutilizando una instancia por-call si hay varios
```

**Step 2:** NO cambiar la firma de las tools (siguen recibiendo `service`). NO tocar las tools de inventario (stock/transfer/etc.).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q tests/unit/test_agent_tools.py tests/unit/test_agent_chat.py tests/unit/test_agent_graph.py --no-cov 2>&1 | tail -3
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
# Hito B2:
wc -l app/services/inventory_service.py  # < 2300
```

#### Expected Report

- [ ] agent/tools.py construye FinanceService/ReportingService
- [ ] Tests de agent — PASS
- [ ] **Hito B2 OK** (inventory_service < 2300 ln, suite verde)

---

### T-e2e-2: Flujos críticos post-B2 (valida services extraídos)

**Depends on:** T15
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_flows_e2e.py` (CREATE)

#### Context

```python
# tests/e2e/conftest.py — live_server siembra stock E2E-SKU1 (10 uds) / E2E-SKU2 (5 uds) en CENTRAL del negocio 'recambios'
# tabs: #tab-expenses, #tab-purchases, #tab-sales, #tab-dividends, #tab-profit-items
# Los forms HTMX swap en #tab-content; los selectores dependen de cada partial
```

#### Objective

E2E que ejerce los métodos MOVIDOS (FinanceService/ReportingService) a través de la UI: gasto, extracción, venta, compra y profit-items. Valida que la extracción B2 no rompió el comportamiento.

#### Instructions

**Step 1:** Crear `tests/e2e/test_flows_e2e.py` con login admin + los flujos. Para cada flujo, seguir el patrón: abrir tab → llenar form → submit → asertar que aparece en la lista.

```python
from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")


def test_expense_flow(live_server: str, page: Page) -> None:
    """Gasto creado aparece en la lista (FinanceService)."""
    _login(page, live_server)
    page.click("#tab-expenses")
    expect(page.locator("#tab-content")).to_contain_text("Gastos")
    # Llenar el form de gasto (nombre de campos según partial tab_expenses)
    # page.fill('input[name="concept"]', "Gasto E2E")
    # page.fill('input[name="amount"]', "12.50")
    # page.click('#tab-content button[type="submit"]')
    # expect(page.locator("#tab-content")).to_contain_text("Gasto E2E")
```

> ⚠️ **Completar los selectores reales de cada form** leyendo el partial del tab
> (app/templates/partials/tab_expenses.html, tab_purchases.html, tab_sales.html,
> tab_dividends.html, tab_profit_items.html). NO inventar nombres de campos:
> usar los `name=` reales. Si un flujo es demasiado complejo (p.ej. venta con
> barcode), cubrir al menos: gasto, extracción y profit-items (los 3 tocan
> services extraídos); venta/compra como bonus si los selectores son simples.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_flows_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: los flujos implementados pasan (al menos gasto + extracción + profit-items)
```

#### Expected Report

- [ ] `test_flows_e2e.py` creado; flujos gasto + extracción + profit-items pasan
- [ ] Selectores tomados de los partials reales (no inventados)

---

## B3 — LocationRepository

### T16: `LocationRepository`

**Depends on:** T10
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/repositories/location_repository.py` (CREATE)

#### Context

```python
# Tres implementaciones de "code → id" divergentes:
#   inventory_service.py:65 _location_id_for_code (lanza HTTPException, auto-crea)
#   ui_tabs.py:243 (retorna None, no auto-crea) — AHORA en ui_tabs_common.py
#   ui_sales_documents.py:183 (retorna None, auto-crea)
```

#### Objective

`LocationRepository.location_id_for_code` única fuente con el comportamiento del service (auto-create + HTTPException).

#### Instructions

**Step 1:** Crear `app/repositories/location_repository.py`:

```python
"""Resolución de ubicaciones: código → id (única fuente de verdad, H-049)."""
from __future__ import annotations
from typing import Optional
from fastapi import HTTPException
from sqlalchemy import and_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from app.models import Location
from app.repositories.business_repository import get_business_config


class LocationRepository:
    def __init__(self, db: Session, business_id: int, business_code: Optional[str] = None):
        self._db = db
        self._business_id = int(business_id)
        self._business_code = business_code or ""

    def location_id_for_code(self, code: str, *, auto_create: bool = True) -> int:
        """Resuelve código→id. Si no existe y auto_create, lo crea (flush)."""
        # (Copiar el cuerpo de inventory_service._location_id_for_code VERBATIM,
        #  con self._business_id/self._db; usar get_business_config para el nombre)
        ...
```

**Step 2:** Copiar el cuerpo de `inventory_service._location_id_for_code` adaptando `self._config()` → `get_business_config(self._db, self._business_id)`.

#### Verification

```bash
python3 -c "from app.repositories.location_repository import LocationRepository; print('import OK')"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] `location_repository.py` con `location_id_for_code`

---

### T17: Migrar callers a LocationRepository

**Depends on:** T16
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/inventory_service.py`, `app/routers/ui_sales_documents.py`, `app/routers/ui_tabs_common.py` (MODIFY)

#### Objective

Los 3 callers usan `LocationRepository`.

#### Instructions

**Step 1 — inventory_service.py:** en `__init__`, `self._locations = LocationRepository(db, business_id, business_code)`; reemplazar `_location_id_for_code` por delegación:

```python
    def _location_id_for_code(self, code: str) -> int:
        return self._locations.location_id_for_code(code, auto_create=True)
```

**Step 2 — ui_sales_documents.py:** `_location_id_for_code(db, *, location_code, business_id)` → construir `LocationRepository(db, business_id)` y llamar `location_id_for_code(location_code, auto_create=...)` (comportamiento previo: auto-crea en algunos flujos; revisar el call site).
**Step 3 — ui_tabs_common.py:** `_location_id_for_code` → `LocationRepository(...).location_id_for_code(code, auto_create=False)` (ui_tabs no auto-creaba).

#### Verification

```bash
grep -rn "def _location_id_for_code\|location_id_for_code" app/ --include="*.py" | grep -v location_repository.py
# Expected: inventory_service delega; ui_sales_documents/ui_tabs_common usan LocationRepository; SIN copias de la lógica
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
```

#### Expected Report

- [ ] 3 callers migrados; 1 sola implementación de resolución
- [ ] Suite unit + e2e — PASS

---

### T18: Hito B3 + verificación completa

**Depends on:** T17
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** (verificación)

#### Instructions

```bash
wc -l app/routers/ui_tabs.py            # no existe o < 200
wc -l app/services/inventory_service.py  # < 2300
grep -rn "os.getenv" app/ --include="*.py" | grep -v settings.py || true  # Fase A intacta
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
git diff --name-only develop
```

Reportar: hitos B1/B2/B3 confirmados, suite verde, scope = design.md §5.
