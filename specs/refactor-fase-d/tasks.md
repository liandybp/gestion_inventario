# Phase 3 — Task Envelopes: Refactor Fase D

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T6.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`

---

### T1: API contract tests (RBAC + schemas de todos los endpoints)

**Depends on:** none
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_api_contracts_e2e.py` (CREATE)

#### Context

```python
# tests/e2e/test_rbac_e2e.py — EXISTE (patrón: live_server + page.request + login admin/operator)
# tests/e2e/conftest.py — siembra admin/admin y operator/operator
# Endpoints: /products (GET,POST), /movements/{purchase,sale,transfer,adjustment,return-supplier-lot}, /stock, /stock/{sku}
```

#### Objective

Contract tests que asertan status code + schema para cada endpoint API, con admin y operator.

#### Instructions

**Step 1:** Crear `tests/e2e/test_api_contracts_e2e.py`:

```python
from __future__ import annotations

import json
import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page

pytestmark = pytest.mark.e2e

ADMIN = ("admin", "admin")
OPERATOR = ("operator", "operator")


def _login(page: Page, live_server: str, user: tuple[str, str]) -> None:
    u, p = user
    page.goto(f"{live_server}/ui/login")
    page.fill('input[name="username"]', u)
    page.fill('input[name="password"]', p)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{live_server}/ui/dashboard**")


def _post_json(page: Page, live_server: str, path: str, payload: dict):
    return page.request.post(
        f"{live_server}{path}",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


def test_admin_rbac_and_schemas(live_server: str, page: Page) -> None:
    """Admin accede a todo; los endpoints devuelven los campos esperados (no 5xx)."""
    _login(page, live_server, ADMIN)

    # purchase → MovementResult {movement, stock_after, ...}
    r = _post_json(page, live_server, "/movements/purchase", {
        "sku": "E2E-SKU1", "quantity": 2, "unit_cost": 1.0,
        "location_code": "CENTRAL",
    })
    assert r.status in (200, 201), f"purchase admin: {r.status}"
    body = r.json()
    assert "movement" in body, f"schema purchase: {body}"

    # sale → MovementResult
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 1, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert r.status in (200, 201), f"sale admin: {r.status}"

    # stock list → list[StockRead]
    r = page.request.get(f"{live_server}/stock")
    assert r.status == 200, f"stock admin: {r.status}"
    rows = r.json()
    assert isinstance(rows, list), "stock debe ser lista"
    if rows:
        assert "sku" in rows[0] and "quantity" in rows[0], f"schema stock: {rows[0]}"

    # products list → list[ProductRead]
    r = page.request.get(f"{live_server}/products")
    assert r.status == 200


def test_operator_rbac_on_sensitive_endpoints(live_server: str, page: Page) -> None:
    """Operator → 403 en ajuste/traspaso/producto; NO 403 en sale/stock/products GET."""
    _login(page, live_server, OPERATOR)

    for path, payload in [
        ("/movements/adjustment", {"sku": "E2E-SKU1", "quantity": 1, "location_code": "CENTRAL"}),
        ("/movements/transfer", {"from_location_code": "CENTRAL", "to_location_code": "POS1",
                                 "lines": [{"sku": "E2E-SKU1", "quantity": 1}]}),
        ("/products", {"sku": "X", "name": "X", "default_sale_price": 1.0}),
    ]:
        r = _post_json(page, live_server, path, payload)
        assert r.status == 403, f"{path}: esperado 403, recibido {r.status}"

    # sale y GET → no 403
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 1, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert r.status != 403, "operator debería vender"
    assert page.request.get(f"{live_server}/stock").status != 403
```

> ⚠️ Los payloads deben coincidir con los schemas reales (`PurchaseCreate`, `SaleCreate`,
> `AdjustmentCreate`, `TransferCreate`, `ProductCreate`) de `app/schemas.py` — leerlos antes,
> NO inventar campos. Si un campo es obligatorio distinto, ajustar el payload.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_api_contracts_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: 2 passed
```

---

### T2: API error paths (4xx, no 5xx)

**Depends on:** T1
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_api_contracts_e2e.py` (MODIFY)

#### Context

Mismo archivo. El admin tiene stock E2E-SKU1 (CENTRAL) y E2E-SKU2.

#### Objective

Payloads inválidos devuelven 4xx (no 5xx): SKU inexistente, cantidad 0, stock insuficiente.

#### Instructions

**Step 1:** Añadir al archivo:

```python
def test_api_error_paths_return_4xx(live_server: str, page: Page) -> None:
    _login(page, live_server, ADMIN)

    # SKU inexistente → 4xx
    r = _post_json(page, live_server, "/movements/adjustment", {
        "sku": "NO_EXISTE", "quantity": 1, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"SKU inexistente: {r.status}"

    # cantidad 0 → 4xx
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 0, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"cantidad 0: {r.status}"

    # stock insuficiente → 4xx
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 9999, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"stock insuficiente: {r.status}"
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_api_contracts_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: 3 passed
```

---

### T3: Extender e2e a clientes, compras y documentos

**Depends on:** none
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_flows_e2e.py` (MODIFY)

#### Context

`test_flows_e2e.py` ya cubre gasto/extracción/profit-items/venta. Leer los partials de
`tab_customers`, `tab_purchases`, `tab_documents` para los `name=` reales de los forms.

#### Objective

Añadir 3 flujos: crear cliente, crear compra, emitir documento de venta.

#### Instructions

**Step 1:** Leer `app/templates/partials/tab_customers.html`, `tab_purchases.html`, `tab_documents.html`
(o sus sub-partials) para los campos reales.

**Step 2:** Añadir al archivo (con selectores reales, NO inventados):

```python
def test_customer_create_flow(live_server: str, page: Page) -> None:
    """Cliente creado desde la UI aparece en la lista."""
    _login(page, live_server)
    page.click("#tab-customers")
    # llenar form (name= según partial) y submit
    # expect: el nombre del cliente aparece en #tab-content


def test_purchase_flow_increases_stock(live_server: str, page: Page) -> None:
    """Compra creada aumenta el stock del producto."""
    _login(page, live_server)
    page.click("#tab-purchases")
    # llenar form (producto E2E-SKU2, cantidad, coste) y submit
    # expect: mensaje de éxito / el movimiento aparece


def test_document_issue_flow(live_server: str, page: Page) -> None:
    """Emitir un documento de venta crea el doc."""
    _login(page, live_server)
    page.click("#tab-documents")
    # añadir ítem al carrito (E2E-SKU1) y emitir
    # expect: el documento aparece en recientes
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_flows_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: 7 passed (4 existentes + 3 nuevos)
```

---

### T4: LLM retry con backoff/jitter

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/graph.py`, `app/settings.py`

#### Context

```python
# graph.py — EXISTE. En _plan_node:
    for _ in range(config.MAX_RETRIES + 1):
        try:
            plan = _PLANNER.invoke(messages)
            breaker.record_success() ...
        except (OutputParserException, ValidationError) as exc:
            error = str(exc); messages.append(...)
        except Exception as exc:
            breaker.record_failure(); ...; return state
```

#### Objective

Backoff exponencial con jitter entre reintentos (no inmediatos).

#### Instructions

**Step 1 — settings.py:** añadir:

```python
AGENT_RETRY_BASE_S = float(env("AGENT_RETRY_BASE_S", "1.0"))
AGENT_RETRY_MAX_S = float(env("AGENT_RETRY_MAX_S", "5.0"))
AGENT_RETRY_JITTER = float(env("AGENT_RETRY_JITTER", "0.5"))
```

**Step 2 — graph.py:** en el loop de retry, antes del `try:` (después del `for`), añadir:

```python
    import time as _time
    import random as _random
    for attempt in range(config.MAX_RETRIES + 1):
        if attempt > 0:
            base = min(config.RETRY_BASE_S * (2 ** (attempt - 1)), config.RETRY_MAX_S)
            _time.sleep(base + _random.uniform(0, config.RETRY_JITTER))
        try:
            ...
```

**Step 3 — app/agent/config.py:** re-exportar `AGENT_RETRY_BASE_S as RETRY_BASE_S`, etc. (patrón existente).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_agent_graph.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T5: Rate limiter login keyed por (IP real + username)

**Depends on:** none
**Skill:** security-architect
**Agent:** coder
**Files:** `app/routers/ui_auth.py`

#### Context

```python
# ui_auth.py — EXISTE
client_ip = request.client.host if request.client else "unknown"
if not _check_login_rate(client_ip): ...
def _check_login_rate(client_ip: str) -> bool:  # keyea _login_attempts por client_ip
```

#### Objective

El bucket se keyea por `(real_ip, username)`; IP real desde X-Forwarded-For (fallback client.host).

#### Instructions

**Step 1:** Añadir helper:

```python
def _real_ip(request) -> str:
    """IP real detrás del proxy: primer valor de X-Forwarded-For, si no client.host."""
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"
```

**Step 2:** En los dos puntos donde se hace `_check_login_rate(client_ip)`, reemplazar por:

```python
    ip = _real_ip(request)
    username = (username or "").strip().lower()
    key = f"{ip}|{username}"
    if not _check_login_rate(key): ...
```

**Step 3:** `_check_login_rate` recibe `key: str` y keyea `_login_attempts` por `key` (misma lógica).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_auth.py tests/unit/test_auth.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T6: Verificación completa de la Fase D

**Depends on:** T1-T5
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** (verificación)

#### Instructions

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
git diff --name-only develop
```

Reportar: contract tests (≥3), e2e extendidos (≥7 flows), backoff LLM, rate limiter IP+user, suite verde, scope = design.md §5.

---

### T7: Fix panel de ventas para rol operator (quirk detectado)

**Depends on:** none
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`, `app/templates/partials/sale_panel.html`

#### Context

```python
# Quirk (no es regresión de Fase C, es pre-existente): al vender como OPERATOR,
# la venta SÍ se crea (API 200, movimiento persistido) PERO el panel re-renderizado
# no muestra el mensaje de éxito ni el SKU en "ventas recientes" (#sale-panel vacío de
# esos datos). Como admin el panel sí los muestra.
# app/routers/ui_sales.py — handler sale() renderiza sale_panel.html
# app/templates/partials/sale_panel.html — panel de ventas
```

#### Objective

Que el panel de ventas funcione igual para `operator` que para `admin` (mensaje de éxito + ventas recientes visibles).

#### Instructions

**Step 1:** Reproducir: login como `operator/operator` → tab Sales → vender E2E-SKU1 en CENTRAL → el panel se renderiza sin mensaje ni recientes.

**Step 2:** Diagnóstico (leer antes de cambiar): el handler `sale()` (ui_sales.py) renderiza `sale_panel.html` con `recent_sales(limit=20)` y el `message`. Verificar si:
- `service.recent_sales()` devuelve vacío para operator (¿filtra por business que no resuelve?),
- o `_sales_doc_context()` falla para operator,
- o el template `sale_panel.html` condiciona el bloque de recientes por `user.role`.

**Step 3:** Aplicar el fix mínimo que hace visible el mensaje y las recientes para operator (sin cambiar el comportamiento de admin). Añadir un e2e de regresión en `test_flows_e2e.py`:

```python
def test_operator_sale_panel_shows_message_and_recent(live_server: str, page: Page) -> None:
    """Regresión: el panel de ventas del operador muestra éxito + recientes."""
    _login(page, live_server, "operator", "operator")
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.fill('input[name="quantity"]', "1")
    page.fill('input[name="unit_price"]', "2.00")
    page.click('#sale-panel form[hx-post="/ui/sale"] button[type="submit"]')
    # El panel debe mostrar el mensaje de éxito y la venta en recientes
    expect(page.locator("#sale-panel").locator("text=Venta registrada")).to_be_visible()
    expect(page.locator("#sale-panel").locator("text=E2E-SKU1").first).to_be_visible()
```

> ⚠️ Requiere `_login` que acepte username/password (como en test_rbac_e2e.py).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_flows_e2e.py -m e2e --no-cov -q 2>&1 | tail -3
# Expected: 8 passed (7 existentes + regresión del panel operator)
```
