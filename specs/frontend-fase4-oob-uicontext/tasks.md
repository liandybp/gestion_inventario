# Task Envelopes — Frontend fase 4: plantillas OOB, `UIContext` y sidebar data-driven

Fuente: `requirements.md` (REQ-1..12) y `design.md` (verificado 2026-11-01). Rama base `develop` `b741a46`.
Rama de implementación: `feature/frontend-fase4`. Sin bump de versión (release agrupado 0.5.0).

**Regla de ejecución (cc_local):** un lote por sesión limpia. Lee solo los envelopes de tu lote. El
envelope es la única fuente: no consultes notas ni docs genéricas. Un commit por lote. Nunca push/sudo.

`Lotes: **A** T1–T5 · **B** T6–T7 · **C** T8–T48 · **D** T49–T59 · **E** T60–T62 · **F** T63–T66`

**Verificación global de cierre:** `pytest tests/unit` · `python3 tests/snapshots/ui_snapshot.py check
tests/snapshots/baseline` · `pytest tests/e2e -m e2e` · `python3 tests/snapshots/style_snapshot.py check`
(fuera de CI).

> **Contexto de firma compartido** (producido en T6, usado por los envelopes de migración del Lote C):
> `from .ui_context import UIContext, ui_context` — `ctx.user` (`Optional[User]`), `ctx.business_id` (`int`,
> lanza 409), `ctx.ensure_admin()`, `ctx.ensure_admin_or_owner()`. `ui_context(request, db=Depends(session_dep))`.

---

## Lote A (Lote 0) — arnés H-303 + baseline desde código sin migrar + e2e de caracterización

### T1: Helper `_request` e imports del arnés

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/snapshots/ui_snapshot.py — ya existe
from app.models import (Business, Customer, InventoryMovement, Location, MoneyExtraction,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User,)  # línea 72-75
def _get(client: TestClient, key: str, url: str, hx: bool) -> str: ...  # línea 352
```

#### Objective

Añadir el helper `_request` (status + canónico, sin exigir 200) y los imports `hash_password` y `UserBusiness`.

#### Instructions

**Step 1: Read the file**
```
Read tests/snapshots/ui_snapshot.py
```

**Step 2: Añadir `UserBusiness` al import de modelos**

```python
# BEFORE (líneas 72-75):
from app.models import (
    Business, Customer, InventoryMovement, Location, MoneyExtraction,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User,
)

# AFTER:
from app.models import (
    Business, Customer, InventoryMovement, Location, MoneyExtraction,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User, UserBusiness,
)
```

**Step 3: Añadir el import de `hash_password`** justo después de `from app.settings import DATABASE_URL` (línea 80):

```python
# AFTER (insertar tras línea 80):
from app.auth import hash_password
```

**Step 4: Añadir `_request`** justo después de la función `_get` (tras la línea 357):

```python
def _request(client: TestClient, key: str, method: str, url: str, data=None, hx: bool = True) -> str:
    resp = client.request(method, url, data=data, headers={"HX-Request": "true"} if hx else {})
    return f"status={resp.status_code}\n" + canonical(resp.text)
```

**Edge cases (ya resueltos):**
- Respuesta < 4 líneas: `_request` la acepta (a diferencia de `_get`). No añadas comprobación de mínimo.
- Código de estado no 200: `_request` lo conserva en `status=NNN`. No exijas 200.

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read())"
# Expected: exit 0 (sin error de sintaxis)
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` modificado — imports + `_request` añadidos
- [ ] Ningún otro archivo modificado
- [ ] El comando de verificación sale con código 0

---

### T2: Sembrar `snap-operator`, `snap-owner` y devolver códigos en `_seed()`

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/snapshots/ui_snapshot.py — ya existe
def _seed() -> dict:  # línea 225
    # usa get_session(); siembra biz, central, pos, Product SNAP-1, cliente, gasto, extracción, documento
    # retorna dict con "bid", "customer", "expense", ... (línea 287-300)
```

#### Objective

Sembrar los usuarios `snap-operator` y `snap-owner` (con `UserBusiness`) y añadir `customer_name`, `pos_code`
y `central_code` al dict devuelto por `_seed()`.

#### Instructions

**Step 1: Read the file**
```
Read tests/snapshots/ui_snapshot.py
```

**Step 2: Añadir los usuarios antes del `db.commit()`** (sustituir la línea `user = db.scalar(select(User).where(User.username == "operator"))` + `db.commit()`):

```python
# BEFORE (líneas 278-279):
        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()

# AFTER:
        user = db.scalar(select(User).where(User.username == "operator"))
        # Usuarios para la captura por rol (REQ-1). Contraseña = nombre de usuario.
        db.add(User(username="snap-operator", password_hash=hash_password("snap-operator"),
                    role="operator", is_active=True, business_id=bid))
        db.add(User(username="snap-owner", password_hash=hash_password("snap-owner"),
                    role="owner", is_active=True, business_id=bid))
        db.flush()
        _snap_owner = db.scalar(select(User).where(User.username == "snap-owner"))
        db.add(UserBusiness(user_id=int(_snap_owner.id), business_id=bid))
        db.commit()
```

**Step 3: Añadir tres claves al dict devuelto** (sustituir las claves `"doc"` y `"user"` del return):

```python
# BEFORE (líneas 298-299):
            "doc": doc.id,
            "user": user.id,

# AFTER:
            "doc": doc.id,
            "user": user.id,
            "customer_name": customer.name,
            "pos_code": pos.code,
            "central_code": central.code,
```

**Edge cases (ya resueltos):**
- `snap-owner` debe resolver su negocio: lleva fila en `UserBusiness`; sin ella `get_user_business_ids` devuelve `[]`.

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read())"
# Expected: exit 0
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` modificado — usuarios + claves del dict añadidos
- [ ] Ningún otro archivo modificado
- [ ] El comando de verificación sale con código 0

---

### T3: Reescribir `capture()` con fragmentos, POST, roles y estados vacíos

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/snapshots/ui_snapshot.py — ya existe
def capture() -> dict[str, str]:  # línea 360
    # login admin, _seed(), _pages(ids), _force_password_change(), modal_must_change_password
TABS = ["home", "agent", ..., "activity"]  # línea 305, 17 slugs
def _get(client, key, url, hx) -> str: ...
def _request(client, key, method, url, data=None, hx=True) -> str: ...
def _pages(ids) -> dict[str, str]: ...
```

#### Objective

Sustituir `capture()` por la secuencia completa del arnés H-303 (fragmentos, POST con error, POST válidos,
negocio vacío, captura por rol y cambio de contraseña), conservando las 40 claves admin actuales.

#### Instructions

**Step 1: Read the file**
```
Read tests/snapshots/ui_snapshot.py
```

**Step 2: Sustituir la función `capture()` completa** (líneas 360-377):

```python
# BEFORE:
def capture() -> dict[str, str]:
    logging.disable(logging.CRITICAL)
    out: dict[str, str] = {}
    with TestClient(app) as client:
        out["login"] = _get(client, "login", "/ui/login", hx=False)
        login = client.post("/ui/login", data={"username": "admin", "password": "admin"},
                            follow_redirects=False)
        if login.status_code != 302:
            raise SystemExit(f"login falló: {login.status_code}")
        ids = _seed()
        for key, url in _pages(ids).items():
            out[key] = _get(client, key, url, hx=not key.startswith(("dashboard", "print_")))
        # Último: el modal solo se renderiza si el usuario debe cambiar la contraseña.
        _force_password_change()
        out["modal_must_change_password"] = _get(
            client, "modal_must_change_password", "/ui/must-change-password-form", hx=True
        )
    return out

# AFTER:
def _login(client: TestClient, username: str, password: str) -> None:
    resp = client.post("/ui/login", data={"username": username, "password": password}, follow_redirects=False)
    if resp.status_code != 302:
        raise SystemExit(f"login {username} falló: {resp.status_code}")


def capture() -> dict[str, str]:
    logging.disable(logging.CRITICAL)
    out: dict[str, str] = {}
    with TestClient(app) as client:
        out["login"] = _get(client, "login", "/ui/login", hx=False)
        _login(client, "admin", "admin")
        ids = _seed()
        # 1. Lo actual: 40 claves admin, sin cambios de clave.
        for key, url in _pages(ids).items():
            out[key] = _get(client, key, url, hx=not key.startswith(("dashboard", "print_")))
        # 2. Fragmentos OOB (REQ-3) como admin.
        out["frag_defaults_purchase"] = _request(client, "frag_defaults_purchase", "GET",
                                                 "/ui/product-defaults/purchase?product=SNAP-1")
        out["frag_defaults_sale"] = _request(client, "frag_defaults_sale", "GET",
                                             "/ui/product-defaults/sale?product=SNAP-1")
        out["frag_salesdoc_product"] = _request(client, "frag_salesdoc_product", "GET",
                                                "/ui/sales-doc/product-defaults?product=SNAP-1")
        out["frag_salesdoc_customer"] = _request(
            client, "frag_salesdoc_customer", "GET",
            f"/ui/sales-doc/customer-defaults?client_name={quote(ids['customer_name'])}")
        out["frag_sale_options"] = _request(client, "frag_sale_options", "GET",
                                            f"/ui/sale/product-options?location_code={ids['pos_code']}")
        out["frag_transfer_options"] = _request(client, "frag_transfer_options", "GET",
                                                f"/ui/transfers/product-options?from_location_code={ids['central_code']}")
        out["frag_lots_empty"] = _request(client, "frag_lots_empty", "GET", "/ui/return-supplier/lots")
        out["frag_lots_none"] = _request(client, "frag_lots_none", "GET",
                                         "/ui/return-supplier/lots?sku=SNAP-NOLOT")
        out["frag_lots_ok"] = _request(client, "frag_lots_ok", "GET",
                                       "/ui/return-supplier/lots?sku=SNAP-1")
        # 3. POST con error (REQ-2) y POST válidos (REQ-3).
        out["post_purchase_qty0"] = _request(client, "post_purchase_qty0", "POST", "/ui/purchase",
                                             data={"product": "SNAP-1", "quantity": "0", "unit_cost": "1.0"})
        out["post_user_duplicate"] = _request(client, "post_user_duplicate", "POST", "/ui/users/create",
                                              data={"username": "admin", "password": "x", "role": "admin",
                                                    "is_active": "true"})
        out["post_transfer_over_stock"] = _request(client, "post_transfer_over_stock", "POST", "/ui/transfers",
                                                   data={"from_location_code": ids["central_code"],
                                                         "to_location_code": ids["pos_code"],
                                                         "product": "SNAP-1", "quantity": "9999"})
        out["post_purchase_new_product"] = _request(client, "post_purchase_new_product", "POST", "/ui/purchase",
                                                    data={"product": "SNAP-NEW", "product_name": "Producto Nuevo",
                                                          "quantity": "1", "unit_cost": "5.0"})
        out["post_sale_ok"] = _request(client, "post_sale_ok", "POST", "/ui/sale",
                                       data={"product": "SNAP-1", "quantity": "1", "unit_price": "2.0",
                                             "location_code": ids["pos_code"]})
        # 4. Negocio vacío snap-empty + cambio de negocio + estados vacíos + producto para REQ-2.
        _create_snap_empty(client)
        empty_bid = _empty_business_id()
        client.post("/ui/active-business", data={"business_id": str(empty_bid)}, follow_redirects=False)
        for slug in TABS:
            out[f"empty_tab_{slug}"] = _get(client, f"empty_tab_{slug}", f"/ui/tabs/{slug}", hx=True)
        _add_empty_product(empty_bid)
        client.post("/ui/active-business", data={"business_id": str(ids['bid'])}, follow_redirects=False)
        # 5. SKU del otro negocio (REQ-2).
        out["post_product_sku_other_business"] = _request(
            client, "post_product_sku_other_business", "POST", "/ui/product/SNAP-1/update",
            data={"new_sku": "SNAP-EMPTY", "name": "SNAP-1", "unit_of_measure": "ud"})
        # 6. Captura por rol (REQ-1): un TestClient nuevo por rol, sin `with`.
        out.update(_capture_role("operator"))
        out.update(_capture_role("owner"))
        # 7. Último, como hoy: cambio de contraseña obligatorio.
        _force_password_change()
        out["modal_must_change_password"] = _get(
            client, "modal_must_change_password", "/ui/must-change-password-form", hx=True)
        out["post_must_change_password_mismatch"] = _request(
            client, "post_must_change_password_mismatch", "POST", "/ui/must-change-password",
            data={"new_password": "x", "confirm_password": "y"})
    return out
```

**Step 3: Añadir los helpers** `_create_snap_empty`, `_empty_business_id`, `_add_empty_product` y
`_capture_role` justo antes de `capture()`:

```python
def _create_snap_empty(client: TestClient) -> None:
    client.post("/ui/businesses/create", data={"code": "snap-empty", "name": "Snap Empty"},
                headers={"HX-Request": "true"}, follow_redirects=False)


def _empty_business_id() -> int:
    db = get_session()
    try:
        return int(db.scalar(select(Business).where(Business.code == "snap-empty")).id)
    finally:
        db.close()


def _add_empty_product(bid: int) -> None:
    db = get_session()
    try:
        db.add(Product(business_id=bid, sku="SNAP-EMPTY", name="Producto Empty", category="Cat",
                       min_stock=0, unit_of_measure="ud", default_purchase_cost=1.0,
                       default_sale_price=2.0, lead_time_days=0))
        db.commit()
    finally:
        db.close()


def _capture_role(role: str) -> dict[str, str]:
    out: dict[str, str] = {}
    client = TestClient(app)  # nuevo por rol; sin `with`: no relanza el lifespan
    _login(client, f"snap-{role}", f"snap-{role}")
    out[f"{role}_dashboard"] = _get(client, f"{role}_dashboard", "/ui/dashboard", hx=False)
    for slug in _ROLE_TABS[role]:
        out[f"{role}_tab_{slug}"] = _get(client, f"{role}_tab_{slug}", f"/ui/tabs/{slug}", hx=True)
    return out
```

**Step 4: Añadir `_ROLE_TABS` y el import de `quote`** (junto a `TABS`, línea 305):

```python
_ROLE_TABS = {
    "operator": ["sales"],
    "owner": ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
              "customers", "expenses", "profit-items", "profit", "dividends", "history"],
}
```

Y en el bloque de imports (arriba, junto a `import re`):

```python
# AFTER (añadir):
from urllib.parse import quote
```

**Edge cases (ya resueltos):**
- `post_transfer_over_stock`: `quantity=9999` supera el stock (10 − 3 en POS) y dispara el error.
- `post_purchase_qty0`: `quantity=0` lo valida el propio handler con 422.
- El cliente sembrado tiene nombre hostil: usa `quote(ids['customer_name'])` para el query.
- `SNAP-NOLOT` no existe: `frag_lots_none` responde "Sin lotes…" (el endpoint no lanza).

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read())"
# Expected: exit 0
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` modificado — `capture()` + 5 helpers + `_ROLE_TABS` + import `quote`
- [ ] Ningún otro archivo modificado
- [ ] El comando de verificación sale con código 0

---

### T4: e2e de caracterización

**Depends on:** T3
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_fase4_characterization_e2e.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/e2e/conftest.py — ya existe, NO MODIFICAR
# live_server: levanta uvicorn real contra SQLite temporal + _seed_stock (E2E-SKU1/E2E-SKU2 + operator/owner)
# fixture autouse js_errors: falla si hay errores de JS/CSP reales
# tests/e2e/test_tabs_smoke_e2e.py — patrón: _login(page, base_url), page.locator("#tab-<slug>")
```

#### Objective

Crear el e2e de caracterización de REQ-9 que pasa con el código sin migrar (sidebar por rol, autorrelleno OOB y
lista de lotes).

#### Instructions

**Step 1: Crear el fichero** `tests/e2e/test_fase4_characterization_e2e.py` con este contenido exacto:

```python
from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

SERVER_IDS = {
    "admin": ["home", "agent", "inventory", "purchases", "transfers", "coach", "sales",
              "documents", "customers", "expenses", "profit-items", "profit", "dividends",
              "history", "users", "businesses", "activity"],
    "owner": ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
              "customers", "expenses", "profit-items", "profit", "dividends", "history"],
    "operator": ["sales"],
}


def _login(page: Page, base_url: str, username: str, password: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', username)
    page.fill('input[name="password"]', password)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.locator("#tab-content > *").first.wait_for()


@pytest.mark.parametrize("role", ["admin", "owner", "operator"])
def test_sidebar_ids_by_role(live_server: str, page: Page, role: str) -> None:
    base_url = live_server
    _login(page, base_url, role, role)
    ids = [el.get_attribute("id") for el in page.locator(".sidebar-item").all()]
    assert ids == [f"tab-{slug}" for slug in SERVER_IDS[role]]
    selected = page.locator(".sidebar-item[aria-selected='true']").get_attribute("id")
    expected = "tab-sales" if role == "operator" else "tab-home"
    assert selected == expected


def test_purchase_autofill_cost(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "purchases")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.locator('input[name="product"]').dispatch_event("change")
    expect(page.locator("#purchase-unit-cost")).to_have_value("1.0")


def test_sale_autofill_price(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "sales")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.locator('input[name="product"]').dispatch_event("change")
    expect(page.locator("#sale-unit-price")).to_have_value("2.0")


def test_sales_doc_autofill_price_and_client(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "documents")
    page.fill('#sales-doc-cart-add-form input[name="product"]', "E2E-SKU1")
    page.locator('#sales-doc-cart-add-form input[name="product"]').dispatch_event("change")
    expect(page.locator("#sales-doc-unit-price")).to_have_value("2.0")


def test_inventory_lots_select_present(live_server: str, page: Page) -> None:
    base_url = live_server
    _login(page, base_url, "admin", "admin")
    page.click("#tab-inventory")
    page.locator("#tab-content #stock-table").first.wait_for()
    expect(page.locator("#return-supplier-lots")).to_contain_text("Ingrese SKU")
```

**Edge cases (ya resueltos):**
- El precio de venta de `E2E-SKU1` es `2.0` y el de compra `1.0` (lo fija `_seed_stock` en `conftest.py`).
- `#return-supplier-lots` con `hx-trigger="load"` pinta el `<select disabled>` con "-- Ingrese SKU --".

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e/test_fase4_characterization_e2e.py -m e2e --no-cov -v
# Expected: 6 passed (pasa con el código actual, sin migrar)
```

#### Expected Report

- [ ] `tests/e2e/test_fase4_characterization_e2e.py` creado
- [ ] Ningún otro archivo modificado
- [ ] 6 tests PASS

---

### T5: Cierre del Lote 0 — baseline y criterio de mutación

**Depends on:** T4
**Skill:** backend-developer
**Agent:** coder
**Files:** `tests/snapshots/baseline/*` (generado por el arnés, commiteado)
**Pillars:** P1, P4, P5, P6, P9

#### Objective

Generar la baseline desde el código sin migrar y probar el criterio de cierre (mutación hace fallar el `check`).

#### Instructions

**Step 1: Verificar que `app/` está sin migrar**
```bash
git diff --stat b741a46 -- app/
# Expected: vacío (cero cambios en app/)
```

**Step 2: Escribir la baseline**
```bash
.venv/bin/python tests/snapshots/ui_snapshot.py write tests/snapshots/baseline
# Expected: "OK: <N> snapshots escritos en tests/snapshots/baseline"
```

**Step 3: Mutación 1 — quitar `hx-swap-oob` del input de precio de venta**
```bash
# Edita app/routers/ui_products.py: en product_defaults_sale, quita hx-swap-oob='true' del input#sale-unit-price.
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline; echo "exit=$?"
# Expected: exit=1 y lista "frag_defaults_sale" (y/o "sale_panel")
git checkout -- app/
```

**Step 4: Mutación 2 — cambiar el texto de error de compra con cantidad 0**
```bash
# Edita app/routers/ui_purchases.py: en purchase(), cambia "La cantidad debe ser mayor que 0" por otro texto.
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline; echo "exit=$?"
# Expected: exit=1 y lista "post_purchase_qty0"
git checkout -- app/
```

**Step 5: Verificar el check limpio final**
```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: "OK: <N> snapshots equivalentes"
git diff --stat b741a46 -- app/
# Expected: vacío
```

**Edge cases (ya resueltos):**
- Después de cada mutación, `git checkout -- app/` restaura el código antes de la siguiente mutación.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && git diff --stat b741a46 -- app/
# Expected: OK + diff vacío
```

#### Expected Report

- [ ] `tests/snapshots/baseline/` generado y commiteado (junto a T1–T4)
- [ ] Ambas mutaciones hacen fallar `check` con exit 1
- [ ] `app/` sin cambios respecto a `b741a46`

---

## Lote B — `UIContext`

### T6: Crear `app/routers/ui_context.py`

**Depends on:** T5
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_context.py`
**Pillars:** P1, P3, P4, P5, P6

#### Context

```python
# app/deps.py — ya existe
def session_dep() -> Generator[Session, None, None]: ...
# app/security.py — ya existe
def get_current_user_from_session(db: Session, request: Request) -> Optional[User]: ...
def require_active_business_id(db: Session, request: Request) -> int: ...  # 409 "Active business_id is required"
# app/routers/ui_common.py — ensure_admin / ensure_admin_or_owner lanzan HTTPException(403, "Admin required")
```

#### Objective

Crear el módulo `UIContext` perezoso (una resolución por dato, mismo orden de errores) y su dependencia `ui_context`.

#### Instructions

**Step 1: Crear el fichero** `app/routers/ui_context.py` con este contenido exacto:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import User
from app.security import get_current_user_from_session, require_active_business_id

_UNSET = object()


@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _business_id: Optional[int] = field(default=None, repr=False)

    @property
    def user(self) -> Optional[User]:
        if self._user is _UNSET:
            self._user = get_current_user_from_session(self.db, self.request)
        return self._user  # type: ignore[return-value]

    @property
    def business_id(self) -> int:
        if self._business_id is None:
            self._business_id = require_active_business_id(self.db, self.request)
        return self._business_id

    def ensure_admin(self) -> None:
        user = self.user
        if user is None or (user.role or "").lower() != "admin":
            raise HTTPException(status_code=403, detail="Admin required")

    def ensure_admin_or_owner(self) -> None:
        user = self.user
        role = (user.role or "").strip().lower() if user is not None else ""
        if user is None or role not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Admin required")


def ui_context(request: Request, db: Session = Depends(session_dep)) -> UIContext:
    return UIContext(request=request, db=db)
```

**Edge cases (ya resueltos):**
- `business_id` no cachea el fallo 409: usa `None` como centinela y una segunda lectura reintenta.
- `user` cachea también `None` (usa `_UNSET` como centinela).
- `ensure_admin_or_owner` replica `.strip()` de `ui_common`; `ensure_admin` no usa `.strip()`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_context.py -v 2>&1 | tail -3
# Nota: este test aún no existe (T7). Primero valida import:
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.routers.ui_context import UIContext, ui_context; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_context.py` creado con el contenido exacto
- [ ] Ningún otro archivo modificado
- [ ] El import no lanza error

---

### T7: Crear `tests/unit/test_ui_context.py`

**Depends on:** T6
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_context.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py — creado en T6
# UIContext(request, db) · .user · .business_id · .ensure_admin() · .ensure_admin_or_owner()
# app.security.require_active_business_id → 409 "Active business_id is required"
```

#### Objective

Crear el test unitario que fija pereza, una llamada por dato, el `ctx.db` compartido y los mismos `detail`.

#### Instructions

**Step 1: Crear el fichero** `tests/unit/test_ui_context.py` con este contenido exacto:

```python
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.routers import ui_context as uc


def _ctx(request=None, db=None):
    return uc.UIContext(request=request if request is not None else MagicMock(),
                        db=db if db is not None else MagicMock())


def test_user_resolved_once_and_cached(monkeypatch):
    calls = []
    fake_user = MagicMock()
    monkeypatch.setattr(uc, "get_current_user_from_session",
                        lambda _db, _req: calls.append(1) or fake_user)
    ctx = _ctx()
    assert ctx.user is fake_user
    assert ctx.user is fake_user
    assert len(calls) == 1


def test_user_caches_none(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: calls.append(1) or None)
    ctx = _ctx()
    assert ctx.user is None
    assert ctx.user is None
    assert len(calls) == 1


def test_business_id_resolved_once(monkeypatch):
    calls = []
    monkeypatch.setattr(uc, "require_active_business_id", lambda _db, _req: calls.append(1) or 7)
    ctx = _ctx()
    assert ctx.business_id == 7
    assert ctx.business_id == 7
    assert len(calls) == 1


def test_business_id_does_not_cache_failure(monkeypatch):
    calls = []
    def _boom(_db, _req):
        calls.append(1)
        raise HTTPException(status_code=409, detail="Active business_id is required")
    monkeypatch.setattr(uc, "require_active_business_id", _boom)
    ctx = _ctx()
    with pytest.raises(HTTPException) as e:
        ctx.business_id
    assert e.value.status_code == 409
    with pytest.raises(HTTPException):
        ctx.business_id
    assert len(calls) == 2


def _user_with_role(role):
    u = MagicMock()
    u.role = role
    u.is_active = True
    return u


def test_ensure_admin_403_for_owner(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("owner"))
    with pytest.raises(HTTPException) as e:
        _ctx().ensure_admin()
    assert e.value.status_code == 403
    assert e.value.detail == "Admin required"


def test_ensure_admin_ok_for_admin(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("admin"))
    assert _ctx().ensure_admin() is None


def test_ensure_admin_or_owner_ok_for_owner(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("owner"))
    assert _ctx().ensure_admin_or_owner() is None


def test_ensure_admin_or_owner_403_for_operator(monkeypatch):
    monkeypatch.setattr(uc, "get_current_user_from_session", lambda _db, _req: _user_with_role("operator"))
    with pytest.raises(HTTPException) as e:
        _ctx().ensure_admin_or_owner()
    assert e.value.status_code == 403


def test_ctx_db_is_the_injected_db():
    db = MagicMock(spec=Session)
    ctx = _ctx(db=db)
    assert ctx.db is db
```

**Edge cases (ya resueltos):**
- El test usa `monkeypatch` sobre `app.routers.ui_context.get_current_user_from_session` /
  `require_active_business_id` (no sobre `app.security`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_context.py -v
# Expected: 9 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_context.py` creado
- [ ] Ningún otro archivo modificado
- [ ] 9 tests PASS

---

## Lote C — migración a `UIContext` (un envelope por router) + adaptación de tests

> Regla común a T8–T39: en cada handler listado, añade `ctx: UIContext = Depends(ui_context)` como último
> parámetro (después de `db: Session = Depends(session_dep)`), sustituye cada llamada según la tabla, y no toques
> las funciones auxiliares sin `@router`. Tras cada envelope, corre el `check` del arnés.

### T8: Migrar `ui_activity.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_activity.py`
**Pillars:** P1, P3, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py (T6): from .ui_context import UIContext, ui_context
```

#### Objective

Sustituir `ensure_admin` por `ctx.ensure_admin()` en `tab_activity`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_activity.py
```

**Step 2: Añadir `ctx` y sustituir**

| handler | antes | después |
|---|---|---|
| `tab_activity` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |

**Step 3: Imports**

```python
# BEFORE:
from .ui_common import (
    ensure_admin,
    month_range,
    parse_dt,
    templates,
)

# AFTER:
from .ui_common import (
    month_range,
    parse_dt,
    templates,
)
from .ui_context import UIContext, ui_context
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK: <N> snapshots equivalentes
```

#### Expected Report

- [ ] `app/routers/ui_activity.py` — `tab_activity` migrado + imports actualizados
- [ ] Ningún otro archivo modificado
- [ ] snapshot check PASS

---

### T9: Migrar `ui_agent.py` (parte 1: `tab_agent`, `messages_view`, `clear_conversation`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_agent.py`
**Pillars:** P1, P3, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py (T6): from app.routers.ui_context import UIContext, ui_context
```

#### Objective

Migrar a `ctx` los tres primeros handlers de `ui_agent.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_agent.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `tab_agent` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_agent` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `messages_view` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `messages_view` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `clear_conversation` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `clear_conversation` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |

**Step 3: Imports** — quita `ensure_admin_or_owner` y `require_active_business_id`, conserva `get_active_business_code`:

```python
# BEFORE:
from app.routers.ui_common import ensure_admin_or_owner, templates
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER:
from app.routers.ui_common import templates
from app.routers.ui_context import UIContext, ui_context
from app.security import get_active_business_code
```

**Edge cases (ya resueltos):**
- `_currency_symbol` (auxiliar) usa `get_active_business_config`; no se toca. `get_active_business_code` se conserva.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_agent.py` — 3 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T10: Migrar `ui_agent.py` (parte 2: `chat`, `delete`, `widgets_view`)

**Depends on:** T9
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_agent.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres handlers restantes de `ui_agent.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_agent.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `chat` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `chat` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `chat` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `delete` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `delete` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `widgets_view` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `widgets_view` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |

**Step 3: Imports** — tras esta parte, `get_current_user_from_session` queda sin uso; elimínalo:

```python
# BEFORE:
from app.security import get_active_business_code

# AFTER:
from app.security import get_active_business_code
```

(Si ya no usas `get_current_user_from_session` ni `require_active_business_id` en ninguna parte del fichero, no
los dejes en el import. Verifica con grep: `grep -n "get_current_user_from_session\|require_active_business_id" app/routers/ui_agent.py` → sin resultados.)

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_agent.py` — 3 handlers migrados; `get_current_user_from_session`/`require_active_business_id` sin uso
- [ ] snapshot check PASS

---

### T11: Migrar `ui_businesses.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_businesses.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cinco handlers de `ui_businesses.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_businesses.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `business_create` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `business_create` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `business_edit_form` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `business_update` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `business_update` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `business_delete` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `business_delete` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `tab_businesses` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |

**Step 3: Imports**

```python
# BEFORE:
from app.routers.ui_common import ensure_admin, templates
from app.security import get_current_user_from_session

# AFTER:
from app.routers.ui_common import templates
from app.routers.ui_context import UIContext, ui_context
```

**Edge cases (ya resueltos):**
- `_write_business_config`, `_reconcile_locations` y el resto de auxiliares sin `@router` no se tocan.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_businesses.py` — 5 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T12: Migrar `ui_coach.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_coach.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres handlers de `ui_coach.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_coach.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `tab_coach` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `tab_coach` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `set_cash_baseline` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `set_cash_baseline` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `set_cash_baseline` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `update_action` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `update_action` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `update_action` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.routers.ui_common import ensure_admin, templates
from app.security import get_current_user_from_session, require_active_business_id

# AFTER:
from app.routers.ui_common import templates
from app.routers.ui_context import UIContext, ui_context
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_coach.py` — 3 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T13: Migrar `ui_customers.py` (parte 1: `customers_table`, `customer_create`, `customer_detail`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_customers.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres primeros handlers de `ui_customers.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_customers.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `customers_table` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customers_table` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `customer_create` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customer_create` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `customer_detail` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customer_detail` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)

# AFTER:
from app.security import get_active_business_code
from app.routers.ui_context import UIContext, ui_context
```

**Edge cases (ya resueltos):**
- `_ = ctx.user` conserva la resolución (y la limpieza de sesión) aunque el valor se descarte.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_customers.py` — 3 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T14: Migrar `ui_customers.py` (parte 2: `customer_edit_form`, `customer_update`, `customer_delete`, `tab_customers`)

**Depends on:** T13
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_customers.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cuatro handlers restantes de `ui_customers.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_customers.py
```

**Step 2: Sustituir en cada handler**

| handler | antes | después |
|---|---|---|
| `customer_edit_form` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customer_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `customer_update` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customer_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `customer_delete` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `customer_delete` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `tab_customers` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `tab_customers` | `_ = get_current_user_from_session(db, request)` | `_ = ctx.user` |
| `tab_customers` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |

**Step 3: Imports** — tras esta parte, elimina `ensure_admin` del import de `ui_common`:

```python
# BEFORE:
from .ui_common import ensure_admin, templates

# AFTER:
from .ui_common import templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_customers.py` — 4 handlers migrados; `ensure_admin` sin uso eliminado
- [ ] snapshot check PASS

---

### T15: Migrar `ui_dividends.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_dividends.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` el único handler de `ui_dividends.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_dividends.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `tab_dividends` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_dividends` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import require_active_business_id
...
from .ui_common import (
    dt_to_local_input,
    ensure_admin_or_owner,
    parse_dt,
    templates,
)

# AFTER:
from app.routers.ui_context import UIContext, ui_context
...
from .ui_common import (
    dt_to_local_input,
    parse_dt,
    templates,
)
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_dividends.py` — `tab_dividends` migrado + imports actualizados
- [ ] snapshot check PASS

---

### T16: Migrar `ui_expenses.py` (parte 1: `expense_create`, `expense_edit_form`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_expenses.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los dos primeros handlers de `ui_expenses.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_expenses.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `expense_create` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `expense_create` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `expense_create` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `expense_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |

**Step 3: Imports** (conserva `get_active_business_code`; solo quita lo que ya no se usa tras esta parte)

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (solo si ya no se usan en el fichero; compruébalo con grep):
from app.security import get_active_business_code
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_expenses.py` — 2 handlers migrados
- [ ] snapshot check PASS

---

### T17: Migrar `ui_expenses.py` (parte 2: `expense_delete`, `expense_update`, `tab_expenses`)

**Depends on:** T16
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_expenses.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres handlers restantes de `ui_expenses.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_expenses.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `expense_delete` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `expense_delete` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `expense_delete` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `expense_update` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `expense_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `expense_update` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `tab_expenses` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_expenses` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |

**Step 3: Imports** — tras esta parte, elimina `ensure_admin_or_owner` de `ui_common` y, si quedan sin uso,
`get_current_user_from_session`/`require_active_business_id`:

```python
# BEFORE:
from .ui_common import dt_to_local_input, ensure_admin_or_owner, month_range, parse_dt, templates

# AFTER:
from .ui_common import dt_to_local_input, month_range, parse_dt, templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_expenses.py` — 3 handlers migrados; imports sin uso eliminados
- [ ] snapshot check PASS

---

### T18: Migrar `ui_extractions.py` (parte 1: `extraction_create`, `extraction_delete`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_extractions.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los dos primeros handlers de `ui_extractions.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_extractions.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `extraction_create` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `extraction_create` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `extraction_create` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `extraction_delete` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `extraction_delete` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `extraction_delete` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (solo si ya no se usan; compruébalo con grep):
from app.security import get_active_business_code
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_extractions.py` — 2 handlers migrados
- [ ] snapshot check PASS

---

### T19: Migrar `ui_extractions.py` (parte 2: `extraction_edit_form`, `extraction_update`)

**Depends on:** T18
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_extractions.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los dos handlers restantes de `ui_extractions.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_extractions.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `extraction_edit_form` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `extraction_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `extraction_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `extraction_update` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `extraction_update` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports** — elimina `ensure_admin_or_owner` de `ui_common`:

```python
# BEFORE:
from .ui_common import dt_to_local_input, ensure_admin_or_owner, month_range, parse_dt, templates

# AFTER:
from .ui_common import dt_to_local_input, month_range, parse_dt, templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_extractions.py` — 2 handlers migrados; `ensure_admin_or_owner` sin uso eliminado
- [ ] snapshot check PASS

---

### T20: Migrar `ui_history.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_history.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los dos handlers de `ui_history.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_history.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `tab_history` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_history` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `tab_history` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `history_adjustments_delete_selected` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `history_adjustments_delete_selected` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `history_adjustments_delete_selected` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import (
    get_current_user_from_session,
    require_active_business_id,
)
...
from .ui_common import (
    ensure_admin_or_owner,
    month_range,
    parse_dt,
    templates,
)

# AFTER:
from app.routers.ui_context import UIContext, ui_context
...
from .ui_common import (
    month_range,
    parse_dt,
    templates,
)
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_history.py` — 2 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T21: Migrar `ui_home.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_home.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `dashboard`, `tab_home` y `home_charts` (sin tocar `get_active_business_id`).

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_home.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `dashboard` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `tab_home` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_home` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `home_charts` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `home_charts` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `home_charts` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import (
    get_active_business_code,
    get_active_business_id,
    get_current_user_from_session,
    get_user_business_ids,
    require_active_business_id,
)
...
from .ui_common import (
    dt_to_local_input,
    ensure_admin_or_owner,
    parse_dt,
    templates,
)

# AFTER:
from app.security import (
    get_active_business_code,
    get_active_business_id,
    get_user_business_ids,
)
...
from .ui_common import (
    dt_to_local_input,
    parse_dt,
    templates,
)
from .ui_context import UIContext, ui_context
```

**Edge cases (ya resueltos):**
- `dashboard` conserva `active_business_id = get_active_business_id(db, request)` (no es `require_…`, no se migra).

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_home.py` — 3 handlers migrados; `get_active_business_id` intacto
- [ ] snapshot check PASS

---

### T22: Migrar `ui_products.py` (parte 1: delete, edit_form, update, edit-inventory, update-inventory)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cinco primeros handlers de `ui_products.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_products.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `product_delete` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_delete` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_delete` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `product_edit_form` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_update` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_update` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `product_edit_form_inventory` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_edit_form_inventory` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_update_inventory` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_update_inventory` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_update_inventory` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (solo si ya no se usan; compruébalo con grep):
from app.security import get_active_business_code
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_products.py` — 5 handlers migrados
- [ ] snapshot check PASS

---

### T23: Migrar `ui_products.py` (parte 2: from-invoice, defaults×2, suggest, tab-inventory)

**Depends on:** T22
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cinco handlers restantes de `ui_products.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_products.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `product_from_invoice` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `product_from_invoice` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `product_from_invoice` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `product_defaults_purchase` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `product_defaults_sale` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `product_suggest` | `business_id = require_active_business_id(db, request)` | `business_id = ctx.business_id` |
| `tab_inventory` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_inventory` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `tab_inventory` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports** — elimina `ensure_admin_or_owner` de `ui_common`:

```python
# BEFORE:
from .ui_common import (
    barcode_to_sku,
    ensure_admin_or_owner,
    extract_sku,
    delete_uploaded_image,
    parse_typed_float,
    save_product_image,
    templates,
)

# AFTER:
from .ui_common import (
    barcode_to_sku,
    extract_sku,
    delete_uploaded_image,
    parse_typed_float,
    save_product_image,
    templates,
)
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_products.py` — 5 handlers migrados; `ensure_admin_or_owner` sin uso eliminado
- [ ] snapshot check PASS

---

### T24: Migrar `ui_profit_items.py` (parte 1: tab, adjustment, transfer-in)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit_items.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `tab_profit_items`, `profit_items_adjustment_*` y `profit_items_transfer_in_*`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_profit_items.py
```

**Step 2: Sustituir** (usa `ctx.ensure_admin()` para los que hoy llaman `ensure_admin(db, request)`):

| handler | antes | después |
|---|---|---|
| `tab_profit_items` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `profit_items_adjustment_edit_form` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `profit_items_adjustment_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `profit_items_adjustment_update` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `profit_items_adjustment_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `profit_items_adjustment_update` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `profit_items_transfer_in_edit_form` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `profit_items_transfer_in_edit_form` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `profit_items_transfer_in_update` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `profit_items_transfer_in_update` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `profit_items_transfer_in_update` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports** — conserva `get_active_business_code`, `get_current_user_from_session` y
`require_active_business_id` (los sigue usando la auxiliar `_render_profit_items_tab`, que NO se migra):

```python
# BEFORE:
from .ui_common import (
    dt_to_local_input,
    ensure_admin,
    ensure_admin_or_owner,
    extract_sku,
    month_range,
    parse_dt,
    templates,
)

# AFTER:
from .ui_common import (
    dt_to_local_input,
    extract_sku,
    month_range,
    parse_dt,
    templates,
)
from .ui_context import UIContext, ui_context
```

**Edge cases (ya resueltos):**
- `_render_profit_items_tab` (auxiliar) conserva `require_active_business_id(db, request)` y
  `get_current_user_from_session(db, request)`; no la toques.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_profit_items.py` — 5 handlers migrados; auxiliar intacta
- [ ] snapshot check PASS

---

### T25: Migrar `ui_profit_items.py` (parte 2: sale, purchase)

**Depends on:** T24
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit_items.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los seis handlers restantes de `ui_profit_items.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_profit_items.py
```

**Step 2: Sustituir** (todos los `ensure_admin(db, request)` → `ctx.ensure_admin()`):

| handler | antes | después |
|---|---|---|
| `profit_items_sale_edit_form` | `ensure_admin(db, request)` / `bid = require_active_business_id(db, request)` | `ctx.ensure_admin()` / `bid = ctx.business_id` |
| `profit_items_sale_update` | `ensure_admin` / `bid = require_…` / `user = get_current_user_from_session` | `ctx.ensure_admin()` / `bid = ctx.business_id` / `user = ctx.user` |
| `profit_items_sale_delete` | `ensure_admin` / `bid = require_…` / `user = get_current_user_from_session` | `ctx.ensure_admin()` / `bid = ctx.business_id` / `user = ctx.user` |
| `profit_items_sale_delete_selected` | `ensure_admin` / `bid = require_…` / `user = get_current_user_from_session` | `ctx.ensure_admin()` / `bid = ctx.business_id` / `user = ctx.user` |
| `profit_items_purchase_edit_form` | `ensure_admin` / `bid = require_…` | `ctx.ensure_admin()` / `bid = ctx.business_id` |
| `profit_items_purchase_update` | `ensure_admin` / `bid = require_…` / `user = get_current_user_from_session` | `ctx.ensure_admin()` / `bid = ctx.business_id` / `user = ctx.user` |

> Sustituye cada línea por su forma exacta en el fichero (`ensure_admin(db, request)` → `ctx.ensure_admin()`,
> `bid = require_active_business_id(db, request)` → `bid = ctx.business_id`,
> `user = get_current_user_from_session(db, request)` → `user = ctx.user`), en el mismo punto.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_profit_items.py` — 6 handlers migrados
- [ ] snapshot check PASS

---

### T26: Migrar `ui_profit.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_profit.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` el único handler de `ui_profit.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_profit.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `tab_profit` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `tab_profit` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `tab_profit` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)
...
from .ui_common import (
    ensure_admin_or_owner,
    parse_dt,
    templates,
)

# AFTER:
from app.security import get_active_business_code
from app.routers.ui_context import UIContext, ui_context
...
from .ui_common import (
    parse_dt,
    templates,
)
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_profit.py` — `tab_profit` migrado + imports actualizados
- [ ] snapshot check PASS

---

### T27: Migrar `ui_purchases.py` (parte 1: `purchase_from_invoice`, `purchase`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `purchase_from_invoice` y `purchase`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_purchases.py
```

**Step 2: Sustituir** — solo las llamadas directas en el cuerpo del handler, NO la auxiliar anidada `_render_error`:

| handler | antes | después |
|---|---|---|
| `purchase_from_invoice` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `purchase_from_invoice` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `purchase_from_invoice` | `user = get_current_user_from_session(db, request)` (la del cuerpo, no la de `_render_error`) | `user = ctx.user` |
| `purchase` | `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |
| `purchase` | `bid = require_active_business_id(db, request)` | `bid = ctx.business_id` |
| `purchase` | `user = get_current_user_from_session(db, request)` (en try y en except) | `user = ctx.user` |

**Edge cases (ya resueltos):**
- `_render_error` (función anidada sin `@router`) conserva `get_current_user_from_session(db, request)`; no la toques.

**Step 3: Imports** — conserva `get_current_user_from_session` (lo usa `_render_error`):

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (solo quita require_active_business_id si ya no se usa; get_current_user_from_session se queda):
from app.security import get_active_business_code, get_current_user_from_session
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — 2 handlers migrados; `_render_error` intacto
- [ ] snapshot check PASS

---

### T28: Migrar `ui_purchases.py` (parte 2: label, edit, update, delete, delete-selected, tab)

**Depends on:** T27
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los seis handlers restantes de `ui_purchases.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_purchases.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `purchase_label_print` | `ensure_admin_or_owner(db, request)` / `bid = require_…` | `ctx.ensure_admin_or_owner()` / `bid = ctx.business_id` |
| `purchase_edit_form` | `ensure_admin_or_owner(db, request)` / `bid = require_…` | `ctx.ensure_admin_or_owner()` / `bid = ctx.business_id` |
| `purchase_update` | `bid = require_…` / `ensure_admin_or_owner` / `user = get_current_user_from_session` | `bid = ctx.business_id` / `ctx.ensure_admin_or_owner()` / `user = ctx.user` |
| `purchase_delete` | `bid = require_…` / `ensure_admin_or_owner` / `user = get_current_user_from_session` | `bid = ctx.business_id` / `ctx.ensure_admin_or_owner()` / `user = ctx.user` |
| `purchase_delete_selected` | `bid = require_…` / `user = get_current_user_from_session` / `ensure_admin_or_owner` | `bid = ctx.business_id` / `user = ctx.user` / `ctx.ensure_admin_or_owner()` |
| `tab_purchases` | `ensure_admin_or_owner` / `bid = require_…` / `user = get_current_user_from_session` | `ctx.ensure_admin_or_owner()` / `bid = ctx.business_id` / `user = ctx.user` |

> Sustituye cada línea por su forma exacta, en el mismo punto, conservando el orden.

**Step 3: Imports** — tras esta parte, `require_active_business_id` queda sin uso:

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session

# AFTER:
from app.security import get_active_business_code
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — 6 handlers migrados
- [ ] snapshot check PASS

---

### T29: Migrar `ui_sales_documents.py` (parte 1: preview, delete, edit, update)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `sales_doc_preview`, `sales_doc_delete`, `sales_doc_edit_form` y `sales_doc_update`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales_documents.py
```

**Step 2: Sustituir** (patrón: `_ = get_current_user_from_session(db, request)` → `_ = ctx.user`,
`bid = require_active_business_id(db, request)` → `bid = ctx.business_id`):

| handler | llamadas a sustituir |
|---|---|
| `sales_doc_preview` | `_ = get_current_user_from_session` · `_ = require_active_business_id` |
| `sales_doc_delete` | `_ = get_current_user_from_session` · `bid = require_active_business_id` |
| `sales_doc_edit_form` | `_ = get_current_user_from_session` · `bid = require_active_business_id` |
| `sales_doc_update` | `_ = get_current_user_from_session` · `bid = require_active_business_id` |

> En `sales_doc_preview`, la llamada es `_ = require_active_business_id(db, request)` (descartada) → `_ = ctx.business_id`.

**Step 3: Imports**

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (solo si ya no se usan; compruébalo con grep):
from app.security import get_active_business_code
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` — 4 handlers migrados
- [ ] snapshot check PASS

---

### T30: Migrar `ui_sales_documents.py` (parte 2: defaults×2, cart, issue, pdf, tab)

**Depends on:** T29
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los nueve handlers restantes de `ui_sales_documents.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales_documents.py
```

**Step 2: Sustituir** (`bid = require_active_business_id(db, request)` → `bid = ctx.business_id`,
`_ = get_current_user_from_session(db, request)` → `_ = ctx.user`, `ensure_admin_or_owner` → `ctx.ensure_admin_or_owner()`):

| handler | llamadas a sustituir |
|---|---|
| `sales_doc_product_defaults` | `bid = require_active_business_id` |
| `sales_doc_customer_defaults` | `bid = require_active_business_id` |
| `sales_doc_cart_add` | `bid = require_active_business_id` |
| `sales_doc_cart_update` | `bid = require_active_business_id` |
| `sales_doc_cart_remove` | `bid = require_active_business_id` |
| `sales_doc_cart_clear` | `bid = require_active_business_id` |
| `sales_doc_issue` | `_ = get_current_user_from_session` · `bid = require_active_business_id` |
| `sales_doc_pdf` | `_ = get_current_user_from_session` · `bid = require_active_business_id` |
| `tab_documents` | `ensure_admin_or_owner` · `_ = get_current_user_from_session` · `bid = require_active_business_id` |

**Step 3: Imports** — tras esta parte, elimina `ensure_admin_or_owner` de `ui_common`:

```python
# BEFORE:
from .ui_common import ensure_admin_or_owner, extract_sku, parse_dt, templates

# AFTER:
from .ui_common import extract_sku, parse_dt, templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` — 9 handlers migrados; `ensure_admin_or_owner` sin uso eliminado
- [ ] snapshot check PASS

---

### T31: Migrar `ui_sales.py` (parte 1: `sale_product_options`, `sale`, `sale_barcode`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `sale_product_options`, `sale` y `sale_barcode`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales.py
```

**Step 2: Sustituir** (`bid = require_active_business_id` → `bid = ctx.business_id`,
`user = get_current_user_from_session` → `user = ctx.user`):

| handler | llamadas a sustituir |
|---|---|
| `sale_product_options` | `user = get_current_user_from_session` · `bid = require_active_business_id` |
| `sale` | `bid = require_active_business_id` · `user = get_current_user_from_session` (en try y en except) |
| `sale_barcode` | `bid = require_active_business_id` · `user = get_current_user_from_session` (en try y en except) |

**Edge cases (ya resueltos):**
- `_sales_doc_context`, `_sale_pos_context`, `_sale_product_options` (auxiliares) no se tocan; `_sales_doc_context`
  conserva su `require_active_business_id(db, request)`.

**Step 3: Imports** — conserva `get_current_user_from_session` y `require_active_business_id` (los usa
`_sales_doc_context`):

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (sin cambios: la auxiliar _sales_doc_context los sigue usando):
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — 3 handlers migrados; auxiliares intactas
- [ ] snapshot check PASS

---

### T32: Migrar `ui_sales.py` (parte 2: edit, update, delete, delete-selected, tab)

**Depends on:** T31
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cinco handlers restantes de `ui_sales.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales.py
```

**Step 2: Sustituir**

| handler | llamadas a sustituir |
|---|---|
| `sale_edit_form` | `bid = require_active_business_id` |
| `sale_update` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` (en try y except) |
| `sale_delete` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` (×3) |
| `sale_delete_selected` | `bid = require_…` · `user = get_current_user_from_session` · `ensure_admin_or_owner` |
| `tab_sales` | `bid = require_…` · `user = get_current_user_from_session` |

**Step 3: Imports** — elimina `ensure_admin_or_owner` y `ensure_admin` de `ui_common`:

```python
# BEFORE:
from .ui_common import (
    barcode_to_sku,
    ensure_admin,
    ensure_admin_or_owner,
    extract_sku,
    month_range,
    parse_dt,
    parse_typed_float,
    templates,
)

# AFTER:
from .ui_common import (
    barcode_to_sku,
    extract_sku,
    month_range,
    parse_dt,
    parse_typed_float,
    templates,
)
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` — 5 handlers migrados; `ensure_admin*` sin uso eliminados
- [ ] snapshot check PASS

---

### T33: Migrar `ui_stock.py` (parte 1: `stock_table`, `stock_delete_product`, `stock_delete_selected`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_stock.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres primeros handlers de `ui_stock.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_stock.py
```

**Step 2: Sustituir**

| handler | llamadas a sustituir |
|---|---|
| `stock_table` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `stock_delete_product` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `stock_delete_selected` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)

# AFTER:
from app.security import get_active_business_code
from app.routers.ui_context import UIContext, ui_context
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_stock.py` — 3 handlers migrados
- [ ] snapshot check PASS

---

### T34: Migrar `ui_stock.py` (parte 2: return-supplier, lots, restock, print)

**Depends on:** T33
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_stock.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cuatro handlers restantes de `ui_stock.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_stock.py
```

**Step 2: Sustituir**

| handler | llamadas a sustituir |
|---|---|
| `ui_supplier_return` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `ui_supplier_return_lots` | `ensure_admin_or_owner` · `bid = require_…` |
| `restock_table` | `ensure_admin_or_owner` · `bid = require_…` |
| `restock_print` | `ensure_admin_or_owner` · `bid = require_…` |

**Step 3: Imports** — elimina `ensure_admin_or_owner` de `ui_common`:

```python
# BEFORE:
from .ui_common import (
    ensure_admin_or_owner,
    templates,
)

# AFTER:
from .ui_common import templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_stock.py` — 4 handlers migrados; `ensure_admin_or_owner` sin uso eliminado
- [ ] snapshot check PASS

---

### T35: Migrar `ui_sync.py`

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_sync.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres handlers de `ui_sync.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sync.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `do_sync` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `sync_status` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |
| `provision` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `provision` | `user = get_current_user_from_session(db, request)` | `user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import get_current_user_from_session
...
from app.routers.ui_common import ensure_admin, templates

# AFTER:
from app.routers.ui_context import UIContext, ui_context
...
from app.routers.ui_common import templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_sync.py` — 3 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T36: Migrar `ui_transfers.py` (parte 1: edit, update, delete-selected, delete)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los handlers `transfer_edit_form`, `transfer_update`, `transfer_delete_selected` y `transfer_delete`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_transfers.py
```

**Step 2: Sustituir**

| handler | llamadas a sustituir |
|---|---|
| `transfer_edit_form` | `ensure_admin_or_owner` · `bid = require_…` |
| `transfer_update` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` (en `_tab_error` y en el cuerpo) |
| `transfer_delete_selected` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `transfer_delete` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |

**Edge cases (ya resueltos):**
- `_tab_error` (función anidada en `transfer_update`) conserva `get_current_user_from_session(db, request)`; no la toques.
- `_parse_transfer_lines`, `_stock_options`, `_resolve_transfer_sku` (auxiliares) no se tocan.

**Step 3: Imports** — conserva `get_current_user_from_session` (lo usa `_tab_error`):

```python
# BEFORE:
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id

# AFTER (quita require_active_business_id si ya no se usa; conserva get_current_user_from_session):
from app.security import get_active_business_code, get_current_user_from_session
```

Añade `from .ui_context import UIContext, ui_context`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 4 handlers migrados; `_tab_error` intacto
- [ ] snapshot check PASS

---

### T37: Migrar `ui_transfers.py` (parte 2: create, product-options, stock, print, bulk, tab)

**Depends on:** T36
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los siete handlers restantes de `ui_transfers.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_transfers.py
```

**Step 2: Sustituir**

| handler | llamadas a sustituir |
|---|---|
| `create_transfer` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `transfer_product_options` | `ensure_admin_or_owner` · `bid = require_…` |
| `get_transfer_stock` | `ensure_admin_or_owner` · `bid = require_…` |
| `transfer_print` | `ensure_admin_or_owner` · `bid = require_…` |
| `transfer_bulk_modal` | `ensure_admin_or_owner` · `bid = require_…` |
| `transfer_bulk_execute` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |
| `tab_transfers` | `ensure_admin_or_owner` · `bid = require_…` · `user = get_current_user_from_session` |

**Step 3: Imports** — elimina `ensure_admin_or_owner` de `ui_common`:

```python
# BEFORE:
from .ui_common import dt_to_local_input, ensure_admin_or_owner, extract_sku, month_range, parse_dt, templates

# AFTER:
from .ui_common import dt_to_local_input, extract_sku, month_range, parse_dt, templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — 7 handlers migrados; `ensure_admin_or_owner` sin uso eliminado
- [ ] snapshot check PASS

---

### T38: Migrar `ui_users.py` (parte 1: `user_create`, `user_edit_form`, `user_update`)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_users.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los tres primeros handlers de `ui_users.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_users.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `user_create` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_create` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `user_edit_form` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_update` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_update` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |

**Step 3: Imports**

```python
# BEFORE:
from app.security import get_current_user_from_session, get_user_business_ids
...
from app.routers.ui_common import ensure_admin, templates

# AFTER:
from app.security import get_user_business_ids
from app.routers.ui_context import UIContext, ui_context
...
from app.routers.ui_common import templates
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_users.py` — 3 handlers migrados + imports actualizados
- [ ] snapshot check PASS

---

### T39: Migrar `ui_users.py` (parte 2: `user_delete`, reset-password×2, tab)

**Depends on:** T38
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_users.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Migrar a `ctx` los cuatro handlers restantes de `ui_users.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_users.py
```

**Step 2: Sustituir**

| handler | antes | después |
|---|---|---|
| `user_delete` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_delete` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `user_reset_password_form` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_reset_password` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `user_reset_password` | `current_user = get_current_user_from_session(db, request)` | `current_user = ctx.user` |
| `tab_users` | `ensure_admin(db, request)` | `ctx.ensure_admin()` |

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_users.py` — 4 handlers migrados
- [ ] snapshot check PASS

---

### T40: Adaptar `tests/unit/test_ui_product_update.py`

**Depends on:** T23 (ui_products migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_product_update.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_context.py (T6)
from app.routers.ui_context import UIContext
# Los handlers de ui_products ahora reciben ctx: UIContext = Depends(ui_context) y llaman
# ctx.business_id / ctx.ensure_admin_or_owner() / ctx.user
```

#### Objective

Retargetar los monkeypatch a `app.routers.ui_context.*` e inyectar `ctx` real en las llamadas directas.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_ui_product_update.py
```

**Step 2: Añadir import** (tras `from app.routers import ui_products`):

```python
from app.routers.ui_context import UIContext
```

**Step 3: Sustituir el fixture `_route`**

```python
# BEFORE:
@pytest.fixture
def _route(monkeypatch, business_id: int):
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(ui_products, "ensure_admin_or_owner", lambda _db, _req: None)
    monkeypatch.setattr(ui_products, "get_current_user_from_session", lambda _db, _req: None)
    ...

# AFTER:
@pytest.fixture
def _route(monkeypatch, business_id: int):
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)
    monkeypatch.setattr(_uc, "get_current_user_from_session", lambda _db, _req: None)
    ...
```

**Step 4: Inyectar `ctx` en cada llamada directa** (`_update`, `_update_inventory`):

```python
# BEFORE:
return ui_products.product_update(request=request, sku=sku, db=db, **fields)

# AFTER:
return ui_products.product_update(request=request, sku=sku, db=db,
                                  ctx=UIContext(request=request, db=db), **fields)
```

Haz lo mismo en `_update_inventory` (`product_update_inventory(...)`).

**Edge cases (ya resueltos):**
- `monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)` anula solo el método, no el resto de `UIContext`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_product_update.py -v
# Expected: 5 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_product_update.py` — monkeypatch retargetado + `ctx` inyectado
- [ ] 5 tests PASS

---

### T41: Adaptar `tests/unit/test_ui_product_from_invoice.py`

**Depends on:** T23
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_product_from_invoice.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Retargetar los monkeypatch e inyectar `ctx` en la llamada a `product_from_invoice`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_ui_product_from_invoice.py
```

**Step 2: Añadir import** (tras `from app.routers import ui_products`):

```python
from app.routers.ui_context import UIContext
```

**Step 3: Sustituir `_patch_route`**

```python
# BEFORE:
    monkeypatch.setattr(ui_products, "ensure_admin_or_owner", lambda _db, _req: None)
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(ui_products, "get_current_user_from_session", lambda _db, _req: None)

# AFTER:
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _req: business_id)
    monkeypatch.setattr(_uc, "get_current_user_from_session", lambda _db, _req: None)
```

**Step 4: Inyectar `ctx`**

```python
# BEFORE:
    response = ui_products.product_from_invoice(request=request, invoice_pdf=_pdf(), db=db_session)

# AFTER:
    response = ui_products.product_from_invoice(request=request, invoice_pdf=_pdf(), db=db_session,
                                                ctx=UIContext(request=request, db=db_session))
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_product_from_invoice.py -v
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_product_from_invoice.py` — adaptado
- [ ] 1 test PASS

---

### T42: Adaptar `tests/unit/test_ui_product_suggest.py`

**Depends on:** T23
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_product_suggest.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Retargetar el monkeypatch e inyectar `ctx` en `product_suggest`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_ui_product_suggest.py
```

**Step 2: Añadir import**

```python
from app.routers.ui_context import UIContext
```

**Step 3: Sustituir `_suggest`**

```python
# BEFORE:
def _suggest(db: Session, business_id: int, monkeypatch, q: str, fmt: str = "label") -> str:
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    request = MagicMock()
    request.session = {}
    resp = ui_products.product_suggest(request=request, q=q, fmt=fmt, db=db)
    return resp.body.decode("utf-8")

# AFTER:
def _suggest(db: Session, business_id: int, monkeypatch, q: str, fmt: str = "label") -> str:
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _req: business_id)
    request = MagicMock()
    request.session = {}
    resp = ui_products.product_suggest(request=request, q=q, fmt=fmt, db=db,
                                       ctx=UIContext(request=request, db=db))
    return resp.body.decode("utf-8")
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_product_suggest.py -v
# Expected: 8 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_product_suggest.py` — adaptado
- [ ] 8 tests PASS

---

### T43: Adaptar `tests/unit/test_transfer_print_ref.py`

**Depends on:** T37 (ui_transfers migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_transfer_print_ref.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Retargetar los monkeypatch e inyectar `ctx` en `transfer_print`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_transfer_print_ref.py
```

**Step 2: Añadir import**

```python
from app.routers.ui_context import UIContext
```

**Step 3: Sustituir `_patch`**

```python
# BEFORE:
    monkeypatch.setattr("app.routers.ui_transfers.ensure_admin_or_owner", lambda _db, _request: None)
    monkeypatch.setattr("app.routers.ui_transfers.require_active_business_id", lambda _db, _request: business_id)

# AFTER:
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _request: business_id)
```

**Step 4: Inyectar `ctx`**

```python
# BEFORE:
    response = transfer_print(request=MagicMock(), db=db_session, ref="", movement_id=mv.id)

# AFTER:
    request = MagicMock()
    request.session = {}
    response = transfer_print(request=request, db=db_session, ref="", movement_id=mv.id,
                              ctx=UIContext(request=request, db=db_session))
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_transfer_print_ref.py -v
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/unit/test_transfer_print_ref.py` — adaptado
- [ ] 1 test PASS

---

### T44: Adaptar `tests/unit/test_ui_agent.py` y `tests/unit/test_agent_chat.py`

**Depends on:** T10 (ui_agent migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_agent.py`, `tests/unit/test_agent_chat.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Inyectar `ctx` real en las llamadas directas a `tab_agent`, `delete` y `messages_view`.

#### Instructions

**Step 1: Read the files**
```
Read tests/unit/test_ui_agent.py
Read tests/unit/test_agent_chat.py
```

**Step 2: Añadir import** en ambos:

```python
from app.routers.ui_context import UIContext
```

**Step 3: Inyectar `ctx` en cada llamada directa** — en `test_ui_agent.py`:

```python
# BEFORE (línea 66):
        ui_agent.tab_agent(req, db=db_session)
# AFTER:
        ui_agent.tab_agent(req, db=db_session, ctx=UIContext(request=req, db=db_session))

# BEFORE (línea 74):
        ui_agent.delete(req, widget_id=1, db=db_session)
# AFTER:
        ui_agent.delete(req, widget_id=1, db=db_session, ctx=UIContext(request=req, db=db_session))
```

En `test_agent_chat.py`:

```python
# BEFORE (línea 51):
        ui_agent.messages_view(req, db=db_session)
# AFTER:
        ui_agent.messages_view(req, db=db_session, ctx=UIContext(request=req, db=db_session))
```

**Edge cases (ya resueltos):**
- Estos tests NO monkeypatchean auth: con `ctx` real, `ctx.ensure_admin_or_owner()` resuelve `operator` de
  `req.session` y lanza 403 como antes.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_agent.py tests/unit/test_agent_chat.py -v
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_ui_agent.py` y `tests/unit/test_agent_chat.py` — `ctx` inyectado
- [ ] tests PASS

---

### T45: Adaptar `tests/unit/test_coach_rbac.py`

**Depends on:** T12 (ui_coach migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_coach_rbac.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Inyectar `ctx` real en las llamadas directas a `tab_coach`, `set_cash_baseline` y `update_action`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_coach_rbac.py
```

**Step 2: Añadir import**

```python
from app.routers.ui_context import UIContext
```

**Step 3: Añadir un helper** tras `_user`:

```python
def _ctx(req, db):
    return UIContext(request=req, db=db)
```

**Step 4: Inyectar `ctx` en cada llamada directa.** En `tab_coach`:

```python
# BEFORE:
        ui_coach.tab_coach(req, db=db_session)
# AFTER:
        ui_coach.tab_coach(req, db=db_session, ctx=_ctx(req, db_session))
```

En `set_cash_baseline` y `update_action`, añade `ctx=_ctx(req, db_session)` a cada llamada (los 9 sitios de
`ui_coach.set_cash_baseline(...)` y `ui_coach.update_action(...)`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_coach_rbac.py -v
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_coach_rbac.py` — `ctx` inyectado en todas las llamadas directas
- [ ] tests PASS

---

### T46: Adaptar `tests/unit/test_offline_hooks.py`

**Depends on:** T32 (ui_sales migrado) y T14 (ui_customers migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_offline_hooks.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Inyectar `ctx` real en las llamadas directas a `ui_sales.sale` y `ui_customers.customer_create`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_offline_hooks.py
```

**Step 2: Añadir import** (tras `from app.routers import ui_customers, ui_sales`):

```python
from app.routers.ui_context import UIContext
```

**Step 3: Inyectar `ctx` en cada llamada directa.** En `ui_sales.sale(...)` (3 sitios, líneas 61, 81, 103):

```python
# BEFORE:
    ui_sales.sale(
        request=request, product="SKU-OFFLINE", location_code="CENTRAL", quantity=1,
        unit_price="", movement_date=None, note=None, paid_by_transfer=False, db=db_session,
    )
# AFTER:
    ui_sales.sale(
        request=request, product="SKU-OFFLINE", location_code="CENTRAL", quantity=1,
        unit_price="", movement_date=None, note=None, paid_by_transfer=False, db=db_session,
        ctx=UIContext(request=request, db=db_session),
    )
```

En `ui_customers.customer_create(...)` (2 sitios, líneas 116 y 133), añade `ctx=UIContext(request=request, db=db_session)`.

**Edge cases (ya resueltos):**
- `_FakeRequest` tiene `session={"username": "opdesktop"}`; `ctx.business_id` resuelve `business_id` del operador sembrado.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_offline_hooks.py -v
# Expected: 5 passed
```

#### Expected Report

- [ ] `tests/unit/test_offline_hooks.py` — `ctx` inyectado en 5 llamadas
- [ ] 5 tests PASS

---

### T47: Adaptar `tests/unit/test_ui_businesses.py`

**Depends on:** T11 (ui_businesses migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_businesses.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Inyectar `ctx` real en las llamadas directas a `business_create`, `business_update`, `business_delete` y
`business_edit_form`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_ui_businesses.py
```

**Step 2: Añadir import**

```python
from app.routers.ui_context import UIContext
```

**Step 3: Añadir un helper** tras `_make_admin_request`:

```python
def _ctx(request, db):
    return UIContext(request=request, db=db)
```

**Step 4: Inyectar `ctx` en cada llamada directa.** Añade `ctx=_ctx(admin_request, db_session)` a cada llamada de
`business_create(...)`, `business_update(...)`, `business_delete(...)` y `business_edit_form(...)` (los 14 tests de
la clase `TestBusinessCreate` y los tests sueltos de editar/borrar).

**Edge cases (ya resueltos):**
- `admin_request` tiene `session={"username": "admin", ...}` y `admin_user` siembra `role="admin"`; `ctx.ensure_admin()`
  resuelve admin sin lanzar.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_businesses.py -v
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_ui_businesses.py` — `ctx` inyectado en todas las llamadas directas
- [ ] tests PASS

---

### T48: Adaptar `tests/unit/test_ui_xss_escapes.py`

**Depends on:** T30 (ui_sales_documents migrado) y T23 (ui_products migrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_xss_escapes.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Retargetar los monkeypatch e inyectar `ctx` en `sales_doc_customer_defaults` y `product_defaults_sale`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_ui_xss_escapes.py
```

**Step 2: Añadir import**

```python
from app.routers.ui_context import UIContext
```

**Step 3: Retargetar los monkeypatch**

```python
# BEFORE:
    monkeypatch.setattr(
        "app.routers.ui_sales_documents.require_active_business_id",
        lambda _db, _request: business_id,
    )
# AFTER:
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc, "require_active_business_id", lambda _db, _request: business_id)
```

Haz lo mismo para `app.routers.ui_products.require_active_business_id` (→ `_uc.require_active_business_id`).

**Step 4: Inyectar `ctx`**

```python
# BEFORE:
    response = sales_doc_customer_defaults(
        request=_request(),
        client_name="Cliente XSS",
        db=db_session,
    )
# AFTER:
    request = _request()
    response = sales_doc_customer_defaults(
        request=request, client_name="Cliente XSS", db=db_session,
        ctx=UIContext(request=request, db=db_session),
    )
```

Haz lo mismo en `product_defaults_sale(...)`.

**Edge cases (ya resueltos):**
- El test 3 (`test_home_charts_json_cannot_break_out_of_script_tag`) llama a `_home_charts_context` (auxiliar) y no cambia.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_xss_escapes.py -v
# Expected: 3 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_xss_escapes.py` — monkeypatch retargetado + `ctx` inyectado
- [ ] 3 tests PASS

---

## Lote D — macros + plantillas de fragmento

### T49: Macros en `components/forms.html`

**Depends on:** T48 (suite unit verde)
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/components/forms.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Añadir las macros `qty_label`, `number_input` y `text_input` a `components/forms.html`.

#### Instructions

**Step 1: Read the file**
```
Read app/templates/components/forms.html
```

**Step 2: Añadir las tres macros al final del fichero** (tras la macro `field`):

```jinja
{% macro qty_label(id, uom='', oob=False) -%}
<label id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %}>{% if uom %}Cantidad ({{ uom }}){% else %}Cantidad{% endif %}</label>
{%- endmacro %}

{% macro number_input(id, name, value=none, oob=False) -%}
<input id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %} name="{{ name }}" type="number" step="0.0001" min="0"{% if value is not none %} value="{{ value }}"{% endif %} />
{%- endmacro %}

{% macro text_input(id, name, value=none, oob=False, required=False) -%}
<input id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %} name="{{ name }}"{% if value is not none %} value="{{ value }}"{% endif %}{% if required %} required{% endif %} />
{%- endmacro %}
```

**Edge cases (ya resueltos):**
- `value=none` omite el atributo; `value=''` lo incluye vacío (igual que el HTML actual).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_components.py -v
# Expected: todos PASS (las macros nuevas no afectan los tests existentes)
```

#### Expected Report

- [ ] `app/templates/components/forms.html` — 3 macros añadidas
- [ ] `tests/unit/test_ui_components.py` PASS

---

### T50: Macros en `purchase_panel.html` y `sale_panel.html`

**Depends on:** T49
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/purchase_panel.html`, `app/templates/partials/sale_panel.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Sustituir el `<label>`/`<input>` por las macros en ambos paneles, con el mismo HTML resultante.

#### Instructions

**Step 1: Read the files**
```
Read app/templates/partials/purchase_panel.html
Read app/templates/partials/sale_panel.html
```

**Step 2: Añadir el import de macros** al inicio de ambos ficheros (línea 1):

```jinja
{% from 'components/forms.html' import qty_label, number_input %}
```

**Step 3: En `purchase_panel.html` (líneas 44-45):**

```html
<!-- BEFORE (línea 44): -->
    <div class="row"><label id="purchase-qty-label">Cantidad</label><input name="quantity" type="number" step="0.0001" min="0.0001" required value="{{ fv.get('quantity', '') }}" /></div>
<!-- AFTER: -->
    <div class="row">{{ qty_label('purchase-qty-label') }}<input name="quantity" type="number" step="0.0001" min="0.0001" required value="{{ fv.get('quantity', '') }}" /></div>

<!-- BEFORE (línea 45): -->
    <div class="row"><label>Costo unitario ({{ purchase_currency_code or 'EUR' }})</label><input id="purchase-unit-cost" name="unit_cost" type="number" step="0.0001" min="0" value="{{ fv.get('unit_cost', '') }}" /></div>
<!-- AFTER: -->
    <div class="row"><label>Costo unitario ({{ purchase_currency_code or 'EUR' }})</label>{{ number_input('purchase-unit-cost', 'unit_cost', fv.get('unit_cost', '')) }}</div>
```

**Step 4: En `sale_panel.html` (líneas 26-27):**

```html
<!-- BEFORE (línea 26): -->
      <div class="row"><label id="sale-qty-label">Cantidad</label><input name="quantity" type="number" step="0.0001" min="0" value="1" required /></div>
<!-- AFTER: -->
      <div class="row">{{ qty_label('sale-qty-label') }}<input name="quantity" type="number" step="0.0001" min="0" value="1" required /></div>

<!-- BEFORE (línea 27): -->
      <div class="row"><label>Precio unitario</label><input id="sale-unit-price" name="unit_price" type="number" step="0.0001" min="0" /></div>
<!-- AFTER: -->
      <div class="row"><label>Precio unitario</label>{{ number_input('sale-unit-price', 'unit_price') }}</div>
```

**Edge cases (ya resueltos):**
- No toques el `<div class="row">` ni la etiqueta de texto que rodea a las macros.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `purchase_panel.html` y `sale_panel.html` — macros aplicadas
- [ ] snapshot check PASS (canónico idéntico)

---

### T51: Macros en `cart_add.html` y `issue_form.html`

**Depends on:** T49
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/components/sales_doc/cart_add.html`, `app/templates/components/sales_doc/issue_form.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Sustituir el `<input>` por las macros en ambos componentes, con el mismo HTML resultante.

#### Instructions

**Step 1: Read the files**
```
Read app/templates/components/sales_doc/cart_add.html
Read app/templates/components/sales_doc/issue_form.html
```

**Step 2: En `cart_add.html` (línea 16):**

```html
<!-- BEFORE: -->
    <div class="row"><label>Precio unitario</label><input id="sales-doc-unit-price" name="unit_price" type="number" step="0.0001" min="0" value="0" /></div>
<!-- AFTER: -->
    <div class="row"><label>Precio unitario</label>{{ number_input('sales-doc-unit-price', 'unit_price', '0') }}</div>
```

Añade arriba el import:

```jinja
{% from 'components/forms.html' import number_input %}
```

**Step 3: En `issue_form.html` (líneas 26-27):**

```html
<!-- BEFORE (línea 26): -->
    <div class="row"><label>Cliente (ID)</label><input id="sales-doc-client-id" name="client_id" value="{{ draft.client_id or '' }}" /></div>
<!-- AFTER: -->
    <div class="row"><label>Cliente (ID)</label>{{ text_input('sales-doc-client-id', 'client_id', draft.client_id or '') }}</div>

<!-- BEFORE (línea 27): -->
    <div class="row"><label>Dirección (opcional)</label><input id="sales-doc-client-address" name="client_address" value="{{ draft.client_address or '' }}" /></div>
<!-- AFTER: -->
    <div class="row"><label>Dirección (opcional)</label>{{ text_input('sales-doc-client-address', 'client_address', draft.client_address or '') }}</div>
```

Añade arriba el import:

```jinja
{% from 'components/forms.html' import text_input %}
```

**Edge cases (ya resueltos):**
- El panel conserva `sin required` en `sales-doc-client-id` (REQ-12); el fragmento sí lleva `required=True`.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `cart_add.html` y `issue_form.html` — macros aplicadas
- [ ] snapshot check PASS

---

### T52: Fragmento `purchases/_defaults_oob.html` + endpoint `product_defaults_purchase`

**Depends on:** T50
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/purchases/_defaults_oob.html`, `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el HTML de `product_defaults_purchase` a una plantilla y devolverla con `TemplateResponse`.

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/purchases/_defaults_oob.html`:

```jinja
{% from 'components/forms.html' import qty_label, number_input %}
{{ qty_label('purchase-qty-label', uom, oob=True) }}
{{ number_input('purchase-unit-cost', 'unit_cost', cost, oob=True) }}
```

**Step 2: Sustituir el handler** en `app/routers/ui_products.py`:

```python
# BEFORE:
    uom = html.escape(p.unit_of_measure or "")
    cost = "" if p.default_purchase_cost is None else str(float(p.default_purchase_cost))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='purchase-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='purchase-unit-cost' hx-swap-oob='true' name='unit_cost' type='number' step='0.0001' min='0' value='{cost}' />"
    )

# AFTER:
    uom = p.unit_of_measure or ""
    cost = "" if p.default_purchase_cost is None else str(float(p.default_purchase_cost))
    return templates.TemplateResponse(
        request=request,
        name="features/purchases/_defaults_oob.html",
        context={"uom": uom, "cost": cost},
    )
```

**Edge cases (ya resueltos):**
- Las ramas que devuelven `HTMLResponse("")` (sin SKU / SKU no existente) se mantienen igual.
- `html` deja de usarse en este handler (Jinja2 autoescapa); el import `import html` se elimina en T59 si queda sin uso.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T53: Fragmento `sales/_defaults_oob.html` + endpoint `product_defaults_sale`

**Depends on:** T50
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/sales/_defaults_oob.html`, `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el HTML de `product_defaults_sale` a una plantilla.

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/sales/_defaults_oob.html`:

```jinja
{% from 'components/forms.html' import qty_label, number_input %}
{{ qty_label('sale-qty-label', uom, oob=True) }}
{{ number_input('sale-unit-price', 'unit_price', price, oob=True) }}
```

**Step 2: Sustituir el handler** en `app/routers/ui_products.py`:

```python
# BEFORE:
    uom = html.escape(p.unit_of_measure or "")
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='sale-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='sale-unit-price' hx-swap-oob='true' name='unit_price' type='number' step='0.0001' min='0' value='{price}' />"
    )

# AFTER:
    uom = p.unit_of_measure or ""
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    return templates.TemplateResponse(
        request=request,
        name="features/sales/_defaults_oob.html",
        context={"uom": uom, "price": price},
    )
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T54: Fragmento `sales/_product_options.html` + endpoint `sale_product_options`

**Depends on:** T50
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/sales/_product_options.html`, `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el bucle de `<option>` de `sale_product_options` a una plantilla compartida.

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/sales/_product_options.html`:

```jinja
{% for p in product_options %}
  <option value="{{ p.sku }} - {{ p.name }}"></option>
  <option value="{{ p.sku }}"></option>
  <option value="{{ p.name }}"></option>
{% endfor %}
```

**Step 2: Sustituir el handler** en `app/routers/ui_sales.py` (función `sale_product_options`):

```python
# BEFORE:
    parts: list[str] = []
    for p in items:
        sku = html.escape(str(getattr(p, "sku", "") or ""))
        name = html.escape(str(getattr(p, "name", "") or ""))
        parts.append(f'<option value="{sku} - {name}"></option>')
        parts.append(f'<option value="{sku}"></option>')
        parts.append(f'<option value="{name}"></option>')
    return HTMLResponse("".join(parts))

# AFTER:
    return templates.TemplateResponse(
        request=request,
        name="features/sales/_product_options.html",
        context={"product_options": items},
    )
```

**Edge cases (ya resueltos):**
- `items` son los productos con stock; la plantilla autoescapa `sku`/`name` (equivalente al `html.escape` actual).

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T55: Datalist de `sale_panel.html` usa `{% include %}`

**Depends on:** T54
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/sale_panel.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Sustituir el bucle del datalist `#product-options` por un `{% include %}` de la plantilla compartida.

#### Instructions

**Step 1: Read the file**
```
Read app/templates/partials/sale_panel.html
```

**Step 2: Sustituir (líneas 35-41):**

```html
<!-- BEFORE: -->
  <datalist id="product-options">
    {% for p in product_options %}
      <option value="{{ p.sku }} - {{ p.name }}"></option>
      <option value="{{ p.sku }}"></option>
      <option value="{{ p.name }}"></option>
    {% endfor %}
  </datalist>

<!-- AFTER: -->
  <datalist id="product-options">
    {% include 'features/sales/_product_options.html' %}
  </datalist>
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] `sale_panel.html` — datalist por `{% include %}`
- [ ] snapshot check PASS

---

### T56: Fragmentos `sales_documents/_product_defaults_oob.html` + endpoint `sales_doc_product_defaults`

**Depends on:** T51
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/sales_documents/_product_defaults_oob.html`, `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el HTML de `sales_doc_product_defaults` a una plantilla.

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/sales_documents/_product_defaults_oob.html`:

```jinja
{% from 'components/forms.html' import number_input %}
{{ number_input('sales-doc-unit-price', 'unit_price', price, oob=True) }}
```

**Step 2: Sustituir el handler** en `app/routers/ui_sales_documents.py`:

```python
# BEFORE:
    p = ProductService(db, business_id=bid).get_by_sku(sku)
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    return HTMLResponse(
        f"<input id='sales-doc-unit-price' hx-swap-oob='true' name='unit_price' type='number' step='0.0001' min='0' value='{html.escape(price)}' />"
    )

# AFTER:
    p = ProductService(db, business_id=bid).get_by_sku(sku)
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    return templates.TemplateResponse(
        request=request,
        name="features/sales_documents/_product_defaults_oob.html",
        context={"price": price},
    )
```

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T57: Fragmento `sales_documents/_customer_defaults_oob.html` + endpoint `sales_doc_customer_defaults`

**Depends on:** T51
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/sales_documents/_customer_defaults_oob.html`, `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el HTML de `sales_doc_customer_defaults` a una plantilla, conservando el `required` (REQ-12).

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/sales_documents/_customer_defaults_oob.html`:

```jinja
{% from 'components/forms.html' import text_input %}
{{ text_input('sales-doc-client-id', 'client_id', client_id, oob=True, required=True) }}
{{ text_input('sales-doc-client-address', 'client_address', client_address, oob=True) }}
```

**Step 2: Sustituir el handler** en `app/routers/ui_sales_documents.py`:

```python
# BEFORE:
    addr = (customer.address or "")
    return HTMLResponse(
        f"<input id='sales-doc-client-id' hx-swap-oob='true' name='client_id' value='{html.escape(str(customer.client_id or ''))}' required />"
        f"<input id='sales-doc-client-address' hx-swap-oob='true' name='client_address' value='{html.escape(addr)}' />"
    )

# AFTER:
    return templates.TemplateResponse(
        request=request,
        name="features/sales_documents/_customer_defaults_oob.html",
        context={"client_id": str(customer.client_id or ""), "client_address": customer.address or ""},
    )
```

**Edge cases (ya resueltos):**
- `required=True` solo en `sales-doc-client-id` (el panel no lo tiene — REQ-12, divergencia conservada).

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T58: Fragmento `transfers/_product_options.html` + endpoint `transfer_product_options`

**Depends on:** T49
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/transfers/_product_options.html`, `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el bucle de `<option>` de `transfer_product_options` a una plantilla (solo este endpoint; el datalist
inicial de `tab_transfers.html` no se toca).

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/transfers/_product_options.html`:

```jinja
{% for p in product_options %}
  {% set sku = (p.sku or '') %}
  {% set name = (p.name or '') %}
  {% if sku %}
  <option value="{{ sku }}{% if name %} - {{ name }}{% endif %}"></option>
  <option value="{{ sku }}"></option>
  {% if name %}<option value="{{ name }}"></option>{% endif %}
  {% endif %}
{% endfor %}
```

**Step 2: Sustituir el handler** en `app/routers/ui_transfers.py` (función `transfer_product_options`):

```python
# BEFORE:
    parts: list[str] = []
    for p in options:
        sku = str(getattr(p, "sku", "") or "")
        name = str(getattr(p, "name", "") or "")
        if not sku:
            continue
        label = f"{sku} - {name}" if name else sku
        parts.append(f'<option value="{escape(label)}"></option>')
        parts.append(f'<option value="{escape(sku)}"></option>')
        if name:
            parts.append(f'<option value="{escape(name)}"></option>')
    return HTMLResponse("\n".join(parts))

# AFTER:
    return templates.TemplateResponse(
        request=request,
        name="features/transfers/_product_options.html",
        context={"product_options": options},
    )
```

**Edge cases (ya resueltos):**
- `from html import escape` deja de usarse en este endpoint; elimínalo solo si no queda otra referencia.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido
- [ ] snapshot check PASS

---

### T59: Fragmento `stock/_supplier_return_lots.html` + endpoint `ui_supplier_return_lots`

**Depends on:** T49
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/stock/_supplier_return_lots.html`, `app/routers/ui_stock.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Mover el `<select>` de `ui_supplier_return_lots` a una plantilla con las 4 variantes (`empty`/`error`/`none`/`list`).

#### Instructions

**Step 1: Crear la plantilla** `app/templates/features/stock/_supplier_return_lots.html`:

```jinja
{% if state == 'empty' %}
<select name='lot_id' disabled><option value=''>-- Ingrese SKU --</option></select>
{% elif state == 'error' %}
<select name='lot_id' disabled><option value=''>Error: {{ error_msg }}</option></select>
{% elif state == 'none' %}
<select name='lot_id' disabled><option value=''>Sin lotes en esta ubicación</option></select>
{% else %}
<select name='lot_id' required>
  <option value=''>-- Selecciona lote --</option>
  {% for lot in lots %}<option value='{{ lot.id }}'>{{ lot.lot_code }} | disp={{ lot.qty_remaining }} | costo={{ lot.unit_cost }}</option>{% endfor %}
</select>
{% endif %}
```

**Step 2: Sustituir el handler** en `app/routers/ui_stock.py` (función `ui_supplier_return_lots`):

```python
# BEFORE (cuerpo desde `sku_clean = ...`):
    sku_clean = (sku or "").strip()
    loc = (location_code or "").strip() or None
    if not sku_clean:
        return HTMLResponse("<select name='lot_id' disabled><option value=''>-- Ingrese SKU --</option></select>")

    try:
        lots = service.available_lots(sku_clean, location_code=loc)
    except Exception as e:
        _log.exception("available_lots fallback en ui_supplier_return_lots: %s", e)
        error_msg = _html.escape(str(e)[:50]) if str(e) else "Error"
        return HTMLResponse(f"<select name='lot_id' disabled><option value=''>Error: {error_msg}</option></select>")

    if not lots:
        return HTMLResponse("<select name='lot_id' disabled><option value=''>Sin lotes en esta ubicación</option></select>")

    parts: list[str] = ["<select name='lot_id' required>"]
    parts.append("<option value=''>-- Selecciona lote --</option>")
    for lot in lots:
        parts.append(
            "<option value='{}'>{} | disp={} | costo={}</option>".format(
                int(lot.id),
                _html.escape(str(lot.lot_code or "")),
                float(lot.qty_remaining or 0),
                float(lot.unit_cost or 0),
            )
        )
    parts.append("</select>")
    return HTMLResponse("".join(parts))

# AFTER:
    sku_clean = (sku or "").strip()
    loc = (location_code or "").strip() or None
    if not sku_clean:
        return templates.TemplateResponse(request=request, name="features/stock/_supplier_return_lots.html",
                                          context={"state": "empty"})
    try:
        lots = service.available_lots(sku_clean, location_code=loc)
    except Exception as e:
        _log.exception("available_lots fallback en ui_supplier_return_lots: %s", e)
        error_msg = str(e)[:50] if str(e) else "Error"
        return templates.TemplateResponse(request=request, name="features/stock/_supplier_return_lots.html",
                                          context={"state": "error", "error_msg": error_msg})
    if not lots:
        return templates.TemplateResponse(request=request, name="features/stock/_supplier_return_lots.html",
                                          context={"state": "none"})
    return templates.TemplateResponse(request=request, name="features/stock/_supplier_return_lots.html",
                                      context={"state": "list", "lots": lots})
```

**Edge cases (ya resueltos):**
- `_html.escape` se sustituye por el autoescape de Jinja2. El recorte `str(e)[:50]` se conserva en Python.
- El import `import html as _html` se elimina si no queda otra referencia.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK
```

#### Expected Report

- [ ] plantilla creada + handler sustituido (4 variantes)
- [ ] snapshot check PASS

---

## Lote E — sidebar data-driven

### T60: Crear `app/routers/ui_nav.py`

**Depends on:** T48
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_nav.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Crear `NAV_ITEMS`, `NavItem`, `visible_nav_items` y `default_tab`.

#### Instructions

**Step 1: Crear el fichero** `app/routers/ui_nav.py` con este contenido exacto:

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

_ALL_ROLES = frozenset({"admin", "owner", "operator"})


@dataclass(frozen=True)
class NavItem:
    slug: str
    label: str
    roles: Optional[frozenset[str]]  # None = visible para cualquier rol (solo "sales")
    server_only: bool = False


NAV_ITEMS: tuple[NavItem, ...] = (
    NavItem("home", "Inicio", frozenset({"admin", "owner"})),
    NavItem("agent", "Dashboard Dinámico", frozenset({"admin", "owner"}), server_only=True),
    NavItem("inventory", "Inventario", frozenset({"admin", "owner"})),
    NavItem("purchases", "Compras", frozenset({"admin", "owner"}), server_only=True),
    NavItem("transfers", "Traspasos", frozenset({"admin", "owner"}), server_only=True),
    NavItem("coach", "Coach", frozenset({"admin"})),
    NavItem("sales", "Ventas", None),
    NavItem("documents", "Documentos", frozenset({"admin", "owner"})),
    NavItem("customers", "Clientes", frozenset({"admin", "owner"})),
    NavItem("expenses", "Costos operativos", frozenset({"admin", "owner"})),
    NavItem("profit-items", "Utilidades por artículo", frozenset({"admin", "owner"})),
    NavItem("profit", "Utilidades", frozenset({"admin", "owner"})),
    NavItem("dividends", "Dividendos", frozenset({"admin", "owner"})),
    NavItem("history", "Historial", frozenset({"admin", "owner"})),
    NavItem("users", "Usuarios", frozenset({"admin"})),
    NavItem("businesses", "Negocios", frozenset({"admin"})),
    NavItem("activity", "Actividad", frozenset({"admin"})),
)


def visible_nav_items(role: str, app_mode: str) -> list[NavItem]:
    out: list[NavItem] = []
    for item in NAV_ITEMS:
        if item.server_only and app_mode == "desktop":
            continue
        if item.roles is None or role in item.roles:
            out.append(item)
    return out


def default_tab(role: str) -> str:
    return "sales" if role == "operator" else "home"
```

**Edge cases (ya resueltos):**
- `role` llega normalizado `(user.role or "").lower()`; `""` (sin usuario) no coincide con ningún rol → solo "sales".

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.routers.ui_nav import NAV_ITEMS, visible_nav_items; assert len(NAV_ITEMS) == 17; assert [i.slug for i in visible_nav_items('operator','server')] == ['sales']; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/routers/ui_nav.py` creado con 17 items
- [ ] comando de verificación imprime OK

---

### T61: Crear `tests/unit/test_ui_nav.py`

**Depends on:** T60
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_nav.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Fijar la tabla de verdad de REQ-8 (6 combinaciones rol × modo) y `default_tab`.

#### Instructions

**Step 1: Crear el fichero** `tests/unit/test_ui_nav.py` con este contenido exacto:

```python
from __future__ import annotations

import pytest

from app.routers.ui_nav import default_tab, visible_nav_items

EXPECTED = {
    ("admin", "server"): ["home", "agent", "inventory", "purchases", "transfers", "coach", "sales",
                          "documents", "customers", "expenses", "profit-items", "profit", "dividends",
                          "history", "users", "businesses", "activity"],
    ("owner", "server"): ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
                          "customers", "expenses", "profit-items", "profit", "dividends", "history"],
    ("operator", "server"): ["sales"],
    ("admin", "desktop"): ["home", "inventory", "coach", "sales", "documents", "customers", "expenses",
                           "profit-items", "profit", "dividends", "history", "users", "businesses", "activity"],
    ("owner", "desktop"): ["home", "inventory", "sales", "documents", "customers", "expenses",
                           "profit-items", "profit", "dividends", "history"],
    ("operator", "desktop"): ["sales"],
}


@pytest.mark.parametrize("role,mode", list(EXPECTED))
def test_visible_nav_items(role: str, mode: str) -> None:
    assert [i.slug for i in visible_nav_items(role, mode)] == EXPECTED[(role, mode)]


def test_default_tab() -> None:
    assert default_tab("operator") == "sales"
    assert default_tab("admin") == "home"
    assert default_tab("owner") == "home"
    assert default_tab("") == "home"
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_nav.py -v
# Expected: 7 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_nav.py` creado
- [ ] 7 tests PASS

---

### T62: Bucle en `dashboard.html` + contexto en `ui_home.py`

**Depends on:** T60
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/dashboard.html`, `app/routers/ui_home.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Sustituir los 17 `<button>` del sidebar por un `{% for %}` y añadir `nav_items`/`default_tab` al contexto.

#### Instructions

**Step 1: Read the files**
```
Read app/templates/dashboard.html
Read app/routers/ui_home.py
```

**Step 2: En `ui_home.py`, en `dashboard`, añadir al contexto** (import `from app.routers.ui_nav import NAV_ITEMS, default_tab, visible_nav_items`):

```python
# BEFORE:
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "user": user,
            "businesses": businesses,
            "active_business_id": active_business_id,
            "active_business": active_business,
            "app_mode": app_mode_label(),
            "state": get_sync_state(db) if is_desktop() else None,
        },
    )

# AFTER:
    role = (user.role or "").lower() if user is not None else ""
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "user": user,
            "businesses": businesses,
            "active_business_id": active_business_id,
            "active_business": active_business,
            "app_mode": app_mode_label(),
            "state": get_sync_state(db) if is_desktop() else None,
            "nav_items": visible_nav_items(role, app_mode_label()),
            "default_tab": default_tab(role),
        },
    )
```

**Step 3: En `dashboard.html`, sustituir las líneas 34-129** (el `<nav class="sidebar-nav">` completo) por:

```html
        <nav class="sidebar-nav" role="navigation" aria-label="Secciones">
          {% for item in nav_items %}
          <button class="sidebar-item" id="tab-{{ item.slug }}" aria-selected="{% if item.slug == default_tab %}true{% else %}false{% endif %}"
                  hx-get="/ui/tabs/{{ item.slug }}" hx-target="#tab-content" hx-swap="innerHTML">
            {{ item.label }}
          </button>
          {% endfor %}
        </nav>
```

**Step 4: En `dashboard.html`, sustituir el `hx-get` de `#tab-content` (línea 136):**

```html
<!-- BEFORE: -->
        <div id="tab-content"
             hx-get="{% if user and (user.role or '')|lower == 'operator' %}/ui/tabs/sales{% else %}/ui/tabs/home{% endif %}" hx-trigger="load" hx-swap="innerHTML">
        </div>
<!-- AFTER: -->
        <div id="tab-content"
             hx-get="/ui/tabs/{{ default_tab }}" hx-trigger="load" hx-swap="innerHTML">
        </div>
```

**Edge cases (ya resueltos):**
- El bloque del copiloto (agente FAB) y `aria-selected` de `tab-home`/`tab-sales` quedan cubiertos por
  `default_tab` (equivalente, como demuestra el test de REQ-8).

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Expected: OK (dashboards de admin/owner/operator idénticos)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_nav.py -v
# Expected: 7 passed
```

#### Expected Report

- [ ] `dashboard.html` — bucle + `default_tab` en `#tab-content`
- [ ] `ui_home.py` — `nav_items`/`default_tab` en el contexto
- [ ] snapshot check + test_ui_nav PASS

---

## Lote F — guarda + limpieza + docs

### T63: Crear `tests/unit/test_routers_no_html.py`

**Depends on:** T59 (fragmentos migrados)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_routers_no_html.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Fijar la guarda de REQ-10: ningún router contiene HTML en Python y ninguna plantilla de `features/` usa `|safe`/`Markup(`.

#### Instructions

**Step 1: Crear el fichero** `tests/unit/test_routers_no_html.py` con este contenido exacto:

```python
from __future__ import annotations

import re
from pathlib import Path

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"
FEATURES_DIR = Path(__file__).resolve().parents[2] / "app" / "templates" / "features"

HTML_MARKERS = ("hx-swap-oob", "<option", "<input", "<select", "<label")


def test_no_html_markers_in_routers() -> None:
    offenders = []
    for path in sorted(ROUTERS_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for marker in HTML_MARKERS:
            if marker in text:
                offenders.append(f"{path.name}: {marker}")
    assert offenders == []


def test_features_templates_never_use_safe_or_markup() -> None:
    unsafe = re.compile(r"\|\s*safe\b|Markup\s*\(")
    offenders = [
        str(path.relative_to(FEATURES_DIR))
        for path in sorted(FEATURES_DIR.rglob("*.html"))
        if unsafe.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_routers_no_html.py -v
# Expected: 2 passed
```

#### Expected Report

- [ ] `tests/unit/test_routers_no_html.py` creado
- [ ] 2 tests PASS

---

### T64: Eliminar `ensure_admin`/`ensure_admin_or_owner` de `ui_common.py` si quedan sin uso

**Depends on:** T48 (tests adaptados) y T63
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_common.py`
**Pillars:** P1, P3, P4, P5, P6

#### Objective

Comprobar con grep que `ensure_admin`/`ensure_admin_or_owner` no tienen referencias y, si es 0, eliminarlas.

#### Instructions

**Step 1: Grep de referencias**
```bash
grep -rn "ensure_admin\b\|ensure_admin_or_owner\b" app/ tests/ || echo "SIN REFERENCIAS"
```

**Step 2: Solo si el grep no da resultados, borrar las dos funciones** en `app/routers/ui_common.py` (líneas 69-79):

```python
# BEFORE:
def ensure_admin(db: Session, request: Request) -> None:
    user = get_current_user_from_session(db, request)
    if user is None or (user.role or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")


def ensure_admin_or_owner(db: Session, request: Request) -> None:
    user = get_current_user_from_session(db, request)
    role = (user.role or "").strip().lower() if user is not None else ""
    if user is None or role not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin required")

# AFTER:
# (borrado)
```

**Step 3: Verificar que `get_current_user_from_session` y `HTTPException` siguen usándose en `ui_common.py`**
(o eliminar sus imports si también quedan sin uso).

**Edge cases (ya resueltos):**
- Si el grep encuentra alguna referencia (test, auxiliar), NO borres: deja las funciones y repórtalo.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q 2>&1 | tail -5
# Expected: todos PASS, 0 failed
```

#### Expected Report

- [ ] `ui_common.py` — `ensure_admin`/`ensure_admin_or_owner` eliminadas (solo si 0 referencias)
- [ ] `pytest tests/unit` verde

---

### T65: `CHANGELOG.md` (0.5.0) y diagnóstico §4 fila 4

**Depends on:** T64
**Skill:** technical-writer
**Agent:** docs
**Files:** `CHANGELOG.md`, `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`
**Pillars:** P1, P4, P5

#### Objective

Actualizar el CHANGELOG (sección 0.5.0) y marcar la fila 4 del diagnóstico como ✅.

#### Instructions

**Step 1: Read the files**
```
Read CHANGELOG.md
Read docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md
```

**Step 2: En `CHANGELOG.md`**, añade bajo `## 0.5.0 (sin publicar ...)` esta entrada:

```markdown
- refactor(frontend): fase 4 — plantillas OOB en `features/<dominio>/` (salen el HTML en Python de
  `product_defaults_purchase/sale`, `sales_doc_product_defaults`, `sales_doc_customer_defaults`,
  `sale_product_options`, `transfer_product_options` y `ui_supplier_return_lots`), macros compartidas
  `qty_label`/`number_input`/`text_input` en `components/forms.html`, dependencia perezosa `UIContext`
  en los 20 routers UI (una resolución de usuario/negocio por petición) y sidebar data-driven con
  `NAV_ITEMS`. Red ampliada: arnés H-303 (fragmentos, POST con error, roles, estados vacíos) y e2e de
  caracterización. HTML renderizado sin cambios (canónico idéntico a la baseline de b741a46).
```

**Step 3: En el diagnóstico**, cambia la fila de la fase 4 (tabla de §4):

```markdown
<!-- BEFORE: -->
| **4** | Plantillas OOB en vez de HTML en Python; `UIContext`; sidebar data-driven. **Prerrequisito en el mismo PAE: H-303** ... | Medio |
<!-- AFTER: -->
| **4** ✅ | Plantillas OOB en vez de HTML en Python; `UIContext`; sidebar data-driven. **Prerrequisito en el mismo PAE: H-303** ... | Medio |
```

#### Verification

```bash
git diff --name-only
# Expected: CHANGELOG.md y docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md
```

#### Expected Report

- [ ] `CHANGELOG.md` — entrada 0.5.0 añadida
- [ ] diagnóstico — fila 4 marcada ✅
- [ ] Ningún otro archivo modificado

---

### T66: `CLAUDE.md` y `.claude/CLAUDE.md`

**Depends on:** T65
**Skill:** technical-writer
**Agent:** docs
**Files:** `CLAUDE.md`, `.claude/CLAUDE.md`
**Pillars:** P1, P4, P5

#### Objective

Actualizar el estado de la fase 4 en los dos CLAUDE.md.

#### Instructions

**Step 1: Read the files**
```
Read CLAUDE.md
Read .claude/CLAUDE.md
```

**Step 2: En `CLAUDE.md`**, en la sección "Estado actual", añade al inicio de la lista:

```markdown
- [x] **Release 0.5.0, fase 4 del frontend: plantillas OOB, `UIContext` y sidebar data-driven (2026-11-01,
      rama `feature/frontend-fase4`, sin push)** — HTML en Python movido a `templates/features/<dominio>/`
      (macros `qty_label`/`number_input`/`text_input` compartidas por panel y fragmento), dependencia perezosa
      `UIContext` inyectada en los 20 routers UI (una resolución de usuario/negocio por petición; `ui_auth.py`
      fuera), sidebar data-driven con `NAV_ITEMS`/`visible_nav_items`/`default_tab`. Red ampliada: arnés H-303
      (fragmentos, POST con error, roles operator/owner, estados vacíos; baseline desde `b741a46`) y e2e de
      caracterización (`tests/e2e/test_fase4_characterization_e2e.py`). Canónico idéntico a la baseline.
      9 tests unitarios adaptados a `UIContext` (inyección de `ctx` real). +2 unit (`test_ui_context.py`,
      `test_ui_nav.py`) +1 guarda (`test_routers_no_html.py`).
```

**Step 3: En `.claude/CLAUDE.md`**, en "Estado actual del proyecto", añade una línea equivalente (fase 4 cerrada,
rama `feature/frontend-fase4`, sin push, entra en 0.5.0).

#### Verification

```bash
git diff --name-only
# Expected: CLAUDE.md y .claude/CLAUDE.md
```

#### Expected Report

- [ ] `CLAUDE.md` y `.claude/CLAUDE.md` actualizados
- [ ] Ningún otro archivo modificado
