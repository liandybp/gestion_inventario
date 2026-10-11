# Tasks — remove-duplicate-product-creation

7 envelopes. Ejecutar **uno por sesión**, en orden. Cada envelope es autocontenido.
Fuentes: [requirements.md](requirements.md) · [design.md](design.md).

---

### T1: Quitar el formulario "Crear producto manualmente"

**Depends on:** none · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/partials/product_panel.html`
**Pillars:** P1, P4, P5, P6

#### Objective
Borrar las líneas 27-39 de `app/templates/partials/product_panel.html`.

#### Instructions
**Step 1:** Leer `app/templates/partials/product_panel.html`.

**Step 2:** Borrar exactamente este bloque (líneas 27-39, incluida la línea en blanco final):

```html
  <h3>Crear producto manualmente</h3>
  <form hx-post="/ui/product" hx-target="#product-panel" hx-swap="outerHTML" hx-encoding="multipart/form-data" enctype="multipart/form-data">
    <div class="row"><label>SKU</label><input name="sku" placeholder="(auto si lo dejas vacío)" /></div>
    <div class="row"><label>Nombre</label><input name="name" required /></div>
    <div class="row"><label>Unidad</label><input name="unit_of_measure" placeholder="(ej: u, kg, m, caja)" /></div>
    <div class="row"><label>Imagen</label><input name="image_file" type="file" accept="image/*" /></div>
    <div class="row"><label>Categoría</label><input name="category" placeholder="(opcional)" /></div>
    <div class="row"><label>Stock mínimo</label><input name="min_stock" type="number" step="0.0001" min="0" value="0" /></div>
    <div class="row"><label>Costo compra por defecto</label><input name="default_purchase_cost" type="number" step="0.0001" min="0" required /></div>
    <div class="row"><label>Precio venta por defecto</label><input name="default_sale_price" type="number" step="0.0001" min="0" required /></div>
    <button type="submit">Crear producto</button>
  </form>

```

Tras el borrado, el `{% endif %}` del aviso de facturas (línea 25) va seguido de una línea en
blanco y de `  <datalist id="product-options">`.

**NO tocar:** el form `from-invoice`, `<datalist id="product-options">` (lo usa el formulario de
compra), el bloque `{% if message %}`, `#product-edit` ni "Historial de productos".

#### Verification
```bash
grep -c 'hx-post="/ui/product"' app/templates/partials/product_panel.html   # Expected: 0
grep -c 'id="product-options"' app/templates/partials/product_panel.html    # Expected: 1
grep -c 'hx-post="/ui/product/from-invoice"' app/templates/partials/product_panel.html  # Expected: 1
```

#### Expected Report
- [ ] `product_panel.html`: líneas 27-39 borradas
- [ ] Ningún otro archivo modificado
- [ ] Los 3 `grep` devuelven 0, 1 y 1

---

### T2: Eliminar la ruta `POST /ui/product`

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-2
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Context
```python
# app/routers/ui_products.py — línea 15 (ESTO EXISTE)
from app.schemas import AdjustmentCreate, ProductCreate, ProductUpdate
# líneas 37-104: @router.post("/product", response_class=HTMLResponse) / def create_product(...)
# líneas 105-106: en blanco · línea 107: @router.post("/product/{sku:path}/delete", ...)
```

#### Objective
Borrar la función `create_product` y quitar `ProductCreate` del import.

#### Instructions
**Step 1:** Leer `app/routers/ui_products.py`.

**Step 2:** Borrar las líneas 37-106: desde `@router.post("/product", response_class=HTMLResponse)`
hasta el final de `def create_product` (su último `)` cierra el `TemplateResponse` del
`except HTTPException`) **más las 2 líneas en blanco siguientes**. Resultado: tras
`router = APIRouter()` vienen 2 líneas en blanco y luego
`@router.post("/product/{sku:path}/delete", response_class=HTMLResponse)`.

**Step 3:** Línea 15:
```python
# BEFORE
from app.schemas import AdjustmentCreate, ProductCreate, ProductUpdate
# AFTER
from app.schemas import AdjustmentCreate, ProductUpdate
```

**NO tocar** ningún otro import: `File`, `UploadFile`, `save_product_image` y `log_event` se
siguen usando en otras rutas del archivo.

#### Verification
```bash
grep -c '@router.post("/product",' app/routers/ui_products.py   # Expected: 0
grep -c 'ProductCreate' app/routers/ui_products.py              # Expected: 0
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "import app.main; print('import OK')"
```

#### Expected Report
- [ ] `create_product` y sus 2 líneas en blanco borradas; `ProductCreate` fuera del import
- [ ] Los 2 `grep` devuelven 0; `import OK`

---

### T3: `purchase()` acepta `sale_price` para el producto nuevo

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-3, REQ-4, REQ-5
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P2, P4, P5, P6

#### Context
```python
# app/routers/ui_common.py — YA EXISTE (importado en ui_purchases.py)
def parse_optional_float(value: Optional[str]) -> Optional[float]: ...  # vacío/no numérico → None
```

#### Objective
Añadir el parámetro `sale_price` a `purchase()` y usarlo en las 2 creaciones de `Product`.

#### Instructions
**Step 1:** Leer `app/routers/ui_purchases.py` (función `purchase`, desde la línea 334).

**Step 2 — firma** (línea 337):
```python
# BEFORE
    product_name: str = Form(""),
    quantity: float = Form(...),
# AFTER
    product_name: str = Form(""),
    sale_price: str = Form(""),
    quantity: float = Form(...),
```

**Step 3 — validación** (líneas 360-361, dentro del `try`):
```python
# BEFORE
    try:
        created_product = None
# AFTER
    try:
        created_product = None
        parsed_sale_price = parse_optional_float(sale_price)
        if parsed_sale_price is not None and parsed_sale_price < 0:
            raise HTTPException(status_code=422, detail="El precio de venta no puede ser negativo")
```

**Step 4 — uso:** en `purchase()` hay exactamente 2 sitios con estas dos líneas seguidas
(con 20 y 16 espacios de sangría respectivamente). Cambiar SOLO la segunda línea en ambos:
```python
# BEFORE
default_purchase_cost=parsed_cost,
default_sale_price=0,
# AFTER (misma sangría que el original)
default_purchase_cost=parsed_cost,
default_sale_price=parsed_sale_price if parsed_sale_price is not None else 0,
```
**NO tocar** el `default_sale_price=0,` de la línea 225 (función `purchase_from_invoice`,
precedido de `default_purchase_cost=unit_cost_vat,`).

#### Verification
```bash
grep -c 'default_sale_price=0,' app/routers/ui_purchases.py                 # Expected: 1
grep -c 'default_sale_price=parsed_sale_price' app/routers/ui_purchases.py  # Expected: 2
grep -c 'sale_price: str = Form("")' app/routers/ui_purchases.py            # Expected: 1
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "import app.main; print('import OK')"
```

#### Expected Report
- [ ] Firma, validación y las 2 asignaciones cambiadas; línea 225 intacta
- [ ] Los 3 `grep` devuelven 1, 2 y 1; `import OK`

---

### T4: Campo "Precio de venta (si es nuevo)" en el formulario de compra

**Depends on:** T3 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-3
**Files:** `app/templates/partials/purchase_panel.html`
**Pillars:** P1, P4, P5, P6

#### Objective
Insertar una fila nueva justo después de "Nombre (si es nuevo)" (línea 41).

#### Instructions
**Step 1:** Leer `app/templates/partials/purchase_panel.html`.

**Step 2:**
```html
<!-- BEFORE (línea 41) -->
    <div class="row"><label>Nombre (si es nuevo)</label><input name="product_name" placeholder="Solo si el SKU no existe" /></div>
<!-- AFTER (líneas 41-42) -->
    <div class="row"><label>Nombre (si es nuevo)</label><input name="product_name" placeholder="Solo si el SKU no existe" /></div>
    <div class="row"><label>Precio de venta (si es nuevo)</label><input name="sale_price" type="number" step="0.0001" min="0" placeholder="Solo si el SKU no existe" /></div>
```

#### Verification
```bash
grep -c 'name="sale_price"' app/templates/partials/purchase_panel.html   # Expected: 1
```

#### Expected Report
- [ ] Fila insertada tras la línea 41; ningún otro cambio

---

### T5: e2e Playwright (1/2) — crear el archivo

**Depends on:** T1, T2 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-1, REQ-2
**Files:** `tests/e2e/test_product_creation_in_purchase_e2e.py` (CREATE)
**Pillars:** P1, P4, P5, P6, P7

#### Context
```python
# tests/e2e/conftest.py — fixture `live_server` (str, URL base), usuario admin/admin.
# Middleware CSRF: POST a /ui/* sin cabecera "HX-Request: true" → 403.
```

#### Objective
Crear `tests/e2e/test_product_creation_in_purchase_e2e.py` con este contenido exacto:

```python
"""E2E: la creación de producto vive solo en "Registrar compra"."""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

FORM = '#purchase-panel form[hx-post="/ui/purchase"]'


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def test_purchases_tab_has_no_manual_product_form(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    expect(page.locator('form[hx-post="/ui/product"]')).to_have_count(0)
    expect(page.locator('form[hx-post="/ui/product/from-invoice"]')).to_have_count(1)
    expect(page.locator("datalist#product-options")).to_have_count(1)


def test_post_ui_product_route_removed(live_server: str, page: Page) -> None:
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/product",
        headers={"HX-Request": "true"},
        form={"name": "X", "default_purchase_cost": "1", "default_sale_price": "1"},
    )
    assert resp.status in (404, 405)
```

#### Verification
```bash
.venv/bin/python -m pytest tests/e2e/test_product_creation_in_purchase_e2e.py -m e2e --no-cov -v
# Expected: 2 passed
```

#### Expected Report
- [ ] Archivo creado con el contenido exacto · 2 passed

---

### T6: e2e Playwright (2/2) — precio de venta

**Depends on:** T3, T4, T5 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-3, REQ-5
**Files:** `tests/e2e/test_product_creation_in_purchase_e2e.py`
**Pillars:** P1, P4, P5, P6, P7

#### Context
```python
# El archivo ya existe (T5) y define: FORM = '#purchase-panel form[hx-post="/ui/purchase"]'
# y _login(page, base_url). products_history.html muestra el precio como '%.2f' → "7.50".
```

#### Objective
Añadir al FINAL de `tests/e2e/test_product_creation_in_purchase_e2e.py` (2 líneas en blanco antes) exactamente:

```python


def test_purchase_new_product_with_sale_price(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    page.fill(f'{FORM} input[name="product_name"]', "Producto Precio E2E")
    page.fill(f'{FORM} input[name="sale_price"]', "7.50")
    page.fill(f'{FORM} input[name="quantity"]', "1")
    page.fill(f'{FORM} input[name="unit_cost"]', "3.00")
    page.click(f'{FORM} button[type="submit"]')
    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()

    page.click("#tab-inventory")
    page.click("#tab-purchases")
    row = page.locator("#product-panel tr", has_text="Producto Precio E2E")
    expect(row).to_contain_text("7.50")


def test_purchase_rejects_negative_sale_price(live_server: str, page: Page) -> None:
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/purchase",
        headers={"HX-Request": "true"},
        form={"product_name": "Producto Negativo E2E", "sale_price": "-1", "quantity": "1", "unit_cost": "1"},
    )
    assert resp.status == 422
    assert "El precio de venta no puede ser negativo" in resp.text()
```

#### Verification
```bash
.venv/bin/python -m pytest tests/e2e/test_product_creation_in_purchase_e2e.py -m e2e --no-cov -v
# Expected: 4 passed
```

#### Expected Report
- [ ] 2 tests añadidos al final, sin tocar los existentes · 4 passed

---

### T7: VERSION 0.4.10 → 0.4.11

**Depends on:** T6 · **Skill:** release-manager · **Agent:** coder · **REQ:** — (DoD)
**Files:** `VERSION`
**Pillars:** P1, P4, P5, P6

#### Instructions
Reemplazar el contenido de `VERSION` (`0.4.10`) por `0.4.11` (una línea, con salto de línea final).

#### Verification
```bash
cat VERSION   # Expected: 0.4.11
```
