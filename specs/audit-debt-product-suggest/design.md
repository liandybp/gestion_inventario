# Design — Sugerencias dinámicas de productos + deuda de la auditoría v0.4.11

Fuente: [requirements.md](requirements.md). Base: `6fce595`.

## 2.1 Codebase grounding

| Archivo | Rol | Acción | REQ |
|---|---|---|---|
| `app/repositories/product_repository.py` | Repo de productos | MODIFY: `list_sku_prefixes(prefix)` (global) | 6 |
| `app/repositories/purchase_repository.py` | Repo de compras | MODIFY: quitar `list_sku_prefixes` | 6 |
| `app/services/product_service.py` | `create`, `create_for_purchase`, `update`, `_generate_sku` | MODIFY: `_ensure_finite`, `_sku_conflict_detail`, `_insert_with_sku_retry` | 4, 5, 6 |
| `app/services/inventory_service.py` | `sale()` (l. 1719-1731) | MODIFY: `math.isfinite` | 4 |
| `app/routers/ui_common.py` | `parse_optional_float` (l. 125-142) | MODIFY: no finito → `None` | 4 |
| `app/routers/ui_purchases.py` | rutas de compras | MODIFY: quitar 9 `product_options`, helper `_product_panel_refresh` | 6, 7 |
| `app/routers/ui_products.py` | `product_update`, `product_from_invoice` | MODIFY: error en el modal; quitar `created_objs`; nueva ruta `product_suggest` | 1, 2, 3, 6 |
| `app/templates/partials/product_edit_form.html` | modal de edición | MODIFY: bloque de error + valores tecleados | 3 |
| `app/templates/partials/product_suggest_options.html` | — | CREATE: `<option>` por formato | 1, 2 |
| `app/static/js/app.js` | JS global (IIFE) | MODIFY: listener `input` para `data-product-suggest` | 1 |
| 9 plantillas con `list="..."` de productos | campos de artículo | MODIFY: atributo `data-product-suggest` | 1 |
| `tests/unit/test_repositories.py`, `test_product_service.py`, `test_inventory_service_unit.py` | tests | MODIFY | 4-6 |
| `tests/unit/test_ui_common_parse.py`, `tests/unit/test_ui_product_suggest.py`, `tests/e2e/test_product_suggest_e2e.py` | tests | CREATE | 1-4 |
| `tests/e2e/test_product_creation_in_purchase_e2e.py` | e2e de v0.4.11 | MODIFY: `test_product_edit_modal_closes_and_shows_error` → el modal se queda | 3 |
| `VERSION`, `.bumpversion.cfg` | 0.4.11 | MODIFY → 0.4.12 | DoD |

## 2.2 Enfoque

### REQ-1/2 — Búsqueda dinámica sin `hx-get`
Los campos de Compras, Ventas y Documentos **ya usan `hx-get`** para cargar valores por defecto al
cambiar (`/ui/product-defaults/...`). Un elemento solo admite un `hx-get`, así que la búsqueda va en
un **listener global de `app.js`** (delegación sobre `input`) para los `<input data-product-suggest="fmt">`:
1. debounce 250 ms; si `value.trim()` < 2 caracteres, no hace nada;
2. `fetch('/ui/product-suggest?q=…&fmt=…')`;
3. descarta la respuesta si `!resp.ok`, si `resp.redirected` (sesión caducada → login) o si el valor
   del campo cambió mientras tanto;
4. `input.list.innerHTML = await resp.text()`.

Al ser un `fetch` sin `HX-Request`, el middleware la cuenta como actividad del usuario (correcto: está
tecleando). El servidor reutiliza `ProductService.search(query, limit=30)` (normalización de tildes ya
implementada) y renderiza `partials/product_suggest_options.html` (autoescape).

### REQ-3 — Error dentro del modal
En el `except HTTPException` de `product_update`, en vez de `product_panel.html` se devuelve
`product_edit_form.html` con los valores tecleados (`SimpleNamespace`), `form_error` y las cabeceras
`HX-Retarget: #product-edit-modal`, `HX-Reswap: outerHTML` (htmx 1.9.12 las soporta). El handler
`htmx:beforeSwap` ya pinta los 4xx HTML; en `htmx:afterRequest` el `target` pasa a ser el propio modal
(`modal.contains(target)` → no se cierra). La URL del formulario sigue usando el SKU **original**; el
campo "SKU" muestra `new_sku_value` (lo tecleado).

### REQ-4 — No finitos
- `parse_optional_float`: `return v if math.isfinite(v) else None`.
  Como `None` significa a la vez "vacío" e "inválido", `purchase()` comprueba si el campo venía
  relleno y da 422 (T9b). Detectado al validar el PAE en un worktree: sin T9b, `sale_price="nan"`
  pasaba a precio 0 y el e2e existente fallaba.
- `ProductService`: función de módulo `_ensure_finite(**fields)` → 422, llamada al inicio de `create`
  y `update`. No se toca `allow_inf_nan` en los schemas: varias rutas UI construyen schemas desde
  `Form(float)` y un `ValidationError` sería un 500 silencioso (lección H-101).
- `InventoryService.sale`: `not math.isfinite(...)` junto a las comprobaciones existentes.

### REQ-5/6 — Asignación de SKU unificada
```python
def _insert_with_sku_retry(self, build: Callable[[str], Product], sku_raw: str, *, commit: bool) -> Product
def _sku_conflict_detail(self, sku_raw: str) -> str
```
Auto SKU: hasta 5 intentos; SKU explícito: 1. `IntegrityError` → `rollback` (en `commit=False` el
llamador debe invocarlo antes de cualquier otra escritura pendiente, igual que hoy). Tras agotar,
409 con `_sku_conflict_detail` (consulta por negocio tras el rollback). `update()` usa también
`_sku_conflict_detail(new_sku or original_sku)`.

**Invariantes:**
- Numeración global de SKU (`products.sku` unique global, ADR-012): `list_sku_prefixes` sin filtro de negocio.
- `create_for_purchase` solo hace `flush` (transacción única de v0.4.11).
- `default_sale_price=None` en la importación de productos (`product_from_invoice`) se mantiene.
- Los mensajes de error de `create()` pasan de "SKU already exists" (inglés) al texto de REQ-5; ningún
  test comprueba el texto antiguo (solo el 409).

## 2.4 Contratos

### GET /ui/product-suggest (nuevo)
- **Auth:** sesión (middleware `/ui`); todos los roles; `require_active_business_id`.
- **Query:** `q: str = ""`, `fmt: str = "label"` (`label|sale|sku`; otro → `label`).
- **200:** `text/html` con `<option>`; vacío si `len(q.strip()) < 2`.

### POST /ui/product/{sku}/update (cambio en error)
- **4xx:** `product_edit_form.html` + `HX-Retarget: #product-edit-modal` + `HX-Reswap: outerHTML`.

## 2.5 Seguridad

| Check | Respuesta |
|---|---|
| Nuevo endpoint | GET de solo lectura, mismo nivel que `/ui/product-defaults/sale` (sesión + negocio) |
| Fuga entre negocios | `ProductService(business_id=activo).search` filtra por negocio |
| XSS | Opciones renderizadas por Jinja con autoescape (test con `<b>`) |
| Enumeración | Solo productos del propio negocio; mínimo 2 caracteres |
| CSRF | GET: no aplica |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Disparo de la búsqueda | Listener global en `app.js` | `hx-get` en el input | Choca con el `hx-get` de valores por defecto |
| Respuesta | `<option>` HTML | JSON | El cliente solo hace `innerHTML`; el escapado lo garantiza Jinja |
| Búsqueda | `ProductService.search` (Python) | SQL `ILIKE` | Mantiene la normalización de tildes; volumen actual asumible |
| No finitos en schemas | No (`_ensure_finite` en servicio) | `allow_inf_nan=False` | `ValidationError` en rutas UI = 500 silencioso |
| Datalist inicial | Se mantiene | Vaciarlo | Sin JS o antes de teclear sigue habiendo sugerencias |

## 2.7 Migración y rollback
Sin migración. Rollback: `git revert`.

## 2.9 Pre-mortem (auditoria-ingenieria)

| Dimensión | Riesgo | Mitigación en envelopes |
|---|---|---|
| 1. SoC/SRP | El helper de SKU con demasiadas responsabilidades | `build` (construcción) separado de `_insert_with_sku_retry` (persistencia) y `_sku_conflict_detail` (mensaje) — T3 |
| 2. Cohesión | `ProductService` dependiendo de `PurchaseRepository` | T1 mueve la consulta a `ProductRepository` y quita el import |
| 3. Capas | Lógica de búsqueda en el router | El router solo valida `q`/`fmt` y delega en `ProductService.search` — T17 |
| 5. Robustez | Respuesta del login insertada en el datalist; respuestas fuera de orden | `resp.redirected` y comparación del valor actual — T19 |
| 5. Robustez | `ValidationError` por schemas estrictos | No se cambian schemas (trade-off) |
| 6. Transaccional | `_insert_with_sku_retry(commit=False)` hace rollback de toda la transacción | Docstring + mismo contrato que hoy; `create_for_purchase` sigue llamándose antes de otras escrituras |
| 9. Pruebas | Tests que no distinguen 0 de None, o e2e que no ejercita el fetch | e2e vacía el datalist antes de teclear (T25); unit con 205 productos (T18) |
