# Phase 3 — Task Envelopes: Traspaso masivo

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T8.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`
> **Regla dura:** ≤2 archivos por envelope · código exacto (before/after) · verificación mecánica · cero placeholders.

---

### T1: Template `transfer_bulk_modal.html`

**Depends on:** none
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/transfer_bulk_modal.html`
**Pillars:** P1, P4, P6

#### Context

```html
<!-- app/templates/partials/transfer_edit_form.html — YA EXISTE (patrón de modal):
     estructura .modal-overlay > .modal-container > .modal-content, botón data-modal-close -->
<!-- app/templates/components/ui.html — YA EXISTE:
     {% macro flash(message=None, message_class='', message_detail=None) -%} ... -->
```

#### Objective

Crear el partial de la ventana flotante de traspaso masivo.

#### Instructions

**Step 1:** Crear `app/templates/partials/transfer_bulk_modal.html` con EXACTAMENTE este contenido:

```html
{% from 'components/ui.html' import flash %}
<div class="modal-overlay" id="transfer-bulk-modal">
  <div class="modal-container" style="max-width: 760px;">
    <div class="modal-content">
      <div class="modal-header">
        <h3 class="modal-title">Traspaso masivo</h3>
        <button type="button" class="modal-close" data-modal-close="transfer-bulk-modal">&times;</button>
      </div>

      {{ flash(message, message_class or '', message_detail) }}

      <p class="muted">
        Origen: <strong>{{ from_name }}</strong> &rarr; Destino: <strong>{{ to_name }}</strong>
      </p>

      <div class="row">
        <label>Buscar</label>
        <input type="text" id="transfer-bulk-search" placeholder="Filtrar por SKU o nombre" />
      </div>

      <form hx-post="/ui/transfers/bulk" hx-target="#transfer-bulk-modal" hx-swap="outerHTML" hx-indicator="#transfer-bulk-loading">
        <input type="hidden" name="from_location_code" value="{{ from_code }}" />
        <input type="hidden" name="to_location_code" value="{{ to_code }}" />
        <input type="hidden" name="movement_date" value="{{ movement_date or '' }}" />
        <input type="hidden" name="note" value="{{ note or '' }}" />

        <div class="table-wrapper" style="max-height: 48vh; overflow-y: auto;">
          <table class="table-compact transfer-bulk-table">
            <thead>
              <tr>
                <th style="width: 3%;">
                  <input type="checkbox" id="transfer-bulk-select-all" aria-label="Seleccionar todo" />
                </th>
                <th>Producto</th>
                <th style="width: 120px;" class="text-right">Stock</th>
                <th style="width: 150px;" class="text-right">Cantidad</th>
              </tr>
            </thead>
            <tbody>
              {% for p in products %}
                {% set is_checked = (p.sku in checked_skus) %}
                {% set qty_value = quantities.get(p.sku, p.stock) %}
              <tr class="transfer-bulk-row" data-search="{{ (p.sku ~ ' ' ~ (p.name or ''))|lower }}">
                <td>
                  <input type="checkbox" class="transfer-bulk-check" {% if is_checked %}checked{% endif %} />
                </td>
                <td><code>{{ p.sku }}</code> {{ p.name }}{% if p.uom %} <span class="muted">({{ p.uom }})</span>{% endif %}</td>
                <td class="text-right nowrap">{{ "%.2f"|format(p.stock) }}</td>
                <td class="text-right">
                  <input type="hidden" name="product" value="{{ p.sku }}" {% if not is_checked %}disabled{% endif %} />
                  <input type="number" name="quantity" step="0.01" min="0" value="{{ qty_value }}" {% if not is_checked %}disabled{% endif %} />
                </td>
              </tr>
              {% endfor %}
              {% if not products %}
              <tr><td colspan="4" class="muted text-center">No hay stock en {{ from_name }}.</td></tr>
              {% endif %}
            </tbody>
          </table>
        </div>

        <div class="mt-12 flex gap-10 items-center">
          <button type="submit" class="mb-0">Traspasar</button>
          <button type="button" class="btn-secondary mb-0" data-modal-close="transfer-bulk-modal">Cancelar</button>
          <span id="transfer-bulk-loading" class="htmx-indicator">Procesando...</span>
        </div>
      </form>
    </div>
  </div>
</div>
```

**Edge cases (ya resueltos):**
- `products` vacío → se renderiza la fila "No hay stock en ..." y el form se envía sin líneas (el POST devuelve error amable).
- `checked_skus`/`quantities` son `set()`/`{}` en el GET inicial → todo deshabilitado.

#### Verification

```bash
python3 -c "from pathlib import Path; print(Path('app/templates/partials/transfer_bulk_modal.html').exists())"
# Expected: True
```

#### Expected Report

- [ ] `app/templates/partials/transfer_bulk_modal.html` creado
- [ ] Verificación imprime `True`

---

### T2: Helpers `_parse_transfer_lines` + `_audit_transfer`

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4

#### Context

```python
# app/routers/ui_transfers.py — línea 16, import YA EXISTE:
from app.schemas import TransferCreate, TransferLineCreate
# app/routers/ui_transfers.py — línea 14, import YA EXISTE:
from app.audit import log_event
# app/routers/ui_transfers.py — línea 10, import YA EXISTE:
from sqlalchemy.orm import Session
```

#### Objective

Añadir dos helpers a nivel de módulo (tras `_resolve_transfer_sku`, antes de `transfer_edit_form`).

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_transfers.py
```

**Step 2: Cambiar el import de la línea 16**

```python
# BEFORE:
from app.schemas import TransferCreate, TransferLineCreate

# AFTER:
from app.schemas import TransferCreate, TransferLineCreate, TransferResult
```

**Step 3: Insertar tras el final de `_resolve_transfer_sku` (línea 48, justo antes de `@router.get("/movement/transfer/{movement_id}/edit"`)**

```python
def _parse_transfer_lines(products: list[str], quantities: list[str]) -> list[TransferLineCreate]:
    lines: list[TransferLineCreate] = []
    for p, q in zip(products, quantities):
        sku = (p or "").strip()
        if " - " in sku:
            sku = sku.split(" - ", 1)[0].strip()
        if not sku:
            continue
        try:
            qty = float((q or "").strip() or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        lines.append(TransferLineCreate(sku=sku, quantity=qty))
    return lines


def _audit_transfer(db: Session, user, result: TransferResult) -> None:
    for line in result.lines:
        for mv_id in (line.movements_out or []) + (line.movements_in or []):
            log_event(
                db,
                user,
                action="transfer_create",
                entity_type="movement",
                entity_id=str(mv_id),
                detail={
                    "sku": line.sku,
                    "quantity": float(line.quantity),
                    "from_location_code": result.from_location_code,
                    "to_location_code": result.to_location_code,
                    "transfer_ref": result.transfer_ref,
                },
            )
```

**Edge cases (ya resueltos):**
- `_parse_transfer_lines` omite SKU vacío, cantidad no numérica (`except` → 0) y cantidad ≤ 0.
- `_audit_transfer` itera `movements_out` + `movements_in` (pueden ser `None`).

#### Verification

```bash
python3 -c "from app.routers.ui_transfers import _parse_transfer_lines, _audit_transfer; print('ok')"
# Expected: ok
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — import `TransferResult` añadido
- [ ] `app/routers/ui_transfers.py` — 2 helpers añadidos tras `_resolve_transfer_sku`
- [ ] Verificación imprime `ok`

---

### T3: Refactor `create_transfer` para usar los helpers

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P4

#### Context

```python
# app/routers/ui_transfers.py — función `create_transfer` (líneas ~435-592).
# Bloque A (parseo inline) está en las líneas 463-479.
# Bloque B (audit inline) está en las líneas 496-512.
```

#### Objective

Sustituir los bloques inline de `create_transfer` por llamadas a los helpers de T2 (comportamiento idéntico).

#### Instructions

**Step 1: Read the file** (localizar los dos bloques exactos)
```
Read app/routers/ui_transfers.py
```

**Step 2: Reemplazar el bloque A (parseo)**

```python
# BEFORE:
    products = form.getlist("product")
    quantities = form.getlist("quantity")

    lines: list[TransferLineCreate] = []
    for p, q in zip(products, quantities):
        sku = (p or "").strip()
        if " - " in sku:
            sku = sku.split(" - ", 1)[0].strip()
        if not sku:
            continue
        try:
            qty = float((q or "").strip() or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        lines.append(TransferLineCreate(sku=sku, quantity=qty))

# AFTER:
    products = form.getlist("product")
    quantities = form.getlist("quantity")
    lines = _parse_transfer_lines(products, quantities)
```

**Step 3: Reemplazar el bloque B (audit)**

```python
# BEFORE:
        if user is not None:
            for line in result.lines:
                for mv_id in (line.movements_out or []) + (line.movements_in or []):
                    log_event(
                        db,
                        user,
                        action="transfer_create",
                        entity_type="movement",
                        entity_id=str(mv_id),
                        detail={
                            "sku": line.sku,
                            "quantity": float(line.quantity),
                            "from_location_code": result.from_location_code,
                            "to_location_code": result.to_location_code,
                            "transfer_ref": result.transfer_ref,
                        },
                    )

# AFTER:
        if user is not None:
            _audit_transfer(db, user, result)
```

**Edge cases (ya resueltos):**
- La variable `products`/`quantities` sigue existiendo → el `except` de `create_transfer` (que arma `preserved_lines`) sigue funcionando.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q
# Expected: PASS (import y refactor no rompen)
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — `create_transfer` usa `_parse_transfer_lines` y `_audit_transfer`
- [ ] Ningún otro archivo modificado
- [ ] Verificación — PASS

---

### T4: Endpoint `GET /ui/transfers/bulk`

**Depends on:** T1, T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P6

#### Context

```python
# app/services/inventory_service.py — YA EXISTE:
#   service.stock_list(query="", location_code=from_code) -> list[StockRead]
#   StockRead: sku, name, unit_of_measure, quantity, min_stock, needs_restock
# app/routers/ui_transfers.py — imports YA presentes: get_active_business_config,
#   ensure_admin_or_owner, require_active_business_id, InventoryService, templates
```

#### Objective

Añadir el endpoint que renderiza la ventana flotante con el checklist filtrado por origen.

#### Instructions

**Step 1:** Añadir al final de `app/routers/ui_transfers.py` (después de la última función `transfer_print`):

```python
@router.get("/transfers/bulk", response_class=HTMLResponse)
def transfer_bulk_modal(
    request: Request,
    db: Session = Depends(session_dep),
    from_location_code: str = "",
    to_location_code: str = "",
    movement_date: str = "",
    note: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)

    from_code = (from_location_code or "").strip() or str(cfg.locations.central.code).strip()
    to_code = (to_location_code or "").strip() or str(getattr(cfg.locations, "default_pos", "POS1") or "POS1")

    code_to_name = {str(cfg.locations.central.code): str(cfg.locations.central.name)}
    for loc in (cfg.locations.pos or []):
        code_to_name[str(loc.code)] = str(loc.name)

    options = [
        p for p in service.stock_list(query="", location_code=from_code)
        if float(getattr(p, "quantity", 0) or 0) > 0
    ]
    products = [
        {
            "sku": str(getattr(p, "sku", "") or ""),
            "name": str(getattr(p, "name", "") or ""),
            "uom": str(getattr(p, "unit_of_measure", "") or ""),
            "stock": float(getattr(p, "quantity", 0) or 0),
        }
        for p in options
        if str(getattr(p, "sku", "") or "")
    ]

    return templates.TemplateResponse(
        request=request,
        name="partials/transfer_bulk_modal.html",
        context={
            "from_code": from_code,
            "to_code": to_code,
            "from_name": code_to_name.get(from_code, from_code),
            "to_name": code_to_name.get(to_code, to_code),
            "movement_date": movement_date,
            "note": note,
            "products": products,
            "message": "",
            "message_class": "",
            "message_detail": "",
            "checked_skus": set(),
            "quantities": {},
        },
    )
```

**Edge cases (ya resueltos):**
- Origen sin stock → `products` vacío (el template muestra el mensaje de "no hay stock").

#### Verification

```bash
python3 -c "from app.routers.ui_transfers import transfer_bulk_modal; print('ok')"
# Expected: ok
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — `GET /ui/transfers/bulk` añadido al final
- [ ] Verificación imprime `ok`

---

### T5: Endpoint `POST /ui/transfers/bulk`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_transfers.py`
**Pillars:** P1, P4, P6

#### Context

```python
# app/routers/ui_transfers.py — helpers YA creados en T2: _parse_transfer_lines, _audit_transfer
# app/routers/ui_transfers.py — imports YA presentes: HTTPException, TransferCreate, parse_dt,
#   get_current_user_from_session, HTMLResponse, Session, Depends, Request
```

#### Objective

Añadir el endpoint que ejecuta el traspaso masivo y devuelve (éxito → cierre+refresh, error → modal preservado).

#### Instructions

**Step 1:** Añadir al final de `app/routers/ui_transfers.py` (después de `transfer_bulk_modal`):

```python
@router.post("/transfers/bulk", response_class=HTMLResponse)
async def transfer_bulk_execute(
    request: Request,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)

    form = await request.form()

    from_code = (form.get("from_location_code") or "").strip() or str(cfg.locations.central.code).strip()
    to_code = (form.get("to_location_code") or "").strip() or str(getattr(cfg.locations, "default_pos", "POS1") or "POS1")
    movement_date_raw = (form.get("movement_date") or "").strip()
    note = (form.get("note") or "").strip() or None

    code_to_name = {str(cfg.locations.central.code): str(cfg.locations.central.name)}
    for loc in (cfg.locations.pos or []):
        code_to_name[str(loc.code)] = str(loc.name)

    lines = _parse_transfer_lines(form.getlist("product"), form.getlist("quantity"))
    movement_date = parse_dt(movement_date_raw) if movement_date_raw else None
    user = get_current_user_from_session(db, request)

    def _reload_options() -> list[dict]:
        options = [
            p for p in service.stock_list(query="", location_code=from_code)
            if float(getattr(p, "quantity", 0) or 0) > 0
        ]
        return [
            {
                "sku": str(getattr(p, "sku", "") or ""),
                "name": str(getattr(p, "name", "") or ""),
                "uom": str(getattr(p, "unit_of_measure", "") or ""),
                "stock": float(getattr(p, "quantity", 0) or 0),
            }
            for p in options
            if str(getattr(p, "sku", "") or "")
        ]

    def _modal_response(message: str, message_class: str, message_detail: str,
                        checked_skus: set, quantities_map: dict, keep_open: bool) -> HTMLResponse:
        response = templates.TemplateResponse(
            request=request,
            name="partials/transfer_bulk_modal.html",
            context={
                "from_code": from_code,
                "to_code": to_code,
                "from_name": code_to_name.get(from_code, from_code),
                "to_name": code_to_name.get(to_code, to_code),
                "movement_date": movement_date_raw,
                "note": note or "",
                "products": _reload_options(),
                "message": message,
                "message_class": message_class,
                "message_detail": message_detail,
                "checked_skus": checked_skus,
                "quantities": quantities_map,
            },
        )
        if keep_open:
            response.headers["X-Modal-Keep"] = "1"
        return response

    if not lines:
        return _modal_response("Error en traspaso", "error",
                               "Selecciona al menos un producto para traspasar.", set(), {}, True)

    try:
        result = service.transfer(
            TransferCreate(
                from_location_code=from_code,
                to_location_code=to_code,
                lines=lines,
                movement_date=movement_date,
                note=note,
            )
        )
    except HTTPException as e:
        checked_skus = {line.sku for line in lines}
        quantities_map = {line.sku: line.quantity for line in lines}
        return _modal_response("Error en traspaso", "error",
                               str(getattr(e, "detail", e)), checked_skus, quantities_map, True)

    if user is not None:
        _audit_transfer(db, user, result)

    response = HTMLResponse("")
    response.headers["HX-Trigger"] = "transferBulkSuccess"
    return response
```

**Edge cases (ya resueltos):**
- `lines` vacío → error amable antes de llamar al service.
- `HTTPException` (stock insuficiente, SKU no existe) → modal re-render con `X-Modal-Keep: 1` y selección preservada.
- Éxito → respuesta vacía + `HX-Trigger`; el `htmx:afterRequest` existente cierra el modal (2xx sin keep).

#### Verification

```bash
python3 -c "from app.routers.ui_transfers import transfer_bulk_execute; import inspect; print(inspect.iscoroutinefunction(transfer_bulk_execute))"
# Expected: True
```

#### Expected Report

- [ ] `app/routers/ui_transfers.py` — `POST /ui/transfers/bulk` añadido
- [ ] Verificación imprime `True`

---

### T6: Botón "Traspaso masivo" en `tab_transfers.html`

**Depends on:** T4
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/tab_transfers.html`
**Pillars:** P4

#### Objective

Añadir el botón que abre la ventana flotante, junto a "Agregar línea".

#### Instructions

**Step 1: Read the file**
```
Read app/templates/partials/tab_transfers.html
```

**Step 2: Insertar el botón entre "Agregar línea" y "Crear traspaso"**

```html
<!-- BEFORE:
      <button type="button" class="mb-0" data-transfer-add-line>Agregar línea</button>
      <button id="transfer-submit" type="submit" class="mb-0">Crear traspaso</button>
-->

<!-- AFTER: -->
      <button type="button" class="mb-0" data-transfer-add-line>Agregar línea</button>
      <button type="button" class="mb-0"
              hx-get="/ui/transfers/bulk"
              hx-include="[name='from_location_code'], [name='to_location_code'], [name='movement_date'], [name='note']"
              hx-target="body"
              hx-swap="beforeend">Traspaso masivo</button>
      <button id="transfer-submit" type="submit" class="mb-0">Crear traspaso</button>
```

**Edge cases (ya resueltos):**
- `hx-include` con selectores por `name` evita incluir las líneas (`product`/`quantity`) del formulario.
- El modal se añade a `body` con `beforeend` (mismo patrón que el botón "Edit" del historial).

#### Verification

```bash
python3 -c "from pathlib import Path; t=Path('app/templates/partials/tab_transfers.html').read_text(); print('Traspaso masivo' in t)"
# Expected: True
```

#### Expected Report

- [ ] `app/templates/partials/tab_transfers.html` — botón "Traspaso masivo" añadido
- [ ] Verificación imprime `True`

---

### T7: JS — handlers del modal

**Depends on:** T1, T6
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/static/js/app.js`
**Pillars:** P1, P4

#### Context

```javascript
// app/static/js/app.js — patrones YA existentes:
//   function onTransferAddLine(evt) { const btn = evt.target.closest('[data-transfer-add-line]'); ... }
//   document.addEventListener('click', onTransferAddLine);
//   function closeModal(modalId) { const modal = document.getElementById(modalId); if (modal) { modal.remove(); } }
```

#### Objective

Añadir los handlers del checkbox/select-all/búsqueda y el listener de refresco post-éxito.

#### Instructions

**Step 1: Read the file**
```
Read app/static/js/app.js
```

**Step 2: Insertar las 3 funciones justo DESPUÉS de `onTransferToggleEdit` (tras su `}` de cierre, antes de `function closeModal`)**

```javascript
  function onTransferBulkCheckboxChange(evt) {
    const cb = evt.target.closest('.transfer-bulk-check');
    if (!cb) return;
    const row = cb.closest('tr');
    if (!row) return;
    const qty = row.querySelector('input[name="quantity"]');
    const prod = row.querySelector('input[name="product"]');
    if (qty) qty.disabled = !cb.checked;
    if (prod) prod.disabled = !cb.checked;
  }

  function onTransferBulkSelectAll(evt) {
    const master = evt.target.closest('#transfer-bulk-select-all');
    if (!master) return;
    const modal = master.closest('.modal-overlay');
    if (!modal) return;
    modal.querySelectorAll('.transfer-bulk-check').forEach((cb) => {
      cb.checked = master.checked;
      const row = cb.closest('tr');
      if (!row) return;
      const qty = row.querySelector('input[name="quantity"]');
      const prod = row.querySelector('input[name="product"]');
      if (qty) qty.disabled = !cb.checked;
      if (prod) prod.disabled = !cb.checked;
    });
  }

  function onTransferBulkSearch(evt) {
    const input = evt.target.closest('#transfer-bulk-search');
    if (!input) return;
    const q = (input.value || '').toLowerCase();
    const modal = input.closest('.modal-overlay');
    if (!modal) return;
    modal.querySelectorAll('.transfer-bulk-row').forEach((row) => {
      const hay = (row.getAttribute('data-search') || '').indexOf(q) !== -1;
      row.style.display = hay ? '' : 'none';
    });
  }
```

**Step 3: Registrar los handlers (añadir junto a `document.addEventListener('change', onTransferProductChange);`)**

```javascript
// BEFORE:
  document.addEventListener('change', onTransferProductChange);

// AFTER:
  document.addEventListener('change', onTransferProductChange);
  document.addEventListener('change', onTransferBulkCheckboxChange);
  document.addEventListener('change', onTransferBulkSelectAll);
  document.addEventListener('input', onTransferBulkSearch);
  document.addEventListener('transferBulkSuccess', () => {
    try {
      window.htmx.ajax('GET', '/ui/tabs/transfers', { target: '#tab-content', swap: 'innerHTML' });
    } catch (e) {}
  });
```

**Edge cases (ya resueltos):**
- Un checkbox deshabilitado NO se envía en el form → `_parse_transfer_lines` solo recibe los marcados.
- `transferBulkSuccess` es el evento que dispara el header `HX-Trigger` del POST; refresca `#tab-content`.

#### Verification

```bash
python3 -c "from pathlib import Path; j=Path('app/static/js/app.js').read_text(); print('onTransferBulkCheckboxChange' in j and 'transferBulkSuccess' in j)"
# Expected: True
```

#### Expected Report

- [ ] `app/static/js/app.js` — 3 funciones + 3 registros + listener añadidos
- [ ] Verificación imprime `True`

---

### T8: Tests

**Depends on:** T2
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_transfers_bulk.py`
**Pillars:** P1

#### Context

```python
# tests/conftest.py — fixtures YA existentes: db_session, business_id
# app/services/inventory_service.py — YA EXISTE:
#   service.purchase(PurchaseCreate(sku=..., quantity=..., unit_cost=...))  # stock a CENTRAL por defecto
#   service.transfer(TransferCreate(from_location_code, to_location_code, lines=[...]))
#   service.stock_for_location(sku, location_code) -> float
# app/schemas.py — YA EXISTE: PurchaseCreate, TransferCreate, TransferLineCreate
# app/models.py — YA EXISTE: Location, Product, InventoryMovement
```

#### Objective

Testear `_parse_transfer_lines` (puro) y el traspaso multi-línea del service.

#### Instructions

**Step 1:** Crear `tests/unit/test_ui_transfers_bulk.py`:

```python
from __future__ import annotations

from sqlalchemy import select

from app.models import InventoryMovement, Location, Product
from app.routers.ui_transfers import _parse_transfer_lines
from app.schemas import PurchaseCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService


def test_parse_transfer_lines_basic():
    lines = _parse_transfer_lines(["SKU1", "SKU2"], ["2", "3.5"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU1", 2.0), ("SKU2", 3.5)]


def test_parse_transfer_lines_strips_sku_label():
    lines = _parse_transfer_lines(["SKU1 - Nombre producto"], ["4"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU1", 4.0)]


def test_parse_transfer_lines_skips_empty_and_nonpositive():
    lines = _parse_transfer_lines(["", "SKU2", "SKU3"], ["1", "0", "-2"])
    assert lines == []


def test_parse_transfer_lines_skips_invalid_quantity():
    lines = _parse_transfer_lines(["SKU1", "SKU2"], ["abc", "5"])
    assert [(l.sku, l.quantity) for l in lines] == [("SKU2", 5.0)]


def test_transfer_multiple_lines_in_one_operation(db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Central")
    pos = Location(business_id=business_id, code="POS1", name="Pos")
    db_session.add_all([central, pos])
    db_session.commit()

    for sku in ("SKU-A", "SKU-B"):
        db_session.add(Product(
            business_id=business_id, sku=sku, name=sku, min_stock=0,
            unit_of_measure="ud", default_purchase_cost=1.0,
            default_sale_price=2.0, lead_time_days=0,
        ))
    db_session.commit()

    service = InventoryService(db_session, business_id=business_id)
    service.purchase(PurchaseCreate(sku="SKU-A", quantity=10, unit_cost=1.0))
    service.purchase(PurchaseCreate(sku="SKU-B", quantity=5, unit_cost=1.0))

    result = service.transfer(TransferCreate(
        from_location_code="CENTRAL",
        to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-A", quantity=3), TransferLineCreate(sku="SKU-B", quantity=2)],
    ))

    assert len(result.lines) == 2
    outs = list(db_session.scalars(select(InventoryMovement).where(InventoryMovement.type == "transfer_out")))
    assert len(outs) == 2
    assert service.stock_for_location("SKU-A", "POS1") == 3
    assert service.stock_for_location("SKU-B", "POS1") == 2
```

**Edge cases (ya resueltos):**
- `purchase` deja el stock en CENTRAL por defecto (igual que `test_sale_supports_central_location`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_transfers_bulk.py -v
# Expected: 5 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_transfers_bulk.py` creado
- [ ] 5 tests PASS
