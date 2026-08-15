# PAE-traspaso-masivo: Traspaso masivo (checklist en ventana flotante)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer sección 5 (Arquitectura objetivo) completa antes de tocar archivos.
     3. Una fase a la vez (T1→T8 en orden). Sin fase N verificada, no empieza N+1.
     4. Si algo no está especificado aquí o en tasks.md: no se inventa, se reporta.
     5. Archivos fuera del inventario (5.2): no se tocan.
     6. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     7. Envelopes completos (código exacto) están en specs/traspaso-masivo/tasks.md
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/traspaso-masivo` |
| **Fecha** | 2026-08-15 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `copiloto-dashboard-ia` |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/traspaso-masivo/requirements.md` | REQ-1..9 con Gherkin + MoSCoW + NFRs |
| Phase 2 | `specs/traspaso-masivo/design.md` | decisiones D1-D7, API, seguridad, firmas reales |
| Phase 3 | `specs/traspaso-masivo/tasks.md` | **envelopes T1..T8 con código exacto** |

## 3. Objetivo

Añadir un botón **"Traspaso masivo"** en la pestaña Traspasos que abre una **ventana flotante**
con el listado de productos con stock en el origen. El usuario marca varios productos (checkbox),
ajusta cantidades (default = stock completo), y ejecuta el traspaso en un solo paso. El formulario
línea-a-línea actual **se mantiene intacto**.

**Hallazgo que reduce alcance:** el backend `InventoryService.transfer()` YA soporta multi-línea en
una sola transacción con FIFO (`TransferCreate.lines`). Este PAE **no toca el service** — es casi
todo UI + 2 endpoints + refactor de 2 helpers.

**Qué NO se toca:** `app/services/inventory_service.py`, `app/models.py`,
`app/repositories/inventory_repository.py`, `app/security.py`, `app/deps.py`, `app/db.py`,
`app/auth.py`, `app/schemas.py`, `app/templates/components/ui.html`, `tests/conftest.py`.

## 4. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Reutilizar `service.transfer(TransferCreate(lines=[...]))` | ya hace multi-línea + FIFO + 1 transacción; cero cambios al service |
| D2 | Modal hereda origen/destino/fecha del formulario (hidden inputs) | evita duplicar selectores |
| D3 | Cantidad default = stock completo al marcar | elegido por el usuario |
| D4 | Éxito: respuesta vacía + `HX-Trigger: transferBulkSuccess` | el `htmx:afterRequest` existente cierra el modal (2xx sin `X-Modal-Keep`) |
| D5 | Error: re-render modal + `X-Modal-Keep: 1` | mantiene el contexto masivo y las selecciones |
| D6 | Filtro client-side (`data-search`) | ~679 productos caben en un render; sin round-trips |
| D7 | Helpers `_parse_transfer_lines` / `_audit_transfer` compartidos | DRY; `create_transfer` los reusa |

## 5. Arquitectura objetivo

### 5.1 Diagrama de componentes

```
[tab_transfers.html]  botón "Traspaso masivo" (hx-get /ui/transfers/bulk + hx-include campos)
        │
        ▼
[ui_transfers.py ★] GET /ui/transfers/bulk
        │   service.stock_list(query="", location_code=from_code)  ← ·existente
        ▼
[transfer_bulk_modal.html ★]  checklist + hidden origin/dest + search + form
        │  POST /ui/transfers/bulk
        ▼
[ui_transfers.py ★] POST /ui/transfers/bulk
        │   _parse_transfer_lines() → service.transfer(TransferCreate) → _audit_transfer()
        ├─ éxito ──▶ HTMLResponse("") + HX-Trigger: transferBulkSuccess
        │               └─▶ [app.js ★] cierra modal + htmx.ajax('GET','/ui/tabs/transfers')
        └─ error ──▶ modal re-render + X-Modal-Keep: 1 (selección preservada)

★ NUEVO/MODIFICADO    · existente (solo lectura)
```

### 5.2 Inventario de archivos (scope duro)

| Archivo | Operación |
|---------|-----------|
| `app/templates/partials/transfer_bulk_modal.html` | CREATE |
| `app/routers/ui_transfers.py` | MODIFY (2 helpers + refactor `create_transfer` + GET/POST bulk) |
| `app/templates/partials/tab_transfers.html` | MODIFY (botón) |
| `app/static/js/app.js` | MODIFY (handlers + listener) |
| `tests/unit/test_ui_transfers_bulk.py` | CREATE |

**REFERENCE (no tocar):** `app/services/inventory_service.py`, `app/models.py`,
`app/repositories/inventory_repository.py`, `app/security.py`, `app/deps.py`,
`app/schemas.py`, `app/templates/components/ui.html`, `tests/conftest.py`.

### 5.3 Firmas reales que usa el código nuevo

```python
# app/services/inventory_service.py — ESTO YA EXISTE, NO MODIFICAR
class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None): ...
    def stock_list(self, query: str = "", location_code: Optional[str] = None) -> list[StockRead]: ...
    def transfer(self, payload: TransferCreate) -> TransferResult: ...

# app/schemas.py — YA EXISTE
class StockRead(BaseModel):
    sku: str; name: Optional[str]; unit_of_measure: Optional[str]
    quantity: float; min_stock: float; needs_restock: bool
class TransferLineCreate(BaseModel): sku: str; quantity: float
class TransferCreate(BaseModel):
    from_location_code: Optional[str]; to_location_code: str
    lines: list[TransferLineCreate]; movement_date: Optional[datetime]; note: Optional[str]
class TransferResult(BaseModel):
    from_location_code: Optional[str]; to_location_code: str
    lines: list[TransferLineResult]; transfer_ref: Optional[str]

# app/audit.py — YA EXISTE
def log_event(db, user, action, entity_type, entity_id=None, detail=None) -> None:  # hace commit

# app/routers/ui_common.py — YA EXISTE
def ensure_admin_or_owner(db, request) -> None
def parse_dt(raw) -> datetime | None
templates  # Jinja2Templates
```

## 6. Fases de implementación (Task Envelopes)

> **Código exacto de cada envelope en `tasks.md`.** Orden de ejecución estricto.

| Envelope | Título | Depends on | Archivos |
|----------|--------|-----------|----------|
| T1 | Template `transfer_bulk_modal.html` | none | `transfer_bulk_modal.html` |
| T2 | Helpers `_parse_transfer_lines` + `_audit_transfer` | none | `ui_transfers.py` |
| T3 | Refactor `create_transfer` (usar helpers) | T2 | `ui_transfers.py` |
| T4 | Endpoint `GET /ui/transfers/bulk` | T1, T2 | `ui_transfers.py` |
| T5 | Endpoint `POST /ui/transfers/bulk` | T4 | `ui_transfers.py` |
| T6 | Botón "Traspaso masivo" | T4 | `tab_transfers.html` |
| T7 | JS handlers + listener | T1, T6 | `app.js` |
| T8 | Tests | T2 | `test_ui_transfers_bulk.py` |

**Bathes sugeridos (compactar cada 4 envelopes):** B1=T1..T4 · B2=T5..T8.

## 7. Plan de tests y verificación

### 7.1 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
# Expected: todos los tests existentes PASS (antes de tocar nada)
```

### 7.2 Verificación por envelope

Cada envelope incluye su comando en `tasks.md`. Tests nuevos (`tests/unit/test_ui_transfers_bulk.py`):
- 4 tests de `_parse_transfer_lines` (puro)
- 1 test de traspaso multi-línea del service

### 7.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit/` completo — sin cambios, todos PASS tras cada batch (en particular
      `test_inventory_service_unit.py`, que ya cubre `service.transfer`).

## 8. Riesgos y mitigaciones

| # | Riesgo | Prob | Impacto | Mitigación |
|---|--------|------|---------|------------|
| R1 | Refactor de `create_transfer` rompe el flujo actual | Baja | Alto | T3 es reemplazo mecánico (helpers idénticos); verificado por suite completa + smoke manual |
| R2 | Checkbox deshabilitado no envía → desalineación product/quantity | Baja | Medio | cada fila tiene 1 product + 1 quantity en orden; `zip` los empareja; testeado en T8 |
| R3 | Modal no se cierra en éxito | Baja | Bajo | doble vía: `htmx:afterRequest` (2xx sin keep) + `HX-Trigger` → listener JS |
| R4 | `stock_list` pesado para catálogos grandes | Baja | Bajo | ya se usa en el formulario actual; filtro client-side evita re-render |

## 9. Definición de hecho (DoD)

- [ ] **Pre-check:** `pytest -v` — 100% tests pasaban antes (guardar output)
- [ ] **Tests:** `pytest -v` — 100% tests pasan después (incluye 5 tests nuevos)
- [ ] **App arranca:** `python -m uvicorn app.main:app` sin errores
- [ ] **Invariantes:** no se viola ninguna de `CLAUDE.md` (Base única, sin deps nuevas, auth centralizado)
- [ ] **Scope:** `git diff --name-only develop` muestra exactamente los archivos de 5.2, nada más
- [ ] **Limpieza:** no hay archivos temporales (`_tmp`, `_draft`, `_vN`)

> **Generado por:** SDD Lead (Phase 4 — PAE Assembly) · 2026-08-15
