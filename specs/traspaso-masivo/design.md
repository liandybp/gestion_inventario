# Phase 2 — Design: Traspaso masivo

## 1. Modelo de datos

Sin cambios. Se reutilizan `InventoryMovement`, `MovementAllocation`, `InventoryLot`,
`Product`, `Location` tal cual. No hay migración.

## 2. API (endpoints nuevos)

| Método | Ruta | Descripción | Respuesta |
|--------|------|-------------|-----------|
| GET | `/ui/transfers/bulk` | Renderiza la ventana flotante con el checklist | `partials/transfer_bulk_modal.html` |
| POST | `/ui/transfers/bulk` | Ejecuta el traspaso masivo | vacío + `HX-Trigger: transferBulkSuccess` (éxito) o modal re-renderizado + `X-Modal-Keep: 1` (error) |

Ambos con `ensure_admin_or_owner` + `require_active_business_id`, igual que el resto de
`ui_transfers.py`.

### Params GET
`from_location_code`, `to_location_code`, `movement_date`, `note` (query strings, opcionales;
vienen de los selectores del formulario principal vía `hx-include`).

### Campos POST (form)
`from_location_code`, `to_location_code`, `movement_date`, `note`, y listas emparejadas
`product[]` + `quantity[]` (solo las filas marcadas, que son las que quedan habilitadas).

## 3. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Backend: reutilizar `service.transfer(TransferCreate(lines=[...]))` | endpoint batch nuevo en el service | ya hace multi-línea + FIFO + 1 transacción | cero cambios al service |
| D2 | Modal hereda origen/destino/fecha del formulario (hidden inputs) | selectores propios en el modal | evita duplicar selectores; el modal es "alternativa" al formulario | si cambia el origen hay que reabrir el modal |
| D3 | Cantidad default = stock completo al marcar | vacío / 1 unidad | elegido por el usuario; "mover todo" es el caso común | riesgo de error si no se revisa (mitigado: ajustable) |
| D4 | Éxito: respuesta vacía + `HX-Trigger`; el `htmx:afterRequest` existente ya cierra el modal (2xx sin `X-Modal-Keep`) | re-renderizar modal de éxito | reutiliza la infra actual de cierre de modales | la pestaña se refresca vía listener JS |
| D5 | Error: re-renderizar modal + `X-Modal-Keep: 1` | volcar al formulario principal | mantiene el contexto masivo y las selecciones | el parseo de líneas se reutiliza |
| D6 | Filtro client-side (`data-search`) | endpoint de búsqueda server-side | ~679 productos caben en un render; sin round-trips | si crece el catálogo, migrar a server-side |
| D7 | Extraer `_parse_transfer_lines` y `_audit_transfer` como helpers | duplicar lógica en el POST bulk | DRY (pilar 4); `create_transfer` los reusa | tocar `create_transfer` (cambio mecánico) |

## 4. Arquitectura objetivo

```
[tab_transfers.html]
   │  botón "Traspaso masivo" (hx-get /ui/transfers/bulk + hx-include campos)
   ▼
[ui_transfers.py ★] GET /ui/transfers/bulk
   │  service.stock_list(query="", location_code=from_code)  ← ·existente
   ▼
[transfer_bulk_modal.html ★]  (checklist + hidden origin/dest + search + form)
   │  POST /ui/transfers/bulk
   ▼
[ui_transfers.py ★] POST /ui/transfers/bulk
   │  _parse_transfer_lines() → service.transfer(TransferCreate) → _audit_transfer()
   ├─ éxito ──▶ HTMLResponse("") + HX-Trigger: transferBulkSuccess
   │               └─▶ [app.js ★] cierra modal + htmx.ajax('GET','/ui/tabs/transfers')
   └─ error ──▶ modal re-render + X-Modal-Keep: 1 (selección preservada)

★ NUEVO/MODIFICADO    · existente (solo lectura)
```

## 5. Inventario de archivos (scope duro)

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/templates/partials/transfer_bulk_modal.html` | CREATE | modal con checklist, search, select-all, form |
| `app/routers/ui_transfers.py` | MODIFY | helpers `_parse_transfer_lines`/`_audit_transfer` + endpoints GET/POST bulk + refactor `create_transfer` |
| `app/templates/partials/tab_transfers.html` | MODIFY | botón "Traspaso masivo" |
| `app/static/js/app.js` | MODIFY | handlers checkbox/select-all/search + listener `transferBulkSuccess` |
| `tests/unit/test_ui_transfers_bulk.py` | CREATE | tests de `_parse_transfer_lines` + traspaso multi-línea |

**REFERENCE (no tocar):** `app/services/inventory_service.py`, `app/models.py`,
`app/repositories/inventory_repository.py`, `app/security.py`, `app/deps.py`,
`app/schemas.py`, `app/templates/components/ui.html`, `tests/conftest.py`.

## 6. Firmas reales que usa el código nuevo

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
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
def dt_to_local_input(dt) -> str
def parse_dt(raw) -> datetime | None
templates  # Jinja2Templates

# app/security.py — YA EXISTE
def require_active_business_id(db, request) -> int
def get_current_user_from_session(db, request) -> User | None
```

## 7. Seguridad

- Endpoints protegidos con `ensure_admin_or_owner` (mismo patrón que el resto del router).
- `business_id` lo inyecta `require_active_business_id` (el cliente nunca lo manda).
- Los valores del formulario se sanitizan igual que `create_transfer` (SKU resuelto contra
  `Product` de ese negocio; cantidad validada > 0; stock validado por `service.transfer`).
- Sin XSS nuevo: el template usa variables escapadas por Jinja (default) y reutiliza `flash`.
