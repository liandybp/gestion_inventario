# Design — Método de pago (transferencia) en venta rápida

## 2.1 Codebase Grounding

Archivos identificados leyendo el repo real (no se asume ninguna firma):

| Archivo | Rol | Qué contiene hoy |
|---------|-----|-------------------|
| `app/models.py` | MODIFY | `InventoryMovement` (línea 221) — sin campo de método de pago |
| `app/migrations.py` | MODIFY | `_run_sqlite_schema_migrations()` (línea 131) y `_run_postgresql_schema_migrations()` (línea 301) — patrón ya establecido de `ADD COLUMN` idempotente por columna, ver bloque `location_id` de `inventory_movements` (líneas 248-257 sqlite, 420-434 postgres) |
| `app/schemas.py` | MODIFY | `SaleCreate` (línea 83), `MovementRead` (línea 107) |
| `app/repositories/inventory_repository.py` | MODIFY | `recent_sales()` (línea 225) — SELECT con tupla fija consumida por `sales_history.html` |
| `app/services/inventory_service.py` | MODIFY | `sale()` (línea 1715), `update_sale()` (línea 1359), `recent_sales()` (línea 1983) — pass-through al repo |
| `app/routers/ui_sales.py` | MODIFY | `sale()` (línea 161), `sale_barcode()` (línea 241), `sale_update()` (línea 366), `tab_sales()` (línea 627) |
| `app/routers/ui_profit_items.py` | REFERENCE | `profit_items_sale_update()` (línea 321) — **otro caller de `update_sale()`**, fuera de scope de UI pero comparte el método de servicio (ver D3) |
| `app/templates/partials/sale_panel.html` | MODIFY | Form de creación (líneas 7-31) + form de filtro del historial (líneas 51-82) |
| `app/templates/partials/sale_edit_form.html` | MODIFY | Form de edición — usa el objeto ORM `movement` directamente (no `MovementRead`) |
| `app/templates/partials/sales_history.html` | MODIFY | Tabla que itera `sales` con tupla fija (línea 33) |
| `tests/unit/test_inventory_service_unit.py` | MODIFY | Tests de `InventoryService.sale/update_sale` — patrón `_create_product`/`_create_locations` ya definido |
| `tests/unit/test_schemas.py` | MODIFY | Tests de `SaleCreate` |
| `tests/e2e/test_flows_e2e.py` | REFERENCE | `test_sale_flow_reduces_stock` — patrón de e2e existente para el panel de venta (selectores `#sale-panel form[hx-post="/ui/sale"]`) |
| `tests/e2e/` (nuevo archivo) | CREATE | `test_sale_payment_method_e2e.py` |
| `VERSION`, `.bumpversion.cfg` | MODIFY | Bump PATCH `0.4.5` → `0.4.6` |

## 2.2 Architecture Approach

Cambio aditivo, capa por capa, siguiendo el flujo de datos existente sin introducir
componentes nuevos:

```
Form checkbox (sale_panel.html / sale_edit_form.html)
  → Form field bool (ui_sales.py: sale / sale_barcode / sale_update / tab_sales)
    → SaleCreate.paid_by_transfer (schemas.py)
      → InventoryService.sale() / update_sale() / recent_sales() (inventory_service.py)
        → InventoryMovement.paid_by_transfer (models.py, columna nueva)
          → InventoryRepository.recent_sales() SELECT + filtro (inventory_repository.py)
            → sales_history.html (columna nueva en la tabla)
```

No se crea ningún componente nuevo (repo, service, endpoint). Se extiende el flujo
"venta" existente en cada capa que ya toca `InventoryMovement`. **Invariante preservado:**
`SalesDocument` no se toca en ningún archivo — confirmado en la tabla 2.1 (ningún archivo
de `sales_document_*` aparece en el inventario).

## 2.3 Data Model

### Entity: InventoryMovement (`inventory_movements`)

| Field | Type | Change | Default | Notes |
|-------|------|--------|---------|-------|
| `paid_by_transfer` | Boolean | NEW | `false` | Solo relevante cuando `type == "sale"`; en otros tipos de movimiento (purchase/transfer/adjustment) queda `false` sin uso, igual que `unit_price` ya queda `NULL`/sin uso en compras |

**Modelo (`app/models.py`, mismo patrón que `is_active`/`must_change_password` en `User`):**
```python
paid_by_transfer: Mapped[bool] = mapped_column(
    Boolean, nullable=False, default=False, server_default="false"
)
```

**Migración SQLite (idempotente, mismo patrón que el bloque `location_id` de `inventory_movements`):**
```sql
ALTER TABLE inventory_movements ADD COLUMN paid_by_transfer BOOLEAN DEFAULT 0
```
(SQLite aplica el `DEFAULT` a las filas existentes al momento del `ALTER TABLE ADD COLUMN`
— no quedan en `NULL`. Mismo comportamiento ya usado para `must_change_password`.)

**Migración PostgreSQL (idempotente, mismo patrón que el bloque `location_id`):**
```sql
ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS paid_by_transfer BOOLEAN NOT NULL DEFAULT FALSE
```

**Rollback:** `ALTER TABLE inventory_movements DROP COLUMN paid_by_transfer;` — sin pérdida
de datos relevante (columna aditiva, ningún otro dato depende de ella).

**Index needed:** No. Columna booleana de baja cardinalidad; el filtro (REQ-6) opera sobre
el mismo `SELECT` que ya filtra por `business_id`/`type`/fecha usando el índice compuesto
existente `ix_inventory_movements_business_type_date` — no hay volumen que justifique un
índice adicional en este momento (negocios de este proyecto son de escala pequeña/mediana).

## 2.4 API Contracts

Todos son endpoints HTMX existentes — se extiende su payload, no se crean rutas nuevas.

### POST /ui/sale (existente, extendido)
**Auth:** Igual que hoy (usuario autenticado; sin restricción de rol adicional)
**Request (nuevo campo, form-encoded):** `paid_by_transfer: bool = Form(False)` — ausente
en el POST (checkbox desmarcado) ⇒ `False`; presente como `"1"` (checkbox marcado) ⇒ `True`
**Response 200:** Igual que hoy (`partials/sale_panel.html`), sin cambio de forma
**Idempotencia:** Sin cambio — cada POST crea un nuevo movimiento, igual que hoy

### POST /ui/sale/barcode (existente, extendido)
Igual contrato que `/ui/sale`, mismo campo nuevo `paid_by_transfer`.

### POST /ui/movement/sale/{movement_id}/update (existente, extendido)
**Auth:** Igual que hoy — `ensure_admin_or_owner` (sin cambio)
**Request (nuevo campo):** `paid_by_transfer: bool = Form(False)`
**Response 200:** Igual que hoy

### GET /ui/movement/sale/{movement_id}/edit (existente, sin cambio de firma)
**Response:** El HTML ahora incluye el checkbox precargado con `checked` si
`movement.paid_by_transfer` es `True` — el objeto `movement` pasado al template ya es el
ORM `InventoryMovement`, así que no requiere cambios en el router, solo en el template.

### GET /ui/tabs/sales (existente, extendido)
**Auth:** Igual que hoy
**Request (nuevo query param, opcional):** `payment_method: Optional[str] = None` —
valores válidos `"transfer"` | `"cash"` | ausente/otro (sin filtro)
**Response 200:** Igual que hoy, con la tabla de historial mostrando la columna nueva

## 2.5 Security Review

| Check | Answer |
|-------|--------|
| **New auth boundary?** | No — se reutilizan los guards existentes de cada endpoint (`ensure_admin_or_owner` en update/delete, sin restricción adicional en creación, igual que hoy) |
| **New sensitive data?** | No — `paid_by_transfer` es un booleano operativo, no PII ni dato financiero sensible en sí mismo |
| **Input validation?** | FastAPI convierte el valor de form a `bool` de forma nativa (`Form(False)`, mismo patrón que `initial_inventory` en `ui_products.py:303` e `is_active` en `ui_users.py:84`); no requiere validador custom |
| **New attack surface?** | Ninguna — mismo endpoint, un campo más en el payload existente |
| **Secrets needed?** | Ninguno |
| **OWASP risks?** | Ninguno nuevo. El filtro `payment_method` en `GET /ui/tabs/sales` se compara contra un `set` cerrado de valores (`"transfer"`/`"cash"`) antes de construir el `WHERE`, sin interpolación de string en SQL (se usa `InventoryMovement.paid_by_transfer.is_(True/False)` vía SQLAlchemy, no SQL crudo) |

No se invoca `security-architect` — no hay cambio de límite de autenticación/autorización
ni dato PII nuevo.

## 2.6 Trade-offs

| Decisión | Elegido | Alternativa | Por qué no la alternativa |
|----------|---------|-------------|----------------------------|
| D1: Nombre y tipo de columna | `paid_by_transfer: Boolean` en `InventoryMovement` | Enum de método de pago (`cash`/`transfer`/`card`/...) | El usuario pidió explícitamente un check binario ("si se pagó por transferencia o no"); un enum es sobre-ingeniería para el requisito actual — se puede migrar a enum más adelante sin romper este booleano (`payment_method = 'transfer' if paid_by_transfer else 'cash'`) |
| D2: Alcance del modelo | Solo `InventoryMovement` (venta rápida) | También `SalesDocument`/`SalesDocumentItem` | Confirmado explícitamente con el usuario vía pregunta directa: NO aplica a documentos de venta en esta iteración |
| D3: Firma de `InventoryService.update_sale()` | `paid_by_transfer: Optional[bool] = None` (si `None`, no se toca el valor actual) | Parámetro `bool` obligatorio con default `False` | `update_sale()` tiene un segundo caller, `ui_profit_items.py::profit_items_sale_update` (línea 340), que **no** pasa este campo. Si el default fuera `False` fijo, cada edición desde el tab "Ganancia por artículo" resetearía silenciosamente `paid_by_transfer` a `false` — regresión real (REQ-8). `Optional[bool] = None` con "no tocar si es None" preserva el valor existente en ese caller sin necesidad de modificarlo |
| D4: Filtro por método de pago | Nuevo query param `payment_method` en `GET /ui/tabs/sales`, mismo endpoint | Endpoint nuevo `/ui/tabs/sales/by-payment` | El tab de ventas ya filtra por `location_code`/`query`/fechas en el mismo endpoint (patrón establecido) — un query param más es consistente, un endpoint nuevo duplicaría lógica de contexto (`_sales_doc_context`, `pos_locations`, etc.) |
| D5: Posición de la columna en la tupla de `recent_sales()` | Se agrega al final (después de `lot_codes`) | Insertarla junto a `unit_price` | `sales_history.html` es el único consumidor de la tupla (verificado con grep en todo `app/templates`); agregar al final evita reordenar posiciones ya usadas por índice en el `for` del template, reduciendo el diff y el riesgo de desalinear columnas |

## 2.7 Migration & Rollback Strategy

```
Migration steps:
1. Modelo ORM (models.py) declara la columna con server_default — cubre instalaciones
   nuevas (Base.metadata.create_all).
2. Migración de arranque (migrations.py) agrega la columna vía ALTER TABLE idempotente
   (mismo patrón que el resto de columnas de este archivo) — cubre instalaciones
   existentes (dev y prod). Corre automáticamente en el próximo arranque de la app,
   sin acción manual.
3. Filas existentes quedan con paid_by_transfer=false (default aplicado por la propia
   sentencia ALTER TABLE, sin backfill manual necesario).
4. Deploy de código que lee/escribe el campo (capas repo → service → router → template).

Rollback:
1. Revertir el deploy de código (los endpoints vuelven a ignorar el campo).
2. La columna puede quedar en la BD sin problema (aditiva, nadie más la usa) — no es
   necesario un DROP COLUMN de emergencia. Si se decide limpiar: 
   ALTER TABLE inventory_movements DROP COLUMN paid_by_transfer;
```

Zero downtime: columna nullable-con-default agregada antes de que el código nuevo la
necesite; el código viejo (durante un despliegue en curso) sigue funcionando porque nunca
referencia la columna nueva.

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría (obligatorio)

| Dimensión | Respuesta para este diseño |
|-----------|------------------------------|
| **1. SoC/SRP** | No se crea ninguna capa nueva (repo/service) — se extiende la ya existente en las 4 capas (modelo → repo → service → router → template). Cada cambio tiene un consumidor real en el mismo PAE (T1 lo consume T2/T3/T4/T5; nada queda "por si acaso") |
| **2. Cohesión/Acoplamiento** | `tab_sales()` en `ui_sales.py` ya llama a `inventory_service.recent_sales()` una sola vez por request (verificado leyendo el archivo) — el nuevo parámetro `payment_method` se pasa en esa misma llamada, sin duplicar el query |
| **3. Capas/Límites** | Dirección de dependencias sin cambio: template → router → service → repo → modelo, igual que el resto del archivo. El template de edición (`sale_edit_form.html`) sigue leyendo el objeto ORM `movement` directamente, como ya hacía antes de esta feature (no se introduce un nuevo patrón mixto) |
| **5. Robustez/Resiliencia** | No se agrega ningún método público nuevo que toque BD/red — se extienden métodos existentes (`sale`, `update_sale`, `recent_sales`) que ya tienen su propio manejo de errores (`HTTPException`, `try/except` en `update_sale`). El campo nuevo no introduce una ruta de fallo adicional: es un booleano con default, sin validación que pueda lanzar excepción nueva |
| **6. Consistencia Transaccional** | No aplica — no hay fórmula, ratio ni comparación entre ventanas temporales en esta feature. El filtro `payment_method` es una condición `WHERE` adicional sobre el mismo query, misma ventana de fechas que ya usa `start_date`/`end_date`/`month`/`year` |

**Riesgo real detectado y mitigado:** D3 (arriba) — sin el `Optional[bool] = None`, el
segundo caller de `update_sale()` (`ui_profit_items.py`) resetearía `paid_by_transfer` a
`false` en cada edición desde ese tab. Mitigación convertida en instrucción literal del
envelope T5 (Fase 3): *"`update_sale` solo escribe `mv.paid_by_transfer` cuando el
parámetro no es `None`; si es `None`, deja el valor actual sin tocar."*

## 2.10 Optional Delegation

Ninguna delegación necesaria — cambio de complejidad moderada, cubierto completamente por
el SDD Lead (< 15 archivos, sin decisión arquitectónica multi-servicio, sin migración
compleja, sin PII).
