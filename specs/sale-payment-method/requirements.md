# Requirements — Método de pago (transferencia) en venta rápida

## Contexto

El usuario necesita identificar, en cada venta registrada desde el panel de venta rápida
(`POST /ui/sale`, `POST /ui/sale/barcode`, `InventoryMovement.type == "sale"`), si se pagó
por transferencia bancaria o no. El dato se guardará para uso futuro (reportes, conciliación
de caja) — esta iteración solo cubre captura, edición, visualización y filtro básico.

**Confirmado con el usuario (no ambiguo):**
- Alcance: solo el panel de venta rápida (`InventoryMovement`). **NO** aplica a Documentos
  de venta (`SalesDocument` — facturas/presupuestos).
- Checkbox desmarcado por defecto.
- Debe mostrarse en el historial de ventas (tab Ventas).
- Debe ser filtrable en el tab de Ventas.
- Debe poder editarse después de creada la venta.

---

## 1. Functional Requirements

### REQ-1: Registrar venta sin marcar transferencia (comportamiento por defecto)
**Priority:** Must Have
**Scenario:** Usuario registra una venta sin tocar el checkbox
  GIVEN un usuario autenticado en el panel de venta
  AND hay stock suficiente del producto en la ubicación seleccionada
  WHEN el usuario envía `POST /ui/sale` sin incluir `paid_by_transfer` en el form
  THEN la venta se registra correctamente
  AND el movimiento creado tiene `paid_by_transfer = false`

### REQ-2: Registrar venta marcando transferencia
**Priority:** Must Have
**Scenario:** Usuario marca el checkbox al registrar la venta
  GIVEN un usuario autenticado en el panel de venta
  WHEN el usuario envía `POST /ui/sale` con `paid_by_transfer=1`
  THEN el movimiento creado tiene `paid_by_transfer = true`

**Scenario:** Igual comportamiento vía escaneo de código de barras
  GIVEN un usuario autenticado en el panel de venta
  WHEN el usuario envía `POST /ui/sale/barcode` con `paid_by_transfer=1`
  THEN el movimiento creado tiene `paid_by_transfer = true`

### REQ-3: Checkbox visible y desmarcado por defecto en el formulario
**Priority:** Must Have
**Scenario:** Formulario de registrar venta se renderiza
  GIVEN el usuario abre el panel de venta (`GET /ui/tabs/sales` o carga inicial)
  THEN el formulario "Registrar venta" muestra un checkbox "Pagado por transferencia"
  AND el checkbox no tiene el atributo `checked`

### REQ-4: Editar el método de pago de una venta existente
**Priority:** Must Have
**Scenario:** Admin/owner corrige el método de pago de una venta ya registrada
  GIVEN una venta existente con `paid_by_transfer = false`
  AND un usuario con rol admin u owner autenticado
  WHEN el usuario abre "Editar venta" y marca el checkbox, luego envía
       `POST /ui/movement/sale/{id}/update` con `paid_by_transfer=1`
  THEN el movimiento queda con `paid_by_transfer = true`

**Scenario:** El formulario de edición precarga el estado actual
  GIVEN una venta existente con `paid_by_transfer = true`
  WHEN el usuario abre `GET /ui/movement/sale/{id}/edit`
  THEN el checkbox del formulario aparece marcado (`checked`)

### REQ-5: Visibilidad en el historial de ventas
**Priority:** Must Have
**Scenario:** Historial muestra el método de pago por fila
  GIVEN existen ventas registradas, algunas con `paid_by_transfer = true` y otras `false`
  WHEN el usuario consulta `GET /ui/tabs/sales`
  THEN cada fila de la tabla de historial muestra si esa venta fue por transferencia

### REQ-6: Filtro por método de pago en el historial
**Priority:** Should Have
**Scenario:** Usuario filtra solo ventas por transferencia
  GIVEN existen ventas con ambos valores de `paid_by_transfer`
  WHEN el usuario selecciona el filtro "Transferencia" y aplica
  THEN `GET /ui/tabs/sales?payment_method=transfer` devuelve solo ventas con `paid_by_transfer = true`

**Scenario:** Usuario filtra solo ventas que no son transferencia
  WHEN el usuario selecciona el filtro "Efectivo/otro" y aplica
  THEN `GET /ui/tabs/sales?payment_method=cash` devuelve solo ventas con `paid_by_transfer = false`

**Scenario:** Sin filtro, se muestran todas
  WHEN no se envía `payment_method` (o se envía vacío)
  THEN el historial no aplica ningún filtro por método de pago

### REQ-7: Datos existentes no se rompen tras la migración
**Priority:** Must Have
**Scenario:** Ventas registradas antes de esta feature
  GIVEN una base de datos con ventas existentes previas a la migración
  WHEN se aplica la migración de arranque (`app/migrations.py`)
  THEN todas las ventas existentes quedan con `paid_by_transfer = false`
  AND ninguna query existente sobre `inventory_movements` falla

### REQ-8: No se rompe la edición de ventas desde el tab "Ganancia por artículo"
**Priority:** Must Have
**Scenario:** Edición de venta desde `ui_profit_items.py` (fuera de scope de UI, pero
comparte `InventoryService.update_sale`)
  GIVEN el flujo existente `POST /ui/tabs/profit-items/sale/{id}/update` (sin campo
  `paid_by_transfer` en su formulario — fuera de scope de esta feature)
  WHEN se envía ese form sin `paid_by_transfer`
  THEN la venta se actualiza igual que hoy
  AND el valor de `paid_by_transfer` de esa venta permanece sin cambios (no se resetea a `false`)

---

## 2. MoSCoW Prioritization

| REQ | Prioridad |
|-----|-----------|
| REQ-1 | Must Have |
| REQ-2 | Must Have |
| REQ-3 | Must Have |
| REQ-4 | Must Have |
| REQ-5 | Must Have |
| REQ-6 | Should Have |
| REQ-7 | Must Have |
| REQ-8 | Must Have (regresión a evitar) |

---

## 3. Non-Functional Requirements

| Category | Question | Answer |
|----------|----------|--------|
| **Performance** | ¿Impacto en tiempo de respuesta? | Ninguno relevante — 1 columna booleana adicional en un SELECT/INSERT ya existente, sin nuevo JOIN. |
| **Security** | ¿Nuevo límite de autorización? | No. Crear/editar venta usa los mismos guards existentes (`ensure_admin_or_owner` en update/delete; creación sin restricción de rol, igual que hoy). |
| **Scalability** | ¿Volumen esperado? | Igual que ventas actuales — sin cambio de escala. |
| **Observability** | ¿Nuevo log/métrica? | El `log_event` existente en `sale_create`/`sale_update` puede incluir `paid_by_transfer` en su `detail` (opcional, no bloqueante). |
| **Reliability** | ¿Comportamiento ante fallo? | Migración aditiva (columna `NOT NULL DEFAULT false`) — sin downtime, sin backfill manual necesario. |

---

## 4. Out of Scope

- `SalesDocument` / `SalesDocumentItem` (facturas y presupuestos) — el usuario confirmó
  explícitamente que esta feature NO aplica a documentos de venta.
- Reportes agregados (profit por método de pago, dashboards, tools del copiloto IA) — el
  dato solo se captura, edita, muestra y filtra en el tab Ventas por ahora.
- Conciliación de caja / cuadre de efectivo vs. transferencias — mencionado por el usuario
  como uso futuro, no como requisito de esta iteración.
- Cambiar el formulario de edición de venta usado desde el tab "Ganancia por artículo"
  (`partials/profit_items_sale_edit_form.html` / `ui_profit_items.py`) — comparte el mismo
  método de servicio pero su UI queda fuera de esta iteración (ver REQ-8 para la regla de
  no-regresión).

## 5. Dependencies

- Ninguna dependencia externa. Depende únicamente de la migración de arranque existente
  (`app/migrations.py::run_startup_tasks`), que ya corre en cada arranque de la app.
- No depende de otro PAE en curso.

## 6. Open Questions

Ninguna. Todos los requisitos fueron confirmados con el usuario antes de iniciar esta
especificación. Procediendo a Phase 2.
