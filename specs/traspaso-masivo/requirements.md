# Phase 1 — Requirements: Traspaso masivo (checklist en ventana flotante)

## Resumen

El usuario hoy traspasa stock CENTRAL → POS **producto por producto** (escribir SKU en el
datalist + "Agregar línea"). Quiere poder **marcar de una sola vez** todos los productos que
desea traspasar y ejecutar en un solo paso, **sin perder** el formulario línea-a-línea actual.

**Hallazgo clave (reduce alcance):** el backend `InventoryService.transfer()` YA soporta
múltiples líneas en una sola transacción con FIFO (`TransferCreate.lines`). El trabajo es
**casi todo UI**: una ventana flotante (modal) con checklist.

## REQs con criterios de aceptación (Gherkin)

### REQ-1 — Botón "Traspaso masivo"
**MoSCoW:** Must
**Como** usuario con rol admin/owner
**Quiero** un botón "Traspaso masivo" junto a "Agregar línea" en la pestaña Traspasos
**Para** abrir la ventana de selección masiva.

```gherkin
Dado que estoy en la pestaña Traspasos con un negocio activo
Cuando pulso "Traspaso masivo"
Entonces se abre una ventana flotante con el listado de productos
  Y la ventana usa el origen/destino/fecha ya seleccionados en el formulario
```

### REQ-2 — Checklist de productos con stock en el origen
**MoSCoW:** Must
```gherkin
Dado que abrí la ventana de traspaso masivo
Entonces veo una tabla con todos los productos con stock > 0 en el origen
  Y cada fila muestra SKU, nombre, unidad y stock disponible
```

### REQ-3 — Checkbox + cantidad ajustable (default = stock completo)
**MoSCoW:** Must
```gherkin
Dado que la ventana muestra los productos
Cuando marco el checkbox de un producto
Entonces su campo cantidad se habilita y se precarga con TODO el stock disponible
  Y puedo ajustar la cantidad
Cuando desmarco el checkbox
Entonces el campo cantidad se deshabilita
```

### REQ-4 — Búsqueda/filtro dentro de la ventana
**MoSCoW:** Should
```gherkin
Dado que la ventana tiene muchos productos
Cuando escribo en el campo de búsqueda
Entonces se filtran las filas por SKU o nombre (sin recargar)
```

### REQ-5 — Seleccionar todo
**MoSCoW:** Should
```gherkin
Dado que la ventana muestra productos
Cuando marco el checkbox "seleccionar todo"
Entonces todas las filas se marcan y sus cantidades se precargan con stock
  Y al desmarcarlo, todas se desmarcan
```

### REQ-6 — Ejecutar traspaso masivo en un solo paso
**MoSCoW:** Must
```gherkin
Dado que marqué N productos con sus cantidades
Cuando pulso "Traspasar"
Entonces se crean los movimientos de salida y entrada de los N productos
  en una sola operación (una transacción, FIFO respetado)
```

### REQ-7 — Éxito: cerrar y refrescar
**MoSCoW:** Must
```gherkin
Dado que el traspaso masivo se ejecutó sin errores
Entonces la ventana se cierra
  Y la pestaña Traspasos se refresca mostrando los nuevos movimientos
```

### REQ-8 — Error: ventana abierta con selección preservada
**MoSCoW:** Must
```gherkin
Dado que el traspaso masivo falla (p.ej. stock insuficiente, sin productos)
Entonces la ventana permanece abierta
  Y muestra el mensaje de error
  Y conserva los productos marcados y sus cantidades
```

### REQ-9 — El formulario línea-a-línea queda intacto
**MoSCoW:** Must
```gherkin
Dado que existe el formulario actual de traspaso línea-a-línea
Entonces sigue funcionando exactamente igual (no se modifica su comportamiento)
```

## Requisitos no funcionales (NFRs)

| NFR | Requisito |
|-----|-----------|
| NFR-1 | Permisos: solo `admin`/`owner` (`ensure_admin_or_owner`), coherente con el flujo actual |
| NFR-2 | Transaccionalidad: N productos = 1 transacción (ya garantizado por `service.transfer()`) |
| NFR-3 | Sin cambios al modelo de datos ni migraciones |
| NFR-4 | Sin nuevas dependencias en `requirements.txt` |
| NFR-5 | Sin tocar `InventoryService`, `security.py`, `deps.py`, `db.py`, `auth.py` |
| NFR-6 | Reutilizar la infraestructura de modal existente (`.modal-overlay` + `closeModal`) |

## Fuera de alcance

- Cambios al backend `InventoryService.transfer()` (ya hace el batch).
- Negocio sin POS (Cervezas Mariela) — decisión separada.
- Paginación server-side del checklist (se usa filtro client-side).
- Edición/borrado masivo de traspasos existentes.
