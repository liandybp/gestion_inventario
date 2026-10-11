# Requirements — Quitar "Crear producto manualmente" de la pestaña Compras

**Feature slug:** `remove-duplicate-product-creation` · **Fecha:** 2026-10-10 · **Rama:** `develop`

## Contexto

La pestaña **Compras** (`partials/tab_purchases.html`) muestra dos bloques:

1. `partials/purchase_panel.html` — formulario **Registrar compra** (`POST /ui/purchase`). Si el
   artículo no existe, crea el producto a partir de "Nombre (si es nuevo)", pero con
   `default_sale_price=0`.
2. `partials/product_panel.html` — "Importar productos desde factura (PDF)", **"Crear producto
   manualmente"** (`POST /ui/product`) e "Historial de productos".

El formulario "Crear producto manualmente" duplica la creación de producto que ya hace
"Registrar compra". El usuario quiere una única vía: la compra.

## 1.1 Requisitos funcionales

### REQ-1: La pestaña Compras no muestra "Crear producto manualmente"
**Priority:** Must Have

WHEN el usuario abre la pestaña Compras, THE SYSTEM SHALL NOT mostrar el formulario
"Crear producto manualmente" ni ningún `form` con `hx-post="/ui/product"`.

```gherkin
Scenario: El formulario de creación manual ya no existe
  GIVEN un admin autenticado
  WHEN abre la pestaña Compras
  THEN no hay ningún form con hx-post="/ui/product"
  AND sigue visible el form "Importar productos desde factura (PDF)" (hx-post="/ui/product/from-invoice")
  AND sigue visible "Historial de productos"
  AND sigue existiendo el datalist #product-options (lo usa el campo "Artículo" de la compra)
```

### REQ-2: La ruta POST /ui/product deja de existir
**Priority:** Must Have

WHEN alguien envía `POST /ui/product`, THE SYSTEM SHALL responder 404 o 405 (la ruta no existe).

```gherkin
Scenario: La ruta eliminada no crea productos
  GIVEN un admin autenticado
  WHEN envía POST /ui/product con cabecera HX-Request: true y name="X", default_purchase_cost=1, default_sale_price=1
  THEN la respuesta es 404 o 405
```

### REQ-3: "Registrar compra" acepta un precio de venta para el producto nuevo
**Priority:** Must Have

WHEN se registra una compra que crea un producto nuevo (artículo vacío + "Nombre (si es nuevo)",
o SKU inexistente), THE SYSTEM SHALL guardar como `default_sale_price` el valor del campo
opcional "Precio de venta (si es nuevo)"; si está vacío, 0 (comportamiento actual).

```gherkin
Scenario: Producto nuevo con precio de venta
  GIVEN un admin autenticado en la pestaña Compras
  WHEN registra una compra con "Nombre (si es nuevo)" = "Producto Precio E2E", cantidad 1,
       costo 3.00 y "Precio de venta (si es nuevo)" = 7.50
  THEN ve "Compra registrada"
  AND en "Historial de productos" la fila de "Producto Precio E2E" muestra precio 7.50

Scenario: Producto nuevo sin precio de venta
  WHEN registra la compra de un producto nuevo dejando el precio de venta vacío
  THEN el producto se crea con precio de venta 0
```

### REQ-4: El precio de venta no altera productos existentes
**Priority:** Must Have

WHEN la compra es de un producto que ya existe, THE SYSTEM SHALL ignorar "Precio de venta
(si es nuevo)" y no modificar su `default_sale_price`.

### REQ-5: Precio de venta negativo se rechaza
**Priority:** Should Have

WHEN el precio de venta para un producto nuevo es negativo, THE SYSTEM SHALL responder con el
panel de error "Error en compra" y el detalle "El precio de venta no puede ser negativo", sin
crear producto ni compra.

## 1.2 MoSCoW

| REQ | Prioridad |
|---|---|
| REQ-1, REQ-2, REQ-3, REQ-4 | Must Have |
| REQ-5 | Should Have |
| Unidad/categoría/stock mínimo/imagen en el form de compra | Won't Have (se editan en "Historial de productos") |

## 1.3 NFRs

| Categoría | Respuesta |
|---|---|
| Performance | No aplica — un campo de formulario más; sin consultas nuevas |
| Security | Superficie menor: se elimina una ruta de escritura. `purchase()` mantiene `ensure_admin_or_owner` y CSRF (`HX-Request`) |
| Scalability | No aplica |
| Observability | Sin cambios: `log_event("product_create", source="manual_purchase")` ya existe |
| Reliability | El precio inválido (no numérico) se trata como vacío (`parse_optional_float` → None → 0) |

## 1.4 Fuera de alcance

- La importación de productos desde factura PDF (`POST /ui/product/from-invoice`) y su `default_sale_price=0`.
- Añadir unidad, categoría, stock mínimo o imagen al formulario de compra.
- `ProductService.create` / `ProductCreate` (siguen existiendo; solo se quita su uso en este router).
- La app de escritorio: la pestaña Compras ya está oculta en modo desktop.

## 1.5 Dependencias

Ninguna. Sin migraciones ni cambios de esquema.

## 1.6 Preguntas abiertas

Resueltas con el usuario (2026-10-10): añadir campo opcional de precio de venta (sí) y eliminar
la ruta `POST /ui/product` (sí). All requirements are unambiguous. Proceeding to Phase 2.
