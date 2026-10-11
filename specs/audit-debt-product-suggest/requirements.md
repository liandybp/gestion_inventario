# Requirements — Sugerencias dinámicas de productos + deuda de la auditoría v0.4.11

**Feature slug:** `audit-debt-product-suggest` · **Fecha:** 2026-10-10 · **Rama:** `develop` (base `6fce595`, v0.4.11)
**Origen:** hallazgos pendientes de `docs/auditorias/2026-10-10-remove-duplicate-product-creation-reaudit.md`
(H-012, H-104, H-105, H-106, H-108, H-109). **Ejecutor:** `cc_local`, un envelope por sesión.

## Contexto

- **H-012:** los `datalist` de productos (Compras, Ventas, Documentos, Traspasos, Devoluciones y modales
  de edición) se rellenan con `ProductService.search("", limit=200)`, que devuelve los 200 productos
  **más antiguos** del negocio (`ProductRepository.search` → `list()[:limit]`, orden por id). En un
  negocio con más de 200 productos, los posteriores nunca aparecen como sugerencia.
  **Decisión del usuario (2026-10-10): búsqueda dinámica** — al teclear, el servidor devuelve coincidencias.
- **H-104:** `nan`/`inf` aún entran por `parse_optional_float` (UI), `ProductService.create/update`
  (UI y API JSON) e `InventoryService.sale`.
- **H-105:** el SKU es único **global**; si choca con un producto de otro negocio, el mensaje dice
  "ya existe" aunque el usuario no lo vea en su negocio.
- **H-106:** el bucle de asignación de SKU con reintentos está duplicado (`ProductService.create` y
  `create_for_purchase`); `list_sku_prefixes` vive en `PurchaseRepository` pero es una consulta de
  productos; la lógica de refresco del panel de productos está escrita tres veces con variantes.
- **H-108:** al fallar "Guardar cambios" en el modal de edición de producto, el error se pinta en
  `#product-panel` y `app.js` cierra el modal: se pierde lo tecleado.
- **H-109:** 9 respuestas de compras calculan `product_options` (carga de la tabla de productos) que
  su plantilla no usa.

## 1.1 Requisitos funcionales

### REQ-1: Sugerencias dinámicas al teclear · Must Have
WHEN el usuario teclea al menos 2 caracteres en un campo de artículo con sugerencias, THE SYSTEM SHALL
sustituir el contenido de su `datalist` por las coincidencias del servidor (nombre o SKU, sin
distinguir tildes ni mayúsculas, máx. 30, solo del negocio activo), respetando el formato de cada campo.

```gherkin
Scenario: Un producto fuera de los 200 primeros aparece al teclear
  GIVEN un negocio con 205 productos
  WHEN el usuario teclea "0205" en el campo "Artículo" de Ventas
  THEN el datalist contiene la opción del producto 205

Scenario: Menos de 2 caracteres no consulta
  WHEN el usuario teclea "a"
  THEN el datalist no cambia
```

Formatos por campo (el valor insertado es lo que interpretan las rutas actuales):

| Campo | Formato |
|---|---|
| Compras, Documentos, edición de compra/venta, Traspasos, beneficios | `label`: `SKU - Nombre` |
| Venta rápida | `sale`: `SKU - Nombre`, `SKU` y `Nombre` |
| Devoluciones (Inventario) | `sku`: valor `SKU`, texto `SKU - Nombre` |

### REQ-2: Endpoint de sugerencias seguro · Must Have
`GET /ui/product-suggest?q=<texto>&fmt=<label|sale|sku>` devuelve `<option>` HTML escapado, solo del
negocio activo, requiere sesión (middleware `/ui`) y está disponible para todos los roles (los
operadores venden). `fmt` desconocido → `label`. `q` con menos de 2 caracteres → respuesta vacía.

```gherkin
Scenario: Aislamiento por negocio
  GIVEN el producto "Ajeno" pertenece a otro negocio
  WHEN se consulta /ui/product-suggest?q=Ajeno
  THEN la respuesta no contiene "Ajeno"

Scenario: Escapado
  GIVEN un producto llamado "<b>X</b>"
  THEN la respuesta contiene "&lt;b&gt;X&lt;/b&gt;" y no "<b>"
```

### REQ-3: El modal de edición de producto conserva lo tecleado tras un error · Must Have
WHEN "Guardar cambios" falla (409/422/404), THE SYSTEM SHALL mantener el modal abierto, mostrar el
error dentro del modal y conservar los valores tecleados (salvo el fichero de imagen).

```gherkin
Scenario: SKU duplicado al editar
  GIVEN el modal de edición de "E2E-SKU1" abierto
  WHEN el usuario cambia el SKU a "E2E-SKU2" (ya existe) y el nombre a "Nombre nuevo" y guarda
  THEN el modal sigue abierto con el error "El SKU 'E2E-SKU2' ya existe"
  AND el campo Nombre conserva "Nombre nuevo"
```

### REQ-4: Valores no finitos rechazados en la raíz · Must Have
- `parse_optional_float("nan"|"inf"|"-inf")` → `None` (igual que cualquier texto no numérico).
- `POST /ui/purchase`: un costo o precio **tecleado** que no se puede convertir (texto, `nan`, `inf`)
  → 422, no se trata como vacío (si no, `sale_price="nan"` acabaría como precio 0 sin avisar).
- `ProductService.create/update` → 422 `"<campo> must be a finite number"` para `min_stock`,
  `default_purchase_cost`, `default_sale_price` no finitos (cubre UI y `POST/PUT /products`).
- `InventoryService.sale` → 422 si `quantity` o `unit_price` no son finitos.

### REQ-5: Mensaje claro si el SKU está en otro negocio · Should Have
WHEN una creación o edición con SKU explícito choca con la unicidad global, THE SYSTEM SHALL responder
409 con `"El SKU 'X' ya existe"` si está en el negocio activo, o `"El SKU 'X' ya está en uso en otro
negocio"` si no. Sin SKU (automático) agotado: `"No se pudo generar un SKU automáticamente"`.

### REQ-6: Refactor sin cambio funcional · Should Have
- Un único bucle de asignación de SKU (`ProductService._insert_with_sku_retry`), usado por `create`
  (commit) y `create_for_purchase` (flush).
- `list_sku_prefixes` pasa a `ProductRepository` (numeración global intacta).
- Un único helper `_product_panel_refresh(product_service)` en `ui_purchases.py`; se elimina la lógica
  "garantizar el recién creado en el datalist" (REQ-1 la hace innecesaria).

### REQ-7: Sin cálculos de `product_options` sin uso en compras · Could Have
Las respuestas de `purchase_edit_form`, `purchase_update`, `purchase_delete` y
`purchase_delete_selected` dejan de calcular `product_options`.

## 1.2 MoSCoW

| Prioridad | REQs |
|---|---|
| Must Have | REQ-1, REQ-2, REQ-3, REQ-4 |
| Should Have | REQ-5, REQ-6 |
| Could Have | REQ-7 |
| Won't Have | Búsqueda en SQL, datalist inicial (se mantiene), modal de "editar inventario" |

## 1.3 NFRs

| Categoría | Respuesta |
|---|---|
| Performance | Petición con debounce de 250 ms y mínimo 2 caracteres; máx. 30 resultados. La búsqueda carga los productos del negocio en Python (`ProductRepository.search`): aceptable a la escala actual (cientos/pocos miles) |
| Security | Endpoint GET de solo lectura bajo el middleware de sesión de `/ui`; filtrado por `business_id`; salida escapada por Jinja |
| Scalability | Ver Performance; búsqueda en SQL fuera de alcance |
| Observability | Sin cambios |
| Reliability | Respuestas obsoletas o redirecciones al login se descartan en el cliente |

## 1.4 Fuera de alcance

- Convertir `ProductRepository.search` a SQL (`ILIKE`) — pierde la normalización de tildes.
- El contenido inicial de los datalist (siguen siendo los 200 primeros; se sustituyen al teclear).
- El modal de "editar inventario" (`product_update_inventory`), mismo patrón que H-108.
- Conservar el fichero de imagen tras un error en el modal (los navegadores no lo permiten).
- `product_options` sin uso en routers distintos de `ui_purchases.py`.

## 1.5 Dependencias

Base `6fce595`. Sin migraciones.

## 1.6 Preguntas abiertas

Resuelta: H-012 → búsqueda dinámica (usuario, 2026-10-10). All requirements are unambiguous.
