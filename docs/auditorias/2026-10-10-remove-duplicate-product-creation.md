# Auditoría de Ingeniería — Quitar "Crear producto manualmente" (v0.4.11)

| Campo | Valor |
|---|---|
| Fecha | 2026-10-10 |
| Alcance | Diff sin commitear de `develop` del PAE `specs/remove-duplicate-product-creation/` |
| Score | **3.70 / 5.00 🟡** (incluye hallazgos verificados de `/code-review`) |
| Implementado por | `cc_local` (Claude Code + Qwen3.6-35B-A3B local), un envelope por sesión |
| Auditor | Claude Code (`auditoria-ingenieria`) |

## 1. Contexto y método

**Archivos auditados:** `app/routers/ui_products.py`, `app/routers/ui_purchases.py`
(`purchase()`), `app/templates/partials/product_panel.html`,
`app/templates/partials/purchase_panel.html`, `tests/e2e/test_product_creation_in_purchase_e2e.py`,
`VERSION`. Se evaluó también el código existente que el cambio convierte en la única vía de
creación de productos desde la interfaz (`purchase()` completo y `ProductService.create`).

**Verificación ejecutada:** 304 unit verdes (cobertura 84,80 %), 28 e2e verdes (24 previos + 4
nuevos), comparación del diff contra los 7 envelopes del PAE.

**Fidelidad al PAE:** el diff coincide línea a línea con los envelopes T1-T7. Scope respetado:
solo los 6 archivos previstos, más dos cambios ajenos al PAE (`.idea/PythonProject.iml`,
`CLAUDE.md`, ver H-006).

**Limitaciones:** auditoría acotada al cambio. Las dimensiones sin impacto (8, 10) se puntúan
por ausencia de regresión.

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ⚠️ | La creación de `Product` vive en el router, duplicada en 2 ramas (`ui_purchases.py:379-396`, `413-426`), sin usar `ProductService.create` (`product_service.py:31`) |
| 2 | Cohesión / Acoplamiento | ✅ | Ruta muerta e import `ProductCreate` eliminados (`ui_products.py:15`); regla "precio ≥ 0" duplicada con el servicio (H-002) |
| 3 | Capas / Límites | ⚠️ | El router hace `db.add`/`db.commit` de `Product` directamente (preexistente, `ui_purchases.py:391-393`, `425-426`) |
| 4 | Seguridad | ✅ | Superficie reducida (ruta de escritura con subida de imagen eliminada); `ensure_admin_or_owner` y CSRF intactos; NaN/inf aceptados (H-003) |
| 5 | Robustez | ✅ | Validación dentro del `try` (`ui_purchases.py:363-365`) → panel "Error en compra" con 422 |
| 6 | Consistencia transaccional | ⚠️ | El precio se valida antes de `db.add`, pero el producto nuevo se commitea antes de que `InventoryService.purchase` valide cantidad/costo (H-007) |
| 7 | Observabilidad | ⚠️ | La rama sin SKU no registra `product_create` (H-001) |
| 8 | Configuración | ✅ | Sin cambios |
| 9 | Pruebas | ✅ | 4 e2e cubren REQ-1, REQ-2, REQ-3 y REQ-5; faltan REQ-3 "vacío → 0" y REQ-4 (H-004) |
| 10 | Escalabilidad | ✅ | Sin consultas nuevas |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 3 | 30 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 40 |
| 3 | Capas / Límites | 10 | 3 | 30 |
| 4 | Seguridad | 15 | 4 | 60 |
| 5 | Robustez | 15 | 4 | 60 |
| 6 | Consistencia transaccional | 10 | 3 | 30 |
| 7 | Observabilidad | 10 | 3 | 30 |
| 8 | Configuración | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 4 | 40 |
| 10 | Escalabilidad | 5 | 5 | 25 |
| | **Total** | **100** | | **370 → 3.70 🟡** |

Las dimensiones 1, 3, 6 y 7 se quedan en 3 por deuda **preexistente** en `purchase()` que este
cambio no introduce, pero sí vuelve más relevante: tras quitar el formulario manual,
`purchase()` es la única vía de la UI para crear productos.

## 4. Hallazgos

### 🔴 Críticos
Ninguno.

### 🟠 Altos

**H-007 — Una compra fallida deja un producto huérfano** · Dim. 6 · *(de `/code-review`, verificado)*
- **Evidencia:** `purchase()` commitea el producto nuevo (`ui_purchases.py:391-393` y `425-426`)
  **antes** de `service.purchase()`, que después puede lanzar 422: `unit_cost is required` si el
  costo viene vacío y el producto nuevo no tiene costo por defecto, o `quantity must be > 0`
  (`inventory_service.py:1417-1425`). En el formulario el costo no es obligatorio y la
  cantidad admite 0 (`min="0"`).
- **Riesgo:** "Nombre (si es nuevo)" + costo vacío → producto creado + panel "Error en compra".
  Al reintentar se crea otro producto con otro SKU automático: duplicados en el catálogo.
  Preexistente, pero ahora es la única vía de creación de la UI.
- **Recomendación:** validar `quantity > 0` y costo presente (o derivable) **antes** de crear el
  producto, o crear producto y compra en una única transacción (`flush` en vez de `commit` y
  un solo `commit` al final, con `rollback` si la compra falla).

### 🟡 Medios

**H-001 — La creación de producto sin SKU no deja evento de auditoría** · Dim. 7
- **Evidencia:** `ui_purchases.py:374-396` (rama "Artículo" vacío + nombre, SKU autogenerado)
  crea y commitea el producto sin `log_event`. Solo la rama de SKU inexistente lo registra
  (`ui_purchases.py:431-440`, `action="product_create"`). El `create_product` eliminado sí lo
  registraba.
- **Riesgo:** la vía de creación más habitual ahora queda sin rastro `product_create` en
  `audit_logs` (sí queda el `purchase_create` con el SKU). Regresión de trazabilidad.
- **Recomendación:** tras el `break` de la rama sin SKU, registrar
  `log_event(..., action="product_create", entity_id=created_product.sku, detail={"name": ..., "source": "manual_purchase"})`,
  incluyendo `default_sale_price` en `detail` en ambas ramas.

**H-002 — Creación de producto duplicada en el router, fuera de `ProductService`** · Dim. 1, 2, 3
- **Evidencia:** dos construcciones `Product(...)` + `db.add`/`commit` en `purchase()`
  (`ui_purchases.py:379-396`, `413-426`) frente a `ProductService.create`
  (`product_service.py:31-95`), que ya centraliza validaciones (nombre, costo, precio, stock
  mínimo) y la generación de SKU con reintentos. La regla "precio ≥ 0" queda escrita dos veces
  (`ui_purchases.py:363-365` y `product_service.py:36-39`).
- **Riesgo:** reglas que divergen (p. ej. `ProductService` exige `default_purchase_cost`; la
  compra no) y correcciones que hay que aplicar en tres sitios.
- **Recomendación:** `ProductService.create_from_purchase(name, sku, cost, sale_price)` (o
  reutilizar `create` con defaults) y que `purchase()` lo llame en ambas ramas.

**H-008 — La lista de sugerencias de "Artículo" no se actualiza tras crear un producto** · Dim. 1 (UX) · *(de `/code-review`, verificado)*
- **Evidencia:** la compra solo re-renderiza `#purchase-panel`; `datalist#product-options` e
  "Historial de productos" viven en `#product-panel` (`product_panel.html`), que no se refresca.
  El formulario eliminado sí re-renderizaba `#product-panel`. El propio e2e lo sortea
  cambiando de pestaña.
- **Riesgo:** el producto recién creado no aparece como sugerencia; el usuario vuelve a escribir
  el nombre en "Nombre (si es nuevo)" y crea un duplicado.
- **Recomendación:** devolver el datalist en la respuesta de la compra con `hx-swap-oob`, o
  disparar un refresco de `#product-panel` (`HX-Trigger`).

### 🟢 Bajos

**H-003 — `sale_price` acepta `nan` e `inf` por POST directo** · Dim. 4, 5
- **Evidencia:** `parse_optional_float("nan")` → `nan`, `("inf")` → `inf` (verificado);
  `nan < 0` es falso, así que se guarda en `default_sale_price` (`Float`, `models.py:223`) y el
  historial mostraría `nan`. El `<input type="number">` lo impide desde el navegador; solo es
  alcanzable con POST manual de un admin/owner. Mismo problema preexistente en `unit_cost`.
- **Recomendación:** validar `math.isfinite(parsed_sale_price)` junto con el `< 0`.

**H-004 — Escenarios del spec sin test** · Dim. 9
- **Evidencia:** no hay test para REQ-3 "precio vacío → 0" ni para REQ-4 "producto existente:
  el precio se ignora".
- **Recomendación:** dos casos más en el e2e (o un test unitario de la ruta con `TestClient`).

**H-005 — El precio se ignora en silencio si el producto ya existe** · Dim. 1 (UX)
- **Evidencia:** REQ-4 se cumple por construcción, pero si el usuario rellena el precio de un
  producto existente no recibe aviso.
- **Recomendación:** añadir al `message_detail` "Precio de venta ignorado: el producto ya existía" cuando `sale_price` venga relleno y no se cree producto.

**H-009 — Un precio negativo bloquea también la compra de un producto existente** · Dim. 5 · *(de `/code-review`, verificado)*
- **Evidencia:** la validación (`ui_purchases.py:363-365`) corre para toda compra, aunque REQ-4
  dice que el campo se ignora para productos existentes y REQ-5 limita el rechazo a productos
  nuevos. Desviación del propio diseño del PAE.
- **Recomendación:** aplicar la validación solo en las ramas que crean producto.

**H-010 — `.bumpversion.cfg` sigue en 0.4.10** · Proceso · *(de `/code-review`, verificado)*
- **Evidencia:** `.bumpversion.cfg:2` `current_version = 0.4.10` con `VERSION` = 0.4.11.
- **Riesgo:** el siguiente `bump2version patch` falla al no encontrar `0.4.10`.
- **Recomendación:** actualizarlo a 0.4.11 (y añadir la entrada en `CHANGELOG.md`).

**H-011 — Capacidades perdidas y subtítulo desactualizado** · Producto · *(de `/code-review`)*
- **Evidencia:** `tab_purchases.html:3` sigue diciendo "Crea productos y registra compras.". Sin
  el formulario manual ya no se puede dar de alta un producto sin compra, ni fijar SKU,
  unidad, categoría, stock mínimo o imagen al crearlo; los productos nuevos nacen con
  `min_stock=0`, así que no generan alerta de reposición hasta editarlos.
- **Recomendación:** ajustar el subtítulo; confirmar con el usuario que estas pérdidas son
  aceptables (se editan después en "Historial de productos").

**H-006 — Documentación y working tree con cambios ajenos** · Proceso
- **Evidencia:** `CLAUDE.md:7` dice "0.4.11 — desplegada en dev y prod", pero el cambio ni
  siquiera está commiteado; `.claude/CLAUDE.md` sigue en 0.4.10 y `CHANGELOG.md` no tiene entrada 0.4.11. `.idea/PythonProject.iml` modificado (cambio del IDE, fuera de scope).
- **Recomendación:** corregir la línea de versión ("pendiente de commit/deploy") y no incluir
  `.idea/` en el commit.

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-001, H-003, H-006, H-009, H-010 |
| 🎯 Alto impacto, esfuerzo medio | H-007, H-008 |
| 🎯 Estratégicos | H-002 |
| 📋 Baja prioridad | H-004, H-005 |
| ❌ Evitar | — |

## 6. Backlog priorizado

1. H-007 — producto huérfano: validar antes de crear o transacción única (antes del commit)
2. H-008 — refrescar `datalist#product-options` tras la compra (antes del commit)
3. H-006 + H-010 — docs, `.bumpversion.cfg`, `CHANGELOG.md`; excluir `.idea/` (antes del commit)
4. H-009 — validación del precio solo al crear
5. H-001 — `log_event` en la rama sin SKU (antes del commit)
6. H-003 — `math.isfinite` en `sale_price` (antes del commit)
7. H-004 — tests de REQ-3 vacío y REQ-4 (+ test de H-007)
8. H-005 — aviso de precio ignorado · H-011 — subtítulo y confirmación con el usuario
9. H-002 — extraer la creación a `ProductService` (PAE propio)

## 7. Plan 30/60/90 días

### 30 días
H-001, H-003, H-004, H-006..H-010 aplicados; re-auditoría ≥ 4.0.

### 60 días
H-002: `ProductService.create_from_purchase`, con `purchase()` y la importación de facturas
(`purchase_from_invoice`, mismo patrón inline) usando el servicio. Criterio: ningún `Product(`
construido en routers UI.

### 90 días
Revisar el mismo patrón (`Product(` inline y `log_event` incompleto) en el resto de routers UI.

## 8. Criterios de cierre

- `audit_logs` contiene `product_create` para productos creados desde compra sin SKU.
- `sale_price=nan|inf` → 422.
- e2e de REQ-3 (vacío → 0) y REQ-4 verdes.
- Compra fallida → ningún producto nuevo en BD (test).
- Tras crear un producto por compra, aparece en `datalist#product-options` sin cambiar de pestaña.
- `CLAUDE.md` refleja el estado real; commit sin `.idea/`.

---

## 9. Remediación y reauditoría (2026-10-10, misma sesión)

Corregido directamente por Claude Code a petición del usuario ("corrige todo"), en dos rondas,
cada una seguida de `/code-review` (high).

### Ronda 1 — hallazgos H-001..H-011

| ID | Corrección |
|---|---|
| H-001 | `product_create` auditado en ambas ramas, ahora en la misma transacción que la compra |
| H-002 | Nuevo `ProductService.create_for_purchase(name, sku, purchase_cost, sale_price, category)` (flush, sin commit). Lo usan la compra manual **y** la importación de facturas: ningún `Product(` en `ui_purchases.py` |
| H-003 | `math.isfinite` en precio y costo (`create_for_purchase`) |
| H-004 | Tests de precio vacío → 0 y de producto existente |
| H-005 | Aviso "Precio de venta ignorado: el producto ya existía" |
| H-006 | `CLAUDE.md` (ambos) y `CHANGELOG.md` al día; `.idea/` fuera del commit |
| H-007 | Producto + auditoría + compra en un único commit; `rollback` en el `except` |
| H-008 | `#product-panel` (datalist + historial) refrescado por `hx-swap-oob` tras crear producto |
| H-009 | La validación del precio solo aplica al crear producto |
| H-010 | `.bumpversion.cfg` → 0.4.11 |
| H-011 | Subtítulo de la pestaña actualizado |

**Bug previo descubierto al probar H-007:** htmx no hace swap de respuestas ≥ 400, así que
**ningún panel de error de la UI se mostraba** (compras, ventas, gastos, extracciones, productos).
Handler global `htmx:beforeSwap` en `app/static/js/app.js` (solo `text/html`).

### Ronda 2 — `/code-review` sobre las correcciones (10 hallazgos)

| # | Hallazgo | Estado |
|---|---|---|
| R2-1 | `search("")` devuelve los 200 productos **más antiguos**: el recién creado no entraba en el datalist | ✅ El producto creado se añade siempre a la lista (compra y factura). ⚠️ Limitación global → **H-012** |
| R2-2 | Handler solo para 409/422; la mayoría de paneles de error usan 404/400 | ✅ Cualquier 4xx `text/html` (excluido el chat del copiloto) |
| R2-3 | `quantity` acepta `nan`/`inf` (Pydantic) y `nan <= 0` es falso | ✅ `math.isfinite` en `InventoryService.purchase` (cantidad y costo; cubre también la API JSON) |
| R2-4 | El test de huérfano fallaba antes del flush | ✅ Test con lote duplicado (falla en el commit); **mutation-test**: cambiando `flush` por `commit` el test falla |
| R2-5 | El panel de error vaciaba el formulario; con modales, el error quedaba detrás del modal abierto | ✅ El formulario de compra conserva lo tecleado; los modales se cierran si el error se pinta fuera de ellos |
| R2-6 | La importación de facturas seguía con `Product(` inline, commit por producto y sin refresco | ✅ Usa `create_for_purchase` con transacción por línea, `rollback` en ambos `except` y refresco del panel |
| R2-7 | La consulta del producto existente tragaba excepciones y acababa creando un producto | ✅ Sin fallback: la excepción se propaga |
| R2-8 | `product_create` en una segunda transacción tras el commit | ✅ Se registra antes del commit de la compra. `purchase_create` sigue siendo best-effort (el id del movimiento solo existe tras el commit) |
| R2-9 | Validación duplicada router/servicio | ✅ Eliminada del router; la validan `create_for_purchase` e `InventoryService.purchase` |
| R2-10 | `_generate_sku` duplicaba la consulta de `list_sku_prefixes`; `product_options` calculado en cada respuesta | ✅ `_generate_sku` reutiliza el repositorio; `product_options` solo al refrescar |

### Hallazgo nuevo que requiere decisión

**H-012 — Las listas de sugerencias de toda la app solo muestran los 200 productos más antiguos** · Preexistente · 🟡 Medio
- **Evidencia:** `ProductRepository.search("")` → `list()[:limit]` ordenado por id ascendente
  (`product_repository.py:41-44`); 29 llamadas con `limit=200` alimentan los datalist de Compras,
  Ventas, Documentos, Inventario, etc.
- **Riesgo:** en un negocio con más de 200 productos, los posteriores al 200 nunca aparecen como
  sugerencia en ninguna pestaña (solo se pueden elegir tecleando el SKU exacto).
- **Recomendación:** decisión de producto: ordenar por nombre o por más recientes, subir el
  límite, o sugerencias dinámicas por búsqueda (`hx-get` con el texto tecleado). PAE propio.

### Verificación final

- 319 unit verdes (cobertura 84,97 %), 36 e2e verdes (12 del cambio).
- Mutation-test del rollback: OK.

### Scorecard tras la remediación

| # | Dimensión | Peso | Antes | Después | Ponderado |
|---|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 3 | 4 | 40 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 4 | 40 |
| 3 | Capas / Límites | 10 | 3 | 4 | 40 |
| 4 | Seguridad | 15 | 4 | 5 | 75 |
| 5 | Robustez | 15 | 4 | 5 | 75 |
| 6 | Consistencia transaccional | 10 | 3 | 5 | 50 |
| 7 | Observabilidad | 10 | 3 | 4 | 40 |
| 8 | Configuración | 5 | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 4 | 5 | 50 |
| 10 | Escalabilidad | 5 | 5 | 4 | 20 |
| | **Total** | **100** | **3.70** | | **455 → 4.55 🟢** |

Dimensión 10 baja a 4 por H-012 (preexistente, fuera del alcance de este cambio). Dimensiones 1, 3
y 7 se quedan en 4: el resto de rutas de compras/ventas mantiene el patrón de calcular datos que
la plantilla no usa y de auditoría best-effort tras el commit.
