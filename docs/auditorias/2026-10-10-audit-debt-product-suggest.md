# Auditoría de Ingeniería — Sugerencias dinámicas + deuda v0.4.11 (v0.4.12)

| Campo | Valor |
|---|---|
| Fecha | 2026-10-10 |
| Alcance | Diff sin commitear de `develop` (base `6fce595`): PAE `specs/audit-debt-product-suggest/` |
| Score | **3.75 / 5.00 🟡 → 4.15 / 5.00 🟡** tras corregir (ver §10) |
| Implementado por | `cc_local` (Claude Code + Qwen3.6-35B-A3B local), 27 envelopes, 61 min |
| Entrada | `/code-review` high (10 hallazgos; los principales verificados a mano) |
| Estado | **Corregidos H-201..H-210 el mismo día** (Claude Code; los errores eran del diseño del PAE). Versión sin cambiar: 0.4.12 |

## 1. Contexto y método

**Verificación ejecutada:** 350 unit (cobertura 86,13 %) + 41 e2e verdes; las verificaciones de
los 27 envelopes dan exactamente los valores esperados; diff limitado a los archivos del inventario
del diseño (24 modificados + 4 nuevos). Resultados idénticos a la validación previa del PAE en worktree.

**Fidelidad al PAE: total.** Los hallazgos de esta auditoría **no** son fallos de ejecución de
`cc_local`: H-201 y H-202 son **errores de diseño del PAE** (de Claude Code); el resto son huecos de
alcance del diseño o deuda previa.

**Verificaciones a mano:**
- `ui_sales.py:83-88` (`_sale_product_options`) y `ui_transfers.py:641-665` (`transfer_product_options`):
  las listas de Ventas y Traspasos se filtran por **stock > 0 en la ubicación**; no estaban limitadas a 200.
- `_normalize_text("E2E-SKU1 - Producto E2E Uno")` no está contenido ni en el SKU ni en el nombre → la
  búsqueda de una opción ya elegida devuelve 0 resultados.
- `InventoryService.update_purchase` (`inventory_service.py:1299`, comprobación en `:1314-1317`) solo
  valida `quantity <= 0` / `unit_cost < 0`.

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ✅ | `_insert_with_sku_retry` + `build` + `_sku_conflict_detail` bien separados (`product_service.py`) |
| 2 | Cohesión / Acoplamiento | ✅ | `ProductService` ya no depende de `PurchaseRepository`; `_product_panel_refresh` único |
| 3 | Capas / Límites | ✅ | `product_suggest` delega en `ProductService.search` (`ui_products.py:607`) |
| 4 | Seguridad | ✅ | Endpoint con sesión + negocio activo + autoescape (tests de aislamiento y XSS) |
| 5 | Robustez | ⚠️ | Ventas/Traspasos pierden el filtro de stock (H-201/202); lista vaciada tras elegir (H-203); `nan` silencioso en ventas y ediciones (H-204/205); imagen huérfana (H-209) |
| 6 | Consistencia transaccional | ✅ | Transacción única de compras intacta; `create_for_purchase` sigue con `flush` |
| 7 | Observabilidad | ✅ | Logging de colisiones de SKU en el helper |
| 8 | Configuración | ✅ | Sin cambios |
| 9 | Pruebas | ⚠️ | Ningún test cubría el filtro de stock de Ventas/Traspasos; el check manual del DoD partía de una premisa falsa |
| 10 | Escalabilidad | ⚠️ | Búsqueda O(N) en Python por pulsación (H-207) |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 4 | 40 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 40 |
| 3 | Capas / Límites | 10 | 4 | 40 |
| 4 | Seguridad | 15 | 4 | 60 |
| 5 | Robustez | 15 | 3 | 45 |
| 6 | Consistencia transaccional | 10 | 4 | 40 |
| 7 | Observabilidad | 10 | 4 | 40 |
| 8 | Configuración | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 3 | 30 |
| 10 | Escalabilidad | 5 | 3 | 15 |
| | **Total** | **100** | | **375 → 3.75 🟡** |

## 4. Hallazgos

### 🔴 Críticos
Ninguno (no hay pérdida de datos ni brecha; sí riesgo funcional alto en Ventas, ver H-201).

### 🟠 Altos

**H-201 — Venta rápida: las sugerencias dejan de filtrar por stock en el punto de venta** · Dim. 5 · Error de diseño del PAE
- **Evidencia:** `sale_panel.html:22` (`data-product-suggest="sale"`). El datalist de Ventas lo rellenan
  `_sale_product_options` (`ui_sales.py:83-88`) y `/ui/sale/product-options`: **solo productos con stock > 0
  en la ubicación seleccionada**; nunca estuvo limitado a 200. Al teclear, se sustituye por todo el catálogo.
- **Riesgo:** el operador elige un producto sin stock en su POS → error de stock insuficiente o venta
  contra el stock equivocado. Es el camino más usado de la app.
- **Recomendación:** quitar `data-product-suggest` de `sale_panel.html` y de `sale_edit_form.html` (comparte
  ese datalist, ver H-206). Si se quiere búsqueda dinámica en Ventas, debe filtrar por stock y ubicación
  (endpoint propio). El check manual del DoD ("en Ventas, teclear un producto posterior al 200") era erróneo.

**H-202 — Traspasos: las sugerencias dejan de filtrar por stock en el origen** · Dim. 5 · Error de diseño del PAE
- **Evidencia:** `tab_transfers.html:55, 69, 93`. `#transfer-products-list` lo rellena
  `transfer_product_options` (`ui_transfers.py:641-665`) con productos con stock en el **origen** seleccionado,
  e incluye opciones de SKU y de nombre sueltos, que el formato `label` pierde.
- **Riesgo:** traspasos de productos sin stock en el origen → el traspaso falla.
- **Recomendación:** quitar `data-product-suggest` de las 3 líneas de `tab_transfers.html`.

**H-203 — Elegir una sugerencia vacía la lista** · Dim. 5
- **Evidencia:** `app.js:647-658`. Al elegir una opción, el `input` dispara una búsqueda con
  `"SKU - Nombre"`, que no coincide con nada (verificado) → `input.list.innerHTML = ""`. Con menos de 2
  caracteres no se restaura la lista inicial.
- **Riesgo:** en formularios con varias líneas o al volver a un campo, no hay sugerencias hasta teclear
  2 caracteres; los escáneres de código de barras disparan búsquedas inútiles.
- **Recomendación:** guardar el contenido inicial del datalist (`dataset.initialOptions`) y restaurarlo
  con menos de 2 caracteres; no sustituir si la respuesta está vacía, o no consultar si el valor ya
  coincide con una opción existente.

### 🟡 Medios

**H-204 — Venta con precio `nan` o no numérico se registra al precio por defecto sin avisar** · Dim. 5
- **Evidencia:** `ui_sales.py:217` `unit_price=parse_optional_float(unit_price)`: tras T9, `nan` → `None`
  → `InventoryService.sale` usa `default_sale_price`. La regla de T9b ("un valor tecleado inválido es un
  error") solo se aplicó a compras. Igual en edición de producto (`ui_products.py`, costos/precios).
- **Recomendación:** aplicar la comprobación de T9b en `sale()` y en `product_update`; idealmente un
  helper común (`parse_required_float_or_422`).

**H-205 — Ediciones de movimientos aceptan `nan`** · Dim. 5 · Preexistente, fuera del alcance del PAE
- **Evidencia:** `update_purchase` (`inventory_service.py:1299`, `:1314-1317`), `update_sale` (`:1359`),
  `supplier_return_by_lot` (`:1484`) solo comprueban `<= 0`/`< 0`; las rutas usan `Form(float)`.
- **Riesgo:** cantidad `nan` en una edición corrompe stock y lotes FIFO (solo con petición manual).
- **Recomendación:** helper de validación compartido (`_require_positive_finite`) en `InventoryService`.

**H-206 — El modal de edición de venta y la venta rápida comparten datalist con formatos distintos** · Dim. 1
- **Evidencia:** `sale_edit_form.html:12` (`label`) usa `list="product-options"` de `sale_panel.html` (`sale`).
- **Recomendación:** se resuelve con H-201 (quitar el atributo de ambos).

**H-207 — Búsqueda O(N) en Python por pulsación** · Dim. 10
- **Evidencia:** `product_suggest` (`ui_products.py:607`) → `ProductRepository.search` carga todos los
  productos del negocio y filtra en Python. Trade-off aceptado en el diseño; con H-201 deja de afectar a Ventas.
- **Recomendación:** prefiltro SQL (`ILIKE` sobre sku/nombre) antes de normalizar; medir con el catálogo real.

**H-208 — DoD incompleto: CHANGELOG y CLAUDE.md** · Proceso
- **Evidencia:** `VERSION` = 0.4.12, sin entrada en `CHANGELOG.md`; `CLAUDE.md`/`.claude/CLAUDE.md` en 0.4.11.
- **Recomendación:** completar al cerrar las correcciones (paso 6 del DoD; tarea de Claude Code, no de un envelope).

### 🟢 Bajos

**H-209 — Imagen huérfana al fallar la edición en el modal** · Dim. 6
- **Evidencia:** `ui_products.py:141` guarda la imagen antes de `update()`; si falla (409/422), el fichero
  queda en disco y el modal re-renderizado no lo referencia.
- **Recomendación:** guardar la imagen tras validar, o borrarla en el `except`.

**H-210 — `update()` atribuye cualquier `IntegrityError` al SKU** · Dim. 5
- **Evidencia:** `product_service.py:233` → con `new_sku` vacío informa "El SKU 'X' ya existe" del propio producto.
- **Recomendación:** usar `_sku_conflict_detail` solo si `new_sku` cambió; si no, mensaje genérico.

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-201, H-202, H-206 (quitar atributos), H-204, H-210, H-208 |
| 🎯 Alto impacto, esfuerzo medio | H-203, H-205 |
| 📋 Baja prioridad | H-209 |
| 🔭 Estratégico | H-207 (búsqueda SQL) |

## 6. Backlog priorizado (para el 2026-10-11)

1. **H-201 + H-202 + H-206** — quitar `data-product-suggest` de `sale_panel.html`, `sale_edit_form.html` y `tab_transfers.html` (×3) + e2e que verifique que Ventas/Traspasos no sugieren productos sin stock
2. **H-203** — restaurar la lista inicial y no vaciarla con respuestas vacías (`app.js`) + e2e (elegir opción → la lista sigue)
3. **H-204** — regla "tecleado inválido = 422" en `sale()` y `product_update`
4. **H-210** — mensaje de `update()` solo si cambió el SKU
5. **H-205** — validación finita compartida en `InventoryService` (ediciones y devoluciones)
6. **H-209** — imagen tras validar
7. **H-208** — CHANGELOG 0.4.12, CLAUDE.md ×2, nota de Obsidian
8. **H-207** — prefiltro SQL (PAE propio, opcional)

## 7. Plan

- **2026-10-11:** puntos 1-7 → re-review + re-auditoría (objetivo ≥ 4.2) → commit → probar en dev
  (Ventas con un POS sin stock de algún producto; Compras con un producto posterior al 200).
- **Después:** H-207 si la búsqueda se nota lenta con el catálogo real.

## 8. Criterios de cierre

- En Ventas y Traspasos, las sugerencias solo muestran productos con stock en la ubicación (e2e).
- Elegir una sugerencia no vacía la lista (e2e).
- `nan`/texto en el precio de venta → 422 visible.
- Re-auditoría ≥ 4.2.

## 9. Lecciones para el harness

- **Grounding de datos, no solo de llamadas**: al diseñar H-012 se contaron las llamadas a
  `search("", 200)` pero no se comprobó la **fuente de cada datalist**. Dos usaban otra fuente (stock
  por ubicación). El pre-mortem del SDD debería incluir "¿de dónde salen los datos que voy a sustituir?".
- La validación del PAE en worktree garantiza que los envelopes se aplican y los tests pasan, pero no
  detecta un requisito mal planteado si ningún test cubre el comportamiento perdido.
- `cc_local` ejecutó los 27 envelopes con fidelidad total en 61 min.

## 10. Re-auditoría tras las correcciones (2026-10-10)

Correcciones hechas por Claude Code (los hallazgos principales eran errores de diseño del PAE), dentro
de la misma versión 0.4.12. **377 unit (cobertura 86,15 %) + 44 e2e verdes.** Los e2e nuevos de H-201 y
H-203 se validaron con mutación (reintroducir el bug → fallan).

| Hallazgo | Corrección | Prueba |
|---|---|---|
| H-201/202/206 | Sin `data-product-suggest` en `sale_panel.html`, `sale_edit_form.html`, `tab_transfers.html` (×3); formato `sale` retirado del endpoint | e2e: Ventas no consulta `/ui/product-suggest` y su lista no cambia; unit: guardia de plantillas |
| H-203 | `app.js`: guarda la lista inicial por datalist y la restaura con < 2 caracteres; no consulta si el valor ya es una opción; respuesta vacía no vacía la lista | e2e: elegir opción → la lista sigue; vaciar → lista inicial |
| H-204 | `ui_common.parse_typed_float` (tecleado inválido → 422) en compras, ventas (`/ui/sale`, `/ui/sale/barcode`) y edición de producto (ambos modales) | unit del helper y de `product_update`; e2e venta con `nan`/texto → 422 |
| H-205 | `math.isfinite` en `update_purchase`, `update_sale`, `supplier_return_by_lot` | unit parametrizados |
| H-207 | `ProductRepository.search` filtra sobre `(id, sku, name)` y carga solo los productos que coinciden (normalización de tildes intacta) | suite de repositorios y de sugerencias |
| H-208 | `CHANGELOG.md` 0.4.12, `CLAUDE.md` ×2, nota de Obsidian | — |
| H-209 | `delete_uploaded_image` (solo nombre de fichero dentro de `uploads/`); la imagen se guarda tras validar y se borra si falla el guardado; en "editar inventario" no se borra si el producto ya se guardó | unit (incluido el caso de fallo posterior al guardado) |
| H-210 | `update()` solo culpa al SKU si cambió; si no, mensaje genérico | unit con `IntegrityError` forzado |

### Scorecard tras corregir

| # | Dimensión | Peso | Antes | Después | Motivo |
|---|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 4 | 4 | — |
| 2 | Cohesión | 10 | 4 | 4 | — |
| 3 | Capas | 10 | 4 | 4 | — |
| 4 | Seguridad | 15 | 4 | 4 | CSP `unsafe-inline` previo (W-1) |
| 5 | Robustez | 15 | 3 | 4 | Filtros de stock intactos, 422 visibles; el `fetch` de sugerencias ignora errores en silencio |
| 6 | Transaccional | 10 | 4 | 4 | Sin imágenes huérfanas |
| 7 | Observabilidad | 10 | 4 | 4 | — |
| 8 | Configuración | 5 | 5 | 5 | — |
| 9 | Pruebas | 10 | 3 | 5 | Cada hallazgo con test; e2e validados por mutación |
| 10 | Escalabilidad | 5 | 3 | 4 | Filtro sobre columnas; sigue siendo O(N) por pulsación (con debounce y mínimo 2 caracteres) |
| | **Total** | 100 | 3.75 | **4.15 🟡** | |

**Sin hallazgos abiertos de este diff.** Lo que separa de 🟢 es previo o estructural: CSP
`unsafe-inline` y búsqueda O(N) en Python (un `ILIKE` en SQL perdería la búsqueda sin tildes;
`unaccent` solo existe en PostgreSQL). **Prueba manual pendiente en dev tras el push:** Ventas con un
POS sin stock de algún producto; Compras con un producto posterior al 200.
