# Re-auditoría (ronda 3) — Quitar "Crear producto manualmente" (v0.4.11)

| Campo | Valor |
|---|---|
| Fecha | 2026-10-10 |
| Alcance | Diff sin commitear de `develop` (PAE `remove-duplicate-product-creation` + remediación de rondas 1 y 2), más las vías de creación de producto vecinas que el cambio deja como únicas |
| Score | **3.35 / 5.00 🟡** al auditar → **3.95 / 5.00 🟡** tras corregir H-101..H-103 (§9). La ronda 2 declaró 4.55, ver §1.1 |
| Entrada | `/code-review` high, ronda 3 (10 hallazgos, los 3 principales verificados a mano) |
| Informe previo | [2026-10-10-remove-duplicate-product-creation.md](2026-10-10-remove-duplicate-product-creation.md) |

## 1. Contexto y método

Diff completo sin commitear (excluidos `.idea/` y `.serena/`): `product_service.py`,
`inventory_service.py`, `ui_purchases.py`, `ui_products.py`, plantillas de Compras, `app.js`,
tests, `CHANGELOG.md`, `VERSION`, `.bumpversion.cfg`, `CLAUDE.md`. Se amplió el alcance a
`ui_products.py::product_from_invoice` (el botón "Importar productos desde factura" del panel
que el cambio conserva) y a `app/audit.py::log_event`, del que ahora depende la transacción única.

**Verificación a mano de los hallazgos principales:**
- `PurchaseCreate(quantity=0)` → `pydantic.ValidationError` (no `HTTPException`); `parse_dt("xx")` → `ValueError`.
- `product_from_invoice` construye `Product(...)` sin `business_id` (columna `nullable=True`, `models.py:210-211`).
- Producción (`laptop-server`, BD `inventario`, consulta de solo lectura): **0 productos con `business_id` nulo**. El bug existe pero aún no ha generado datos.
- `log_event` hace `db.rollback()` si falla su `flush` (`audit.py:39-41`).

### 1.1 Por qué baja de 4.55 a 3.35
1. **Regresión propia de la ronda 2**: se quitó de `purchase()` la comprobación `quantity <= 0` por
   "duplicada", pero el validador de `PurchaseCreate` lanza `ValidationError` antes de llegar al
   servicio → cantidad 0 da un 500 silencioso (H-101).
2. **Alcance ampliado**: la ronda 2 no evaluó la otra vía de creación de productos que queda
   (`product_from_invoice`, H-102) ni el efecto de `log_event` sobre la transacción única (H-103).
3. **Sobreestimación**: la ronda 2 puntuó 5 en Seguridad, Robustez y Pruebas sin haber probado
   cantidad 0, fechas inválidas ni la ruta de importación con una línea fallida.

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ⚠️ | Bucle SKU/reintentos duplicado en `create` (`product_service.py:68-100`) y `create_for_purchase` (`:133-160`); lógica de refresco del datalist escrita dos veces (`ui_purchases.py:314-317`, `:429-431`) |
| 2 | Cohesión / Acoplamiento | ⚠️ | `ProductService` importa `PurchaseRepository` para una consulta de productos (`product_service.py:12, 34`) |
| 3 | Capas / Límites | ⚠️ | `product_from_invoice` hace ORM directo en el router (`ui_products.py:450, 456`) |
| 4 | Seguridad | ⚠️ | Productos sin `business_id` (aislamiento multi-negocio, `ui_products.py:456`); `nan`/`inf` aún aceptados en `ProductService.create/update` y `parse_optional_float` |
| 5 | Robustez | ❌ | Cantidad 0 o fecha inválida → 500 silencioso (`ui_purchases.py:397-402`, `except HTTPException` en `:455`); `product_from_invoice` sin `rollback` en su `except` (`ui_products.py:487`) |
| 6 | Consistencia transaccional | ⚠️ | Commit único correcto, pero `log_event` puede revertir la transacción del llamador (`audit.py:41`) |
| 7 | Observabilidad | ✅ | `product_create` en ambas vías de compra; logging de colisiones en `create_for_purchase` |
| 8 | Configuración | ✅ | Sin cambios |
| 9 | Pruebas | ⚠️ | Sin test de cantidad 0/fecha inválida; el test de "precio vacío → 0" no distingue `None` de `0` (`products_history.html:30`); la importación de facturas con línea fallida no tiene test |
| 10 | Escalabilidad | ⚠️ | H-012 (datalist con los 200 más antiguos); 9 respuestas de compras siguen cargando la tabla de productos para una variable que no se usa (`ui_purchases.py:540, 597, 614, 658, 675…`) |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Ronda 2 | Ronda 3 | Ponderado |
|---|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 4 | 3 | 30 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 3 | 30 |
| 3 | Capas / Límites | 10 | 4 | 3 | 30 |
| 4 | Seguridad | 15 | 5 | 3 | 45 |
| 5 | Robustez | 15 | 5 | 3 | 45 |
| 6 | Consistencia transaccional | 10 | 5 | 4 | 40 |
| 7 | Observabilidad | 10 | 4 | 4 | 40 |
| 8 | Configuración | 5 | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 5 | 3 | 30 |
| 10 | Escalabilidad | 5 | 4 | 4 | 20 |
| | **Total** | **100** | **4.55** | | **335 → 3.35 🟡** |

## 4. Hallazgos

### 🔴 Críticos
Ninguno.

### 🟠 Altos

**H-101 — Cantidad 0 o fecha inválida en "Registrar compra" dan un 500 silencioso** · Dim. 5 · Regresión de la ronda 2
- **Evidencia:** `PurchaseCreate` valida `quantity > 0` con un validador Pydantic
  (`schemas.py:61-65`) que lanza `ValidationError`; `parse_dt` lanza `ValueError`. Ambos se
  construyen en `ui_purchases.py:397-402` y solo se captura `HTTPException` (`:455`). El input
  de cantidad tiene `min="0"`, así que el navegador deja enviar 0.
- **Riesgo:** uso normal (cantidad 0) → HTTP 500; el handler de `app.js` solo swapea 4xx, así
  que el usuario no ve nada. El `CHANGELOG` afirma lo contrario.
- **Recomendación:** convertir `ValidationError`/`ValueError` en 422 dentro del `try`, poner
  `min` > 0 en el input y añadir el e2e de cantidad 0.

**H-102 — "Importar productos desde factura" crea productos sin negocio** · Dim. 3, 4, 5 · Preexistente
- **Evidencia:** `ui_products.py:456` construye `Product(...)` sin `business_id`; la existencia
  se comprueba sobre todos los negocios (`:450`); commit por producto y `except Exception` sin
  `rollback` (`:487`).
- **Riesgo:** los productos importados no pertenecen a ningún negocio: no aparecen en historial ni
  en sugerencias, y su SKU (único global) bloquea a todos los negocios: comprarlo después da
  409. Una línea fallida deja la sesión inutilizable para el resto de líneas. Producción: 0
  afectados hoy.
- **Recomendación:** usar `ProductService.create_for_purchase` (o `create`) con el negocio
  activo, comprobar existencia por negocio y hacer `rollback` por línea.

**H-103 — `log_event` puede revertir la transacción del llamador** · Dim. 6
- **Evidencia:** `audit.py:39-41`: si su `flush` falla, hace `db.rollback()` y sigue.
- **Riesgo:** con la transacción única, un fallo de la auditoría tras `create_for_purchase`
  descarta el producto; `service.purchase` responde "Product not found", un error engañoso.
- **Recomendación:** que `log_event` use un `SAVEPOINT` (`db.begin_nested()`) en vez de revertir
  toda la transacción.

### 🟡 Medios

**H-104 — Valores no finitos solo cerrados en dos puntos** · Dim. 4
- **Evidencia:** `math.isfinite` en `create_for_purchase` e `InventoryService.purchase`, pero no
  en `ProductService.create/update`, `PurchaseCreate` ni `parse_optional_float`.
- **Riesgo:** `POST /products` o la edición de producto con `nan` guardan precio `NaN`.
- **Recomendación:** cerrar en la raíz: `parse_optional_float` rechaza no finitos y los schemas
  usan `allow_inf_nan=False`.

**H-105 — Mensaje engañoso cuando el SKU existe en otro negocio** · Dim. 5 (UX)
- **Evidencia:** `product_service.py:154-163` → "El SKU 'X' ya existe" aunque no exista en el negocio del usuario.
- **Recomendación:** distinguir "ya existe en otro negocio" (consulta global al recibir el `IntegrityError`).

**H-106 — Lógica duplicada en el servicio y el router** · Dim. 1, 2
- **Evidencia:** bucle SKU/reintentos en `create` y `create_for_purchase`; refresco del
  datalist dos veces en `ui_purchases.py`; `_generate_sku` vía `PurchaseRepository`.
- **Recomendación:** `_insert_with_sku_retry(build, commit: bool)`; helper
  `_product_panel_refresh(...)`; mover `list_sku_prefixes` a `ProductRepository`.

**H-107 — Huecos de test** · Dim. 9
- **Evidencia:** sin test de cantidad 0/fecha inválida; "precio vacío → 0" indistinguible de
  `None` en el historial; `purchase_from_invoice` con línea fallida sin test.
- **Recomendación:** test unitario que compruebe `default_sale_price == 0` en BD; test de
  importación con una línea que falla.

### 🟢 Bajos

**H-108 — Cerrar el modal descarta lo tecleado** · Dim. 5 (UX)
- **Evidencia:** `app.js:685-687` cierra el modal al pintar el error detrás.
- **Recomendación:** la ruta devuelve el formulario de edición con el error y `HX-Retarget` al modal.

**H-109 — Cálculo de `product_options` sin uso en 9 respuestas** · Dim. 10
- **Evidencia:** `ui_purchases.py:540, 597, 614, 658, 675…` (preexistente).
- **Recomendación:** eliminarlo de las respuestas que renderizan `purchase_panel.html`.

**H-012 (abierto, preexistente)** — datalist limitado a los 200 productos más antiguos. Pendiente de decisión de producto.

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-101, H-103, H-109 |
| 🎯 Estratégicos | H-102, H-104, H-106, H-012 |
| 📋 Baja prioridad | H-105, H-107, H-108 |
| ❌ Evitar | — |

## 6. Backlog priorizado

1. **H-101** — 422 para `ValidationError`/`ValueError` en `purchase()` + e2e (bloquea el commit: regresión)
2. **H-103** — `SAVEPOINT` en `log_event`
3. **H-102** — `product_from_invoice` vía `ProductService` con negocio activo
4. **H-107** — tests de cantidad 0, precio 0 en BD e importación con línea fallida
5. **H-104** — no finitos en la raíz (`parse_optional_float` + schemas)
6. **H-106** — deduplicar el bucle SKU y el refresco del datalist
7. **H-105, H-108, H-109**
8. **H-012** — PAE propio

## 7. Plan 30/60/90 días

### 30 días
H-101, H-102, H-103, H-107 corregidos antes del merge a `main`. Criterio: cantidad 0 → panel de error 422; importación de productos con `business_id` y `rollback` por línea; re-auditoría ≥ 4.2.

### 60 días
H-104, H-106: validación de no finitos en la raíz; un solo bucle de asignación de SKU.

### 90 días
H-012 (sugerencias dinámicas) y H-108/H-109.

## 8. Criterios de cierre

- `POST /ui/purchase` con `quantity=0` o fecha inválida → 422 con panel visible (e2e).
- Importar productos desde factura → productos con el `business_id` activo (test).
- Fallo de `log_event` no revierte producto ni compra (test unitario).
- Re-auditoría ≥ 4.2.

---

## 9. Remediación (2026-10-10, misma sesión)

Alcance acordado con el usuario: corregir **H-101, H-102 y H-103** con sus tests antes del commit;
el resto queda para un PAE aparte.

| ID | Corrección | Test |
|---|---|---|
| H-101 | `purchase()` valida `not quantity > 0` (cubre también NaN) y `parse_dt` dentro del `try` → 422 con panel. Input de cantidad con `min="0.0001"` | e2e `test_purchase_invalid_quantity_or_date_returns_422_panel` (cantidad 0, fecha inválida) + `test_purchase_quantity_input_blocks_zero`. **Mutation-test**: sin la comprobación, el caso de cantidad 0 falla |
| H-102 | `product_from_invoice` usa `ProductService.create_for_purchase` con el negocio activo, comprueba existencia por negocio (`PurchaseRepository.get_product_by_sku`), commit por línea con `rollback` en ambos `except`; mantiene unidad `u` y precio de venta vacío; los creados entran en el datalist | Unit `test_ui_product_from_invoice.py`: negocio correcto, línea con SKU de otro negocio falla sin bloquear el resto, ningún producto sin negocio |
| H-103 | `log_event(..., isolated=True)`: flush dentro de un `SAVEPOINT`; si falla, solo se pierde el evento. Opt-in: el resto de la app no cambia. Usado en los `product_create` de compras e importación | Unit `test_audit_isolated.py` (SQLite) + verificación contra PostgreSQL de dev con `rollback` final |

**Estado de datos en producción:** 0 productos con `business_id` nulo (consulta de solo lectura): H-102 no llegó a generar datos.

**Verificación:** 323 unit (cobertura 85,53 %) + 39 e2e verdes.

### Scorecard tras la remediación

| # | Dimensión | Peso | Ronda 3 | Tras remediación | Ponderado |
|---|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 3 | 3 | 30 |
| 2 | Cohesión / Acoplamiento | 10 | 3 | 3 | 30 |
| 3 | Capas / Límites | 10 | 3 | 4 | 40 |
| 4 | Seguridad | 15 | 3 | 4 | 60 |
| 5 | Robustez | 15 | 3 | 4 | 60 |
| 6 | Consistencia transaccional | 10 | 4 | 5 | 50 |
| 7 | Observabilidad | 10 | 4 | 4 | 40 |
| 8 | Configuración | 5 | 5 | 5 | 25 |
| 9 | Pruebas | 10 | 3 | 4 | 40 |
| 10 | Escalabilidad | 5 | 4 | 4 | 20 |
| | **Total** | **100** | **3.35** | | **395 → 3.95 🟡** |

Se queda por debajo de 4.2 por la deuda que se dejó a propósito para un PAE aparte:

### Pendiente (PAE aparte)
- **H-104** — no finitos en la raíz (`parse_optional_float`, schemas, `ProductService.create/update`)
- **H-105** — mensaje "el SKU existe en otro negocio"
- **H-106** — bucle SKU duplicado (`create` / `create_for_purchase`), refresco del datalist duplicado, `list_sku_prefixes` a `ProductRepository`
- **H-108** — el modal de edición pierde lo tecleado al mostrar el error
- **H-109** — `product_options` sin uso en 9 respuestas de compras
- **H-012** — datalist limitado a los 200 productos más antiguos (decisión de producto)
