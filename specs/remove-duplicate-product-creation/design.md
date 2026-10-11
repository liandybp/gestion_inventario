# Design — Quitar "Crear producto manualmente" de la pestaña Compras

Fuente: [requirements.md](requirements.md).

## 2.1 Codebase grounding

| Archivo | Rol | Acción |
|---|---|---|
| `app/templates/partials/product_panel.html` | Panel "Producto" (import PDF, creación manual, historial) | **MODIFY** — borrar líneas 27-39 (`<h3>Crear producto manualmente</h3>` + su `<form>` + línea en blanco) |
| `app/routers/ui_products.py` | Rutas UI de producto | **MODIFY** — borrar `create_product` (`@router.post("/product")`, líneas 37-104) y `ProductCreate` del import |
| `app/routers/ui_purchases.py` | `purchase()` — `POST /ui/purchase` | **MODIFY** — parámetro `sale_price`, validación y uso en las 2 ramas que crean `Product` |
| `app/templates/partials/purchase_panel.html` | Formulario "Registrar compra" | **MODIFY** — fila "Precio de venta (si es nuevo)" tras "Nombre (si es nuevo)" (línea 41) |
| `tests/e2e/test_product_creation_in_purchase_e2e.py` | e2e Playwright | **CREATE** |
| `VERSION` | `0.4.10` | **MODIFY** → `0.4.11` (PATCH) |
| `app/routers/ui_common.py::parse_optional_float` | `(value: Optional[str]) -> Optional[float]`; vacío o no numérico → `None` | REFERENCE |
| `app/templates/partials/products_history.html` | Columna "Precio def." = `'%.2f'|format(p.default_sale_price or 0)` | REFERENCE (verificación e2e) |
| `app/main.py` (middleware CSRF) | POST a `/ui/*` sin `HX-Request: true` → 403 | REFERENCE (e2e debe enviar la cabecera) |

## 2.2 Enfoque

1. **Plantilla:** se elimina solo el bloque de creación manual. Se conservan el import PDF,
   el aviso de productos importados, `datalist#product-options`, el bloque de mensajes,
   `#product-edit` e "Historial de productos".
2. **Ruta:** se elimina `create_product`. `ProductCreate` deja de usarse en el router →
   se quita del import (`AdjustmentCreate` y `ProductUpdate` se siguen usando).
3. **Compra:** `purchase()` recibe `sale_price: str = Form("")`. Dentro del `try` (antes de
   crear nada) se calcula `parsed_sale_price = parse_optional_float(sale_price)`; si es
   negativo → `HTTPException(422, "El precio de venta no puede ser negativo")`, que el
   `except HTTPException` existente convierte en el panel "Error en compra". En las dos
   construcciones de `Product(...)` de `purchase()` se usa
   `default_sale_price=parsed_sale_price if parsed_sale_price is not None else 0`.
   La compra de un producto existente no entra en esas ramas → REQ-4 se cumple por
   construcción.

**Invariantes a preservar:**
- `datalist#product-options` vive en `product_panel.html` y lo usa `purchase_panel.html` (`list="product-options"`).
- `purchase_from_invoice()` (línea 225, `default_purchase_cost=unit_cost_vat`) mantiene `default_sale_price=0`: fuera de alcance.
- `ensure_admin_or_owner` + `require_active_business_id` en `purchase()`; filtro multi-negocio `business_id=int(bid)`.

## 2.3 Modelo de datos

Sin cambios. `Product.default_sale_price` ya existe.

## 2.4 Contratos

### POST /ui/purchase (cambio)
- **Nuevo campo de formulario:** `sale_price` (str, opcional, por defecto `""`).
- Producto nuevo + `sale_price` vacío/no numérico → `default_sale_price=0`.
- Producto nuevo + `sale_price ≥ 0` → `default_sale_price=<valor>`.
- `sale_price < 0` → 422, panel "Error en compra", detalle "El precio de venta no puede ser negativo".
- Producto existente → `sale_price` ignorado.

### POST /ui/product (eliminado)
- Respuesta tras el cambio: 404/405 (no hay ruta POST en `/ui/product`; existen otras bajo `/ui/product/...`).

## 2.5 Seguridad

| Check | Respuesta |
|---|---|
| Nuevo límite de auth | No. `purchase()` ya exige admin/owner |
| Datos sensibles | No |
| Validación de entrada | `parse_optional_float` + rechazo de negativos en servidor; `min="0"` en el input |
| Superficie de ataque | **Disminuye**: se elimina una ruta de escritura con subida de imagen |
| OWASP | Sin cambios; CSRF por `HX-Request` sigue aplicando |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Ruta `POST /ui/product` | Eliminar | Mantener | Código muerto sin consumidores (ningún test ni plantilla) |
| Datos extra del producto | Solo precio de venta | Todos los campos del form eliminado | El form de compra se volvería largo; el resto se edita en "Historial de productos" |
| Precio negativo | 422 en servidor | Clamp a 0 | Silenciar un error de tecleo es peor que avisar |

## 2.7 Migración y rollback

Sin migración. Rollback: `git revert` del commit.

## 2.9 Pre-mortem (auditoria-ingenieria)

| Dimensión | Riesgo | Mitigación en envelopes |
|---|---|---|
| 1. SoC/SRP | Ruta y import sin consumidor tras quitar el form | T2 borra `create_product` y `ProductCreate` del import |
| 2. Cohesión | `parse_optional_float(sale_price)` calculado varias veces | T3 lo calcula una vez (`parsed_sale_price`) y lo reutiliza en ambas ramas |
| 3. Capas | Ninguno: el cambio queda en router + plantilla, mismo patrón que `unit_cost` | — |
| 5. Robustez | Validación fuera del `try` → 500 en vez de panel de error | T3 coloca la validación dentro del `try` existente, cubierta por `except HTTPException` |
| 6. Transaccional | Validar después de crear el producto dejaría un producto huérfano | T3 valida antes de cualquier `db.add` |
