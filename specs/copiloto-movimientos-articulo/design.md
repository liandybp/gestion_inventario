# Design — Copiloto: movimientos de un artículo en un período (`product_movements`)

Requisitos: `requirements.md` (REQ-1..10). Rama base `develop` `9655c8c`.

## 2.1 Anclaje en el código (verificado 2026-11-02 sobre `9655c8c`)

### Lo que se reutiliza

- `app/repositories/inventory_repository.py::InventoryRepository.movement_history(sku, query, movement_type,
  location_id, start_date, end_date, limit)`. Hace un `join` con `Product`, filtra por `business_id`, ordena
  `movement_date DESC, id DESC`, `limit=None` = sin límite. Filas `MovementHistoryRow(id, movement_date, type,
  location_id, sku, name, unit_of_measure, quantity, unit_cost, unit_price, note, username)`. **Ojo:** para
  `purchase`, `unit_price` es `Product.default_sale_price` (expresión `case`), no un precio del movimiento: el coste
  de una compra se toma de `unit_cost`.
- `InventoryRepository.stock_for_product_id(product_id, location_id=None)` = `sum(InventoryLot.qty_remaining)`. Es
  el stock que muestra la app.
- `InventoryService.transfer_codes_from_out_note(note) -> (from_code, to_code, ref)`. Interpreta
  `Transfer <from>-><to> ref=…` y lanza `HTTPException(409)` si la nota no tiene ese formato.
- `ProductService.resolve_sku(value)`: SKU, "SKU - nombre" o nombre exacto, 422 si está vacío y 404 si no existe.
  `ProductRepository.get_by_sku(sku)` (del negocio).
- `app/agent/tools.py`: `@tool(name, description, params)`, `_iso`, `_parse_end` (fin inclusivo) y
  `_resolve_location_id(service, location)` (código o nombre, sin autocrear, 404). Las tools reciben
  `(service: InventoryService, args: dict)`; `service.db` y `service.business_id` existen.
- `app/agent/graph.py::_execute_node`: una `HTTPException` de la tool queda en `state["error"]` con el texto
  `Tool <name>: <detail>` y el widget recibe `{}` (aviso de fallo, PAE-015). El `payload` del widget es el `dict`
  que devuelve la tool, tal cual (`_attach_payloads`). Se guarda en `DashboardWidget.params` y **no se recalcula**
  al pintar (`widgets_with_payload`).
- `app/templates/features/agent/widget_table.html`: pinta `payload.columns` y `payload.rows`, más una fila "Sin
  resultados para el rango solicitado." si no hay filas. Hoy no tiene nota al pie.
- `app/agent/prompts.py`: reglas de enrutado en `SYSTEM_PROMPT_TEMPLATE`. La línea 63-64 manda "historial de
  movimientos/traspasos/ajustes" a `movement_history`.
- `tests/eval/test_agent_tool_selection_eval.py`: casos `{"id", "message", "expected_tool", "expected_type",
  "expected_args"}`. Referencia actual: 96/96/92 %.

### Datos reales (BD de dev, solo lectura, 2026-11-02)

| Dato | Valor | Consecuencia |
|---|---|---|
| Tipos y signo | `purchase` 829 (+), `sale` 760 (todas −), `transfer_out` 1042 (todas −), `transfer_in` 1126 (+), `adjustment` 236 (80 −), `return_supplier` 0 | El saldo es la suma con signo, sin conversiones. |
| `transfer_in` vs `transfer_out` | 1126 vs 1042 | Una salida genera **una entrada por lote consumido** (`inventory_service.py:1676`, `quantity=a_qty`). Agrupar por `out_id`, no emparejar 1:1. |
| `transfer_in` sin `out_id` | 97 (nota `Transfer in from CENTRAL lot=…`) | Fila propia; el origen sale de "Transfer in from <code>". |
| Nota de salida | `Transfer ROPA_POS1->ROPA_CENTRAL ref=TP-…` | `transfer_codes_from_out_note` sirve. |
| Nota de entrada | `Transfer in from ROPA_POS1 out_id=4611 ref=… lot=…` | Regex `\bout_id=(\d+)` y `Transfer in from\s+([^\s:;]+)`. |
| Suma de movimientos vs lotes | 1 de 1.294 combinaciones artículo×ubicación difiere | El saldo por movimientos es fiable; REQ-7 avisa del caso raro. |
| Fechas | Desde 1970-01-01 (stock inicial importado) hasta hoy | El saldo inicial suma también esos movimientos. |
| Volumen | Máx. 64 movimientos por artículo en toda la historia | El límite de 500 filas no se alcanza hoy. |

## 2.2 Enfoque

La lógica (agrupar traspasos, saldos, nota) va a un método de **`ReportingService`**, que ya consulta el ORM
directamente y vive en la capa de servicios. La tool solo resuelve argumentos y llama al servicio.

1. **Resolución (tool):** `sku` con `ProductService.resolve_sku`, después `ProductRepository.get_by_sku` para el
   `product_id`; `location` con `_resolve_location_id`; fechas con `_iso`/`_parse_end` o los valores por defecto de
   REQ-6.
2. **Datos (servicio):**
   - `movement_history(sku=…, location_id=…, start_date=start, end_date=end, limit=None)`, invertido a orden ascendente.
   - `quantity_before(product_id, start, location_id)` (nuevo) para el saldo inicial.
   - `LocationRepository.labels()` (nuevo) para pasar de id y código a nombre.
3. **Agrupación de traspasos:**
   - **Sin ubicación:** cada `transfer_out` es una fila, y se le suman los `transfer_in` de la lista cuyo `out_id`
     apunta a él. Un `transfer_in` cuyo `out_id` no está en la lista, o que no tiene `out_id`, va como fila propia.
   - **Con ubicación:** no se agrupa. Cada movimiento de la ubicación es una fila.
4. **Saldo:** `saldo = inicial` y después, por cada fila, `saldo += suma de las cantidades de sus movimientos`.
   Todas las cantidades cuentan, también las de las entradas agrupadas: así el saldo final es exactamente
   `inicial + Σ período`.
5. **Límite (REQ-6):** si hay más de 500 filas, se muestran las 500 primeras y se añade la nota. Los saldos de esas
   filas siguen siendo correctos porque se calculan en orden.
6. **Comprobación (REQ-7):** si `end >= ahora`, se compara el saldo final con
   `stock_for_product_id(product_id, location_id)`.

### Invariantes

- Solo lectura: ningún `add`, `delete` ni `commit`.
- Todo filtrado por el negocio activo (`business_id` del servicio y de los repositorios).
- Ventana única: el saldo inicial usa `movement_date < start` y las filas `start <= movement_date < end` (la misma
  frontera de `movement_history`). La comprobación contra lotes solo se hace cuando la ventana llega a hoy.
- `movement_history` y su tool no cambian.

## 2.3 Componentes

### `app/repositories/inventory_repository.py` (MODIFY)

```python
def quantity_before(self, product_id: int, before: datetime, location_id: Optional[int] = None) -> float:
    """Suma con signo de las cantidades del artículo con movement_date < before (saldo inicial)."""
    stmt = select(func.coalesce(func.sum(InventoryMovement.quantity), 0)).where(
        InventoryMovement.business_id == self._business_id,
        InventoryMovement.product_id == int(product_id),
        InventoryMovement.movement_date < before,
    )
    if location_id is not None:
        stmt = stmt.where(InventoryMovement.location_id == int(location_id))
    return float(self._db.scalar(stmt) or 0)
```

### `app/repositories/location_repository.py` (MODIFY)

`labels(self) -> tuple[dict[int, str], dict[str, str]]`: una consulta `select(Location.id, Location.code,
Location.name).where(Location.business_id == self._business_id)`; devuelve `({id: name}, {code: name})`. El nombre
vacío cae al código.

### `app/services/reporting_service.py` (MODIFY)

Constantes de módulo: `_OUT_ID_RE = re.compile(r"\bout_id=(\d+)")`,
`_IN_FROM_RE = re.compile(r"Transfer in from\s+([^\s:;]+)")`, `PRODUCT_MOVEMENTS_MAX_ROWS = 500`,
`_TYPE_LABELS = {"purchase": "Compra", "sale": "Venta", "adjustment": "Ajuste",
"return_supplier": "Devolución a proveedor", "transfer_out": "Traspaso", "transfer_in": "Traspaso"}`.

`product_movements(self, *, product_id: int, sku: str, start: datetime, end: datetime,
location_id: Optional[int] = None) -> dict` devuelve:

```python
{
  "columns": ["Fecha", "Tipo", "Ubicación", "Cantidad", "Costo/Precio", "Saldo", "Usuario"],
  "rows": [["", "Saldo inicial", <ubicación o "Todas">, "", "", <inicial>, ""], ...],
  # "note" solo si aplica (REQ-6 / REQ-7); si hay dos avisos, separados por " "
}
```

- Fila de movimiento: `[fecha "YYYY-MM-DD HH:MM", tipo, ubicación, cantidad (float), costo/precio (float o ""),
  saldo (float redondeado a 4 decimales), usuario ("" si None)]`.
- Costo/Precio: `unit_cost` para `purchase`/`adjustment`/`return_supplier`, `unit_price` para `sale` y `""` para
  los traspasos (REQ-1).
- Ubicación:
  - movimiento normal: `names_by_id.get(location_id, "")`;
  - traspaso agrupado: `"<origen> → <destino>"`, con los códigos de `transfer_codes_from_out_note` y su nombre en
    `names_by_code`;
  - salida con ubicación filtrada: `"→ <destino>"`;
  - entrada: `"← <origen>"` (de `_IN_FROM_RE`).
  Si la nota no se puede leer (`HTTPException` de `transfer_codes_from_out_note` o regex sin coincidencia), se usa
  el nombre de la ubicación del propio movimiento. Nunca lanza.
- Cantidad de un traspaso agrupado: `abs(transfer_out.quantity)`. Con ubicación filtrada: la cantidad de BD (− o +).
- `transfer_codes_from_out_note` se llama con una instancia local de `InventoryService(self._db, self._business_id)`
  y un import dentro del método (como `_finance()`), para no crear un ciclo de imports.

### `app/agent/tools.py` (MODIFY)

```python
@tool("product_movements",
      "Movimientos de UN artículo (sku: código, 'SKU - nombre' o nombre exacto) en un período: compras, ventas, "
      "traspasos, ajustes y devoluciones, con saldo inicial y saldo acumulado. start/end ISO opcionales "
      "(por defecto los últimos 3 meses); location opcional (código o nombre) para un solo punto de venta.",
      {"type": "object", "properties": {"sku": {"type": "string"}, "start": {"type": "string"},
       "end": {"type": "string"}, "location": {"type": "string"}}, "required": ["sku"]})
def product_movements(service: InventoryService, args: dict) -> dict:
    ...
```

- Sin `sku`: el `HTTPException(422, "product is required")` que lanza `resolve_sku` con valor vacío.
- `start` por defecto: `(hoy.replace(day=1) - 2 meses)` a las 00:00 UTC; `end` por defecto `_parse_end(hoy ISO)`.
- Devuelve el `dict` del servicio sin tocarlo.

### `app/templates/features/agent/widget_table.html` (MODIFY)

Tras `</table>`: `{% if w.payload.get('note') %}<p class="muted">{{ w.payload.get('note') }}</p>{% endif %}`.
`muted` ya existe en el CSS; nada de `style=` (lo prohíbe `test_frontend_css.py`).

### `app/agent/prompts.py` (MODIFY)

Una regla nueva antes de la de `movement_history` (línea 63): "Para "movimientos"/"historial"/"entradas y
salidas"/"kárdex" de UN artículo concreto (código o nombre) usa product_movements (table) con sku=<código> y
start/end del período pedido; si pide un punto de venta, pasa location. movement_history es para los últimos
movimientos SIN artículo concreto."

### Tests

| Fichero | Qué cubre |
|---|---|
| `tests/unit/test_reporting_product_movements.py` (CREATE) | REQ-1..3 y REQ-6/7 con datos sembrados: orden, etiquetas, saldo total y por ubicación, traspaso con dos entradas = una fila, entrada sin `out_id` como fila propia, movimiento anterior al período en el saldo inicial, otro negocio invisible, nota de descuadre (lote modificado a mano), límite de 500 (con `PRODUCT_MOVEMENTS_MAX_ROWS` parcheado a 2). |
| `tests/unit/test_agent_tools.py` (MODIFY) | La tool: SKU inexistente → `HTTPException` 404; ubicación inexistente → 404; fechas por defecto (3 meses). |
| `tests/unit/test_repositories.py` (MODIFY) | `quantity_before` (frontera `<`, por ubicación, aislamiento por negocio) y `LocationRepository.labels`. |
| `tests/unit/test_ui_xss_escapes.py` (MODIFY) | `note` hostil (`<script>`) sale escapada en `widget_table.html`. |
| `tests/e2e/test_agent_product_movements_e2e.py` (CREATE, Lote 0) | Siembra compra + traspaso + venta, ejecuta la tool de verdad, guarda el resultado como widget (`add_widget`) y abre la pestaña del copiloto en Playwright: la tabla tiene "Saldo inicial", una fila "… → …" y el saldo final esperado; sin errores de JS/CSP (fixture `js_errors`). |
| `tests/eval/test_agent_tool_selection_eval.py` (MODIFY) | 3 casos: "movimientos del artículo SKU000190 de los últimos 3 meses", "kárdex de SKU000190 entre junio y septiembre", "movimientos de SKU000190 en Playa este mes" → `product_movements`/`table`, con `sku` (y `location` en el tercero). |

## 2.4 Contratos de API

Ninguna ruta HTTP nueva. Contrato de la tool: entrada según el schema de §2.3; salida `{"columns", "rows"[, "note"]}`.

## 2.5 Seguridad

| Comprobación | Respuesta |
|---|---|
| Frontera nueva | No. Tool de solo lectura dentro del copiloto (pestaña admin/owner; el chat ya rechaza al operador). |
| Aislamiento | `movement_history`, `quantity_before`, `labels`, `resolve_sku` y `stock_for_product_id` filtran por `business_id`. Un test siembra otro negocio con el mismo SKU (el SKU es único global, así que se usa otro SKU con el mismo nombre). |
| Inyección | Los argumentos del LLM solo llegan a filtros de SQLAlchemy y a `datetime.fromisoformat`. |
| XSS | `note` y las celdas pasan por autoescape de Jinja2; test con payload hostil. |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Dónde vive la lógica | `ReportingService.product_movements` | En la tool | Las tools son adaptadores finos; la lógica testeable sin LLM va en servicios. |
| Saldo | Suma de movimientos + aviso contra lotes | Reconstruir con lotes | Los lotes no guardan historia por fecha; con 1/1.294 descuadres, la suma es fiable. |
| Agrupar traspasos | Por `out_id` en la nota | Por `ref` | `out_id` es exacto por salida; una `ref` agrupa varias líneas (varios SKU). |
| Interpretar la nota de salida | `InventoryService.transfer_codes_from_out_note` | Regex duplicada | Un solo formato conocido; se captura su 409. |
| Límite | 500 filas | Paginación | El máximo real es 64; la tabla del widget no pagina. |

## 2.7 Migración y rollback

Sin cambios de BD. Rollback: revert del merge. Los widgets ya guardados con `product_movements` siguen pintándose
porque guardan su `payload`.

## 2.8 Contexto externo (Vía 1)

No hace falta.

## 2.9 Pre-mortem contra las 10 dimensiones

| Dimensión | Riesgo | Mitigación (texto literal para los envelopes) |
|---|---|---|
| **1. SoC** | Lógica de saldo en la tool. | La tool solo resuelve argumentos y llama a `ReportingService.product_movements`. |
| **2. Cohesión** | `transfer_codes_from_out_note` duplicado. | Se reutiliza el de `InventoryService`; solo se añaden las regex que no existen (`out_id`, "in from"). |
| **3. Capas** | Import circular `reporting_service` ↔ `inventory_service`. | Import local dentro del método, como `_finance()`. |
| **4. Seguridad** | Escape de la nota; SKU de otro negocio. | Autoescape + test hostil; `resolve_sku` del negocio + test de aislamiento. |
| **5. Robustez** | Una nota rara rompe la tabla entera. | El parseo de notas nunca lanza: `try/except HTTPException` y caída al nombre de la ubicación del movimiento. Los errores de argumentos (SKU o ubicación inexistentes) sí lanzan `HTTPException`, que ya gestiona `_execute_node`. |
| **6. Transaccional/ventanas** | Saldo inicial y filas con fronteras distintas (doble conteo o hueco en `start`). | Saldo inicial `< start`, filas `>= start` y `< end` (las de `movement_history`). Test con un movimiento exactamente en `start`. La comparación con lotes solo si `end >= ahora`. |
| **7. Observabilidad** | — | Sin logs nuevos; `_execute_node` ya registra los fallos de tool. |
| **8. Configuración** | `500` mágico. | Constante `PRODUCT_MOVEMENTS_MAX_ROWS`. |
| **9. Pruebas** | Tests que pasan sin datos (tabla vacía). | Cada test de servicio afirma filas y saldos concretos; el e2e afirma el texto de las celdas. |
| **10. Escalabilidad** | Subconsulta de usuario por fila en `movement_history`. | Ya existe y con ≤ 500 filas es aceptable. |

## 2.10 Plan de lotes para la Fase 3

| Lote | Contenido | Cierre |
|---|---|---|
| **A (Lote 0)** | `quantity_before`, `labels`, `ReportingService.product_movements`, tool `product_movements`, `test_reporting_product_movements.py` (camino feliz de REQ-1/2/3) y el e2e sembrado. | e2e nuevo verde; unit completo verde. |
| **B** | Casos de borde del servicio (entrada sin `out_id`, frontera `start`, aislamiento, límite, nota de descuadre); tests de la tool y de los repositorios. | Unit completo verde. |
| **C** | Nota en `widget_table.html` + test de escape; regla del prompt; 3 casos de eval; docs (REQ-9). | Unit + `ui_snapshot.py check` (la tabla sin nota no cambia) + eval ejecutado contra el LLM real con resultado ≥ 96/96/92 %. |

Verificación global: `pytest tests/unit`, `ui_snapshot.py check`, `pytest tests/e2e -m e2e`, eval
`pytest tests/eval -m agent_eval -v -s`.
