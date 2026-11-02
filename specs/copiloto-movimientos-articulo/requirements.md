# Requirements — Copiloto: movimientos de un artículo en un período (`product_movements`)

**Release:** 0.5.0 (agrupado, sin bump por commit). **Rama base:** `develop` `9655c8c` (fases 5a y 5b mergeadas).
**Pedido del usuario (2026-11-02):** "cuando le pida el movimiento de un artículo específico por el código en un
período de tiempo determinado (pueden ser varios meses), que me devuelva una tabla con todos los movimientos de ese
artículo en el período: compras, traspasos y ventas, en todos los puntos de venta o en un punto de venta
determinado".
**Decisiones del usuario (2026-11-02):**
1. Todos los tipos de movimiento: compra, venta, traspaso, ajuste y devolución a proveedor.
2. Fila de saldo inicial y columna de saldo acumulado.
3. En la vista de todas las ubicaciones, un traspaso es **una** fila "origen → destino". Con una ubicación
   filtrada, es una salida (−) o una entrada (+) de esa ubicación.
4. Tool nueva `product_movements`. `movement_history` no cambia.

## 1. Requisitos funcionales

### REQ-1: La tool devuelve los movimientos del artículo en el período
**Priority:** Must Have
WHEN el copiloto ejecuta `product_movements` con `sku` y un período, THE SYSTEM SHALL devolver una tabla con todos
los movimientos de ese artículo del negocio activo cuya fecha cae en el período, en orden cronológico ascendente
(fecha y después id). Entran los tipos `purchase`, `sale`, `transfer_out`, `transfer_in`, `adjustment` y
`return_supplier`.

Columnas, en este orden: `Fecha`, `Tipo`, `Ubicación`, `Cantidad`, `Costo/Precio`, `Saldo`, `Usuario`.

| Tipo en BD | `Tipo` mostrado | `Cantidad` | `Costo/Precio` |
|---|---|---|---|
| `purchase` | Compra | + | `unit_cost` |
| `sale` | Venta | − | `unit_price` |
| `adjustment` | Ajuste | ± (la de BD) | `unit_cost` si existe, si no vacío |
| `return_supplier` | Devolución a proveedor | − | `unit_cost` si existe, si no vacío |
| traspaso | Traspaso | ver REQ-3 | vacío |

```gherkin
Scenario: compra, traspaso y venta en el período
  GIVEN el artículo "SKU-M1" con una compra de 10 en CENTRAL el 2026-08-05,
        un traspaso de 4 de CENTRAL a Playa el 2026-08-10 y una venta de 1 en Playa el 2026-09-02
  WHEN el copiloto ejecuta product_movements(sku="SKU-M1", start="2026-08-01", end="2026-09-30")
  THEN la tabla tiene, en este orden, las filas Saldo inicial, Compra, Traspaso y Venta
  AND la fila de Traspaso muestra Ubicación "Almacén central → Playa" y Cantidad 4

Scenario: los movimientos fuera del período no salen como filas
  GIVEN una venta del artículo el 2026-07-31
  WHEN el período empieza el 2026-08-01
  THEN esa venta no aparece como fila y sí cuenta en el saldo inicial
```

### REQ-2: Saldo inicial y saldo acumulado
**Priority:** Must Have
WHEN se devuelve la tabla, THE SYSTEM SHALL empezar con una fila `Saldo inicial` cuyo `Saldo` es la suma de las
cantidades de todos los movimientos del artículo anteriores al inicio del período, en la ubicación filtrada o en
todas. Cada fila siguiente muestra en `Saldo` el saldo tras ese movimiento.

```gherkin
Scenario: saldo en la vista de todas las ubicaciones
  GIVEN el caso de REQ-1 y ningún movimiento anterior al 2026-08-01
  THEN los saldos son 0 (inicial), 10 (compra), 10 (traspaso) y 9 (venta)

Scenario: saldo en una ubicación
  GIVEN el mismo caso
  WHEN se filtra location="Playa"
  THEN las filas son Saldo inicial 0, Traspaso +4 con saldo 4 y Venta −1 con saldo 3
```

### REQ-3: Traspasos como una fila
**Priority:** Must Have
WHEN no se filtra ubicación, THE SYSTEM SHALL mostrar cada traspaso como una sola fila. Agrupa el `transfer_out` con
los `transfer_in` que lo referencian (`out_id=<id>` en la nota). Esa fila tiene `Ubicación` "<origen> → <destino>",
`Cantidad` igual a la cantidad trasladada en valor absoluto, y un cambio de saldo igual a la suma de sus movimientos
(normalmente 0). WHEN se filtra una ubicación, THE SYSTEM SHALL mostrar el `transfer_out` de esa ubicación como
`Traspaso` con `Ubicación` "→ <destino>" y cantidad negativa, y el `transfer_in` como `Traspaso` con "← <origen>" y
cantidad positiva. Un `transfer_in` sin `out_id` (traspasos antiguos: 97 en la BD de dev) sale siempre como fila
propia, con "← <origen>" tomado de su nota.

```gherkin
Scenario: una salida con dos entradas (dos lotes consumidos) es una fila
  GIVEN un traspaso de 5 que consumió dos lotes (transfer_out −5, transfer_in +2 y +3 con out_id del primero)
  WHEN se pide la vista de todas las ubicaciones
  THEN hay una sola fila Traspaso con Cantidad 5 y el saldo no cambia
```

### REQ-4: Filtro por ubicación
**Priority:** Must Have
WHEN se pasa `location` (código o nombre), THE SYSTEM SHALL limitar filas y saldos a esa ubicación, resolviéndola
con la misma función que las demás tools (`_resolve_location_id` de `app/agent/tools.py`: sin autocrear, 404 si no
existe). La columna `Ubicación` muestra el nombre de la ubicación, no su código.

### REQ-5: Identificación del artículo
**Priority:** Must Have
WHEN `sku` llega con un SKU, una etiqueta "SKU - nombre" o un nombre exacto, THE SYSTEM SHALL resolverlo con
`ProductService.resolve_sku` (del negocio activo). WHEN no existe, THE SYSTEM SHALL fallar con el 404 de esa función,
que el copiloto muestra como aviso de fallo de la tool (mecanismo de PAE-015).

### REQ-6: Período por defecto y límites
**Priority:** Must Have
WHEN faltan `start` o `end`, THE SYSTEM SHALL usar como inicio el día 1 del mes de hace dos meses y como fin hoy
(tres meses calendario, incluido el actual). `end` es inclusivo (`_parse_end`). WHEN hay más de 500 movimientos en
el período, THE SYSTEM SHALL devolver los 500 más antiguos con saldos correctos y la nota de REQ-7 "Se muestran los
500 primeros movimientos de N".

### REQ-7: Nota al pie de la tabla
**Priority:** Should Have
WHEN el fin del período es hoy o posterior, THE SYSTEM SHALL comparar el saldo final con el stock por lotes del
artículo (`sum(qty_remaining)`, en la ubicación o en total). WHEN difieren en más de 0,0001, THE SYSTEM SHALL añadir
al resultado `note` = "El saldo calculado (X) no coincide con el stock actual por lotes (Y)." La plantilla
`widget_table.html` muestra `payload.note` bajo la tabla cuando existe (autoescape). La BD de dev tiene 1
combinación artículo×ubicación de 1.294 donde no coinciden.

### REQ-8: El copiloto elige la tool
**Priority:** Must Have
WHEN el usuario pide "movimientos/historial/entradas y salidas/kárdex del artículo <código>", con o sin período y con
o sin punto de venta, THE SYSTEM SHALL elegir `product_movements` con `type` "table". `movement_history` sigue para
"últimos movimientos" sin artículo concreto. El eval de precisión gana al menos 3 casos y no baja de su referencia
actual (96/96/92 %).

### REQ-9: Documentación
**Priority:** Should Have
WHEN termina, THE SYSTEM SHALL tener actualizado: el recuento de tools (34 → 35) en `CLAUDE.md` y
`.claude/CLAUDE.md`, la entrada de 0.5.0 del `CHANGELOG.md`.

### REQ-10: Exportar la tabla
**Priority:** Won't Have (esta iteración). La exportación CSV es la oportunidad #6 del roadmap.

## 2. Requisitos no funcionales

| Categoría | Respuesta |
|---|---|
| **Performance** | 3 consultas por llamada (movimientos del período, suma anterior al inicio y stock por lotes) más la resolución del SKU y de la ubicación. El máximo real es de 64 movimientos por artículo en toda la historia (BD de dev). |
| **Security** | Solo lectura y siempre del negocio activo: `ProductService`/`InventoryRepository` filtran por `business_id`. La nota se pinta con autoescape. Las tools no tienen RBAC propio; la pestaña del copiloto ya es solo admin/owner. |
| **Scalability** | Límite de 500 filas por respuesta (REQ-6). |
| **Observability** | Sin logs nuevos; los fallos de la tool ya los registra `_execute_node`. |
| **Reliability** | El saldo se calcula con todos los movimientos, también los que no se muestran como fila (entradas de un traspaso agrupado), así que cuadra aunque haya entradas huérfanas. REQ-7 avisa si no cuadra con los lotes. |

## 3. Fuera de alcance

- Cambiar `movement_history` o su forma de mostrar traspasos.
- Gráfico de la evolución del saldo (la tool devuelve tabla). Puede ser una iteración posterior.
- Exportar (REQ-10). Filtrar por tipo de movimiento.
- Arreglar la combinación artículo×ubicación que no cuadra en dev (es un dato, no código).

## 4. Dependencias

- `ProductService.resolve_sku` e `InventoryService.movement_history(sku, location_id, start_date, end_date, limit)`
  (existen tras la fase 5b).
- Mecanismo de aviso de fallo de la tool del widget (PAE-015).

## 5. Preguntas abiertas

Ninguna. Las cuatro decisiones de diseño (tipos, saldo, traspasos, tool nueva) se tomaron con el usuario el
2026-11-02.
