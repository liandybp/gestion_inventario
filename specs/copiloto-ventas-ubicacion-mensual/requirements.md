# Requirements — Copiloto: ventas por punto de venta mes a mes

> SDD Phase 1 · PAE-015 · 2026-10-18, regenerado 2026-10-23 · Proyecto `gestion_inventario` · rama
> `feature/frontend-modular` (release 0.5.0, sin bump de versión). Tras la fase 2 del frontend (PAE-017) el
> gráfico del widget se dibuja en `app/static/js/components/widget-chart.js`.

## Contexto (bug reportado en producción, 2026-10-18)

En el Dashboard Dinámico (negocio "Recambios"), el usuario pidió:

> "quiero un diagrama de barras donde se reflejen las ventas de cada punto de venta por mes de
> los últimos 6 meses, pero no un resumen, mes a mes, cada barra representa un punto de venta,
> o sea que en cada mes tienen que existir tres barras"

Qué devolvió el copiloto (capturas 2026-10-18 22:50):

1. Primer turno: reply "Voy a obtener los datos…" + un widget **tabla** titulado
   `sales_by_location` (nombre interno de la tool) con el total por ubicación.
2. Segundo turno ("un diagrama de barras"): widget **bar** "Ventas por Punto de Venta - Últimos
   6 Meses" con **una barra por ubicación** (total agregado, sin meses), y reply "He solicitado un
   diagrama de barras que muestra las ventas por cada punto de venta en los últimos 6 meses".

Causas raíz verificadas en código y en la BD de producción (solo lectura):

| # | Síntoma | Causa raíz | Evidencia |
|---|---------|-----------|-----------|
| C1 | Sin desglose mensual | No existe tool de ventas por ubicación × mes. `sales_by_location` (`app/agent/tools.py:215`) solo agrega por ubicación en un rango; las mensuales (`monthly_overview`, `purchases_by_month`, `expenses_by_month`) no desglosan por ubicación | `grep '@tool(' app/agent/tools.py` (33 tools) |
| C2 | Imposible pintar barras agrupadas | `chartConfig` construye siempre **un único dataset** con `payload.values` (en el diagnóstico original, el `<script>` inline de `widget_chart.html:80-95`) | `app/static/js/components/widget-chart.js` (`chartConfig`) |
| C3 | Reply afirma haber hecho lo pedido | `prompts.py` no obliga a declarar cuándo la petición no se puede cumplir exactamente | `app/agent/prompts.py:13-71` |
| C4 | "Últimos 6 meses" = 7 meses | El LLM calculó `start=2026-04-01` (abr–oct). Totales del widget (Playa 225 u / 14775.4) coinciden exactamente con ventas desde 2026-04-01; 6 meses calendario (may–oct) dan 186 u / 12432.9 | Query de producción, `inventory_movements type='sale'` |
| C5 | Título `sales_by_location` | Widget de rescate en `graph.py` (`_plan_node`, reintentos agotados con `widgets` vacío) usa `title=p.tool` | `app/agent/graph.py` — `WidgetSpec(type="table", title=p.tool, ...)` |

Datos reales esperados (Recambios, ventas en USD, `type='sale'`, `abs(quantity)*unit_price`):

| Mes | Punto de venta Playa | Taller Boyeros | Taller Cerro |
|-----|---------------------:|---------------:|-------------:|
| 2026-05 | 2690.5 | 980.5 | 967.5 |
| 2026-06 | 1292.5 | 90.0 | 976.0 |
| 2026-07 | 2741.9 | 545.4 | 971.0 |
| 2026-08 | 2001.0 | 1453.5 | 1181.7 |
| 2026-09 | 873.0 | 791.2 | 413.2 |
| 2026-10 (al 18) | 2834.0 | 152.5 | 0 |

---

## 1.1 Requisitos funcionales

### REQ-1: Datos de ventas por ubicación y por mes
**Priority:** Must Have

WHEN se solicitan las ventas por ubicación de los últimos N meses, THE SYSTEM SHALL devolver, para
cada ubicación con al menos una venta en la ventana, el importe vendido en cada uno de los N meses
calendario (incluyendo el mes actual), con 0 en los meses sin ventas.

```gherkin
Scenario: Ventas de 2 ubicaciones en la ventana de 6 meses
  GIVEN un negocio con las ubicaciones "Tienda A" (position 2) y "Tienda B" (position 1)
  AND una venta este mes en "Tienda A" de 2 unidades a 10.0
  AND una venta este mes en "Tienda B" de 1 unidad a 5.0
  AND una venta hace 250 días en "Tienda A"
  AND una compra este mes en "Tienda A"
  WHEN se piden las ventas por ubicación de los últimos 6 meses
  THEN "months" tiene 6 claves "YYYY-MM" y la última es el mes actual
  AND las series vienen en orden "Tienda B", "Tienda A" (por position)
  AND el último valor de "Tienda A" es 20.0 y el de "Tienda B" es 5.0
  AND los otros 5 valores de cada serie son 0.0
  AND la venta de hace 250 días y la compra no cuentan

Scenario: Ubicación sin ventas en la ventana no aparece
  GIVEN una ubicación "Tienda C" sin ventas
  WHEN se piden las ventas por ubicación de los últimos 6 meses
  THEN no hay ninguna serie "Tienda C"

Scenario: Aislamiento por negocio
  GIVEN ventas de otro negocio en el mismo período
  WHEN se piden las ventas por ubicación del negocio activo
  THEN las ventas del otro negocio no se suman
```

### REQ-2: Tool del copiloto `sales_by_location_monthly`
**Priority:** Must Have

WHEN el planner elige `sales_by_location_monthly` con `months=N`, THE SYSTEM SHALL devolver un
payload multi-serie `labels` (meses) + `datasets` (una serie por ubicación) y además
`columns`/`rows` (tabla Mes × ubicación).

```gherkin
Scenario: Payload multi-serie y tabla
  GIVEN una venta este mes en la ubicación "Tienda X" por 7.5
  WHEN se ejecuta la tool "sales_by_location_monthly" con {"months": 2}
  THEN "labels" tiene 2 meses
  AND "datasets" == [{"label": "Tienda X", "data": [0.0, 7.5]}]
  AND "columns" == ["Mes", "Tienda X"]
  AND "rows" tiene 2 filas, la última [<mes actual>, 7.5]

Scenario: months fuera de rango se acota
  WHEN se ejecuta la tool con {"months": 0}
  THEN "labels" tiene 1 mes
  WHEN se ejecuta la tool con {"months": 500}
  THEN "labels" tiene 24 meses
```

### REQ-3: Gráfico de barras agrupadas (multi-serie) en el dashboard
**Priority:** Must Have

WHEN un widget `bar` o `line` tiene `payload.datasets` no vacío, THE SYSTEM SHALL dibujar una
serie por dataset (barras agrupadas por mes), cada una con su color de la paleta, y mostrar la
leyenda. Los widgets con un solo `values` se siguen dibujando exactamente igual que hoy.

```gherkin
Scenario: El usuario ve 3 barras por mes
  GIVEN el admin tiene en su dashboard un widget bar con 6 meses y 3 datasets
    ("Punto de venta Playa", "Taller Boyeros", "Taller Cerro")
  WHEN abre la pestaña Copiloto
  THEN el gráfico del widget tiene 3 datasets y 6 etiquetas de mes
  AND la leyenda está visible

Scenario: Widgets de una serie no cambian
  GIVEN un widget bar con payload {"labels": [...], "values": [...]}
  WHEN se dibuja
  THEN tiene exactamente 1 dataset y la leyenda oculta
```

### REQ-4: El planner sabe usar la tool nueva y resuelve "últimos N meses" a N meses
**Priority:** Must Have

WHEN el usuario pide ventas por punto de venta "por mes" / "mes a mes", THE SYSTEM SHALL indicar en
el prompt que se use `sales_by_location_monthly` con `months=N`, y que "últimos N meses" son N
meses calendario incluyendo el actual.

```gherkin
Scenario: Reglas presentes en el system prompt
  WHEN se construye el system prompt
  THEN contiene "sales_by_location_monthly"
  AND contiene la regla "N meses calendario INCLUYENDO el actual"

Scenario: Eval contra el LLM real (a demanda, no gate)
  GIVEN el LLM configurado
  WHEN el usuario escribe "Diagrama de barras con las ventas de cada punto de venta mes a mes en los últimos 6 meses"
  THEN el plan usa la tool "sales_by_location_monthly", widget "bar" y months == 6
```

### REQ-5: Honestidad del reply
**Priority:** Should Have

WHEN ninguna tool produce exactamente lo pedido, THE SYSTEM SHALL decirlo en el reply ("No tengo
X; te muestro Y") en vez de afirmar haberlo hecho; y WHEN la tool del widget falla, THE SYSTEM
SHALL añadir al reply un aviso genérico de que no se pudieron obtener los datos.

```gherkin
Scenario: Regla de honestidad en el prompt
  WHEN se construye el system prompt
  THEN contiene "No tengo X; te muestro Y"

Scenario: Aviso cuando la tool falla
  GIVEN un plan cuyo widget usa la tool "stock" con un SKU inexistente
  WHEN el grafo se ejecuta
  THEN el reply empieza con el reply del plan
  AND contiene "No pude obtener los datos de este widget"
  AND no contiene el detalle técnico de la excepción
```

### REQ-6: Título legible en el widget sintetizado
**Priority:** Should Have

WHEN el planner agota los reintentos con `widgets` vacío y se sintetiza un widget, THE SYSTEM SHALL
titularlo con el primer tramo de la descripción de la tool (ej. "Ventas agrupadas por
ubicación/punto de venta"), y con el nombre de la tool solo si no tiene descripción.

```gherkin
Scenario: Título desde la descripción
  GIVEN una tool registrada "fake_tool" con descripción "Ventas de prueba (start/end ISO)."
  AND el planner devuelve siempre tool="fake_tool" con widgets vacío
  WHEN el grafo agota los reintentos
  THEN el widget sintetizado se titula "Ventas de prueba"

Scenario: Sin descripción
  GIVEN la tool "fake_tool" con descripción ""
  THEN el widget sintetizado se titula "fake_tool"
```

## 1.2 MoSCoW

| REQ | Prioridad |
|-----|-----------|
| REQ-1, REQ-2, REQ-3, REQ-4 | Must Have (4/6) |
| REQ-5, REQ-6 | Should Have (2/6) |
| Desglose mensual por ubicación de **ganancia** (profit) | Won't Have (esta iteración) |

## 1.3 NFRs

| Categoría | Respuesta |
|-----------|-----------|
| **Performance** | Una sola query (`inventory_movements` filtrada por `business_id`, `type`, `movement_date` → índice compuesto existente `(business_id, type, movement_date)`) + agregación en Python, igual que `purchases_by_month`. Volumen actual: ~600 ventas/año en Recambios. Ventana máx. 24 meses |
| **Security** | Sin endpoints nuevos ni cambio de auth: la tool corre dentro de `POST /ui/agent/chat` (admin/owner) con `InventoryService(db, business_id)` del negocio activo. Nombres de ubicación van al payload vía `tojson` (Jinja escapa `<>&'`) y Chart.js los pinta en canvas → sin XSS. `months` acotado a 1..24 |
| **Scalability** | No aplica — ventana acotada a 24 meses por negocio |
| **Observability** | Ya cubierta: `agent_plan` loguea tool/tool_args; los errores de tool quedan en `state["error"]`. Sin logs nuevos |
| **Reliability** | Si la tool falla, `_execute_node` ya captura la excepción (payload vacío) y REQ-5 avisa en el reply |

## 1.4 Fuera de alcance

- Desglose mensual por ubicación de ganancia/margen (`profit_by_location` por mes).
- Filtro por ubicación concreta en la tool nueva (siempre devuelve todas las que vendieron).
- Routing en dos pasos o few-shot del planner (hallazgo H-003 de `docs/auditorias/2026-09-21-agent-copiloto.md`).
- Cambiar el `type` del widget sintetizado (sigue siendo `table`).
- Migrar los widgets ya guardados en producción (el widget erróneo lo borra el usuario con ✕).
- Cambiar `sales_by_location` (sigue igual para totales por ubicación).

## 1.5 Dependencias

- Ninguna externa nueva. Chart.js ya cargado en `dashboard.html` (`chart.umd.min.js`).
- Sin cambios de esquema de BD.

## 1.6 Preguntas abiertas

Ninguna. El usuario fijó el comportamiento esperado (3 barras por mes, una por punto de venta, 6
meses). "Últimos N meses = N meses calendario incluyendo el actual" sigue la convención existente
de `ReportingService._month_series`. Se pasa a la Fase 2.
