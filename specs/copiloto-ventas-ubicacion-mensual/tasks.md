# Task Envelopes — Copiloto: ventas por punto de venta mes a mes

> SDD Phase 3 · PAE-015 · Fuente: `requirements.md` (REQ-1..6), `design.md` (D1..D8)
> Regenerado 2026-10-23 sobre `feature/frontend-modular` (tras la fase 2 del frontend, PAE-017): el gráfico
> vive en `app/static/js/components/widget-chart.js` (T3, T7), sin bump de versión (release 0.5.0, T8),
> series agrupadas por id de ubicación (T1a, T1b).

**Comando de tests unitarios (todas las verificaciones):**
`SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest <ruta> --no-cov -q`
(abreviado abajo como `PYTEST <ruta>`).

**Decisión de herramienta e2e:** el proyecto no usa `behave`; su e2e es **Playwright** sobre
uvicorn real + SQLite temporal (`tests/e2e/conftest.py`). REQ-3 (usuario ve 3 barras por mes)
depende del render real de Chart.js en el navegador → **Playwright** (T7). REQ-1/2/4/5/6 se
verifican completos en la capa de servicio/grafo → unit tests. REQ-4 contra el LLM real → golden
case del eval (T6, a demanda; el LLM no se puede invocar en CI).

**Trazabilidad:** REQ-1→T1a,T1b · REQ-2→T2 · REQ-3→T3,T7 · REQ-4→T4,T6 · REQ-5→T4,T5 · REQ-6→T5 · versión→T8 (sin cambio) · docs→T9

**Orden y batches:** [T1a, T3, T4] (independientes) → [T1b (dep. T1a), T2 (dep. T1a), T5] → [T6 (dep. T2, T4), T7 (dep. T3)] → [T8, T9]

---

### T1a: `ReportingService.sales_by_location_monthly` (código)

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** app/services/reporting_service.py
**Pillars:** P1, P3, P4, P5, P6, P8

#### Context

```python
# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
from sqlalchemy import and_, case, func, select          # ya importados
from app.models import InventoryLot, Location, InventoryMovement, ...  # ya importados
class ReportingService:
    self._db: Session; self._business_id: int
    def _month_series(self, months: int) -> tuple[datetime, list[str]]:
        """(inicio del mes más antiguo UTC, ['YYYY-MM'] de N meses incluyendo el actual)"""
```

#### Objective

Añadir el método que devuelve ventas por ubicación y por mes (REQ-1).

#### Instructions

**Step 1:** `Read app/services/reporting_service.py`

**Step 2a — constante de módulo:**
```python
# BEFORE:
from app.utils import month_range as utils_month_range


class ReportingService:
# AFTER:
from app.utils import month_range as utils_month_range

# Ventana máxima (meses) de sales_by_location_monthly: acota lo que pida el LLM.
_MAX_MONTHS_WINDOW = 24


class ReportingService:
```

**Step 2b — método nuevo** (el `return` de `purchases_by_month` + la línea siguiente):
```python
# BEFORE:
        return [{"month": k, "total": by_month.get(k, 0.0)} for k in keys]

    def sales_by_product(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> tuple[float, list[dict]]:
# AFTER:
        return [{"month": k, "total": by_month.get(k, 0.0)} for k in keys]

    def sales_by_location_monthly(self, months: int = 6) -> dict:
        """Ventas (importe) por ubicación y por mes en N meses calendario incluyendo el actual.

        Devuelve {"months": ["YYYY-MM", ...], "series": [{"location": str, "values": [float, ...]}]}.
        Solo ubicaciones con al menos una venta en la ventana, ordenadas por position y nombre.
        """
        months = max(1, min(int(months), _MAX_MONTHS_WINDOW))
        start, keys = self._month_series(months)
        location_expr = func.coalesce(Location.name, Location.code, "Sin ubicación")
        rows = self._db.execute(
            select(
                InventoryMovement.movement_date,
                InventoryMovement.location_id,
                location_expr,
                func.coalesce(Location.position, 9999),
                func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0),
            )
            .select_from(InventoryMovement)
            .outerjoin(Location, Location.id == InventoryMovement.location_id)
            .where(
                InventoryMovement.business_id == self._business_id,
                InventoryMovement.type == "sale",
                InventoryMovement.movement_date >= start,
            )
        ).all()
        valid = set(keys)
        # Clave = id de ubicación (None = venta sin ubicación): dos ubicaciones con el mismo
        # nombre son dos series distintas.
        by_location: dict[Optional[int], dict[str, float]] = {}
        meta: dict[Optional[int], tuple[int, str]] = {}
        for dt, loc_id, name, position, amount in rows:
            key = dt.strftime("%Y-%m")
            if key not in valid:
                continue
            bucket = by_location.setdefault(loc_id, {})
            bucket[key] = bucket.get(key, 0.0) + float(amount or 0)
            meta[loc_id] = (int(position), name)
        ids = sorted(by_location, key=lambda i: (meta[i][0], meta[i][1], i or 0))
        return {
            "months": keys,
            "series": [
                {"location": meta[i][1], "values": [round(by_location[i].get(k, 0.0), 2) for k in keys]}
                for i in ids
            ],
        }

    def sales_by_product(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> tuple[float, list[dict]]:
```

**Edge cases (resueltos):** venta sin ubicación → serie "Sin ubicación" (clave `None`); `position=0` (central)
se respeta (el `coalesce` es SQL, no `or`); dos ubicaciones con el mismo nombre → dos series; venta con fecha futura
(mes fuera de `keys`) → se ignora; `unit_price` NULL → importe 0; sin try/except en el método
(mismo contrato que `purchases_by_month`; `_execute_node` captura los errores).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "from app.services.reporting_service import ReportingService, _MAX_MONTHS_WINDOW; assert _MAX_MONTHS_WINDOW == 24 and hasattr(ReportingService, 'sales_by_location_monthly'); print('OK')"
PYTEST tests/unit/test_reporting_service.py
# Expected: OK + los tests existentes PASS
```

#### Expected Report
- [ ] `app/services/reporting_service.py` — constante + método nuevo, nada más · verificación OK

---

### T1b: Tests de `sales_by_location_monthly`

**Depends on:** T1a (`ReportingService.sales_by_location_monthly(months: int = 6) -> {"months": list[str], "series": [{"location", "values"}]}`, acota months a 1..24)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** tests/unit/test_reporting_service.py
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/conftest.py — fixtures db_session (SQLite memoria, negocio "test") y business_id
# app.models: Business(code, name), Location(business_id, code ÚNICO global, name, position),
#   InventoryMovement(business_id, product_id, location_id, type, quantity, unit_cost, unit_price, movement_date),
#   Product(business_id, sku, name, min_stock, unit_of_measure, default_purchase_cost, default_sale_price, lead_time_days)
```

#### Objective

Cubrir REQ-1: ventana y orden, aislamiento por negocio, ubicaciones con el mismo nombre y acotado de `months`.

#### Instructions

**Step 1:** `Read tests/unit/test_reporting_service.py`

**Step 2 — imports:**
```python
# BEFORE:
from datetime import datetime, timezone

from app.models import InventoryMovement, OperatingExpense, Product
# AFTER:
from datetime import datetime, timedelta, timezone

from app.models import Business, InventoryMovement, Location, OperatingExpense, Product
```
Añadir al final del archivo:
```python
def _product(db, business_id, sku):
    p = Product(business_id=business_id, sku=sku, name=sku, min_stock=0, unit_of_measure="ud",
                default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db.add(p)
    db.commit()
    return p


def _sale(db, business_id, product_id, location_id, qty, price, when):
    db.add(InventoryMovement(business_id=business_id, product_id=product_id, location_id=location_id,
                             type="sale", quantity=-qty, unit_cost=1.0, unit_price=price, movement_date=when))


def test_sales_by_location_monthly(db_session, business_id):
    now = datetime.now(timezone.utc)
    p = _product(db_session, business_id, "RSLM-SKU")
    a = Location(business_id=business_id, code="RSLM-A", name="Tienda A", position=2)
    b = Location(business_id=business_id, code="RSLM-B", name="Tienda B", position=1)
    c = Location(business_id=business_id, code="RSLM-C", name="Tienda C", position=3)
    db_session.add_all([a, b, c])
    db_session.commit()
    _sale(db_session, business_id, p.id, a.id, 2, 10.0, now)
    _sale(db_session, business_id, p.id, b.id, 1, 5.0, now)
    _sale(db_session, business_id, p.id, a.id, 3, 10.0, now - timedelta(days=250))
    db_session.add(InventoryMovement(business_id=business_id, product_id=p.id, location_id=a.id,
                                     type="purchase", quantity=4, unit_cost=2.0, unit_price=None, movement_date=now))
    db_session.commit()

    result = ReportingService(db_session, business_id).sales_by_location_monthly(months=6)

    assert len(result["months"]) == 6
    assert result["months"][-1] == now.strftime("%Y-%m")
    assert [s["location"] for s in result["series"]] == ["Tienda B", "Tienda A"]
    assert result["series"][0]["values"] == [0.0] * 5 + [5.0]
    assert result["series"][1]["values"] == [0.0] * 5 + [20.0]


def test_sales_by_location_monthly_scoped_to_business(db_session, business_id):
    now = datetime.now(timezone.utc)
    other = Business(code="rslm-other", name="Other")
    db_session.add(other)
    db_session.commit()
    p = _product(db_session, other.id, "RSLM-OTHER")
    loc = Location(business_id=other.id, code="RSLM-O", name="Otra")
    db_session.add(loc)
    db_session.commit()
    _sale(db_session, other.id, p.id, loc.id, 1, 99.0, now)
    db_session.commit()

    result = ReportingService(db_session, business_id).sales_by_location_monthly(months=1)

    assert result == {"months": [now.strftime("%Y-%m")], "series": []}


def test_sales_by_location_monthly_same_name_locations_are_separate(db_session, business_id):
    now = datetime.now(timezone.utc)
    p = _product(db_session, business_id, "RSLM-DUP")
    x = Location(business_id=business_id, code="RSLM-X1", name="Tienda", position=1)
    y = Location(business_id=business_id, code="RSLM-X2", name="Tienda", position=2)
    db_session.add_all([x, y])
    db_session.commit()
    _sale(db_session, business_id, p.id, x.id, 1, 3.0, now)
    _sale(db_session, business_id, p.id, y.id, 1, 4.0, now)
    db_session.commit()

    series = ReportingService(db_session, business_id).sales_by_location_monthly(months=1)["series"]

    assert series == [{"location": "Tienda", "values": [3.0]}, {"location": "Tienda", "values": [4.0]}]


def test_sales_by_location_monthly_clamps_months(db_session, business_id):
    service = ReportingService(db_session, business_id)
    assert len(service.sales_by_location_monthly(months=0)["months"]) == 1
    assert len(service.sales_by_location_monthly(months=500)["months"]) == 24
```

#### Verification

```bash
PYTEST tests/unit/test_reporting_service.py
# Expected: todos PASS (incluye los 4 tests nuevos)
```

#### Expected Report
- [ ] `tests/unit/test_reporting_service.py` — imports + 2 helpers + 4 tests · PASS
- [ ] Ningún otro archivo modificado

---

### T2: Tool `sales_by_location_monthly`

**Depends on:** T1a (`ReportingService.sales_by_location_monthly(months) -> {"months", "series"}`)
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/tools.py, tests/unit/test_agent_tools.py
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/tools.py — YA EXISTE
from app.services.reporting_service import ReportingService   # ya importado
def tool(name: str, description: str, params: dict) -> Callable  # decorador, registra en TOOL_REGISTRY
# service: InventoryService con .db y .business_id
```

#### Objective

Registrar la tool que expone T1 como payload multi-serie + tabla (REQ-2).

#### Instructions

**Step 1:** `Read app/agent/tools.py` (fin de la función `sales_by_location`, ~línea 250)

**Step 2:**
```python
# BEFORE:
        "columns": ["Ubicación", "Cantidad", "Ventas"],
        "rows": [[(r[1] or r[0]), float(r[2]), float(r[3])] for r in rows],
    }
# AFTER:
        "columns": ["Ubicación", "Cantidad", "Ventas"],
        "rows": [[(r[1] or r[0]), float(r[2]), float(r[3])] for r in rows],
    }


@tool("sales_by_location_monthly",
      "Ventas de cada ubicación/punto de venta mes a mes, una serie por ubicación (months opcional, "
      "default 6, máx 24: últimos N meses calendario incluyendo el actual). Para barras agrupadas por mes.",
      {"type": "object", "properties": {"months": {"type": "integer"}}})
def sales_by_location_monthly(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    data = reporting.sales_by_location_monthly(months=int(args.get("months") or 6))
    series = data["series"]
    return {
        "labels": data["months"],
        "datasets": [{"label": s["location"], "data": s["values"]} for s in series],
        "columns": ["Mes"] + [s["location"] for s in series],
        "rows": [[m] + [s["values"][i] for s in series] for i, m in enumerate(data["months"])],
    }
```
(El BEFORE es único: es el `return` de `sales_by_location`.)

**Step 3 — tests** (`tests/unit/test_agent_tools.py`):
```python
# BEFORE:
from app.models import Location
# AFTER:
from app.models import InventoryMovement, Location, Product
```
```python
# BEFORE:
    assert len(tool_schemas()) == 33
# AFTER:
    assert len(tool_schemas()) == 34
```
Añadir al final:
```python
def test_sales_by_location_monthly_payload(db_session, business_id):
    p = Product(business_id=business_id, sku="SLM-SKU", name="P", min_stock=0, unit_of_measure="ud",
                default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    loc = Location(business_id=business_id, code="SLM-X", name="Tienda X")
    db_session.add_all([p, loc])
    db_session.commit()
    db_session.add(InventoryMovement(business_id=business_id, product_id=p.id, location_id=loc.id,
                                     type="sale", quantity=-1, unit_cost=1.0, unit_price=7.5,
                                     movement_date=datetime.now(timezone.utc)))
    db_session.commit()

    result = TOOL_REGISTRY["sales_by_location_monthly"]["fn"](
        InventoryService(db_session, business_id), {"months": 2})

    month = datetime.now(timezone.utc).strftime("%Y-%m")
    assert len(result["labels"]) == 2 and result["labels"][-1] == month
    assert result["datasets"] == [{"label": "Tienda X", "data": [0.0, 7.5]}]
    assert result["columns"] == ["Mes", "Tienda X"]
    assert result["rows"][-1] == [month, 7.5]
```

**Edge cases:** `months` ausente o `0`/`None` → 6 (`or 6`); valores fuera de rango los acota T1;
`months="abc"` → `ValueError`, capturado por `_execute_node`.

#### Verification

```bash
PYTEST tests/unit/test_agent_tools.py
# Expected: todos PASS (test_tool_schemas_count con 34)
```

#### Expected Report
- [ ] `app/agent/tools.py` — 1 tool nueva tras `sales_by_location`
- [ ] `tests/unit/test_agent_tools.py` — import, conteo 34, 1 test nuevo · PASS

---

### T3: Gráfico multi-serie en `components/widget-chart.js`

**Depends on:** none
**Skill:** frontend-developer
**Agent:** coder
**Files:** app/static/js/components/widget-chart.js
**Pillars:** P1, P4, P5, P6

#### Context

```javascript
// app/static/js/components/widget-chart.js — YA EXISTE (fase 2 del frontend, PAE-017)
// partials/widget_chart.html solo declara <canvas data-js-widget-chart='{"type","title","payload"}'>;
// este módulo lo dibuja. chartConfig(spec) devuelve la config de Chart.js con UN dataset (payload.values).
// No hay test runner de JS: la verificación es `node --check` + el e2e de T7 (validado por mutación).
```

#### Objective

Si el widget es `bar` o `line` y trae `payload.datasets`, un dataset por serie con su color y leyenda
visible; en cualquier otro caso, exactamente lo de hoy (REQ-3). Pie/doughnut/radar no admiten
multi-serie (una porción por color no tiene sentido con varias series).

#### Instructions

**Step 1:** `Read app/static/js/components/widget-chart.js` (función `chartConfig`)

**Step 2a:**
```javascript
// BEFORE:
  const values = payload.values || [];
// AFTER:
  const values = payload.values || [];
  // Multi-serie (p. ej. ventas por ubicación y mes): payload.datasets = [{label, data}].
  // Solo en ejes cartesianos; pie/doughnut/radar siguen con una serie.
  const multi = (type === 'bar' || type === 'line')
    && Array.isArray(payload.datasets) && payload.datasets.length > 0;
```

**Step 2b:**
```javascript
// BEFORE:
    opts.plugins = { legend: { display: false } };
// AFTER:
    opts.plugins = { legend: { display: multi } };
```

**Step 2c:**
```javascript
// BEFORE:
  return {
    type,
    data: {
      labels,
      datasets: [{
        label: spec.title,
        data: values,
        backgroundColor,
        borderColor,
        borderWidth: 1,
        tension: 0.3,
      }],
    },
// AFTER:
  const datasets = multi
    ? payload.datasets.map((ds, i) => {
      const color = palette[i % palette.length];
      return {
        label: ds.label, data: ds.data || [], backgroundColor: color,
        borderColor: color, borderWidth: 1, tension: 0.3, fill: false,
      };
    })
    : [{
      label: spec.title,
      data: values,
      backgroundColor,
      borderColor,
      borderWidth: 1,
      tension: 0.3,
    }];

  return {
    type,
    data: {
      labels,
      datasets,
    },
```
(`options: opts, plugins: [percentPlugin]` y el resto quedan igual. La rama de una serie es la misma
config que hoy, carácter a carácter salvo la indentación.)

**Edge cases:** `datasets: []` → `multi=false`; `ds.data` ausente → `[]`; `pie` con `datasets` y sin
`values` → gráfico vacío como hoy (el prompt de T4 no lo propone); nombres de ubicación: llegan por
`tojson` al atributo y Chart.js los pinta en `<canvas>` (sin HTML).

#### Verification

```bash
node --check app/static/js/components/widget-chart.js && echo OK
grep -c "legend: { display: multi }" app/static/js/components/widget-chart.js   # Expected: 1
PYTEST tests/unit/test_frontend_js_modules.py   # Expected: todos PASS (grafo de módulos, sin console.)
# El comportamiento lo fija T7 (e2e).
```

#### Expected Report
- [ ] `widget-chart.js` — 3 cambios (multi, legend, datasets) · `node --check` OK · guardas PASS

---

### T4: Reglas del system prompt

**Depends on:** none (el nombre `sales_by_location_monthly` lo fija T2)
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/prompts.py, tests/unit/test_agent_prompts.py
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/prompts.py — YA EXISTE
SYSTEM_PROMPT_TEMPLATE = """..."""   # se usa con .format(schemas=..., today=...)
def build_system_prompt(last_tool: Optional[dict] = None) -> str
```

#### Objective

Enseñar al planner la tool nueva, la regla "últimos N meses" y la regla de honestidad (REQ-4, REQ-5).

#### Instructions

**Step 1:** `Read app/agent/prompts.py`

**Step 2a:**
```text
# BEFORE:
  * sales_by_product devuelve AMBOS (labels/values Y columns/rows Producto/Cantidad/Ventas),
    así que sirve para chart (bar/pie) o para tabla (table).
# AFTER:
  * sales_by_product devuelve AMBOS (labels/values Y columns/rows Producto/Cantidad/Ventas),
    así que sirve para chart (bar/pie) o para tabla (table).
  * sales_by_location_monthly devuelve labels/datasets (una serie por ubicación) Y
    columns/rows (Mes x ubicación): sirve para bar (barras agrupadas), line o table.
```

**Step 2b:**
```text
# BEFORE:
- Para "ventas por ubicación"/"por punto de venta" usa sales_by_location (bar/pie/table).
# AFTER:
- Para "ventas por ubicación"/"por punto de venta" usa sales_by_location (bar/pie/table).
- Para ventas por ubicación/punto de venta "por mes"/"mes a mes"/"cada mes" usa
  sales_by_location_monthly con months=N (type "bar" para barras agrupadas, "line" o
  "table"). NO uses sales_by_location para eso: solo da el total del período, sin meses.
- "Últimos N meses" son N meses calendario INCLUYENDO el actual. Si la tool acepta
  months, pasa months=N y no calcules start/end. Si solo acepta start/end, start es el
  día 1 del mes actual menos N-1 meses y end es hoy.
- Si ninguna tool da EXACTAMENTE lo que el usuario pide, dilo en reply con la forma
  "No tengo X; te muestro Y". Nunca afirmes haber generado algo distinto de lo que
  muestra el widget.
```
**PROHIBIDO** escribir `{` o `}` en el texto nuevo (el template usa `.format()`).

**Step 3 — añadir al final de `tests/unit/test_agent_prompts.py`:**
```python
def test_prompt_has_monthly_location_rules():
    prompt = build_system_prompt()
    assert "sales_by_location_monthly con months=N" in prompt
    assert "N meses calendario INCLUYENDO el actual" in prompt


def test_prompt_has_honesty_rule():
    assert "No tengo X; te muestro Y" in build_system_prompt()
```

#### Verification

```bash
PYTEST tests/unit/test_agent_prompts.py tests/unit/test_agent_tools.py::test_system_prompt_contains_current_date
# Expected: todos PASS (sin KeyError de .format)
```

#### Expected Report
- [ ] `prompts.py` — 2 inserciones, sin llaves · 2 tests nuevos · PASS

---

### T5: Título legible del widget sintetizado + aviso cuando la tool falla

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/graph.py, tests/unit/test_agent_graph.py
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/graph.py — YA EXISTE
from app.agent.tools import TOOL_REGISTRY      # dict name -> {"description", "params", "fn"}
# _execute_node pone state["error"] = f"Tool {tool_name}: {exc...}" si la tool falla
def _resolve_widget_tool_args(plan: PlanModel, widget: WidgetSpec) -> dict
def _narrate_node(state: AgentState) -> AgentState
```

#### Objective

Titular el widget sintetizado con la descripción de la tool (REQ-6) y avisar en el reply si la tool falló (REQ-5).

#### Instructions

**Step 1:** `Read app/agent/graph.py`

**Step 2a:**
```python
# BEFORE:
            widgets=[WidgetSpec(type="table", title=p.tool, tool=p.tool, tool_args=p.tool_args)],
# AFTER:
            widgets=[WidgetSpec(type="table", title=_synth_title(p.tool), tool=p.tool, tool_args=p.tool_args)],
```

**Step 2b** (insertar justo antes de `def _resolve_widget_tool_args`):
```python
def _synth_title(tool_name: str) -> str:
    """Título legible del widget sintetizado (no el nombre interno de la tool):
    la descripción hasta el primer " (" o ".", o el nombre si no hay descripción."""
    spec = TOOL_REGISTRY.get(tool_name) or {}
    desc = (spec.get("description") or "").split(" (")[0].split(".")[0].strip()
    return desc[:255] or tool_name


```

**Step 2c:**
```python
# BEFORE:
def _narrate_node(state: AgentState) -> AgentState:
    state["reply"] = state["plan"].reply
    return state
# AFTER:
_TOOL_ERROR_NOTICE = "No pude obtener los datos de este widget; revisa el rango de fechas o los filtros."


def _narrate_node(state: AgentState) -> AgentState:
    reply = state["plan"].reply
    # Si la tool del widget falló, avisar sin el detalle técnico de la excepción.
    if (state.get("error") or "").startswith("Tool "):
        reply = f"{reply}\n\n{_TOOL_ERROR_NOTICE}"
    state["reply"] = reply
    return state
```

**Step 3 — añadir al final de `tests/unit/test_agent_graph.py`:**
```python
def _run_synth(db_session, business_id, monkeypatch, description):
    registry = {"fake_tool": {"fn": lambda svc, args: {"columns": ["A"], "rows": [[1]]},
                              "description": description, "params": {}}}
    monkeypatch.setattr(g, "TOOL_REGISTRY", registry)
    monkeypatch.setattr(g.config, "RETRY_BASE_S", 0.0)
    monkeypatch.setattr(g.config, "RETRY_JITTER", 0.0)
    monkeypatch.setattr(g, "_PLANNER", _FakePlanner(
        PlanModel(tool="fake_tool", tool_args={}, widgets=[], reply="Voy a obtenerlo.")))
    result = build_graph().invoke({"db": db_session, "business_id": business_id, "message": "x", "history": []})
    return result["widgets"][0]["title"]


def test_synth_title_uses_tool_description(db_session, business_id, monkeypatch):
    assert _run_synth(db_session, business_id, monkeypatch, "Ventas de prueba (start/end ISO).") == "Ventas de prueba"


def test_synth_title_falls_back_to_tool_name(db_session, business_id, monkeypatch):
    assert _run_synth(db_session, business_id, monkeypatch, "") == "fake_tool"


def test_reply_warns_when_widget_tool_fails(db_session, business_id, monkeypatch):
    plan = PlanModel(tool="stock", tool_args={"sku": "SKU-NO-EXISTE"},
                     widgets=[WidgetSpec(type="kpi", title="Stock", tool="stock", tool_args={"sku": "SKU-NO-EXISTE"})],
                     reply="Consultando.")
    monkeypatch.setattr(g, "_PLANNER", _FakePlanner(plan))
    result = build_graph().invoke({"db": db_session, "business_id": business_id, "message": "x", "history": []})
    assert result["reply"].startswith("Consultando.")
    assert "No pude obtener los datos de este widget" in result["reply"]
    assert "SKU-NO-EXISTE" not in result["reply"]
```

**Edge cases:** errores del planner ("LLM en cooldown", errores de formato) no empiezan por
`"Tool "` → reply sin aviso; tool no registrada → `_synth_title` devuelve el nombre.

#### Verification

```bash
PYTEST tests/unit/test_agent_graph.py
# Expected: todos PASS (los 15 existentes + 3 nuevos)
```

#### Expected Report
- [ ] `graph.py` — `_synth_title`, título sintetizado, `_narrate_node` · 3 tests nuevos · PASS

---

### T6: Golden case del eval (LLM real, a demanda)

**Depends on:** T2, T4
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** tests/eval/test_agent_tool_selection_eval.py
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/eval/test_agent_tool_selection_eval.py — YA EXISTE
GOLDEN_CASES: list[dict]  # {"id", "message", "expected_tool", "expected_type": set, "expected_args"?: dict}
```

#### Objective

Añadir el caso que reproduce el bug reportado (REQ-4).

#### Instructions

**Step 1:** `Read tests/eval/test_agent_tool_selection_eval.py`

**Step 2:**
```python
# BEFORE:
    {"id": "sales_by_location", "message": "Ventas por ubicación este mes",
     "expected_tool": "sales_by_location", "expected_type": {"bar", "pie", "table"}, "date_range": "this_month"},
# AFTER:
    {"id": "sales_by_location", "message": "Ventas por ubicación este mes",
     "expected_tool": "sales_by_location", "expected_type": {"bar", "pie", "table"}, "date_range": "this_month"},
    {"id": "sales_by_location_monthly_6m",
     "message": "Diagrama de barras con las ventas de cada punto de venta mes a mes en los últimos 6 meses",
     "expected_tool": "sales_by_location_monthly", "expected_type": {"bar"}, "expected_args": {"months": 6}},
```

#### Verification

```bash
.venv/bin/python -m pytest tests/eval -m agent_eval --collect-only --no-cov -o addopts=""
# Expected: "1 test collected", exit 0 (la ejecución contra el LLM la hace Claude Code en el DoD)
grep -c "sales_by_location_monthly_6m" tests/eval/test_agent_tool_selection_eval.py   # Expected: 1
```

#### Expected Report
- [ ] 1 caso añadido · colecta OK

---

### T7: e2e Playwright — 3 barras por mes en el Copiloto

**Depends on:** T3
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** tests/e2e/test_agent_multiseries_chart_e2e.py (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/e2e/conftest.py — YA EXISTE
@pytest.fixture
def central_server() -> tuple[str, str]   # (base_url, db_path) uvicorn real + SQLite temp; admin/admin
# app.models.DashboardWidget(business_id, type, title, params: str JSON, position)
# params = {"tool", "tool_args", "payload", "unit"}; el partial pinta <canvas id="chart-{id}">
```

#### Objective

Comprobar en el navegador que un widget multi-serie dibuja 3 datasets con leyenda y que uno de una serie sigue igual (REQ-3).
El fixture `js_errors` de `tests/e2e/conftest.py` es `autouse`: una excepción de JS o una violación de CSP hace
fallar el test sin código adicional.

#### Instructions

**Step 1 — CREATE el archivo con exactamente:**
```python
from __future__ import annotations

import json
import time

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import Business, DashboardWidget, User

pytestmark = pytest.mark.e2e

MONTHS = ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"]
MULTI = {"labels": MONTHS, "datasets": [
    {"label": "Punto de venta Playa", "data": [2690.5, 1292.5, 2741.9, 2001.0, 873.0, 2834.0]},
    {"label": "Taller Boyeros", "data": [980.5, 90.0, 545.4, 1453.5, 791.2, 152.5]},
    {"label": "Taller Cerro", "data": [967.5, 976.0, 971.0, 1181.7, 413.2, 0.0]}]}
SINGLE = {"labels": ["A", "B"], "values": [1.0, 2.0]}


def _seed_widget(db_path: str, payload: dict) -> int:
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    try:
        admin = db.scalar(select(User).where(User.username == "admin"))
        bid = admin.business_id if admin is not None and admin.business_id is not None else \
            db.scalar(select(Business.id).where(Business.code == "recambios"))
        w = DashboardWidget(business_id=bid, type="bar", title="Ventas por punto de venta", position=1,
                            params=json.dumps({"tool": "sales_by_location_monthly", "tool_args": {"months": 6},
                                               "payload": payload, "unit": None}))
        db.add(w)
        db.commit()
        return int(w.id)
    finally:
        db.close()
        engine.dispose()


def _chart_info(page: Page, base_url: str, wid: int) -> dict:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined'")
    page.locator("#tab-agent").click()
    page.locator(f"#chart-{wid}").wait_for(state="attached", timeout=10000)
    # Polling con page.evaluate (NO wait_for_function): la CSP de la app no permite
    # 'unsafe-eval' y wait_for_function con reintentos evalúa strings en la página.
    js = (f"() => {{ const el = document.getElementById('chart-{wid}');"
          " const c = (el && window.Chart) ? Chart.getChart(el) : null;"
          " return c ? {n: c.data.datasets.length, labels: c.data.labels.length,"
          " legend: c.options.plugins.legend.display, names: c.data.datasets.map(d => d.label)} : null; }")
    deadline = time.time() + 10
    info = page.evaluate(js)
    while info is None and time.time() < deadline:
        time.sleep(0.2)
        info = page.evaluate(js)
    assert info is not None, "el gráfico del widget no se inicializó"
    return info


def test_multiseries_bar_chart_one_dataset_per_location(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_widget(db_path, MULTI)
    info = _chart_info(page, base_url, wid)
    assert info == {"n": 3, "labels": 6, "legend": True,
                    "names": ["Punto de venta Playa", "Taller Boyeros", "Taller Cerro"]}


def test_single_series_bar_chart_unchanged(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_widget(db_path, SINGLE)
    info = _chart_info(page, base_url, wid)
    assert info["n"] == 1 and info["labels"] == 2 and info["legend"] is False
```

#### Verification

```bash
.venv/bin/python -m pytest tests/e2e/test_agent_multiseries_chart_e2e.py -m e2e --no-cov -v
# Expected: 2 passed
```

#### Expected Report
- [ ] Archivo creado · 2 passed · ningún otro archivo modificado

---

### T8: Versión — sin cambio

**Depends on:** T1a–T7 en verde
**Skill:** release-manager
**Agent:** docs
**Files:** ninguno
**Pillars:** P5

#### Instructions

No tocar `VERSION` ni `.bumpversion.cfg`. Esta rama (`feature/frontend-modular`) es el release 0.5.0, que
acumula sus cambios sin bump por commit; la versión se fija una sola vez al publicarlo.

#### Verification
```bash
git diff --name-only 2638bf8 -- VERSION .bumpversion.cfg   # Expected: vacío
```

---

### T9: CHANGELOG

**Depends on:** T1a–T7
**Skill:** technical-writer
**Agent:** docs
**Files:** CHANGELOG.md
**Pillars:** P1, P4, P5, P6

#### Instructions

**CHANGELOG.md** — insertar justo después de la línea `## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)`
y su línea en blanco, como primeras viñetas de esa sección:
```markdown
- feat(agent): nueva tool `sales_by_location_monthly` — ventas de cada punto de venta mes a mes
  (una serie por ubicación, últimos N meses calendario incluyendo el actual). El copiloto ya puede
  dibujar barras agrupadas por mes (antes devolvía el total del período, sin meses). 34 tools.
- feat(ui): los gráficos de barras y líneas del Dashboard Dinámico aceptan varias series
  (`payload.datasets`) con leyenda (`components/widget-chart.js`).
- fix(agent): "últimos N meses" se resuelve como N meses calendario (antes el modelo tomaba N+1).
- fix(agent): el reply avisa cuando no puede dar exactamente lo pedido o cuando falla la tool, y el
  widget de rescate se titula con la descripción de la tool en vez de su nombre interno.
```
**No tocar** `CLAUDE.md` ni `.claude/CLAUDE.md`: los actualiza Claude Code al cerrar (las menciones a
"33 tools" de sus sesiones pasadas son históricas y se quedan).

#### Verification
```bash
grep -c "sales_by_location_monthly" CHANGELOG.md   # Expected: 1
git diff --name-only -- CHANGELOG.md               # Expected: CHANGELOG.md
```
