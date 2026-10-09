# Task Envelopes — Diagnóstico y memoria del copiloto IA

**e2e tool decision:** ninguno de los 3 hallazgos (REQ-1/2, REQ-3, REQ-4) tiene un
criterio de aceptación de UI/flujo de usuario nuevo — son logging interno, contexto
extra del LLM (invisible en el DOM) y un test de eval que no pasa por el navegador.
El endpoint `POST /ui/agent/chat` no cambia de contrato. La suite e2e existente
(`tests/e2e/test_flows_e2e.py`, etc.) no se toca y debe seguir en verde sin
modificación — no se agrega ningún envelope de `behave`/Playwright.

**Orden de ejecución:** T1 y T2 son independientes entre sí. T3 depende de T1
(prompts.py). T4 depende de T1 y T3 (mismo archivo, edita el resultado de T3). T5
depende de T4. T6 depende de T2 y T5. T7 es independiente. T8 depende de T7 (marker
registrado). Batch sugerido: [T1, T2, T7] → [T3] → [T4] → [T5, T6] → [T8].

---

### T1: `build_system_prompt` acepta contexto de tool previa

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/prompts.py, tests/unit/test_agent_prompts.py (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/prompts.py — ESTO YA EXISTE
import json
from datetime import datetime, timezone
from app.agent.tools import tool_schemas

_TOOL_SCHEMAS_JSON = json.dumps(tool_schemas(), ensure_ascii=False)
SYSTEM_PROMPT_TEMPLATE = """..."""  # no tocar el contenido del template
```

#### Objective

Agregar un parámetro opcional `last_tool` a `build_system_prompt()` que, si se pasa,
añade un párrafo con la tool/args/type del turno anterior al final del prompt.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/agent/prompts.py
```

**Step 2: Añadir import de Optional (línea 1-5)**

```python
# BEFORE (líneas 1-6):
from __future__ import annotations

import json
from datetime import datetime, timezone

from app.agent.tools import tool_schemas

# AFTER:
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from app.agent.tools import tool_schemas
```

**Step 3: Modificar `build_system_prompt` (últimas 6 líneas del archivo)**

```python
# BEFORE:
def build_system_prompt() -> str:
    # Fecha fresca en cada llamada: el modelo necesita saber qué día es para
    # resolver fechas relativas correctamente (ej. "mes anterior" -> 2026-07,
    # no 2023-10 como salía antes por no tener contexto de fecha).
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return SYSTEM_PROMPT_TEMPLATE.format(schemas=_TOOL_SCHEMAS_JSON, today=today)

# AFTER:
def build_system_prompt(last_tool: Optional[dict] = None) -> str:
    # Fecha fresca en cada llamada: el modelo necesita saber qué día es para
    # resolver fechas relativas correctamente (ej. "mes anterior" -> 2026-07,
    # no 2023-10 como salía antes por no tener contexto de fecha).
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prompt = SYSTEM_PROMPT_TEMPLATE.format(schemas=_TOOL_SCHEMAS_JSON, today=today)
    if last_tool:
        prompt += (
            "\n\nContexto adicional: el último widget mostrado en esta conversación usó "
            f"la tool \"{last_tool['tool']}\" con tool_args "
            f"{json.dumps(last_tool['tool_args'], ensure_ascii=False)} (type \"{last_tool['type']}\"). "
            "Si el mensaje actual es una pregunta de seguimiento (ej. \"¿y por ubicación?\", "
            "\"¿y el mes pasado?\"), parte de esa tool/args como base y ajusta solo lo que "
            "el usuario pide cambiar."
        )
    return prompt
```

**Step 4: Crear `tests/unit/test_agent_prompts.py` (archivo nuevo completo)**

```python
from __future__ import annotations

from app.agent.prompts import build_system_prompt


def test_build_system_prompt_without_last_tool_has_no_context_block():
    prompt = build_system_prompt()
    assert "Contexto adicional" not in prompt


def test_build_system_prompt_with_last_tool_includes_it():
    last_tool = {"tool": "sales_by_product",
                 "tool_args": {"start": "2026-01-01", "end": "2026-01-31"}, "type": "bar"}
    prompt = build_system_prompt(last_tool=last_tool)
    assert "Contexto adicional" in prompt
    assert "sales_by_product" in prompt
    assert '"start": "2026-01-01"' in prompt
```

**Edge cases (ya resueltos):**
- `last_tool=None` (default): el prompt es idéntico al actual, sin el párrafo extra
- `last_tool={}` (dict vacío, falsy): tratado igual que `None` (el `if last_tool:` ya lo cubre)

#### Verification

```bash
pytest tests/unit/test_agent_prompts.py -v
pytest tests/unit/test_agent_graph.py -v
# Expected: todos PASS (test_agent_graph.py no debe romperse por la firma nueva,
# ya que el parámetro es opcional con default None)
```

#### Expected Report

- [ ] `app/agent/prompts.py` modificado — import + firma de `build_system_prompt`
- [ ] `tests/unit/test_agent_prompts.py` creado — 2 tests
- [ ] Ningún otro archivo modificado
- [ ] Los 2 comandos de verificación — PASS

---

### T2: `latest_widget_meta` en widgets.py

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/widgets.py, tests/unit/test_agent_widgets.py
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/widgets.py — ESTO YA EXISTE
from app.models import DashboardWidget
from sqlalchemy import func, select
from sqlalchemy.orm import Session

def list_widgets(db: Session, business_id: int) -> list[DashboardWidget]:
    return list(db.scalars(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
    ).all())

# DashboardWidget.params (Text) es JSON: {"tool": str, "tool_args": dict, "payload": dict, "unit": str|None}
```

#### Objective

Agregar `latest_widget_meta(db, business_id)` que devuelve `{"tool", "tool_args", "type"}`
del widget más reciente del negocio, o `None`.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/agent/widgets.py
```

**Step 2: Insertar la función nueva entre `list_widgets` y `widgets_with_payload`**

```python
# BEFORE:
def list_widgets(db: Session, business_id: int) -> list[DashboardWidget]:
    return list(db.scalars(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
    ).all())


def widgets_with_payload(db: Session, business_id: int) -> list[dict]:

# AFTER:
def list_widgets(db: Session, business_id: int) -> list[DashboardWidget]:
    return list(db.scalars(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
    ).all())


def latest_widget_meta(db: Session, business_id: int) -> Optional[dict]:
    """Tool/tool_args/type del último widget persistido del negocio, o None (REQ-3)."""
    widget = db.scalar(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.desc(), DashboardWidget.id.desc())
        .limit(1)
    )
    if widget is None:
        return None
    try:
        params = json.loads(widget.params or "{}")
    except (TypeError, ValueError):
        # Mismo patrón defensivo que widgets_with_payload: params corrupto no
        # debe romper el chat, solo perder el contexto de continuidad.
        return None
    tool = params.get("tool")
    if not tool:
        return None
    return {"tool": tool, "tool_args": params.get("tool_args") or {}, "type": widget.type}


def widgets_with_payload(db: Session, business_id: int) -> list[dict]:
```

**Imports needed:** ninguno nuevo — `json`, `select`, `Session`, `DashboardWidget`,
`Optional` ya están importados al inicio de `app/agent/widgets.py`.

**Edge cases (ya resueltos):**
- Negocio sin ningún widget: `db.scalar(...)` devuelve `None` → la función devuelve `None`
- `params` corrupto (no JSON válido): `try/except` devuelve `None`
- `params` válido pero sin clave `"tool"` (widget legacy o corrupto de otra forma): `tool` es `None` → devuelve `None`

**Step 3: Añadir test en `tests/unit/test_agent_widgets.py`**

```python
# BEFORE (línea 1):
from app.agent.widgets import add_widget, delete_widget, list_widgets, widgets_with_payload

# AFTER:
from app.agent.widgets import (
    add_widget, delete_widget, latest_widget_meta, list_widgets, widgets_with_payload,
)
```

Añadir al final del archivo:
```python
def test_latest_widget_meta_returns_none_without_widgets(db_session, business_id):
    assert latest_widget_meta(db_session, business_id) is None


def test_latest_widget_meta_returns_last_tool_and_args(db_session, business_id):
    add_widget(db_session, business_id, type="kpi", title="A", tool="total_sales",
              tool_args={"start": "2026-01-01"}, payload={}, unit=None, created_by=None)
    add_widget(db_session, business_id, type="bar", title="B", tool="sales_by_product",
              tool_args={"limit": 5}, payload={}, unit=None, created_by=None)
    meta = latest_widget_meta(db_session, business_id)
    assert meta == {"tool": "sales_by_product", "tool_args": {"limit": 5}, "type": "bar"}
```

#### Verification

```bash
pytest tests/unit/test_agent_widgets.py -v
# Expected: 4 tests PASS (2 existentes + 2 nuevos)
```

#### Expected Report

- [ ] `app/agent/widgets.py` modificado — función `latest_widget_meta` añadida
- [ ] `tests/unit/test_agent_widgets.py` modificado — import + 2 tests nuevos
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_widgets.py -v` — PASS

---

### T3: Logging estructurado en `_plan_node` (H-001)

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/graph.py, tests/unit/test_agent_graph.py
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/graph.py — ESTO YA EXISTE (línea 22)
_log_agent = get_logger(__name__)
# app/logger.py::get_logger ya centraliza formato JSON + request_id vía contextvars.
# Patrón de logging del repo: _log.info("msg %s", arg) — NUNCA extra={...} (el
# _JsonFormatter solo serializa record.getMessage()).
```

#### Objective

Agregar 5 puntos de log (`INFO`/`WARNING`/`ERROR`) en `_plan_node` para poder
diagnosticar post-hoc qué plan eligió el LLM en cada turno.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/agent/graph.py
```

**Step 2: Reemplazar el cuerpo completo de `_plan_node`**

```python
# BEFORE (función completa, líneas 64-151):
def _plan_node(state: AgentState) -> AgentState:
    messages = [SystemMessage(content=build_system_prompt())]
    for turn in (state.get("history") or [])[-20:]:
        content = turn.get("content", "")
        if turn.get("role") == "assistant":
            messages.append(AIMessage(content=content))
        else:
            messages.append(HumanMessage(content=content))
    messages.append(HumanMessage(content=state["message"]))
    if not breaker.allow_request():
        _log_agent.warning("LLM circuit breaker OPEN — degradando sin llamar al LLM")
        state["error"] = "LLM en cooldown"
        state["plan"] = PlanModel(
            tool=None, widgets=[],
            reply="El copiloto está temporalmente no disponible. Inténtalo en un momento.",
        )
        return state
    error: Optional[str] = None
    last_tool_without_widget: Optional[PlanModel] = None
    for attempt in range(config.MAX_RETRIES + 1):
        if attempt > 0:
            backoff = min(config.RETRY_BASE_S * (2 ** (attempt - 1)), config.RETRY_MAX_S)
            time.sleep(backoff + random.uniform(0, config.RETRY_JITTER))
        try:
            plan = _PLANNER.invoke(messages)
            breaker.record_success()
            if not isinstance(plan, PlanModel):
                raise OutputParserException(
                    f"El planner devolvió {type(plan).__name__}, se esperaba PlanModel"
                )
            if plan.tool and not plan.widgets:
                last_tool_without_widget = plan
                raise OutputParserException(
                    f"Se seleccionó la tool '{plan.tool}' pero 'widgets' quedó vacío; "
                    "todo tool_args debe reflejarse también en un widget con su "
                    "type y title."
                )
            state["plan"] = plan
            state["error"] = None
            return state
        except (OutputParserException, ValidationError) as exc:
            error = str(exc)
            messages.append(HumanMessage(content=f"Error de formato: {error}. Devuelve JSON válido."))
        except Exception as exc:
            breaker.record_failure()
            state["error"] = f"LLM no disponible: {exc}"
            state["plan"] = PlanModel(
                tool=None, widgets=[], reply="El copiloto no está disponible en este momento."
            )
            return state
    state["error"] = error
    if last_tool_without_widget is not None:
        p = last_tool_without_widget
        state["plan"] = PlanModel(
            tool=p.tool,
            tool_args=p.tool_args,
            reply=p.reply,
            widgets=[WidgetSpec(type="table", title=p.tool, tool=p.tool, tool_args=p.tool_args)],
        )
        return state
    state["plan"] = PlanModel(tool=None, widgets=[], reply="No pude interpretar tu consulta.")
    return state


# AFTER:
def _plan_node(state: AgentState) -> AgentState:
    messages = [SystemMessage(content=build_system_prompt())]
    for turn in (state.get("history") or [])[-20:]:
        content = turn.get("content", "")
        if turn.get("role") == "assistant":
            messages.append(AIMessage(content=content))
        else:
            messages.append(HumanMessage(content=content))
    messages.append(HumanMessage(content=state["message"]))
    if not breaker.allow_request():
        _log_agent.warning("LLM circuit breaker OPEN — degradando sin llamar al LLM")
        state["error"] = "LLM en cooldown"
        state["plan"] = PlanModel(
            tool=None, widgets=[],
            reply="El copiloto está temporalmente no disponible. Inténtalo en un momento.",
        )
        return state
    error: Optional[str] = None
    last_tool_without_widget: Optional[PlanModel] = None
    for attempt in range(config.MAX_RETRIES + 1):
        if attempt > 0:
            backoff = min(config.RETRY_BASE_S * (2 ** (attempt - 1)), config.RETRY_MAX_S)
            time.sleep(backoff + random.uniform(0, config.RETRY_JITTER))
        try:
            plan = _PLANNER.invoke(messages)
            breaker.record_success()
            if not isinstance(plan, PlanModel):
                raise OutputParserException(
                    f"El planner devolvió {type(plan).__name__}, se esperaba PlanModel"
                )
            if plan.tool and not plan.widgets:
                last_tool_without_widget = plan
                raise OutputParserException(
                    f"Se seleccionó la tool '{plan.tool}' pero 'widgets' quedó vacío; "
                    "todo tool_args debe reflejarse también en un widget con su "
                    "type y title."
                )
            _log_agent.info(
                "agent_plan message=%r tool=%s tool_args=%s widgets=%d attempt=%d",
                state["message"], plan.tool, plan.tool_args, len(plan.widgets), attempt + 1,
            )
            state["plan"] = plan
            state["error"] = None
            return state
        except (OutputParserException, ValidationError) as exc:
            error = str(exc)
            _log_agent.warning("agent_plan_retry attempt=%d error=%s", attempt + 1, error)
            messages.append(HumanMessage(content=f"Error de formato: {error}. Devuelve JSON válido."))
        except Exception as exc:
            breaker.record_failure()
            _log_agent.error("agent_plan_llm_failure error=%s", exc, exc_info=True)
            state["error"] = f"LLM no disponible: {exc}"
            state["plan"] = PlanModel(
                tool=None, widgets=[], reply="El copiloto no está disponible en este momento."
            )
            return state
    state["error"] = error
    if last_tool_without_widget is not None:
        p = last_tool_without_widget
        _log_agent.warning("agent_plan_synthesized_widget tool=%s", p.tool)
        state["plan"] = PlanModel(
            tool=p.tool,
            tool_args=p.tool_args,
            reply=p.reply,
            widgets=[WidgetSpec(type="table", title=p.tool, tool=p.tool, tool_args=p.tool_args)],
        )
        return state
    _log_agent.warning("agent_plan_unparseable message=%r last_error=%s", state["message"], error)
    state["plan"] = PlanModel(tool=None, widgets=[], reply="No pude interpretar tu consulta.")
    return state
```

**Edge cases (ya resueltos):**
- Ninguna llamada a `_log_agent.*` va envuelta en try/except propio — logging estándar
  con formato `%s` lazy no lanza excepciones por valores arbitrarios (mismo patrón que
  el resto del repo, ej. `app/migrations.py:616`)

**Step 3: Añadir test con `caplog` en `tests/unit/test_agent_graph.py`**

Añadir al final del archivo:
```python
def test_plan_node_logs_successful_plan(db_session, business_id, caplog):
    import logging
    original = g._PLANNER
    g._PLANNER = _FakePlanner(PlanModel(
        tool="total_sales", tool_args={"start": "2026-01-01", "end": "2026-01-31"},
        widgets=[WidgetSpec(type="kpi", title="Ventas", tool="total_sales",
                            tool_args={"start": "2026-01-01", "end": "2026-01-31"})],
        reply="ok",
    ))
    try:
        with caplog.at_level(logging.INFO, logger="app.agent.graph"):
            graph = build_graph()
            graph.invoke({"db": db_session, "business_id": business_id, "message": "ventas de enero", "history": []})
        assert any("agent_plan" in r.message and "total_sales" in r.message for r in caplog.records)
    finally:
        g._PLANNER = original
```

#### Verification

```bash
pytest tests/unit/test_agent_graph.py -v
# Expected: todos PASS (los 12 existentes + el nuevo test de caplog)
```

#### Expected Report

- [ ] `app/agent/graph.py` modificado — 5 líneas de log añadidas en `_plan_node`, sin
      cambiar ninguna lógica de control existente
- [ ] `tests/unit/test_agent_graph.py` modificado — 1 test nuevo
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_graph.py -v` — PASS

---

### T4: `_plan_node` consume `last_tool` (H-005)

**Depends on:** T1 (`build_system_prompt(last_tool=...)`), T3 (estado actual de `_plan_node`)
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/graph.py, tests/unit/test_agent_graph.py
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/prompts.py — YA EXISTE (añadido en T1)
def build_system_prompt(last_tool: Optional[dict] = None) -> str: ...

# app/agent/graph.py — YA EXISTE
class AgentState(TypedDict, total=False):
    db: Any
    business_id: int
    message: str
    history: list[dict]
    plan: Optional[PlanModel]
    tool_results: list[dict]
    widgets: list[dict]
    reply: str
    error: Optional[str]
```

#### Objective

Agregar `last_tool` a `AgentState` y pasarlo a `build_system_prompt()` en `_plan_node`.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/agent/graph.py
```

**Step 2: Agregar el campo a `AgentState`**

```python
# BEFORE:
class AgentState(TypedDict, total=False):
    db: Any
    business_id: int
    message: str
    history: list[dict]
    plan: Optional[PlanModel]
    tool_results: list[dict]
    widgets: list[dict]
    reply: str
    error: Optional[str]

# AFTER:
class AgentState(TypedDict, total=False):
    db: Any
    business_id: int
    message: str
    history: list[dict]
    last_tool: Optional[dict]
    plan: Optional[PlanModel]
    tool_results: list[dict]
    widgets: list[dict]
    reply: str
    error: Optional[str]
```

**Step 3: Usar `last_tool` al construir el prompt del sistema**

```python
# BEFORE (primera línea de _plan_node):
def _plan_node(state: AgentState) -> AgentState:
    messages = [SystemMessage(content=build_system_prompt())]

# AFTER:
def _plan_node(state: AgentState) -> AgentState:
    messages = [SystemMessage(content=build_system_prompt(last_tool=state.get("last_tool")))]
```

**Edge cases (ya resueltos):**
- `state` sin la clave `"last_tool"` (tests existentes que no la pasan): `.get("last_tool")`
  devuelve `None` → `build_system_prompt(last_tool=None)` → comportamiento idéntico al actual

**Step 4: Añadir test en `tests/unit/test_agent_graph.py`**

Añadir al final del archivo:
```python
def test_plan_node_includes_last_tool_in_system_prompt(db_session, business_id):
    captured = {}

    class _CaptureSystemPlanner:
        def invoke(self, messages):
            captured["system"] = messages[0].content
            return PlanModel(tool=None, widgets=[], reply="ok")

    original = g._PLANNER
    g._PLANNER = _CaptureSystemPlanner()
    try:
        graph = build_graph()
        graph.invoke({
            "db": db_session, "business_id": business_id, "message": "¿y por ubicación?",
            "history": [{"role": "user", "content": "ventas de enero"}],
            "last_tool": {"tool": "sales_by_product", "tool_args": {"start": "2026-01-01"}, "type": "bar"},
        })
        assert "sales_by_product" in captured["system"]
        assert "Contexto adicional" in captured["system"]
    finally:
        g._PLANNER = original
```

#### Verification

```bash
pytest tests/unit/test_agent_graph.py -v
# Expected: todos PASS (13 anteriores + 1 nuevo)
```

#### Expected Report

- [ ] `app/agent/graph.py` modificado — `AgentState.last_tool` + línea de `_plan_node`
- [ ] `tests/unit/test_agent_graph.py` modificado — 1 test nuevo
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_graph.py -v` — PASS

---

### T5: `run_agent` acepta y propaga `last_tool`

**Depends on:** T4 (`AgentState.last_tool`)
**Skill:** backend-developer
**Agent:** coder
**Files:** app/agent/runner.py, tests/unit/test_agent_runner.py
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/graph.py — YA EXISTE (añadido en T4)
class AgentState(TypedDict, total=False):
    last_tool: Optional[dict]
    # ... resto de campos sin cambio
```

#### Objective

Agregar el parámetro `last_tool: Optional[dict] = None` a `run_agent()` y meterlo en
el `state` que recibe el grafo.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/agent/runner.py
```

**Step 2: Modificar la firma y el `state`**

```python
# BEFORE:
async def run_agent(business_id: int, message: str, history: Optional[list[dict]] = None) -> dict:
    async with _semaphore:
        def _run() -> dict:
            db = get_session()
            try:
                state = {
                    "db": db,
                    "business_id": int(business_id),
                    "message": message,
                    "history": history or [],
                }
                return _GRAPH.invoke(state)
            finally:
                db.close()

# AFTER:
async def run_agent(
    business_id: int, message: str, history: Optional[list[dict]] = None,
    last_tool: Optional[dict] = None,
) -> dict:
    async with _semaphore:
        def _run() -> dict:
            db = get_session()
            try:
                state = {
                    "db": db,
                    "business_id": int(business_id),
                    "message": message,
                    "history": history or [],
                    "last_tool": last_tool,
                }
                return _GRAPH.invoke(state)
            finally:
                db.close()
```

**Edge cases (ya resueltos):**
- `last_tool=None` (default, llamadas existentes sin el argumento): se guarda `None`
  en el state, `_plan_node` ya lo maneja como "sin contexto extra" (T4)

**Step 3: Añadir test en `tests/unit/test_agent_runner.py`**

Añadir al final del archivo:
```python
def test_run_agent_forwards_last_tool_to_planner_prompt(monkeypatch):
    """REQ-3: last_tool llega al system prompt que ve el LLM."""
    captured = {}

    class _CaptureSystemPlanner:
        def invoke(self, messages):
            captured["system"] = messages[0].content
            return PlanModel(tool=None, widgets=[], reply="ok")

    monkeypatch.setattr(g, "_PLANNER", _CaptureSystemPlanner())
    last_tool = {"tool": "sales_by_product", "tool_args": {"start": "2026-01-01"}, "type": "bar"}
    result = asyncio.run(runner.run_agent(1, "¿y por ubicación?", last_tool=last_tool))
    assert result["error"] is None
    assert "sales_by_product" in captured["system"]
```

#### Verification

```bash
pytest tests/unit/test_agent_runner.py -v
# Expected: 3 tests PASS (2 existentes + 1 nuevo)
```

#### Expected Report

- [ ] `app/agent/runner.py` modificado — firma de `run_agent` + `state["last_tool"]`
- [ ] `tests/unit/test_agent_runner.py` modificado — 1 test nuevo
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_runner.py -v` — PASS

---

### T6: `ui_agent.py::chat()` obtiene y pasa `last_tool`

**Depends on:** T2 (`latest_widget_meta`), T5 (`run_agent(..., last_tool=...)`)
**Skill:** backend-developer
**Agent:** coder
**Files:** app/routers/ui_agent.py, tests/unit/test_ui_agent.py
**Pillars:** P1, P4, P5, P6

#### Context (≤ 200 tokens)

```python
# app/agent/widgets.py — YA EXISTE (añadido en T2)
def latest_widget_meta(db: Session, business_id: int) -> Optional[dict]: ...

# app/agent/runner.py — YA EXISTE (añadido en T5)
async def run_agent(business_id: int, message: str, history=None, last_tool=None) -> dict: ...
```

#### Objective

En `chat()`, obtener `latest_widget_meta` solo si hay historial previo en la
conversación, y pasarlo a `run_agent`.

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read app/routers/ui_agent.py
```

**Step 2: Modificar el bloque de contexto conversacional**

```python
# BEFORE:
    # Contexto conversacional: turnos previos (sin el mensaje recién agregado)
    # para que el modelo responda con memoria de la conversación.
    prior = chat_store.list_messages(db, business_id)[:-1]
    history = [{"role": m.role, "content": m.content} for m in prior]
    result = await runner.run_agent(business_id, message, history=history)

# AFTER:
    # Contexto conversacional: turnos previos (sin el mensaje recién agregado)
    # para que el modelo responda con memoria de la conversación.
    prior = chat_store.list_messages(db, business_id)[:-1]
    history = [{"role": m.role, "content": m.content} for m in prior]
    # Memoria de tool previa (REQ-3): solo si esta conversación ya tuvo un turno
    # (evita anclar a un widget de otra conversación tras "nueva conversación").
    last_tool = widget_store.latest_widget_meta(db, business_id) if prior else None
    result = await runner.run_agent(business_id, message, history=history, last_tool=last_tool)
```

**Edge cases (ya resueltos):**
- Primer mensaje de una conversación (`prior == []`): `last_tool = None`, sin query extra
- Tras "nueva conversación" (`chat_store.clear_messages` ya vació `chat_messages`,
  `dashboard_widgets` queda intacto): `prior == []` en el primer mensaje → `last_tool = None`

**Step 3: Añadir test en `tests/unit/test_ui_agent.py`**

Añadir al final del archivo:
```python
def test_chat_passes_last_tool_only_when_history_exists(db_session, business_id, monkeypatch):
    """REQ-3: last_tool solo se pasa al planner si hay turnos previos en la conversación."""
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    captured = {}

    async def _fake_run_agent(biz_id, message, history=None, last_tool=None):
        captured["last_tool"] = last_tool
        return {"widgets": [], "reply": "ok", "error": None}

    monkeypatch.setattr(ui_agent.runner, "run_agent", _fake_run_agent)
    monkeypatch.setattr(ui_agent.templates, "TemplateResponse", lambda *a, **kw: "RENDERED")

    asyncio.run(ui_agent.chat(req, message="hola", db=db_session))
    assert captured["last_tool"] is None

    widget_store.add_widget(db_session, business_id, type="bar", title="Ventas",
                            tool="sales_by_product", tool_args={"start": "2026-01-01", "end": "2026-01-31"},
                            payload={}, unit=None, created_by=None)

    asyncio.run(ui_agent.chat(req, message="¿y por ubicación?", db=db_session))
    assert captured["last_tool"] == {
        "tool": "sales_by_product",
        "tool_args": {"start": "2026-01-01", "end": "2026-01-31"},
        "type": "bar",
    }
```

#### Verification

```bash
pytest tests/unit/test_ui_agent.py -v
# Expected: todos PASS (existentes + 1 nuevo)
```

#### Expected Report

- [ ] `app/routers/ui_agent.py` modificado — bloque de `chat()` con `last_tool`
- [ ] `tests/unit/test_ui_agent.py` modificado — 1 test nuevo
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_ui_agent.py -v` — PASS

---

### T7: Marker `agent_eval` en pytest.ini

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** pytest.ini
**Pillars:** P1, P4, P6

#### Context (≤ 200 tokens)

```ini
# pytest.ini — ESTO YA EXISTE
[pytest]
markers =
    e2e: end-to-end test (live server + Playwright browser)
```

#### Objective

Registrar el marker `agent_eval` y excluirlo del run por defecto (mismo patrón que `e2e`).

#### Instructions (≤ 400 tokens)

**Step 1: Read the file**
```
Read pytest.ini
```

**Step 2: Aplicar el cambio (archivo completo, 9 líneas)**

```ini
# BEFORE:
[pytest]
testpaths = tests
python_files = test_*.py *_test.py
addopts = -q -m "not e2e" --cov=app.auth --cov=app.audit --cov=app.business_config --cov=app.db --cov=app.deps --cov=app.repositories.product_repository --cov=app.repositories.inventory_repository --cov=app.routers.products --cov=app.routers.inventory --cov=app.routers.ui_agent --cov=app.schemas --cov=app.security --cov=app.services.product_service --cov=app.agent --cov=app.utils --cov-report=term-missing --cov-fail-under=80
asyncio_default_fixture_loop_scope = function
markers =
    e2e: end-to-end test (live server + Playwright browser)

# AFTER:
[pytest]
testpaths = tests
python_files = test_*.py *_test.py
addopts = -q -m "not e2e and not agent_eval" --cov=app.auth --cov=app.audit --cov=app.business_config --cov=app.db --cov=app.deps --cov=app.repositories.product_repository --cov=app.repositories.inventory_repository --cov=app.routers.products --cov=app.routers.inventory --cov=app.routers.ui_agent --cov=app.schemas --cov=app.security --cov=app.services.product_service --cov=app.agent --cov=app.utils --cov-report=term-missing --cov-fail-under=80
asyncio_default_fixture_loop_scope = function
markers =
    e2e: end-to-end test (live server + Playwright browser)
    agent_eval: agent tool-selection accuracy eval against the real LLM (run on demand, `pytest tests/eval -m agent_eval -v -s`)
```

**Edge cases (ya resueltos):** ninguno — cambio de config puro, sin lógica.

#### Verification

```bash
pytest --collect-only -q | tail -5
# Expected: no warnings de "Unknown pytest.mark.agent_eval"
pytest -v
# Expected: exit 0, la suite normal sigue excluyendo e2e y ahora también agent_eval
```

#### Expected Report

- [ ] `pytest.ini` modificado — `addopts` + `markers`
- [ ] Ningún otro archivo modificado
- [ ] Los 2 comandos de verificación — PASS, sin warnings de marker desconocido

---

### T8: Eval set de tool-selection (H-004)

**Depends on:** T7 (marker `agent_eval` registrado)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** tests/eval/test_agent_tool_selection_eval.py (CREATE), .gitignore
**Pillars:** P1, P4, P5, P6, P9

#### Context (≤ 200 tokens)

```python
# app/agent/graph.py — YA EXISTE
def build_graph():  # StateGraph compilado: planner -> execute -> build -> narrate
    ...
# graph.invoke({"db": Session, "business_id": int, "message": str, "history": list[dict]})
#   -> dict con state final, incluye "plan": PlanModel(tool, tool_args, widgets: list[WidgetSpec], reply)

# tests/conftest.py — YA EXISTE
# fixtures db_session (SQLite en memoria) y business_id, visibles automáticamente
# en tests/eval/ (no requiere conftest propio)
```

#### Objective

Crear `tests/eval/test_agent_tool_selection_eval.py` con 24 casos dorados que
invocan el grafo real (LLM sin mockear) y reportan % de aciertos.

#### Instructions

**Step 1: Crear el archivo nuevo (contenido completo)**

```python
"""Eval de precisión de tool-selection del copiloto IA contra el LLM real (H-004).

No corre en el run normal (marcado `agent_eval`, excluido por default en pytest.ini,
mismo patrón que `e2e`). Requiere el LLM configurado en AGENT_LLM_* (dev: Qwen3-8B
local en localhost:8080) disponible y alcanzable.

Comando: pytest tests/eval -m agent_eval -v -s

No es un gate de precisión: reporta el % de aciertos (tool, tipo de widget, args
clave) para poder comparar antes/después cuando se toque app/agent/prompts.py o el
modelo (hallazgo H-003 de la auditoría). La única aserción dura es que ninguna
llamada al LLM falle por conectividad.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from app.agent.graph import build_graph

pytestmark = pytest.mark.agent_eval


GOLDEN_CASES: list[dict] = [
    {"id": "total_sales_this_month", "message": "¿Cuánto vendí este mes?",
     "expected_tool": "total_sales", "expected_type": {"kpi"}, "date_range": "this_month"},
    {"id": "sales_by_product_this_month", "message": "Ventas por producto este mes",
     "expected_tool": "sales_by_product", "expected_type": {"bar", "pie", "table"}, "date_range": "this_month"},
    {"id": "profit_by_category", "message": "¿Qué categoría de producto es más rentable?",
     "expected_tool": "profit_by_category", "expected_type": {"bar", "table"}},
    {"id": "profit_by_location", "message": "Ganancia por ubicación este mes",
     "expected_tool": "profit_by_location", "expected_type": {"bar", "table"}, "date_range": "this_month"},
    {"id": "sales_by_location", "message": "Ventas por ubicación este mes",
     "expected_tool": "sales_by_location", "expected_type": {"bar", "pie", "table"}, "date_range": "this_month"},
    {"id": "top5_sales_by_product", "message": "Dame el top 5 de productos más vendidos",
     "expected_tool": "sales_by_product", "expected_type": {"bar", "pie"}, "expected_args": {"limit": 5}},
    {"id": "top5_profit_by_product", "message": "¿Cuáles son los 5 productos que más ganancia generaron?",
     "expected_tool": "profit_by_product", "expected_type": {"bar", "pie"}, "expected_args": {"limit": 5}},
    {"id": "profit_last_month", "message": "¿Cuánto gané el mes pasado?",
     "expected_tool": "monthly_profit_summary", "expected_type": {"kpi"}, "date_range": "last_month"},
    {"id": "monthly_overview_6m", "message": "Evolución de ventas y compras en los últimos 6 meses",
     "expected_tool": "monthly_overview", "expected_type": {"line"}, "expected_args": {"months": 6}},
    {"id": "sales_by_product_location_filter", "message": "Ventas de todos los productos en la central",
     "expected_tool": "sales_by_product", "expected_type": {"bar", "pie", "table"}, "expected_args_present": ["location"]},
    {"id": "profit_by_product_location_filter", "message": "Ganancia por producto en la central este mes",
     "expected_tool": "profit_by_product", "expected_type": {"bar", "table"}, "expected_args_present": ["location"]},
    {"id": "stock_table", "message": "Dame la tabla de stock actual",
     "expected_tool": "stock_list", "expected_type": {"table"}},
    {"id": "low_stock", "message": "¿Qué productos necesito reponer ya?",
     "expected_tool": "low_stock", "expected_type": {"table"}},
    {"id": "replenishment", "message": "Dame el punto de reorden de cada producto",
     "expected_tool": "replenishment_table", "expected_type": {"table"}},
    {"id": "dead_stock", "message": "¿Qué productos no se han vendido en los últimos 90 días?",
     "expected_tool": "dead_stock", "expected_type": {"bar", "table"}},
    {"id": "inventory_turnover", "message": "¿Cada cuánto rota mi inventario?",
     "expected_tool": "inventory_turnover", "expected_type": {"kpi"}},
    {"id": "extractions_by_party", "message": "¿Cuánto retiró cada socio este mes?",
     "expected_tool": "extractions_by_party", "expected_type": {"bar", "table"}, "date_range": "this_month"},
    {"id": "dividends_pending", "message": "¿Cuánto tiene pendiente cada socio?",
     "expected_tool": "dividends_report", "expected_type": {"table"}},
    {"id": "expenses_by_month", "message": "Gastos por mes de los últimos 12 meses",
     "expected_tool": "expenses_by_month", "expected_type": {"line"}},
    {"id": "purchases_by_month", "message": "Compras por mes",
     "expected_tool": "purchases_by_month", "expected_type": {"line"}},
    {"id": "movement_history_sales", "message": "Historial de movimientos de tipo venta",
     "expected_tool": "movement_history", "expected_type": {"table"}, "expected_args": {"type": "sale"}},
    {"id": "sales_by_customer", "message": "¿Cuáles son mis mejores clientes por facturación?",
     "expected_tool": "sales_by_customer", "expected_type": {"bar", "table"}},
    {"id": "customers_summary", "message": "¿Cuántos clientes tengo registrados?",
     "expected_tool": "customers_summary", "expected_type": {"kpi", "table"}},
    {"id": "sales_documents_summary", "message": "¿Cuánto facturé este mes comparado con lo presupuestado?",
     "expected_tool": "sales_documents_summary", "expected_type": {"kpi", "table"}, "date_range": "this_month"},
]


def _this_month_bounds() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start.replace(year=start.year + 1, month=1) if start.month == 12
           else start.replace(month=start.month + 1))
    return start, end


def _last_month_bounds() -> tuple[datetime, datetime]:
    this_start, _ = _this_month_bounds()
    prev_end = this_start - timedelta(seconds=1)
    prev_start = prev_end.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return prev_start, this_start


_DATE_RANGE_BOUNDS = {"this_month": _this_month_bounds, "last_month": _last_month_bounds}


def _start_in_range(value, start: datetime, end: datetime) -> bool:
    if not value:
        return False
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return start <= dt < end


def _resolve_tool_args(plan, widget) -> dict:
    """Mismo fallback que _execute_node (graph.py): tool_args del widget, o del
    plan si el widget usa la MISMA tool."""
    if widget is None:
        return {}
    return widget.tool_args or (plan.tool_args if widget.tool == plan.tool else {})


def _run_case(case: dict, db_session, business_id: int) -> dict:
    graph = build_graph()
    result = graph.invoke({
        "db": db_session, "business_id": business_id,
        "message": case["message"], "history": [],
    })
    plan = result.get("plan")
    widget = plan.widgets[0] if plan and plan.widgets else None
    tool_ok = bool(plan) and plan.tool == case["expected_tool"]
    type_ok = bool(widget) and widget.type in case["expected_type"]
    tool_args = _resolve_tool_args(plan, widget) if plan else {}

    args_ok = True
    details: list[str] = []
    for key, expected_value in case.get("expected_args", {}).items():
        actual = tool_args.get(key)
        ok = actual == expected_value
        args_ok = args_ok and ok
        details.append(f"{key}={actual!r} (esperado {expected_value!r}) {'OK' if ok else 'FALLO'}")
    for key in case.get("expected_args_present", []):
        actual = tool_args.get(key)
        ok = actual not in (None, "")
        args_ok = args_ok and ok
        details.append(f"{key} presente={actual!r} {'OK' if ok else 'FALLO'}")
    if "date_range" in case:
        start, end = _DATE_RANGE_BOUNDS[case["date_range"]]()
        start_ok = _start_in_range(tool_args.get("start"), start, end)
        args_ok = args_ok and start_ok
        details.append(f"start={tool_args.get('start')!r} en rango {case['date_range']} {'OK' if start_ok else 'FALLO'}")

    llm_error = result.get("error")
    return {
        "id": case["id"], "message": case["message"], "tool_ok": tool_ok, "type_ok": type_ok,
        "args_ok": args_ok, "actual_tool": plan.tool if plan else None,
        "actual_type": widget.type if widget else None, "details": details, "llm_error": llm_error,
    }


def test_tool_selection_accuracy_eval(db_session, business_id):
    results = [_run_case(case, db_session, business_id) for case in GOLDEN_CASES]

    total = len(results)
    tool_hits = sum(r["tool_ok"] for r in results)
    type_hits = sum(r["type_ok"] for r in results)
    args_hits = sum(r["args_ok"] for r in results)
    connectivity_failures = sum(
        1 for r in results if r["llm_error"] and "LLM no disponible" in str(r["llm_error"])
    )

    print("\n=== Eval de tool-selection del copiloto ===")
    for r in results:
        status = "OK" if (r["tool_ok"] and r["type_ok"] and r["args_ok"]) else "FALLO"
        print(f"[{status}] {r['id']}: tool={r['actual_tool']!r} type={r['actual_type']!r}")
        for d in r["details"]:
            print(f"    {d}")
        if r["llm_error"]:
            print(f"    error: {r['llm_error']}")
    print(f"\nTool accuracy:  {tool_hits}/{total} ({tool_hits / total:.0%})")
    print(f"Type accuracy:  {type_hits}/{total} ({type_hits / total:.0%})")
    print(f"Args accuracy:  {args_hits}/{total} ({args_hits / total:.0%})")

    report_path = "tests/eval/results_latest.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(
            {"tool_accuracy": tool_hits / total, "type_accuracy": type_hits / total,
             "args_accuracy": args_hits / total, "cases": results},
            f, ensure_ascii=False, indent=2,
        )
    print(f"\nReporte guardado en {report_path}")

    assert connectivity_failures == 0, (
        f"{connectivity_failures}/{total} casos fallaron por conectividad al LLM "
        f"(AGENT_LLM_BASE_URL={os.environ.get('AGENT_LLM_BASE_URL', '(default) http://localhost:8080/v1')})"
    )
```

**Step 2: Agregar el reporte generado a `.gitignore`**

```
# BEFORE (líneas 20-25):
# Build / caches
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
htmlcov/

# AFTER:
# Build / caches
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
htmlcov/

# Eval reports (generados, no versionar)
tests/eval/results_latest.json
```

**Edge cases (ya resueltos):**
- `plan` es `None` (fallo total del planner): `tool_ok`/`type_ok`/`args_ok` quedan en
  `False`, no se lanza excepción (todos los `if plan else ...` ya lo cubren)
- LLM no alcanzable: cada caso individual captura `result.get("error")`, el test NO
  se cuelga (respeta `AGENT_LLM_TIMEOUT_S`), y el assert final falla con un mensaje
  claro en vez de un traceback de conexión crudo

#### Verification

```bash
pytest tests/eval -m agent_eval -v -s
# Expected: requiere AGENT_LLM_BASE_URL alcanzable (dev: llama-swap en :8080).
# Si el LLM está arriba: test PASS + reporte impreso + tests/eval/results_latest.json escrito.
# Si el LLM está caído: test FAILS con mensaje "N/24 casos fallaron por conectividad".
pytest -v
# Expected: exit 0 — el caso agent_eval NO corre en el run normal (marker excluido por T7)
```

#### Expected Report

- [ ] `tests/eval/test_agent_tool_selection_eval.py` creado — 24 casos + lógica de reporte
- [ ] `.gitignore` modificado — `tests/eval/results_latest.json` añadido
- [ ] Ningún otro archivo modificado
- [ ] `pytest -v` (run normal) — PASS, sin ejecutar el caso `agent_eval`
