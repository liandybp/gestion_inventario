# Phase 3 — Task Envelopes: Copiloto de negocio

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T21.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`
> **Regla dura:** ≤2 archivos por envelope · código exacto (before/after) · verificación mecánica · cero placeholders.

---

### T1: Añadir dependencias (langgraph, langchain-openai, httpx)

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `requirements.txt`
**Pillars:** P4, P5

#### Context

```text
# requirements.txt — ESTO YA EXISTE (últimas líneas):
itsdangerous==2.2.0
bump2version==1.0.1
```

#### Objective

Añadir las 3 dependencias del agente a `requirements.txt`.

#### Instructions

**Step 1: Read the file**
```
Read requirements.txt
```

**Step 2: Append at end of file**

```text
# BEFORE (última línea):
bump2version==1.0.1

# AFTER:
bump2version==1.0.1

# Agente copiloto (LangGraph + LLM OpenAI-compatible)
langgraph==0.2.68
langchain-openai==0.2.14
httpx==0.27.2
```

**Edge cases (ya resueltos):**
- Si el entorno resuelve versiones distintas al instalar, NO cambiar el pin — reportar el error tal cual.

#### Verification

```bash
pip install -r requirements.txt
python3 -c "import langgraph, langchain_openai, httpx; print('ok')"
# Expected: "ok"
```

#### Expected Report

- [ ] `requirements.txt` modificado — 3 líneas añadidas al final
- [ ] `python3 -c "import langgraph, langchain_openai, httpx"` — imprime "ok"

---

### T2: Modelo DashboardWidget

**Depends on:** T1
**Skill:** data-engineer
**Agent:** data
**Files:** `app/models.py`
**Pillars:** P1, P4

#### Context

```python
# app/models.py — YA EXISTE (última clase, para ver el patrón):
class OperatingExpense(Base):
    __tablename__ = "operating_expenses"
    ...
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

# Imports YA presentes al inicio del archivo:
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
```

#### Objective

Añadir la clase `DashboardWidget` al final de `app/models.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/models.py
```

**Step 2: Append después de la última clase (OperatingExpense)**

```python
# AFTER (al final del archivo):

class DashboardWidget(Base):
    __tablename__ = "dashboard_widgets"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    type: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(255))
    params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    position: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    created_by: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
```

**Imports needed:** ninguno (todos los tipos ya están importados).

**Edge cases (ya resueltos):**
- `params` es `Text` (JSON serializado). La tabla la crea `Base.metadata.create_all()` en startup; no requiere `ALTER`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q
# Expected: PASS (import de app.models no rompe)
```

#### Expected Report

- [ ] `app/models.py` — clase `DashboardWidget` añadida al final
- [ ] pytest smoke — PASS

---

### T3: app/agent/config.py

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/__init__.py`, `app/agent/config.py`
**Pillars:** P5, P6

#### Objective

Crear el paquete `app/agent` y su módulo de configuración.

#### Instructions

**Step 1:** Crear `app/agent/__init__.py` vacío (0 bytes).

**Step 2:** Crear `app/agent/config.py`:

```python
from __future__ import annotations

import os


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


LLM_BASE_URL = _env("AGENT_LLM_BASE_URL", "https://api.openai.com/v1")
LLM_MODEL = _env("AGENT_LLM_MODEL", "gpt-4o-mini")
LLM_API_KEY = _env("AGENT_LLM_API_KEY", "")
LLM_TIMEOUT_S = float(_env("AGENT_LLM_TIMEOUT_S", "60"))
MAX_CONCURRENCY = int(_env("AGENT_MAX_CONCURRENCY", "8"))
MAX_RETRIES = int(_env("AGENT_MAX_RETRIES", "2"))
```

**Edge cases (ya resueltos):**
- `AGENT_LLM_API_KEY` sin default (obligatorio en producción). Proveedor configurable vía env vars (Gemini Flash-Lite, GPT-4o mini, DeepSeek — todos OpenAI-compatible). El runtime NUNCA usa el modelo local.

#### Verification

```bash
python3 -c "from app.agent import config; print(config.LLM_MODEL, config.MAX_CONCURRENCY)"
# Expected: "gpt-4o-mini 8"
```

#### Expected Report

- [ ] `app/agent/__init__.py` (vacío) y `app/agent/config.py` creados
- [ ] Verificación imprime `gpt-4o-mini 8`

---

### T4: app/agent/llm.py

**Depends on:** T3
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/llm.py`
**Pillars:** P5, P6

#### Objective

Crear `build_llm()` con `ChatOpenAI` + `base_url`.

#### Instructions

Crear `app/agent/llm.py`:

```python
from __future__ import annotations

from langchain_openai import ChatOpenAI

from app.agent import config


def build_llm() -> ChatOpenAI:
    return ChatOpenAI(
        model=config.LLM_MODEL,
        base_url=config.LLM_BASE_URL,
        api_key=config.LLM_API_KEY,
        temperature=0,
        timeout=config.LLM_TIMEOUT_S,
    )
```

**Edge cases (ya resueltos):**
- `temperature=0` para JSON estable.

#### Verification

```bash
python3 -c "from app.agent.llm import build_llm; print(build_llm().model_name)"
# Expected: "gpt-4o-mini"
```

#### Expected Report

- [ ] `app/agent/llm.py` creado
- [ ] Verificación imprime `gpt-4o-mini`

---

### T5: app/agent/schemas.py

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/schemas.py`
**Pillars:** P1, P4

#### Objective

Crear los schemas pydantic que validan el plan del LLM.

#### Instructions

Crear `app/agent/schemas.py`:

```python
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class WidgetSpec(BaseModel):
    type: Literal["kpi", "line", "bar", "table", "list"]
    title: str = Field(min_length=1, max_length=255)
    tool: str = Field(min_length=1)
    tool_args: dict = Field(default_factory=dict)
    unit: Optional[str] = None


class PlanModel(BaseModel):
    tool: Optional[str] = None
    tool_args: dict = Field(default_factory=dict)
    widgets: list[WidgetSpec] = Field(default_factory=list)
    reply: str = Field(min_length=1)
```

**Edge cases (ya resueltos):**
- `tool=None` + `widgets=[]` = respuesta Q&A pura (sin datos).

#### Verification

```bash
python3 -c "from app.agent.schemas import PlanModel; p = PlanModel(reply='hola'); print(p.tool, p.widgets)"
# Expected: "None []"
```

#### Expected Report

- [ ] `app/agent/schemas.py` creado
- [ ] Verificación imprime `None []`

---

### T6: app/agent/tools.py (parte 1 — infraestructura + tools KPI)

**Depends on:** T5
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P6

#### Context (firmas reales que envuelve)

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
# InventoryService(db: Session, business_id: int | None = None)
#   .inventory_value_total(location_id=None) -> float
#   .inventory_sale_value_total(location_id=None) -> float
#   .total_expenses(start=None, end=None) -> float
#   .top_expense_concept(start, end) -> Optional[dict]   # {"concept": str, "total": float}
#   .stock(sku, location_code=None) -> StockRead          # StockRead: sku,name,quantity,min_stock,...
```

#### Objective

Crear `tools.py` con el decorador `tool`, `TOOL_REGISTRY`, `tool_schemas()` y las 5 tools KPI.
Cada tool devuelve un **payload canónico**: `{"value": float, "label": str|None}`.

#### Instructions

Crear `app/agent/tools.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from app.services.inventory_service import InventoryService

TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


def _iso(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def tool(name: str, description: str, params: dict) -> Callable:
    def deco(fn: Callable) -> Callable:
        TOOL_REGISTRY[name] = {"description": description, "params": params, "fn": fn}
        return fn
    return deco


@tool("inventory_value_total", "Valor total del inventario a costo.",
      {"type": "object", "properties": {}})
def inventory_value_total(service: InventoryService, args: dict) -> dict:
    return {"value": service.inventory_value_total(), "label": None}


@tool("inventory_sale_value_total", "Valor total del inventario a precio de venta.",
      {"type": "object", "properties": {}})
def inventory_sale_value_total(service: InventoryService, args: dict) -> dict:
    return {"value": service.inventory_sale_value_total(), "label": None}


@tool("total_expenses", "Gastos operativos totales (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def total_expenses(service: InventoryService, args: dict) -> dict:
    total = service.total_expenses(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_iso(args["end"]) if args.get("end") else None,
    )
    return {"value": total, "label": None}


@tool("top_expense_concept", "Concepto de gasto más alto en un rango (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def top_expense_concept(service: InventoryService, args: dict) -> dict:
    result = service.top_expense_concept(_iso(args["start"]), _iso(args["end"]))
    if result is None:
        return {"value": 0.0, "label": None}
    return {"value": result["total"], "label": result["concept"]}


@tool("stock", "Stock actual de un SKU concreto.",
      {"type": "object", "properties": {"sku": {"type": "string"}}, "required": ["sku"]})
def stock(service: InventoryService, args: dict) -> dict:
    s = service.stock(str(args["sku"]))
    return {"value": float(s.quantity), "label": s.name}


def tool_schemas() -> list[dict]:
    return [{"name": name, "description": spec["description"], "parameters": spec["params"]}
            for name, spec in TOOL_REGISTRY.items()]
```

**Edge cases (ya resueltos):**
- `top_expense_concept` sin datos devuelve `{"value": 0.0, "label": None}` (no None).

#### Verification

```bash
python3 -c "from app.agent.tools import tool_schemas; print(len(tool_schemas()))"
# Expected: "5"
```

#### Expected Report

- [ ] `app/agent/tools.py` creado con 5 tools KPI + `tool_schemas()`
- [ ] Verificación imprime `5`

---

### T7: app/agent/tools.py (parte 2 — tools chart/table)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P6

#### Context

```python
# app/agent/tools.py — YA EXISTE (creado en T6): el decorador @tool, TOOL_REGISTRY, _iso, tool_schemas()
# app/services/inventory_service.py — YA EXISTE:
#   .sales_by_product(start, end, location_id=None) -> tuple[float, list[dict]]   # item: {"sku","name","qty","sales"}
#   .daily_sales_series(start, end, location_id=None) -> list[dict]                # item: {"day","sales"}
#   .stock_list(query="", location_code=None) -> list[StockRead]                    # StockRead: sku,name,quantity,min_stock,needs_restock
#   .available_lots(sku, *, location_code=None) -> list[InventoryLot]              # InventoryLot: lot_code,qty_remaining,unit_cost
```

#### Objective

Añadir 4 tools chart/table al final de `tools.py`, con payloads canónicos
(`{"labels":[str],"values":[float]}` y `{"columns":[str],"rows":[[...]]}`).

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Append después de la función `tool_schemas()`**

```python
# AFTER (al final del archivo):

@tool("sales_by_product", "Ventas totales y desglose por producto en un rango (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def sales_by_product(service: InventoryService, args: dict) -> dict:
    _total, items = service.sales_by_product(_iso(args["start"]), _iso(args["end"]))
    return {"labels": [i["name"] for i in items], "values": [i["sales"] for i in items]}


@tool("daily_sales_series", "Serie diaria de ventas entre start y end ISO.",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def daily_sales_series(service: InventoryService, args: dict) -> dict:
    series = service.daily_sales_series(_iso(args["start"]), _iso(args["end"]))
    return {"labels": [d["day"] for d in series], "values": [d["sales"] for d in series]}


@tool("stock_list", "Lista de stock con flag de reposición (query opcional).",
      {"type": "object", "properties": {"query": {"type": "string"}}})
def stock_list(service: InventoryService, args: dict) -> dict:
    items = service.stock_list(query=str(args.get("query", "")))
    return {
        "columns": ["SKU", "Nombre", "Stock", "Mínimo", "Reponer"],
        "rows": [[i.sku, i.name, float(i.quantity), float(i.min_stock), "Sí" if i.needs_restock else "No"] for i in items],
    }


@tool("available_lots", "Lotes disponibles de un SKU.",
      {"type": "object", "properties": {"sku": {"type": "string"}}, "required": ["sku"]})
def available_lots(service: InventoryService, args: dict) -> dict:
    lots = service.available_lots(str(args["sku"]))
    return {
        "columns": ["Lote", "Restante", "Costo"],
        "rows": [[l.lot_code, float(l.qty_remaining), float(l.unit_cost)] for l in lots],
    }
```

**Edge cases (ya resueltos):**
- `sales_by_product`/`daily_sales_series` usan los keys exactos del dict real (`name`,`sales`,`day`).

#### Verification

```bash
python3 -c "from app.agent.tools import tool_schemas; print(len(tool_schemas()))"
# Expected: "9"
```

#### Expected Report

- [ ] `app/agent/tools.py` — 4 tools añadidas al final
- [ ] Verificación imprime `9`

---

### T8: app/agent/prompts.py

**Depends on:** T7
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/prompts.py`
**Pillars:** P4, P6

#### Objective

Crear el system prompt que instruye al LLM a devolver un `PlanModel`.

#### Instructions

Crear `app/agent/prompts.py`:

```python
from __future__ import annotations

import json

from app.agent.tools import tool_schemas

SYSTEM_PROMPT_TEMPLATE = """\
Eres el copiloto de un negocio de inventario. Responde con un JSON PlanModel.

Reglas:
- Usa UNA tool de datos como máximo por turno (o tool=null para Q&A puro).
- Los números SIEMPRE los obtiene la tool; nunca los inventes.
- widgets: máximo 1 por turno. type según el dato:
  * "kpi" para un valor único (total, valor de inventario, stock de un SKU)
  * "bar" para comparar categorías (ventas por producto)
  * "line" para series temporales (ventas diarias)
  * "table" para filas (stock, lotes)
- tool_args debe respetar el schema de parámetros de la tool.
- reply: explicación breve en español.

Tools (name, description, parameters):
{schemas}
"""


def build_system_prompt() -> str:
    return SYSTEM_PROMPT_TEMPLATE.format(schemas=json.dumps(tool_schemas(), ensure_ascii=False))
```

**Edge cases (ya resueltos):**
- `tool_schemas()` se serializa una vez por petición.

#### Verification

```bash
python3 -c "from app.agent.prompts import build_system_prompt; p = build_system_prompt(); print('sales_by_product' in p, 'PlanModel' in p)"
# Expected: "True True"
```

#### Expected Report

- [ ] `app/agent/prompts.py` creado
- [ ] Verificación imprime `True True`

---

### T9: app/agent/graph.py

**Depends on:** T4, T5, T8
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/graph.py`
**Pillars:** P1, P4, P6

#### Objective

Construir el grafo LangGraph `plan → execute → build → narrate`, con retry interno en `plan`.

#### Instructions

Crear `app/agent/graph.py`:

```python
from __future__ import annotations

from typing import Any, Optional, TypedDict

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph

from app.agent import config
from app.agent.llm import build_llm
from app.agent.prompts import build_system_prompt
from app.agent.schemas import PlanModel
from app.agent.tools import TOOL_REGISTRY
from app.services.inventory_service import InventoryService


class AgentState(TypedDict, total=False):
    db: Any
    business_id: int
    message: str
    history: list[dict]
    plan: Optional[PlanModel]
    tool_result: Optional[dict]
    widgets: list[dict]
    reply: str
    error: Optional[str]


def _plan_node(state: AgentState) -> AgentState:
    llm = build_llm()
    planner = llm.with_structured_output(PlanModel)
    messages = [SystemMessage(content=build_system_prompt()), HumanMessage(content=state["message"])]
    error: Optional[str] = None
    for _ in range(config.MAX_RETRIES + 1):
        try:
            state["plan"] = planner.invoke(messages)
            state["error"] = None
            return state
        except Exception as exc:
            error = str(exc)
            messages.append(HumanMessage(content=f"Error de formato: {error}. Devuelve JSON válido."))
    state["error"] = error
    state["plan"] = PlanModel(tool=None, widgets=[], reply="No pude interpretar tu consulta.")
    return state


def _execute_node(state: AgentState) -> AgentState:
    plan: PlanModel = state["plan"]
    if not plan.tool or plan.tool not in TOOL_REGISTRY:
        state["tool_result"] = None
        return state
    service = InventoryService(state["db"], state["business_id"])
    state["tool_result"] = TOOL_REGISTRY[plan.tool]["fn"](service, plan.tool_args)
    return state


def _build_node(state: AgentState) -> AgentState:
    plan: PlanModel = state["plan"]
    payload = state.get("tool_result") or {}
    widgets = []
    for w in plan.widgets:
        widgets.append({"type": w.type, "title": w.title, "tool": w.tool,
                        "tool_args": w.tool_args, "unit": w.unit, "payload": payload})
    state["widgets"] = widgets
    return state


def _narrate_node(state: AgentState) -> AgentState:
    state["reply"] = state["plan"].reply
    return state


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("plan", _plan_node)
    graph.add_node("execute", _execute_node)
    graph.add_node("build", _build_node)
    graph.add_node("narrate", _narrate_node)
    graph.set_entry_point("plan")
    graph.add_edge("plan", "execute")
    graph.add_edge("execute", "build")
    graph.add_edge("build", "narrate")
    graph.add_edge("narrate", END)
    return graph.compile()
```

**Edge cases (ya resueltos):**
- `db` es parte de `AgentState` (inyectado por el runner); `_execute_node` lo usa para construir `InventoryService`.

#### Verification

```bash
python3 -c "from app.agent.graph import build_graph; print(build_graph() is not None)"
# Expected: "True"
```

#### Expected Report

- [ ] `app/agent/graph.py` creado
- [ ] Verificación imprime `True`

---

### T10: app/agent/runner.py

**Depends on:** T9
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/runner.py`
**Pillars:** P3, P4, P8

#### Objective

Crear el runner (threadpool + semáforo + timeout).

#### Instructions

Crear `app/agent/runner.py`:

```python
from __future__ import annotations

import asyncio
from typing import Optional

from sqlalchemy.orm import Session

from app.agent import config
from app.agent.graph import build_graph

_semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY)


async def run_agent(db: Session, business_id: int, message: str, history: Optional[list[dict]] = None) -> dict:
    async with _semaphore:
        graph = build_graph()
        state = {"db": db, "business_id": int(business_id), "message": message, "history": history or []}
        result = await asyncio.to_thread(graph.invoke, state)
        return {
            "widgets": result.get("widgets", []),
            "reply": result.get("reply", "No pude responder."),
            "tool_result": result.get("tool_result"),
        }
```

**Edge cases (ya resueltos):**
- `asyncio.Semaphore` encola (no rechaza) si está saturado.
- `asyncio.to_thread` aísla el grafo síncrono del event loop.

#### Verification

```bash
python3 -c "from app.agent.runner import run_agent; import inspect; print(inspect.iscoroutinefunction(run_agent))"
# Expected: "True"
```

#### Expected Report

- [ ] `app/agent/runner.py` creado
- [ ] Verificación imprime `True`

---

### T11: app/agent/widgets.py

**Depends on:** T2, T7
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/widgets.py`
**Pillars:** P1, P4, P6

#### Objective

Crear persistencia y serialización de `DashboardWidget` (payload en `params`).

#### Instructions

Crear `app/agent/widgets.py`:

```python
from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DashboardWidget


def add_widget(db: Session, business_id: int, *, type: str, title: str, tool: str,
               tool_args: dict, payload: dict, unit: Optional[str], created_by: Optional[int]) -> DashboardWidget:
    position = len(list_widgets(db, business_id))
    widget = DashboardWidget(
        business_id=int(business_id),
        type=type,
        title=title,
        params=json.dumps({"tool": tool, "tool_args": tool_args, "payload": payload, "unit": unit}, ensure_ascii=False),
        position=position,
        created_by=created_by,
    )
    db.add(widget)
    db.commit()
    db.refresh(widget)
    return widget


def list_widgets(db: Session, business_id: int) -> list[DashboardWidget]:
    return list(db.scalars(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
    ).all())


def widgets_with_payload(db: Session, business_id: int) -> list[dict]:
    out = []
    for w in list_widgets(db, business_id):
        params = json.loads(w.params or "{}")
        out.append({"id": w.id, "type": w.type, "title": w.title, "payload": params.get("payload", {})})
    return out


def delete_widget(db: Session, business_id: int, widget_id: int) -> bool:
    widget = db.scalar(select(DashboardWidget).where(
        DashboardWidget.id == int(widget_id),
        DashboardWidget.business_id == int(business_id),
    ))
    if widget is None:
        return False
    db.delete(widget)
    db.commit()
    return True
```

**Edge cases (ya resueltos):**
- `delete_widget` devuelve `False` si el widget no es del negocio activo (IDOR protegido).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q
# Expected: PASS (import no rompe)
```

#### Expected Report

- [ ] `app/agent/widgets.py` creado
- [ ] Verificación — PASS

---

### T12: app/routers/ui_agent.py

**Depends on:** T10, T11
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_agent.py`
**Pillars:** P1, P4, P6

#### Context (firmas reales)

```python
# app/routers/ui_common.py — YA EXISTE
from app.routers.ui_common import ensure_admin_or_owner, templates
# app/security.py — YA EXISTE
from app.security import require_active_business_id
# app/deps.py — YA EXISTE
from app.deps import session_dep
```

#### Objective

Crear el router HTMX del copiloto (tab, chat, delete, widgets).

#### Instructions

Crear `app/routers/ui_agent.py`:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.agent import runner
from app.agent import widgets as widget_store
from app.deps import session_dep
from app.routers.ui_common import ensure_admin_or_owner, templates
from app.security import require_active_business_id

router = APIRouter(prefix="/ui", tags=["ui-agent"])


@router.get("/tabs/agent", response_class=HTMLResponse)
def tab_agent(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    business_id = require_active_business_id(db, request)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(request, "partials/tab_agent.html", {"widgets": widgets})


@router.post("/agent/chat", response_class=HTMLResponse)
async def chat(request: Request, message: str = Form(...), db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    result = await runner.run_agent(db, business_id, message)
    for w in result["widgets"]:
        widget_store.add_widget(db, business_id, type=w["type"], title=w["title"],
                                tool=w["tool"], tool_args=w["tool_args"], payload=w.get("payload", {}),
                                unit=w.get("unit"), created_by=None)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(request, "partials/agent_result.html",
                                      {"reply": result["reply"], "widgets": widgets})


@router.post("/agent/widget/{widget_id}/delete", response_class=HTMLResponse)
def delete(request: Request, widget_id: int, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    widget_store.delete_widget(db, business_id, widget_id)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(request, "partials/widget_grid.html", {"widgets": widgets})


@router.get("/agent/widgets", response_class=HTMLResponse)
def widgets_view(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    business_id = require_active_business_id(db, request)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(request, "partials/widget_grid.html", {"widgets": widgets})
```

**Edge cases (ya resueltos):**
- `created_by=None` (columna nullable; no se resuelve user_id en el MVP).

#### Verification

```bash
python3 -c "from app.routers.ui_agent import router; print(len(router.routes))"
# Expected: "4"
```

#### Expected Report

- [ ] `app/routers/ui_agent.py` creado
- [ ] Verificación imprime `4`

---

### T13: Registrar router y botón sidebar

**Depends on:** T12
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/main.py`, `app/templates/dashboard.html`
**Pillars:** P4

#### Instructions

**Step 1:** en `app/main.py`, añadir import y registro (patrón de `ui_businesses`):

```python
# BEFORE (bloque de imports):
    ui_businesses,
    ui_expenses,

# AFTER:
    ui_businesses,
    ui_agent,
    ui_expenses,
```

```python
# BEFORE (bloque de include_router):
app.include_router(ui_businesses.router, prefix="/ui")

# AFTER:
app.include_router(ui_businesses.router, prefix="/ui")
app.include_router(ui_agent.router, prefix="/ui")
```

**Step 2:** en `app/templates/dashboard.html`, añadir el botón tras "Historial":

```html
<!-- BEFORE:
          <button class="sidebar-item" id="tab-history" aria-selected="false"
                  hx-get="/ui/tabs/history" hx-target="#tab-content" hx-swap="innerHTML">
            Historial
          </button>
          {% endif %}
-->

<!-- AFTER: -->
          <button class="sidebar-item" id="tab-history" aria-selected="false"
                  hx-get="/ui/tabs/history" hx-target="#tab-content" hx-swap="innerHTML">
            Historial
          </button>
          <button class="sidebar-item" id="tab-agent" aria-selected="false"
                  hx-get="/ui/tabs/agent" hx-target="#tab-content" hx-swap="innerHTML">
            Copiloto
          </button>
          {% endif %}
```

**Edge cases (ya resueltos):**
- El botón queda en el bloque `admin/owner`; operator accede por URL (GET es público para cualquier rol).

#### Verification

```bash
python3 -c "import app.main; print('ok')"
# Expected: "ok"
```

#### Expected Report

- [ ] `app/main.py` — import + include_router añadidos
- [ ] `dashboard.html` — botón "Copiloto" tras "Historial"
- [ ] Verificación imprime `ok`

---

### T14: Templates — tab_agent.html + widget_grid.html

**Depends on:** T12
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/tab_agent.html`, `app/templates/partials/widget_grid.html`
**Pillars:** P4

#### Objective

Crear el template del tab y el grid (que despacha a los partials por tipo).

#### Instructions

Crear `app/templates/partials/tab_agent.html`:

```html
<div class="agent-container">
  <h3>Copiloto</h3>
  <form hx-post="/ui/agent/chat" hx-target="#agent-content" hx-swap="innerHTML">
    <input type="text" name="message" placeholder="Ej: ventas por producto este mes" required style="width:70%; padding:10px;" />
    <button type="submit" style="padding:10px;">Preguntar</button>
  </form>
  <div id="agent-content">
    <div class="agent-reply">Bienvenido. Preguntame sobre tu negocio.</div>
    {% include "partials/widget_grid.html" %}
  </div>
</div>
```

Crear `app/templates/partials/widget_grid.html`:

```html
{% if widgets %}
<div class="widget-grid">
  {% for w in widgets %}
  <div class="widget-card" id="widget-{{ w.id }}">
    <div class="widget-header">
      <span>{{ w.title }}</span>
      <button hx-post="/ui/agent/widget/{{ w.id }}/delete" hx-target="#agent-content" hx-swap="innerHTML">✕</button>
    </div>
    {% if w.type in ("line", "bar") %}
      {% include "partials/widget_chart.html" %}
    {% elif w.type == "kpi" %}
      {% include "partials/widget_kpi.html" %}
    {% elif w.type == "table" %}
      {% include "partials/widget_table.html" %}
    {% else %}
      {% include "partials/widget_list.html" %}
    {% endif %}
  </div>
  {% endfor %}
</div>
{% else %}
<p class="muted">Aún no hay widgets. Pregunta algo arriba para construir tu tablero.</p>
{% endif %}
```

**Edge cases (ya resueltos):**
- `w` es un dict (`id`,`type`,`title`,`payload`); Jinja accede por atributo a dicts.

#### Verification

```bash
python3 -c "from pathlib import Path; print(Path('app/templates/partials/tab_agent.html').exists(), Path('app/templates/partials/widget_grid.html').exists())"
# Expected: "True True"
```

#### Expected Report

- [ ] `tab_agent.html` y `widget_grid.html` creados
- [ ] Verificación imprime `True True`

---

### T15: Templates — widget_kpi.html + widget_chart.html

**Depends on:** T14
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/widget_kpi.html`, `app/templates/partials/widget_chart.html`
**Pillars:** P4

#### Objective

Crear los partials KPI y chart que renderizan el payload real.

#### Instructions

Crear `app/templates/partials/widget_kpi.html`:

```html
<div class="widget-kpi">
  <span class="kpi-value">{{ w.payload.value if w.payload else '—' }}</span>
  <span class="kpi-label">{{ w.title }}</span>
</div>
```

Crear `app/templates/partials/widget_chart.html`:

```html
<canvas id="chart-{{ w.id }}" height="160"></canvas>
<script>
  (function () {
    var el = document.getElementById("chart-{{ w.id }}");
    if (!el || !window.Chart) return;
    var payload = {{ (w.payload or {}) | tojson }};
    new Chart(el, {
      type: "{{ w.type }}",
      data: {
        labels: payload.labels || [],
        datasets: [{ label: "{{ w.title }}", data: payload.values || [] }]
      },
      options: { responsive: true, maintainAspectRatio: false }
    });
  })();
</script>
```

**Edge cases (ya resueltos):**
- `tojson` escapa el payload a JS seguro; `payload.values`/`labels` caen a `[]` si faltan.

#### Verification

```bash
python3 -c "from pathlib import Path; print(Path('app/templates/partials/widget_kpi.html').exists(), Path('app/templates/partials/widget_chart.html').exists())"
# Expected: "True True"
```

#### Expected Report

- [ ] `widget_kpi.html` y `widget_chart.html` creados
- [ ] Verificación imprime `True True`

---

### T16: Templates — widget_table.html + widget_list.html

**Depends on:** T14
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/widget_table.html`, `app/templates/partials/widget_list.html`
**Pillars:** P4

#### Objective

Crear los partials de tabla y lista que renderizan el payload real.

#### Instructions

Crear `app/templates/partials/widget_table.html`:

```html
<table class="widget-table">
  <thead>
    <tr>{% for c in w.payload.columns %}<th>{{ c }}</th>{% endfor %}</tr>
  </thead>
  <tbody>
    {% for row in w.payload.rows %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
  </tbody>
</table>
```

Crear `app/templates/partials/widget_list.html`:

```html
<ul class="widget-list">
  {% for item in w.payload.items %}<li>{{ item }}</li>{% endfor %}
</ul>
```

**Edge cases (ya resueltos):**
- `w.payload.columns`/`rows`/`items` vacíos renderizan una tabla/lista vacía (sin error).

#### Verification

```bash
python3 -c "from pathlib import Path; print(Path('app/templates/partials/widget_table.html').exists(), Path('app/templates/partials/widget_list.html').exists())"
# Expected: "True True"
```

#### Expected Report

- [ ] `widget_table.html` y `widget_list.html` creados
- [ ] Verificación imprime `True True`

---

### T17: Templates — agent_result.html

**Depends on:** T14
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/agent_result.html`
**Pillars:** P4

#### Objective

Crear el partial de respuesta del chat (narración + grid).

#### Instructions

Crear `app/templates/partials/agent_result.html`:

```html
<div class="agent-reply">{{ reply }}</div>
{% include "partials/widget_grid.html" %}
```

**Edge cases (ya resueltos):**
- Se swapea en `#agent-content` (innerHTML), reemplazando la respuesta anterior + grid.

#### Verification

```bash
python3 -c "from pathlib import Path; print(Path('app/templates/partials/agent_result.html').exists())"
# Expected: "True"
```

#### Expected Report

- [ ] `agent_result.html` creado
- [ ] Verificación imprime `True`

---

### T18: Tests — modelo y CRUD de widgets

**Depends on:** T2, T11
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_agent_widgets.py`
**Pillars:** P1

#### Context (fixtures reales)

```python
# tests/conftest.py — YA EXISTE
@pytest.fixture
def db_session() -> Session
@pytest.fixture
def business_id(db_session) -> int
```

#### Objective

Testear CRUD y aislamiento por negocio de `DashboardWidget`.

#### Instructions

Crear `tests/unit/test_agent_widgets.py`:

```python
from app.agent.widgets import add_widget, delete_widget, list_widgets, widgets_with_payload


def test_add_and_list_widget(db_session, business_id):
    w = add_widget(db_session, business_id, type="kpi", title="Valor", tool="inventory_value_total",
                   tool_args={}, payload={"value": 10.0, "label": None}, unit=None, created_by=None)
    assert w.id is not None
    widgets = widgets_with_payload(db_session, business_id)
    assert len(widgets) == 1
    assert widgets[0]["title"] == "Valor"
    assert widgets[0]["payload"] == {"value": 10.0, "label": None}


def test_delete_widget_scoped_to_business(db_session, business_id):
    w = add_widget(db_session, business_id, type="kpi", title="X", tool="stock", tool_args={},
                   payload={"value": 1.0, "label": None}, unit=None, created_by=None)
    assert delete_widget(db_session, business_id, w.id) is True
    assert delete_widget(db_session, business_id, 999999) is False
    assert list_widgets(db_session, business_id) == []
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_agent_widgets.py -v
# Expected: 2 passed
```

#### Expected Report

- [ ] `tests/unit/test_agent_widgets.py` creado
- [ ] 2 tests PASS

---

### T19: Tests — tools

**Depends on:** T7
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_agent_tools.py`
**Pillars:** P1, P6

#### Objective

Testear que las tools despachan a `InventoryService` y devuelven payload canónico.

#### Instructions

Crear `tests/unit/test_agent_tools.py`:

```python
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 9


def test_inventory_value_total_payload(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["inventory_value_total"]["fn"](service, {})
    assert result == {"value": 0.0, "label": None}


def test_total_expenses_payload(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["total_expenses"]["fn"](service, {})
    assert result == {"value": 0.0, "label": None}
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_agent_tools.py -v
# Expected: 3 passed
```

#### Expected Report

- [ ] `tests/unit/test_agent_tools.py` creado
- [ ] 3 tests PASS

---

### T20: Tests — grafo con LLM fake

**Depends on:** T9
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_agent_graph.py`
**Pillars:** P1

#### Objective

Testear `build_graph` con un plan fijo (sin LLM real).

#### Instructions

Crear `tests/unit/test_agent_graph.py`:

```python
import app.agent.graph as g
from app.agent.graph import build_graph
from app.agent.schemas import PlanModel


def _fake_llm():
    class _FakePlanner:
        def invoke(self, messages):
            return PlanModel(tool=None, widgets=[], reply="Hola, soy el copiloto.")
    return type("L", (), {"with_structured_output": lambda self, m: _FakePlanner()})()


def test_graph_narrate_without_tool(db_session, business_id):
    original = g.build_llm
    g.build_llm = _fake_llm
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "hola", "history": []})
        assert result["reply"] == "Hola, soy el copiloto."
        assert result["widgets"] == []
    finally:
        g.build_llm = original
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_agent_graph.py -v
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/unit/test_agent_graph.py` creado
- [ ] 1 test PASS

---

### T21: Tests — router (auth del tab)

**Depends on:** T12
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_ui_agent.py`
**Pillars:** P1, P6

#### Objective

Testear que el tab exige autenticación (middleware redirige sin sesión).

#### Instructions

Crear `tests/unit/test_ui_agent.py`:

```python
from fastapi.testclient import TestClient

from app.main import app


def test_tab_agent_requires_auth():
    client = TestClient(app)
    resp = client.get("/ui/tabs/agent")
    assert resp.status_code in (302, 401)
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_agent.py -v
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/unit/test_ui_agent.py` creado
- [ ] 1 test PASS
