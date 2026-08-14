from __future__ import annotations

from typing import Any, Optional, TypedDict

from fastapi import HTTPException
from langchain_core.exceptions import OutputParserException
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langgraph.graph import END, StateGraph
from pydantic import ValidationError

from app.agent import config
from app.agent.llm import build_planner
from app.agent.prompts import build_system_prompt
from app.agent.schemas import PlanModel
from app.agent.tools import TOOL_REGISTRY
from app.services.inventory_service import InventoryService


class _Planner:
    """Planner con salida forzada por JSON Schema (grammar de llama.cpp).

    Los modelos locales no son fiables ni con function-calling (devuelven None
    en saludos sin tool) ni con json_mode plano (JSON con shape distinto al
    schema). Con response_format=json_schema, llama.cpp fuerza la salida al
    schema exacto de PlanModel: funciona para preguntas con tool y sin tool.
    """

    def __init__(self, llm):
        self._llm = llm

    def invoke(self, messages):
        response = self._llm.invoke(messages)
        content = getattr(response, "content", None) or ""
        if not content:
            raise OutputParserException("El planner no devolvió contenido")
        return PlanModel.model_validate_json(content)


# El planner se construye con build_planner (llm.py): el schema se adapta a los
# requisitos strict de OpenAI (C1). La fecha del prompt NO se cachea: se calcula
# en cada _plan_node para que "mes pasado" se resuelva contra la fecha real (A1).
_PLANNER = _Planner(build_planner(PlanModel))


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


def _plan_node(state: AgentState) -> AgentState:
    # Contexto conversacional: reconstruir los turnos previos (user/assistant)
    # antes del mensaje actual, para que el modelo responda con memoria (no
    # cada petición como si empezara de cero). Se limita a las últimas 20
    # para no inflar el prompt.
    # La fecha se calcula en CADA llamada (no se cachea) para que las fechas
    # relativas del modelo sean correctas aunque el server corra días (A1).
    messages = [SystemMessage(content=build_system_prompt())]
    for turn in (state.get("history") or [])[-20:]:
        content = turn.get("content", "")
        if turn.get("role") == "assistant":
            messages.append(AIMessage(content=content))
        else:
            messages.append(HumanMessage(content=content))
    messages.append(HumanMessage(content=state["message"]))
    error: Optional[str] = None
    for _ in range(config.MAX_RETRIES + 1):
        try:
            plan = _PLANNER.invoke(messages)
            # El modelo puede devolver None (salida estructurada vacía) en vez de
            # un PlanModel; si lo guardamos tal cual, _execute_node crashea con
            # AttributeError y el chat da 500. Tratar None como error de formato.
            if not isinstance(plan, PlanModel):
                raise OutputParserException(
                    f"El planner devolvió {type(plan).__name__}, se esperaba PlanModel"
                )
            state["plan"] = plan
            state["error"] = None
            return state
        except (OutputParserException, ValidationError) as exc:
            # Parse/validation errors are retryable with feedback (D1).
            error = str(exc)
            messages.append(HumanMessage(content=f"Error de formato: {error}. Devuelve JSON válido."))
        except Exception as exc:
            # Hard failure (network/auth/timeout): do not burn retries on a
            # dead endpoint; degrade immediately and surface the reason.
            # (reply must be non-empty: PlanModel enforces min_length=1.)
            state["error"] = f"LLM no disponible: {exc}"
            state["plan"] = PlanModel(
                tool=None, widgets=[], reply="El copiloto no está disponible en este momento."
            )
            return state
    state["error"] = error
    state["plan"] = PlanModel(tool=None, widgets=[], reply="No pude interpretar tu consulta.")
    return state


def _execute_node(state: AgentState) -> AgentState:
    """Execute each widget's own tool and capture a per-tool payload.

    Every tool runs inside try/except so a failing tool (unknown SKU -> 404,
    malformed date -> ValueError, missing arg -> KeyError) degrades instead of
    propagating a 500 through the chat (findings A1/B1/C1/G3).
    """
    plan: PlanModel = state["plan"]
    # Resultados alineados por ÍNDICE de widget (no por nombre de tool): así dos
    # widgets con la misma tool pero args distintos no colisionan (A2/C3).
    # Se ejecutan todos los widgets (lógica correcta por widget); _build_node
    # luego aplica el límite de 1 widget por turno.
    results: list[dict] = []
    service = InventoryService(state["db"], state["business_id"])
    for w in plan.widgets:
        tool_name = w.tool
        if not tool_name or tool_name not in TOOL_REGISTRY:
            results.append({})
            continue
        # tool_args del plan solo se usan si el widget usa la MISMA tool (C4).
        tool_args = w.tool_args or (plan.tool_args if w.tool == plan.tool else {})
        try:
            results.append(TOOL_REGISTRY[tool_name]["fn"](service, tool_args))
        except HTTPException as exc:
            state["error"] = f"Tool {tool_name}: {exc.detail}"
            results.append({})
        except Exception as exc:
            state["error"] = f"Tool {tool_name}: {exc}"
            results.append({})
    state["tool_results"] = results
    return state


def _build_node(state: AgentState) -> AgentState:
    """Attach each widget the payload of ITS OWN tool, not a shared one.

    Fixes findings A2/B5/G2: before, the single tool_result was copied to every
    widget, persisting wrong data when the plan had multiple widgets/tools.
    """
    plan: PlanModel = state["plan"]
    results = state.get("tool_results") or []
    widgets = []
    for i, w in enumerate(plan.widgets[:1]):
        payload = results[i] if i < len(results) else {}
        widgets.append({"type": w.type, "title": w.title, "tool": w.tool,
                        "tool_args": w.tool_args, "unit": w.unit, "payload": payload})
    state["widgets"] = widgets
    return state


def _narrate_node(state: AgentState) -> AgentState:
    state["reply"] = state["plan"].reply
    return state


def build_graph():
    graph = StateGraph(AgentState)
    graph.add_node("planner", _plan_node)
    graph.add_node("execute", _execute_node)
    graph.add_node("build", _build_node)
    graph.add_node("narrate", _narrate_node)
    graph.set_entry_point("planner")
    graph.add_edge("planner", "execute")
    graph.add_edge("execute", "build")
    graph.add_edge("build", "narrate")
    graph.add_edge("narrate", END)
    return graph.compile()
