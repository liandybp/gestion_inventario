import app.agent.graph as g
from app.agent.graph import build_graph
from app.agent.schemas import PlanModel, WidgetSpec


class _FakePlanner:
    def __init__(self, plan: PlanModel):
        self._plan = plan

    def invoke(self, messages):
        return self._plan


def test_graph_narrate_without_tool(db_session, business_id):
    original = g._PLANNER
    g._PLANNER = _FakePlanner(PlanModel(tool=None, widgets=[], reply="Hola, soy el copiloto."))
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "hola", "history": []})
        assert result["reply"] == "Hola, soy el copiloto."
        assert result["widgets"] == []
    finally:
        g._PLANNER = original


def test_graph_produces_only_first_widget(db_session, business_id, monkeypatch):
    """Máximo 1 widget por turno: si el plan trae 2, solo se produce el primero."""
    registry = {
        "inventory_value_total": {"fn": lambda svc, args: {"value": 10.0}, "description": "", "params": {}},
        "sales_by_product": {"fn": lambda svc, args: {"labels": ["X"], "values": [1.0]}, "description": "", "params": {}},
    }
    monkeypatch.setattr(g, "TOOL_REGISTRY", registry)
    original = g._PLANNER
    plan = PlanModel(
        tool="inventory_value_total",
        tool_args={},
        widgets=[
            WidgetSpec(type="kpi", title="Valor", tool="inventory_value_total", tool_args={}),
            WidgetSpec(type="bar", title="Ventas", tool="sales_by_product",
                       tool_args={"start": "2026-01-01", "end": "2026-01-31"}),
        ],
        reply="Dos widgets.",
    )
    g._PLANNER = _FakePlanner(plan)
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "x", "history": []})
        assert len(result["widgets"]) == 1
        assert result["widgets"][0]["title"] == "Valor"
        assert result["widgets"][0]["payload"] == {"value": 10.0}
    finally:
        g._PLANNER = original


def test_graph_degrades_when_tool_fails(db_session, business_id):
    """Regression for A1/B1/G3: a failing tool (unknown SKU) must not 500."""
    original = g._PLANNER
    plan = PlanModel(
        tool="stock",
        tool_args={"sku": "SKU-NO-EXISTE"},
        widgets=[WidgetSpec(type="kpi", title="Stock", tool="stock", tool_args={"sku": "SKU-NO-EXISTE"})],
        reply="Consultando.",
    )
    g._PLANNER = _FakePlanner(plan)
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "x", "history": []})
        # tool failure is captured as error and an empty payload, no exception
        assert result.get("error") is not None
        assert result["widgets"][0]["payload"] == {}
    finally:
        g._PLANNER = original


def test_execute_uses_plan_tool_args_when_widget_empty(db_session, business_id):
    """Regression: el modelo puede llenar tool_args a nivel plan y dejar vacío el
    del widget; _execute_node debe usar el del plan (no KeyError 'start')."""
    original = g._PLANNER
    plan = PlanModel(
        tool="total_sales",
        tool_args={"start": "2026-07-01", "end": "2026-07-31"},
        widgets=[WidgetSpec(type="kpi", title="Ventas", tool="total_sales", tool_args={})],
        reply="ok",
    )
    g._PLANNER = _FakePlanner(plan)
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "x", "history": []})
        payload = result["widgets"][0]["payload"]
        assert "value" in payload  # usó el tool_args del plan, no KeyError
        assert result.get("error") is None
    finally:
        g._PLANNER = original


def test_plan_includes_history(db_session, business_id):
    """El planner debe recibir el historial previo (contexto conversacional),
    no solo el mensaje actual."""
    captured = {}

    class _CapturePlanner:
        def invoke(self, messages):
            captured["contents"] = [getattr(m, "content", "") for m in messages]
            return PlanModel(tool=None, widgets=[], reply="ok")

    original = g._PLANNER
    g._PLANNER = _CapturePlanner()
    try:
        graph = build_graph()
        graph.invoke({"db": db_session, "business_id": business_id,
                      "message": "¿y la semana pasada?",
                      "history": [
                          {"role": "user", "content": "¿cuánto vendí?"},
                          {"role": "assistant", "content": "Vendiste 100."},
                      ]})
        contents = captured["contents"]
        assert "¿cuánto vendí?" in contents
        assert "Vendiste 100." in contents
        assert contents[-1] == "¿y la semana pasada?"  # el mensaje actual va último
    finally:
        g._PLANNER = original


def test_graph_degrades_when_planner_returns_none(db_session, business_id):
    """Regression: el planner puede devolver None (salida estructurada vacía) y
    eso NO debe crashear _execute_node con AttributeError -> 500."""
    class _NonePlanner:
        def invoke(self, messages):
            return None

    original = g._PLANNER
    g._PLANNER = _NonePlanner()
    try:
        graph = build_graph()
        result = graph.invoke({"db": db_session, "business_id": business_id, "message": "hola", "history": []})
        # degradó a un fallback sin crashear
        assert result["reply"]
        assert result["widgets"] == []
    finally:
        g._PLANNER = original


def test_tighten_schema_marks_closed_and_required():
    """El schema que se envía al LLM (json_schema, no strict) se endurece como hint:
    additionalProperties:false + required completo, también en modelos anidados."""
    from app.agent.llm import _tighten_schema
    from app.agent.schemas import PlanModel

    s = _tighten_schema(PlanModel.model_json_schema())
    assert s.get("additionalProperties") is False
    assert set(s.get("required", [])) == {"tool", "tool_args", "widgets", "reply"}
    ws = s["$defs"]["WidgetSpec"]
    assert ws.get("additionalProperties") is False
    assert set(ws.get("required", [])) == {"type", "title", "tool", "tool_args", "unit"}


def test_build_planner_does_not_force_strict():
    """Regresión del LLM 400 en prod: el response_format json_schema NO debe ir en
    modo strict (OpenAI rechaza anyOf/additionalProperties:true con strict:true)."""
    from app.agent.llm import build_planner
    from app.agent.schemas import PlanModel

    llm = build_planner(PlanModel)
    rf = llm.model_kwargs.get("response_format", {})
    assert rf.get("type") == "json_schema"
    js = rf.get("json_schema", {})
    assert js.get("name") == "PlanModel"
    assert "strict" not in js


def test_execute_keys_results_by_index(db_session, business_id, monkeypatch):
    """A2/C3: _execute_node alinea resultados por ÍNDICE de widget; dos widgets con
    la MISMA tool pero args distintos no colisionan."""
    registry = {
        "fake_tool": {"fn": lambda svc, args: {"value": args.get("marker", 0)}, "description": "", "params": {}},
    }
    monkeypatch.setattr(g, "TOOL_REGISTRY", registry)
    plan = PlanModel(
        tool="fake_tool", tool_args={},
        widgets=[
            WidgetSpec(type="kpi", title="A", tool="fake_tool", tool_args={"marker": 1}),
            WidgetSpec(type="kpi", title="B", tool="fake_tool", tool_args={"marker": 2}),
        ],
        reply="ok",
    )
    state = g._execute_node({"db": db_session, "business_id": business_id, "plan": plan})
    assert state["tool_results"] == [{"value": 1}, {"value": 2}]


def test_execute_plan_args_only_for_same_tool(db_session, business_id, monkeypatch):
    """C4: el fallback de tool_args del plan solo aplica al widget de la MISMA tool."""
    registry = {
        "fake_a": {"fn": lambda svc, args: {"got": dict(args)}, "description": "", "params": {}},
        "fake_b": {"fn": lambda svc, args: {"got": dict(args)}, "description": "", "params": {}},
    }
    monkeypatch.setattr(g, "TOOL_REGISTRY", registry)
    plan = PlanModel(
        tool="fake_a", tool_args={"plan_arg": 1},
        widgets=[
            WidgetSpec(type="kpi", title="A", tool="fake_a", tool_args={}),
            WidgetSpec(type="kpi", title="B", tool="fake_b", tool_args={}),
        ],
        reply="ok",
    )
    state = g._execute_node({"db": db_session, "business_id": business_id, "plan": plan})
    assert state["tool_results"][0] == {"got": {"plan_arg": 1}}
    assert state["tool_results"][1] == {"got": {}}
