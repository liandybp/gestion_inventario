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

from app.agent.graph import _resolve_widget_tool_args, build_graph
from app.utils import month_range

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
    return month_range(datetime.now(timezone.utc))


def _last_month_bounds() -> tuple[datetime, datetime]:
    this_start, _ = _this_month_bounds()
    return month_range(this_start - timedelta(seconds=1))


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


def _run_case(graph, case: dict, db_session, business_id: int) -> dict:
    result = graph.invoke({
        "db": db_session, "business_id": business_id,
        "message": case["message"], "history": [],
    })
    plan = result.get("plan")
    widget = plan.widgets[0] if plan and plan.widgets else None
    tool_ok = bool(plan) and plan.tool == case["expected_tool"]
    type_ok = bool(widget) and widget.type in case["expected_type"]
    tool_args = _resolve_widget_tool_args(plan, widget) if plan and widget else {}

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
    # Grafo compilado una sola vez y reutilizado entre los 24 casos: es
    # stateless (mismo patrón que app/agent/runner.py::_GRAPH a nivel módulo).
    graph = build_graph()
    results = [_run_case(graph, case, db_session, business_id) for case in GOLDEN_CASES]

    total = len(results)
    tool_hits = sum(r["tool_ok"] for r in results)
    type_hits = sum(r["type_ok"] for r in results)
    args_hits = sum(r["args_ok"] for r in results)
    # "LLM en cooldown" = circuit breaker abierto (graph.py, breaker compartido
    # entre los 24 casos); si no se cuenta, un breaker abierto a mitad de eval
    # queda invisible y se lee como mala precisión del modelo en vez de outage.
    connectivity_failures = sum(
        1 for r in results if r["llm_error"] and (
            "LLM no disponible" in str(r["llm_error"]) or "LLM en cooldown" in str(r["llm_error"])
        )
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
