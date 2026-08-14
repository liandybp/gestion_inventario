from datetime import datetime, timezone

from app.agent.prompts import build_system_prompt
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 12


def test_inventory_value_total_payload(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["inventory_value_total"]["fn"](service, {})
    assert result == {"value": 0.0, "label": None, "currency": True}


def test_total_expenses_payload(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["total_expenses"]["fn"](service, {})
    assert result == {"value": 0.0, "label": None, "currency": True}


def test_total_sales_payload(db_session, business_id):
    """total_sales returns a KPI payload (value/label/currency), not a chart."""
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["total_sales"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31"}
    )
    assert result == {"value": 0.0, "label": None, "currency": True}


def test_system_prompt_contains_current_date():
    """Regression: the model must know today's date to resolve relative dates
    (previously it guessed 2023 for "mes anterior")."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prompt = build_system_prompt()
    assert today in prompt
    assert "total_sales" in prompt


def test_sales_by_product_dual_format(db_session, business_id):
    """sales_by_product devuelve labels/values (chart) Y columns/rows (table)."""
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_product"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31"}
    )
    assert "labels" in result and "values" in result
    assert "columns" in result and "rows" in result
    assert result["columns"] == ["Producto", "Cantidad", "Ventas"]
