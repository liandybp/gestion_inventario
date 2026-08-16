from datetime import datetime, timezone

from app.agent.prompts import build_system_prompt
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 29


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


def test_sales_by_location_dual_format(db_session, business_id):
    """sales_by_location agrupa ventas por ubicación (labels/values + columns/rows)."""
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_location"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31"}
    )
    assert "labels" in result and "values" in result
    assert "columns" in result and "rows" in result
    assert result["columns"] == ["Ubicación", "Cantidad", "Ventas"]


def test_monthly_profit_summary_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["monthly_profit_summary"]["fn"](service, {})
    assert result["value"] == 0.0 and result["currency"] is True
    assert result["columns"] == ["Métrica", "Valor"]
    assert len(result["rows"]) == 6


def test_profit_by_product_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_product"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Producto", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]


def test_monthly_overview_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["monthly_overview"]["fn"](service, {"months": 3})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Mes", "Ventas", "Compras", "Ganancia bruta"]
    assert len(result["rows"]) == 3


def test_dividends_report_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["dividends_report"]["fn"](service, {})
    assert result["columns"] == ["Socio", "Extraído", "Pendiente"]


def test_extractions_by_party_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["extractions_by_party"]["fn"](service, {})
    assert result["columns"] == ["Socio", "Retirado"]


def test_replenishment_table_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["replenishment_table"]["fn"](service, {})
    assert result["columns"] == ["Producto", "Stock", "Mínimo", "Punto reorden", "Faltante"]


def test_expenses_list_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["expenses_list"]["fn"](service, {})
    assert result["columns"] == ["Fecha", "Concepto", "Importe"]


def test_movement_history_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["movement_history"]["fn"](service, {})
    assert result["columns"] == ["Fecha", "Tipo", "SKU", "Producto", "Cantidad", "Usuario"]


def test_low_stock_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["low_stock"]["fn"](service, {})
    assert result["columns"] == ["SKU", "Nombre", "Stock", "Mínimo"]


def test_sales_by_customer_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_customer"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Cliente", "Facturas", "Total"]


def test_customers_summary_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["customers_summary"]["fn"](service, {})
    assert result["value"] == 0
    assert result["columns"] == ["Cliente", "ID", "Teléfono"]


def test_sales_documents_summary_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_documents_summary"]["fn"](service, {})
    assert result["currency"] is True
    assert result["columns"] == ["Tipo", "Cantidad", "Total"]


def test_expenses_by_month_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["expenses_by_month"]["fn"](service, {"months": 2})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Mes", "Gastos"]
    assert len(result["rows"]) == 2


def test_purchases_by_month_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["purchases_by_month"]["fn"](service, {"months": 2})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Mes", "Compras"]
    assert len(result["rows"]) == 2
