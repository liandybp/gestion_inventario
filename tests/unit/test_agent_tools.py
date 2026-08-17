from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.agent.prompts import build_system_prompt
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.models import Location
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 33


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


def test_dead_stock_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["dead_stock"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["SKU", "Nombre", "Valor inmovilizado"]
    assert result["rows"] == []


def test_dead_stock_days_arg(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["dead_stock"]["fn"](service, {"days": 30})
    assert result["rows"] == []


def test_dead_stock_days_zero_not_overridden(db_session, business_id, monkeypatch):
    """Regression H-002: days=0 explícito es falsy en Python y `or 90` lo
    sobreescribía silenciosamente. Debe llegar a InventoryService.dead_stock tal cual."""
    service = InventoryService(db_session, business_id)
    captured = {}
    original = service.dead_stock

    def spy(days=90, now=None):
        captured["days"] = days
        return original(days=days, now=now)

    monkeypatch.setattr(service, "dead_stock", spy)
    TOOL_REGISTRY["dead_stock"]["fn"](service, {"days": 0})
    assert captured["days"] == 0


def test_inventory_turnover_no_data(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["inventory_turnover"]["fn"](service, {})
    assert result == {"value": 0.0, "label": "Sin datos suficientes", "currency": False}


def test_inventory_turnover_uses_actual_period_length(db_session, business_id, sample_product):
    """Regression H-001: InventoryService.inventory_turnover_days() asume /30 días
    fijo (correcto solo para su otro consumidor, CoachService, que siempre pasa un
    mes completo). La tool ahora calcula cogs_daily con el largo real del rango
    pedido en vez de reusar ese método para rangos arbitrarios."""
    from datetime import timedelta

    from app.agent.tools import _iso, _parse_end
    from app.schemas import PurchaseCreate, SaleCreate

    service = InventoryService(db_session, business_id)
    base = datetime(2026, 1, 10, tzinfo=timezone.utc)
    service.purchase(PurchaseCreate(sku="SKU100001", quantity=10, unit_cost=2.0, movement_date=base))
    service.sale(SaleCreate(
        sku="SKU100001", quantity=5, unit_price=5.0, location_code="CENTRAL",
        movement_date=base + timedelta(days=1),
    ))

    start_arg, end_arg = "2026-01-10", "2026-01-13"
    result = TOOL_REGISTRY["inventory_turnover"]["fn"](service, {"start": start_arg, "end": end_arg})

    period_days = (_parse_end(end_arg) - _iso(start_arg)).days
    cogs_total = 10.0  # 5 unidades vendidas * costo unitario 2.0
    inv_value = 10.0  # 5 unidades remanentes * costo unitario 2.0
    expected = round(inv_value / (cogs_total / period_days), 1)
    assert result["value"] == expected
    assert result["value"] != 30.0  # valor que daría el bug (cogs_total/30 fijo)


def test_profit_by_category_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_category"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]


def test_profit_by_category_aggregates_and_groups_null_category(db_session, business_id, sample_product):
    """Regression H-005/H-007: profit_by_category pasó de agregar en Python sobre
    monthly_profit_items_report a un GROUP BY en SQL (ReportingService.profit_by_category).
    Verifica que la suma por categoría (incluida la categoría NULL -> "Sin categoría")
    sigue siendo correcta con la query nueva."""
    from app.models import Product
    from app.schemas import PurchaseCreate, SaleCreate

    other = Product(
        business_id=business_id, sku="SKU200002", name="Con categoria", category="Bebidas",
        min_stock=1, unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
        lead_time_days=0,
    )
    db_session.add(other)
    db_session.commit()

    service = InventoryService(db_session, business_id)
    base = datetime(2026, 2, 1, tzinfo=timezone.utc)
    service.purchase(PurchaseCreate(sku="SKU100001", quantity=10, unit_cost=2.0, movement_date=base))
    service.sale(SaleCreate(sku="SKU100001", quantity=4, unit_price=5.0, location_code="CENTRAL", movement_date=base))
    service.purchase(PurchaseCreate(sku="SKU200002", quantity=10, unit_cost=1.0, movement_date=base))
    service.sale(SaleCreate(sku="SKU200002", quantity=6, unit_price=3.0, location_code="CENTRAL", movement_date=base))

    result = TOOL_REGISTRY["profit_by_category"]["fn"](service, {"start": "2026-02-01", "end": "2026-02-02"})
    assert "Sin categoría" in result["labels"]
    assert "Bebidas" in result["labels"]
    idx_sin = result["labels"].index("Sin categoría")
    idx_beb = result["labels"].index("Bebidas")
    assert result["rows"][idx_sin][4] == 12.0  # SKU100001: sales 4*5=20, cogs 4*2=8, profit 12
    assert result["rows"][idx_beb][4] == 12.0  # SKU200002: sales 6*3=18, cogs 6*1=6, profit 12


def test_profit_by_location_aggregates_by_location(db_session, business_id, sample_product):
    """Test de datos reales para profit_by_location (paralelo al de profit_by_category):
    verifica que la agregación por ubicación vía GROUP BY en SQL
    (ReportingService.profit_by_location) suma correctamente cuando el mismo
    producto se vende en 2 ubicaciones distintas."""
    from app.schemas import PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate

    service = InventoryService(db_session, business_id)
    base = datetime(2026, 3, 1, tzinfo=timezone.utc)
    # purchase() siempre entra por CENTRAL; para tener stock en POS1 hace falta un traspaso.
    service.purchase(PurchaseCreate(sku="SKU100001", quantity=10, unit_cost=2.0, movement_date=base))
    service.transfer(TransferCreate(
        to_location_code="POS1", lines=[TransferLineCreate(sku="SKU100001", quantity=6)], movement_date=base,
    ))
    service.sale(SaleCreate(sku="SKU100001", quantity=4, unit_price=5.0, location_code="CENTRAL", movement_date=base))
    service.sale(SaleCreate(sku="SKU100001", quantity=6, unit_price=5.0, location_code="POS1", movement_date=base))

    result = TOOL_REGISTRY["profit_by_location"]["fn"](service, {"start": "2026-03-01", "end": "2026-03-02"})

    # profit_by_location agrupa por Location.name (no por code); resolver los
    # nombres reales tal como quedaron creados (auto_create usa el nombre de
    # negocio configurado o el código crudo, no necesariamente "CENTRAL"/"POS1").
    names_by_code = {loc.code: loc.name for loc in db_session.query(Location).filter(Location.business_id == business_id)}
    central_label = next(name for code, name in names_by_code.items() if code.endswith("CENTRAL"))
    pos1_label = next(name for code, name in names_by_code.items() if code.endswith("POS1"))

    assert central_label in result["labels"]
    assert pos1_label in result["labels"]
    idx_central = result["labels"].index(central_label)
    idx_pos1 = result["labels"].index(pos1_label)
    assert result["rows"][idx_central][4] == 12.0  # 4 unidades * (5-2) margen unitario
    assert result["rows"][idx_pos1][4] == 18.0  # 6 unidades * (5-2) margen unitario


def test_profit_by_location_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_location"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]


def test_sales_by_product_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["sales_by_product"]["fn"](
            service, {"start": "2026-01-01", "end": "2026-01-31", "location": "NOPE"}
        )


def test_profit_by_product_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["profit_by_product"]["fn"](service, {"location": "NOPE"})


def test_daily_sales_series_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["daily_sales_series"]["fn"](
            service, {"start": "2026-01-01", "end": "2026-01-31", "location": "NOPE"}
        )


def test_sales_by_product_known_location_resolves(db_session, business_id):
    db_session.add(Location(business_id=business_id, code="CENTRAL", name="Central"))
    db_session.commit()
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_product"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31", "location": "central"}
    )
    assert result["rows"] == []


def test_sales_by_product_exact_code_resolves_via_location_repository(db_session, business_id):
    """Regression H-003: el match exacto de código debe resolver por el path
    delegado a LocationRepository.location_id_for_code, no solo por el fallback
    case-insensitive propio de _resolve_location_id."""
    db_session.add(Location(business_id=business_id, code="CENTRAL", name="Central"))
    db_session.commit()
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_product"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31", "location": "CENTRAL"}
    )
    assert result["rows"] == []
