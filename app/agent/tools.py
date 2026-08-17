from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.services.finance_service import FinanceService
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService
from app.services.sales_document_service import SalesDocumentService
from app.services.coach_service import CoachService

TOOL_REGISTRY: dict[str, dict[str, Any]] = {}


def _iso(value: str) -> datetime:
    """Parse an ISO date; unknown format raises ValueError (handled upstream in _execute_node)."""
    dt = datetime.fromisoformat(value)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def _parse_end(value: str) -> datetime:
    """Parse an `end` bound as inclusive.

    InventoryService filters `movement_date < end` (exclusive). When the LLM
    passes a date-only bound (midnight), a plain parse would silently drop the
    last day; treat date-only as end-of-day (finding A5/CANDIDATO B).
    """
    dt = _iso(value)
    if dt.time() == datetime.min.time():
        dt = dt + timedelta(days=1)
    return dt


def _resolve_location_id(service: InventoryService, location: str) -> int:
    """Código/nombre -> id, solo lectura, scoped a business_id. Primero intenta la
    resolución canónica vía InventoryService.resolve_location_id (incluye el
    fallback de prefijo por negocio, sin autocrear); si no matchea, cae a un
    fallback propio case-insensitive por código o nombre en una sola query. Nunca
    autocrea: un código alucinado por el LLM debe fallar con 404, no crear una
    ubicación fantasma."""
    from fastapi import HTTPException
    from sqlalchemy import func, or_, select

    from app.models import Location

    value = (location or "").strip()
    try:
        return service.resolve_location_id(value, auto_create=False)
    except HTTPException:
        pass

    loc_id = service.db.scalar(
        select(Location.id).where(
            Location.business_id == service.business_id,
            or_(func.lower(Location.code) == value.lower(), func.lower(Location.name) == value.lower()),
        )
    )
    if loc_id is None:
        raise HTTPException(status_code=404, detail=f"Ubicación no encontrada: {location}")
    return int(loc_id)


def tool(name: str, description: str, params: dict) -> Callable:
    def deco(fn: Callable) -> Callable:
        TOOL_REGISTRY[name] = {"description": description, "params": params, "fn": fn}
        return fn
    return deco


@tool("inventory_value_total", "Valor total del inventario a costo.",
      {"type": "object", "properties": {}})
def inventory_value_total(service: InventoryService, args: dict) -> dict:
    return {"value": service.inventory_value_total(), "label": None, "currency": True}


@tool("inventory_sale_value_total", "Valor total del inventario a precio de venta.",
      {"type": "object", "properties": {}})
def inventory_sale_value_total(service: InventoryService, args: dict) -> dict:
    return {"value": service.inventory_sale_value_total(), "label": None, "currency": True}


@tool("total_expenses", "Gastos operativos totales (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def total_expenses(service: InventoryService, args: dict) -> dict:
    finance = FinanceService(service.db, service.business_id)
    total = finance.total_expenses(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    return {"value": total, "label": None, "currency": True}


@tool("top_expense_concept", "Concepto de gasto más alto en un rango (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def top_expense_concept(service: InventoryService, args: dict) -> dict:
    finance = FinanceService(service.db, service.business_id)
    result = finance.top_expense_concept(_iso(args["start"]), _parse_end(args["end"]))
    if result is None:
        return {"value": 0.0, "label": None, "currency": True}
    return {"value": result["total"], "label": result["concept"], "currency": True}


@tool("stock", "Stock actual de un SKU concreto.",
      {"type": "object", "properties": {"sku": {"type": "string"}}, "required": ["sku"]})
def stock(service: InventoryService, args: dict) -> dict:
    s = service.stock(str(args["sku"]))
    return {"value": float(s.quantity), "label": s.name}


@tool("total_sales", "Ventas totales (en dinero) en un rango de fechas (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def total_sales(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    total, _items = reporting.sales_by_product(_iso(args["start"]), _parse_end(args["end"]))
    return {"value": total, "label": None, "currency": True}


def tool_schemas() -> list[dict]:
    return [{"name": name, "description": spec["description"], "parameters": spec["params"]}
            for name, spec in TOOL_REGISTRY.items()]


@tool("sales_by_product", "Ventas desglose por producto (start/end ISO; limit opcional = top N por ventas; "
      "location opcional = código o nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"},
                                          "location": {"type": "string"}},
       "required": ["start", "end"]})
def sales_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    _total, items = reporting.sales_by_product(_iso(args["start"]), _parse_end(args["end"]), location_id=location_id)
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    # Devuelve ambos formatos: labels/values para chart (bar/pie/line) y
    # columns/rows para widget de tipo table, según lo que pida el usuario.
    return {
        "labels": [i["name"] for i in items],
        "values": [i["sales"] for i in items],
        "columns": ["Producto", "Cantidad", "Ventas"],
        "rows": [[i["name"], i["qty"], i["sales"]] for i in items],
    }


@tool("daily_sales_series", "Serie diaria de ventas entre start y end ISO (location opcional = código "
      "o nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "location": {"type": "string"}},
       "required": ["start", "end"]})
def daily_sales_series(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    series = reporting.daily_sales_series(_iso(args["start"]), _parse_end(args["end"]), location_id=location_id)
    return {"labels": [d["day"] for d in series], "values": [d["sales"] for d in series]}


@tool("total_purchases", "Total gastado en compras en un rango (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def total_purchases(service: InventoryService, args: dict) -> dict:
    from sqlalchemy import func, select

    from app.models import InventoryMovement

    stmt = (
        select(func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_cost), 0))
        .where(
            InventoryMovement.type == "purchase",
            InventoryMovement.movement_date >= _iso(args["start"]),
            InventoryMovement.movement_date < _parse_end(args["end"]),
        )
    )
    if service.business_id is not None:
        stmt = stmt.where(InventoryMovement.business_id == service.business_id)
    total = service.db.scalar(stmt) or 0
    return {"value": float(total), "label": None, "currency": True}


@tool("purchases_by_product", "Compras por producto en un rango (start/end ISO; limit opcional).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}},
       "required": ["start", "end"]})
def purchases_by_product(service: InventoryService, args: dict) -> dict:
    from sqlalchemy import func, select

    from app.models import InventoryMovement, Product

    stmt = (
        select(
            Product.sku,
            Product.name,
            func.coalesce(func.sum(InventoryMovement.quantity), 0).label("qty"),
            func.coalesce(func.sum(InventoryMovement.quantity * InventoryMovement.unit_cost), 0).label("cost"),
        )
        .select_from(InventoryMovement)
        .join(Product, Product.id == InventoryMovement.product_id)
        .where(
            InventoryMovement.type == "purchase",
            InventoryMovement.movement_date >= _iso(args["start"]),
            InventoryMovement.movement_date < _parse_end(args["end"]),
            True if service.business_id is None else (InventoryMovement.business_id == service.business_id),
        )
        .group_by(Product.id)
        .order_by(func.sum(InventoryMovement.quantity * InventoryMovement.unit_cost).desc())
    )
    rows = list(service.db.execute(stmt).all())
    limit = args.get("limit")
    if limit:
        rows = rows[: int(limit)]
    return {
        "labels": [r[1] for r in rows],
        "values": [float(r[3]) for r in rows],
        "columns": ["Producto", "Cantidad", "Costo"],
        "rows": [[r[1], float(r[2]), float(r[3])] for r in rows],
    }


@tool("sales_by_location", "Ventas agrupadas por ubicación/punto de venta (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def sales_by_location(service: InventoryService, args: dict) -> dict:
    from sqlalchemy import func, select

    from app.models import InventoryMovement, Location

    stmt = (
        select(
            Location.code,
            Location.name,
            func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
            func.coalesce(
                func.sum(func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0)),
                0,
            ).label("sales"),
        )
        .select_from(InventoryMovement)
        .join(Location, Location.id == InventoryMovement.location_id)
        .where(
            InventoryMovement.type == "sale",
            InventoryMovement.movement_date >= _iso(args["start"]),
            InventoryMovement.movement_date < _parse_end(args["end"]),
            True if service.business_id is None else (InventoryMovement.business_id == service.business_id),
        )
        .group_by(Location.id)
        .order_by(
            func.sum(func.abs(InventoryMovement.quantity) * func.coalesce(InventoryMovement.unit_price, 0)).desc()
        )
    )
    rows = list(service.db.execute(stmt).all())
    return {
        "labels": [(r[1] or r[0]) for r in rows],
        "values": [float(r[3]) for r in rows],
        "columns": ["Ubicación", "Cantidad", "Ventas"],
        "rows": [[(r[1] or r[0]), float(r[2]), float(r[3])] for r in rows],
    }


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


@tool("monthly_profit_summary", "Rentabilidad del período: ventas, costo, gastos, margen bruto/neto (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def monthly_profit_summary(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    summary, _items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    return {
        "value": float(summary["net_total"]),
        "label": None,
        "currency": True,
        "columns": ["Métrica", "Valor"],
        "rows": [
            ["Ventas", float(summary["sales_total"])],
            ["Costo de ventas", float(summary["cogs_total"])],
            ["Margen bruto", float(summary["gross_total"])],
            ["Gastos", float(summary["expenses_total"])],
            ["Neto", float(summary["net_total"])],
            ["Margen neto %", round(float(summary["net_margin_pct"]), 1)],
        ],
    }


@tool("profit_by_product", "Ganancia y margen por producto en un período, ordenado de mayor a menor ganancia "
      "(start/end ISO opcionales; limit opcional = top N por ganancia; location opcional = código o "
      "nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"},
                                          "location": {"type": "string"}}})
def profit_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    _summary, items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
        location_id=location_id,
    )
    items = sorted(items, key=lambda i: i["gross"], reverse=True)
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["gross"]) for i in items],
        "columns": ["Producto", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": [
            [i["name"], float(i["qty"]), float(i["sales"]), float(i["cogs"]),
             float(i["gross"]), round(float(i["gross_pct"]), 1)]
            for i in items
        ],
    }


@tool("monthly_overview", "Evolución mensual de ventas, compras y ganancia bruta (months opcional, default 12).",
      {"type": "object", "properties": {"months": {"type": "integer"}}})
def monthly_overview(service: InventoryService, args: dict) -> dict:
    months = int(args.get("months") or 12)
    reporting = ReportingService(service.db, service.business_id)
    series = reporting.monthly_overview(months=months)
    return {
        "labels": [s["month"] for s in series],
        "values": [float(s["sales"]) for s in series],
        "columns": ["Mes", "Ventas", "Compras", "Ganancia bruta"],
        "rows": [[s["month"], float(s["sales"]), float(s["purchases"]), float(s["gross_profit"])] for s in series],
    }


@tool("dividends_report", "Dividendos y pendiente por socio en un período (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def dividends_report(service: InventoryService, args: dict) -> dict:
    finance = FinanceService(service.db, service.business_id)
    rep = finance.monthly_dividends_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    parties = list(rep["pending"].keys())
    return {
        "columns": ["Socio", "Extraído", "Pendiente"],
        "rows": [
            [p, float(rep["extractions"].get(p, 0)), float(rep["pending"].get(p, 0))]
            for p in parties
        ],
    }


@tool("extractions_by_party", "Retiros de dinero agrupados por socio (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def extractions_by_party(service: InventoryService, args: dict) -> dict:
    finance = FinanceService(service.db, service.business_id)
    by_party = finance.total_extractions_by_party(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    return {
        "labels": list(by_party.keys()),
        "values": [float(v) for v in by_party.values()],
        "columns": ["Socio", "Retirado"],
        "rows": [[p, float(v)] for p, v in by_party.items()],
    }


@tool("replenishment_table", "Reposición: stock, punto de reorden y faltante por producto.",
      {"type": "object", "properties": {}})
def replenishment_table(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    items = reporting.sales_metrics_table(datetime.now(timezone.utc))
    items = sorted(items, key=lambda i: float(i["reorder_shortage"]), reverse=True)
    return {
        "columns": ["Producto", "Stock", "Mínimo", "Punto reorden", "Faltante"],
        "rows": [
            [i["name"], float(i["stock_qty"]), float(i["min_stock"]),
             float(i["reorder_point"]), float(i["reorder_shortage"])]
            for i in items
        ],
    }


@tool("expenses_list", "Últimos gastos operativos (start/end ISO opcionales; limit opcional).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def expenses_list(service: InventoryService, args: dict) -> dict:
    finance = FinanceService(service.db, service.business_id)
    rows = finance.list_expenses(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
        limit=int(args.get("limit") or 50),
    )
    return {
        "columns": ["Fecha", "Concepto", "Importe"],
        "rows": [
            [e.expense_date.strftime("%Y-%m-%d") if e.expense_date else "", e.concept, float(e.amount)]
            for e in rows
        ],
    }


@tool("movement_history", "Historial de movimientos recientes (tipo opcional: sale/purchase/adjustment/transfer_out/transfer_in; start/end ISO opcionales).",
      {"type": "object", "properties": {"type": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def movement_history(service: InventoryService, args: dict) -> dict:
    rows = service.movement_history(
        movement_type=args.get("type") or None,
        start_date=_iso(args["start"]) if args.get("start") else None,
        end_date=_parse_end(args["end"]) if args.get("end") else None,
        limit=int(args.get("limit") or 30),
    )
    return {
        "columns": ["Fecha", "Tipo", "SKU", "Producto", "Cantidad", "Usuario"],
        "rows": [
            [r[1].strftime("%Y-%m-%d %H:%M") if r[1] else "", r[2], r[4], r[5],
             float(r[7] or 0), (r[11] or "")]
            for r in rows
        ],
    }


@tool("low_stock", "Productos con stock por debajo del mínimo (necesitan reposición).",
      {"type": "object", "properties": {}})
def low_stock(service: InventoryService, args: dict) -> dict:
    items = [p for p in service.stock_list() if p.needs_restock]
    return {
        "columns": ["SKU", "Nombre", "Stock", "Mínimo"],
        "rows": [[p.sku, p.name, float(p.quantity), float(p.min_stock)] for p in items],
    }


@tool("sales_by_customer", "Facturación agrupada por cliente (top clientes; start/end ISO opcionales; limit opcional).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def sales_by_customer(service: InventoryService, args: dict) -> dict:
    doc_service = SalesDocumentService(service.db, service.business_id)
    rows = doc_service.sales_by_customer(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
        limit=int(args.get("limit") or 20),
    )
    return {
        "labels": [r["client_name"] for r in rows],
        "values": [r["total"] for r in rows],
        "columns": ["Cliente", "Facturas", "Total"],
        "rows": [[r["client_name"], r["doc_count"], r["total"]] for r in rows],
    }


@tool("customers_summary", "Resumen de clientes: cantidad y listado.",
      {"type": "object", "properties": {}})
def customers_summary(service: InventoryService, args: dict) -> dict:
    doc_service = SalesDocumentService(service.db, service.business_id)
    summary = doc_service.customers_summary()
    return {
        "value": summary["count"],
        "label": None,
        "columns": ["Cliente", "ID", "Teléfono"],
        "rows": [[c["name"], c["client_id"], c["phone"]] for c in summary["customers"]],
    }


@tool("sales_documents_summary", "Facturado vs presupuestado por tipo de documento (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def sales_documents_summary(service: InventoryService, args: dict) -> dict:
    doc_service = SalesDocumentService(service.db, service.business_id)
    rows = doc_service.documents_summary(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    labels = {"F": "Facturas", "P": "Presupuestos"}
    total_invoiced = sum(r["total"] for r in rows if r["doc_type"] == "F")
    return {
        "value": float(total_invoiced),
        "label": None,
        "currency": True,
        "labels": [labels.get(r["doc_type"], r["doc_type"]) for r in rows],
        "values": [r["total"] for r in rows],
        "columns": ["Tipo", "Cantidad", "Total"],
        "rows": [[labels.get(r["doc_type"], r["doc_type"]), r["count"], r["total"]] for r in rows],
    }


@tool("expenses_by_month", "Gastos operativos por mes (months opcional, default 12).",
      {"type": "object", "properties": {"months": {"type": "integer"}}})
def expenses_by_month(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    series = reporting.expenses_by_month(months=int(args.get("months") or 12))
    return {
        "labels": [s["month"] for s in series],
        "values": [s["total"] for s in series],
        "columns": ["Mes", "Gastos"],
        "rows": [[s["month"], s["total"]] for s in series],
    }


@tool("purchases_by_month", "Compras por mes (months opcional, default 12).",
      {"type": "object", "properties": {"months": {"type": "integer"}}})
def purchases_by_month(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    series = reporting.purchases_by_month(months=int(args.get("months") or 12))
    return {
        "labels": [s["month"] for s in series],
        "values": [s["total"] for s in series],
        "columns": ["Mes", "Compras"],
        "rows": [[s["month"], s["total"]] for s in series],
    }


@tool("coach_health_score", "Score de salud del negocio (1-10).",
      {"type": "object", "properties": {}})
def coach_health_score(service: InventoryService, args: dict) -> dict:
    report = CoachService(service.db, service.business_id).health_score()
    return {"value": report["score"], "label": report["level"]}


@tool("coach_recommendations", "Recomendaciones activas del coach.",
      {"type": "object", "properties": {}})
def coach_recommendations(service: InventoryService, args: dict) -> dict:
    coach = CoachService(service.db, service.business_id)
    recommendations = coach.recommendations()
    return {
        "columns": ["Síntoma", "Acción", "Impacto"],
        "rows": [[r["symptom"], r["action"], r.get("impact_text", "")] for r in recommendations],
    }


@tool("dead_stock", "Productos sin venta reciente y su valor inmovilizado a costo (days opcional, default 90).",
      {"type": "object", "properties": {"days": {"type": "integer"}}})
def dead_stock(service: InventoryService, args: dict) -> dict:
    raw_days = args.get("days")
    days = int(raw_days) if raw_days is not None else 90
    items = sorted(service.dead_stock(days=days), key=lambda i: i["value"], reverse=True)
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["value"]) for i in items],
        "columns": ["SKU", "Nombre", "Valor inmovilizado"],
        "rows": [[i["sku"], i["name"], float(i["value"])] for i in items],
    }


@tool("inventory_turnover", "Rotación de inventario en días, según el costo de ventas del período "
      "(start/end ISO opcionales, default mes actual).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def inventory_turnover(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    summary, _items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    cogs_total = float(summary["cogs_total"])
    if cogs_total <= 0:
        return {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    # No reusa InventoryService.inventory_turnover_days (asume siempre /30 días,
    # correcto solo para su otro consumidor CoachService que siempre pasa un mes
    # completo). Aquí start/end pueden ser cualquier rango, así que se calcula con
    # el largo real del período pedido.
    period_days = max((summary["month_end"] - summary["month_start"]).days, 1)
    cogs_daily = cogs_total / period_days
    inv_value = service.inventory_value_total()
    turnover = inv_value / cogs_daily if cogs_daily > 0 else None
    if turnover is None:
        return {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    return {"value": round(float(turnover), 1), "label": None, "currency": False}


def _profit_rows(items: list[dict], key: str) -> list[list]:
    """Formatea items de ReportingService.profit_by_category/profit_by_location
    (ya agregados y ordenados por ganancia en SQL) a filas de tabla."""
    return [
        [i[key], float(i["qty"]), float(i["sales"]), float(i["cogs"]),
         float(i["profit"]), round(float(i["margin_pct"]), 1)]
        for i in items
    ]


@tool("profit_by_category", "Ganancia y margen agrupados por categoría de producto en un período, "
      "ordenado de mayor a menor ganancia (start/end ISO opcionales; limit opcional = top N).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def profit_by_category(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    items = reporting.profit_by_category(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    rows = _profit_rows(items, "category")
    return {
        "labels": [r[0] for r in rows],
        "values": [r[4] for r in rows],
        "columns": ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": rows,
    }


@tool("profit_by_location", "Ganancia y margen agrupados por ubicación/POS en un período, "
      "ordenado de mayor a menor ganancia (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def profit_by_location(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    items = reporting.profit_by_location(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    rows = _profit_rows(items, "location")
    return {
        "labels": [r[0] for r in rows],
        "values": [r[4] for r in rows],
        "columns": ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": rows,
    }
