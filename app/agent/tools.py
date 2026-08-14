from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.services.inventory_service import InventoryService

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
    total = service.total_expenses(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    return {"value": total, "label": None, "currency": True}


@tool("top_expense_concept", "Concepto de gasto más alto en un rango (start/end ISO).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def top_expense_concept(service: InventoryService, args: dict) -> dict:
    result = service.top_expense_concept(_iso(args["start"]), _parse_end(args["end"]))
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
    total, _items = service.sales_by_product(_iso(args["start"]), _parse_end(args["end"]))
    return {"value": total, "label": None, "currency": True}


def tool_schemas() -> list[dict]:
    return [{"name": name, "description": spec["description"], "parameters": spec["params"]}
            for name, spec in TOOL_REGISTRY.items()]


@tool("sales_by_product", "Ventas desglose por producto (start/end ISO; limit opcional = top N por ventas).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}},
       "required": ["start", "end"]})
def sales_by_product(service: InventoryService, args: dict) -> dict:
    _total, items = service.sales_by_product(_iso(args["start"]), _parse_end(args["end"]))
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


@tool("daily_sales_series", "Serie diaria de ventas entre start y end ISO.",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def daily_sales_series(service: InventoryService, args: dict) -> dict:
    series = service.daily_sales_series(_iso(args["start"]), _parse_end(args["end"]))
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
    if service._business_id is not None:
        stmt = stmt.where(InventoryMovement.business_id == service._business_id)
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
            True if service._business_id is None else (InventoryMovement.business_id == service._business_id),
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
