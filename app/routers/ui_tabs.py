from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

from .ui_common import dt_to_local_input, month_range, templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def ui_root() -> RedirectResponse:
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={},
    )


@router.get("/tabs/home", response_class=HTMLResponse)
def tab_home(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)

    products = product_service.list()
    stock_items = inventory_service.stock_list()
    restock_items = [i for i in stock_items if i.needs_restock]

    totals = {
        "products": len(products),
        "stock_total": float(sum(i.quantity for i in stock_items)),
        "to_restock": len(restock_items),
    }

    monthly = inventory_service.monthly_overview(months=12)

    monthly_chart_json = json.dumps(
        {
            "labels": [m.get("month") for m in monthly],
            "sales": [m.get("sales", 0) for m in monthly],
            "purchases": [m.get("purchases", 0) for m in monthly],
            "profit": [m.get("gross_profit", 0) for m in monthly],
        }
    )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_home.html",
        context={"totals": totals, "monthly": monthly, "monthly_chart_json": monthly_chart_json},
    )


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={},
    )


@router.get("/tabs/purchases", response_class=HTMLResponse)
def tab_purchases(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_purchases.html",
        context={
            "products": product_service.recent(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "purchases": inventory_service.recent_purchases(limit=20),
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.get("/tabs/sales", response_class=HTMLResponse)
def tab_sales(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_sales.html",
        context={
            "sales": inventory_service.recent_sales(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.get("/tabs/expenses", response_class=HTMLResponse)
def tab_expenses(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    start, end = month_range(datetime.now(timezone.utc))
    expenses = inventory_service.list_expenses(start=start, end=end, limit=200)
    total = inventory_service.total_expenses(start=start, end=end)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_expenses.html",
        context={
            "expenses": expenses,
            "expenses_total": total,
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.get("/tabs/dividends", response_class=HTMLResponse)
def tab_dividends(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    service = InventoryService(db)
    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    summary = service.monthly_dividends_report(now=now)
    extractions = service.list_extractions(start=start, end=end, limit=200)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_dividends.html",
        context={
            "summary": summary,
            "extractions": extractions,
            "movement_date_default": dt_to_local_input(now),
        },
    )


@router.get("/tabs/profit", response_class=HTMLResponse)
def tab_profit(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    summary, items = inventory_service.monthly_profit_report()
    expenses = inventory_service.list_expenses(
        start=summary["month_start"],
        end=summary["month_end"],
        limit=50,
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit.html",
        context={
            "summary": summary,
            "items": items,
            "expenses": expenses,
        },
    )


@router.get("/tabs/profit-items", response_class=HTMLResponse)
def tab_profit_items(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    summary, items = inventory_service.monthly_profit_items_report()
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit_items.html",
        context={
            "summary": summary,
            "items": items,
        },
    )


@router.get("/stock-table", response_class=HTMLResponse)
def stock_table(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
) -> HTMLResponse:
    service = InventoryService(db)
    items = service.stock_list(query=query)
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={"items": items},
    )


@router.get("/restock-table", response_class=HTMLResponse)
def restock_table(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    items = [i for i in inventory_service.stock_list() if i.needs_restock]
    return templates.TemplateResponse(
        request=request,
        name="partials/restock_table.html",
        context={"items": items},
    )
