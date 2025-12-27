from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import AuditLog
from app.security import get_current_user_from_session
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

from .ui_common import dt_to_local_input, ensure_admin, month_range, parse_dt, templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def ui_root() -> RedirectResponse:
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"user": user},
    )


@router.get("/tabs/home", response_class=HTMLResponse)
def tab_home(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
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

    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    _summary, profit_items = inventory_service.monthly_profit_report(now=now)

    month_names = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    month_label = f"{month_names[int(now.month) - 1].title()} {now.year}"

    top_margin = None
    if profit_items:
        top_margin = max(profit_items, key=lambda r: float(r.get("gross_pct") or 0))

    year_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    year_end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    year_total_sales, year_items = inventory_service.sales_by_product(start=year_start, end=year_end)
    year_items_nonzero = [i for i in year_items if float(i.get("sales") or 0) > 0]
    top_year = year_items_nonzero[0] if year_items_nonzero else None
    bottom_year = year_items_nonzero[-1] if year_items_nonzero else None
    if year_items_nonzero:
        top_year = max(year_items_nonzero, key=lambda r: float(r.get("sales") or 0))
        bottom_year = min(year_items_nonzero, key=lambda r: float(r.get("sales") or 0))

    top_year_pct = ((float(top_year.get("sales") or 0) / year_total_sales) * 100.0) if (top_year and year_total_sales) else 0.0
    bottom_year_pct = ((float(bottom_year.get("sales") or 0) / year_total_sales) * 100.0) if (bottom_year and year_total_sales) else 0.0

    inventory_value_total = inventory_service.inventory_value_total()
    inventory_sale_value_total = inventory_service.inventory_sale_value_total()
    top_expense = inventory_service.top_expense_concept(start=start, end=end)

    pie_labels: list[str] = []
    pie_values: list[float] = []
    pie_qtys: list[float] = []
    if profit_items:
        items_sorted = sorted(profit_items, key=lambda r: float(r.get("sales") or 0), reverse=True)
        top_n = 10
        top_items = items_sorted[:top_n]
        rest_items = items_sorted[top_n:]

        for it in top_items:
            label = str(it.get("sku") or "")
            name = str(it.get("name") or "").strip()
            if name:
                label = f"{label} - {name}" if label else name
            pie_labels.append(label or "(sin nombre)")
            pie_values.append(float(it.get("sales") or 0))
            pie_qtys.append(float(it.get("qty") or 0))

        rest_total = float(sum(float(r.get("sales") or 0) for r in rest_items))
        if rest_total > 0:
            pie_labels.append("Otros")
            pie_values.append(rest_total)
            pie_qtys.append(float(sum(float(r.get("qty") or 0) for r in rest_items)))

    monthly_sales_pie_json = json.dumps({"labels": pie_labels, "values": pie_values, "qtys": pie_qtys})

    daily = inventory_service.daily_sales_series(start=start, end=end)
    monthly_sales_daily_line_json = json.dumps(
        {
            "labels": [d.get("day") for d in daily],
            "sales": [d.get("sales", 0) for d in daily],
        }
    )

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
        context={
            "totals": totals,
            "monthly": monthly,
            "monthly_chart_json": monthly_chart_json,
            "month_label": month_label,
            "top_margin": top_margin,
            "year": now.year,
            "year_total_sales": year_total_sales,
            "top_year": top_year,
            "bottom_year": bottom_year,
            "top_year_pct": top_year_pct,
            "bottom_year_pct": bottom_year_pct,
            "inventory_value_total": inventory_value_total,
            "inventory_sale_value_total": inventory_sale_value_total,
            "top_expense": top_expense,
            "monthly_sales_pie_json": monthly_sales_pie_json,
            "monthly_sales_daily_line_json": monthly_sales_daily_line_json,
        },
    )


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={},
    )


@router.get("/tabs/purchases", response_class=HTMLResponse)
def tab_purchases(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    user = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_purchases.html",
        context={
            "user": user,
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
    user = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_sales.html",
        context={
            "user": user,
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
    ensure_admin(db, request)
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
    ensure_admin(db, request)
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
    ensure_admin(db, request)
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
    ensure_admin(db, request)
    service = InventoryService(db)
    user = get_current_user_from_session(db, request)
    items = service.stock_list(query=query)
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={"items": items, "user": user},
    )


@router.post("/stock/{sku}/delete", response_class=HTMLResponse)
def stock_delete_product(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
    query: str = Form(""),
) -> HTMLResponse:
    ensure_admin(db, request)
    user = get_current_user_from_session(db, request)
    product_service = ProductService(db)
    inventory_service = InventoryService(db)

    message = None
    message_detail = None
    message_class = None
    try:
        product_service.delete(sku)
        message = "Producto eliminado"
        message_detail = f"SKU: {sku}"
        message_class = "ok"
    except Exception as e:
        message = "No se pudo eliminar"
        message_detail = str(getattr(e, "detail", e))
        message_class = "error"

    items = inventory_service.stock_list(query=query)
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
        },
    )


@router.get("/restock-table", response_class=HTMLResponse)
def restock_table(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    inventory_service = InventoryService(db)
    items = [i for i in inventory_service.stock_list() if i.needs_restock]
    return templates.TemplateResponse(
        request=request,
        name="partials/restock_table.html",
        context={"items": items},
    )


@router.get("/tabs/history", response_class=HTMLResponse)
def tab_history(
    request: Request,
    db: Session = Depends(session_dep),
    sku: Optional[str] = None,
    movement_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin(db, request)
    product_service = ProductService(db)
    inventory_service = InventoryService(db)

    sku_filter = sku.strip() if sku else None
    type_filter = movement_type.strip() if movement_type else None
    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    movements = inventory_service.movement_history(
        sku=sku_filter or None,
        movement_type=type_filter or None,
        start_date=start_dt,
        end_date=end_dt,
        limit=200,
    )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_history.html",
        context={
            "movements": movements,
            "product_options": product_service.search(query="", limit=200),
            "sku_filter": sku_filter or "",
            "type_filter": type_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )


@router.get("/tabs/activity", response_class=HTMLResponse)
def tab_activity(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)

    username: str = (request.query_params.get("username") or "").strip()
    role_filter: str = (request.query_params.get("role") or "").strip().lower()
    start_date = (request.query_params.get("start_date") or "").strip()
    end_date = (request.query_params.get("end_date") or "").strip()
    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    stmt = (
        select(AuditLog)
        .where(AuditLog.entity_type == "auth")
        .where(AuditLog.action.in_(["login", "logout"]))
    )
    if username:
        stmt = stmt.where(AuditLog.username == username)
    if start_dt is not None:
        stmt = stmt.where(AuditLog.created_at >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(AuditLog.created_at < end_dt)

    stmt = stmt.order_by(AuditLog.created_at.desc()).limit(2000)
    rows = db.execute(stmt).scalars().all()

    def fmt_dt(dt: Optional[datetime]) -> str:
        if dt is None:
            return ""
        try:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(dt)

    def parse_detail(raw: Optional[str]) -> dict:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    if role_filter and role_filter != "all":
        filtered_rows: list[AuditLog] = []
        for ev in rows:
            detail = parse_detail(ev.detail)
            role = str(detail.get("role") or "").strip().lower()
            if role == role_filter:
                filtered_rows.append(ev)
        rows = filtered_rows

    events = list(reversed(rows))
    pending_login: dict[str, dict] = {}
    sessions: list[dict] = []

    for ev in events:
        username = (ev.username or "").strip() or "(desconocido)"
        detail = parse_detail(ev.detail)
        role = str(detail.get("role") or "") or "-"
        if ev.action == "login":
            pending_login[username] = {"login_at": ev.created_at, "role": role}
            continue

        if ev.action == "logout":
            login_info = pending_login.pop(username, None)
            login_at = (login_info or {}).get("login_at")
            logout_at = ev.created_at
            duration_seconds = None
            if isinstance(login_at, datetime) and isinstance(logout_at, datetime):
                try:
                    duration_seconds = max(0, int((logout_at - login_at).total_seconds()))
                except Exception:
                    duration_seconds = None

            sessions.append(
                {
                    "username": username,
                    "role": (login_info or {}).get("role") or role,
                    "login_at": fmt_dt(login_at) if login_at else "",
                    "logout_at": fmt_dt(logout_at),
                    "duration_seconds": duration_seconds,
                }
            )

    sessions.sort(key=lambda r: (r.get("logout_at") or r.get("login_at") or ""), reverse=True)

    event_rows: list[dict] = []
    for ev in rows[:200]:
        detail = parse_detail(ev.detail)
        event_rows.append(
            {
                "at": fmt_dt(ev.created_at),
                "username": (ev.username or "").strip() or "(desconocido)",
                "action": ev.action,
                "role": str(detail.get("role") or "") or "-",
            }
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_activity.html",
        context={
            "sessions": sessions,
            "events": event_rows,
            "username_filter": username,
            "role_filter": role_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )
