from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import Business
from app.security import (
    get_active_business_code,
    get_active_business_id,
    get_current_user_from_session,
    get_user_business_ids,
    require_active_business_id,
)
from app.services.finance_service import FinanceService
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService
from app.services.product_service import ProductService
from app.utils import month_range
from app.settings import app_mode_label, is_desktop
from app.sync.client import get_sync_state

from .ui_common import (
    dt_to_local_input,
    ensure_admin_or_owner,
    parse_dt,
    templates,
)
from .ui_tabs_common import _home_locations_context, _location_id_for_code, _month_label_es

router = APIRouter()


def _home_charts_context(
    reporting: ReportingService,
    now: datetime,
    location_id: Optional[int] = None,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> dict:
    # Charts default to current month unless a date window is provided.
    chart_start: datetime
    chart_end: datetime
    profit_items: list[dict]
    if start_dt is None and end_dt is None:
        chart_start, chart_end = month_range(now)
        _summary, profit_items = reporting.monthly_profit_report(now=now, location_id=location_id)
        month_label = _month_label_es(now)
    else:
        now_dt = now
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        chart_end = end_dt or (now_dt + timedelta(days=1))
        anchor_dt = chart_end - timedelta(seconds=1)
        chart_start = start_dt or month_range(anchor_dt)[0]

        total_sales, items = reporting.sales_by_product(
            start=chart_start,
            end=chart_end,
            location_id=location_id,
        )
        _ = total_sales
        profit_items = items

        label_parts: list[str] = []
        if start_dt is not None:
            label_parts.append(f"{chart_start.date().isoformat()}")
        else:
            label_parts.append("-")
        label_parts.append("a")
        label_parts.append((chart_end - timedelta(days=1)).date().isoformat() if chart_end else "-")
        month_label = " ".join(label_parts)

    pie_labels: list[str] = []
    pie_values: list[float] = []
    pie_qtys: list[float] = []
    if profit_items:
        items_sorted = sorted(profit_items, key=lambda r: float(r.get("sales") or 0), reverse=True)
        top_n = 30
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

    daily = reporting.daily_sales_series(start=chart_start, end=chart_end, location_id=location_id)
    monthly_sales_daily_line_json = json.dumps(
        {
            "labels": [d.get("day") for d in daily],
            "sales": [d.get("sales", 0) for d in daily],
        }
    )

    if start_dt is None and end_dt is None:
        monthly = reporting.monthly_overview(months=12, now=now, location_id=location_id)
    else:
        end_anchor = (chart_end - timedelta(seconds=1)) if chart_end is not None else now
        start_month = datetime(chart_start.year, chart_start.month, 1, tzinfo=timezone.utc)
        end_month = datetime(end_anchor.year, end_anchor.month, 1, tzinfo=timezone.utc)
        months = (end_month.year - start_month.year) * 12 + (end_month.month - start_month.month) + 1
        months = max(1, int(months))
        monthly = reporting.monthly_overview(
            months=months,
            now=end_anchor,
            location_id=location_id,
            start_dt=chart_start,
            end_dt=chart_end,
        )
    monthly_chart_json = json.dumps(
        {
            "labels": [m.get("month") for m in monthly],
            "sales": [m.get("sales", 0) for m in monthly],
            "purchases": [m.get("purchases", 0) for m in monthly],
            "profit": [m.get("gross_profit", 0) for m in monthly],
        }
    )

    metrics_items = reporting.sales_metrics_table(
        now=now,
        months=12,
        location_id=location_id,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    return {
        "month_label": month_label,
        "monthly_sales_pie_json": monthly_sales_pie_json,
        "monthly_sales_daily_line_json": monthly_sales_daily_line_json,
        "monthly_chart_json": monthly_chart_json,
        "metrics_items": metrics_items,
        "metrics_start_date_value": dt_to_local_input(start_dt)[:10] if start_dt else "",
        "metrics_end_date_value": dt_to_local_input((end_dt - timedelta(seconds=1)))[:10] if end_dt else "",
    }


@router.get("/", response_class=HTMLResponse)
def ui_root() -> RedirectResponse:
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    active_business_id = get_active_business_id(db, request)
    businesses = []
    active_business = None
    if active_business_id is not None:
        active_business = db.get(Business, int(active_business_id))
    if user is not None:
        role = (user.role or "").lower()
        if role == "admin":
            businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        elif role == "owner":
            assigned = get_user_business_ids(db, user.id)
            if assigned:
                businesses = list(
                    db.scalars(
                        select(Business)
                        .where(Business.id.in_(assigned))
                        .order_by(Business.code.asc())
                    )
                )
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "user": user,
            "businesses": businesses,
            "active_business_id": active_business_id,
            "active_business": active_business,
            "app_mode": app_mode_label(),
            "state": get_sync_state(db) if is_desktop() else None,
        },
    )


@router.get("/tabs/home", response_class=HTMLResponse)
def tab_home(
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
    metrics_start_date: Optional[str] = None,
    metrics_end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)
    reporting = ReportingService(db, business_id=bid)
    finance = FinanceService(db, business_id=bid)

    locations, _default_loc_code = _home_locations_context(db, request)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(
        db,
        selected_location_code,
        business_id=bid,
        business_code=business_code,
    )

    products = product_service.list()
    stock_items = inventory_service.stock_list(location_code=selected_location_code or None)
    restock_items = [i for i in stock_items if i.needs_restock]

    products_metric = len(products)
    if selected_location_code:
        products_metric = sum(1 for i in stock_items if float(i.quantity or 0) > 0)

    totals = {
        "products": products_metric,
        "stock_total": float(sum(i.quantity for i in stock_items)),
        "to_restock": len(restock_items),
    }

    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    _summary, profit_items = reporting.monthly_profit_report(now=now, location_id=selected_location_id)

    month_label = _month_label_es(now)

    top_margin = None
    if profit_items:
        top_margin = max(profit_items, key=lambda r: float(r.get("gross_pct") or 0))

    year_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    year_end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    year_total_sales, year_items = reporting.sales_by_product(
        start=year_start,
        end=year_end,
        location_id=selected_location_id,
    )
    year_items_nonzero = [i for i in year_items if float(i.get("sales") or 0) > 0]
    top_year = year_items_nonzero[0] if year_items_nonzero else None
    bottom_year = year_items_nonzero[-1] if year_items_nonzero else None
    if year_items_nonzero:
        top_year = max(year_items_nonzero, key=lambda r: float(r.get("sales") or 0))
        bottom_year = min(year_items_nonzero, key=lambda r: float(r.get("sales") or 0))

    top_year_pct = ((float(top_year.get("sales") or 0) / year_total_sales) * 100.0) if (top_year and year_total_sales) else 0.0
    bottom_year_pct = ((float(bottom_year.get("sales") or 0) / year_total_sales) * 100.0) if (bottom_year and year_total_sales) else 0.0

    inventory_value_total = inventory_service.inventory_value_total(location_id=selected_location_id)
    inventory_sale_value_total = inventory_service.inventory_sale_value_total(location_id=selected_location_id)

    top_expense = None
    if not selected_location_code:
        top_expense = finance.top_expense_concept(start=start, end=end)

    start_dt = parse_dt(metrics_start_date) if metrics_start_date else None
    end_dt = parse_dt(metrics_end_date) if metrics_end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    charts_ctx = _home_charts_context(
        reporting,
        now,
        location_id=selected_location_id,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_home.html",
        context={
            "totals": totals,
            "locations": locations,
            "selected_location_code": selected_location_code,
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
            **charts_ctx,
        },
    )


@router.get("/home-charts", response_class=HTMLResponse)
def home_charts(
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
    metrics_start_date: Optional[str] = None,
    metrics_end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    reporting = ReportingService(db, business_id=bid)
    now = datetime.now(timezone.utc)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(
        db,
        selected_location_code,
        business_id=bid,
        business_code=business_code,
    )

    start_dt = parse_dt(metrics_start_date) if metrics_start_date else None
    end_dt = parse_dt(metrics_end_date) if metrics_end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    charts_ctx = _home_charts_context(
        reporting,
        now,
        location_id=selected_location_id,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/home_charts.html",
        context={
            "selected_location_code": selected_location_code,
            **charts_ctx,
        },
    )
