from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.utils import month_range, query_match
from app.audit import log_event
from app.deps import session_dep
from app.models import AuditLog
from app.models import Business
from app.models import Customer
from app.models import User
from app.models import InventoryLot
from app.models import InventoryMovement
from app.models import Location
from app.models import Product
from app.models import SalesDocument
from app.security import (
    get_active_business_code,
    get_active_business_id,
    get_current_user_from_session,
    require_active_business_id,
)
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.business_config import load_business_config
from app.schemas import SupplierReturnLotCreate

from .ui_common import (
    dt_to_local_input,
    ensure_admin,
    ensure_admin_or_owner,
    extract_sku,
    month_range,
    parse_dt,
    templates,
)

router = APIRouter()


def _deletable_skus(db: Session, skus: list[str], *, business_id: int) -> set[str]:
    clean = [str(s).strip() for s in (skus or []) if str(s).strip()]
    if not clean:
        return set()

    rows = db.execute(
        select(Product.id, Product.sku).where(
            Product.sku.in_(clean),
            Product.business_id == int(business_id),
        )
    ).all()
    if not rows:
        return set()

    product_ids = [int(pid) for pid, _ in rows]
    id_to_sku = {int(pid): str(sku) for pid, sku in rows}

    movement_pids = set(
        db.scalars(
            select(InventoryMovement.product_id)
            .where(InventoryMovement.product_id.in_(product_ids))
            .where(InventoryMovement.business_id == int(business_id))
            .distinct()
        ).all()
    )
    lot_pids = set(
        db.scalars(
            select(InventoryLot.product_id)
            .where(InventoryLot.product_id.in_(product_ids))
            .where(InventoryLot.business_id == int(business_id))
            .distinct()
        ).all()
    )
    blocked = movement_pids | lot_pids

    return {sku for pid, sku in id_to_sku.items() if pid not in blocked}


def _month_label_es(dt: datetime) -> str:
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
    return f"{month_names[int(dt.month) - 1].title()} {dt.year}"


def _home_charts_context(
    inventory_service: InventoryService,
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
        _summary, profit_items = inventory_service.monthly_profit_report(now=now, location_id=location_id)
        month_label = _month_label_es(now)
    else:
        now_dt = now
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=timezone.utc)
        now_dt = now_dt.astimezone(timezone.utc)

        chart_end = end_dt or (now_dt + timedelta(days=1))
        anchor_dt = chart_end - timedelta(seconds=1)
        chart_start = start_dt or month_range(anchor_dt)[0]

        total_sales, items = inventory_service.sales_by_product(
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

    daily = inventory_service.daily_sales_series(start=chart_start, end=chart_end, location_id=location_id)
    monthly_sales_daily_line_json = json.dumps(
        {
            "labels": [d.get("day") for d in daily],
            "sales": [d.get("sales", 0) for d in daily],
        }
    )

    if start_dt is None and end_dt is None:
        monthly = inventory_service.monthly_overview(months=12, now=now, location_id=location_id)
    else:
        end_anchor = (chart_end - timedelta(seconds=1)) if chart_end is not None else now
        start_month = datetime(chart_start.year, chart_start.month, 1, tzinfo=timezone.utc)
        end_month = datetime(end_anchor.year, end_anchor.month, 1, tzinfo=timezone.utc)
        months = (end_month.year - start_month.year) * 12 + (end_month.month - start_month.month) + 1
        months = max(1, int(months))
        monthly = inventory_service.monthly_overview(
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

    metrics_items = inventory_service.sales_metrics_table(
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


def _home_locations_context(business_code: Optional[str] = None) -> tuple[list[dict], str]:
    config = load_business_config(business_code)
    locations: list[dict] = [{"code": "", "name": "General"}]
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})
    default_code = ""
    return locations, default_code


def _location_id_for_code(
    db: Session,
    location_code: str,
    business_id: int,
    business_code: Optional[str] = None,
) -> Optional[int]:
    code = (location_code or "").strip()
    if not code:
        return None

    row = db.execute(
        select(Location.id).where(
            Location.code == code,
            Location.business_id == int(business_id),
        )
    ).first()
    if row:
        return int(row[0])

    bcode = (business_code or "").strip()
    if not bcode:
        return None

    prefix = "".join(ch if ch.isalnum() else "_" for ch in (bcode.upper() or "BUSINESS"))
    alt_code = code if code.startswith(f"{prefix}_") else f"{prefix}_{code}"
    row = db.execute(
        select(Location.id).where(
            Location.code == alt_code,
            Location.business_id == int(business_id),
        )
    ).first()
    if not row:
        return None
    return int(row[0])


def _location_name_for_code(location_code: str, business_code: Optional[str] = None) -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(business_code)
    for loc in locations:
        if (str(loc.get("code") or "").strip()) == code:
            name = str(loc.get("name") or "").strip()
            return name or (code or "General")
    return code or "General"


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
    if user is not None and (user.role or "").lower() == "admin":
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "user": user,
            "businesses": businesses,
            "active_business_id": active_business_id,
            "active_business": active_business,
        },
    )


@router.get("/tabs/customers", response_class=HTMLResponse)
def tab_customers(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    _ = get_current_user_from_session(db, request)
    bid = require_active_business_id(db, request)
    q = (query or "").strip()
    stmt = select(Customer)
    stmt = stmt.where(Customer.business_id == int(bid))
    if q:
        like = f"%{q}%"
        stmt = stmt.where((Customer.name.ilike(like)) | (Customer.client_id.ilike(like)))
    customers = list(db.scalars(stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_customers.html",
        context={
            "customers": customers,
            "query": q,
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

    locations, _default_loc_code = _home_locations_context(business_code)
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
    _summary, profit_items = inventory_service.monthly_profit_report(now=now, location_id=selected_location_id)

    month_label = _month_label_es(now)

    top_margin = None
    if profit_items:
        top_margin = max(profit_items, key=lambda r: float(r.get("gross_pct") or 0))

    year_start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    year_end = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    year_total_sales, year_items = inventory_service.sales_by_product(
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
        top_expense = inventory_service.top_expense_concept(start=start, end=end)

    start_dt = parse_dt(metrics_start_date) if metrics_start_date else None
    end_dt = parse_dt(metrics_end_date) if metrics_end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    charts_ctx = _home_charts_context(
        inventory_service,
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
    inventory_service = InventoryService(db, business_id=bid)
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
        inventory_service,
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


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    print(f"[DEBUG] tab_inventory - User: {user.username if user else 'None'}, Role: {user.role if user else 'None'}, user.business_id: {user.business_id if user else 'None'}, active bid: {bid}")
    business_code = get_active_business_code(db, request)
    config = load_business_config(business_code)
    locations = [{"code": config.locations.central.code, "name": config.locations.central.name}]
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})
    
    product_options = []
    try:
        from app.services.product_service import ProductService
        product_options = ProductService(db, business_id=bid).search(query="", limit=200)
    except Exception:
        product_options = []

    categories: list[str] = []
    try:
        rows = db.execute(
            select(Product.category)
            .where(Product.business_id == int(bid))
            .distinct()
            .order_by(Product.category.asc())
        ).all()
        categories = [str(c or "").strip() for (c,) in rows if str(c or "").strip()]
    except Exception:
        categories = []
    
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={
            "locations": locations,
            "default_location_code": config.locations.central.code,
            "product_options": product_options,
            "categories": categories,
        },
    )


@router.get("/tabs/purchases", response_class=HTMLResponse)
def tab_purchases(
    request: Request,
    month: Optional[str] = None,
    year: Optional[int] = None,
    show_all: Optional[str] = None,
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)
    
    # Determine filter values
    now = datetime.now(timezone.utc)

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    date_range_active = (start_dt is not None) or (end_dt is not None)
    
    # If a date range is provided, don't apply month/year filtering.
    if date_range_active:
        filter_month = None
        filter_year = None
        display_month = ""
        display_year = now.year
    # If show_all is set, don't filter by month/year
    elif show_all:
        filter_month = None
        filter_year = None
        display_month = ''
        display_year = now.year
    # If month or year is explicitly provided, use those values
    elif month is not None or year is not None:
        filter_month = month if month else None
        filter_year = year if year else now.year
        display_month = month if month else ''
        display_year = filter_year
    # Default: show current month
    else:
        filter_month = now.strftime('%m')
        filter_year = now.year
        display_month = filter_month
        display_year = filter_year

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_purchases.html",
        context={
            "user": user,
            "products": product_service.recent(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "purchases": inventory_service.recent_purchases(
                query=query,
                limit=100,
                start_date=start_dt,
                end_date=end_dt,
                month=filter_month,
                year=filter_year,
            ),
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            "filter_month": display_month,
            "filter_year": display_year,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "filter_show_all": bool(show_all),
        },
    )


@router.get("/tabs/sales", response_class=HTMLResponse)
def tab_sales(
    request: Request,
    month: Optional[str] = None,
    year: Optional[int] = None,
    show_all: Optional[str] = None,
    location_code: str = "",
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)
    
    # Determine filter values
    now = datetime.now(timezone.utc)

    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    date_range_active = (start_dt is not None) or (end_dt is not None)
    
    # If a date range is provided, don't apply month/year filtering.
    if date_range_active:
        filter_month = None
        filter_year = None
        display_month = ""
        display_year = now.year
    # If show_all is set, don't filter by month/year
    elif show_all:
        filter_month = None
        filter_year = None
        display_month = ''
        display_year = now.year
    # If month or year is explicitly provided, use those values
    elif month is not None or year is not None:
        filter_month = month if month else None
        filter_year = year if year else now.year
        display_month = month if month else ''
        display_year = filter_year
    # Default: show current month
    else:
        filter_month = now.strftime('%m')
        filter_year = now.year
        display_month = filter_month
        display_year = filter_year

    business_code = get_active_business_code(db, request)
    config = load_business_config(business_code)
    session = getattr(request, "session", None) or {}
    cart = session.get("sales_doc_cart")
    if not isinstance(cart, list):
        cart = []
    draft = session.get("sales_doc_draft")
    if not isinstance(draft, dict):
        draft = {}

    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    doc_stmt = select(SalesDocument).where(SalesDocument.business_id == int(bid))
    if start_dt is not None:
        doc_stmt = doc_stmt.where(SalesDocument.issue_date >= start_dt)
    if end_dt is not None:
        doc_stmt = doc_stmt.where(SalesDocument.issue_date < end_dt)
    doc_stmt = doc_stmt.order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(200)
    recent_documents = list(db.scalars(doc_stmt))
    q = (query or "").strip()
    if q:
        recent_documents = [
            d
            for d in recent_documents
            if query_match(q, str(getattr(d, "code", "") or ""), str(getattr(d, "client_name", "") or ""))
        ]
    recent_documents = recent_documents[:10]

    cust_stmt = select(Customer).where(Customer.business_id == int(bid))
    customers = list(db.scalars(cust_stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_sale_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    selected_filter_location_code = (location_code or "").strip()
    selected_filter_location_id = _location_id_for_code(
        db,
        selected_filter_location_code,
        business_id=bid,
        business_code=business_code,
    )

    product_options = [
        p
        for p in inventory_service.stock_list(query="", location_code=default_sale_location_code)
        if float(p.quantity or 0) > 0
    ]
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_sales.html",
        context={
            "user": user,
            "sales": inventory_service.recent_sales(
                query=query,
                limit=100,
                start_date=start_dt,
                end_date=end_dt,
                month=filter_month,
                year=filter_year,
                location_id=selected_filter_location_id,
            ),
            "filter_month": display_month,
            "filter_year": display_year,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "filter_show_all": bool(show_all),
            "filter_location_code": selected_filter_location_code,
            "product_options": product_options,
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            "pos_locations": pos_locations,
            "default_sale_location_code": default_sale_location_code,
            "sale_location_code": default_sale_location_code,
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": cart,
            "recent_documents": recent_documents,
            "customers": customers,
            "draft": draft,
        },
    )


@router.get("/tabs/documents", response_class=HTMLResponse)
def tab_documents(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    _ = get_current_user_from_session(db, request)
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    config = load_business_config(get_active_business_code(db, request))

    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_doc_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    session = getattr(request, "session", None) or {}
    cart = session.get("sales_doc_cart")
    if not isinstance(cart, list):
        cart = []
    draft = session.get("sales_doc_draft")
    if not isinstance(draft, dict):
        draft = {}

    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    doc_stmt = select(SalesDocument).where(SalesDocument.business_id == int(bid))
    if start_dt is not None:
        doc_stmt = doc_stmt.where(SalesDocument.issue_date >= start_dt)
    if end_dt is not None:
        doc_stmt = doc_stmt.where(SalesDocument.issue_date < end_dt)
    doc_stmt = doc_stmt.order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(200)
    recent_documents = list(db.scalars(doc_stmt))
    q = (query or "").strip()
    if q:
        recent_documents = [
            d
            for d in recent_documents
            if query_match(q, str(getattr(d, "code", "") or ""), str(getattr(d, "client_name", "") or ""))
        ]
    recent_documents = recent_documents[:10]
    cust_stmt = select(Customer).where(Customer.business_id == int(bid))
    customers = list(db.scalars(cust_stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_documents.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "pos_locations": pos_locations,
            "default_doc_location_code": default_doc_location_code,
            "cart": cart,
            "recent_documents": recent_documents,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "customers": customers,
            "draft": draft,
            "product_options": product_service.search(query="", limit=200),
        },
    )


@router.get("/tabs/expenses", response_class=HTMLResponse)
def tab_expenses(
    request: Request,
    month: Optional[str] = None,
    year: Optional[int] = None,
    show_all: Optional[str] = None,
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    
    now = datetime.now(timezone.utc)
    
    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    # If show_all is set, show all records
    if show_all:
        start = None
        end = None
        display_month = ''
        display_year = now.year
    # If month or year is explicitly provided, use those values
    elif month is not None or year is not None:
        target_year = year if year else now.year
        target_month = int(month) if month else now.month
        target_date = datetime(target_year, target_month, 1, tzinfo=timezone.utc)
        start, end = month_range(target_date)
        display_month = month if month else ''
        display_year = target_year
    # Default: show current month
    else:
        start, end = month_range(now)
        display_month = now.strftime('%m')
        display_year = now.year
    
    eff_start = start_dt if start_dt is not None else start
    eff_end = end_dt if end_dt is not None else end

    expenses = inventory_service.list_expenses(start=eff_start, end=eff_end, limit=500)
    q = (query or "").strip()
    if q:
        expenses = [e for e in expenses if query_match(q, str(e.concept or ""))]
    expenses = expenses[:200]
    total = inventory_service.total_expenses(start=eff_start, end=eff_end)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_expenses.html",
        context={
            "expenses": expenses,
            "expenses_total": total,
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            "filter_month": display_month,
            "filter_year": display_year,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "filter_show_all": bool(show_all),
        },
    )


@router.get("/tabs/dividends", response_class=HTMLResponse)
def tab_dividends(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    now = datetime.now(timezone.utc)
    month_start, month_end = month_range(now)
    config = load_business_config(get_active_business_code(db, request))
    summary = service.monthly_dividends_report(now=now)

    start_dt = parse_dt(start_date) if start_date else month_start
    end_dt = parse_dt(end_date) if end_date else month_end
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    extractions = service.list_extractions(start=start_dt, end=end_dt, limit=500)
    q = (query or "").strip()
    if q:
        extractions = [
            r
            for r in extractions
            if query_match(q, str(r.concept or ""), str(r.party or ""))
        ]
    extractions = extractions[:200]
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_dividends.html",
        context={
            "summary": summary,
            "extractions": extractions,
            "movement_date_default": dt_to_local_input(now),
            "dividends": config.dividends.model_dump(),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.get("/tabs/transfers", response_class=HTMLResponse)
def tab_transfers(
    request: Request,
    db: Session = Depends(session_dep),
    success: int = 0,
    from_location_code: str = "",
    to_location_code: str = "",
    filter_from_location_code: str = "",
    filter_to_location_code: str = "",
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)

    inventory_service = InventoryService(db, business_id=bid)
    business_code = get_active_business_code(db, request)
    config = load_business_config(business_code)

    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    selected_from_code = (from_location_code or "").strip() or default_from_location_code
    selected_to_code = (to_location_code or "").strip() or default_to_location_code

    history_from_code = (filter_from_location_code or "").strip()
    history_to_code = (filter_to_location_code or "").strip()
    history_from_id = (
        _location_id_for_code(db, history_from_code, business_id=bid, business_code=business_code)
        if history_from_code
        else None
    )
    history_to_id = (
        _location_id_for_code(db, history_to_code, business_id=bid, business_code=business_code)
        if history_to_code
        else None
    )

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    message = None
    message_detail = None
    message_class = None
    show_only_in = False

    try:
        recent_transfer_out = inventory_service.movement_history(
            movement_type="transfer_out",
            query=query,
            location_id=history_from_id,
            start_date=start_dt,
            end_date=end_dt,
            limit=50,
        )
        recent_transfer_in = inventory_service.movement_history(
            movement_type="transfer_in",
            query=query,
            location_id=history_to_id,
            start_date=start_dt,
            end_date=end_dt,
            limit=50,
        )
    except Exception as e:
        recent_transfer_out = []
        recent_transfer_in = []
        message = "Error al cargar historial de traspasos"
        message_detail = str(getattr(e, "detail", e))
        message_class = "error"

    if history_to_code:
        pat = re.compile(r"Transfer\s+[^\s:;]+->" + re.escape(history_to_code) + r"\b")
        recent_transfer_out = [r for r in recent_transfer_out if pat.search(str(r[10] or ""))]

    if history_from_code:
        needle = f"Transfer in from {history_from_code}".lower()
        recent_transfer_in = [r for r in recent_transfer_in if needle in str(r[10] or "").lower()]

    if success == 1:
        message = "Traspaso registrado"
        message_detail = "El traspaso se ha creado correctamente"
        message_class = "ok"
        show_only_in = True

    product_options = []
    try:
        product_options = [
            p
            for p in inventory_service.stock_list(query="", location_code=selected_from_code)
            if float(p.quantity or 0) > 0
        ]
    except Exception as e:
        if message is None:
            message = "Error al cargar traspasos"
            message_detail = str(getattr(e, "detail", e))
            message_class = "error"

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_transfers.html",
        context={
            "user": user,
            "product_options": product_options,
            "all_locations": all_locations,
            "from_location_code": selected_from_code,
            "to_location_code": selected_to_code,
            "default_from_location_code": default_from_location_code,
            "default_to_location_code": default_to_location_code,
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            "rows_count": 12,
            "recent_transfer_out": recent_transfer_out,
            "recent_transfer_in": recent_transfer_in,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
            "show_only_in": show_only_in,
            "filter_from_location_code": history_from_code,
            "filter_to_location_code": history_to_code,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
        },
    )


@router.get("/tabs/profit", response_class=HTMLResponse)
def tab_profit(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
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
def tab_profit_items(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    return _render_profit_items_tab(
        request=request,
        db=db,
        query=query,
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/tabs/profit-items/adjustment/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_adjustment_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "adjustment":
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    product = db.get(Product, mv.product_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/profit_items_adjustment_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.post("/tabs/profit-items/adjustment/{movement_id}/update", response_class=HTMLResponse)
def profit_items_adjustment_update(
    request: Request,
    movement_id: int,
    unit_cost: float = Form(...),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    service.update_adjustment_movement(movement_id=movement_id, unit_cost=float(unit_cost))

    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_adjustment_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"unit_cost": float(unit_cost)},
        )
    return _render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date)


@router.get("/tabs/profit-items/transfer-in/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_transfer_in_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "transfer_in":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    product = db.get(Product, mv.product_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/profit_items_transfer_in_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.post("/tabs/profit-items/transfer-in/{movement_id}/update", response_class=HTMLResponse)
def profit_items_transfer_in_update(
    request: Request,
    movement_id: int,
    unit_cost: float = Form(...),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "transfer_in":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Transfer movement not found")

    result = service.update_transfer_movement(
        movement_id=movement_id,
        quantity=float(mv.quantity or 0),
        unit_cost=float(unit_cost),
        movement_date=mv.movement_date,
        note=mv.note,
    )
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_transfer_in_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"unit_cost": float(unit_cost)},
        )
    return _render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date)


def _render_profit_items_tab(
    *,
    request: Request,
    db: Session,
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    message: Optional[str] = None,
    message_detail: Optional[str] = None,
    message_class: Optional[str] = None,
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    inventory_service = InventoryService(db, business_id=bid)

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    summary, items = inventory_service.monthly_profit_items_report(start=start_dt, end=end_dt)
    q = (query or "").strip()
    if q:
        def _field(row, key: str) -> str:
            if isinstance(row, dict):
                return str(row.get(key, "") or "")
            return str(getattr(row, key, "") or "")

        items = [
            r
            for r in items
            if query_match(
                q,
                _field(r, "sku"),
                _field(r, "name"),
                _field(r, "category"),
                _field(r, "lot_code"),
            )
        ]
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit_items.html",
        context={
            "user": user,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
            "summary": summary,
            "items": items,
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.get("/tabs/profit-items/sale/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "sale":
        raise HTTPException(status_code=404, detail="Sale movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = db.get(Product, mv.product_id)
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="partials/profit_items_sale_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "product_options": product_service.search(query="", limit=200),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.post("/tabs/profit-items/sale/{movement_id}/update", response_class=HTMLResponse)
def profit_items_sale_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    sku = extract_sku(product)
    result = service.update_sale(
        movement_id=movement_id,
        sku=sku,
        quantity=quantity,
        unit_price=unit_price,
        movement_date=parse_dt(movement_date),
        note=note or None,
    )
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_sale_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"sku": sku, "quantity": float(quantity), "unit_price": float(unit_price)},
        )
    return _render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date)


@router.post("/tabs/profit-items/sale/{movement_id}/delete", response_class=HTMLResponse)
def profit_items_sale_delete(
    request: Request,
    movement_id: int,
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    service.delete_sale_movement(movement_id)
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_sale_delete",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={},
        )
    return _render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date)


@router.post("/tabs/profit-items/sale/delete-selected", response_class=HTMLResponse)
def profit_items_sale_delete_selected(
    request: Request,
    sale_ids: list[int] = Form([]),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)

    ids: list[int] = []
    seen: set[int] = set()
    for raw in (sale_ids or []):
        try:
            mid = int(raw)
        except Exception:
            continue
        if mid <= 0 or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)

    if not ids:
        return _render_profit_items_tab(
            request=request,
            db=db,
            query=query,
            start_date=start_date,
            end_date=end_date,
            message="No se seleccionó ninguna venta",
            message_class="warn",
        )

    deleted = 0
    errors: list[str] = []
    for mid in ids:
        try:
            service.delete_sale_movement(mid)
            deleted += 1
            if user is not None:
                log_event(
                    db,
                    user,
                    action="profit_items_sale_delete",
                    entity_type="movement",
                    entity_id=str(mid),
                    detail={"bulk": True},
                )
        except HTTPException as e:
            errors.append(f"{mid}: {e.detail}")
        except Exception as e:
            errors.append(f"{mid}: {e}")

    msg = "Ventas eliminadas" if deleted > 0 else "No se pudo eliminar"
    detail = f"Se eliminaron {deleted} venta(s)."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:5])
    return _render_profit_items_tab(
        request=request,
        db=db,
        query=query,
        start_date=start_date,
        end_date=end_date,
        message=msg,
        message_detail=detail,
        message_class="ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
    )


@router.get("/tabs/profit-items/purchase/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_purchase_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = db.get(Product, mv.product_id)
    lot = db.scalar(select(InventoryLot).where(InventoryLot.movement_id == mv.id))
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="partials/profit_items_purchase_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "lot_code": lot.lot_code if lot else "",
            "product_options": product_service.search(query="", limit=200),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.post("/tabs/profit-items/purchase/{movement_id}/update", response_class=HTMLResponse)
def profit_items_purchase_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_cost: float = Form(...),
    movement_date: Optional[str] = Form(None),
    lot_code: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    sku = extract_sku(product)
    result = service.update_purchase(
        movement_id=movement_id,
        sku=sku,
        quantity=quantity,
        unit_cost=unit_cost,
        movement_date=parse_dt(movement_date),
        lot_code=lot_code or None,
        note=note or None,
    )
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_purchase_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"sku": sku, "quantity": float(quantity), "unit_cost": float(unit_cost)},
        )
    return _render_profit_items_tab(request=request, db=db, query=query, start_date=start_date, end_date=end_date)


@router.get("/stock-table", response_class=HTMLResponse)
def stock_table(
    request: Request,
    location_code: str = "",
    stock_filter: str = "all",
    category: str = "",
    query: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    print(f"[DEBUG] stock_table - User: {user.username if user else 'None'}, Role: {user.role if user else 'None'}, bid: {bid}, location: {location_code}, category: {category}")
    inventory_service = InventoryService(db, business_id=bid)
    config = load_business_config(get_active_business_code(db, request))
    central_code = str(config.locations.central.code).strip()
    loc = (location_code or "").strip() or None
    effective_code = (loc or central_code).strip()

    # Always default to "all" to show products even with zero stock
    default_filter = "all"
    effective_filter = stock_filter if stock_filter in ("all", "in_stock", "zero") else default_filter

    items = inventory_service.stock_list(query=query, location_code=loc)
    cat = (category or "").strip()
    if cat:
        items = [i for i in items if str(getattr(i, "category", "") or "").strip() == cat]
    if effective_filter == "in_stock":
        items = [i for i in items if float(i.quantity or 0) > 0]
    elif effective_filter == "zero":
        items = [i for i in items if float(i.quantity or 0) <= 0]

    deletable_skus = _deletable_skus(db, [i.sku for i in items], business_id=int(bid))
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "deletable_skus": deletable_skus,
            "stock_filter": effective_filter,
            "selected_category": cat,
        },
    )


@router.post("/stock/{sku}/delete", response_class=HTMLResponse)
def stock_delete_product(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
    query: str = Form(""),
    location_code: str = Form(""),
    stock_filter: str = Form(""),
    category: str = Form(""),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)

    config = load_business_config(get_active_business_code(db, request))
    central_code = str(config.locations.central.code).strip()

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

    loc = (location_code or "").strip() or None
    effective_code = (loc or central_code).strip()

    default_filter = "all" if effective_code == central_code else "in_stock"
    effective_filter = stock_filter if stock_filter in ("all", "in_stock", "zero") else default_filter

    items = inventory_service.stock_list(query=query, location_code=loc)
    cat = (category or "").strip()
    if cat:
        items = [i for i in items if str(getattr(i, "category", "") or "").strip() == cat]
    if effective_filter == "in_stock":
        items = [i for i in items if float(i.quantity or 0) > 0]
    elif effective_filter == "zero":
        items = [i for i in items if float(i.quantity or 0) <= 0]

    deletable_skus = _deletable_skus(db, [i.sku for i in items], business_id=int(bid))
    response = templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "deletable_skus": deletable_skus,
            "stock_filter": effective_filter,
            "selected_category": cat,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
        },
    )
    response.headers["HX-Trigger"] = "stockTableRefresh"
    return response


@router.post("/stock/delete-selected", response_class=HTMLResponse)
def stock_delete_selected(
    request: Request,
    skus: list[str] = Form([]),
    db: Session = Depends(session_dep),
    query: str = Form(""),
    location_code: str = Form(""),
    stock_filter: str = Form(""),
    category: str = Form(""),
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)

    config = load_business_config(get_active_business_code(db, request))
    central_code = str(config.locations.central.code).strip()

    clean_skus: list[str] = []
    seen: set[str] = set()
    for raw in (skus or []):
        s = str(raw or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        clean_skus.append(s)

    deleted = 0
    errors: list[str] = []
    if not clean_skus:
        errors.append("No se seleccionó ningún artículo")
    else:
        for sku in clean_skus:
            try:
                product_service.delete(sku)
                deleted += 1
            except HTTPException as e:
                errors.append(f"{sku}: {e.detail}")
            except Exception as e:
                errors.append(f"{sku}: {e}")

    loc = (location_code or "").strip() or None
    effective_code = (loc or central_code).strip()

    default_filter = "all" if effective_code == central_code else "in_stock"
    effective_filter = stock_filter if stock_filter in ("all", "in_stock", "zero") else default_filter

    items = inventory_service.stock_list(query=query, location_code=loc)
    cat = (category or "").strip()
    if cat:
        items = [i for i in items if str(getattr(i, "category", "") or "").strip() == cat]
    if effective_filter == "in_stock":
        items = [i for i in items if float(i.quantity or 0) > 0]
    elif effective_filter == "zero":
        items = [i for i in items if float(i.quantity or 0) <= 0]

    deletable_skus = _deletable_skus(db, [i.sku for i in items], business_id=int(bid))

    message = "Artículos eliminados" if deleted > 0 else "No se pudo eliminar"
    detail = f"Se eliminaron {deleted} artículo(s)."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:6])

    response = templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "deletable_skus": deletable_skus,
            "stock_filter": effective_filter,
            "selected_category": cat,
            "message": message,
            "message_detail": detail,
            "message_class": "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
        },
    )
    response.headers["HX-Trigger"] = "stockTableRefresh"
    return response


@router.post("/movements/return-supplier", response_class=HTMLResponse)
def ui_supplier_return(
    request: Request,
    db: Session = Depends(session_dep),
    sku: str = Form(""),
    lot_id: int = Form(0),
    quantity: float = Form(0),
    note: str = Form(""),
    location_code: str = Form(""),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    config = load_business_config(get_active_business_code(db, request))

    locations = [{"code": config.locations.central.code, "name": config.locations.central.name}]
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})

    selected_location_code = (location_code or "").strip() or config.locations.central.code

    service = InventoryService(db, business_id=bid)
    message = None
    message_detail = None
    message_class = None
    
    product_options = []
    try:
        from app.services.product_service import ProductService
        product_options = ProductService(db, business_id=bid).search(query="", limit=200)
    except Exception:
        product_options = []

    try:
        if int(lot_id or 0) <= 0:
            raise Exception("lot_id is required")
        result = service.supplier_return_by_lot(
            SupplierReturnLotCreate(
                lot_id=int(lot_id),
                quantity=float(quantity or 0),
                note=(note or "").strip() or None,
                location_code=selected_location_code,
            )
        )
        log_event(
            db,
            user,
            action="supplier_return_create",
            entity_type="movement",
            entity_id=str(result.movement.id),
            detail={
                "sku": (sku or "").strip(),
                "lot_id": int(lot_id or 0),
                "quantity": float(quantity or 0),
                "location_code": selected_location_code,
            },
        )
        message = "Devolución registrada"
        message_detail = f"SKU: {(sku or '').strip()}"
        message_class = "ok"
    except Exception as e:
        message = "No se pudo registrar la devolución"
        message_detail = str(getattr(e, "detail", e))
        message_class = "error"

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={
            "locations": locations,
            "default_location_code": config.locations.central.code,
            "selected_location_code": selected_location_code,
            "product_options": product_options,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
        },
    )


@router.get("/return-supplier/lots", response_class=HTMLResponse)
def ui_supplier_return_lots(
    request: Request,
    db: Session = Depends(session_dep),
    sku: str = "",
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    sku_clean = (sku or "").strip()
    loc = (location_code or "").strip() or None
    if not sku_clean:
        return HTMLResponse("<select name='lot_id' disabled><option value=''>-- Ingrese SKU --</option></select>")

    try:
        lots = service.available_lots(sku_clean, location_code=loc)
    except Exception as e:
        error_msg = str(e)[:50] if str(e) else "Error"
        return HTMLResponse(f"<select name='lot_id' disabled><option value=''>Error: {error_msg}</option></select>")

    if not lots:
        return HTMLResponse("<select name='lot_id' disabled><option value=''>Sin lotes en esta ubicación</option></select>")

    parts: list[str] = ["<select name='lot_id' required>"]
    parts.append("<option value=''>-- Selecciona lote --</option>")
    for lot in lots:
        parts.append(
            "<option value='{}'>{} | disp={} | costo={}</option>".format(
                int(lot.id),
                str(lot.lot_code or ""),
                float(lot.qty_remaining or 0),
                float(lot.unit_cost or 0),
            )
        )
    parts.append("</select>")
    return HTMLResponse("".join(parts))


@router.get("/restock-table", response_class=HTMLResponse)
def restock_table(request: Request, db: Session = Depends(session_dep), location_code: str = "") -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    selected_location_code = (location_code or "").strip()
    items = [
        i
        for i in inventory_service.stock_list(location_code=selected_location_code or None)
        if i.needs_restock
    ]
    return templates.TemplateResponse(
        request=request,
        name="partials/restock_table.html",
        context={
            "items": items,
            "selected_location_code": selected_location_code,
            "selected_location_name": _location_name_for_code(selected_location_code, business_code),
        },
    )


@router.get("/restock-print", response_class=HTMLResponse)
def restock_print(request: Request, db: Session = Depends(session_dep), location_code: str = "") -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    selected_location_code = (location_code or "").strip()
    items = [
        i
        for i in inventory_service.stock_list(location_code=selected_location_code or None)
        if i.needs_restock
    ]
    return templates.TemplateResponse(
        request=request,
        name="restock_print.html",
        context={
            "items": items,
            "selected_location_code": selected_location_code,
            "selected_location_name": _location_name_for_code(selected_location_code, business_code),
            "print_mode": True,
            "generated_at": datetime.now(timezone.utc),
        },
    )


@router.get("/tabs/history", response_class=HTMLResponse)
def tab_history(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    movement_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    inventory_service = InventoryService(db, business_id=bid)

    query_filter = (query or "").strip()
    type_filter = movement_type.strip() if movement_type else None
    
    # Default to current month if no dates specified
    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    movements = inventory_service.movement_history(
        query=query_filter,
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
            "user": user,
            "message": None,
            "message_detail": None,
            "message_class": None,
            "filter_query": query_filter,
            "type_filter": type_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )


@router.post("/tabs/history/adjustments/delete-selected", response_class=HTMLResponse)
def history_adjustments_delete_selected(
    request: Request,
    movement_ids: list[int] = Form([]),
    query: str = Form(""),
    movement_type: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)

    ids: list[int] = []
    seen: set[int] = set()
    for raw in (movement_ids or []):
        try:
            mid = int(raw)
        except Exception:
            continue
        if mid <= 0 or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)

    deleted = 0
    errors: list[str] = []
    if not ids:
        errors.append("No se seleccionó ningún ajuste")
    else:
        for mid in ids:
            try:
                inventory_service.delete_adjustment_movement(mid)
                deleted += 1
                if user is not None:
                    log_event(
                        db,
                        user,
                        action="history_adjustment_delete",
                        entity_type="movement",
                        entity_id=str(mid),
                        detail={"bulk": True},
                    )
            except HTTPException as e:
                errors.append(f"{mid}: {e.detail}")
            except Exception as e:
                errors.append(f"{mid}: {e}")

    query_filter = (query or "").strip()
    type_filter = movement_type.strip() if movement_type else None

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    movements = inventory_service.movement_history(
        query=query_filter,
        movement_type=type_filter or None,
        start_date=start_dt,
        end_date=end_dt,
        limit=200,
    )

    msg = "Ajustes eliminados" if deleted > 0 else "No se pudo eliminar"
    detail = f"Se eliminaron {deleted} ajuste(s)."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:6])

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_history.html",
        context={
            "movements": movements,
            "user": user,
            "message": msg,
            "message_detail": detail,
            "message_class": "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
            "filter_query": query_filter,
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
    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
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


@router.get("/tabs/users", response_class=HTMLResponse)
def tab_users(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
        },
    )
