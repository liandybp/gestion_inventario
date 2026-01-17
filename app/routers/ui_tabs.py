from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, Request
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
from app.security import get_active_business_code, get_active_business_id, get_current_user_from_session
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.business_config import load_business_config
from app.schemas import SupplierReturnLotCreate

from .ui_common import dt_to_local_input, ensure_admin, ensure_admin_or_owner, month_range, parse_dt, templates

router = APIRouter()


def _deletable_skus(db: Session, skus: list[str]) -> set[str]:
    clean = [str(s).strip() for s in (skus or []) if str(s).strip()]
    if not clean:
        return set()

    rows = db.execute(select(Product.id, Product.sku).where(Product.sku.in_(clean))).all()
    if not rows:
        return set()

    product_ids = [int(pid) for pid, _ in rows]
    id_to_sku = {int(pid): str(sku) for pid, sku in rows}

    movement_pids = set(
        db.scalars(
            select(InventoryMovement.product_id)
            .where(InventoryMovement.product_id.in_(product_ids))
            .distinct()
        ).all()
    )
    lot_pids = set(
        db.scalars(
            select(InventoryLot.product_id)
            .where(InventoryLot.product_id.in_(product_ids))
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


def _parse_ym(ym: Optional[str]) -> Optional[datetime]:
    if not ym:
        return None
    s = ym.strip()
    if not s:
        return None
    try:
        parts = s.split("-")
        if len(parts) != 2:
            return None
        year = int(parts[0])
        month = int(parts[1])
        if month < 1 or month > 12:
            return None
        return datetime(year, month, 1, tzinfo=timezone.utc)
    except Exception:
        return None


def _home_charts_context(inventory_service: InventoryService, now: datetime, location_id: Optional[int] = None) -> dict:
    start, end = month_range(now)
    _summary, profit_items = inventory_service.monthly_profit_report(now=now, location_id=location_id)

    month_label = _month_label_es(now)

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

    daily = inventory_service.daily_sales_series(start=start, end=end, location_id=location_id)
    monthly_sales_daily_line_json = json.dumps(
        {
            "labels": [d.get("day") for d in daily],
            "sales": [d.get("sales", 0) for d in daily],
        }
    )

    monthly = inventory_service.monthly_overview(months=12, now=now, location_id=location_id)
    monthly_chart_json = json.dumps(
        {
            "labels": [m.get("month") for m in monthly],
            "sales": [m.get("sales", 0) for m in monthly],
            "purchases": [m.get("purchases", 0) for m in monthly],
            "profit": [m.get("gross_profit", 0) for m in monthly],
        }
    )

    metrics_items = inventory_service.sales_metrics_table(now=now, months=12, location_id=location_id)

    return {
        "month_label": month_label,
        "monthly_sales_pie_json": monthly_sales_pie_json,
        "monthly_sales_daily_line_json": monthly_sales_daily_line_json,
        "monthly_chart_json": monthly_chart_json,
        "metrics_items": metrics_items,
    }


def _home_locations_context(business_code: Optional[str] = None) -> tuple[list[dict], str]:
    config = load_business_config(business_code)
    locations: list[dict] = [{"code": "", "name": "General"}]
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})
    default_code = ""
    return locations, default_code


def _location_id_for_code(db: Session, location_code: str) -> Optional[int]:
    code = (location_code or "").strip()
    if not code:
        return None
    row = db.execute(select(Location.id).where(Location.code == code)).first()
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
    bid = get_active_business_id(db, request)
    q = (query or "").strip()
    stmt = select(Customer)
    if bid is not None:
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
    ym: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = get_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)

    locations, _default_loc_code = _home_locations_context(business_code)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(db, selected_location_code)

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

    now = _parse_ym(ym) or datetime.now(timezone.utc)
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

    charts_ctx = _home_charts_context(inventory_service, now, location_id=selected_location_id)
    selected_ym = now.strftime("%Y-%m")

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
            "selected_ym": selected_ym,
            **charts_ctx,
        },
    )


@router.get("/home-charts", response_class=HTMLResponse)
def home_charts(
    request: Request,
    db: Session = Depends(session_dep),
    ym: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    now = _parse_ym(ym) or datetime.now(timezone.utc)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(db, selected_location_code)
    charts_ctx = _home_charts_context(inventory_service, now, location_id=selected_location_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/home_charts.html",
        context={
            "selected_ym": now.strftime("%Y-%m"),
            "selected_location_code": selected_location_code,
            **charts_ctx,
        },
    )


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
    config = load_business_config(get_active_business_code(db, request))
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
    
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={
            "locations": locations,
            "default_location_code": config.locations.central.code,
            "product_options": product_options,
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
    bid = get_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
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
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
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

    config = load_business_config(get_active_business_code(db, request))
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

    doc_stmt = select(SalesDocument)
    if bid is not None:
        doc_stmt = doc_stmt.where(SalesDocument.business_id == int(bid))
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

    cust_stmt = select(Customer)
    if bid is not None:
        cust_stmt = cust_stmt.where(Customer.business_id == int(bid))
    customers = list(db.scalars(cust_stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_sale_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

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
            ),
            "filter_month": display_month,
            "filter_year": display_year,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "filter_show_all": bool(show_all),
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
    bid = get_active_business_id(db, request)
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

    doc_stmt = select(SalesDocument)
    if bid is not None:
        doc_stmt = doc_stmt.where(SalesDocument.business_id == int(bid))
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
    cust_stmt = select(Customer)
    if bid is not None:
        cust_stmt = cust_stmt.where(Customer.business_id == int(bid))
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
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
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
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
    user = get_current_user_from_session(db, request)

    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)
    config = load_business_config(get_active_business_code(db, request))

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

    start_dt = parse_dt(start_date) if start_date else None
    end_dt = parse_dt(end_date) if end_date else None
    if end_dt is not None:
        end_dt = end_dt + timedelta(days=1)

    recent_transfer_out = inventory_service.movement_history(
        movement_type="transfer_out",
        query=query,
        start_date=start_dt,
        end_date=end_dt,
        limit=50,
    )
    recent_transfer_in = inventory_service.movement_history(
        movement_type="transfer_in",
        query=query,
        start_date=start_dt,
        end_date=end_dt,
        limit=50,
    )

    message = None
    message_detail = None
    message_class = None
    show_only_in = False
    
    if success == 1:
        message = "Traspaso registrado"
        message_detail = "El traspaso se ha creado correctamente"
        message_class = "ok"
        show_only_in = True

    product_options = [
        p for p in inventory_service.stock_list(query="", location_code=selected_from_code) if float(p.quantity or 0) > 0
    ]

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
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
        },
    )


@router.get("/tabs/profit", response_class=HTMLResponse)
def tab_profit(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
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
def tab_profit_items(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    summary, items = inventory_service.monthly_profit_items_report()
    q = (query or "").strip()
    if q:
        items = [
            r
            for r in items
            if query_match(
                q,
                str(getattr(r, "sku", "") or ""),
                str(getattr(r, "name", "") or ""),
                str(getattr(r, "category", "") or ""),
                str(getattr(r, "lot_code", "") or ""),
            )
        ]
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit_items.html",
        context={
            "summary": summary,
            "items": items,
            "filter_query": query,
        },
    )


@router.get("/stock-table", response_class=HTMLResponse)
def stock_table(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    location_code: str = "",
    stock_filter: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)
    config = load_business_config(get_active_business_code(db, request))
    central_code = str(config.locations.central.code).strip()
    loc = (location_code or "").strip() or None
    effective_code = (loc or central_code).strip()

    default_filter = "all" if effective_code == central_code else "in_stock"
    effective_filter = stock_filter if stock_filter in ("all", "in_stock", "zero") else default_filter

    items = service.stock_list(query=query, location_code=loc)
    if effective_filter == "in_stock":
        items = [i for i in items if float(i.quantity or 0) > 0]
    elif effective_filter == "zero":
        items = [i for i in items if float(i.quantity or 0) <= 0]

    deletable_skus = _deletable_skus(db, [i.sku for i in items])
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "deletable_skus": deletable_skus,
            "stock_filter": effective_filter,
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
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = get_active_business_id(db, request)
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
    if effective_filter == "in_stock":
        items = [i for i in items if float(i.quantity or 0) > 0]
    elif effective_filter == "zero":
        items = [i for i in items if float(i.quantity or 0) <= 0]

    deletable_skus = _deletable_skus(db, [i.sku for i in items])
    response = templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={
            "items": items,
            "user": user,
            "deletable_skus": deletable_skus,
            "stock_filter": effective_filter,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
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
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
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
