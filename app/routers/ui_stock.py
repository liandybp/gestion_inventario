from __future__ import annotations

import html as _html
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import Product
from app.repositories.business_repository import get_active_business_config
from app.schemas import SupplierReturnLotCreate
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.logger import get_logger

from .ui_common import (
    ensure_admin_or_owner,
    templates,
)
from .ui_tabs_common import (
    _deletable_skus,
    _location_id_for_code,
    _location_name_for_code,
    _normalize_restock_scope,
    _restock_items_with_location_history,
)

_log = get_logger(__name__)

router = APIRouter()


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
    _log.debug("stock_table - User: %s, Role: %s, bid: %s, location: %s, category: %s",
               user.username if user else 'None', user.role if user else 'None', bid, location_code, category)
    inventory_service = InventoryService(db, business_id=bid)
    config = get_active_business_config(db, request)
    central_code = str(config.locations.central.code).strip()
    loc = (location_code or "").strip() or None
    effective_code = (loc or central_code).strip()

    # Always default to "all" to show products even with zero stock
    default_filter = "all"
    effective_filter = stock_filter if stock_filter in ("all", "in_stock", "zero") else default_filter

    items = inventory_service.stock_list(query=query, location_code=loc, limit=200)
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

    config = get_active_business_config(db, request)
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
        _log.exception("delete producto fallback en stock delete: %r", sku)
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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)

    config = get_active_business_config(db, request)
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
                _log.exception("delete-selected producto fallback %s: %s", sku, e)
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
    config = get_active_business_config(db, request)

    locations = [{"code": config.locations.central.code, "name": config.locations.central.name}]
    pos_locations: list[dict[str, str]] = []
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            code = str(loc.code)
            name = str(loc.name)
            locations.append({"code": code, "name": name})
            pos_locations.append({"code": code, "name": name})

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
        _log.error("product_options fallback en devoluciones")
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
        _log.error("categories fallback en devoluciones")
        categories = []

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
        db.commit()
        message = "Devolución registrada"
        message_detail = f"SKU: {(sku or '').strip()}"
        message_class = "ok"
    except Exception as e:
        _log.exception("devolución fallback en supplier_return: %s", e)
        message = "No se pudo registrar la devolución"
        message_detail = str(getattr(e, "detail", e))
        message_class = "error"

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={
            "locations": locations,
            "pos_locations": pos_locations,
            "default_location_code": config.locations.central.code,
            "selected_location_code": selected_location_code,
            "product_options": product_options,
            "categories": categories,
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
        _log.exception("available_lots fallback en ui_supplier_return_lots: %s", e)
        error_msg = _html.escape(str(e)[:50]) if str(e) else "Error"
        return HTMLResponse(f"<select name='lot_id' disabled><option value=''>Error: {error_msg}</option></select>")

    if not lots:
        return HTMLResponse("<select name='lot_id' disabled><option value=''>Sin lotes en esta ubicación</option></select>")

    parts: list[str] = ["<select name='lot_id' required>"]
    parts.append("<option value=''>-- Selecciona lote --</option>")
    for lot in lots:
        parts.append(
            "<option value='{}'>{} | disp={} | costo={}</option>".format(
                int(lot.id),
                _html.escape(str(lot.lot_code or "")),
                float(lot.qty_remaining or 0),
                float(lot.unit_cost or 0),
            )
        )
    parts.append("</select>")
    return HTMLResponse("".join(parts))


@router.get("/restock-table", response_class=HTMLResponse)
def restock_table(
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
    restock_scope: str = "with_history",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(
        db,
        selected_location_code,
        business_id=bid,
        business_code=business_code,
    )
    selected_scope = _normalize_restock_scope(restock_scope)
    items = [
        i
        for i in inventory_service.stock_list(location_code=selected_location_code or None)
        if i.needs_restock
    ]
    if selected_scope == "with_history":
        items = _restock_items_with_location_history(
            db,
            items,
            business_id=bid,
            location_id=selected_location_id,
        )
    items.sort(key=lambda r: float(getattr(r, "reorder_shortage", 0) or 0), reverse=True)
    return templates.TemplateResponse(
        request=request,
        name="partials/restock_table.html",
        context={
            "items": items,
            "selected_location_code": selected_location_code,
            "selected_location_name": _location_name_for_code(selected_location_code, db, request),
            "restock_scope": selected_scope,
        },
    )


@router.get("/restock-print", response_class=HTMLResponse)
def restock_print(
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
    restock_scope: str = "with_history",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_code = get_active_business_code(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    selected_location_code = (location_code or "").strip()
    selected_location_id = _location_id_for_code(
        db,
        selected_location_code,
        business_id=bid,
        business_code=business_code,
    )
    selected_scope = _normalize_restock_scope(restock_scope)
    items = [
        i
        for i in inventory_service.stock_list(location_code=selected_location_code or None)
        if i.needs_restock
    ]
    if selected_scope == "with_history":
        items = _restock_items_with_location_history(
            db,
            items,
            business_id=bid,
            location_id=selected_location_id,
        )
    return templates.TemplateResponse(
        request=request,
        name="restock_print.html",
        context={
            "items": items,
            "selected_location_code": selected_location_code,
            "selected_location_name": _location_name_for_code(selected_location_code, db, request),
            "print_mode": True,
            "generated_at": datetime.now(timezone.utc),
        },
    )
