from __future__ import annotations

import html
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.business_config import load_business_config
from app.deps import session_dep
from app.models import Customer, InventoryMovement, Product, SalesDocument
from app.schemas import SaleCreate
from app.security import get_active_business_id, get_current_user_from_session
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

from .ui_common import (
    barcode_to_sku,
    dt_to_local_input,
    ensure_admin,
    extract_sku,
    parse_dt,
    parse_optional_float,
    templates,
)

router = APIRouter()


@router.get("/sale/product-options", response_class=HTMLResponse)
def sale_product_options(
    request: Request,
    location_code: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    if user is None:
        raise HTTPException(status_code=403, detail="Not authenticated")
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    config = load_business_config()
    default_sale_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    selected_location_code = (location_code or "").strip() or default_sale_location_code

    items = [
        p
        for p in service.stock_list(query="", location_code=selected_location_code)
        if float(p.quantity or 0) > 0
    ]

    parts: list[str] = []
    for p in items:
        sku = html.escape(str(getattr(p, "sku", "") or ""))
        name = html.escape(str(getattr(p, "name", "") or ""))
        parts.append(f'<option value="{sku} - {name}"></option>')
    return HTMLResponse("".join(parts))


def _sales_doc_context(db: Session, request: Request) -> dict:
    config = load_business_config()
    bid = get_active_business_id(db, request)
    session = getattr(request, "session", None) or {}
    cart = session.get("sales_doc_cart")
    if not isinstance(cart, list):
        cart = []
    draft = session.get("sales_doc_draft")
    if not isinstance(draft, dict):
        draft = {}
    doc_stmt = select(SalesDocument)
    if bid is not None:
        doc_stmt = doc_stmt.where(SalesDocument.business_id == int(bid))
    recent_documents = list(
        db.scalars(doc_stmt.order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(10))
    )
    cust_stmt = select(Customer)
    if bid is not None:
        cust_stmt = cust_stmt.where(Customer.business_id == int(bid))
    customers = list(db.scalars(cust_stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(200)))
    return {
        "sales_doc_config": config.sales_documents.model_dump(),
        "currency": config.currency.model_dump(),
        "issuer": config.issuer.model_dump(),
        "cart": cart,
        "recent_documents": recent_documents,
        "customers": customers,
        "draft": draft,
    }


@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    user = get_current_user_from_session(db, request)
    sku = extract_sku(product)
    config = load_business_config()
    pos_locations = [{"code": l.code, "name": l.name} for l in (config.locations.pos or [])]
    default_sale_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    selected_location_code = (location_code or "").strip() or default_sale_location_code

    product_options = [
        p
        for p in service.stock_list(query="", location_code=selected_location_code)
        if float(p.quantity or 0) > 0
    ]
    try:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
            )
        )
        if user is not None:
            log_event(
                db,
                user,
                action="sale_create",
                entity_type="movement",
                entity_id=str(result.movement.id),
                detail={"sku": sku, "quantity": quantity, "unit_price": parse_optional_float(unit_price)},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Venta registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(db, request),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(db, request),
            },
            status_code=e.status_code,
        )


@router.post("/sale/barcode", response_class=HTMLResponse)
def sale_barcode(
    request: Request,
    barcode: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    config = load_business_config()
    pos_locations = [{"code": l.code, "name": l.name} for l in (config.locations.pos or [])]
    default_sale_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    selected_location_code = (location_code or "").strip() or default_sale_location_code

    product_options = [
        p
        for p in service.stock_list(query="", location_code=selected_location_code)
        if float(p.quantity or 0) > 0
    ]
    try:
        sku = barcode_to_sku(db, barcode)
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
            )
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="sale_create",
                entity_type="movement",
                entity_id=str(result.movement.id),
                detail={"sku": sku, "quantity": quantity, "unit_price": parse_optional_float(unit_price)},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Venta registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
            },
            status_code=e.status_code,
        )
    except Exception as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
            status_code=400,
        )


@router.get("/movement/sale/{movement_id}/edit", response_class=HTMLResponse)
def sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "sale":
        raise HTTPException(status_code=404, detail="Sale movement not found")
    if bid is not None and int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = db.get(Product, mv.product_id)
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="partials/sale_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "product_options": product_service.search(query="", limit=200),
        },
    )


@router.post("/movement/sale/{movement_id}/update", response_class=HTMLResponse)
def sale_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    sku = extract_sku(product)
    try:
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
                action="sale_update",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={"sku": sku, "quantity": quantity, "unit_price": unit_price},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Venta actualizada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error al actualizar venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
            status_code=e.status_code,
        )


@router.post("/movement/sale/{movement_id}/delete", response_class=HTMLResponse)
def sale_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    try:
        service.delete_sale_movement(movement_id)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="sale_delete",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Venta eliminada",
                "message_class": "ok",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar venta",
                "message_detail": str(e),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                **_sales_doc_context(db, request),
            },
            status_code=400,
        )
