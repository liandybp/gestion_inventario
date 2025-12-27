from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import InventoryMovement, Product
from app.schemas import SaleCreate
from app.security import get_current_user_from_session
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


@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    user = get_current_user_from_session(db, request)
    sku = extract_sku(product)
    try:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
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
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
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
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.post("/sale/barcode", response_class=HTMLResponse)
def sale_barcode(
    request: Request,
    barcode: str = Form(...),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    try:
        sku = barcode_to_sku(db, barcode)
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
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
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
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
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
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
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.get("/movement/sale/{movement_id}/edit", response_class=HTMLResponse)
def sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "sale":
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = db.get(Product, mv.product_id)
    product_service = ProductService(db)
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
    service = InventoryService(db)
    product_service = ProductService(db)
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
            },
            status_code=e.status_code,
        )


@router.post("/movement/sale/{movement_id}/delete", response_class=HTMLResponse)
def sale_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
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
            },
            status_code=400,
        )
