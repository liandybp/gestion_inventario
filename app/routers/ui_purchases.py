from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import InventoryLot, InventoryMovement, Product
from app.schemas import PurchaseCreate
from app.security import get_current_user_from_session
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

from .ui_common import (
    _DEV_ACTIONS_ENABLED,
    dt_to_local_input,
    ensure_admin,
    extract_sku,
    parse_dt,
    parse_optional_float,
    templates,
)

router = APIRouter()


@router.post("/purchase", response_class=HTMLResponse)
def purchase(
    request: Request,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_cost: str = Form(""),
    movement_date: Optional[str] = Form(None),
    lot_code: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    product_service = ProductService(db)
    sku = extract_sku(product)
    try:
        result = service.purchase(
            PurchaseCreate(
                sku=sku,
                quantity=quantity,
                unit_cost=parse_optional_float(unit_cost),
                movement_date=parse_dt(movement_date),
                lot_code=lot_code or None,
                note=note or None,
            )
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="purchase_create",
                entity_type="movement",
                entity_id=str(result.movement.id),
                detail={"sku": sku, "quantity": quantity, "unit_cost": parse_optional_float(unit_cost)},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Compra registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Error en compra",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.get("/purchase/{movement_id}/label", response_class=HTMLResponse)
def purchase_label_print(
    request: Request,
    movement_id: int,
    copies: int = 1,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = db.get(Product, mv.product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")

    label = {"sku": product.sku, "name": product.name}
    return templates.TemplateResponse(
        request=request,
        name="label_single_print.html",
        context={"label": label, "copies": max(1, int(copies or 1))},
    )


@router.get("/movement/purchase/{movement_id}/edit", response_class=HTMLResponse)
def purchase_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = db.get(Product, mv.product_id)
    lot = db.scalar(select(InventoryLot).where(InventoryLot.movement_id == mv.id))
    product_service = ProductService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/purchase_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "lot_code": lot.lot_code if lot else "",
            "product_options": product_service.search(query="", limit=200),
        },
    )


@router.post("/movement/purchase/{movement_id}/update", response_class=HTMLResponse)
def purchase_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_cost: float = Form(...),
    movement_date: Optional[str] = Form(None),
    lot_code: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    ensure_admin(db, request)
    sku = extract_sku(product)
    try:
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
                action="purchase_update",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={"sku": sku, "quantity": quantity, "unit_cost": unit_cost},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Compra actualizada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Error al actualizar compra",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.post("/movement/purchase/{movement_id}/delete", response_class=HTMLResponse)
def purchase_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    try:
        ensure_admin(db, request)
        service.delete_purchase_movement(movement_id)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="purchase_delete",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Compra eliminada",
                "message_class": "ok",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar compra",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar compra",
                "message_detail": str(e),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.post("/dev/reset-purchases-sales", response_class=HTMLResponse)
def dev_reset_purchases_sales(
    request: Request,
    panel: str = Form("purchase"),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    if not _DEV_ACTIONS_ENABLED:
        raise HTTPException(status_code=404, detail="Not found")

    ensure_admin(db, request)

    service = InventoryService(db)
    product_service = ProductService(db)
    service.reset_purchases_and_sales()

    if panel == "sale":
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Ventas y compras borradas",
                "message_detail": "Se eliminaron todos los movimientos de compra y venta.",
                "message_class": "ok",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/purchase_panel.html",
        context={
            "message": "Ventas y compras borradas",
            "message_detail": "Se eliminaron todos los movimientos de compra y venta.",
            "message_class": "ok",
            "purchases": service.recent_purchases(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
        },
    )
