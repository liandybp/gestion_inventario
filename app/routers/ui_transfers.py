from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.schemas import TransferCreate, TransferLineCreate
from app.security import get_current_user_from_session
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.business_config import load_business_config
from app.models import InventoryMovement, Product

from .ui_common import dt_to_local_input, ensure_admin, parse_dt, templates

router = APIRouter()


@router.get("/movement/transfer/{movement_id}/edit", response_class=HTMLResponse)
def transfer_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv = db.get(InventoryMovement, out_id)
    if mv is None or mv.type != "transfer_out":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    product = db.get(Product, mv.product_id)

    note_value = ""
    raw = str(mv.note or "")
    if ":" in raw:
        note_value = raw.split(":", 1)[1].strip()
    return templates.TemplateResponse(
        request=request,
        name="partials/transfer_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "quantity_value": (abs(float(mv.quantity or 0)) if float(mv.quantity or 0) < 0 else float(mv.quantity or 0)),
            "note_value": note_value,
        },
    )


@router.post("/movement/transfer/{movement_id}/update", response_class=HTMLResponse)
def transfer_update(
    request: Request,
    movement_id: int,
    quantity: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    central_code = str(config.locations.central.code).strip()
    product_options = [
        p for p in service.stock_list(query="", location_code=central_code) if float(p.quantity or 0) > 0
    ]

    try:
        result = service.update_transfer_shipment(
            movement_id=movement_id,
            quantity=float(quantity),
            movement_date=parse_dt(movement_date),
            note=note or None,
        )
        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="transfer_update",
                entity_type="movement",
                entity_id=str(result.movement.id),
                detail={"quantity": float(quantity)},
            )

        recent_transfer_out = service.movement_history(movement_type="transfer_out", start_date=None, end_date=None, limit=50)
        recent_transfer_in = service.movement_history(movement_type="transfer_in", start_date=None, end_date=None, limit=50)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "Envío actualizado",
                "message_detail": f"Stock en CENTRAL después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "pos_locations": pos_locations,
                "default_to_location_code": default_to_location_code,
                "note_value": "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
            },
        )
    except HTTPException as e:
        out_id = service._transfer_out_id_for_movement_id(movement_id)
        mv = db.get(InventoryMovement, out_id)
        product = db.get(Product, mv.product_id) if mv else None

        response = templates.TemplateResponse(
            request=request,
            name="partials/transfer_edit_form.html",
            context={
                "movement": mv,
                "product_label": f"{product.sku} - {product.name}" if product else "",
                "movement_date_value": dt_to_local_input(mv.movement_date) if mv else "",
                "quantity_value": float(quantity or 0),
                "note_value": note or "",
                "message": "Error al actualizar envío",
                "message_detail": str(e.detail),
                "message_class": "error",
            },
        )
        response.headers["X-Modal-Keep"] = "1"
        return response


@router.post("/movement/transfer/{movement_id}/delete", response_class=HTMLResponse)
def transfer_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")
    central_code = str(config.locations.central.code).strip()
    product_options = [
        p for p in service.stock_list(query="", location_code=central_code) if float(p.quantity or 0) > 0
    ]

    try:
        service.delete_transfer_shipment(movement_id)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="transfer_delete",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={},
            )

        recent_transfer_out = service.movement_history(movement_type="transfer_out", start_date=None, end_date=None, limit=50)
        recent_transfer_in = service.movement_history(movement_type="transfer_in", start_date=None, end_date=None, limit=50)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "Envío eliminado",
                "message_class": "ok",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "pos_locations": pos_locations,
                "default_to_location_code": default_to_location_code,
                "note_value": "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
            },
        )
    except HTTPException as e:
        out_id = service._transfer_out_id_for_movement_id(movement_id)
        mv = db.get(InventoryMovement, out_id)
        product = db.get(Product, mv.product_id) if mv else None

        response = templates.TemplateResponse(
            request=request,
            name="partials/transfer_edit_form.html",
            context={
                "movement": mv,
                "product_label": f"{product.sku} - {product.name}" if product else "",
                "movement_date_value": dt_to_local_input(mv.movement_date) if mv else "",
                "quantity_value": abs(float(mv.quantity or 0)) if mv else 0,
                "note_value": "",
                "message": "Error al eliminar envío",
                "message_detail": str(e.detail),
                "message_class": "error",
            },
        )
        response.headers["X-Modal-Keep"] = "1"
        return response


@router.post("/transfers", response_class=HTMLResponse)
async def create_transfer(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)

    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    form = await request.form()

    to_location_code = (form.get("to_location_code") or "").strip()
    movement_date_raw = (form.get("movement_date") or "").strip()
    note = (form.get("note") or "").strip() or None

    central_code = str(config.locations.central.code).strip()
    product_options = [
        p for p in service.stock_list(query="", location_code=central_code) if float(p.quantity or 0) > 0
    ]

    products = form.getlist("product")
    quantities = form.getlist("quantity")

    lines: list[TransferLineCreate] = []
    for p, q in zip(products, quantities):
        sku = (p or "").strip()
        if " - " in sku:
            sku = sku.split(" - ", 1)[0].strip()
        if not sku:
            continue
        try:
            qty = float((q or "").strip() or 0)
        except Exception:
            qty = 0
        if qty <= 0:
            continue
        lines.append(TransferLineCreate(sku=sku, quantity=qty))

    movement_date = parse_dt(movement_date_raw) if movement_date_raw else None

    user = get_current_user_from_session(db, request)

    try:
        result = service.transfer(
            TransferCreate(
                to_location_code=to_location_code,
                lines=lines,
                movement_date=movement_date,
                note=note,
            )
        )

        if user is not None:
            for line in result.lines:
                for mv_id in (line.movements_out or []) + (line.movements_in or []):
                    log_event(
                        db,
                        user,
                        action="transfer_create",
                        entity_type="movement",
                        entity_id=str(mv_id),
                        detail={
                            "sku": line.sku,
                            "quantity": float(line.quantity),
                            "to_location_code": result.to_location_code,
                        },
                    )

        recent_transfer_out = service.movement_history(
            movement_type="transfer_out",
            start_date=None,
            end_date=None,
            limit=50,
        )
        recent_transfer_in = service.movement_history(
            movement_type="transfer_in",
            start_date=None,
            end_date=None,
            limit=50,
        )

        return templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "Envío registrado",
                "message_detail": f"Destino: {result.to_location_code}. Líneas: {len(result.lines)}",
                "message_class": "ok",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "pos_locations": pos_locations,
                "default_to_location_code": default_to_location_code,
                "note_value": "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
            },
        )
    except Exception as e:
        recent_transfer_out = service.movement_history(
            movement_type="transfer_out",
            start_date=None,
            end_date=None,
            limit=50,
        )
        recent_transfer_in = service.movement_history(
            movement_type="transfer_in",
            start_date=None,
            end_date=None,
            limit=50,
        )
        preserved_lines = []
        for p, q in zip(products, quantities):
            sku = (p or "").strip()
            qty_str = (q or "").strip()
            if sku or qty_str:
                preserved_lines.append({"product": p, "quantity": qty_str})
        
        response = templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "Error en envío",
                "message_detail": str(getattr(e, "detail", e)),
                "message_class": "error",
                "movement_date_default": movement_date_raw or dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "pos_locations": pos_locations,
                "default_to_location_code": default_to_location_code,
                "to_location_code": to_location_code,
                "note_value": note or "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
                "preserved_lines": preserved_lines,
            },
        )
        response.status_code = 200
        return response


@router.get("/transfers/stock/{sku}", response_class=HTMLResponse)
async def get_transfer_stock(sku: str, request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    product_service = ProductService(db)
    
    try:
        product = product_service.get_by_sku(sku)
        if not product:
            return HTMLResponse("0")
        
        config = load_business_config()
        central_code = config.locations.central.code
        stock = service.stock_for_location(sku, location_code=central_code)
        return HTMLResponse(str(int(stock)))
    except Exception:
        return HTMLResponse("0")
