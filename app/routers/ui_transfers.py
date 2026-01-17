from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.sql import func

from app.audit import log_event
from app.deps import session_dep
from app.schemas import TransferCreate, TransferLineCreate
from app.security import get_active_business_id, get_current_user_from_session
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
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv = db.get(InventoryMovement, out_id)
    if mv is None or mv.type != "transfer_out":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    if bid is not None and int(getattr(mv, "business_id", 0) or 0) != int(bid):
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
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv_out = db.get(InventoryMovement, out_id)
    from_code = default_from_location_code
    to_code = default_to_location_code
    try:
        if mv_out is not None:
            from_code, to_code, _ref = service._transfer_codes_from_out_note(str(mv_out.note or ""))
    except Exception:
        from_code = default_from_location_code
        to_code = default_to_location_code

    product_options = [
        p for p in service.stock_list(query="", location_code=from_code) if float(p.quantity or 0) > 0
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
                "message": "Traspaso actualizado",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "all_locations": all_locations,
                "from_location_code": from_code,
                "to_location_code": to_code,
                "default_from_location_code": default_from_location_code,
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
                "message": "Error al actualizar traspaso",
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
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv_out = db.get(InventoryMovement, out_id)
    from_code = default_from_location_code
    to_code = default_to_location_code
    try:
        if mv_out is not None:
            from_code, to_code, _ref = service._transfer_codes_from_out_note(str(mv_out.note or ""))
    except Exception:
        from_code = default_from_location_code
        to_code = default_to_location_code

    product_options = [
        p for p in service.stock_list(query="", location_code=from_code) if float(p.quantity or 0) > 0
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
                "message": "Traspaso eliminado",
                "message_class": "ok",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "all_locations": all_locations,
                "from_location_code": from_code,
                "to_location_code": to_code,
                "default_from_location_code": default_from_location_code,
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
                "message": "Error al eliminar traspaso",
                "message_detail": str(e.detail),
                "message_class": "error",
            },
        )
        response.headers["X-Modal-Keep"] = "1"
        return response


@router.post("/transfers", response_class=HTMLResponse)
async def create_transfer(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    config = load_business_config()
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = str(getattr(config.locations, "default_pos", "POS1") or "POS1")

    form = await request.form()

    from_location_code = (form.get("from_location_code") or "").strip() or default_from_location_code
    to_location_code = (form.get("to_location_code") or "").strip() or default_to_location_code
    movement_date_raw = (form.get("movement_date") or "").strip()
    note = (form.get("note") or "").strip() or None

    product_options = [
        p for p in service.stock_list(query="", location_code=from_location_code) if float(p.quantity or 0) > 0
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
                from_location_code=from_location_code,
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
                            "from_location_code": result.from_location_code,
                            "to_location_code": result.to_location_code,
                            "transfer_ref": result.transfer_ref,
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
                "message": "Traspaso registrado",
                "message_detail": f"Destino: {result.to_location_code}. Líneas: {len(result.lines)}",
                "message_class": "ok",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "all_locations": all_locations,
                "from_location_code": from_location_code,
                "to_location_code": to_location_code,
                "default_from_location_code": default_from_location_code,
                "default_to_location_code": default_to_location_code,
                "note_value": "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
                "transfer_ref": result.transfer_ref,
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
                "message": "Error en traspaso",
                "message_detail": str(getattr(e, "detail", e)),
                "message_class": "error",
                "movement_date_default": movement_date_raw or dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": product_options,
                "all_locations": all_locations,
                "from_location_code": from_location_code,
                "to_location_code": to_location_code,
                "default_from_location_code": default_from_location_code,
                "default_to_location_code": default_to_location_code,
                "note_value": note or "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
                "preserved_lines": preserved_lines,
            },
        )
        response.status_code = 200
        return response


@router.get("/transfers/product-options", response_class=HTMLResponse)
def transfer_product_options(
    request: Request,
    db: Session = Depends(session_dep),
    from_location_code: str = "",
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = load_business_config()
    from_code = (from_location_code or "").strip() or str(cfg.locations.central.code).strip()
    options = [
        p
        for p in service.stock_list(query="", location_code=from_code)
        if float(getattr(p, "quantity", 0) or 0) > 0
    ]
    parts: list[str] = []
    for p in options:
        sku = str(getattr(p, "sku", "") or "")
        name = str(getattr(p, "name", "") or "")
        if not sku:
            continue
        parts.append(f'<option value="{sku}">{sku} - {name}</option>')
    return HTMLResponse("\n".join(parts))


@router.get("/transfers/stock/{sku}", response_class=HTMLResponse)
async def get_transfer_stock(
    sku: str,
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    
    try:
        product = product_service.get_by_sku(sku)
        if not product:
            return HTMLResponse("0")
        
        config = load_business_config()
        effective_code = (location_code or "").strip() or str(config.locations.central.code)
        stock = service.stock_for_location(sku, location_code=effective_code)
        return HTMLResponse(str(int(stock)))
    except Exception:
        return HTMLResponse("0")


@router.get("/transfers/print", response_class=HTMLResponse)
def transfer_print(
    request: Request,
    db: Session = Depends(session_dep),
    ref: str = "",
    movement_id: int = 0,
) -> HTMLResponse:
    ensure_admin(db, request)
    bid = get_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = load_business_config()

    ref_clean = (ref or "").strip()
    if not ref_clean and movement_id:
        mv = db.get(InventoryMovement, int(movement_id))
        if mv is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
        if bid is not None and int(getattr(mv, "business_id", 0) or 0) != int(bid):
            raise HTTPException(status_code=404, detail="Transfer not found")
        raw = str(mv.note or "")
        if "ref=" in raw:
            try:
                ref_clean = raw.split("ref=", 1)[1].split()[0].strip()
            except Exception:
                ref_clean = ""

    if not ref_clean:
        raise HTTPException(status_code=422, detail="ref is required")

    mvs = list(
        db.scalars(
            select(InventoryMovement)
            .where(
                InventoryMovement.type == "transfer_out",
                InventoryMovement.note.ilike(f"%ref={ref_clean}%"),
            )
            .order_by(InventoryMovement.movement_date.asc(), InventoryMovement.id.asc())
        )
    )
    if bid is not None:
        mvs = [m for m in mvs if int(getattr(m, "business_id", 0) or 0) == int(bid)]
    if not mvs:
        raise HTTPException(status_code=404, detail="Transfer not found")

    from_code, to_code, _ref = service._transfer_codes_from_out_note(str(mvs[0].note or ""))
    movement_date = mvs[0].movement_date

    code_to_name = {str(cfg.locations.central.code): str(cfg.locations.central.name)}
    for loc in (cfg.locations.pos or []):
        code_to_name[str(loc.code)] = str(loc.name)

    stmt = (
        select(
            Product.sku,
            Product.name,
            func.coalesce(func.sum(func.abs(InventoryMovement.quantity)), 0).label("qty"),
            func.coalesce(Product.default_sale_price, 0).label("sale_price"),
        )
        .select_from(InventoryMovement)
        .join(Product, Product.id == InventoryMovement.product_id)
        .where(
            InventoryMovement.type == "transfer_out",
            InventoryMovement.note.ilike(f"%ref={ref_clean}%"),
        )
        .group_by(Product.id)
        .order_by(Product.sku.asc())
    )
    if bid is not None:
        stmt = stmt.where(InventoryMovement.business_id == int(bid))
    rows = db.execute(stmt).all()

    items: list[dict] = []
    for sku, name, qty, sale_price in rows:
        sp = float(sale_price or 0)
        items.append(
            {
                "sku": str(sku or ""),
                "name": str(name or ""),
                "qty": float(qty or 0),
                "sale_price": sp,
                "sale_price_10": sp * 0.10,
            }
        )

    return templates.TemplateResponse(
        request=request,
        name="transfer_print.html",
        context={
            "ref": ref_clean,
            "from_code": from_code,
            "to_code": to_code,
            "from_name": code_to_name.get(from_code, from_code),
            "to_name": code_to_name.get(to_code, to_code),
            "movement_date": movement_date,
            "items": items,
        },
    )
