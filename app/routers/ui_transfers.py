from __future__ import annotations
from app.repositories.business_repository import get_active_business_config

from datetime import datetime, timedelta, timezone
from html import escape
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy.sql import func

from app.audit import log_event
from app.deps import session_dep
from app.logger import get_logger
from app.schemas import TransferCreate, TransferLineCreate
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id
from app.services.inventory_service import InventoryService
from app.models import InventoryMovement, Product

from .ui_common import dt_to_local_input, ensure_admin_or_owner, extract_sku, parse_dt, templates

_log = get_logger(__name__)

router = APIRouter()


def _resolve_transfer_sku(db: Session, business_id: int, product_input: str) -> str:
    value = (product_input or "").strip()
    if not value:
        raise HTTPException(status_code=422, detail="product is required")

    extracted = extract_sku(value)
    for candidate in (extracted, value):
        cand = (candidate or "").strip()
        if not cand:
            continue
        p = db.scalar(select(Product).where(Product.business_id == int(business_id), Product.sku == cand))
        if p is not None:
            return str(p.sku)

    by_name = db.scalar(
        select(Product)
        .where(Product.business_id == int(business_id), Product.name.ilike(value))
        .order_by(Product.id.asc())
    )
    if by_name is not None:
        return str(by_name.sku)

    raise HTTPException(status_code=404, detail="Producto no encontrado")


def _parse_transfer_lines(products: list[str], quantities: list[str]) -> list[TransferLineCreate]:
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
            _log.debug("transfer quantity parse skip: %r", q)
            qty = 0
        if qty <= 0:
            continue
        lines.append(TransferLineCreate(sku=sku, quantity=qty))
    return lines


def _stock_options(service: InventoryService, from_code: str) -> list[dict]:
    options = [
        p for p in service.stock_list(query="", location_code=from_code)
        if float(getattr(p, "quantity", 0) or 0) > 0
    ]
    return [
        {
            "sku": str(getattr(p, "sku", "") or ""),
            "name": str(getattr(p, "name", "") or ""),
            "uom": str(getattr(p, "unit_of_measure", "") or ""),
            "stock": float(getattr(p, "quantity", 0) or 0),
        }
        for p in options
        if str(getattr(p, "sku", "") or "")
    ]


@router.get("/movement/transfer/{movement_id}/edit", response_class=HTMLResponse)
def transfer_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv = db.get(InventoryMovement, out_id)
    if mv is None or mv.type != "transfer_out":
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    config = get_active_business_config(db, request)
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = config.locations.default_sale_code()
    from_code = default_from_location_code
    to_code = default_to_location_code

    mv_out: Optional[InventoryMovement] = None

    def _safe_product_options(code: str) -> list:
        try:
            return [
                p for p in service.stock_list(query="", location_code=code) if float(p.quantity or 0) > 0
            ]
        except Exception:
            _log.error("product_options fallback en _safe_product_options")
            return []

    def _tab_error(detail: str) -> HTMLResponse:
        user = get_current_user_from_session(db, request)
        try:
            recent_transfer_out = service.movement_history(
                movement_type="transfer_out", start_date=None, end_date=None, limit=50
            )
            recent_transfer_in = service.movement_history(
                movement_type="transfer_in", start_date=None, end_date=None, limit=50
            )
        except Exception:
            _log.error("recent transfer history fallback en _tab_error")
            recent_transfer_out = []
            recent_transfer_in = []

        return templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "Error al actualizar traspaso",
                "message_detail": detail,
                "message_class": "error",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": _safe_product_options(from_code),
                "all_locations": all_locations,
                "from_location_code": from_code,
                "to_location_code": to_code,
                "default_from_location_code": default_from_location_code,
                "default_to_location_code": default_to_location_code,
                "note_value": note or "",
                "rows_count": 12,
                "recent_transfer_out": recent_transfer_out,
                "recent_transfer_in": recent_transfer_in,
            },
        )

    try:
        out_id = service._transfer_out_id_for_movement_id(movement_id)
        mv_out = db.get(InventoryMovement, out_id)
        if mv_out is None or mv_out.type != "transfer_out":
            raise HTTPException(status_code=404, detail="Transfer movement not found")
        if int(getattr(mv_out, "business_id", 0) or 0) != int(bid):
            raise HTTPException(status_code=404, detail="Transfer movement not found")
    except HTTPException as e:
        return _tab_error(str(e.detail))
    except Exception as e:
        _log.exception("transfer_update lookup fallback: %s", e)
        return _tab_error(str(e))

    from_code = default_from_location_code
    to_code = default_to_location_code
    try:
        if mv_out is not None:
            from_code, to_code, _ref = service._transfer_codes_from_out_note(str(mv_out.note or ""))
    except Exception:
        _log.error("transfer_codes_from_out_note fallback en transfer_update")
        from_code = default_from_location_code
        to_code = default_to_location_code

    product_options = _safe_product_options(from_code)
    if not product_options and from_code != default_from_location_code:
        from_code = default_from_location_code
        product_options = _safe_product_options(from_code)

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
            db.commit()  # H-041: el audit flushea; commit explícito (update_transfer_shipment ya commiteó)

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
        return _tab_error(str(e.detail))
    except Exception as e:
        _log.exception("update_transfer_shipment fallback: %s", e)
        return _tab_error(str(e))


@router.post("/movement/transfer/delete-selected", response_class=HTMLResponse)
def transfer_delete_selected(
    request: Request,
    movement_ids: list[int] = Form([]),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    config = get_active_business_config(db, request)
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = config.locations.default_sale_code()

    from_code = default_from_location_code
    to_code = default_to_location_code

    user = get_current_user_from_session(db, request)
    if not movement_ids:
        recent_transfer_out = service.movement_history(movement_type="transfer_out", start_date=None, end_date=None, limit=50)
        recent_transfer_in = service.movement_history(movement_type="transfer_in", start_date=None, end_date=None, limit=50)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_transfers.html",
            context={
                "user": user,
                "message": "No hay traspasos seleccionados",
                "message_detail": "Selecciona uno o varios traspasos para eliminarlos.",
                "message_class": "warn",
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                "product_options": [
                    p for p in service.stock_list(query="", location_code=from_code) if float(p.quantity or 0) > 0
                ],
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

    out_ids: set[int] = set()
    for mid in movement_ids:
        try:
            out_ids.add(int(service._transfer_out_id_for_movement_id(int(mid))))
        except Exception:
            _log.debug("transfer out_id lookup skip: %r", mid)
            continue

    deleted = 0
    errors: list[str] = []
    for out_id in sorted(out_ids):
        try:
            service.delete_transfer_shipment(int(out_id))
            deleted += 1
            if user is not None:
                log_event(
                    db,
                    user,
                    action="transfer_delete",
                    entity_type="movement",
                    entity_id=str(out_id),
                    detail={"bulk": True},
                )
        except HTTPException as e:
            errors.append(str(e.detail))
        except Exception as e:
            _log.exception("delete_transfer_shipment fallback %s: %s", out_id, e)
            errors.append(str(e))
    db.commit()

    recent_transfer_out = service.movement_history(movement_type="transfer_out", start_date=None, end_date=None, limit=50)
    recent_transfer_in = service.movement_history(movement_type="transfer_in", start_date=None, end_date=None, limit=50)

    msg = "Traspasos eliminados" if deleted > 0 else "No se pudieron eliminar traspasos"
    detail = f"Se eliminaron {deleted} traspaso(s)." if deleted > 0 else "No se eliminó ningún traspaso."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:5])
    msg_class = "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error")

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_transfers.html",
        context={
            "user": user,
            "message": msg,
            "message_detail": detail,
            "message_class": msg_class,
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            "product_options": [
                p for p in service.stock_list(query="", location_code=from_code) if float(p.quantity or 0) > 0
            ],
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


@router.post("/movement/transfer/{movement_id}/delete", response_class=HTMLResponse)
def transfer_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    config = get_active_business_config(db, request)
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = config.locations.default_sale_code()

    out_id = service._transfer_out_id_for_movement_id(movement_id)
    mv_out = db.get(InventoryMovement, out_id)
    from_code = default_from_location_code
    to_code = default_to_location_code
    try:
        if mv_out is not None:
            from_code, to_code, _ref = service._transfer_codes_from_out_note(str(mv_out.note or ""))
    except Exception:
        _log.error("transfer_codes_from_out_note fallback en transfer_delete")
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
            db.commit()

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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    config = get_active_business_config(db, request)
    pos_locations = [
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    ]
    central_code = str(config.locations.central.code).strip()
    all_locations = [{"code": central_code, "name": str(config.locations.central.name)}] + pos_locations
    default_from_location_code = central_code
    default_to_location_code = config.locations.default_sale_code()

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
    lines = _parse_transfer_lines(products, quantities)

    movement_date = parse_dt(movement_date_raw) if movement_date_raw else None

    user = get_current_user_from_session(db, request)

    try:
        if user is not None:
            for line in lines:
                log_event(
                    db,
                    user,
                    action="transfer_create",
                    entity_type="movement",
                    entity_id=None,
                    detail={
                        "sku": line.sku,
                        "quantity": float(line.quantity),
                        "from_location_code": from_location_code,
                        "to_location_code": to_location_code,
                    },
                )
        result = service.transfer(
            TransferCreate(
                from_location_code=from_location_code,
                to_location_code=to_location_code,
                lines=lines,
                movement_date=movement_date,
                note=note,
            )
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
        _log.exception("create_transfer fallback: %s", e)
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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)
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
        label = f"{sku} - {name}" if name else sku
        parts.append(f'<option value="{escape(label)}"></option>')
        parts.append(f'<option value="{escape(sku)}"></option>')
        if name:
            parts.append(f'<option value="{escape(name)}"></option>')
    return HTMLResponse("\n".join(parts))


@router.get("/transfers/stock/{sku}", response_class=HTMLResponse)
async def get_transfer_stock(
    sku: str,
    request: Request,
    db: Session = Depends(session_dep),
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)

    try:
        resolved_sku = _resolve_transfer_sku(db, business_id=bid, product_input=sku)
        config = get_active_business_config(db, request)
        effective_code = (location_code or "").strip() or str(config.locations.central.code)
        stock = service.stock_for_location(resolved_sku, location_code=effective_code)
        return HTMLResponse(f"{float(stock or 0):g}")
    except Exception:
        _log.debug("get_transfer_stock fallback: %r", sku)
        return HTMLResponse("0")


@router.get("/transfers/print", response_class=HTMLResponse)
def transfer_print(
    request: Request,
    db: Session = Depends(session_dep),
    ref: str = "",
    movement_id: int = 0,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)

    ref_clean = (ref or "").strip()
    if not ref_clean and movement_id:
        mv = db.get(InventoryMovement, int(movement_id))
        if mv is None:
            raise HTTPException(status_code=404, detail="Transfer not found")
        if int(getattr(mv, "business_id", 0) or 0) != int(bid):
            raise HTTPException(status_code=404, detail="Transfer not found")
        raw = str(mv.note or "")
        if "ref=" in raw:
            try:
                ref_clean = raw.split("ref=", 1)[1].split()[0].strip()
            except Exception:
                _log.debug("transfer_print ref parse fallback")
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


@router.get("/transfers/bulk", response_class=HTMLResponse)
def transfer_bulk_modal(
    request: Request,
    db: Session = Depends(session_dep),
    from_location_code: str = "",
    to_location_code: str = "",
    movement_date: str = "",
    note: str = "",
    from_date: str = "",
    to_date: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)

    from_code = (from_location_code or "").strip() or str(cfg.locations.central.code).strip()
    to_code = (to_location_code or "").strip() or cfg.locations.default_sale_code()

    code_to_name = {str(cfg.locations.central.code): str(cfg.locations.central.name)}
    for loc in (cfg.locations.pos or []):
        code_to_name[str(loc.code)] = str(loc.name)

    from_name = code_to_name.get(from_code, from_code)
    to_name = code_to_name.get(to_code, to_code)

    if from_code == to_code:
        return templates.TemplateResponse(
            request=request,
            name="partials/transfer_bulk_modal.html",
            context={
                "from_code": from_code,
                "to_code": to_code,
                "from_name": from_name,
                "to_name": to_name,
                "movement_date": movement_date,
                "note": note,
                "from_date": (from_date or "").strip(),
                "to_date": (to_date or "").strip(),
                "products": [],
                "message": "Origen y destino son iguales",
                "message_class": "warn",
                "message_detail": "Selecciona ubicaciones diferentes para poder traspasar.",
                "checked_skus": set(),
                "quantities": {},
            },
        )

    products = _stock_options(service, from_code)

    from_date_s = (from_date or "").strip()
    to_date_s = (to_date or "").strip()
    if from_date_s or to_date_s:
        start_dt = parse_dt(from_date_s) if from_date_s else None
        end_dt = parse_dt(to_date_s) if to_date_s else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)
        mv_filters = [
            InventoryMovement.business_id == bid,
            InventoryMovement.type == "transfer_out",
        ]
        if start_dt is not None:
            mv_filters.append(InventoryMovement.movement_date >= start_dt)
        if end_dt is not None:
            mv_filters.append(InventoryMovement.movement_date < end_dt)
        transferred_skus = set(
            db.scalars(
                select(Product.sku)
                .join(InventoryMovement, InventoryMovement.product_id == Product.id)
                .where(*mv_filters)
                .distinct()
            ).all()
        )
        products = [p for p in products if p["sku"] in transferred_skus]

    return templates.TemplateResponse(
        request=request,
        name="partials/transfer_bulk_modal.html",
        context={
            "from_code": from_code,
            "to_code": to_code,
            "from_name": from_name,
            "to_name": to_name,
            "movement_date": movement_date,
            "note": note,
            "from_date": from_date_s,
            "to_date": to_date_s,
            "products": products,
            "message": "",
            "message_class": "",
            "message_detail": "",
            "checked_skus": set(),
            "quantities": {},
        },
    )


@router.post("/transfers/bulk", response_class=HTMLResponse)
async def transfer_bulk_execute(
    request: Request,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    cfg = get_active_business_config(db, request)

    form = await request.form()

    from_code = (form.get("from_location_code") or "").strip() or str(cfg.locations.central.code).strip()
    to_code = (form.get("to_location_code") or "").strip() or cfg.locations.default_sale_code()
    movement_date_raw = (form.get("movement_date") or "").strip()
    note = (form.get("note") or "").strip() or None

    code_to_name = {str(cfg.locations.central.code): str(cfg.locations.central.name)}
    for loc in (cfg.locations.pos or []):
        code_to_name[str(loc.code)] = str(loc.name)

    submitted_products = form.getlist("product")
    lines = _parse_transfer_lines(submitted_products, form.getlist("quantity"))

    try:
        movement_date = parse_dt(movement_date_raw) if movement_date_raw else None
    except ValueError:
        movement_date = None

    user = get_current_user_from_session(db, request)

    def _modal_response(message: str, message_class: str, message_detail: str,
                        checked_skus: set, quantities_map: dict, keep_open: bool) -> HTMLResponse:
        response = templates.TemplateResponse(
            request=request,
            name="partials/transfer_bulk_modal.html",
            context={
                "from_code": from_code,
                "to_code": to_code,
                "from_name": code_to_name.get(from_code, from_code),
                "to_name": code_to_name.get(to_code, to_code),
                "movement_date": movement_date_raw,
                "note": note or "",
                "products": _stock_options(service, from_code),
                "message": message,
                "message_class": message_class,
                "message_detail": message_detail,
                "checked_skus": checked_skus,
                "quantities": quantities_map,
            },
        )
        if keep_open:
            response.headers["X-Modal-Keep"] = "1"
        return response

    parsed_skus = {line.sku for line in lines}
    dropped = [sku for sku in submitted_products if (sku or "").strip() and (sku or "").strip() not in parsed_skus]
    if dropped:
        quantities_map = {line.sku: line.quantity for line in lines}
        return _modal_response("Error en traspaso", "error",
                               "Cantidad inválida o vacía para: " + ", ".join(dropped),
                               set(quantities_map.keys()), quantities_map, True)

    if not lines:
        return _modal_response("Error en traspaso", "error",
                               "Selecciona al menos un producto para traspasar.", set(), {}, True)

    try:
        if user is not None:
            for line in lines:
                log_event(
                    db,
                    user,
                    action="transfer_create",
                    entity_type="movement",
                    entity_id=None,
                    detail={
                        "sku": line.sku,
                        "quantity": float(line.quantity),
                        "from_location_code": from_code,
                        "to_location_code": to_code,
                    },
                )
        result = service.transfer(
            TransferCreate(
                from_location_code=from_code,
                to_location_code=to_code,
                lines=lines,
                movement_date=movement_date,
                note=note,
            )
        )
    except HTTPException as e:
        checked_skus = {line.sku for line in lines}
        quantities_map = {line.sku: line.quantity for line in lines}
        return _modal_response("Error en traspaso", "error",
                               str(getattr(e, "detail", e)), checked_skus, quantities_map, True)

    response = HTMLResponse("")
    response.headers["HX-Trigger"] = "transferBulkSuccess"
    return response
