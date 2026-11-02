from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.repositories.inventory_repository import InventoryRepository
from app.repositories.purchase_repository import PurchaseRepository
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService
from app.services.product_service import ProductService, extract_sku
from app.logger import get_logger
from app.utils import query_match

from .ui_common import (
    dt_to_local_input,
    month_range,
    parse_dt,
    templates,
)
from .ui_context import UIContext, ui_context
from .ui_tabs_common import _home_locations_context, _location_id_for_code

_log = get_logger(__name__)

router = APIRouter()


def _render_profit_items_tab(
    *,
    ctx: "UIContext",
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
    message: Optional[str] = None,
    message_detail: Optional[str] = None,
    message_class: Optional[str] = None,
) -> HTMLResponse:
    bid = ctx.business_id
    user = ctx.user
    reporting = ReportingService(ctx.db, business_id=bid)

    business_code = ctx.business_code
    locations, default_loc_code = _home_locations_context(ctx)
    selected_location_code = (location_code or "").strip() or (default_loc_code or "")
    location_id = (
        _location_id_for_code(ctx.db, selected_location_code, business_id=bid, business_code=business_code)
        if selected_location_code
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

    summary, items = reporting.monthly_profit_items_report(start=start_dt, end=end_dt, location_id=location_id)
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
        request=ctx.request,
        name="features/profit_items/tab_profit_items.html",
        context={
            "user": user,
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
            "summary": summary,
            "items": items,
            "locations": locations,
            "location_code": selected_location_code,
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )


@router.get("/tabs/profit-items", response_class=HTMLResponse)
def tab_profit_items(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin_or_owner()
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
    )


@router.get("/tabs/profit-items/adjustment/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_adjustment_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("adjustment",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Adjustment movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
    return templates.TemplateResponse(
        request=request,
        name="features/profit_items/profit_items_edit_form.html",
        context={
            "kind": "adjustment",
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
            "filter_location_code": (location_code or "").strip(),
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
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)

    service.update_adjustment_movement(movement_id=movement_id, unit_cost=float(unit_cost))

    user = ctx.user
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_adjustment_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"unit_cost": float(unit_cost)},
        )
        db.commit()
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
    )


@router.get("/tabs/profit-items/transfer-in/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_transfer_in_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("transfer_in",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Transfer movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
    return templates.TemplateResponse(
        request=request,
        name="features/profit_items/profit_items_edit_form.html",
        context={
            "kind": "transfer_in",
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
            "filter_location_code": (location_code or "").strip(),
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
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)

    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("transfer_in",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Transfer movement not found")

    result = service.update_transfer_movement(
        movement_id=movement_id,
        quantity=float(mv.quantity or 0),
        unit_cost=float(unit_cost),
        movement_date=mv.movement_date,
        note=mv.note,
    )
    user = ctx.user
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_transfer_in_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"unit_cost": float(unit_cost)},
        )
        db.commit()
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
    )


@router.get("/tabs/profit-items/sale/{movement_id}/edit", response_class=HTMLResponse)
def profit_items_sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("sale",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="features/profit_items/profit_items_edit_form.html",
        context={
            "kind": "sale",
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "product_options": product_service.search(query="", limit=200),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
            "filter_location_code": (location_code or "").strip(),
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
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
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
    user = ctx.user
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_sale_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"sku": sku, "quantity": float(quantity), "unit_price": float(unit_price)},
        )
        db.commit()
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
    )


@router.post("/tabs/profit-items/sale/{movement_id}/delete", response_class=HTMLResponse)
def profit_items_sale_delete(
    request: Request,
    movement_id: int,
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    service.delete_sale_movement(movement_id)
    user = ctx.user
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_sale_delete",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={},
        )
        db.commit()
    return _render_profit_items_tab(ctx=ctx, query=query, start_date=start_date, end_date=end_date, location_code=location_code)


@router.post("/tabs/profit-items/sale/delete-selected", response_class=HTMLResponse)
def profit_items_sale_delete_selected(
    request: Request,
    sale_ids: list[int] = Form([]),
    query: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    user = ctx.user

    ids: list[int] = []
    seen: set[int] = set()
    for raw in (sale_ids or []):
        try:
            mid = int(raw)
        except Exception:
            _log.debug("sale_id parse skip: %r", raw)
            continue
        if mid <= 0 or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)

    if not ids:
        return _render_profit_items_tab(
            ctx=ctx,
            query=query,
            start_date=start_date,
            end_date=end_date,
            location_code=location_code,
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
            _log.exception("delete_sale_movement fallback %s: %s", mid, e)
            errors.append(f"{mid}: {e}")
    db.commit()

    msg = "Ventas eliminadas" if deleted > 0 else "No se pudo eliminar"
    detail = f"Se eliminaron {deleted} venta(s)."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:5])
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
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
    location_code: str = "",
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("purchase",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
    lot = PurchaseRepository(db, bid).get_lot_by_movement(mv.id)
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="features/profit_items/profit_items_edit_form.html",
        context={
            "kind": "purchase",
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": dt_to_local_input(mv.movement_date),
            "lot_code": lot.lot_code if lot else "",
            "product_options": product_service.search(query="", limit=200),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
            "filter_location_code": (location_code or "").strip(),
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
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    bid = ctx.business_id
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
    user = ctx.user
    if user is not None:
        log_event(
            db,
            user,
            action="profit_items_purchase_update",
            entity_type="movement",
            entity_id=str(movement_id),
            detail={"sku": sku, "quantity": float(quantity), "unit_cost": float(unit_cost)},
        )
        db.commit()
    return _render_profit_items_tab(
        ctx=ctx,
        query=query,
        start_date=start_date,
        end_date=end_date,
        location_code=location_code,
    )
