from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.logger import get_logger
from app.schemas import SaleCreate
from app.services.sales_document_service import SalesDocumentService
from app.repositories.inventory_repository import InventoryRepository
from app.repositories.purchase_repository import PurchaseRepository
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService, extract_sku
from app.settings import is_desktop
from app.sync.outbox import append_event

_log = get_logger(__name__)


def _dt_to_local_input_s(dt: datetime) -> str:
    """Igual que `dt_to_local_input` pero con milisegundos.

    `dt_to_local_input` formatea solo hasta el minuto; `InventoryService.update_sale()`
    pasa ese valor truncado de vuelta a `_rebuild_product_fifo()`, que reordena TODOS
    los movimientos del producto por `movement_date`. Si una compra y una venta caen
    en la misma ventana truncada, la venta puede quedar ANTES que la compra en ese
    reordenamiento, y el rebuild rechaza la edición con "Insufficient stock" aunque el
    stock real sea suficiente — descubierto por el e2e de sale-payment-method. Con
    segundos enteros seguía siendo reproducible bajo carga (navegador ya "caliente"
    tras correr el resto de la suite e2e, interacción de UI completada en <1s); con
    milisegundos la ventana de colisión es inferior al overhead real de red/render.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


from .ui_common import (
    parse_dt,
    parse_typed_float,
    templates,
)
from .ui_context import UIContext, ui_context
from .ui_tabs_common import _location_id_for_code
from app.utils import query_match

router = APIRouter()


def _sale_pos_context(ctx: "UIContext", location_code: str = "") -> tuple[list[dict[str, str]], str, str]:
    config = ctx.config
    pos_locations: list[dict[str, str]] = []
    central = getattr(config.locations, "central", None)
    central_code = str(getattr(central, "code", "") or "").strip()
    if central_code:
        pos_locations.append(
            {
                "code": central_code,
                "name": str(getattr(central, "name", "") or "Almacen central"),
            }
        )
    pos_locations.extend(
        {"code": l.code, "name": l.name}
        for l in (config.locations.pos or [])
        if getattr(l, "code", None)
    )
    default_sale_location_code = config.locations.default_sale_code()
    selected_location_code = (location_code or "").strip() or default_sale_location_code
    return pos_locations, default_sale_location_code, selected_location_code


def _sale_product_options(service: InventoryService, location_code: str) -> list:
    return [
        p
        for p in service.stock_list(query="", location_code=location_code)
        if float(p.quantity or 0) > 0
    ]


@router.get("/sale/product-options", response_class=HTMLResponse)
def sale_product_options(
    request: Request,
    location_code: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    user = ctx.user
    if user is None:
        raise HTTPException(status_code=403, detail="Not authenticated")
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    _pos_locations, _default_sale_location_code, selected_location_code = _sale_pos_context(
        ctx,
        location_code,
    )
    items = _sale_product_options(service, location_code=selected_location_code)

    return templates.TemplateResponse(
        request=request,
        name="features/sales/_product_options.html",
        context={"product_options": items},
    )


def _sales_doc_context(ctx: "UIContext") -> dict:
    config = ctx.config
    bid = ctx.business_id
    session = getattr(ctx.request, "session", None) or {}
    cart = session.get("sales_doc_cart")
    if not isinstance(cart, list):
        cart = []
    draft = session.get("sales_doc_draft")
    if not isinstance(draft, dict):
        draft = {}
    svc = SalesDocumentService(ctx.db, bid)
    recent_documents = svc.list_recent(limit=10)
    customers = svc.customers_list(limit=200)
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
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    user = ctx.user
    pos_locations, default_sale_location_code, selected_location_code = _sale_pos_context(
        ctx,
        location_code,
    )
    product_options = _sale_product_options(service, location_code=selected_location_code)
    try:
        parsed_price = parse_typed_float(unit_price, "El precio unitario debe ser un número mayor o igual que 0")
        sku = ProductService(db, business_id=bid).resolve_sku(product, barcode_fallback=True)
        if user is not None:
            log_event(
                db,
                user,
                action="sale_create",
                entity_type="movement",
                entity_id=None,
                detail={"sku": sku, "quantity": quantity, "unit_price": parsed_price},
            )
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parsed_price,
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
                paid_by_transfer=paid_by_transfer,
            )
        )
        if is_desktop():
            try:
                append_event(
                    db, business_id=bid, event_type="sale",
                    payload={"sku": sku, "quantity": quantity,
                             "unit_price": parsed_price,
                             "movement_date": movement_date, "note": note or None,
                             "location_code": selected_location_code,
                             "paid_by_transfer": paid_by_transfer},
                )
                db.commit()
            except Exception:  # noqa: BLE001
                db.rollback()
                _log.error(
                    "No se pudo encolar la venta %s en el outbox offline", sku, exc_info=True
                )
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Venta registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
        )
    except HTTPException as e:
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
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
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    pos_locations, default_sale_location_code, selected_location_code = _sale_pos_context(
        ctx,
        location_code,
    )
    product_options = _sale_product_options(service, location_code=selected_location_code)
    try:
        parsed_price = parse_typed_float(unit_price, "El precio unitario debe ser un número mayor o igual que 0")
        sku = ProductService(db, business_id=bid).barcode_to_sku(barcode)
        user = ctx.user
        if user is not None:
            log_event(
                db,
                user,
                action="sale_create",
                entity_type="movement",
                entity_id=None,
                detail={"sku": sku, "quantity": quantity, "unit_price": parsed_price},
            )
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parsed_price,
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
                paid_by_transfer=paid_by_transfer,
            )
        )
        if is_desktop():
            try:
                append_event(
                    db, business_id=bid, event_type="sale",
                    payload={"sku": sku, "quantity": quantity,
                             "unit_price": parsed_price,
                             "movement_date": movement_date, "note": note or None,
                             "location_code": selected_location_code,
                             "paid_by_transfer": paid_by_transfer},
                )
                db.commit()
            except Exception:  # noqa: BLE001
                db.rollback()
                _log.error(
                    "No se pudo encolar la venta %s en el outbox offline", sku, exc_info=True
                )
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Venta registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
            },
        )
    except HTTPException as e:
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
            },
            status_code=e.status_code,
        )
    except Exception as e:
        _log.exception("sale_create fallback: %s", e)
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error en venta",
                "message_detail": str(e),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=400,
        )


@router.get("/movement/sale/{movement_id}/edit", response_class=HTMLResponse)
def sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin_or_owner()
    bid = ctx.business_id
    mv = InventoryRepository(db, business_id=bid).get_movement(movement_id, ("sale",))
    if mv is None:
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = PurchaseRepository(db, bid).get_product(mv.product_id)
    product_service = ProductService(db, business_id=bid)
    return templates.TemplateResponse(
        request=request,
        name="features/sales/sale_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": _dt_to_local_input_s(mv.movement_date),
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
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin_or_owner()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    pos_locations, default_sale_location_code, selected_location_code = _sale_pos_context(ctx)
    product_options = _sale_product_options(service, location_code=selected_location_code)
    sku = extract_sku(product)
    try:
        result = service.update_sale(
            movement_id=movement_id,
            sku=sku,
            quantity=quantity,
            unit_price=unit_price,
            movement_date=parse_dt(movement_date),
            note=note or None,
            paid_by_transfer=paid_by_transfer,
        )

        user = ctx.user
        if user is not None:
            log_event(
                db,
                user,
                action="sale_update",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={"sku": sku, "quantity": quantity, "unit_price": unit_price},
            )
            db.commit()
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Venta actualizada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
        )
    except HTTPException as e:
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error al actualizar venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=e.status_code,
        )


@router.post("/movement/sale/{movement_id}/delete", response_class=HTMLResponse)
def sale_delete(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin_or_owner()
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    pos_locations, default_sale_location_code, selected_location_code = _sale_pos_context(ctx)
    product_options = _sale_product_options(service, location_code=selected_location_code)
    try:
        service.delete_sale_movement(movement_id)

        user = ctx.user
        if user is not None:
            log_event(
                db,
                user,
                action="sale_delete",
                entity_type="movement",
                entity_id=str(movement_id),
                detail={},
            )
            db.commit()
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Venta eliminada",
                "message_class": "ok",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
        )
    except HTTPException as e:
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        _log.exception("sale_delete fallback: %s", e)
        user = ctx.user
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar venta",
                "message_detail": str(e),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=400,
        )


@router.post("/movement/sale/delete-selected", response_class=HTMLResponse)
def sale_delete_selected(
    request: Request,
    sale_ids: list[int] = Form([]),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    bid = ctx.business_id
    service = InventoryService(db, business_id=bid)
    pos_locations, default_sale_location_code, selected_location_code = _sale_pos_context(ctx)
    product_options = _sale_product_options(service, location_code=selected_location_code)
    user = ctx.user

    try:
        ctx.ensure_admin_or_owner()
        ids = [int(x) for x in (sale_ids or []) if int(x) > 0]
        if not ids:
            return templates.TemplateResponse(
                request=request,
                name="features/sales/sale_panel.html",
                context={
                    "user": user,
                    "message": "No se seleccionó ninguna venta",
                    "message_class": "warn",
                    "sales": service.recent_sales(limit=20),
                    "product_options": product_options,
                    "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                    "pos_locations": pos_locations,
                    "default_sale_location_code": default_sale_location_code,
                    "sale_location_code": selected_location_code,
                    **_sales_doc_context(ctx),
                },
                status_code=200,
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
                        action="sale_delete",
                        entity_type="movement",
                        entity_id=str(mid),
                        detail={"bulk": True},
                    )
            except HTTPException as e:
                errors.append(f"{mid}: {e.detail}")
            except Exception as e:
                _log.exception("bulk sale delete fallback %s: %s", mid, e)
                errors.append(f"{mid}: {e}")
        db.commit()

        msg = "Ventas eliminadas" if deleted > 0 else "No se pudo eliminar"
        detail = f"Se eliminaron {deleted} venta(s)."
        if errors:
            detail = detail + " Errores: " + "; ".join(errors[:5])

        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": msg,
                "message_detail": detail,
                "message_class": "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=200,
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="features/sales/sale_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar ventas",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_options,
                "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
                "pos_locations": pos_locations,
                "default_sale_location_code": default_sale_location_code,
                "sale_location_code": selected_location_code,
                **_sales_doc_context(ctx),
            },
            status_code=200,
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
    payment_method: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
    ctx = ui_context(request, db)
    bid = ctx.business_id
    inventory_service = InventoryService(db, business_id=bid)
    user = ctx.user
    
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

    business_code = ctx.business_code
    config = ctx.config
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

    svc = SalesDocumentService(db, bid)
    recent_documents = svc.list_recent(limit=200, start=start_dt, end=end_dt)
    q = (query or "").strip()
    if q:
        recent_documents = [
            d
            for d in recent_documents
            if query_match(q, str(getattr(d, "code", "") or ""), str(getattr(d, "client_name", "") or ""))
        ]
    recent_documents = recent_documents[:10]

    customers = svc.customers_list(limit=200)
    pos_locations: list[dict[str, str]] = []
    central = getattr(config.locations, "central", None)
    central_code = str(getattr(central, "code", "") or "").strip()
    if central_code:
        pos_locations.append(
            {
                "code": central_code,
                "name": str(getattr(central, "name", "") or "Almacen central"),
            }
        )
    pos_locations.extend(
        {"code": loc.code, "name": loc.name}
        for loc in (config.locations.pos or [])
        if getattr(loc, "code", None)
    )
    default_sale_location_code = config.locations.default_sale_code()

    selected_filter_location_code = (location_code or "").strip()
    selected_filter_location_id = _location_id_for_code(
        db,
        selected_filter_location_code,
        business_id=bid,
        business_code=business_code,
    )
    selected_payment_method = (payment_method or "").strip().lower()
    if selected_payment_method not in ("transfer", "cash"):
        selected_payment_method = ""

    product_options = [
        p
        for p in inventory_service.stock_list(query="", location_code=default_sale_location_code)
        if float(p.quantity or 0) > 0
    ]
    return templates.TemplateResponse(
        request=request,
        name="features/sales/tab_sales.html",
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
                payment_method=selected_payment_method or None,
            ),
            "filter_month": display_month,
            "filter_year": display_year,
            "filter_query": query,
            "filter_start_date": start_date or "",
            "filter_end_date": end_date or "",
            "filter_show_all": bool(show_all),
            "filter_location_code": selected_filter_location_code,
            "filter_payment_method": selected_payment_method,
            "product_options": product_options,
            "movement_date_default": _dt_to_local_input_s(datetime.now(timezone.utc)),
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
