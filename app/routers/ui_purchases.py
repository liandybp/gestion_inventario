from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import InventoryLot, InventoryMovement, Product
from app.schemas import PurchaseCreate
from app.security import get_current_user_from_session, require_active_business_id
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.invoice_parsers import parse_autodoc_pdf

from .ui_common import (
    _DEV_ACTIONS_ENABLED,
    dt_to_local_input,
    ensure_admin_or_owner,
    extract_sku,
    parse_dt,
    parse_optional_float,
    templates,
)

router = APIRouter()


@router.post("/purchase/from-invoice", response_class=HTMLResponse)
def purchase_from_invoice(
    request: Request,
    invoice_pdf: UploadFile = File(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)

    def _render_error(message: str, detail: str) -> HTMLResponse:
        user = get_current_user_from_session(db, request)
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": message,
                "message_detail": detail,
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=200,
        )

    if invoice_pdf is None or not invoice_pdf.filename:
        return _render_error("No se pudo importar la factura", "Debes adjuntar un PDF.")

    content_type = (invoice_pdf.content_type or "").lower()
    if ("pdf" not in content_type) and (not invoice_pdf.filename.lower().endswith(".pdf")):
        return _render_error(
            "No se pudo importar la factura",
            "Tipo de archivo inválido. Debes subir un archivo .pdf.",
        )

    try:
        parsed = parse_autodoc_pdf(invoice_pdf.file)
    except Exception as e:
        return _render_error(
            "No se pudo importar la factura",
            f"No se pudo leer el PDF: {e}",
        )

    if parsed.invoice_date is None:
        return _render_error(
            "No se pudo importar la factura",
            "No se encontró la fecha de factura en el PDF. Verifica que sea una factura AUTODOC y que el PDF tenga texto (no escaneado como imagen).",
        )

    if not parsed.lines:
        return _render_error(
            "No se pudo importar la factura",
            "No se encontraron líneas de productos en el PDF. Verifica que sea una factura AUTODOC y que el PDF tenga texto (no escaneado como imagen).",
        )

    user = get_current_user_from_session(db, request)

    now = datetime.now(timezone.utc)
    invoice_movement_dt = parsed.invoice_date.replace(
        hour=now.hour,
        minute=now.minute,
        second=now.second,
        microsecond=0,
    )

    invoice_tag = (
        f"Factura AUTODOC {parsed.invoice_number}" if parsed.invoice_number else "Factura AUTODOC"
    )

    created_products: list[dict[str, str]] = []
    created_movements = 0
    errors: list[str] = []

    for line in parsed.lines:
        sku = (line.sku or "").strip()
        if not sku:
            continue

        name = (line.name or "").strip()
        if not name:
            name = sku

        try:
            unit_cost_vat = round(float(line.net_unit_price) * 1.21, 4)
        except Exception:
            errors.append(f"{sku}: precio neto inválido")
            continue

        try:
            stmt = select(Product).where(Product.sku == sku)
            stmt = stmt.where(Product.business_id == int(bid))
            product = db.scalar(stmt)
            if product is None:
                product = Product(
                    business_id=int(bid),
                    sku=sku,
                    name=name,
                    category=None,
                    min_stock=0,
                    unit_of_measure=None,
                    default_purchase_cost=unit_cost_vat,
                    default_sale_price=0,
                    lead_time_days=0,
                    image_url=None,
                )
                db.add(product)
                db.commit()
                db.refresh(product)
                created_products.append({"sku": product.sku, "name": product.name})

                if user is not None:
                    log_event(
                        db,
                        user,
                        action="product_create",
                        entity_type="product",
                        entity_id=product.sku,
                        detail={
                            "name": product.name,
                            "source": "invoice_pdf",
                            "invoice_number": parsed.invoice_number,
                            "invoice_date": invoice_movement_dt.isoformat() if invoice_movement_dt else None,
                        },
                    )

            result = service.purchase(
                PurchaseCreate(
                    sku=sku,
                    quantity=float(line.quantity),
                    unit_cost=unit_cost_vat,
                    movement_date=invoice_movement_dt,
                    lot_code=None,
                    note=invoice_tag,
                )
            )
            created_movements += 1

            if user is not None:
                log_event(
                    db,
                    user,
                    action="purchase_create",
                    entity_type="movement",
                    entity_id=str(result.movement.id),
                    detail={
                        "sku": sku,
                        "quantity": float(line.quantity),
                        "unit_cost": unit_cost_vat,
                        "invoice_number": parsed.invoice_number,
                        "invoice_date": invoice_movement_dt.isoformat() if invoice_movement_dt else None,
                    },
                )
        except HTTPException as e:
            errors.append(f"{sku}: {e.detail}")
        except Exception as e:
            errors.append(f"{sku}: {e}")

    if created_movements == 0:
        message = "No se pudo importar la factura"
        detail = "No se pudo crear ninguna compra. " + ("; ".join(errors) if errors else "")
        message_class = "error"
        status = 200
    else:
        message = "Factura importada"
        detail = f"Se registraron {created_movements} línea(s) de compra."
        if errors:
            detail = detail + " Errores: " + "; ".join(errors[:5])
        message_class = "ok" if not errors else "warn"
        status = 200

    return templates.TemplateResponse(
        request=request,
        name="partials/purchase_panel.html",
        context={
            "user": user,
            "message": message,
            "message_detail": detail,
            "message_class": message_class,
            "invoice_created_products": created_products,
            "purchases": service.recent_purchases(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
        },
        status_code=status,
    )


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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
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
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    if int(getattr(mv, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = db.get(Product, mv.product_id)
    lot = db.scalar(select(InventoryLot).where(InventoryLot.movement_id == mv.id))
    product_service = ProductService(db, business_id=bid)
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
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    ensure_admin_or_owner(db, request)
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
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    try:
        ensure_admin_or_owner(db, request)
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
