from __future__ import annotations
from app.repositories.business_repository import get_active_business_config
from types import SimpleNamespace

from datetime import datetime, timezone
import html
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import Product
from app.repositories.purchase_repository import PurchaseRepository
from app.schemas import AdjustmentCreate, ProductUpdate
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.invoice_parsers import parse_autodoc_pdf
from sqlalchemy import select

from .ui_common import (
    barcode_to_sku,
    ensure_admin_or_owner,
    extract_sku,
    delete_uploaded_image,
    parse_typed_float,
    save_product_image,
    templates,
)
from app.logger import get_logger

_log = get_logger(__name__)

router = APIRouter()


@router.post("/product/{sku:path}/delete", response_class=HTMLResponse)
def product_delete(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    try:
        ensure_admin_or_owner(db, request)
        product_service.delete(sku)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="product_delete",
                entity_type="product",
                entity_id=sku,
                detail={},
            )
            db.commit()
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Producto eliminado",
                "message_class": "ok",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Error al eliminar producto",
                "message_detail": str(e.detail),
                "message_class": "error",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
            status_code=e.status_code,
        )
    except Exception as e:
        _log.exception("product_delete fallback: %s", e)
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Error al eliminar producto",
                "message_detail": str(e),
                "message_class": "error",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
            status_code=400,
        )


@router.get("/product/{sku:path}/edit", response_class=HTMLResponse)
def product_edit_form(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    product = product_service.get_by_sku(sku)
    return templates.TemplateResponse(
        request=request,
        name="partials/product_edit_form.html",
        context={"product": product},
    )


@router.post("/product/{sku:path}/update", response_class=HTMLResponse)
def product_update(
    request: Request,
    sku: str,
    new_sku: str = Form(""),
    name: str = Form(...),
    unit_of_measure: str = Form(""),
    image_file: Optional[UploadFile] = File(None),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: str = Form(""),
    default_sale_price: str = Form(""),
    lead_time_days: Optional[int] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    new_image_url: Optional[str] = None
    try:
        ensure_admin_or_owner(db, request)
        existing = product_service.get_by_sku(sku)
        purchase_cost = parse_typed_float(default_purchase_cost, "El costo de compra debe ser un número")
        sale_price = parse_typed_float(default_sale_price, "El precio de venta debe ser un número")
        if image_file is not None:
            new_image_url = save_product_image(image_file)
        image_url = new_image_url or existing.image_url
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=purchase_cost,
                default_sale_price=sale_price,
                lead_time_days=lead_time_days,
                image_url=image_url or None,
            ),
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="product_update",
                entity_type="product",
                entity_id=updated.sku,
                detail={"from_sku": sku, "to_sku": updated.sku, "name": updated.name},
            )
            db.commit()
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Producto actualizado",
                "message_detail": f"SKU: {updated.sku}",
                "message_class": "ok",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
        )
    except HTTPException as e:
        db.rollback()
        # La imagen nueva no quedó asociada a ningún producto: no dejarla huérfana en disco.
        delete_uploaded_image(new_image_url)
        # REQ-3: el error se muestra DENTRO del modal con lo tecleado (antes se pintaba en
        # #product-panel detrás del modal y app.js lo cerraba, perdiendo los datos).
        try:
            image_url = product_service.get_by_sku(sku).image_url
        except HTTPException:
            image_url = None
        form_product = SimpleNamespace(
            sku=sku,
            name=name,
            unit_of_measure=unit_of_measure,
            image_url=image_url,
            category=category or "",
            min_stock=min_stock,
            default_purchase_cost=default_purchase_cost,
            default_sale_price=default_sale_price,
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/product_edit_form.html",
            context={"product": form_product, "new_sku_value": new_sku or sku, "form_error": str(e.detail)},
            status_code=e.status_code,
            headers={"HX-Retarget": "#product-edit-modal", "HX-Reswap": "outerHTML"},
        )


@router.get("/product/{sku:path}/edit-inventory", response_class=HTMLResponse)
def product_edit_form_inventory(
    request: Request,
    sku: str,
    location_code: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)
    product = product_service.get_by_sku(sku)
    loc = (location_code or "").strip() or None
    current_stock = inventory_service.stock(sku, location_code=loc).quantity
    return templates.TemplateResponse(
        request=request,
        name="partials/product_edit_form_inventory.html",
        context={
            "product": product,
            "current_stock": float(current_stock or 0),
            "location_code": loc or "",
        },
    )


@router.post("/product/{sku:path}/update-inventory", response_class=HTMLResponse)
def product_update_inventory(
    request: Request,
    sku: str,
    new_sku: str = Form(""),
    name: str = Form(...),
    unit_of_measure: str = Form(""),
    image_file: Optional[UploadFile] = File(None),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: str = Form(""),
    default_sale_price: str = Form(""),
    lead_time_days: Optional[int] = Form(None),
    desired_stock: Optional[float] = Form(None),
    initial_inventory: bool = Form(False),
    location_code: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=bid)
    inventory_service = InventoryService(db, business_id=bid)
    unsaved_image_url: Optional[str] = None
    try:
        ensure_admin_or_owner(db, request)
        existing = product_service.get_by_sku(sku)

        loc = (location_code or "").strip() or None
        stock_before = inventory_service.stock(sku, location_code=loc).quantity
        parsed_purchase_cost = parse_typed_float(default_purchase_cost, "El costo de compra debe ser un número")
        parsed_sale_price = parse_typed_float(default_sale_price, "El precio de venta debe ser un número")
        if image_file is not None:
            unsaved_image_url = save_product_image(image_file)
        image_url = unsaved_image_url or existing.image_url
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=parsed_purchase_cost,
                default_sale_price=parsed_sale_price,
                lead_time_days=lead_time_days,
                image_url=image_url or None,
            ),
        )
        unsaved_image_url = None  # update() hizo commit: la imagen ya es del producto

        if desired_stock is not None:
            desired = float(desired_stock)
            if desired < 0:
                raise HTTPException(status_code=422, detail="desired_stock must be >= 0")

            delta = float(desired) - float(stock_before or 0)
            if abs(delta) > 1e-9:
                unit_cost = None
                if delta > 0:
                    unit_cost = parsed_purchase_cost
                    if unit_cost is None:
                        unit_cost = float(getattr(existing, "default_purchase_cost", 0) or 0)
                    if initial_inventory and unit_cost is None:
                        unit_cost = 0
                    if (not initial_inventory) and (unit_cost is None):
                        raise HTTPException(
                            status_code=422,
                            detail="Para aumentar stock necesitas un costo de compra (define 'Costo compra por defecto' o ingrésalo en el formulario)",
                        )
                    if unit_cost is not None and unit_cost < 0:
                        raise HTTPException(
                            status_code=422,
                            detail="El costo de compra por defecto debe ser >= 0",
                        )

                movement_date = None
                note = "Ajuste por edición manual de stock"
                if initial_inventory and delta > 0:
                    note = "Inventario inicial (ajuste por edición manual de stock)"

                result = inventory_service.adjustment(
                    AdjustmentCreate(
                        sku=updated.sku,
                        quantity_delta=float(delta),
                        unit_cost=float(unit_cost) if unit_cost is not None else None,
                        movement_date=movement_date,
                        note=note,
                        location_code=loc,
                    )
                )

                user = get_current_user_from_session(db, request)
                if user is not None:
                    log_event(
                        db,
                        user,
                        action="adjustment_create",
                        entity_type="movement",
                        entity_id=str(result.movement.id),
                        detail={
                            "sku": updated.sku,
                            "quantity_delta": float(delta),
                            "unit_cost": float(unit_cost) if unit_cost is not None else None,
                            "note": note,
                        },
                    )
                    db.commit()

        config = get_active_business_config(db, request)
        locations = [{"code": "CENTRAL", "name": "Almacén Central"}]
        for pos_loc in (config.locations.pos or []):
            if getattr(pos_loc, "code", None):
                locations.append({"code": pos_loc.code, "name": pos_loc.name})
        default_location_code = str(getattr(config.locations.central, "code", "CENTRAL") or "CENTRAL")

        product_options = []
        try:
            product_options = product_service.search(query="", limit=200)
        except Exception:
            _log.error("product_options fallback en product_update")
            product_options = []

        return templates.TemplateResponse(
            request=request,
            name="partials/tab_inventory.html",
            context={
                "locations": locations,
                "default_location_code": default_location_code,
                "selected_location_code": loc or "",
                "product_options": product_options,
                "message": "Producto actualizado",
                "message_detail": f"SKU: {updated.sku}",
                "message_class": "ok",
            },
        )
    except HTTPException as e:
        delete_uploaded_image(unsaved_image_url)
        config = get_active_business_config(db, request)
        locations = [{"code": "CENTRAL", "name": "Almacén Central"}]
        for pos_loc in (config.locations.pos or []):
            if getattr(pos_loc, "code", None):
                locations.append({"code": pos_loc.code, "name": pos_loc.name})
        default_location_code = str(getattr(config.locations.central, "code", "CENTRAL") or "CENTRAL")

        product_options = []
        try:
            product_options = product_service.search(query="", limit=200)
        except Exception:
            _log.error("product_options fallback en product_update")
            product_options = []

        return templates.TemplateResponse(
            request=request,
            name="partials/tab_inventory.html",
            context={
                "locations": locations,
                "default_location_code": default_location_code,
                "selected_location_code": (location_code or "").strip() or "",
                "product_options": product_options,
                "message": "Error al actualizar producto",
                "message_detail": str(e.detail),
                "message_class": "error",
            },
            status_code=e.status_code,
        )


@router.post("/product/from-invoice", response_class=HTMLResponse)
def product_from_invoice(
    request: Request,
    invoice_pdf: UploadFile = File(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    config = get_active_business_config(db, request)
    purchase_cfg = getattr(config, "purchase", None)
    vat_rate = float(getattr(purchase_cfg, "invoice_vat_rate", 1.21) or 1.21)
    fx_rate = getattr(purchase_cfg, "invoice_fx_rate", None)
    try:
        fx_rate = float(fx_rate) if fx_rate is not None else None
    except Exception:
        _log.debug("fx_rate parse fallback: %r", fx_rate)
        fx_rate = None
    product_service = ProductService(db, business_id=bid)

    if invoice_pdf is None or not invoice_pdf.filename:
        raise HTTPException(status_code=422, detail="invoice_pdf is required")

    content_type = (invoice_pdf.content_type or "").lower()
    if ("pdf" not in content_type) and (not invoice_pdf.filename.lower().endswith(".pdf")):
        raise HTTPException(status_code=422, detail="Invalid file type. Please upload a PDF")

    try:
        parsed = parse_autodoc_pdf(invoice_pdf.file)
    except Exception as e:
        _log.exception("parse_autodoc_pdf fallback: %s", e)
        raise HTTPException(status_code=400, detail=f"No se pudo leer el PDF: {e}") from e

    if not parsed.lines:
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "No se pudo importar la factura",
                "message_detail": "No se encontraron líneas de productos en el PDF.",
                "message_class": "error",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
            },
            status_code=400,
        )

    user = get_current_user_from_session(db, request)

    created_products: list[dict[str, str]] = []
    skipped_products: list[str] = []
    errors: list[str] = []

    for line in parsed.lines:
        sku = (line.sku or "").strip()
        if not sku:
            continue

        name = (line.name or "").strip()
        if not name:
            name = sku

        try:
            fx_factor = fx_rate if (fx_rate is not None and fx_rate > 0) else 1.0
            unit_cost_with_vat = round(float(line.net_unit_price) * vat_rate * fx_factor, 4)
        except Exception:
            _log.debug("unit cost parse skip para sku %r", sku)
            errors.append(f"{sku}: precio neto inválido")
            continue

        try:
            # Existencia en el negocio activo (antes se buscaba en TODOS los negocios).
            if PurchaseRepository(db, bid).get_product_by_sku(sku) is not None:
                skipped_products.append(f"{sku} - {name}")
                continue

            # Antes: Product(...) sin business_id → producto sin negocio, invisible en
            # historial y sugerencias y con el SKU (único global) bloqueado para todos.
            product = product_service.create_for_purchase(
                name=name,
                sku=sku,
                purchase_cost=unit_cost_with_vat,
                sale_price=None,
            )
            product.unit_of_measure = "u"
            product.default_sale_price = None  # lo completa el usuario (aviso en el panel)

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
                    },
                    isolated=True,
                )
            # Commit por línea; si falla, el rollback del except deja la sesión usable.
            db.commit()
            created_products.append({"sku": product.sku, "name": product.name})

        except HTTPException as e:
            db.rollback()
            errors.append(f"{sku}: {e.detail}")
        except Exception as e:
            db.rollback()
            _log.exception("product_create desde invoice fallback %s: %s", sku, e)
            errors.append(f"{sku}: {str(e)}")

    # Build message
    message_parts = []
    if created_products:
        message_parts.append(f"✓ {len(created_products)} productos creados")
    if skipped_products:
        message_parts.append(f"⊘ {len(skipped_products)} productos ya existían")
    if errors:
        message_parts.append(f"✗ {len(errors)} errores")

    message = " | ".join(message_parts) if message_parts else "No se procesaron productos"
    message_class = "ok" if created_products and not errors else ("warn" if created_products else "error")

    # Build detail message
    detail_parts = []
    if created_products:
        detail_parts.append("Productos creados: " + ", ".join([f"{p['sku']}" for p in created_products]))
    if skipped_products:
        detail_parts.append("Ya existían: " + ", ".join(skipped_products[:5]) + (f" (+{len(skipped_products)-5} más)" if len(skipped_products) > 5 else ""))
    if errors:
        detail_parts.append("Errores: " + "; ".join(errors[:3]) + (f" (+{len(errors)-3} más)" if len(errors) > 3 else ""))

    message_detail = " | ".join(detail_parts) if detail_parts else None

    return templates.TemplateResponse(
        request=request,
        name="partials/product_panel.html",
        context={
            "message": message,
            "message_detail": message_detail,
            "message_class": message_class,
            "products": product_service.recent(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "invoice_created_products": created_products,
        },
    )


@router.get("/product-defaults/purchase", response_class=HTMLResponse)
def product_defaults_purchase(request: Request, product: str = "", db: Session = Depends(session_dep)) -> HTMLResponse:
    business_id = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=business_id)
    sku = extract_sku(product)
    if not sku:
        return HTMLResponse("")

    try:
        p = product_service.get_by_sku(sku)
    except HTTPException:
        # SKU aún no existente (p. ej. al teclear un producto nuevo): sin defaults.
        return HTMLResponse("")

    uom = html.escape(p.unit_of_measure or "")
    cost = "" if p.default_purchase_cost is None else str(float(p.default_purchase_cost))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='purchase-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='purchase-unit-cost' hx-swap-oob='true' name='unit_cost' type='number' step='0.0001' min='0' value='{cost}' />"
    )


@router.get("/product-defaults/sale", response_class=HTMLResponse)
def product_defaults_sale(request: Request, product: str = "", db: Session = Depends(session_dep)) -> HTMLResponse:
    business_id = require_active_business_id(db, request)
    product_service = ProductService(db, business_id=business_id)
    value = (product or "").strip()
    if not value:
        return HTMLResponse("")

    sku = extract_sku(value)
    p: Optional[Product] = None

    if sku:
        try:
            p = product_service.get_by_sku(sku)
        except HTTPException:
            p = None

    if p is None:
        p = db.scalar(
            select(Product)
            .where(Product.business_id == int(business_id), Product.name.ilike(value))
            .order_by(Product.id.asc())
        )

    if p is None:
        try:
            p = product_service.get_by_sku(barcode_to_sku(db, value, business_id=business_id))
        except HTTPException:
            return HTMLResponse("")

    uom = html.escape(p.unit_of_measure or "")
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='sale-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='sale-unit-price' hx-swap-oob='true' name='unit_price' type='number' step='0.0001' min='0' value='{price}' />"
    )


# Ventas y Traspasos NO usan esta búsqueda: sus datalist filtran por stock en la ubicación
# (ui_sales.py::_sale_product_options, ui_transfers.py::transfer_product_options).
_SUGGEST_FORMATS = {"label", "sku"}


@router.get("/product-suggest", response_class=HTMLResponse)
def product_suggest(
    request: Request,
    q: str = "",
    fmt: str = "label",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    """Sugerencias de producto al teclear (todos los roles; negocio activo).

    Los datalist iniciales solo traen los 200 primeros productos: app.js sustituye su
    contenido con esta respuesta (inputs con data-product-suggest).
    """
    business_id = require_active_business_id(db, request)
    query = (q or "").strip()
    if len(query) < 2:
        return HTMLResponse("")
    products = ProductService(db, business_id=business_id).search(query=query, limit=30)
    return templates.TemplateResponse(
        request=request,
        name="partials/product_suggest_options.html",
        context={"products": products, "fmt": fmt if fmt in _SUGGEST_FORMATS else "label"},
    )


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    business_code = get_active_business_code(db, request)
    config = get_active_business_config(db, request)
    locations = [{"code": "", "name": "Vista General"}, {"code": config.locations.central.code, "name": "Almacén central"}]
    pos_locations: list[dict[str, str]] = []
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})
            pos_locations.append({"code": str(loc.code), "name": str(loc.name)})
    
    product_options = []
    try:
        from app.services.product_service import ProductService
        product_options = ProductService(db, business_id=bid).search(query="", limit=200)
    except Exception:
        _log.error("product_options fallback en tab_inventory")
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
        _log.error("categories fallback en tab_inventory")
        categories = []

    response = templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={
            "locations": locations,
            "pos_locations": pos_locations,
            "default_location_code": "",
            "product_options": product_options,
            "categories": categories,
        },
    )
    return response
