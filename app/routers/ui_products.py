from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import Product
from app.schemas import ProductCreate, ProductUpdate
from app.security import get_current_user_from_session
from app.services.product_service import ProductService
from app.invoice_parsers import parse_autodoc_pdf
from sqlalchemy import select

from .ui_common import (
    ensure_admin,
    extract_sku,
    parse_optional_float,
    save_product_image,
    templates,
)

router = APIRouter()


@router.post("/product", response_class=HTMLResponse)
def create_product(
    request: Request,
    sku: str = Form(""),
    name: str = Form(...),
    unit_of_measure: str = Form(""),
    image_file: Optional[UploadFile] = File(None),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: float = Form(...),
    default_sale_price: float = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    try:
        ensure_admin(db, request)
        image_url = save_product_image(image_file) if image_file is not None else None
        created = product_service.create(
            ProductCreate(
                sku=sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=default_purchase_cost,
                default_sale_price=default_sale_price,
                image_url=image_url,
            )
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="product_create",
                entity_type="product",
                entity_id=created.sku,
                detail={"name": created.name},
            )
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Producto creado",
                "message_detail": f"SKU: {created.sku}",
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
                "message": "Error al crear producto",
                "message_detail": str(e.detail),
                "message_class": "error",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
            status_code=e.status_code,
        )


@router.post("/product/{sku}/delete", response_class=HTMLResponse)
def product_delete(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    try:
        ensure_admin(db, request)
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


@router.get("/product/{sku}/edit", response_class=HTMLResponse)
def product_edit_form(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    product_service = ProductService(db)
    product = product_service.get_by_sku(sku)
    return templates.TemplateResponse(
        request=request,
        name="partials/product_edit_form.html",
        context={"product": product},
    )


@router.post("/product/{sku}/update", response_class=HTMLResponse)
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
    product_service = ProductService(db)
    try:
        ensure_admin(db, request)
        existing = product_service.get_by_sku(sku)
        image_url = save_product_image(image_file) if image_file is not None else existing.image_url
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=parse_optional_float(default_purchase_cost),
                default_sale_price=parse_optional_float(default_sale_price),
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
        return templates.TemplateResponse(
            request=request,
            name="partials/product_panel.html",
            context={
                "message": "Error al actualizar producto",
                "message_detail": str(e.detail),
                "message_class": "error",
                "products": product_service.recent(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "edit_product": None,
            },
            status_code=e.status_code,
        )


@router.get("/product/{sku}/edit-inventory", response_class=HTMLResponse)
def product_edit_form_inventory(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    product_service = ProductService(db)
    product = product_service.get_by_sku(sku)
    return templates.TemplateResponse(
        request=request,
        name="partials/product_edit_form_inventory.html",
        context={"product": product},
    )


@router.post("/product/{sku}/update-inventory", response_class=HTMLResponse)
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
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    try:
        existing = product_service.get_by_sku(sku)
        image_url = save_product_image(image_file) if image_file is not None else existing.image_url
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=parse_optional_float(default_purchase_cost),
                default_sale_price=parse_optional_float(default_sale_price),
                lead_time_days=lead_time_days,
                image_url=image_url or None,
            ),
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_inventory.html",
            context={
                "message": "Producto actualizado",
                "message_detail": f"SKU: {updated.sku}",
                "message_class": "ok",
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_inventory.html",
            context={
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
    ensure_admin(db, request)
    product_service = ProductService(db)

    if invoice_pdf is None or not invoice_pdf.filename:
        raise HTTPException(status_code=422, detail="invoice_pdf is required")

    content_type = (invoice_pdf.content_type or "").lower()
    if ("pdf" not in content_type) and (not invoice_pdf.filename.lower().endswith(".pdf")):
        raise HTTPException(status_code=422, detail="Invalid file type. Please upload a PDF")

    try:
        parsed = parse_autodoc_pdf(invoice_pdf.file)
    except Exception as e:
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
            # Calculate cost with 21% VAT
            unit_cost_with_vat = round(float(line.net_unit_price) * 1.21, 4)
        except Exception:
            errors.append(f"{sku}: precio neto inválido")
            continue

        try:
            # Check if product already exists
            existing_product = db.scalar(select(Product).where(Product.sku == sku))
            if existing_product is not None:
                skipped_products.append(f"{sku} - {name}")
                continue

            # Create new product
            product = Product(
                sku=sku,
                name=name,
                category=None,
                min_stock=0,
                unit_of_measure="u",  # Default unit
                default_purchase_cost=unit_cost_with_vat,
                default_sale_price=None,  # Leave empty for user to fill
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
                    },
                )

        except Exception as e:
            errors.append(f"{sku}: {str(e)}")
            continue

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
def purchase_product_defaults(
    product: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    sku = extract_sku(product)
    if not sku:
        return HTMLResponse("")
    p = ProductService(db).get_by_sku(sku)
    uom = p.unit_of_measure or ""
    cost = "" if p.default_purchase_cost is None else str(float(p.default_purchase_cost))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='purchase-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='purchase-unit-cost' hx-swap-oob='true' name='unit_cost' type='number' step='0.0001' min='0' value='{cost}' />"
    )


@router.get("/product-defaults/sale", response_class=HTMLResponse)
def sale_product_defaults(
    product: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    sku = extract_sku(product)
    if not sku:
        return HTMLResponse("")
    p = ProductService(db).get_by_sku(sku)
    uom = p.unit_of_measure or ""
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    label = f"Cantidad ({uom})" if uom else "Cantidad"
    return HTMLResponse(
        f"<label id='sale-qty-label' hx-swap-oob='true'>{label}</label>"
        f"<input id='sale-unit-price' hx-swap-oob='true' name='unit_price' type='number' step='0.0001' min='0' value='{price}' />"
    )
