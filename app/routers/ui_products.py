from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.schemas import ProductCreate, ProductUpdate
from app.security import get_current_user_from_session
from app.services.product_service import ProductService

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
