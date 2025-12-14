from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.schemas import ProductCreate, PurchaseCreate, SaleCreate
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

router = APIRouter(prefix="/ui", tags=["ui"])

templates = Jinja2Templates(directory="app/templates")


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_sku(product_field: str) -> str:
    value = (product_field or "").strip()
    if " - " in value:
        value = value.split(" - ", 1)[0].strip()
    return value


@router.get("/", response_class=HTMLResponse)
def ui_root() -> RedirectResponse:
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)

    products = product_service.recent(limit=20)
    product_options = product_service.search(query="", limit=200)
    purchases = inventory_service.recent_purchases(limit=20)
    sales = inventory_service.recent_sales(limit=20)

    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "products": products,
            "product_options": product_options,
            "purchases": purchases,
            "sales": sales,
        },
    )


@router.get("/stock-table", response_class=HTMLResponse)
def stock_table(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
) -> HTMLResponse:
    service = InventoryService(db)
    items = service.stock_list(query=query)
    return templates.TemplateResponse(
        request=request,
        name="partials/stock_table.html",
        context={"items": items},
    )


@router.post("/purchase", response_class=HTMLResponse)
def purchase(
    request: Request,
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
    sku = _extract_sku(product)
    try:
        result = service.purchase(
            PurchaseCreate(
                sku=sku,
                quantity=quantity,
                unit_cost=unit_cost,
                movement_date=_parse_dt(movement_date),
                lot_code=lot_code or None,
                note=note or None,
            )
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "message": "Compra registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "message": "Error en compra",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
            },
            status_code=e.status_code,
        )


@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    sku = _extract_sku(product)
    try:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=unit_price,
                movement_date=_parse_dt(movement_date),
                note=note or None,
            )
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Venta registrada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Error en venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
            },
            status_code=e.status_code,
        )


@router.post("/product", response_class=HTMLResponse)
def create_product(
    request: Request,
    sku: str = Form(""),
    name: str = Form(...),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_sale_price: Optional[float] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    try:
        created = product_service.create(
            ProductCreate(
                sku=sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                default_sale_price=default_sale_price,
            )
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
            },
            status_code=e.status_code,
        )
