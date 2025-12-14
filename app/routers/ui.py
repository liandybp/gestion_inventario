from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import InventoryLot, InventoryMovement, Product
from app.schemas import ProductCreate, ProductUpdate, PurchaseCreate, SaleCreate
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService

router = APIRouter(prefix="/ui", tags=["ui"])

templates = Jinja2Templates(directory="app/templates")


_DEV_ACTIONS_ENABLED = os.getenv("DEV_ACTIONS_ENABLED", "1") == "1"


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _dt_to_local_input(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")


def _month_range(now: datetime) -> tuple[datetime, datetime]:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def _extract_sku(product_field: str) -> str:
    value = (product_field or "").strip()
    if " - " in value:
        value = value.split(" - ", 1)[0].strip()
    return value


def _parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    return float(s)


@router.get("/", response_class=HTMLResponse)
def ui_root() -> RedirectResponse:
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={},
    )


@router.get("/tabs/profit", response_class=HTMLResponse)
def tab_profit(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    summary, items = inventory_service.monthly_profit_report()
    expenses = inventory_service.list_expenses(
        start=summary["month_start"],
        end=summary["month_end"],
        limit=50,
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit.html",
        context={
            "summary": summary,
            "items": items,
            "expenses": expenses,
        },
    )


@router.get("/tabs/profit-items", response_class=HTMLResponse)
def tab_profit_items(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    summary, items = inventory_service.monthly_profit_items_report()
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit_items.html",
        context={
            "summary": summary,
            "items": items,
        },
    )


@router.get("/tabs/expenses", response_class=HTMLResponse)
def tab_expenses(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    start, end = _month_range(datetime.now(timezone.utc))
    expenses = inventory_service.list_expenses(start=start, end=end, limit=200)
    total = inventory_service.total_expenses(start=start, end=end)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_expenses.html",
        context={
            "expenses": expenses,
            "expenses_total": total,
            "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.post("/expenses/create", response_class=HTMLResponse)
def expense_create(
    request: Request,
    expense_date: Optional[str] = Form(None),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    start, end = _month_range(datetime.now(timezone.utc))
    try:
        service.create_expense(amount=amount, concept=concept, expense_date=_parse_dt(expense_date))
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Gasto registrado",
                "message_class": "ok",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except Exception as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al registrar gasto",
                "message_detail": str(e),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.get("/expense/{expense_id}/edit", response_class=HTMLResponse)
def expense_edit_form(
    request: Request,
    expense_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    expense = service.get_expense(expense_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/expense_edit_form.html",
        context={
            "expense": expense,
            "expense_date_value": _dt_to_local_input(expense.expense_date),
        },
    )


@router.post("/expense/{expense_id}/update", response_class=HTMLResponse)
def expense_update(
    request: Request,
    expense_id: int,
    expense_date: Optional[str] = Form(None),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    start, end = _month_range(datetime.now(timezone.utc))
    try:
        service.update_expense(
            expense_id=expense_id,
            amount=amount,
            concept=concept,
            expense_date=_parse_dt(expense_date),
        )
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Gasto actualizado",
                "message_class": "ok",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al actualizar gasto",
                "message_detail": str(e.detail),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al actualizar gasto",
                "message_detail": str(e),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.get("/tabs/home", response_class=HTMLResponse)
def tab_home(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)

    products = product_service.list()
    stock_items = inventory_service.stock_list()
    restock_items = [i for i in stock_items if i.needs_restock]

    totals = {
        "products": len(products),
        "stock_total": float(sum(i.quantity for i in stock_items)),
        "to_restock": len(restock_items),
    }

    monthly = inventory_service.monthly_overview(months=12)

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_home.html",
        context={"totals": totals, "monthly": monthly},
    )


@router.get("/restock-table", response_class=HTMLResponse)
def restock_table(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    inventory_service = InventoryService(db)
    items = [i for i in inventory_service.stock_list() if i.needs_restock]
    return templates.TemplateResponse(
        request=request,
        name="partials/restock_table.html",
        context={"items": items},
    )


@router.get("/tabs/inventory", response_class=HTMLResponse)
def tab_inventory(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_inventory.html",
        context={},
    )


@router.get("/tabs/purchases", response_class=HTMLResponse)
def tab_purchases(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_purchases.html",
        context={
            "products": product_service.recent(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "purchases": inventory_service.recent_purchases(limit=20),
            "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.get("/tabs/sales", response_class=HTMLResponse)
def tab_sales(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    product_service = ProductService(db)
    inventory_service = InventoryService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_sales.html",
        context={
            "sales": inventory_service.recent_sales(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
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
    unit_cost: str = Form(""),
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
                unit_cost=_parse_optional_float(unit_cost),
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
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
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
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.post("/dev/reset-purchases-sales", response_class=HTMLResponse)
def dev_reset_purchases_sales(
    request: Request,
    panel: str = Form("purchase"),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    if not _DEV_ACTIONS_ENABLED:
        raise HTTPException(status_code=404, detail="Not found")

    service = InventoryService(db)
    product_service = ProductService(db)
    service.reset_purchases_and_sales()

    if panel == "sale":
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Ventas y compras borradas",
                "message_detail": "Se eliminaron todos los movimientos de compra y venta.",
                "message_class": "ok",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/purchase_panel.html",
        context={
            "message": "Ventas y compras borradas",
            "message_detail": "Se eliminaron todos los movimientos de compra y venta.",
            "message_class": "ok",
            "purchases": service.recent_purchases(limit=20),
            "product_options": product_service.search(query="", limit=200),
            "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
        },
    )


@router.get("/movement/purchase/{movement_id}/edit", response_class=HTMLResponse)
def purchase_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "purchase":
        raise HTTPException(status_code=404, detail="Purchase movement not found")
    product = db.get(Product, mv.product_id)
    lot = db.scalar(select(InventoryLot).where(InventoryLot.movement_id == mv.id))
    product_service = ProductService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/purchase_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": _dt_to_local_input(mv.movement_date),
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
    service = InventoryService(db)
    product_service = ProductService(db)
    sku = _extract_sku(product)
    try:
        result = service.update_purchase(
            movement_id=movement_id,
            sku=sku,
            quantity=quantity,
            unit_cost=unit_cost,
            movement_date=_parse_dt(movement_date),
            lot_code=lot_code or None,
            note=note or None,
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "message": "Compra actualizada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "message": "Error al actualizar compra",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: str = Form(""),
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
                unit_price=_parse_optional_float(unit_price),
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
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
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
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.get("/movement/sale/{movement_id}/edit", response_class=HTMLResponse)
def sale_edit_form(
    request: Request,
    movement_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    mv = db.get(InventoryMovement, movement_id)
    if mv is None or mv.type != "sale":
        raise HTTPException(status_code=404, detail="Sale movement not found")
    product = db.get(Product, mv.product_id)
    product_service = ProductService(db)
    return templates.TemplateResponse(
        request=request,
        name="partials/sale_edit_form.html",
        context={
            "movement": mv,
            "product_label": f"{product.sku} - {product.name}" if product else "",
            "movement_date_value": _dt_to_local_input(mv.movement_date),
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
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    product_service = ProductService(db)
    sku = _extract_sku(product)
    try:
        result = service.update_sale(
            movement_id=movement_id,
            sku=sku,
            quantity=quantity,
            unit_price=unit_price,
            movement_date=_parse_dt(movement_date),
            note=note or None,
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Venta actualizada",
                "message_detail": f"Stock después: {result.stock_after}",
                "message_class": "ok" if not result.warning else "warn",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/sale_panel.html",
            context={
                "message": "Error al actualizar venta",
                "message_detail": str(e.detail),
                "message_class": "error",
                "sales": service.recent_sales(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": _dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )


@router.post("/product", response_class=HTMLResponse)
def create_product(
    request: Request,
    sku: str = Form(""),
    name: str = Form(...),
    unit_of_measure: str = Form(""),
    image_url: str = Form(""),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: float = Form(...),
    default_sale_price: float = Form(...),
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
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=default_purchase_cost,
                default_sale_price=default_sale_price,
                image_url=image_url or None,
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


@router.get("/product/{sku}/edit", response_class=HTMLResponse)
def product_edit_form(
    request: Request,
    sku: str,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
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
    image_url: str = Form(""),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: str = Form(""),
    default_sale_price: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    try:
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=_parse_optional_float(default_purchase_cost),
                default_sale_price=_parse_optional_float(default_sale_price),
                image_url=image_url or None,
            ),
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
    image_url: str = Form(""),
    category: Optional[str] = Form(None),
    min_stock: float = Form(0),
    default_purchase_cost: str = Form(""),
    default_sale_price: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    product_service = ProductService(db)
    try:
        updated = product_service.update(
            sku,
            ProductUpdate(
                sku=new_sku or None,
                name=name,
                category=category or None,
                min_stock=min_stock,
                unit_of_measure=unit_of_measure or None,
                default_purchase_cost=_parse_optional_float(default_purchase_cost),
                default_sale_price=_parse_optional_float(default_sale_price),
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
    sku = _extract_sku(product)
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
    sku = _extract_sku(product)
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
