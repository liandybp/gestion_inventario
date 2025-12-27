from __future__ import annotations

from datetime import timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.business_config import load_business_config
from app.deps import session_dep
from app.models import Customer, SalesDocument, SalesDocumentItem
from app.security import get_current_user_from_session

from .ui_common import templates

router = APIRouter()


def _list_customers(db: Session, query: str, limit: int = 200) -> list[Customer]:
    q = (query or "").strip()
    stmt = select(Customer)
    if q:
        like = f"%{q}%"
        stmt = stmt.where((Customer.name.ilike(like)) | (Customer.client_id.ilike(like)))
    stmt = stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(limit)
    return list(db.scalars(stmt))


@router.get("/customers/table", response_class=HTMLResponse)
def customers_table(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    customers = _list_customers(db, query=query)
    return templates.TemplateResponse(
        request=request,
        name="partials/customers_table.html",
        context={
            "customers": customers,
            "query": query,
        },
    )


@router.post("/customer/create", response_class=HTMLResponse)
def customer_create(
    request: Request,
    client_id: str = Form(""),
    name: str = Form(""),
    address: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    client_id = (client_id or "").strip()
    name = (name or "").strip()
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")
    if not name:
        raise HTTPException(status_code=422, detail="name is required")

    customer = Customer(
        client_id=client_id,
        name=name,
        address=(address or "").strip() or None,
        phone=(phone or "").strip() or None,
        email=(email or "").strip() or None,
    )
    db.add(customer)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese ID")

    customers = _list_customers(db, query="")
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_customers.html",
        context={
            "customers": customers,
            "query": "",
            "message": "Cliente creado",
            "message_class": "ok",
        },
    )


@router.get("/customer/{customer_id}", response_class=HTMLResponse)
def customer_detail(request: Request, customer_id: int, db: Session = Depends(session_dep)) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    docs = list(
        db.scalars(
            select(SalesDocument)
            .where(SalesDocument.customer_id == customer.id)
            .order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc())
            .limit(200)
        )
    )

    total_invoices = float(
        db.scalar(
            select(func.coalesce(func.sum(SalesDocument.total), 0)).where(
                (SalesDocument.customer_id == customer.id) & (SalesDocument.doc_type == "F")
            )
        )
        or 0
    )
    total_quotes = float(
        db.scalar(
            select(func.coalesce(func.sum(SalesDocument.total), 0)).where(
                (SalesDocument.customer_id == customer.id) & (SalesDocument.doc_type == "P")
            )
        )
        or 0
    )
    count_invoices = int(
        db.scalar(
            select(func.count(SalesDocument.id)).where(
                (SalesDocument.customer_id == customer.id) & (SalesDocument.doc_type == "F")
            )
        )
        or 0
    )
    last_invoice_dt = db.scalar(
        select(func.max(SalesDocument.issue_date)).where(
            (SalesDocument.customer_id == customer.id) & (SalesDocument.doc_type == "F")
        )
    )
    last_purchase = ""
    if last_invoice_dt is not None:
        try:
            last_purchase = last_invoice_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            last_purchase = str(last_invoice_dt)

    top_items = db.execute(
        select(
            SalesDocumentItem.description,
            func.coalesce(func.sum(SalesDocumentItem.line_total), 0).label("total"),
        )
        .join(SalesDocument, SalesDocumentItem.document_id == SalesDocument.id)
        .where(SalesDocument.customer_id == customer.id)
        .group_by(SalesDocumentItem.description)
        .order_by(func.sum(SalesDocumentItem.line_total).desc())
        .limit(10)
    ).all()

    config = load_business_config()

    return templates.TemplateResponse(
        request=request,
        name="partials/customer_detail.html",
        context={
            "customer": customer,
            "documents": docs,
            "metrics": {
                "total_invoices": total_invoices,
                "total_quotes": total_quotes,
                "count_invoices": count_invoices,
                "last_purchase": last_purchase,
            },
            "top_items": [{"description": d, "total": float(t or 0)} for d, t in top_items],
            "currency_symbol": config.currency.symbol,
        },
    )


@router.get("/customer/{customer_id}/edit", response_class=HTMLResponse)
def customer_edit_form(request: Request, customer_id: int, db: Session = Depends(session_dep)) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    return templates.TemplateResponse(
        request=request,
        name="partials/customer_edit_form.html",
        context={
            "customer": customer,
        },
    )


@router.post("/customer/{customer_id}/update", response_class=HTMLResponse)
def customer_update(
    request: Request,
    customer_id: int,
    client_id: str = Form(""),
    name: str = Form(""),
    address: str = Form(""),
    phone: str = Form(""),
    email: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    client_id = (client_id or "").strip()
    name = (name or "").strip()
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")
    if not name:
        raise HTTPException(status_code=422, detail="name is required")

    customer.client_id = client_id
    customer.name = name
    customer.address = (address or "").strip() or None
    customer.phone = (phone or "").strip() or None
    customer.email = (email or "").strip() or None

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese ID")

    customers = _list_customers(db, query="")
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_customers.html",
        context={
            "customers": customers,
            "query": "",
            "message": "Cliente actualizado",
            "message_class": "ok",
        },
    )


@router.post("/customer/{customer_id}/delete", response_class=HTMLResponse)
def customer_delete(request: Request, customer_id: int, db: Session = Depends(session_dep)) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    doc_count = int(
        db.scalar(select(func.count(SalesDocument.id)).where(SalesDocument.customer_id == customer.id)) or 0
    )
    if doc_count > 0:
        customers = _list_customers(db, query="")
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_customers.html",
            context={
                "customers": customers,
                "query": "",
                "message": "No se puede eliminar: el cliente tiene documentos asociados",
                "message_class": "error",
            },
        )

    db.delete(customer)
    db.commit()

    customers = _list_customers(db, query="")
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_customers.html",
        context={
            "customers": customers,
            "query": "",
            "message": "Cliente eliminado",
            "message_class": "ok",
        },
    )
