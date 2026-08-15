from __future__ import annotations
from app.repositories.business_repository import get_active_business_config

from datetime import timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.repositories.customer_repository import CustomerRepository
from app.models import Customer
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)

from .ui_common import ensure_admin, templates

router = APIRouter()


def _list_customers(db: Session, query: str, limit: int = 200, business_id: int = 0) -> list[Customer]:
    return CustomerRepository(db, business_id).list(query=query, limit=limit)


@router.get("/customers/table", response_class=HTMLResponse)
def customers_table(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    bid = require_active_business_id(db, request)
    customers = _list_customers(db, query=query, business_id=bid)
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
    bid = require_active_business_id(db, request)
    client_id = (client_id or "").strip()
    name = (name or "").strip()
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")
    if not name:
        raise HTTPException(status_code=422, detail="name is required")

    customer = CustomerRepository(db, bid).create(
        client_id=client_id,
        name=name,
        address=(address or "").strip() or None,
        phone=(phone or "").strip() or None,
        email=(email or "").strip() or None,
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Ya existe un cliente con ese ID")

    customers = _list_customers(db, query="", business_id=bid)
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
    bid = require_active_business_id(db, request)
    customer = CustomerRepository(db, bid).get(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if int(getattr(customer, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    repo = CustomerRepository(db, bid)
    docs = repo.recent_documents(customer.id, limit=200)

    total_invoices = repo.document_totals(customer.id, "F")
    total_quotes = repo.document_totals(customer.id, "P")
    count_invoices = repo.document_count(customer.id, "F")
    last_invoice_dt = repo.last_invoice_date(customer.id)
    last_purchase = ""
    if last_invoice_dt is not None:
        try:
            last_purchase = last_invoice_dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
        except Exception:
            last_purchase = str(last_invoice_dt)

    top_items = repo.top_items(customer.id, limit=10)

    config = get_active_business_config(db, request)

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
    bid = require_active_business_id(db, request)
    customer = CustomerRepository(db, bid).get(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if int(getattr(customer, "business_id", 0) or 0) != int(bid):
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
    bid = require_active_business_id(db, request)
    customer = CustomerRepository(db, bid).get(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if int(getattr(customer, "business_id", 0) or 0) != int(bid):
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

    customers = _list_customers(db, query="", business_id=bid)
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
    bid = require_active_business_id(db, request)
    customer = CustomerRepository(db, bid).get(customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if int(getattr(customer, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    doc_count = CustomerRepository(db, bid).document_count(customer.id)
    if doc_count > 0:
        customers = _list_customers(db, query="", business_id=bid)
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

    customers = _list_customers(db, query="", business_id=bid)
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


@router.get("/tabs/customers", response_class=HTMLResponse)
def tab_customers(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    ensure_admin(db, request)
    _ = get_current_user_from_session(db, request)
    bid = require_active_business_id(db, request)
    q = (query or "").strip()
    customers = CustomerRepository(db, bid).list_like(query=q, limit=200)
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_customers.html",
        context={
            "customers": customers,
            "query": q,
        },
    )
