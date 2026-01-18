from __future__ import annotations

from datetime import timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import and_, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.business_config import load_business_config
from app.deps import session_dep
from app.models import Customer, SalesDocument, SalesDocumentItem
from app.security import get_active_business_code, get_active_business_id, get_current_user_from_session
from app.utils import query_match

from .ui_common import templates

router = APIRouter()


def _list_customers(db: Session, query: str, limit: int = 200, business_id: Optional[int] = None) -> list[Customer]:
    q = (query or "").strip()
    prefetch_limit = max(int(limit or 0) * 10, 500) if q else int(limit or 0)
    stmt = select(Customer)
    if business_id is not None:
        stmt = stmt.where(Customer.business_id == int(business_id))
    stmt = stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(prefetch_limit)
    rows = list(db.scalars(stmt))
    if q:
        rows = [c for c in rows if query_match(q, str(c.name or ""), str(c.client_id or ""))]
    return rows[: int(limit or 0)]


@router.get("/customers/table", response_class=HTMLResponse)
def customers_table(request: Request, db: Session = Depends(session_dep), query: str = "") -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    bid = get_active_business_id(db, request)
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
    bid = get_active_business_id(db, request)
    client_id = (client_id or "").strip()
    name = (name or "").strip()
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")
    if not name:
        raise HTTPException(status_code=422, detail="name is required")

    customer = Customer(
        business_id=int(bid) if bid is not None else None,
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
    bid = get_active_business_id(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if bid is not None and int(getattr(customer, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    docs = list(
        db.scalars(
            select(SalesDocument)
            .where(SalesDocument.customer_id == customer.id)
            .where(True if bid is None else (SalesDocument.business_id == int(bid)))
            .order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc())
            .limit(200)
        )
    )

    total_invoices = float(
        db.scalar(
            select(func.coalesce(func.sum(SalesDocument.total), 0)).where(
                (SalesDocument.customer_id == customer.id)
                & (SalesDocument.doc_type == "F")
                & (True if bid is None else (SalesDocument.business_id == int(bid)))
            )
        )
        or 0
    )
    total_quotes = float(
        db.scalar(
            select(func.coalesce(func.sum(SalesDocument.total), 0)).where(
                (SalesDocument.customer_id == customer.id)
                & (SalesDocument.doc_type == "P")
                & (True if bid is None else (SalesDocument.business_id == int(bid)))
            )
        )
        or 0
    )
    count_invoices = int(
        db.scalar(
            select(func.count(SalesDocument.id)).where(
                (SalesDocument.customer_id == customer.id)
                & (SalesDocument.doc_type == "F")
                & (True if bid is None else (SalesDocument.business_id == int(bid)))
            )
        )
        or 0
    )
    last_invoice_dt = db.scalar(
        select(func.max(SalesDocument.issue_date)).where(
            (SalesDocument.customer_id == customer.id)
            & (SalesDocument.doc_type == "F")
            & (True if bid is None else (SalesDocument.business_id == int(bid)))
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
        .where(
            and_(
                SalesDocument.customer_id == customer.id,
                True if bid is None else (SalesDocument.business_id == int(bid)),
            )
        )
        .group_by(SalesDocumentItem.description)
        .order_by(func.sum(SalesDocumentItem.line_total).desc())
        .limit(10)
    ).all()

    config = load_business_config(get_active_business_code(db, request))

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
    bid = get_active_business_id(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if bid is not None and int(getattr(customer, "business_id", 0) or 0) != int(bid):
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
    bid = get_active_business_id(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if bid is not None and int(getattr(customer, "business_id", 0) or 0) != int(bid):
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
    bid = get_active_business_id(db, request)
    customer = db.get(Customer, customer_id)
    if customer is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    if bid is not None and int(getattr(customer, "business_id", 0) or 0) != int(bid):
        raise HTTPException(status_code=404, detail="Cliente no encontrado")

    doc_count = int(
        db.scalar(
            select(func.count(SalesDocument.id)).where(
                and_(
                    SalesDocument.customer_id == customer.id,
                    True if bid is None else (SalesDocument.business_id == int(bid)),
                )
            )
        )
        or 0
    )
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
