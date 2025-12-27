from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, Response
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.business_config import load_business_config
from app.deps import session_dep
from app.models import Product, SalesDocument, SalesDocumentItem
from app.sales_document_pdf import build_sales_document_pdf
from app.security import get_current_user_from_session
from app.services.product_service import ProductService

from .ui_common import extract_sku, templates

router = APIRouter()


def _calc_cart_items(cart: list[dict]) -> tuple[list[dict], float]:
    items: list[dict] = []
    subtotal = 0.0
    for i, it in enumerate(cart, start=1):
        desc = str(it.get("description") or "").strip() or "(sin descripción)"
        qty = float(it.get("quantity") or 0)
        unit_price = float(it.get("unit_price") or 0)
        line_total = float(qty * unit_price)
        subtotal += line_total
        items.append(
            {
                "line_no": i,
                "description": desc,
                "quantity": qty,
                "unit_price": unit_price,
                "line_total": line_total,
            }
        )
    return items, subtotal


def _get_cart(request: Request) -> list[dict]:
    session = getattr(request, "session", None) or {}
    cart = session.get("sales_doc_cart")
    if not isinstance(cart, list):
        return []
    out: list[dict] = []
    for it in cart:
        if not isinstance(it, dict):
            continue
        out.append(it)
    return out


def _set_cart(request: Request, cart: list[dict]) -> None:
    try:
        request.session["sales_doc_cart"] = cart
    except Exception:
        pass


def _clear_cart(request: Request) -> None:
    try:
        request.session.pop("sales_doc_cart", None)
    except Exception:
        pass


def _recent_documents(db: Session, limit: int = 10) -> list[SalesDocument]:
    return list(
        db.scalars(select(SalesDocument).order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(limit))
    )


@router.post("/sales-doc/preview", response_class=HTMLResponse)
def sales_doc_preview(
    request: Request,
    doc_type: str = Form(""),
    client_name: str = Form(""),
    client_id: str = Form(""),
    client_address: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    config = load_business_config()

    doc_type_norm = (doc_type or config.sales_documents.default_type or "F").strip().upper()
    if doc_type_norm not in ("F", "P"):
        doc_type_norm = "F"

    client_name = (client_name or "").strip()
    client_id = (client_id or "").strip()
    client_address = (client_address or "").strip() or None
    notes = (notes or "").strip() or None

    if not client_name or not client_id:
        raise HTTPException(status_code=422, detail="Cliente (Nombre e ID) es obligatorio")

    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    items, subtotal = _calc_cart_items(cart)
    total = subtotal
    doc_label = config.sales_documents.invoice_label if doc_type_norm == "F" else config.sales_documents.quote_label

    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_preview.html",
        context={
            "doc_type": doc_type_norm,
            "doc_label": doc_label,
            "client_name": client_name,
            "client_id": client_id,
            "client_address": client_address,
            "notes": notes,
            "items": items,
            "subtotal": subtotal,
            "total": total,
            "currency_symbol": config.currency.symbol,
        },
    )


@router.get("/sales-doc/{doc_id}/edit", response_class=HTMLResponse)
def sales_doc_edit_form(
    request: Request,
    doc_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    doc = db.get(SalesDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Documento no encontrado")
    items = list(
        db.scalars(
            select(SalesDocumentItem)
            .where(SalesDocumentItem.document_id == doc.id)
            .order_by(SalesDocumentItem.line_no.asc(), SalesDocumentItem.id.asc())
        )
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_edit_form.html",
        context={
            "doc": doc,
            "items": items,
        },
    )


@router.post("/sales-doc/{doc_id}/update", response_class=HTMLResponse)
async def sales_doc_update(
    request: Request,
    doc_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    config = load_business_config()

    doc = db.get(SalesDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Documento no encontrado")

    form = await request.form()
    client_name = (form.get("client_name") or "").strip()
    client_id = (form.get("client_id") or "").strip()
    client_address = (form.get("client_address") or "").strip() or None
    notes = (form.get("notes") or "").strip() or None

    if not client_name:
        raise HTTPException(status_code=422, detail="client_name is required")
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")

    items = list(
        db.scalars(
            select(SalesDocumentItem)
            .where(SalesDocumentItem.document_id == doc.id)
            .order_by(SalesDocumentItem.line_no.asc(), SalesDocumentItem.id.asc())
        )
    )

    subtotal = 0.0
    next_line = 1
    for it in items:
        del_flag = form.get(f"del_{it.id}")
        if del_flag:
            db.delete(it)
            continue

        desc = (form.get(f"desc_{it.id}") or it.description or "").strip() or "(sin descripción)"
        try:
            qty = float(form.get(f"qty_{it.id}") or 0)
        except Exception:
            qty = 0.0
        try:
            price = float(form.get(f"price_{it.id}") or 0)
        except Exception:
            price = 0.0

        it.line_no = next_line
        next_line += 1
        it.description = desc
        it.quantity = float(qty)
        it.unit_price = float(price)
        it.line_total = float(qty * price)
        subtotal += float(it.line_total or 0)

    new_desc = (form.get("new_desc") or "").strip()
    try:
        new_qty = float(form.get("new_qty") or 0)
    except Exception:
        new_qty = 0.0
    try:
        new_price = float(form.get("new_price") or 0)
    except Exception:
        new_price = 0.0

    if new_desc and new_qty > 0:
        new_line_total = float(new_qty * new_price)
        db.add(
            SalesDocumentItem(
                document_id=doc.id,
                line_no=next_line,
                sku=None,
                description=new_desc,
                unit_of_measure=None,
                quantity=float(new_qty),
                unit_price=float(new_price),
                line_total=new_line_total,
            )
        )
        subtotal += new_line_total

    doc.client_name = client_name
    doc.client_id = client_id
    doc.client_address = client_address
    doc.notes = notes
    doc.currency_code = config.currency.code
    doc.currency_symbol = config.currency.symbol
    doc.subtotal = subtotal
    doc.total = subtotal

    db.commit()

    cart = _get_cart(request)
    doc_label = config.sales_documents.invoice_label if (doc.doc_type or "").upper() == "F" else config.sales_documents.quote_label

    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": cart,
            "recent_documents": _recent_documents(db, limit=10),
            "message": f"{doc_label} actualizada: {doc.code}",
            "message_class": "ok",
            "issued_doc": doc,
        },
    )


@router.get("/sales-doc/product-defaults", response_class=HTMLResponse)
def sales_doc_product_defaults(
    product: str = "",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    sku = extract_sku(product)
    if not sku:
        return HTMLResponse("")
    p = ProductService(db).get_by_sku(sku)
    price = "" if p.default_sale_price is None else str(float(p.default_sale_price))
    return HTMLResponse(
        f"<input id='sales-doc-unit-price' hx-swap-oob='true' name='unit_price' type='number' step='0.0001' min='0' value='{price}' />"
    )


@router.post("/sales-doc/cart/add", response_class=HTMLResponse)
def sales_doc_cart_add(
    request: Request,
    product: str = Form(""),
    description: str = Form(""),
    quantity: float = Form(1),
    unit_price: float = Form(0),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    config = load_business_config()
    product_service = ProductService(db)

    sku = extract_sku(product)
    desc = (description or "").strip()
    uom: Optional[str] = None

    if sku:
        p = product_service.get_by_sku(sku)
        if p is not None:
            if not desc:
                desc = f"{p.sku} - {p.name}".strip(" -")
            uom = p.unit_of_measure
        elif not desc:
            desc = product.strip()

    if not desc:
        desc = product.strip() or "(sin descripción)"

    if quantity <= 0:
        raise HTTPException(status_code=422, detail="quantity must be > 0")

    cart = _get_cart(request)
    cart.append(
        {
            "sku": sku or None,
            "description": desc,
            "unit_of_measure": uom,
            "quantity": float(quantity),
            "unit_price": float(unit_price or 0),
        }
    )
    _set_cart(request, cart)

    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": cart,
            "recent_documents": _recent_documents(db, limit=10),
            "message": "Producto agregado al documento",
            "message_class": "ok",
        },
    )


@router.post("/sales-doc/cart/remove", response_class=HTMLResponse)
def sales_doc_cart_remove(
    request: Request,
    index: int = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    config = load_business_config()
    cart = _get_cart(request)
    if 0 <= index < len(cart):
        cart.pop(index)
    _set_cart(request, cart)

    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": cart,
            "recent_documents": _recent_documents(db, limit=10),
        },
    )


@router.post("/sales-doc/cart/clear", response_class=HTMLResponse)
def sales_doc_cart_clear(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    config = load_business_config()
    _clear_cart(request)
    cart: list[dict] = []
    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": cart,
            "recent_documents": _recent_documents(db, limit=10),
        },
    )


@router.post("/sales-doc/issue", response_class=HTMLResponse)
def sales_doc_issue(
    request: Request,
    doc_type: str = Form(""),
    client_name: str = Form(""),
    client_id: str = Form(""),
    client_address: str = Form(""),
    notes: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    config = load_business_config()

    doc_type_norm = (doc_type or config.sales_documents.default_type or "F").strip().upper()
    if doc_type_norm not in ("F", "P"):
        doc_type_norm = "F"

    client_name = (client_name or "").strip()
    client_id = (client_id or "").strip()
    client_address = (client_address or "").strip() or None
    notes = (notes or "").strip() or None

    if not client_name:
        raise HTTPException(status_code=422, detail="client_name is required")
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")

    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    now = datetime.now(timezone.utc)
    year_month = now.strftime("%Y%m")
    yyyymmdd = now.strftime("%Y%m%d")

    max_seq = db.scalar(select(func.max(SalesDocument.seq)).where(SalesDocument.year_month == year_month))
    seq = int(max_seq or 0) + 1
    code = f"{doc_type_norm}{yyyymmdd}{seq:04d}"

    issuer_address_parts = [
        config.issuer.address,
        " ".join([p for p in [config.issuer.postal_code, config.issuer.city] if p]),
        config.issuer.country,
    ]
    issuer_address = ", ".join([p for p in issuer_address_parts if (p or "").strip()]) or None

    items: list[SalesDocumentItem] = []
    subtotal = 0.0
    for i, it in enumerate(cart, start=1):
        desc = str(it.get("description") or "").strip() or "(sin descripción)"
        qty = float(it.get("quantity") or 0)
        unit_price = float(it.get("unit_price") or 0)
        line_total = float(qty * unit_price)
        subtotal += line_total
        items.append(
            SalesDocumentItem(
                line_no=i,
                sku=(str(it.get("sku") or "").strip() or None),
                description=desc,
                unit_of_measure=(str(it.get("unit_of_measure") or "").strip() or None),
                quantity=qty,
                unit_price=unit_price,
                line_total=line_total,
            )
        )

    doc = SalesDocument(
        doc_type=doc_type_norm,
        year_month=year_month,
        seq=seq,
        code=code,
        issue_date=now,
        issuer_name=config.issuer.name,
        issuer_tax_id=config.issuer.tax_id or None,
        issuer_address=issuer_address,
        client_name=client_name,
        client_id=client_id,
        client_address=client_address,
        currency_code=config.currency.code,
        currency_symbol=config.currency.symbol,
        notes=notes,
        subtotal=subtotal,
        total=subtotal,
    )

    db.add(doc)
    db.flush()

    for it in items:
        it.document_id = doc.id
        db.add(it)

    db.commit()

    _clear_cart(request)

    doc_label = config.sales_documents.invoice_label if doc_type_norm == "F" else config.sales_documents.quote_label

    return templates.TemplateResponse(
        request=request,
        name="partials/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "cart": [],
            "recent_documents": _recent_documents(db, limit=10),
            "message": f"{doc_label} emitida: {code}",
            "message_class": "ok",
            "issued_doc": doc,
        },
    )


@router.get("/sales-doc/{doc_id}/pdf")
def sales_doc_pdf(
    request: Request,
    doc_id: int,
    db: Session = Depends(session_dep),
    download: int = 0,
) -> Response:
    _ = get_current_user_from_session(db, request)
    doc = db.get(SalesDocument, doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Documento no encontrado")

    items = list(
        db.scalars(
            select(SalesDocumentItem)
            .where(SalesDocumentItem.document_id == doc.id)
            .order_by(SalesDocumentItem.line_no.asc())
        )
    )

    config = load_business_config()
    doc_label = config.sales_documents.invoice_label if (doc.doc_type or "").upper() == "F" else config.sales_documents.quote_label

    try:
        pdf_bytes = build_sales_document_pdf(
            doc_label=doc_label,
            code=doc.code,
            issue_date=doc.issue_date,
            currency_symbol=doc.currency_symbol,
            issuer_name=doc.issuer_name,
            issuer_tax_id=doc.issuer_tax_id,
            issuer_address=doc.issuer_address,
            client_name=doc.client_name,
            client_id=doc.client_id,
            client_address=doc.client_address,
            items=[
                {
                    "description": i.description,
                    "quantity": i.quantity,
                    "unit_price": i.unit_price,
                    "line_total": i.line_total,
                }
                for i in items
            ],
            subtotal=float(doc.subtotal or 0),
            total=float(doc.total or 0),
            notes=doc.notes,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))

    filename = f"{doc.code}.pdf"
    disposition = "attachment" if download else "inline"

    headers = {"Content-Disposition": f"{disposition}; filename=\"{filename}\""}
    return Response(content=pdf_bytes, media_type="application/pdf", headers=headers)
