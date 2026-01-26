from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
import pdfplumber
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import InventoryLot, InventoryMovement, Product
from app.schemas import PurchaseCreate
from app.security import get_current_user_from_session, require_active_business_id
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.invoice_parsers import parse_invoice_pdf

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
        parsed = parse_invoice_pdf(invoice_pdf.file)
    except Exception as e:
        return _render_error(
            "No se pudo importar la factura",
            f"No se pudo leer el PDF: {e}",
        )

    def _zara_pdf_diagnostics() -> Optional[str]:
        try:
            invoice_pdf.file.seek(0)
        except Exception:
            return None

        try:
            with pdfplumber.open(invoice_pdf.file) as pdf:
                ref_re = re.compile(r"\b\d+/\d{3,5}/\d{3}\b")
                per_page: list[str] = []
                for idx, page in enumerate(pdf.pages, 1):
                    text = page.extract_text() or ""
                    refs_text = ref_re.findall(text)
                    words = page.extract_words(use_text_flow=True) or []
                    word_texts = [(w.get("text") or "").strip() for w in words]
                    refs_words = [t for t in word_texts if ref_re.fullmatch(t or "")]
                    per_page.append(
                        f"p{idx}: refs_text={len(refs_text)} refs_words={len(refs_words)} words={len(words)}"
                    )
                return " | ".join(per_page)
        except Exception:
            return None
        finally:
            try:
                invoice_pdf.file.seek(0)
            except Exception:
                pass

    if parsed.invoice_date is None:
        return _render_error(
            "No se pudo importar la factura",
            "No se encontró la fecha de factura en el PDF. Verifica que sea una factura AUTODOC, ZARA o H&M y que el PDF tenga texto (no escaneado como imagen).",
        )

    if not parsed.lines:
        inv = parsed.invoice_number or "(sin número)"
        fdt = parsed.invoice_date.strftime("%Y-%m-%d") if parsed.invoice_date else "(sin fecha)"
        diag = _zara_pdf_diagnostics()
        diag_txt = f" Diagnóstico ZARA: {diag}." if diag else ""
        return _render_error(
            "No se pudo importar la factura",
            "No se encontraron líneas de productos en el PDF. "
            f"Detectado: factura {inv}, fecha {fdt}. "
            "Verifica que sea una factura AUTODOC, ZARA o H&M y que el PDF tenga texto (no escaneado como imagen)."
            + diag_txt,
        )

    user = get_current_user_from_session(db, request)

    invoice_movement_dt = parsed.invoice_date

    invoice_tag = f"Factura {parsed.invoice_number}" if parsed.invoice_number else "Factura"

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

        def _looks_like_color(w: str) -> bool:
            colors = {
                "blanco",
                "negro",
                "rojo",
                "verde",
                "azul",
                "azulon",
                "azulón",
                "amarillo",
                "beige",
                "gris",
                "marron",
                "marrón",
                "naranja",
                "rosa",
                "lila",
                "cava",
                "crudo",
                "plata",
                "dorado",
            }
            return (w or "").strip().lower() in colors

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
                category = getattr(line, "category", None)
                product = Product(
                    business_id=int(bid),
                    sku=sku,
                    name=name,
                    category=(category or None),
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
            else:
                category = getattr(line, "category", None)
                if category and not (product.category or "").strip():
                    product.category = category
                    db.commit()

                # If product name was previously saved as "COLOR ..." and we now have a clean description,
                # update it to remove the color prefix.
                old_name = (product.name or "").strip()
                old_first = (old_name.split(" ", 1)[0] if old_name else "")
                new_first = (name.split(" ", 1)[0] if name else "")
                if old_name and name and old_name != name and _looks_like_color(old_first) and not _looks_like_color(new_first):
                    product.name = name
                    db.commit()

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
        if created_movements <= 3:
            diag = _zara_pdf_diagnostics()
            if diag:
                detail = detail + f" Diagnóstico ZARA: {diag}."
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
    product: str = Form(""),
    product_name: str = Form(""),
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

    def _generate_auto_sku(prefix: str = "SKU", width: int = 6) -> str:
        rows = db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
        existing = [str(sku or "") for (sku,) in rows]
        max_n = 0
        for s in existing:
            suffix = s[len(prefix) :]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"

    sku = extract_sku(product)
    try:
        created_product = None

        if not sku:
            name_only = (product_name or "").strip()
            if not name_only:
                raise HTTPException(status_code=422, detail="Debes ingresar 'Artículo' (SKU) o 'Nombre (si es nuevo)'")

            parsed_cost = parse_optional_float(unit_cost)
            last_error: Exception | None = None
            for _ in range(5):
                auto_sku = _generate_auto_sku()
                created_product = Product(
                    business_id=int(bid),
                    sku=auto_sku,
                    name=name_only,
                    category=None,
                    min_stock=0,
                    unit_of_measure=None,
                    default_purchase_cost=parsed_cost,
                    default_sale_price=0,
                    lead_time_days=0,
                    image_url=None,
                )
                db.add(created_product)
                try:
                    db.commit()
                    db.refresh(created_product)
                    sku = created_product.sku
                    break
                except IntegrityError as e:
                    db.rollback()
                    last_error = e
                    created_product = None
            if not sku:
                raise HTTPException(status_code=409, detail="No se pudo generar un SKU automáticamente") from last_error

        try:
            stmt = select(Product).where(Product.sku == sku)
            stmt = stmt.where(Product.business_id == int(bid))
            existing_product = db.scalar(stmt)
        except Exception:
            existing_product = None

        if existing_product is None and created_product is None:
            name = (product_name or "").strip() or sku
            parsed_cost = parse_optional_float(unit_cost)
            created_product = Product(
                business_id=int(bid),
                sku=sku,
                name=name,
                category=None,
                min_stock=0,
                unit_of_measure=None,
                default_purchase_cost=parsed_cost,
                default_sale_price=0,
                lead_time_days=0,
                image_url=None,
            )
            db.add(created_product)
            db.commit()
            db.refresh(created_product)

            user = get_current_user_from_session(db, request)
            if user is not None:
                log_event(
                    db,
                    user,
                    action="product_create",
                    entity_type="product",
                    entity_id=created_product.sku,
                    detail={
                        "name": created_product.name,
                        "source": "manual_purchase",
                    },
                )

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
        message_detail = f"Stock después: {result.stock_after}"
        if created_product is not None:
            message_detail = (
                f"Producto creado: {created_product.sku} - {created_product.name}. " + message_detail
            )

        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Compra registrada",
                "message_detail": message_detail,
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


@router.post("/movement/purchase/delete-selected", response_class=HTMLResponse)
def purchase_delete_selected(
    request: Request,
    movement_ids: list[int] = Form([]),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    bid = require_active_business_id(db, request)
    service = InventoryService(db, business_id=bid)
    product_service = ProductService(db, business_id=bid)
    user = get_current_user_from_session(db, request)

    try:
        ensure_admin_or_owner(db, request)
        raw_ids = [int(x) for x in (movement_ids or []) if int(x) > 0]

        skipped: list[str] = []

        def _resolve_purchase_movement_id(candidate_id: int) -> Optional[int]:
            mv = db.get(InventoryMovement, int(candidate_id))
            if mv is not None:
                if mv.type == "purchase" and int(getattr(mv, "business_id", 0) or 0) == int(bid):
                    return int(mv.id)
                skipped.append(f"{candidate_id}: no es compra o es de otro negocio")
                return None

            lot = db.get(InventoryLot, int(candidate_id))
            if lot is None:
                skipped.append(f"{candidate_id}: no existe")
                return None
            if int(getattr(lot, "business_id", 0) or 0) != int(bid):
                skipped.append(f"{candidate_id}: lote de otro negocio")
                return None
            mv2 = db.get(InventoryMovement, int(getattr(lot, "movement_id", 0) or 0))
            if mv2 is None:
                skipped.append(f"{candidate_id}: lote sin movimiento")
                return None
            if mv2.type != "purchase":
                skipped.append(f"{candidate_id}: lote no pertenece a compra")
                return None
            if int(getattr(mv2, "business_id", 0) or 0) != int(bid):
                skipped.append(f"{candidate_id}: compra de otro negocio")
                return None
            return int(mv2.id)

        ids: list[int] = []
        seen: set[int] = set()
        for rid in raw_ids:
            resolved = _resolve_purchase_movement_id(rid)
            if resolved is None:
                continue
            if resolved in seen:
                continue
            seen.add(resolved)
            ids.append(resolved)
        if not ids:
            detail = "No se pudo resolver ninguna compra para eliminar."
            if raw_ids:
                detail = detail + f" Recibidos: {len(raw_ids)}."
            if skipped:
                detail = detail + " Detalle: " + "; ".join(skipped[:6])
            return templates.TemplateResponse(
                request=request,
                name="partials/purchase_panel.html",
                context={
                    "user": user,
                    "message": "No se pudo eliminar",
                    "message_detail": detail,
                    "message_class": "warn",
                    "purchases": service.recent_purchases(limit=20),
                    "product_options": product_service.search(query="", limit=200),
                    "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
                },
                status_code=200,
            )

        deleted = 0
        errors: list[str] = []
        for mid in ids:
            try:
                service.delete_purchase_movement(mid)
                deleted += 1
                if user is not None:
                    log_event(
                        db,
                        user,
                        action="purchase_delete",
                        entity_type="movement",
                        entity_id=str(mid),
                        detail={"bulk": True},
                    )
            except HTTPException as e:
                errors.append(f"{mid}: {e.detail}")
            except Exception as e:
                errors.append(f"{mid}: {e}")

        msg = "Compras eliminadas" if deleted > 0 else "No se pudo eliminar"
        detail = f"Se eliminaron {deleted} compra(s)."
        if errors:
            detail = detail + " Errores: " + "; ".join(errors[:5])
        if skipped:
            detail = detail + f" Ignorados: {len(skipped)} (" + "; ".join(skipped[:3]) + ")"
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": msg,
                "message_detail": detail,
                "message_class": "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=200,
        )
    except HTTPException as e:
        return templates.TemplateResponse(
            request=request,
            name="partials/purchase_panel.html",
            context={
                "user": user,
                "message": "Error al eliminar compras",
                "message_detail": str(e.detail),
                "message_class": "error",
                "purchases": service.recent_purchases(limit=20),
                "product_options": product_service.search(query="", limit=200),
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=200,
        )
