from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit import log_event
from app.business_config import parse_opening_pending
from app.deps import session_dep
from app.repositories.business_repository import (
    count_other_businesses,
    count_related,
    create_business,
    create_locations,
    delete_business,
    delete_locations,
    get_business,
    get_business_by_code,
    get_business_by_code_excluding,
    get_business_config,
    list_businesses,
    reconcile_locations,
)
from app.routers.ui_common import templates
from app.routers.ui_context import ui_context

_log = logging.getLogger(__name__)
_APP_DIR = Path(__file__).resolve().parent.parent

router = APIRouter()


def _parse_float(value, fallback: float = 0.0) -> float:
    s = (value or "").strip().replace(",", ".")
    if not s:
        return fallback
    try:
        return float(s)
    except Exception:
        return fallback


def _parse_optional_float(value) -> float | None:
    s = (value or "").strip().replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_spec_of(raw, fb_code, fb_name) -> tuple[str, str]:
    s = (raw or "").strip()
    if not s:
        return fb_code, fb_name
    if ":" in s:
        c, n = s.split(":", 1)
        return ((c or "").strip() or fb_code), ((n or "").strip() or fb_name)
    return s, s


def _str_form(value, fallback: str = "") -> str:
    """Coerce a FastAPI Form parameter to str.

    When called via HTTP, ``value`` is already a ``str``.
    When called directly (e.g. in tests) without the parameter,
    the :class:`Form` default object is passed instead.
    """
    if isinstance(value, str):
        return value.strip() or fallback
    # Likely a fastapi.params.FormInfo — pull its .default
    default = getattr(value, "default", None)
    if isinstance(default, str) and default.strip():
        return default.strip()
    return fallback


def _prefix_location_code(loc_code: str, business_code: str) -> str:
    """Prefix a location code with the business code to keep it globally unique.

    ``Location.code`` is UNIQUE across the whole DB, so bare codes like
    ``CENTRAL``/``POS1`` would collide between businesses. Already-prefixed
    codes are left unchanged.

    Fix (bug doble prefijo): la comprobación es case-insensitive y el prefijo se
    normaliza a mayúsculas para coincidir con la convención existente
    (``ROPA_CENTRAL`` aunque el negocio sea ``ropa``). Antes, un código ya
    prefijado con otra capitalización (``ROPA_POST2`` vs negocio ``ropa``) se
    prefijaba de nuevo → ``ROPA_ropa_POST2``.
    """
    loc_code = (loc_code or "").strip()
    if not loc_code:
        return ""  # sin código (p.ej. negocio sin POS) → vacío, sin prefijo suelto
    prefix = f"{business_code.strip().upper()}_"
    if loc_code.lower().startswith(prefix.lower()):
        return loc_code  # ya prefijado (cualquier capitalización)
    return f"{prefix}{loc_code}"


def _prefix_location_spec(spec: str, business_code: str) -> str:
    """Prefix the code part of a ``CODE:NAME`` spec (``CENTRAL:Almacén`` →
    ``bazar_CENTRAL:Almacén``). Already-prefixed codes are left unchanged."""
    spec = (spec or "").strip()
    if ":" in spec:
        code, name = spec.split(":", 1)
        code = code.strip().lower()
        name = name.strip() or code
        return f"{_prefix_location_code(code, business_code)}:{name}"
    return _prefix_location_code(spec, business_code)




@router.post("/businesses/create", response_class=HTMLResponse)
def business_create(
    request: Request,
    # ── business identity ───────────────────────────────────────
    code: str = Form(...),
    name: str = Form(...),
    # ── issuer ──────────────────────────────────────────────────
    issuer_holder: str = Form(""),
    issuer_tax_id: str = Form(""),
    issuer_address: str = Form(""),
    issuer_city: str = Form(""),
    issuer_postal_code: str = Form(""),
    issuer_country: str = Form(""),
    issuer_email: str = Form(""),
    issuer_phone: str = Form(""),
    # ── currency ────────────────────────────────────────────────
    currency_code: str = Form("USD"),
    currency_symbol: str = Form("$"),
    # ── purchase ────────────────────────────────────────────────
    purchase_source_currency: str = Form("EUR"),
    purchase_vat_rate: str = Form("1.21"),
    purchase_fx_rate: str = Form(""),
    # ── sales_documents ─────────────────────────────────────────
    sales_default_type: str = Form("F"),
    sales_enabled_types: str = Form("F,P"),
    sales_invoice_label: str = Form("Factura"),
    sales_quote_label: str = Form("Presupuesto"),
    # ── dividends ───────────────────────────────────────────────
    dividends_business_label: str = Form("Negocio"),
    dividends_partners: str = Form(""),
    dividends_opening_pending: str = Form(""),
    dividends_opening_pending_as_of: str = Form(""),
    # ── locations ───────────────────────────────────────────────
    locations_central: str = Form("CENTRAL:Almacén Central"),
    locations_pos: str = Form(""),
    locations_default_pos: str = Form(""),
    # ── inventory ───────────────────────────────────────────────
    inventory_replenishment_lead_time_days: str = Form("25"),
    # ── dependencies ────────────────────────────────────────────
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    current_user = ctx.user

    code = code.strip().lower()
    name = name.strip()

    if not code:
        raise HTTPException(status_code=422, detail="Código es requerido")
    if not name:
        raise HTTPException(status_code=422, detail="Nombre es requerido")

    # ── Normalise optional Form params ──────────────────────────
    def _s(v, fb: str = "") -> str:
        return _str_form(v, fb)

    existing = get_business_by_code(db, code)
    if existing is not None:
        businesses = list_businesses(db)
        return templates.TemplateResponse(
            request=request,
            name="features/businesses/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )

    new_business = create_business(db, code, name)

    # ── Locations: parse CODE:NAME specs from the form ──────────
    def _parse_loc(raw: str, fb_code: str, fb_name: str) -> tuple[str, str]:
        """Parse ``CODE:NAME`` into *(code, name)*."""
        s = (raw or "").strip()
        if not s:
            return fb_code, fb_name
        if ":" in s:
            c, n = s.split(":", 1)
            c2 = (c or "").strip()
            n2 = (n or "").strip()
            return c2 or fb_code, n2 or fb_name
        return s, s

    def _parse_loc_list(raw: str) -> list[tuple[str, str]]:
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        result: list[tuple[str, str]] = []
        for part in parts:
            if ":" in part:
                c, n = part.split(":", 1)
                result.append(((c or "").strip(), (n or "").strip() or c.strip()))
            else:
                result.append((part, part))
        return result

    # CENTRAL
    central_code, central_name = _parse_loc(
        _s(locations_central), "CENTRAL", "Almacén Central"
    )
    central_code = _prefix_location_code(central_code, code)

    # POS list (vacío = negocio sin POS); descartar specs sin código (p.ej. ':Tienda')
    pos_specs = [s for s in _parse_loc_list(_s(locations_pos)) if (s[0] or "").strip()]
    default_pos_code = _prefix_location_code(_s(locations_default_pos), code)

    pos_final = [(_prefix_location_code(pcode, code), pname) for pcode, pname in pos_specs]
    create_locations(db, new_business.id, (central_code, central_name), pos_final, default_pos_code)

    # ── Config en BD (fuente única de verdad; el .conf es legacy) ──
    new_business.issuer_name = name
    new_business.issuer_holder = _s(issuer_holder)
    new_business.issuer_tax_id = _s(issuer_tax_id)
    new_business.issuer_address = _s(issuer_address)
    new_business.issuer_city = _s(issuer_city)
    new_business.issuer_postal_code = _s(issuer_postal_code)
    new_business.issuer_country = _s(issuer_country)
    new_business.issuer_email = _s(issuer_email)
    new_business.issuer_phone = _s(issuer_phone)
    new_business.currency_code = _s(currency_code, "USD")
    new_business.currency_symbol = _s(currency_symbol, "$")
    new_business.purchase_invoice_source_currency = _s(purchase_source_currency, "EUR")
    new_business.purchase_invoice_vat_rate = _parse_float(_s(purchase_vat_rate, "1.21"), 1.21)
    new_business.purchase_invoice_fx_rate = _parse_optional_float(_s(purchase_fx_rate))
    new_business.sales_default_type = (_s(sales_default_type, "F").upper() or "F")
    new_business.sales_enabled_types = _s(sales_enabled_types, "F,P")
    new_business.sales_invoice_label = _s(sales_invoice_label, "Factura")
    new_business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
    new_business.dividends_business_label = _s(dividends_business_label, "Negocio")
    new_business.dividends_partners = _s(dividends_partners)
    new_business.dividends_opening_pending = json.dumps(parse_opening_pending(_s(dividends_opening_pending)))
    new_business.dividends_opening_pending_as_of = _s(dividends_opening_pending_as_of)
    new_business.inventory_replenishment_lead_time_days = _parse_float(_s(inventory_replenishment_lead_time_days, "25"), 25.0)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        businesses = list_businesses(db)
        return templates.TemplateResponse(
            request=request,
            name="features/businesses/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"El código '{code}' ya existe (creado por otra sesión)",
                "message_class": "error",
            },
            status_code=409,
        )

    db.refresh(new_business)

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_create",
            entity_type="business",
            entity_id=str(new_business.id),
            detail={
                "code": code,
                "name": name,
                "currency": _s(currency_code, "USD"),
                "pos_count": len(pos_specs),
            },
        )
        db.commit()

    businesses = list_businesses(db)
    return templates.TemplateResponse(
        request=request,
        name="features/businesses/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio creado",
            "message_detail": (
                f"Negocio '{name}' creado con {len(pos_specs)} POS"
            ),
            "message_class": "ok",
        },
    )


@router.get("/business/{business_id}/edit", response_class=HTMLResponse)
def business_edit_form(
    request: Request,
    business_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    business = get_business(db, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    config = get_business_config(db, business_id)

    return templates.TemplateResponse(
        request=request,
        name="features/businesses/business_edit_form.html",
        context={"business": business, "config": config},
    )


@router.post("/business/{business_id}/update", response_class=HTMLResponse)
def business_update(
    request: Request,
    business_id: int,
    # ── business identity ───────────────────────────────────────
    code: str = Form(...),
    name: str = Form(...),
    # ── issuer ──────────────────────────────────────────────────
    issuer_holder: str = Form(""),
    issuer_tax_id: str = Form(""),
    issuer_address: str = Form(""),
    issuer_city: str = Form(""),
    issuer_postal_code: str = Form(""),
    issuer_country: str = Form(""),
    issuer_email: str = Form(""),
    issuer_phone: str = Form(""),
    # ── currency ────────────────────────────────────────────────
    currency_code: str = Form("USD"),
    currency_symbol: str = Form("$"),
    # ── purchase ────────────────────────────────────────────────
    purchase_source_currency: str = Form("EUR"),
    purchase_vat_rate: str = Form("1.21"),
    purchase_fx_rate: str = Form(""),
    # ── sales_documents ─────────────────────────────────────────
    sales_default_type: str = Form("F"),
    sales_enabled_types: str = Form("F,P"),
    sales_invoice_label: str = Form("Factura"),
    sales_quote_label: str = Form("Presupuesto"),
    # ── dividends ───────────────────────────────────────────────
    dividends_business_label: str = Form("Negocio"),
    dividends_partners: str = Form(""),
    dividends_opening_pending: str = Form(""),
    dividends_opening_pending_as_of: str = Form(""),
    # ── locations ───────────────────────────────────────────────
    locations_central: str = Form("CENTRAL:Almacén Central"),
    locations_pos: str = Form(""),
    locations_default_pos: str = Form(""),
    # ── inventory ───────────────────────────────────────────────
    inventory_replenishment_lead_time_days: str = Form("25"),
    # ── dependencies ────────────────────────────────────────────
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    current_user = ctx.user
    business = get_business(db, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    code = code.strip().lower()
    name = name.strip()

    if not code:
        raise HTTPException(status_code=422, detail="Código es requerido")
    if not name:
        raise HTTPException(status_code=422, detail="Nombre es requerido")

    # ── Normalise optional Form params ──────────────────────────
    def _s(v, fb: str = "") -> str:
        return _str_form(v, fb)

    existing = get_business_by_code_excluding(db, code, business_id)
    if existing is not None:
        response = templates.TemplateResponse(
            request=request,
            name="features/businesses/business_edit_form.html",
            context={
                "business": business,
                "config": get_business_config(db, business_id),
                "message": "Error al actualizar",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    business.code = code
    business.name = name

    # ── Config en BD (fuente única de verdad; el .conf es legacy) ──
    business.issuer_name = name
    business.issuer_holder = _s(issuer_holder)
    business.issuer_tax_id = _s(issuer_tax_id)
    business.issuer_address = _s(issuer_address)
    business.issuer_city = _s(issuer_city)
    business.issuer_postal_code = _s(issuer_postal_code)
    business.issuer_country = _s(issuer_country)
    business.issuer_email = _s(issuer_email)
    business.issuer_phone = _s(issuer_phone)
    business.currency_code = _s(currency_code, "USD")
    business.currency_symbol = _s(currency_symbol, "$")
    business.purchase_invoice_source_currency = _s(purchase_source_currency, "EUR")
    business.purchase_invoice_vat_rate = _parse_float(_s(purchase_vat_rate, "1.21"), 1.21)
    business.purchase_invoice_fx_rate = _parse_optional_float(_s(purchase_fx_rate))
    business.sales_default_type = (_s(sales_default_type, "F").upper() or "F")
    business.sales_enabled_types = _s(sales_enabled_types, "F,P")
    business.sales_invoice_label = _s(sales_invoice_label, "Factura")
    business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
    business.dividends_business_label = _s(dividends_business_label, "Negocio")
    business.dividends_partners = _s(dividends_partners)
    business.dividends_opening_pending = json.dumps(parse_opening_pending(_s(dividends_opening_pending)))
    business.dividends_opening_pending_as_of = _s(dividends_opening_pending_as_of)
    business.inventory_replenishment_lead_time_days = _parse_float(_s(inventory_replenishment_lead_time_days, "25"), 25.0)

    # ── Reconciliar ubicaciones en sitio (sin borrar las que tienen datos) ──
    central_raw, pos_raw, default_pos_raw = _s(locations_central), _s(locations_pos), _s(locations_default_pos)
    central_code, central_name = _parse_spec_of(central_raw, "CENTRAL", "Almacén Central")
    central_final = (_prefix_location_code(central_code, code), central_name)
    raw_parts = [p.strip() for p in (pos_raw or "").split(",") if p.strip()]
    parsed = [
        s for s in (_parse_spec_of(p, "POS1", "Punto de venta 1") for p in raw_parts)
        if (s[0] or "").strip()
    ]
    pos_final = [(_prefix_location_code(pc, code), pn) for pc, pn in parsed]
    default_pos_final = _prefix_location_code((default_pos_raw or "").strip(), code)
    reconcile_error = reconcile_locations(db, business.id, central_final, pos_final, default_pos_final)
    if reconcile_error:
        db.rollback()
        response = templates.TemplateResponse(
            request=request,
            name="features/businesses/business_edit_form.html",
            context={
                "business": business,
                "config": get_business_config(db, business_id),
                "message": "Error al actualizar",
                "message_detail": reconcile_error,
                "message_class": "error",
            },
            status_code=409,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_update",
            entity_type="business",
            entity_id=str(business.id),
            detail={"code": code, "name": name},
        )
        db.commit()

    businesses = list_businesses(db)
    return templates.TemplateResponse(
        request=request,
        name="features/businesses/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio actualizado",
            "message_detail": f"Negocio '{name}' actualizado correctamente",
            "message_class": "ok",
        },
    )


@router.post("/business/{business_id}/delete", response_class=HTMLResponse)
def business_delete(
    request: Request,
    business_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    current_user = ctx.user
    business = get_business(db, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    # Prevent deleting the last business
    remaining = count_other_businesses(db, business_id)
    if remaining == 0:
        businesses = list_businesses(db)
        return templates.TemplateResponse(
            request=request,
            name="features/businesses/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": "No puedes eliminar el último negocio",
                "message_class": "error",
            },
            status_code=400,
        )

    code = business.code
    name = business.name

    # Explicit dependency checks (works on both SQLite and Postgres)
    related = count_related(db, business_id, code)
    user_count = related["users"]
    product_count = related["products"]
    other_location_count = related["locations"]
    user_business_count = related["user_businesses"]

    if user_count > 0 or product_count > 0 or other_location_count > 0 or user_business_count > 0:
        businesses = list_businesses(db)
        parts = []
        if user_count > 0:
            parts.append(f"{user_count} usuario(s)")
        if user_business_count > 0:
            parts.append(f"{user_business_count} asignación(es) a dueño")
        if product_count > 0:
            parts.append(f"{product_count} producto(s)")
        if other_location_count > 0:
            parts.append("ubicaciones adicionales")
        return templates.TemplateResponse(
            request=request,
            name="features/businesses/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": (
                    f"No se puede eliminar '{name}': "
                    f"tiene {', '.join(parts)} asociado(s). "
                    "Elimina esos datos primero"
                ),
                "message_class": "error",
            },
            status_code=409,
        )

    # Borrar todas las ubicaciones del negocio, luego el negocio
    delete_locations(db, business_id)

    try:
        delete_business(db, business_id)
        db.commit()
    except IntegrityError:
        db.rollback()
        businesses = list_businesses(db)
        return templates.TemplateResponse(
            request=request,
            name="features/businesses/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": (
                    f"No se puede eliminar '{name}': "
                    "tiene datos asociados que no se pudieron verificar"
                ),
                "message_class": "error",
            },
            status_code=409,
        )

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_delete",
            entity_type="business",
            entity_id=str(business_id),
            detail={"code": code, "name": name},
        )
        db.commit()

    businesses = list_businesses(db)
    return templates.TemplateResponse(
        request=request,
        name="features/businesses/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio eliminado",
            "message_detail": f"Negocio '{name}' eliminado correctamente",
            "message_class": "ok",
        },
    )


@router.get("/tabs/businesses", response_class=HTMLResponse)
def tab_businesses(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ctx = ui_context(request, db)
    ctx.ensure_admin()
    businesses = list_businesses(db)
    return templates.TemplateResponse(
        request=request,
        name="features/businesses/tab_businesses.html",
        context={"businesses": businesses},
    )
