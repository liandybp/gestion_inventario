from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit import log_event
from app.business_config import parse_opening_pending
from app.deps import session_dep
from app.models import (
    Business,
    InventoryLot,
    InventoryMovement,
    Location,
    Product,
    SalesDocument,
    User,
    UserBusiness,
)
from app.repositories.business_repository import get_business_config
from app.routers.ui_common import ensure_admin, templates
from app.security import get_current_user_from_session

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


def _location_has_data(db, location_id: int) -> bool:
    """True si la ubicación tiene movimientos/lotes/documentos asociados."""
    for model in (InventoryMovement, InventoryLot, SalesDocument):
        try:
            count = db.scalar(
                select(func.count()).select_from(model).where(model.location_id == int(location_id))
            )
            if count:
                return True
        except Exception:
            _log.exception("_location_has_data: fallo de query en %s", model.__tablename__)
            continue
    return False


def _reconcile_locations(db, business, central_raw, pos_raw, default_pos_raw, code) -> str | None:
    """Reconcilia las ubicaciones del negocio en BD con los specs del form.

    Actualiza central/POS en sitio, añade los nuevos y borra los sobrantes SOLO
    si no tienen datos asociados; si los tienen, devuelve un mensaje de error.
    """
    def _parse_spec(raw, fb_code, fb_name) -> tuple[str, str]:
        s = (raw or "").strip()
        if not s:
            return fb_code, fb_name
        if ":" in s:
            c, n = s.split(":", 1)
            return ((c or "").strip() or fb_code), ((n or "").strip() or fb_name)
        return s, s

    bid = int(business.id)

    # CENTRAL
    central_code, central_name = _parse_spec(central_raw, "CENTRAL", "Almacén Central")
    central_code = _prefix_location_code(central_code, code)
    central = db.scalar(select(Location).where(Location.business_id == bid, Location.role == "central"))
    if central is None:
        central = db.scalar(select(Location).where(Location.business_id == bid, Location.code == central_code))
    if central is None:
        central = Location(business_id=bid, code=central_code, name=central_name, role="central", position=0)
        db.add(central)
    else:
        central.code = central_code
        central.name = central_name
        central.role = "central"
        central.position = 0

    # POS (vacío = negocio sin POS: no crear y borrar los sobrantes sin datos)
    raw_parts = [p.strip() for p in (pos_raw or "").split(",") if p.strip()]
    parsed = [
        s for s in (_parse_spec(p, "POS1", "Punto de venta 1") for p in raw_parts)
        if (s[0] or "").strip()
    ]
    default_pos_code = _prefix_location_code((default_pos_raw or "").strip(), code)

    existing = list(db.scalars(
        select(Location).where(Location.business_id == bid, Location.role == "pos")
        .order_by(Location.position.asc(), Location.id.asc())
    ).all())

    for loc in existing:
        loc.is_default = False

    for i, (pcode, pname) in enumerate(parsed, start=1):
        loc_code = _prefix_location_code(pcode, code)
        is_def = (loc_code == default_pos_code)
        if i - 1 < len(existing):
            loc = existing[i - 1]
            loc.code = loc_code
            loc.name = pname
            loc.role = "pos"
            loc.position = i
            loc.is_default = is_def
        else:
            db.add(Location(business_id=bid, code=loc_code, name=pname,
                            role="pos", position=i, is_default=is_def))

    if len(existing) > len(parsed):
        for loc in existing[len(parsed):]:
            if _location_has_data(db, int(loc.id)):
                return f"No se puede eliminar la ubicación '{loc.name}' ({loc.code}) porque tiene datos asociados."
            db.delete(loc)

    db.flush()
    return None


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


def _prefix_location_list(spec_list: str, business_code: str) -> str:
    """Prefix every ``CODE:NAME`` entry in a comma-separated list."""
    return ", ".join(
        _prefix_location_spec(p, business_code)
        for p in (spec_list or "").split(",")
        if p.strip()
    )


def _write_business_config(
    *,
    code: str,
    # ── issuer ──────────────────────────────────────────────
    issuer_name: str,
    issuer_tax_id: str = "",
    issuer_address: str = "",
    issuer_city: str = "",
    issuer_postal_code: str = "",
    issuer_country: str = "",
    issuer_email: str = "",
    issuer_phone: str = "",
    # ── currency ────────────────────────────────────────────
    currency_code: str = "USD",
    currency_symbol: str = "$",
    # ── purchase ────────────────────────────────────────────
    purchase_source_currency: str = "EUR",
    purchase_vat_rate: str = "1.21",
    purchase_fx_rate: str = "",
    # ── sales_documents ─────────────────────────────────────
    sales_default_type: str = "F",
    sales_enabled_types: str = "F,P",
    sales_invoice_label: str = "Factura",
    sales_quote_label: str = "Presupuesto",
    # ── dividends ───────────────────────────────────────────
    dividends_business_label: str = "Negocio",
    dividends_partners: str = "",
    dividends_opening_pending: str = "",
    dividends_opening_pending_as_of: str = "",
    # ── locations ───────────────────────────────────────────
    locations_central: str = "",
    locations_pos: str = "",
    locations_default_pos: str = "",
    # ── inventory ───────────────────────────────────────────
    inventory_replenishment_lead_time_days: str = "25",
) -> Path:
    """Generate business_config.<code>.conf and return its path.

    Accepts every field that can appear in a business config file so the
    operator can customise everything from the UI.  Sensible defaults are
    provided for optional fields (matching :class:`BusinessConfig`).
    """
    config_path = _APP_DIR / f"business_config.{code}.conf"

    # ── Normalise optional fields ───────────────────────────
    _s = lambda v, fb="": (v or "").strip() or fb

    # issuer
    iss_name = _s(issuer_name, "Mi Negocio")
    iss_tax = _s(issuer_tax_id)
    iss_addr = _s(issuer_address)
    iss_city = _s(issuer_city)
    iss_pc = _s(issuer_postal_code)
    iss_country = _s(issuer_country)
    iss_email = _s(issuer_email)
    iss_phone = _s(issuer_phone)

    # currency
    cur_code = _s(currency_code, "USD")
    cur_sym = _s(currency_symbol, "$")

    # purchase
    pur_src = _s(purchase_source_currency, "EUR")
    pur_vat = _s(purchase_vat_rate, "1.21")
    pur_fx = _s(purchase_fx_rate)  # empty → no conversion

    # sales documents
    sd_type = _s(sales_default_type, "F").upper()
    if sd_type not in ("F", "P"):
        sd_type = "F"
    sd_enabled = _s(sales_enabled_types, "F,P").upper()
    sd_inv_label = _s(sales_invoice_label, "Factura")
    sd_quote_label = _s(sales_quote_label, "Presupuesto")

    # dividends
    div_biz_label = _s(dividends_business_label, "Negocio")
    div_partners_raw = _s(dividends_partners)
    div_partners = ", ".join(
        p.strip() for p in div_partners_raw.split(",") if p.strip()
    )
    div_opening = _s(dividends_opening_pending)
    div_opening_as_of = _s(dividends_opening_pending_as_of)

    # locations — prefix codes with the business code to keep them unique
    loc_central = _prefix_location_spec(
        _s(locations_central) or "CENTRAL:Almacén Central", code
    )
    loc_pos = _prefix_location_list(_s(locations_pos), code)
    loc_def = _prefix_location_code(_s(locations_default_pos), code)

    # inventory
    inv_lead = _s(inventory_replenishment_lead_time_days, "25")

    # ── Write sections ──────────────────────────────────────
    parser = configparser.ConfigParser()
    parser["issuer"] = {
        "name": iss_name,
        "tax_id": iss_tax,
        "address": iss_addr,
        "city": iss_city,
        "postal_code": iss_pc,
        "country": iss_country,
        "email": iss_email,
        "phone": iss_phone,
    }
    parser["currency"] = {
        "code": cur_code,
        "symbol": cur_sym,
    }
    parser["purchase"] = {
        "invoice_source_currency": pur_src,
        "invoice_vat_rate": pur_vat,
        "invoice_fx_rate": pur_fx,
    }
    parser["sales_documents"] = {
        "default_type": sd_type,
        "enabled_types": sd_enabled,
        "invoice_label": sd_inv_label,
        "quote_label": sd_quote_label,
    }
    parser["dividends"] = {
        "business_label": div_biz_label,
        "partners": div_partners,
        "opening_pending": div_opening,
        "opening_pending_as_of": div_opening_as_of,
    }
    parser["locations"] = {
        "central": loc_central,
        "pos": loc_pos,
        "default_pos": loc_def,
    }
    parser["inventory"] = {
        "replenishment_lead_time_days": inv_lead,
    }

    with open(config_path, "w", encoding="utf-8") as f:
        parser.write(f)

    _log.info("Config file written: %s", config_path)
    return config_path


@router.post("/businesses/create", response_class=HTMLResponse)
def business_create(
    request: Request,
    # ── business identity ───────────────────────────────────────
    code: str = Form(...),
    name: str = Form(...),
    # ── issuer ──────────────────────────────────────────────────
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
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)

    code = code.strip().lower()
    name = name.strip()

    if not code:
        raise HTTPException(status_code=422, detail="Código es requerido")
    if not name:
        raise HTTPException(status_code=422, detail="Nombre es requerido")

    # ── Normalise optional Form params ──────────────────────────
    def _s(v, fb: str = "") -> str:
        return _str_form(v, fb)

    existing = db.scalar(select(Business).where(Business.code == code))
    if existing is not None:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )

    new_business = Business(code=code, name=name)
    db.add(new_business)
    db.flush()

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
    db.add(Location(
        business_id=new_business.id,
        code=central_code,
        name=central_name,
        role="central",
        position=0,
    ))

    # POS list (vacío = negocio sin POS); descartar specs sin código (p.ej. ':Tienda')
    pos_specs = [s for s in _parse_loc_list(_s(locations_pos)) if (s[0] or "").strip()]
    default_pos_code = _prefix_location_code(_s(locations_default_pos), code)

    for i, (pcode, pname) in enumerate(pos_specs, start=1):
        ploc_code = _prefix_location_code(pcode, code)
        db.add(Location(
            business_id=new_business.id,
            code=ploc_code,
            name=pname,
            role="pos",
            position=i,
            is_default=(ploc_code == default_pos_code),
        ))

    # ── Config en BD (fuente única de verdad; el .conf es legacy) ──
    new_business.issuer_name = name
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
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
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

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
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
    ensure_admin(db, request)
    business = db.get(Business, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    config = get_business_config(db, business_id)

    return templates.TemplateResponse(
        request=request,
        name="partials/business_edit_form.html",
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
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    business = db.get(Business, business_id)
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

    existing = db.scalar(
        select(Business).where(Business.code == code, Business.id != business_id)
    )
    if existing is not None:
        response = templates.TemplateResponse(
            request=request,
            name="partials/business_edit_form.html",
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
    reconcile_error = _reconcile_locations(db, business, _s(locations_central), _s(locations_pos), _s(locations_default_pos), code)
    if reconcile_error:
        db.rollback()
        response = templates.TemplateResponse(
            request=request,
            name="partials/business_edit_form.html",
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

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
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
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    business = db.get(Business, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    # Prevent deleting the last business
    remaining = db.scalar(
        select(func.count()).select_from(Business).where(Business.id != business_id)
    ) or 0
    if remaining == 0:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
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
    user_count = db.scalar(
        select(func.count()).select_from(User).where(User.business_id == business_id)
    ) or 0
    product_count = db.scalar(
        select(func.count()).select_from(Product).where(Product.business_id == business_id)
    ) or 0
    # Ubicaciones "adicionales" = todo menos la CENTRAL (por rol o por código,
    # en mayúsculas o minúsculas — bug case-sensitivity corregido).
    other_location_count = db.scalar(
        select(func.count())
        .select_from(Location)
        .where(
            Location.business_id == business_id,
            or_(Location.role.is_(None), Location.role != "central"),
            Location.code.not_in([f"{code}_CENTRAL", f"{code.upper()}_CENTRAL"]),
        )
    ) or 0
    user_business_count = db.scalar(
        select(func.count())
        .select_from(UserBusiness)
        .where(UserBusiness.business_id == business_id)
    ) or 0

    if user_count > 0 or product_count > 0 or other_location_count > 0 or user_business_count > 0:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
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
            name="partials/tab_businesses.html",
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
    db.query(Location).filter(Location.business_id == business_id).delete(synchronize_session=False)

    try:
        db.delete(business)
        db.commit()
    except IntegrityError:
        db.rollback()
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
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

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio eliminado",
            "message_detail": f"Negocio '{name}' eliminado correctamente",
            "message_class": "ok",
        },
    )


@router.get("/tabs/businesses", response_class=HTMLResponse)
def tab_businesses(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={"businesses": businesses},
    )
