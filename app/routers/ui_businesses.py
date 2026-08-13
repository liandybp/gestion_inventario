from __future__ import annotations

import configparser
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import Business, Location, Product, User, UserBusiness
from app.security import get_current_user_from_session
from app.routers.ui_common import ensure_admin, templates

_log = logging.getLogger(__name__)
_APP_DIR = Path(__file__).resolve().parent.parent

router = APIRouter()


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
    """
    loc_code = (loc_code or "").strip()
    prefix = f"{business_code}_"
    return loc_code if loc_code.startswith(prefix) else f"{prefix}{loc_code}"


def _prefix_location_spec(spec: str, business_code: str) -> str:
    """Prefix the code part of a ``CODE:NAME`` spec (``CENTRAL:Almacén`` →
    ``bazar_CENTRAL:Almacén``). Already-prefixed codes are left unchanged."""
    spec = (spec or "").strip()
    if ":" in spec:
        code, name = spec.split(":", 1)
        code = code.strip()
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
    loc_pos = _prefix_location_list(
        _s(locations_pos) or "POS1:Punto de venta 1", code
    )
    loc_def = _prefix_location_code(_s(locations_default_pos) or "POS1", code)

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
    locations_pos: str = Form("POS1:Punto de venta 1"),
    locations_default_pos: str = Form("POS1"),
    # ── inventory ───────────────────────────────────────────────
    inventory_replenishment_lead_time_days: str = Form("25"),
    # ── dependencies ────────────────────────────────────────────
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)

    code = code.strip()
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
    ))

    # POS list
    pos_specs = _parse_loc_list(_s(locations_pos))
    if not pos_specs:
        pos_specs = [("POS1", "Punto de venta 1")]

    for pcode, pname in pos_specs:
        db.add(Location(
            business_id=new_business.id,
            code=_prefix_location_code(pcode, code),
            name=pname,
        ))

    # ── Write business_config.<code>.conf ─────────────────────────
    try:
        _write_business_config(
            code=code,
            issuer_name=name,
            issuer_tax_id=_s(issuer_tax_id),
            issuer_address=_s(issuer_address),
            issuer_city=_s(issuer_city),
            issuer_postal_code=_s(issuer_postal_code),
            issuer_country=_s(issuer_country),
            issuer_email=_s(issuer_email),
            issuer_phone=_s(issuer_phone),
            currency_code=_s(currency_code, "USD"),
            currency_symbol=_s(currency_symbol, "$"),
            purchase_source_currency=_s(purchase_source_currency, "EUR"),
            purchase_vat_rate=_s(purchase_vat_rate, "1.21"),
            purchase_fx_rate=_s(purchase_fx_rate),
            sales_default_type=_s(sales_default_type, "F"),
            sales_enabled_types=_s(sales_enabled_types, "F,P"),
            sales_invoice_label=_s(sales_invoice_label, "Factura"),
            sales_quote_label=_s(sales_quote_label, "Presupuesto"),
            dividends_business_label=_s(dividends_business_label, "Negocio"),
            dividends_partners=_s(dividends_partners),
            dividends_opening_pending=_s(dividends_opening_pending),
            dividends_opening_pending_as_of=_s(dividends_opening_pending_as_of),
            locations_central=_s(locations_central),
            locations_pos=_s(locations_pos),
            locations_default_pos=_s(locations_default_pos),
            inventory_replenishment_lead_time_days=_s(inventory_replenishment_lead_time_days, "25"),
        )
    except OSError as exc:
        db.rollback()
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"No se pudo escribir el archivo de configuración: {exc}",
                "message_class": "error",
            },
            status_code=500,
        )

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

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio creado",
            "message_detail": (
                f"Negocio '{name}' creado con {len(pos_specs)} POS "
                f"y configuración {code}.conf"
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

    # Load existing config if the .conf file exists; otherwise use defaults
    from app.business_config import BusinessConfig, load_business_config

    config_path = _APP_DIR / f"business_config.{business.code}.conf"
    if config_path.exists():
        config = load_business_config(business.code)
    else:
        config = BusinessConfig()

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
    locations_pos: str = Form("POS1:Punto de venta 1"),
    locations_default_pos: str = Form("POS1"),
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

    code = code.strip()
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
        from app.business_config import BusinessConfig, load_business_config

        config_path = _APP_DIR / f"business_config.{business.code}.conf"
        if config_path.exists():
            config = load_business_config(business.code)
        else:
            config = BusinessConfig()
        response = templates.TemplateResponse(
            request=request,
            name="partials/business_edit_form.html",
            context={
                "business": business,
                "config": config,
                "message": "Error al actualizar",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    old_code = business.code
    business.code = code
    business.name = name

    # Rewrite the config file (or create it for legacy businesses)
    try:
        _write_business_config(
            code=code,
            issuer_name=name,
            issuer_tax_id=_s(issuer_tax_id),
            issuer_address=_s(issuer_address),
            issuer_city=_s(issuer_city),
            issuer_postal_code=_s(issuer_postal_code),
            issuer_country=_s(issuer_country),
            issuer_email=_s(issuer_email),
            issuer_phone=_s(issuer_phone),
            currency_code=_s(currency_code, "USD"),
            currency_symbol=_s(currency_symbol, "$"),
            purchase_source_currency=_s(purchase_source_currency, "EUR"),
            purchase_vat_rate=_s(purchase_vat_rate, "1.21"),
            purchase_fx_rate=_s(purchase_fx_rate),
            sales_default_type=_s(sales_default_type, "F"),
            sales_enabled_types=_s(sales_enabled_types, "F,P"),
            sales_invoice_label=_s(sales_invoice_label, "Factura"),
            sales_quote_label=_s(sales_quote_label, "Presupuesto"),
            dividends_business_label=_s(dividends_business_label, "Negocio"),
            dividends_partners=_s(dividends_partners),
            dividends_opening_pending=_s(dividends_opening_pending),
            dividends_opening_pending_as_of=_s(dividends_opening_pending_as_of),
            locations_central=_s(locations_central),
            locations_pos=_s(locations_pos),
            locations_default_pos=_s(locations_default_pos),
            inventory_replenishment_lead_time_days=_s(inventory_replenishment_lead_time_days, "25"),
        )
        # If the business code changed, remove the old config file
        if old_code != code:
            old_config_path = _APP_DIR / f"business_config.{old_code}.conf"
            if old_config_path.exists():
                old_config_path.unlink()
                _log.info("Old config file removed: %s", old_config_path)
    except OSError as exc:
        db.rollback()
        from app.business_config import BusinessConfig, load_business_config

        config_path = _APP_DIR / f"business_config.{business.code}.conf"
        if config_path.exists():
            config = load_business_config(business.code)
        else:
            config = BusinessConfig()
        response = templates.TemplateResponse(
            request=request,
            name="partials/business_edit_form.html",
            context={
                "business": business,
                "config": config,
                "message": "Error al actualizar",
                "message_detail": f"No se pudo escribir la configuración: {exc}",
                "message_class": "error",
            },
            status_code=500,
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
            detail={"code": code, "name": name, "old_code": old_code},
        )

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
    other_location_count = db.scalar(
        select(func.count())
        .select_from(Location)
        .where(
            Location.business_id == business_id,
            Location.code != f"{code}_CENTRAL",
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

    # Delete auto-created location first, then the business
    auto_location = db.scalar(
        select(Location).where(
            Location.business_id == business_id,
            Location.code == f"{code}_CENTRAL",
        )
    )
    if auto_location is not None:
        db.delete(auto_location)

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

    # Remove associated config file if it exists
    config_path = _APP_DIR / f"business_config.{code}.conf"
    if config_path.exists():
        try:
            config_path.unlink()
            _log.info("Config file removed: %s", config_path)
        except OSError:
            pass  # Non-critical — leftover file won't break anything

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_delete",
            entity_type="business",
            entity_id=str(business_id),
            detail={"code": code, "name": name},
        )

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
