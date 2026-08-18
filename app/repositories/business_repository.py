"""Acceso a la configuración de negocio desde la BD (fuente única de verdad).

Reemplaza a `load_business_config` (que leía los .conf legacy): reconstruye el
mismo `BusinessConfig` pydantic desde las columnas de `businesses` + `locations`.
Los consumidores existentes (`config.issuer.*`, `config.locations.*`, etc.) no
cambian: solo cambia la procedencia de los datos.
"""

from __future__ import annotations

from typing import Optional

from fastapi import Request
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.business_config import (
    BusinessConfig,
    CurrencyConfig,
    DividendsConfig,
    InventoryConfig,
    IssuerConfig,
    LocationSpec,
    LocationsConfig,
    PurchaseConfig,
    SalesDocumentsConfig,
    parse_enabled_types,
    parse_opening_pending,
    parse_partners,
)
from app.models import Business, Location, Product, User, UserBusiness
from app.security import get_active_business_id


def get_business_locations(db: Session, business_id: int) -> LocationsConfig:
    """Ubicaciones (central/pos/default) del negocio desde la tabla `locations`.

    Rows legacy sin `role` se infieren por convención de código (CENTRAL/endswith
    _CENTRAL). Fallbacks replican el comportamiento del parser .conf.
    """
    rows = list(db.scalars(
        select(Location)
        .where(Location.business_id == int(business_id))
        .order_by(Location.position.asc().nulls_last(), Location.id.asc())
    ).all())

    central: Optional[LocationSpec] = None
    pos: list[LocationSpec] = []
    default_pos: Optional[str] = None

    for r in rows:
        if r.role == "central":
            central = LocationSpec(code=r.code, name=r.name)
        elif r.role == "pos":
            pos.append(LocationSpec(code=r.code, name=r.name))
            if r.is_default:
                default_pos = r.code
        elif central is None:
            # legacy (role NULL): inferir por convención de código, solo si no
            # hay ya una central explícita (role='central') resuelta antes.
            upper = (r.code or "").upper()
            if upper == "CENTRAL" or upper.endswith("_CENTRAL"):
                central = LocationSpec(code=r.code, name=r.name)
            else:
                pos.append(LocationSpec(code=r.code, name=r.name))
        else:
            pos.append(LocationSpec(code=r.code, name=r.name))

    if central is None:
        central = LocationSpec(code="CENTRAL", name="Almacén Central")
    pos_codes = {p.code for p in pos}
    if pos and (default_pos is None or default_pos not in pos_codes):
        default_pos = pos[0].code
    return LocationsConfig(central=central, pos=pos, default_pos=default_pos)


def get_business_config(db: Session, business_id: Optional[int]) -> BusinessConfig:
    """Reconstruye el `BusinessConfig` desde las columnas de BD (null-safe)."""
    if business_id is None:
        return BusinessConfig()
    business = db.get(Business, int(business_id))
    if business is None:
        return BusinessConfig()

    default_type = (business.sales_default_type or "F").strip().upper()
    if default_type not in ("F", "P"):
        default_type = "F"

    return BusinessConfig(
        issuer=IssuerConfig(
            name=business.issuer_name or business.name,
            tax_id=business.issuer_tax_id or "",
            address=business.issuer_address or "",
            city=business.issuer_city or "",
            postal_code=business.issuer_postal_code or "",
            country=business.issuer_country or "",
            email=business.issuer_email or "",
            phone=business.issuer_phone or "",
        ),
        currency=CurrencyConfig(
            code=(business.currency_code or "EUR").strip() or "EUR",
            symbol=(business.currency_symbol or "€").strip() or "€",
        ),
        purchase=PurchaseConfig(
            invoice_source_currency=(business.purchase_invoice_source_currency or "EUR").strip() or "EUR",
            invoice_vat_rate=float(business.purchase_invoice_vat_rate or 1.21),
            invoice_fx_rate=(
                float(business.purchase_invoice_fx_rate)
                if business.purchase_invoice_fx_rate is not None
                else None
            ),
        ),
        sales_documents=SalesDocumentsConfig(
            default_type=default_type,
            enabled_types=parse_enabled_types(business.sales_enabled_types or "F,P"),
            invoice_label=(business.sales_invoice_label or "Factura").strip() or "Factura",
            quote_label=(business.sales_quote_label or "Presupuesto").strip() or "Presupuesto",
        ),
        dividends=DividendsConfig(
            business_label=(business.dividends_business_label or "Negocio").strip() or "Negocio",
            partners=parse_partners(business.dividends_partners or ""),
            opening_pending=parse_opening_pending(business.dividends_opening_pending or ""),
            opening_pending_as_of=(business.dividends_opening_pending_as_of or "").strip(),
        ),
        locations=get_business_locations(db, int(business_id)),
        inventory=InventoryConfig(
            replenishment_lead_time_days=float(business.inventory_replenishment_lead_time_days or 25.0),
        ),
    )


def get_active_business_config(db: Session, request: Request) -> BusinessConfig:
    """Config del negocio activo de la sesión (reemplazo directo de
    `load_business_config(get_active_business_code(db, request))`)."""
    bid = get_active_business_id(db, request)
    return get_business_config(db, bid)


# ── CRUD de negocios (extraído de ui_businesses, H-028) ───────────────────────

def list_businesses(db: Session) -> list[Business]:
    return list(db.scalars(select(Business).order_by(Business.code.asc())))


def get_business(db: Session, business_id: int) -> Optional[Business]:
    return db.get(Business, int(business_id))


def get_business_by_code(db: Session, code: str) -> Optional[Business]:
    return db.scalar(select(Business).where(Business.code == code))


def get_business_by_code_excluding(db: Session, code: str, business_id: int) -> Optional[Business]:
    return db.scalar(
        select(Business).where(Business.code == code, Business.id != int(business_id))
    )


def create_business(db: Session, code: str, name: str) -> Business:
    business = Business(code=code, name=name)
    db.add(business)
    db.flush()
    return business


def delete_business(db: Session, business_id: int) -> None:
    business = db.get(Business, int(business_id))
    if business is not None:
        db.delete(business)


def count_other_businesses(db: Session, business_id: int) -> int:
    return int(
        db.scalar(
            select(func.count()).select_from(Business).where(Business.id != int(business_id))
        )
        or 0
    )


def count_related(db: Session, business_id: int, code: Optional[str] = None) -> dict:
    bid = int(business_id)
    user_count = int(
        db.scalar(select(func.count()).select_from(User).where(User.business_id == bid)) or 0
    )
    product_count = int(
        db.scalar(select(func.count()).select_from(Product).where(Product.business_id == bid)) or 0
    )
    bcode = (code or "").strip()
    other_location_count = int(
        db.scalar(
            select(func.count())
            .select_from(Location)
            .where(
                Location.business_id == bid,
                or_(Location.role.is_(None), Location.role != "central"),
                Location.code.not_in([f"{bcode}_CENTRAL", f"{bcode.upper()}_CENTRAL"]),
            )
        )
        or 0
    )
    user_business_count = int(
        db.scalar(
            select(func.count())
            .select_from(UserBusiness)
            .where(UserBusiness.business_id == bid)
        )
        or 0
    )
    return {
        "users": user_count,
        "products": product_count,
        "locations": other_location_count,
        "user_businesses": user_business_count,
    }
