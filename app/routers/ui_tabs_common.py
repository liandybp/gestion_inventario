from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, Location, Product
from app.repositories.business_repository import get_active_business_config
from app.repositories.location_repository import LocationRepository


def _deletable_skus(db: Session, skus: list[str], *, business_id: int) -> set[str]:
    clean = [str(s).strip() for s in (skus or []) if str(s).strip()]
    if not clean:
        return set()

    rows = db.execute(
        select(Product.id, Product.sku).where(
            Product.sku.in_(clean),
            Product.business_id == int(business_id),
        )
    ).all()
    if not rows:
        return set()

    product_ids = [int(pid) for pid, _ in rows]
    id_to_sku = {int(pid): str(sku) for pid, sku in rows}

    movement_pids = set(
        db.scalars(
            select(InventoryMovement.product_id)
            .where(InventoryMovement.product_id.in_(product_ids))
            .where(InventoryMovement.business_id == int(business_id))
            .distinct()
        ).all()
    )
    lot_pids = set(
        db.scalars(
            select(InventoryLot.product_id)
            .where(InventoryLot.product_id.in_(product_ids))
            .where(InventoryLot.business_id == int(business_id))
            .distinct()
        ).all()
    )
    blocked = movement_pids | lot_pids

    return {sku for pid, sku in id_to_sku.items() if pid not in blocked}


def _month_label_es(dt: datetime) -> str:
    month_names = [
        "enero",
        "febrero",
        "marzo",
        "abril",
        "mayo",
        "junio",
        "julio",
        "agosto",
        "septiembre",
        "octubre",
        "noviembre",
        "diciembre",
    ]
    return f"{month_names[int(dt.month) - 1].title()} {dt.year}"


def _home_locations_context(db: Session, request: Request) -> tuple[list[dict], str]:
    config = get_active_business_config(db, request)
    locations: list[dict] = [{"code": "", "name": "Vista General"}]
    if getattr(getattr(config, "locations", None), "central", None) is not None:
        central_code = str(getattr(config.locations.central, "code", "") or "").strip()
        if central_code:
            locations.append({"code": central_code, "name": "Almacén central"})
    for loc in (config.locations.pos or []):
        if getattr(loc, "code", None):
            locations.append({"code": loc.code, "name": loc.name})
    default_code = ""
    return locations, default_code


def _location_id_for_code(
    db: Session,
    location_code: str,
    business_id: int,
    business_code: Optional[str] = None,
) -> Optional[int]:
    code = (location_code or "").strip()
    if not code:
        return None
    try:
        return LocationRepository(db, int(business_id), business_code or "").location_id_for_code(
            code, auto_create=False
        )
    except HTTPException:
        return None
def _location_name_for_code(location_code: str, db: Session, request: Request) -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(db, request)
    for loc in locations:
        if (str(loc.get("code") or "").strip()) == code:
            name = str(loc.get("name") or "").strip()
            return name or (code or "General")
    return code or "General"


def _normalize_restock_scope(restock_scope: str) -> str:
    return "all" if (restock_scope or "").strip().lower() == "all" else "with_history"


def _restock_items_with_location_history(
    db: Session,
    items: list,
    *,
    business_id: int,
    location_id: Optional[int],
) -> list:
    if location_id is None or not items:
        return items

    skus = [str(getattr(i, "sku", "") or "").strip() for i in items]
    skus = [sku for sku in skus if sku]
    if not skus:
        return items

    history_skus = {
        str(sku).strip()
        for sku in db.scalars(
            select(Product.sku)
            .select_from(InventoryMovement)
            .join(Product, Product.id == InventoryMovement.product_id)
            .where(
                InventoryMovement.business_id == int(business_id),
                InventoryMovement.location_id == int(location_id),
                Product.sku.in_(skus),
            )
            .distinct()
        ).all()
        if str(sku or "").strip()
    }
    if not history_skus:
        return []

    return [i for i in items if str(getattr(i, "sku", "") or "").strip() in history_skus]
