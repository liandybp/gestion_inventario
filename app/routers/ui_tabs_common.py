from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.routers.ui_context import UIContext
from app.repositories.inventory_repository import InventoryRepository
from app.repositories.location_repository import LocationRepository




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


def _home_locations_context(ctx: "UIContext") -> tuple[list[dict], str]:
    config = ctx.config
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
def _location_name_for_code(location_code: str, ctx: "UIContext") -> str:
    code = (location_code or "").strip()
    locations, _default_loc_code = _home_locations_context(ctx)
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

    history_skus = InventoryRepository(
        db, business_id=int(business_id)
    ).skus_with_location_history(int(location_id), skus)
    if not history_skus:
        return []

    return [i for i in items if str(getattr(i, "sku", "") or "").strip() in history_skus]
