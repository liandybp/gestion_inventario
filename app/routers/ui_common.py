from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple
from uuid import uuid4

from fastapi import HTTPException, Request, UploadFile
from fastapi.templating import Jinja2Templates
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import InventoryLot, Product
from app.security import get_current_user_from_session
from app.utils import month_range

templates = Jinja2Templates(directory="app/templates")

_DEV_ACTIONS_ENABLED = os.getenv("DEV_ACTIONS_ENABLED", "1") == "1"

_UPLOAD_DIR = Path("app/static/uploads")


def ensure_admin(db: Session, request: Request) -> None:
    user = get_current_user_from_session(db, request)
    if user is None or (user.role or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")


def ensure_admin_or_owner(db: Session, request: Request) -> None:
    user = get_current_user_from_session(db, request)
    role = (user.role or "").lower() if user is not None else ""
    if user is None or role not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin required")


def save_product_image(image_file: UploadFile) -> str:
    if image_file is None:
        raise HTTPException(status_code=422, detail="image_file is required")
    if not image_file.filename:
        raise HTTPException(status_code=422, detail="image_file is required")

    content_type = (image_file.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=422, detail="Invalid image type")

    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    ext = Path(image_file.filename).suffix.lower()
    if not ext or len(ext) > 10:
        ext = ".img"

    filename = f"{uuid4().hex}{ext}"
    out_path = _UPLOAD_DIR / filename
    with out_path.open("wb") as f:
        shutil.copyfileobj(image_file.file, f)

    return f"/static/uploads/{filename}"


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def dt_to_local_input(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M")




def extract_sku(product_field: str) -> str:
    value = (product_field or "").strip()
    if " - " in value:
        value = value.split(" - ", 1)[0].strip()
    return value


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def barcode_to_sku(db: Session, barcode: str) -> str:
    code = (barcode or "").strip()
    if not code:
        raise HTTPException(status_code=422, detail="barcode is required")

    direct = code.split("|", 1)[0].strip()
    if direct:
        product = db.scalar(select(Product).where(Product.sku == direct))
        if product is not None:
            return product.sku

    token = code.split("|", 1)[0].strip()
    sku_candidate = token.split("-", 1)[0].strip()
    if sku_candidate:
        product = db.scalar(select(Product).where(Product.sku == sku_candidate))
        if product is not None:
            return product.sku

    lot = db.scalar(select(InventoryLot).where(InventoryLot.lot_code == code))
    if lot is None and not code.endswith("00000"):
        lot = db.scalar(select(InventoryLot).where(InventoryLot.lot_code == f"{code}00000"))
    if lot is not None:
        product = db.get(Product, lot.product_id)
        if product is not None:
            return product.sku

    raise HTTPException(status_code=404, detail="Barcode not recognized")
