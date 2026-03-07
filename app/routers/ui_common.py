from __future__ import annotations

import os
import re
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
    role = (user.role or "").strip().lower() if user is not None else ""
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
    s = s.replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def barcode_to_sku(db: Session, barcode: str, business_id: Optional[int] = None) -> str:
    code = (barcode or "").strip()
    if not code:
        raise HTTPException(status_code=422, detail="barcode is required")

    business_id_int = int(business_id) if business_id is not None else None

    def _product_by_sku(sku: str) -> Optional[Product]:
        stmt = select(Product).where(Product.sku == sku)
        if business_id_int is not None:
            stmt = stmt.where(Product.business_id == business_id_int)
        return db.scalar(stmt)

    def _barcode_sku_candidates(raw_code: str) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []

        def _add(value: str) -> None:
            v = (value or "").strip()
            if not v:
                return
            key = v.upper()
            if key in seen:
                return
            seen.add(key)
            out.append(v)

        # Existing heuristics.
        head = raw_code.split("|", 1)[0].strip()
        if head:
            _add(head)
            _add(head.split("-", 1)[0].strip())

        code_upper = raw_code.upper()

        # Query-string style (e.g. ...?sku=SKU0001 or ...&spp=SKU0001)
        qs_match = re.search(r"(?:^|[?&;])(sku|spp)=([^&;\s]+)", raw_code, flags=re.IGNORECASE)
        if qs_match:
            _add(qs_match.group(2))

        # Key-value style (e.g. SKU:SKU0001, SKU=SKU0001, SPP:SKU0001)
        kv_match = re.search(r"(?:SKU|SPP)\s*[:=]\s*([A-Za-z0-9._\-/]+)", raw_code, flags=re.IGNORECASE)
        if kv_match:
            _add(kv_match.group(1))

        # Prefixed SPP payloads (e.g. SPP|SKU0001, SPP-SKU0001, SPP SKU0001)
        if code_upper.startswith("SPP"):
            tail = raw_code[3:].lstrip(" :|=-_/\\")
            if tail:
                _add(tail)
                _add(tail.split("|", 1)[0].strip())

        return out

    for sku_candidate in _barcode_sku_candidates(code):
        product = _product_by_sku(sku_candidate)
        if product is not None:
            return product.sku

    lot_stmt = select(InventoryLot).where(InventoryLot.lot_code == code)
    if business_id_int is not None:
        lot_stmt = lot_stmt.where(InventoryLot.business_id == business_id_int)
    lot = db.scalar(lot_stmt)
    if lot is None and not code.endswith("00000"):
        lot_stmt = select(InventoryLot).where(InventoryLot.lot_code == f"{code}00000")
        if business_id_int is not None:
            lot_stmt = lot_stmt.where(InventoryLot.business_id == business_id_int)
        lot = db.scalar(lot_stmt)
    if lot is not None:
        product = db.get(Product, lot.product_id)
        if product is not None and (
            business_id_int is None or int(getattr(product, "business_id", 0) or 0) == business_id_int
        ):
            return product.sku

    raise HTTPException(status_code=404, detail="Barcode not recognized")
