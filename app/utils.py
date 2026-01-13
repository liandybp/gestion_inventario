from __future__ import annotations

import os
import secrets
import unicodedata
from datetime import datetime, timezone
from typing import Tuple


def get_session_secret() -> str:
    """Obtiene el secret key para sesiones. Genera uno seguro si no está definido."""
    secret = os.getenv("SESSION_SECRET", "").strip()
    if not secret:
        secret = secrets.token_hex(32)
    return secret


def month_range(now: datetime) -> Tuple[datetime, datetime]:
    """Calcula el rango del mes actual (inicio y fin)."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def normalize_text(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    nfd = unicodedata.normalize("NFD", v)
    return "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")


def query_match(query: str, *values: str) -> bool:
    q = normalize_text(query).lower().strip()
    if not q:
        return True
    for v in values:
        if q in normalize_text(v).lower():
            return True
    return False
