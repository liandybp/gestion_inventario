from __future__ import annotations

import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import DeviceToken
from app.settings import DEVICE_RATE_LIMIT, DEVICE_RATE_WINDOW, DEVICE_TOKEN_MAX_INACTIVITY_DAYS

# Rate limiter en memoria por IP (H-007), mismo patrón que app/routers/ui_auth.py.
_device_attempts: dict[str, list[float]] = defaultdict(list)
_device_lock = threading.Lock()


def _real_ip(request: Request) -> str:
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_device_rate(key: str) -> bool:
    """True si se permite el intento; registra el intento actual."""
    now = time.time()
    with _device_lock:
        attempts = [t for t in _device_attempts.get(key, []) if now - t < DEVICE_RATE_WINDOW]
        _device_attempts[key] = attempts
        if len(attempts) >= DEVICE_RATE_LIMIT:
            return False
        _device_attempts[key].append(now)
        return True


def require_device(
    request: Request, db: Session = Depends(session_dep)
) -> DeviceToken:
    """Autentica un nodo por header X-Sync-Token.

    401 si el header falta, el token no existe, fue revocado, o lleva más de
    DEVICE_TOKEN_MAX_INACTIVITY_DAYS sin sincronizar (H-007). Rate limit por IP
    antes de tocar la BD, para no dar oráculo de fuerza bruta contra tokens.
    Actualiza last_seen_at del dispositivo tras una autenticación exitosa.
    """
    if not _check_device_rate(_real_ip(request)):
        raise HTTPException(status_code=429, detail="Demasiados intentos, reintente más tarde")

    token = request.headers.get("X-Sync-Token", "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="X-Sync-Token requerido")
    device = db.scalar(select(DeviceToken).where(DeviceToken.token == token))
    if device is None or device.revoked:
        raise HTTPException(status_code=401, detail="Token de dispositivo inválido o revocado")

    last_activity = device.last_seen_at or device.created_at
    if last_activity is not None:
        max_age = timedelta(days=DEVICE_TOKEN_MAX_INACTIVITY_DAYS)
        now = datetime.now(timezone.utc)
        cutoff = last_activity if last_activity.tzinfo else last_activity.replace(tzinfo=timezone.utc)
        if now - cutoff > max_age:
            raise HTTPException(
                status_code=401,
                detail="Token de dispositivo expirado por inactividad, re-aprovisionar",
            )

    device.last_seen_at = datetime.now(timezone.utc)
    db.commit()
    return device
