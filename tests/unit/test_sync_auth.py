"""Rate limiter y expiración por inactividad del token de dispositivo (H-007)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from unittest.mock import MagicMock

from app.models import DeviceToken
from app.sync.auth import _check_device_rate, _device_attempts, require_device


def test_check_device_rate_blocks_after_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.sync.auth.DEVICE_RATE_LIMIT", 2)
    monkeypatch.setattr("app.sync.auth.DEVICE_RATE_WINDOW", 300)
    _device_attempts.clear()
    try:
        assert _check_device_rate("1.1.1.1") is True
        assert _check_device_rate("1.1.1.1") is True
        assert _check_device_rate("1.1.1.1") is False
        # Otra IP no está bloqueada
        assert _check_device_rate("2.2.2.2") is True
    finally:
        _device_attempts.clear()


def _fake_request() -> MagicMock:
    req = MagicMock()
    req.headers.get = MagicMock(return_value="")
    req.client = MagicMock()
    req.client.host = "9.9.9.9"
    return req


def test_require_device_rejects_expired_by_inactivity(db_session, business_id, monkeypatch):
    monkeypatch.setattr("app.sync.auth.DEVICE_TOKEN_MAX_INACTIVITY_DAYS", 1)
    stale = datetime.now(timezone.utc) - timedelta(days=5)
    device = DeviceToken(token="stale-tok", business_id=business_id, label="POS",
                          created_at=stale, last_seen_at=stale)
    db_session.add(device)
    db_session.commit()

    req = _fake_request()
    req.headers.get = MagicMock(side_effect=lambda k, d="": "stale-tok" if k == "X-Sync-Token" else d)

    with pytest.raises(HTTPException) as ei:
        require_device(req, db_session)
    assert ei.value.status_code == 401
    assert "expirado" in ei.value.detail
