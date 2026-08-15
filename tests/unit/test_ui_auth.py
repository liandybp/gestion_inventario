"""Rate limiter del login keyed por (IP real + username) — Fase D (D3).

- `_real_ip` resuelve la IP real detrás del proxy (X-Forwarded-For) con fallback
  a `request.client.host`.
- `_check_login_rate` keyea por la clave compuesta `IP|user`, así un atacante no
  agota el bucket global y el brute-force por usuario se frena.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.routers.ui_auth import _check_login_rate, _login_attempts, _real_ip


def _request(xff: str | None = None, host: str = "10.0.0.7") -> MagicMock:
    req = MagicMock()
    headers = {}
    if xff is not None:
        headers["X-Forwarded-For"] = xff
    req.headers.get = MagicMock(side_effect=lambda k, d=None: headers.get(k, d))
    req.client = MagicMock()
    req.client.host = host
    return req


def test_real_ip_prefers_xff_first_value() -> None:
    req = _request(xff="203.0.113.9, 10.0.0.1, 192.168.1.5")
    assert _real_ip(req) == "203.0.113.9"


def test_real_ip_falls_back_to_client_host() -> None:
    assert _real_ip(_request(xff=None)) == "10.0.0.7"


def test_real_ip_handles_missing_client() -> None:
    req = MagicMock()
    req.headers.get = MagicMock(return_value=None)
    req.client = None
    assert _real_ip(req) == "unknown"


def test_check_login_rate_keyed_by_ip_and_user(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("app.routers.ui_auth.LOGIN_RATE_LIMIT", 2)
    monkeypatch.setattr("app.routers.ui_auth.LOGIN_RATE_WINDOW", 300)
    _login_attempts.clear()
    try:
        # 2 intentos de (ip1, admin) → permite, 3º bloquea
        assert _check_login_rate("1.1.1.1|admin") is True
        assert _check_login_rate("1.1.1.1|admin") is True
        assert _check_login_rate("1.1.1.1|admin") is False
        # Otro usuario desde la misma IP NO está bloqueado
        assert _check_login_rate("1.1.1.1|operator") is True
        # Otra IP para el mismo usuario NO está bloqueada
        assert _check_login_rate("2.2.2.2|admin") is True
    finally:
        _login_attempts.clear()