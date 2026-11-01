"""Reportes de violaciones de CSP (``report-uri /csp-report`` en ``app.main.security_headers_middleware``).

Sin sesión ni CSRF: el navegador los envía solo, sin cookies garantizadas ni cabeceras de htmx. Por eso el cuerpo
se acota en tamaño, los campos se truncan y hay un tope de reportes registrados por minuto (un atacante no puede
inundar el log). Siempre responde 204 salvo cuerpo demasiado grande (413).
"""

from __future__ import annotations

import json
import threading
import time

from fastapi import APIRouter, Request, Response

from app.logger import get_logger

router = APIRouter(tags=["csp"])
_log = get_logger(__name__)

MAX_BODY_BYTES = 16 * 1024
MAX_REPORTS_PER_MINUTE = 60
_FIELD_MAX = 300

# (clave en report-uri clásico, clave en la Reporting API) -> nombre en el log
_FIELDS = {
    "document": ("document-uri", "documentURL"),
    "directive": ("effective-directive", "effectiveDirective"),
    "violated": ("violated-directive", None),
    "blocked": ("blocked-uri", "blockedURL"),
    "source": ("source-file", "sourceFile"),
    "line": ("line-number", "lineNumber"),
    "disposition": ("disposition", "disposition"),
}

_lock = threading.Lock()
_window_start = 0.0
_window_count = 0


def _allow() -> bool:
    global _window_start, _window_count
    now = time.monotonic()
    with _lock:
        if now - _window_start >= 60:
            _window_start, _window_count = now, 0
        _window_count += 1
        return _window_count <= MAX_REPORTS_PER_MINUTE


def _bodies(payload: object) -> list[tuple[dict, bool]]:
    """Normaliza los dos formatos: ``{"csp-report": {...}}`` y ``[{"type": "csp-violation", "body": {...}}]``."""
    if isinstance(payload, dict) and isinstance(payload.get("csp-report"), dict):
        return [(payload["csp-report"], True)]
    if isinstance(payload, list):
        return [(r["body"], False) for r in payload
                if isinstance(r, dict) and r.get("type") == "csp-violation" and isinstance(r.get("body"), dict)]
    return []


def _summary(body: dict, legacy: bool) -> dict:
    out = {}
    for name, (legacy_key, api_key) in _FIELDS.items():
        key = legacy_key if legacy else api_key
        value = body.get(key) if key else None
        if value not in (None, ""):
            out[name] = str(value)[:_FIELD_MAX]
    return out


@router.post("/csp-report", status_code=204)
async def csp_report(request: Request) -> Response:
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        return Response(status_code=413)
    try:
        payload = json.loads(raw or b"null")
    except (ValueError, UnicodeDecodeError):
        return Response(status_code=204)
    for body, legacy in _bodies(payload)[:10]:
        if not _allow():
            break
        _log.warning("csp_violation %s", json.dumps(_summary(body, legacy), ensure_ascii=False, sort_keys=True))
    return Response(status_code=204)
