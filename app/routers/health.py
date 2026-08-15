from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from sqlalchemy import text

from app.db import SessionLocal

router = APIRouter(tags=["health"])

_APP_DIR = Path(__file__).resolve().parents[2]


def _app_version() -> str:
    try:
        v = (_APP_DIR / "VERSION").read_text(encoding="utf-8").strip()
        return v or "unknown"
    except OSError:
        return "unknown"


def _schema_version() -> str:
    try:
        db = SessionLocal()
        try:
            row = db.execute(text("SELECT version FROM schema_version WHERE id = 1")).fetchone()
            return str(row[0]) if row else "missing"
        finally:
            db.close()
    except Exception:
        return "error"


def _db_ok() -> str:
    try:
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
        return "ok"
    except Exception:
        return "degraded"


def _agent_llm_ok() -> str:
    """Reachability opcional del backend LLM (timeout 2s). No baja el status."""
    import app.settings as settings

    base = settings.AGENT_LLM_BASE_URL
    if not base:
        return "disabled"
    try:
        import urllib.request

        urllib.request.urlopen(f"{base}/models", timeout=2).read()
        return "ok"
    except Exception:
        return "degraded"


@router.get("/health")
def health() -> dict:
    db_status = _db_ok()
    schema = _schema_version()
    llm_status = _agent_llm_ok()
    critical_ok = db_status == "ok" and schema not in ("error", "missing")
    overall = "ok" if critical_ok else "degraded"
    payload = {
        "status": overall,
        "version": _app_version(),
        "checks": {
            "database": db_status,
            "schema_version": schema,
            "agent_llm": llm_status,
        },
    }
    # H-065: el healthcheck del CD debe fallar (503) ante degradación crítica
    from fastapi.responses import JSONResponse

    return JSONResponse(content=payload, status_code=200 if overall == "ok" else 503)
