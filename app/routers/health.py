from __future__ import annotations

from fastapi import APIRouter
from sqlalchemy import select, text

from app.db import SessionLocal

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict:
    db_status = "ok"
    try:
        db = SessionLocal()
        try:
            db.execute(text("SELECT 1"))
        finally:
            db.close()
    except Exception:
        db_status = "degraded"

    overall = "ok" if db_status == "ok" else "degraded"
    status_code = 200 if overall == "ok" else 503

    return {
        "status": overall,
        "checks": {
            "database": db_status,
        },
    }
