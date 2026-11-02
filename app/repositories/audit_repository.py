"""Acceso a datos de auditoría (extraído de ui_activity)."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import AuditLog


def list_auth_events(
    db: Session,
    *,
    username: str,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int = 2000,
) -> list[AuditLog]:
    stmt = (
        select(AuditLog)
        .where(AuditLog.entity_type == "auth")
        .where(AuditLog.action.in_(["login", "logout"]))
    )
    if username:
        stmt = stmt.where(AuditLog.username == username)
    if start is not None:
        stmt = stmt.where(AuditLog.created_at >= start)
    if end is not None:
        stmt = stmt.where(AuditLog.created_at < end)
    stmt = stmt.order_by(AuditLog.created_at.desc()).limit(int(limit))
    return list(db.execute(stmt).scalars().all())
