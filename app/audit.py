from __future__ import annotations

import json
from typing import Any, Optional

from sqlalchemy.orm import Session

from app.logger import get_logger
from app.models import AuditLog, User

_log = get_logger(__name__)


def log_event(
    db: Session,
    user: Optional[User],
    action: str,
    entity_type: str,
    entity_id: Optional[str] = None,
    detail: Optional[dict[str, Any]] = None,
) -> None:
    row = AuditLog(
        user_id=user.id if user is not None else None,
        username=user.username if user is not None else None,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        detail=json.dumps(detail or {}, ensure_ascii=False) if detail is not None else None,
    )
    db.add(row)
    try:
        db.commit()
    except Exception:
        db.rollback()
        _log.exception("Failed to persist audit event: action=%s entity=%s", action, entity_type)
