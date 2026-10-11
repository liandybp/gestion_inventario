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
    *,
    isolated: bool = False,
) -> None:
    """Registra un evento de auditoría en la MISMA transacción del caller.

    Usa `flush()` (no `commit()`): el AuditLog se persiste con el commit que
    haga el caller. En ventas/traspasos se llama ANTES de la operación que
    commitea (atómico). En el resto es best-effort: el caller debe commitear
    tras llamarlo (o aceptar la pérdida si no lo hace).

    `isolated=True`: el flush va dentro de un SAVEPOINT. Si falla, solo se descarta
    el evento; la transacción del caller (p. ej. un producto ya flusheado) sigue
    intacta. Sin él, el fallo hace rollback de TODO lo pendiente. Requiere que la
    transacción ya esté iniciada (con SQLite/pysqlite el SAVEPOINT necesita un DML previo).
    """
    row = AuditLog(
        user_id=user.id if user is not None else None,
        username=user.username if user is not None else None,
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        detail=json.dumps(detail or {}, ensure_ascii=False) if detail is not None else None,
    )
    if isolated:
        try:
            with db.begin_nested():
                db.add(row)
        except Exception:
            _log.exception("Failed to persist audit event (isolated): action=%s entity=%s", action, entity_type)
        return

    db.add(row)
    try:
        db.flush()
    except Exception:
        db.rollback()
        _log.exception("Failed to persist audit event: action=%s entity=%s", action, entity_type)
