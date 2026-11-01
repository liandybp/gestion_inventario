from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.models import User
from app.security import get_current_user_from_session, require_active_business_id

_UNSET = object()


def role_of(user: Optional[User]) -> str:
    """Rol normalizado ("" sin usuario). Única normalización para guardias y sidebar."""
    return (user.role or "").strip().lower() if user is not None else ""


@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _business_id: Optional[int] = field(default=None, repr=False)

    @property
    def user(self) -> Optional[User]:
        if self._user is _UNSET:
            self._user = get_current_user_from_session(self.db, self.request)
        return self._user  # type: ignore[return-value]

    @property
    def role(self) -> str:
        return role_of(self.user)

    @property
    def business_id(self) -> int:
        if self._business_id is None:
            self._business_id = require_active_business_id(self.db, self.request)
        return self._business_id

    def ensure_admin(self) -> None:
        if self.role != "admin":
            raise HTTPException(status_code=403, detail="Admin required")

    def ensure_admin_or_owner(self) -> None:
        if self.role not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Admin required")


def ui_context(request: Request, db: Session) -> UIContext:
    """Contexto de la petición. Se llama directamente en el cuerpo del handler, no como Depends."""
    return UIContext(request=request, db=db)
