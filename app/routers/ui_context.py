from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import User
from app.security import get_current_user_from_session, require_active_business_id

_UNSET = object()


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
    def business_id(self) -> int:
        if self._business_id is None:
            self._business_id = require_active_business_id(self.db, self.request)
        return self._business_id

    def ensure_admin(self) -> None:
        user = self.user
        if user is None or (user.role or "").lower() != "admin":
            raise HTTPException(status_code=403, detail="Admin required")

    def ensure_admin_or_owner(self) -> None:
        user = self.user
        role = (user.role or "").strip().lower() if user is not None else ""
        if user is None or role not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Admin required")


def ui_context(request: Request, db: Session = Depends(session_dep)) -> UIContext:
    return UIContext(request=request, db=db)
