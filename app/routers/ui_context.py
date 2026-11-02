from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy.orm import Session

from app.models import User
from app.repositories.business_repository import get_business_code, get_business_config
from app.security import get_active_business_id, get_current_user_from_session

_UNSET = object()


def role_of(user: Optional[User]) -> str:
    """Rol normalizado ("" sin usuario). Única normalización para guardias y sidebar."""
    return (user.role or "").strip().lower() if user is not None else ""


@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _active_bid: object = field(default=_UNSET, repr=False)
    _business_code: object = field(default=_UNSET, repr=False)
    _config: object = field(default=_UNSET, repr=False)

    @property
    def user(self) -> Optional[User]:
        if self._user is _UNSET:
            self._user = get_current_user_from_session(self.db, self.request)
        return self._user  # type: ignore[return-value]

    @property
    def role(self) -> str:
        return role_of(self.user)

    # Valores de la petición: no releer tras modificar el negocio (ui_businesses.py no
    # lee config/código tras escribir; ui_auth.py no usa UIContext).
    @property
    def active_business_id(self) -> Optional[int]:
        if self._active_bid is _UNSET:
            self._active_bid = get_active_business_id(self.db, self.request, user=self.user)
        return self._active_bid  # type: ignore[return-value]

    @property
    def business_id(self) -> int:
        if self.active_business_id is None:
            raise HTTPException(status_code=409, detail="Active business_id is required")
        return int(self.active_business_id)

    @property
    def business_code(self) -> Optional[str]:
        if self._business_code is _UNSET:
            self._business_code = get_business_code(self.db, self.active_business_id)
        return self._business_code  # type: ignore[return-value]

    @property
    def config(self) -> object:
        if self._config is _UNSET:
            self._config = get_business_config(self.db, self.active_business_id)
        return self._config

    def ensure_admin(self) -> None:
        if self.role != "admin":
            raise HTTPException(status_code=403, detail="Admin required")

    def ensure_admin_or_owner(self) -> None:
        if self.role not in ("admin", "owner"):
            raise HTTPException(status_code=403, detail="Admin required")


def ui_context(request: Request, db: Session) -> UIContext:
    """Contexto de la petición. Se llama directamente en el cuerpo del handler, no como Depends."""
    return UIContext(request=request, db=db)
