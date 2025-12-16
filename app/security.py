from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.auth import get_user_by_username
from app.deps import session_dep
from app.models import User


def get_current_user_from_session(db: Session, request: Request) -> Optional[User]:
    session = getattr(request, "session", None) or {}
    username = session.get("username")
    if not username:
        return None
    user = get_user_by_username(db, str(username))
    if user is None or not user.is_active:
        return None
    return user


def require_user_api(
    request: Request,
    db: Session = Depends(session_dep),
) -> User:
    user = get_current_user_from_session(db, request)
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_admin_api(
    request: Request,
    db: Session = Depends(session_dep),
) -> User:
    user = require_user_api(request=request, db=db)
    if (user.role or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")
    return user
