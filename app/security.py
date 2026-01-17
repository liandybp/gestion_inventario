from __future__ import annotations

from typing import Optional

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth import get_user_by_username
from app.deps import session_dep
from app.models import Business, User


def get_current_user_from_session(db: Session, request: Request) -> Optional[User]:
    session = getattr(request, "session", None) or {}
    username = session.get("username")
    if not username:
        return None
    user = get_user_by_username(db, str(username))
    if user is None or not user.is_active:
        return None
    return user


def get_active_business_id(db: Session, request: Request) -> Optional[int]:
    user = get_current_user_from_session(db, request)
    if user is None:
        return None

    if (user.role or "").lower() != "admin":
        return int(user.business_id) if user.business_id is not None else None

    session = getattr(request, "session", None) or {}
    raw = session.get("active_business_id")
    if raw is not None:
        try:
            bid = int(raw)
            if db.get(Business, bid) is not None:
                return bid
        except Exception:
            pass

    default_id = db.scalar(select(Business.id).where(Business.code == "recambios"))
    if default_id is None:
        b = Business(code="recambios", name="Recambios")
        db.add(b)
        db.commit()
        db.refresh(b)
        default_id = int(b.id)

    try:
        session["active_business_id"] = int(default_id)
    except Exception:
        pass
    return int(default_id)


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
