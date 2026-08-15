from __future__ import annotations

import threading
import time
from collections import defaultdict

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import authenticate
from app.auth import hash_password
from app.deps import session_dep
from app.models import Business, User
from app.security import get_current_user_from_session, get_user_business_ids
from app.settings import LOGIN_RATE_LIMIT, LOGIN_RATE_WINDOW

from .ui_common import templates

router = APIRouter()

# Simple in-memory rate limiter for login
_login_attempts: dict[str, list[float]] = defaultdict(list)
_login_lock = threading.Lock()


def _real_ip(request) -> str:
    """IP real detrás del proxy: primer valor de X-Forwarded-For, si no client.host."""
    xff = (request.headers.get("X-Forwarded-For") or "").strip()
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_login_rate(key: str) -> bool:
    """Returns True if the client is allowed to attempt login."""
    now = time.time()
    with _login_lock:
        attempts = [t for t in _login_attempts.get(key, []) if now - t < LOGIN_RATE_WINDOW]
        _login_attempts[key] = attempts
        if len(attempts) >= LOGIN_RATE_LIMIT:
            return False
        _login_attempts[key].append(now)
        return True


@router.get("/login", response_class=HTMLResponse)
def login_form(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request=request, name="login.html", context={})


@router.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(session_dep),
):
    ip = _real_ip(request)
    username_key = (username or "").strip().lower()
    key = f"{ip}|{username_key}"
    if not _check_login_rate(key):
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Demasiados intentos. Espera unos minutos."},
            status_code=429,
        )

    user = authenticate(db, username=username, password=password)
    if user is None:
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Usuario o contraseña inválidos"},
            status_code=401,
        )

    request.session["username"] = user.username
    request.session["last_activity"] = int(time.time())
    # Set session business_id for roles that can switch (admin/owner).
    # Operators are pinned to user.business_id and never get it in session.
    role = (user.role or "").lower()
    if role == "admin" and user.business_id is not None:
        request.session["active_business_id"] = int(user.business_id)
    elif role == "owner":
        assigned = get_user_business_ids(db, user.id)
        primary = assigned[0] if assigned else (int(user.business_id) if user.business_id else None)
        if primary is not None:
            request.session["active_business_id"] = primary
    else:
        try:
            request.session.pop("active_business_id", None)
        except Exception:
            pass
    log_event(
        db,
        user,
        action="login",
        entity_type="auth",
        entity_id=user.username,
        detail={"role": user.role},
    )
    db.commit()
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/must-change-password-form", response_class=HTMLResponse)
def must_change_password_form(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    if user is None:
        return RedirectResponse(url="/ui/login", status_code=302)
    if not bool(getattr(user, "must_change_password", False)):
        return HTMLResponse("")
    return templates.TemplateResponse(
        request=request,
        name="partials/must_change_password_form.html",
        context={"user": user},
    )


@router.post("/must-change-password", response_class=HTMLResponse)
def must_change_password_submit(
    request: Request,
    new_password: str = Form(...),
    confirm_password: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    if user is None:
        return RedirectResponse(url="/ui/login", status_code=302)
    if not bool(getattr(user, "must_change_password", False)):
        resp = HTMLResponse("")
        resp.headers["HX-Refresh"] = "true"
        return resp
    if not new_password:
        response = templates.TemplateResponse(
            request=request,
            name="partials/must_change_password_form.html",
            context={
                "user": user,
                "message": "Error",
                "message_detail": "La nueva contraseña es requerida",
                "message_class": "error",
            },
            status_code=422,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response
    if (confirm_password or "") != (new_password or ""):
        response = templates.TemplateResponse(
            request=request,
            name="partials/must_change_password_form.html",
            context={
                "user": user,
                "message": "Error",
                "message_detail": "Las contraseñas no coinciden",
                "message_class": "error",
            },
            status_code=422,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    row = db.get(User, int(user.id))
    if row is None:
        return RedirectResponse(url="/ui/login", status_code=302)
    row.password_hash = hash_password(new_password)
    row.must_change_password = False
    db.commit()
    db.refresh(row)
    request.session["username"] = row.username
    role = (row.role or "").lower()
    if role == "admin" and row.business_id is not None:
        request.session["active_business_id"] = int(row.business_id)
    elif role == "owner":
        assigned = get_user_business_ids(db, row.id)
        primary = assigned[0] if assigned else (int(row.business_id) if row.business_id else None)
        if primary is not None:
            request.session["active_business_id"] = primary
    else:
        try:
            request.session.pop("active_business_id", None)
        except Exception:
            pass

    log_event(
        db,
        row,
        action="must_change_password",
        entity_type="auth",
        entity_id=row.username,
        detail={"role": row.role},
    )
    db.commit()
    resp = HTMLResponse("")
    resp.headers["HX-Refresh"] = "true"
    return resp


@router.post("/active-business")
def set_active_business(
    request: Request,
    business_id: int = Form(...),
    db: Session = Depends(session_dep),
) -> RedirectResponse:
    user = get_current_user_from_session(db, request)
    if user is None:
        return RedirectResponse(url="/ui/login", status_code=302)

    role = (user.role or "").lower()
    bid = int(business_id)

    if db.get(Business, bid) is None:
        return RedirectResponse(url="/ui/dashboard", status_code=302)

    if role == "owner":
        # Owners can only switch to businesses they are assigned to
        assigned = get_user_business_ids(db, user.id)
        if bid not in assigned:
            return RedirectResponse(url="/ui/dashboard", status_code=302)
    elif role != "admin":
        # Operators cannot switch businesses
        return RedirectResponse(url="/ui/dashboard", status_code=302)

    request.session["active_business_id"] = bid
    try:
        request.session.pop("sales_doc_cart", None)
        request.session.pop("sales_doc_draft", None)
    except Exception:
        pass
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/logout")
def logout(request: Request, db: Session = Depends(session_dep)) -> RedirectResponse:
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(
            db,
            user,
            action="logout",
            entity_type="auth",
            entity_id=user.username,
            detail={"role": user.role},
        )
        db.commit()
    try:
        request.session.clear()
    except Exception:
        request.session.pop("username", None)
    return RedirectResponse(url="/ui/login", status_code=302)
