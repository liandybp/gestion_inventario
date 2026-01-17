from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import authenticate
from app.deps import session_dep
from app.models import Business
from app.security import get_current_user_from_session

from .ui_common import ensure_admin, templates

router = APIRouter()


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
    user = authenticate(db, username=username, password=password)
    if user is None:
        return templates.TemplateResponse(
            request=request,
            name="login.html",
            context={"error": "Usuario o contraseña inválidos"},
            status_code=401,
        )

    request.session["username"] = user.username
    if (user.role or "").lower() != "admin":
        if user.business_id is not None:
            request.session["active_business_id"] = int(user.business_id)
    else:
        default_id = db.scalar(select(Business.id).where(Business.code == "recambios"))
        if default_id is not None:
            request.session["active_business_id"] = int(default_id)
    log_event(
        db,
        user,
        action="login",
        entity_type="auth",
        entity_id=user.username,
        detail={"role": user.role},
    )
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.post("/active-business")
def set_active_business(
    request: Request,
    business_id: int = Form(...),
    db: Session = Depends(session_dep),
) -> RedirectResponse:
    ensure_admin(db, request)
    bid = int(business_id)
    if db.get(Business, bid) is None:
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
    try:
        request.session.clear()
    except Exception:
        request.session.pop("username", None)
    return RedirectResponse(url="/ui/login", status_code=302)
