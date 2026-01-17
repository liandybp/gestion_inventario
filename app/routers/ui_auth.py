from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import authenticate
from app.auth import hash_password
from app.deps import session_dep
from app.models import Business, User
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

    log_event(
        db,
        row,
        action="must_change_password",
        entity_type="auth",
        entity_id=row.username,
        detail={"role": row.role},
    )
    resp = HTMLResponse("")
    resp.headers["HX-Refresh"] = "true"
    return resp


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
