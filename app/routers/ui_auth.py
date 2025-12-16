from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import authenticate
from app.deps import session_dep
from app.security import get_current_user_from_session

from .ui_common import templates

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
    log_event(db, user, action="login", entity_type="auth", entity_id=user.username, detail={})
    return RedirectResponse(url="/ui/dashboard", status_code=302)


@router.get("/logout")
def logout(request: Request, db: Session = Depends(session_dep)) -> RedirectResponse:
    user = get_current_user_from_session(db, request)
    if user is not None:
        log_event(db, user, action="logout", entity_type="auth", entity_id=user.username, detail={})
    try:
        request.session.clear()
    except Exception:
        request.session.pop("username", None)
    return RedirectResponse(url="/ui/login", status_code=302)
