from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import hash_password
from app.deps import session_dep
from app.models import Business, User
from app.security import get_current_user_from_session
from app.routers.ui_common import ensure_admin, templates

router = APIRouter()


@router.post("/users/create", response_class=HTMLResponse)
def user_create(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    business_id: Optional[int] = Form(None),
    is_active: bool = Form(True),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)

    username = username.strip()
    if not username:
        raise HTTPException(status_code=422, detail="Username es requerido")
    if not password:
        raise HTTPException(status_code=422, detail="Password es requerido")

    existing = db.scalar(select(User).where(User.username == username))
    if existing is not None:
        users = list(db.scalars(select(User).order_by(User.username.asc())))
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "message": "Error al crear usuario",
                "message_detail": f"El usuario '{username}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )

    role_norm = (role or "operator").strip().lower()
    if role_norm not in ("admin", "owner", "operator"):
        role_norm = "operator"

    if role_norm in ("owner", "operator") and not business_id:
        users = list(db.scalars(select(User).order_by(User.username.asc())))
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "message": "Error al crear usuario",
                "message_detail": "Debes asignar un negocio a usuarios Operador/Dueño",
                "message_class": "error",
            },
            status_code=422,
        )

    new_user = User(
        username=username,
        password_hash=hash_password(password),
        role=role_norm,
        business_id=int(business_id) if business_id else None,
        is_active=bool(is_active),
        must_change_password=True,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_create",
            entity_type="user",
            entity_id=str(new_user.id),
            detail={"username": username, "role": role_norm},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "message": "Usuario creado",
            "message_detail": f"Usuario '{username}' creado correctamente",
            "message_class": "ok",
        },
    )


@router.get("/user/{user_id}/edit", response_class=HTMLResponse)
def user_edit_form(
    request: Request,
    user_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/user_edit_form.html",
        context={"user": user, "businesses": businesses},
    )


@router.post("/user/{user_id}/update", response_class=HTMLResponse)
def user_update(
    request: Request,
    user_id: int,
    username: str = Form(...),
    role: str = Form(...),
    business_id: Optional[int] = Form(None),
    is_active: bool = Form(True),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    username = username.strip()
    if not username:
        raise HTTPException(status_code=422, detail="Username es requerido")

    existing = db.scalar(select(User).where(User.username == username, User.id != user_id))
    if existing is not None:
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        response = templates.TemplateResponse(
            request=request,
            name="partials/user_edit_form.html",
            context={
                "user": user,
                "businesses": businesses,
                "message": "Error al actualizar",
                "message_detail": f"El username '{username}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    role_norm = (role or "operator").strip().lower()
    if role_norm not in ("admin", "owner", "operator"):
        role_norm = "operator"

    if role_norm in ("owner", "operator") and not business_id:
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        response = templates.TemplateResponse(
            request=request,
            name="partials/user_edit_form.html",
            context={
                "user": user,
                "businesses": businesses,
                "message": "Error al actualizar",
                "message_detail": "Debes asignar un negocio a usuarios Operador/Dueño",
                "message_class": "error",
            },
            status_code=422,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    user.username = username
    user.role = role_norm
    user.business_id = int(business_id) if business_id else None
    user.is_active = bool(is_active)
    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_update",
            entity_type="user",
            entity_id=str(user.id),
            detail={"username": username, "role": role_norm},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "message": "Usuario actualizado",
            "message_detail": f"Usuario '{username}' actualizado correctamente",
            "message_class": "ok",
        },
    )


@router.post("/user/{user_id}/delete", response_class=HTMLResponse)
def user_delete(
    request: Request,
    user_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    if current_user and current_user.id == user.id:
        users = list(db.scalars(select(User).order_by(User.username.asc())))
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": "No puedes eliminar tu propio usuario",
                "message_class": "error",
            },
            status_code=400,
        )

    username = user.username
    db.delete(user)
    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_delete",
            entity_type="user",
            entity_id=str(user_id),
            detail={"username": username},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "message": "Usuario eliminado",
            "message_detail": f"Usuario '{username}' eliminado correctamente",
            "message_class": "ok",
        },
    )


@router.get("/user/{user_id}/reset-password-form", response_class=HTMLResponse)
def user_reset_password_form(
    request: Request,
    user_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    return templates.TemplateResponse(
        request=request,
        name="partials/user_reset_password_form.html",
        context={"user": user},
    )


@router.post("/user/{user_id}/reset-password", response_class=HTMLResponse)
def user_reset_password(
    request: Request,
    user_id: int,
    new_password: str = Form(...),
    must_change: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    user = db.get(User, user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    if not new_password:
        raise HTTPException(status_code=422, detail="Nueva contraseña es requerida")

    user.password_hash = hash_password(new_password)
    user.must_change_password = bool(must_change)
    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_reset_password",
            entity_type="user",
            entity_id=str(user.id),
            detail={"username": user.username, "must_change": must_change},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "message": "Contraseña reseteada",
            "message_detail": f"Contraseña de '{user.username}' actualizada",
            "message_class": "ok",
        },
    )
