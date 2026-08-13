from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.audit import log_event
from app.auth import hash_password
from app.deps import session_dep
from app.models import Business, User, UserBusiness
from app.security import get_current_user_from_session, get_user_business_ids
from app.routers.ui_common import ensure_admin, templates

router = APIRouter()


def _coerce_business_ids(raw) -> list[int]:
    """Coerce a FastAPI Form list to a list of ints.

    ``raw`` is a list of ints when submitted via HTTP, but a FormInfo
    default when the function is called directly (e.g. tests).
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        out: list[int] = []
        for x in raw:
            try:
                out.append(int(x))
            except (ValueError, TypeError):
                continue
        return out
    default = getattr(raw, "default", None)
    if isinstance(default, list):
        return [int(x) for x in default if x]
    if default is not None:
        try:
            return [int(default)]
        except (ValueError, TypeError):
            return []
    return []


def _sync_user_businesses(db: Session, user: User, business_ids: list[int]) -> None:
    """Replace the user's business assignments with the given list.

    ``User.business_id`` is kept as the primary business (first of the
    list) for backward compatibility with operators and legacy code.
    Operators are pinned to a single business, so only the first is kept.
    """
    # Operators can only have one business — keep just the first
    if (user.role or "").lower() == "operator" and len(business_ids) > 1:
        business_ids = business_ids[:1]

    # Remove existing assignments
    db.query(UserBusiness).filter(UserBusiness.user_id == int(user.id)).delete(
        synchronize_session=False
    )
    for bid in business_ids:
        db.add(UserBusiness(user_id=int(user.id), business_id=int(bid)))
    user.business_id = int(business_ids[0]) if business_ids else None


def _users_tab_context(db: Session) -> dict:
    """Build the shared context for the users tab, including a map of
    user_id → assigned business_ids for rendering the multi-business column."""
    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    user_business_map: dict[int, list[int]] = {}
    for u in users:
        user_business_map[int(u.id)] = get_user_business_ids(db, int(u.id))
    return {
        "users": users,
        "businesses": businesses,
        "user_business_map": user_business_map,
    }


@router.post("/users/create", response_class=HTMLResponse)
def user_create(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    role: str = Form(...),
    business_ids: list[int] = Form([]),
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
        user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "user_business_map": user_business_map,
                "message": "Error al crear usuario",
                "message_detail": f"El usuario '{username}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )

    role_norm = (role or "operator").strip().lower()
    if role_norm not in ("admin", "owner", "operator"):
        role_norm = "operator"

    bid_list = _coerce_business_ids(business_ids)

    if role_norm in ("owner", "operator") and not bid_list:
        users = list(db.scalars(select(User).order_by(User.username.asc())))
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "user_business_map": user_business_map,
                "message": "Error al crear usuario",
                "message_detail": "Debes asignar al menos un negocio a usuarios Operador/Dueño",
                "message_class": "error",
            },
            status_code=422,
        )

    new_user = User(
        username=username,
        password_hash=hash_password(password),
        role=role_norm,
        business_id=None,
        is_active=bool(is_active),
        must_change_password=True,
    )
    db.add(new_user)
    db.flush()
    _sync_user_businesses(db, new_user, bid_list)
    db.commit()
    db.refresh(new_user)

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_create",
            entity_type="user",
            entity_id=str(new_user.id),
            detail={"username": username, "role": role_norm, "business_ids": bid_list},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "user_business_map": user_business_map,
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
    user_business_ids = get_user_business_ids(db, user_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/user_edit_form.html",
        context={
            "user": user,
            "businesses": businesses,
            "user_business_ids": user_business_ids,
        },
    )


@router.post("/user/{user_id}/update", response_class=HTMLResponse)
def user_update(
    request: Request,
    user_id: int,
    username: str = Form(...),
    role: str = Form(...),
    business_ids: list[int] = Form([]),
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
        user_business_ids = get_user_business_ids(db, user_id)
        response = templates.TemplateResponse(
            request=request,
            name="partials/user_edit_form.html",
            context={
                "user": user,
                "businesses": businesses,
                "user_business_ids": user_business_ids,
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

    bid_list = _coerce_business_ids(business_ids)

    if role_norm in ("owner", "operator") and not bid_list:
        businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
        user_business_ids = get_user_business_ids(db, user_id)
        response = templates.TemplateResponse(
            request=request,
            name="partials/user_edit_form.html",
            context={
                "user": user,
                "businesses": businesses,
                "user_business_ids": user_business_ids,
                "message": "Error al actualizar",
                "message_detail": "Debes asignar al menos un negocio a usuarios Operador/Dueño",
                "message_class": "error",
            },
            status_code=422,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    user.username = username
    user.role = role_norm
    user.is_active = bool(is_active)
    db.flush()
    _sync_user_businesses(db, user, bid_list)
    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="user_update",
            entity_type="user",
            entity_id=str(user.id),
            detail={"username": username, "role": role_norm, "business_ids": bid_list},
        )

    users = list(db.scalars(select(User).order_by(User.username.asc())))
    businesses = list(db.scalars(select(Business).order_by(Business.name.asc())))
    user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "user_business_map": user_business_map,
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
        user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_users.html",
            context={
                "users": users,
                "businesses": businesses,
                "user_business_map": user_business_map,
                "message": "Error al eliminar",
                "message_detail": "No puedes eliminar tu propio usuario",
                "message_class": "error",
            },
            status_code=400,
        )

    username = user.username
    db.query(UserBusiness).filter(UserBusiness.user_id == int(user.id)).delete(
        synchronize_session=False
    )
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
    user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "user_business_map": user_business_map,
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
    user_business_map = {int(u.id): get_user_business_ids(db, int(u.id)) for u in users}
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_users.html",
        context={
            "users": users,
            "businesses": businesses,
            "user_business_map": user_business_map,
            "message": "Contraseña reseteada",
            "message_detail": f"Contraseña de '{user.username}' actualizada",
            "message_class": "ok",
        },
    )
