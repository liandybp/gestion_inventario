from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import Business, Location, Product, User
from app.security import get_current_user_from_session
from app.routers.ui_common import ensure_admin, templates

router = APIRouter()


@router.post("/businesses/create", response_class=HTMLResponse)
def business_create(
    request: Request,
    code: str = Form(...),
    name: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)

    code = code.strip()
    name = name.strip()

    if not code:
        raise HTTPException(status_code=422, detail="Código es requerido")
    if not name:
        raise HTTPException(status_code=422, detail="Nombre es requerido")

    existing = db.scalar(select(Business).where(Business.code == code))
    if existing is not None:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )

    new_business = Business(code=code, name=name)
    db.add(new_business)
    db.flush()

    location_code = f"{code}_CENTRAL"
    location = Location(
        business_id=new_business.id,
        code=location_code,
        name="Almacén Central",
    )
    db.add(location)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al crear negocio",
                "message_detail": f"El código '{code}' ya existe (creado por otra sesión)",
                "message_class": "error",
            },
            status_code=409,
        )

    db.refresh(new_business)

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_create",
            entity_type="business",
            entity_id=str(new_business.id),
            detail={"code": code, "name": name},
        )

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio creado",
            "message_detail": f"Negocio '{name}' creado correctamente con ubicación CENTRAL",
            "message_class": "ok",
        },
    )


@router.get("/business/{business_id}/edit", response_class=HTMLResponse)
def business_edit_form(
    request: Request,
    business_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    business = db.get(Business, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")
    return templates.TemplateResponse(
        request=request,
        name="partials/business_edit_form.html",
        context={"business": business},
    )


@router.post("/business/{business_id}/update", response_class=HTMLResponse)
def business_update(
    request: Request,
    business_id: int,
    code: str = Form(...),
    name: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    business = db.get(Business, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    code = code.strip()
    name = name.strip()

    if not code:
        raise HTTPException(status_code=422, detail="Código es requerido")
    if not name:
        raise HTTPException(status_code=422, detail="Nombre es requerido")

    existing = db.scalar(
        select(Business).where(Business.code == code, Business.id != business_id)
    )
    if existing is not None:
        response = templates.TemplateResponse(
            request=request,
            name="partials/business_edit_form.html",
            context={
                "business": business,
                "message": "Error al actualizar",
                "message_detail": f"El código '{code}' ya existe",
                "message_class": "error",
            },
            status_code=409,
        )
        response.headers["X-Modal-Keep"] = "1"
        return response

    old_code = business.code
    business.code = code
    business.name = name
    db.commit()

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_update",
            entity_type="business",
            entity_id=str(business.id),
            detail={"code": code, "name": name, "old_code": old_code},
        )

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio actualizado",
            "message_detail": f"Negocio '{name}' actualizado correctamente",
            "message_class": "ok",
        },
    )


@router.post("/business/{business_id}/delete", response_class=HTMLResponse)
def business_delete(
    request: Request,
    business_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    current_user = get_current_user_from_session(db, request)
    business = db.get(Business, business_id)
    if business is None:
        raise HTTPException(status_code=404, detail="Negocio no encontrado")

    # Prevent deleting the last business
    remaining = db.scalar(
        select(func.count()).select_from(Business).where(Business.id != business_id)
    ) or 0
    if remaining == 0:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": "No puedes eliminar el último negocio",
                "message_class": "error",
            },
            status_code=400,
        )

    code = business.code
    name = business.name

    # Explicit dependency checks (works on both SQLite and Postgres)
    user_count = db.scalar(
        select(func.count()).select_from(User).where(User.business_id == business_id)
    ) or 0
    product_count = db.scalar(
        select(func.count()).select_from(Product).where(Product.business_id == business_id)
    ) or 0
    other_location_count = db.scalar(
        select(func.count())
        .select_from(Location)
        .where(
            Location.business_id == business_id,
            Location.code != f"{code}_CENTRAL",
        )
    ) or 0

    if user_count > 0 or product_count > 0 or other_location_count > 0:
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        parts = []
        if user_count > 0:
            parts.append(f"{user_count} usuario(s)")
        if product_count > 0:
            parts.append(f"{product_count} producto(s)")
        if other_location_count > 0:
            parts.append("ubicaciones adicionales")
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": (
                    f"No se puede eliminar '{name}': "
                    f"tiene {', '.join(parts)} asociado(s). "
                    "Elimina esos datos primero"
                ),
                "message_class": "error",
            },
            status_code=409,
        )

    # Delete auto-created location first, then the business
    auto_location = db.scalar(
        select(Location).where(
            Location.business_id == business_id,
            Location.code == f"{code}_CENTRAL",
        )
    )
    if auto_location is not None:
        db.delete(auto_location)

    try:
        db.delete(business)
        db.commit()
    except IntegrityError:
        db.rollback()
        businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_businesses.html",
            context={
                "businesses": businesses,
                "message": "Error al eliminar",
                "message_detail": (
                    f"No se puede eliminar '{name}': "
                    "tiene datos asociados que no se pudieron verificar"
                ),
                "message_class": "error",
            },
            status_code=409,
        )

    if current_user is not None:
        log_event(
            db,
            current_user,
            action="business_delete",
            entity_type="business",
            entity_id=str(business_id),
            detail={"code": code, "name": name},
        )

    businesses = list(db.scalars(select(Business).order_by(Business.code.asc())))
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_businesses.html",
        context={
            "businesses": businesses,
            "message": "Negocio eliminado",
            "message_detail": f"Negocio '{name}' eliminado correctamente",
            "message_class": "ok",
        },
    )
