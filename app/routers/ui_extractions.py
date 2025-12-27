from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.security import get_current_user_from_session
from app.services.inventory_service import InventoryService

from .ui_common import dt_to_local_input, ensure_admin, month_range, parse_dt, templates

router = APIRouter()


@router.post("/extractions/create", response_class=HTMLResponse)
def extraction_create(
    request: Request,
    extraction_date: Optional[str] = Form(None),
    party: str = Form(...),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    try:
        ensure_admin(db, request)
        service.create_extraction(
            party=party,
            amount=amount,
            concept=concept,
            extraction_date=parse_dt(extraction_date),
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="extraction_create",
                entity_type="extraction",
                entity_id=None,
                detail={"party": party, "amount": amount, "concept": concept, "extraction_date": extraction_date},
            )
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Retiro registrado",
                "message_class": "ok",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
        )
    except Exception as e:
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Error al registrar retiro",
                "message_detail": str(e),
                "message_class": "error",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
            status_code=400,
        )


@router.post("/extraction/{extraction_id}/delete", response_class=HTMLResponse)
def extraction_delete(
    request: Request,
    extraction_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    try:
        ensure_admin(db, request)
        service.delete_extraction(extraction_id)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="extraction_delete",
                entity_type="extraction",
                entity_id=str(extraction_id),
                detail={},
            )
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Retiro eliminado",
                "message_class": "ok",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
        )
    except HTTPException as e:
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Error al eliminar retiro",
                "message_detail": str(e.detail),
                "message_class": "error",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Error al eliminar retiro",
                "message_detail": str(e),
                "message_class": "error",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
            status_code=400,
        )


@router.get("/extraction/{extraction_id}/edit", response_class=HTMLResponse)
def extraction_edit_form(
    request: Request,
    extraction_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    service = InventoryService(db)
    extraction = service.get_extraction(extraction_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/extraction_edit_form.html",
        context={
            "extraction": extraction,
            "extraction_date_value": dt_to_local_input(extraction.extraction_date),
        },
    )


@router.post("/extraction/{extraction_id}/update", response_class=HTMLResponse)
def extraction_update(
    request: Request,
    extraction_id: int,
    extraction_date: Optional[str] = Form(None),
    party: str = Form(...),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    now = datetime.now(timezone.utc)
    start, end = month_range(now)
    try:
        ensure_admin(db, request)
        service.update_extraction(
            extraction_id=extraction_id,
            party=party,
            amount=amount,
            concept=concept,
            extraction_date=parse_dt(extraction_date),
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="extraction_update",
                entity_type="extraction",
                entity_id=str(extraction_id),
                detail={"party": party, "amount": amount, "concept": concept, "extraction_date": extraction_date},
            )
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Retiro actualizado",
                "message_class": "ok",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
        )
    except HTTPException as e:
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Error al actualizar retiro",
                "message_detail": str(e.detail),
                "message_class": "error",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        summary = service.monthly_dividends_report(now=now)
        extractions = service.list_extractions(start=start, end=end, limit=200)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_dividends.html",
            context={
                "message": "Error al actualizar retiro",
                "message_detail": str(e),
                "message_class": "error",
                "summary": summary,
                "extractions": extractions,
                "movement_date_default": dt_to_local_input(now),
            },
            status_code=400,
        )
