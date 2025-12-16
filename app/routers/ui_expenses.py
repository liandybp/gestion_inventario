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


@router.post("/expenses/create", response_class=HTMLResponse)
def expense_create(
    request: Request,
    expense_date: Optional[str] = Form(None),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    start, end = month_range(datetime.now(timezone.utc))
    try:
        service.create_expense(amount=amount, concept=concept, expense_date=parse_dt(expense_date))

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="expense_create",
                entity_type="expense",
                entity_id=None,
                detail={"amount": amount, "concept": concept, "expense_date": expense_date},
            )
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Costo operativo registrado",
                "message_class": "ok",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except Exception as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al registrar gasto",
                "message_detail": str(e),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.get("/expense/{expense_id}/edit", response_class=HTMLResponse)
def expense_edit_form(
    request: Request,
    expense_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    expense = service.get_expense(expense_id)
    return templates.TemplateResponse(
        request=request,
        name="partials/expense_edit_form.html",
        context={
            "expense": expense,
            "expense_date_value": dt_to_local_input(expense.expense_date),
        },
    )


@router.post("/expense/{expense_id}/delete", response_class=HTMLResponse)
def expense_delete(
    request: Request,
    expense_id: int,
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    start, end = month_range(datetime.now(timezone.utc))
    try:
        ensure_admin(db, request)
        service.delete_expense(expense_id)

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="expense_delete",
                entity_type="expense",
                entity_id=str(expense_id),
                detail={},
            )
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Costo operativo eliminado",
                "message_class": "ok",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al eliminar costo operativo",
                "message_detail": str(e.detail),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al eliminar costo operativo",
                "message_detail": str(e),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )


@router.post("/expense/{expense_id}/update", response_class=HTMLResponse)
def expense_update(
    request: Request,
    expense_id: int,
    expense_date: Optional[str] = Form(None),
    amount: float = Form(...),
    concept: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    service = InventoryService(db)
    start, end = month_range(datetime.now(timezone.utc))
    try:
        service.update_expense(
            expense_id=expense_id,
            amount=amount,
            concept=concept,
            expense_date=parse_dt(expense_date),
        )

        user = get_current_user_from_session(db, request)
        if user is not None:
            log_event(
                db,
                user,
                action="expense_update",
                entity_type="expense",
                entity_id=str(expense_id),
                detail={"amount": amount, "concept": concept, "expense_date": expense_date},
            )
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Costo operativo actualizado",
                "message_class": "ok",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
        )
    except HTTPException as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al actualizar gasto",
                "message_detail": str(e.detail),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=e.status_code,
        )
    except Exception as e:
        expenses = service.list_expenses(start=start, end=end, limit=200)
        total = service.total_expenses(start=start, end=end)
        return templates.TemplateResponse(
            request=request,
            name="partials/tab_expenses.html",
            context={
                "message": "Error al actualizar gasto",
                "message_detail": str(e),
                "message_class": "error",
                "expenses": expenses,
                "expenses_total": total,
                "movement_date_default": dt_to_local_input(datetime.now(timezone.utc)),
            },
            status_code=400,
        )
