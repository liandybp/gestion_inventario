from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import CashBaseline
from app.routers.ui_common import ensure_admin, templates
from app.schemas import CashBaselineCreate, CoachActionUpdate
from app.security import get_current_user_from_session, require_active_business_id
from app.services.coach_service import CoachService

# Sin prefix: main.py registra este router con prefix="/ui" (igual que el resto).
router = APIRouter(tags=["ui-coach"])


@router.get("/tabs/coach", response_class=HTMLResponse)
def tab_coach(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)
    business_id = require_active_business_id(db, request)
    service = CoachService(db, business_id)
    report = service.health_score()
    recommendations = service.recommendations()
    return templates.TemplateResponse(
        request, "partials/tab_coach.html",
        {"report": report, "recommendations": recommendations},
    )


@router.post("/coach/cash-baseline", response_class=HTMLResponse)
def set_cash_baseline(
    request: Request,
    baseline_date: str = Form(...),
    amount: float = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    business_id = require_active_business_id(db, request)
    try:
        payload = CashBaselineCreate(
            baseline_date=datetime.fromisoformat(baseline_date), amount=amount
        )
    except ValidationError as e:
        detail = "; ".join(err["msg"] for err in e.errors())
        raise HTTPException(status_code=422, detail=detail) from e
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    user = get_current_user_from_session(db, request)
    row = CashBaseline(
        business_id=business_id,
        baseline_date=payload.baseline_date,
        amount=payload.amount,
        created_by=user.id if user else None,
    )
    db.add(row)
    db.commit()
    service = CoachService(db, business_id)
    report = service.health_score()
    recommendations = service.recommendations()
    return templates.TemplateResponse(
        request, "partials/tab_coach.html",
        {"report": report, "recommendations": recommendations},
    )


@router.post("/coach/action", response_class=HTMLResponse)
def update_action(
    request: Request,
    rule_key: str = Form(...),
    status: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin(db, request)
    business_id = require_active_business_id(db, request)
    try:
        payload = CoachActionUpdate(rule_key=rule_key, status=status)
    except ValidationError as e:
        detail = "; ".join(err["msg"] for err in e.errors())
        raise HTTPException(status_code=422, detail=detail) from e
    user = get_current_user_from_session(db, request)
    CoachService(db, business_id).mark_action(payload.rule_key, payload.status, user.id if user else 0)
    service = CoachService(db, business_id)
    report = service.health_score()
    recommendations = service.recommendations()
    return templates.TemplateResponse(
        request, "partials/tab_coach.html",
        {"report": report, "recommendations": recommendations},
    )