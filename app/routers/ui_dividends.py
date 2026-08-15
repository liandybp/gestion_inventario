from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.repositories.business_repository import get_active_business_config
from app.security import require_active_business_id
from app.services.finance_service import FinanceService
from app.utils import month_range, query_match

from .ui_common import (
    dt_to_local_input,
    ensure_admin_or_owner,
    parse_dt,
    templates,
)

router = APIRouter()


@router.get("/tabs/dividends", response_class=HTMLResponse)
def tab_dividends(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    service = FinanceService(db, business_id=bid)
    now = datetime.now(timezone.utc)
    month_start, month_end = month_range(now)
    config = get_active_business_config(db, request)

    if not start_date and not end_date:
        start_dt, end_dt = month_start, month_end
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)
        if start_dt is None:
            start_dt = month_start
        if end_dt is None:
            end_dt = month_end

    summary = service.monthly_dividends_report(now=now, start=start_dt, end=end_dt)

    extractions = service.list_extractions(start=start_dt, end=end_dt, limit=500)
    q = (query or "").strip()
    if q:
        extractions = [
            r
            for r in extractions
            if query_match(q, str(r.concept or ""), str(r.party or ""))
        ]
    extractions = extractions[:200]
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_dividends.html",
        context={
            "summary": summary,
            "extractions": extractions,
            "movement_date_default": dt_to_local_input(now),
            "dividends": config.dividends.model_dump(),
            "filter_query": query,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )
