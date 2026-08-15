from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.security import (
    get_active_business_code,
    get_current_user_from_session,
    require_active_business_id,
)
from app.services.finance_service import FinanceService
from app.services.reporting_service import ReportingService
from app.utils import month_range

from .ui_common import (
    ensure_admin_or_owner,
    parse_dt,
    templates,
)
from .ui_tabs_common import _home_locations_context, _location_id_for_code

router = APIRouter()


@router.get("/tabs/profit", response_class=HTMLResponse)
def tab_profit(
    request: Request,
    db: Session = Depends(session_dep),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    location_code: str = "",
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    reporting = ReportingService(db, business_id=bid)
    finance = FinanceService(db, business_id=bid)
    business_code = get_active_business_code(db, request)

    locations, default_loc_code = _home_locations_context(db, request)
    selected_location_code = (location_code or "").strip() or (default_loc_code or "")
    location_id = (
        _location_id_for_code(db, selected_location_code, business_id=bid, business_code=business_code)
        if selected_location_code
        else None
    )

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    summary, items = reporting.monthly_profit_report(start=start_dt, end=end_dt, location_id=location_id)
    expenses = finance.list_expenses(
        start=summary["month_start"],
        end=summary["month_end"],
        limit=50,
    )
    return templates.TemplateResponse(
        request=request,
        name="partials/tab_profit.html",
        context={
            "user": user,
            "summary": summary,
            "items": items,
            "expenses": expenses,
            "locations": locations,
            "location_code": selected_location_code,
            "filter_start_date": (start_date or "")[:10],
            "filter_end_date": (end_date or "")[:10],
        },
    )
