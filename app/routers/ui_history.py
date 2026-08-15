from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.security import (
    get_current_user_from_session,
    require_active_business_id,
)
from app.services.inventory_service import InventoryService
from app.logger import get_logger

from .ui_common import (
    ensure_admin_or_owner,
    month_range,
    parse_dt,
    templates,
)

_log = get_logger(__name__)

router = APIRouter()


@router.get("/tabs/history", response_class=HTMLResponse)
def tab_history(
    request: Request,
    db: Session = Depends(session_dep),
    query: str = "",
    movement_type: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    user = get_current_user_from_session(db, request)
    inventory_service = InventoryService(db, business_id=bid)

    query_filter = (query or "").strip()
    type_filter = movement_type.strip() if movement_type else None
    
    # Default to current month if no dates specified
    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    movements = inventory_service.movement_history(
        query=query_filter,
        movement_type=type_filter or None,
        start_date=start_dt,
        end_date=end_dt,
        limit=200,
    )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_history.html",
        context={
            "movements": movements,
            "user": user,
            "message": None,
            "message_detail": None,
            "message_class": None,
            "filter_query": query_filter,
            "type_filter": type_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )


@router.post("/tabs/history/adjustments/delete-selected", response_class=HTMLResponse)
def history_adjustments_delete_selected(
    request: Request,
    movement_ids: list[int] = Form([]),
    query: str = Form(""),
    movement_type: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    bid = require_active_business_id(db, request)
    inventory_service = InventoryService(db, business_id=bid)
    user = get_current_user_from_session(db, request)

    ids: list[int] = []
    seen: set[int] = set()
    for raw in (movement_ids or []):
        try:
            mid = int(raw)
        except Exception:
            _log.debug("movement_id parse skip en adjustments delete: %r", raw)
            continue
        if mid <= 0 or mid in seen:
            continue
        seen.add(mid)
        ids.append(mid)

    deleted = 0
    errors: list[str] = []
    if not ids:
        errors.append("No se seleccionó ningún ajuste")
    else:
        for mid in ids:
            try:
                inventory_service.delete_adjustment_movement(mid)
                deleted += 1
                if user is not None:
                    log_event(
                        db,
                        user,
                        action="history_adjustment_delete",
                        entity_type="movement",
                        entity_id=str(mid),
                        detail={"bulk": True},
                    )
            except HTTPException as e:
                errors.append(f"{mid}: {e.detail}")
            except Exception as e:
                _log.exception("delete_adjustment fallback %s: %s", mid, e)
                errors.append(f"{mid}: {e}")
        db.commit()

    query_filter = (query or "").strip()
    type_filter = movement_type.strip() if movement_type else None

    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    movements = inventory_service.movement_history(
        query=query_filter,
        movement_type=type_filter or None,
        start_date=start_dt,
        end_date=end_dt,
        limit=200,
    )

    msg = "Ajustes eliminados" if deleted > 0 else "No se pudo eliminar"
    detail = f"Se eliminaron {deleted} ajuste(s)."
    if errors:
        detail = detail + " Errores: " + "; ".join(errors[:6])

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_history.html",
        context={
            "movements": movements,
            "user": user,
            "message": msg,
            "message_detail": detail,
            "message_class": "ok" if deleted > 0 and not errors else ("warn" if deleted > 0 else "error"),
            "filter_query": query_filter,
            "type_filter": type_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )
