from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import AuditLog
from app.logger import get_logger

from .ui_common import (
    ensure_admin,
    month_range,
    parse_dt,
    templates,
)

_log = get_logger(__name__)

router = APIRouter()


@router.get("/tabs/activity", response_class=HTMLResponse)
def tab_activity(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin(db, request)

    username: str = (request.query_params.get("username") or "").strip()
    role_filter: str = (request.query_params.get("role") or "").strip().lower()
    start_date = (request.query_params.get("start_date") or "").strip()
    end_date = (request.query_params.get("end_date") or "").strip()
    if not start_date and not end_date:
        now = datetime.now(timezone.utc)
        start_dt, end_dt = month_range(now)
    else:
        start_dt = parse_dt(start_date) if start_date else None
        end_dt = parse_dt(end_date) if end_date else None
        if end_dt is not None:
            end_dt = end_dt + timedelta(days=1)

    stmt = (
        select(AuditLog)
        .where(AuditLog.entity_type == "auth")
        .where(AuditLog.action.in_(["login", "logout"]))
    )
    if username:
        stmt = stmt.where(AuditLog.username == username)
    if start_dt is not None:
        stmt = stmt.where(AuditLog.created_at >= start_dt)
    if end_dt is not None:
        stmt = stmt.where(AuditLog.created_at < end_dt)

    stmt = stmt.order_by(AuditLog.created_at.desc()).limit(2000)
    rows = db.execute(stmt).scalars().all()

    def fmt_dt(dt: Optional[datetime]) -> str:
        if dt is None:
            return ""
        try:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            _log.error("fmt_dt fallback en tab_activity")
            return str(dt)

    def parse_detail(raw: Optional[str]) -> dict:
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            _log.error("parse_detail fallback en tab_activity")
            return {}

    if role_filter and role_filter != "all":
        filtered_rows: list[AuditLog] = []
        for ev in rows:
            detail = parse_detail(ev.detail)
            role = str(detail.get("role") or "").strip().lower()
            if role == role_filter:
                filtered_rows.append(ev)
        rows = filtered_rows

    events = list(reversed(rows))
    pending_login: dict[str, dict] = {}
    sessions: list[dict] = []

    for ev in events:
        username = (ev.username or "").strip() or "(desconocido)"
        detail = parse_detail(ev.detail)
        role = str(detail.get("role") or "") or "-"
        if ev.action == "login":
            pending_login[username] = {"login_at": ev.created_at, "role": role}
            continue

        if ev.action == "logout":
            login_info = pending_login.pop(username, None)
            login_at = (login_info or {}).get("login_at")
            logout_at = ev.created_at
            duration_seconds = None
            if isinstance(login_at, datetime) and isinstance(logout_at, datetime):
                try:
                    duration_seconds = max(0, int((logout_at - login_at).total_seconds()))
                except Exception:
                    _log.error("duration calc fallback en tab_activity")
                    duration_seconds = None

            sessions.append(
                {
                    "username": username,
                    "role": (login_info or {}).get("role") or role,
                    "login_at": fmt_dt(login_at) if login_at else "",
                    "logout_at": fmt_dt(logout_at),
                    "duration_seconds": duration_seconds,
                }
            )

    sessions.sort(key=lambda r: (r.get("logout_at") or r.get("login_at") or ""), reverse=True)

    event_rows: list[dict] = []
    for ev in rows[:200]:
        detail = parse_detail(ev.detail)
        event_rows.append(
            {
                "at": fmt_dt(ev.created_at),
                "username": (ev.username or "").strip() or "(desconocido)",
                "action": ev.action,
                "role": str(detail.get("role") or "") or "-",
            }
        )

    return templates.TemplateResponse(
        request=request,
        name="partials/tab_activity.html",
        context={
            "sessions": sessions,
            "events": event_rows,
            "username_filter": username,
            "role_filter": role_filter or "",
            "start_date_value": (start_date or "")[:10],
            "end_date_value": (end_date or "")[:10],
        },
    )
