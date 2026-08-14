from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import DashboardWidget
from app.settings import AGENT_MAX_WIDGETS as MAX_WIDGETS


def add_widget(db: Session, business_id: int, *, type: str, title: str, tool: str,
               tool_args: dict, payload: dict, unit: Optional[str], created_by: Optional[int]) -> DashboardWidget:
    # MAX(position)+1 en vez de len(list_widgets) (F3): evita materializar filas.
    position = (db.scalar(
        select(func.coalesce(func.max(DashboardWidget.position), 0))
        .where(DashboardWidget.business_id == int(business_id))
    ) or 0) + 1
    widget = DashboardWidget(
        business_id=int(business_id),
        type=type,
        title=title,
        params=json.dumps({"tool": tool, "tool_args": tool_args, "payload": payload, "unit": unit}, ensure_ascii=False),
        position=position,
        created_by=created_by,
    )
    db.add(widget)
    db.commit()
    db.refresh(widget)
    _trim_widgets(db, business_id)
    return widget


def list_widgets(db: Session, business_id: int) -> list[DashboardWidget]:
    return list(db.scalars(
        select(DashboardWidget)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
    ).all())


def widgets_with_payload(db: Session, business_id: int) -> list[dict]:
    out = []
    for w in list_widgets(db, business_id):
        try:
            params = json.loads(w.params or "{}")
        except (TypeError, ValueError):
            # A corrupt params blob must not take down the whole Copiloto
            # surface (C5); treat it as an empty widget.
            params = {}
        out.append({"id": w.id, "type": w.type, "title": w.title, "payload": params.get("payload", {})})
    return out


def delete_widget(db: Session, business_id: int, widget_id: int) -> bool:
    widget = db.scalar(select(DashboardWidget).where(
        DashboardWidget.id == int(widget_id),
        DashboardWidget.business_id == int(business_id),
    ))
    if widget is None:
        return False
    db.delete(widget)
    db.commit()
    return True


def _trim_widgets(db: Session, business_id: int) -> None:
    """Drop the oldest widgets beyond MAX_WIDGETS (B6) sin cargar todo el grid."""
    count = db.scalar(select(func.count(DashboardWidget.id)).where(DashboardWidget.business_id == int(business_id))) or 0
    excess = int(count) - MAX_WIDGETS
    if excess <= 0:
        return
    ids = list(db.scalars(
        select(DashboardWidget.id)
        .where(DashboardWidget.business_id == int(business_id))
        .order_by(DashboardWidget.position.asc(), DashboardWidget.id.asc())
        .limit(excess)
    ).all())
    db.query(DashboardWidget).filter(DashboardWidget.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
