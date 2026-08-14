from __future__ import annotations

from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import ChatMessage
from app.settings import AGENT_MAX_MESSAGES

# Límite de mensajes por negocio para que el historial no crezca sin fin.
MAX_MESSAGES = AGENT_MAX_MESSAGES


def add_message(db: Session, business_id: int, role: str, content: str,
                user_id: Optional[int] = None) -> ChatMessage:
    message = ChatMessage(
        business_id=int(business_id),
        user_id=user_id,
        role=role,
        content=content,
    )
    db.add(message)
    db.commit()
    db.refresh(message)
    _trim_messages(db, business_id)
    return message


def list_messages(db: Session, business_id: int, limit: int | None = MAX_MESSAGES) -> list[ChatMessage]:
    stmt = (
        select(ChatMessage)
        .where(ChatMessage.business_id == int(business_id))
        .order_by(ChatMessage.created_at.asc(), ChatMessage.id.asc())
    )
    if limit is not None:
        stmt = stmt.limit(limit)
    return list(db.scalars(stmt).all())


def clear_messages(db: Session, business_id: int) -> int:
    """Borra toda la conversación del negocio (nueva conversación)."""
    deleted = db.query(ChatMessage).filter(ChatMessage.business_id == int(business_id)).delete()
    db.commit()
    return int(deleted)


def _trim_messages(db: Session, business_id: int) -> None:
    """Recorta a MAX_MESSAGES sin cargar todo el historial (F1)."""
    count = db.scalar(select(func.count(ChatMessage.id)).where(ChatMessage.business_id == int(business_id))) or 0
    excess = int(count) - MAX_MESSAGES
    if excess <= 0:
        return
    ids = list(db.scalars(
        select(ChatMessage.id)
        .where(ChatMessage.business_id == int(business_id))
        .order_by(ChatMessage.id.asc())
        .limit(excess)
    ).all())
    db.query(ChatMessage).filter(ChatMessage.id.in_(ids)).delete(synchronize_session=False)
    db.commit()
