from __future__ import annotations
from app.repositories.business_repository import get_active_business_config

import threading
import time
from collections import defaultdict

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.agent import chat_store
from app.agent import runner
from app.agent import widgets as widget_store
from app.deps import session_dep
from app.routers.ui_common import ensure_admin_or_owner, templates
from app.security import get_active_business_code, get_current_user_from_session, require_active_business_id
from app.settings import AGENT_RATE_LIMIT, AGENT_RATE_WINDOW

# Sin prefix: main.py registra este router con prefix="/ui" (igual que los demás
# routers UI). Un prefix aquí duplicaría la ruta (/ui/ui/tabs/agent).
router = APIRouter(tags=["ui-agent"])

# Simple in-memory rate limiter for the (paid) LLM chat endpoint (B4/D2).
_agent_attempts: dict[str, list[float]] = defaultdict(list)
_agent_lock = threading.Lock()


def _currency_symbol(db, request) -> str:
    """Símbolo de moneda del negocio activo (H-011, consistente con el dashboard)."""
    config = get_active_business_config(db, request)
    symbol = getattr(getattr(config, "currency", None), "symbol", None)
    return str(symbol) if symbol else "€"


def _check_agent_rate(key: str) -> bool:
    now = time.time()
    with _agent_lock:
        attempts = [t for t in _agent_attempts.get(key, []) if now - t < AGENT_RATE_WINDOW]
        _agent_attempts[key] = attempts
        if len(attempts) >= AGENT_RATE_LIMIT:
            return False
        _agent_attempts[key].append(now)
        return True


@router.get("/tabs/agent", response_class=HTMLResponse)
def tab_agent(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    # GETs enforce admin/owner too (G5/D5): the Copiloto tab shows financial
    # widgets and the sidebar only exposes it to admin/owner, so a direct URL
    # must not let an operator read them.
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(
        request, "partials/tab_agent.html", {"widgets": widgets, "currency_symbol": _currency_symbol(db, request)}
    )


@router.get("/agent/messages", response_class=HTMLResponse)
def messages_view(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    """Historial de la conversación del copiloto (por negocio)."""
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    messages = chat_store.list_messages(db, business_id)
    return templates.TemplateResponse(request, "partials/agent_messages.html", {"messages": messages})


@router.post("/agent/conversation/clear", response_class=HTMLResponse)
def clear_conversation(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    """Empezar una nueva conversación: borra el historial del negocio (no los widgets)."""
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    chat_store.clear_messages(db, business_id)
    messages = chat_store.list_messages(db, business_id)  # queda vacío
    return templates.TemplateResponse(request, "partials/agent_messages.html", {"messages": messages})


@router.post("/agent/chat", response_class=HTMLResponse)
async def chat(request: Request, message: str = Form(...), db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)

    user = get_current_user_from_session(db, request)
    rate_key = user.username if user is not None else (request.client.host if request.client else "unknown")
    currency_symbol = _currency_symbol(db, request)

    message = (message or "").strip()
    if not message:
        # A blank message would fire paid LLM calls for nothing (B3).
        return templates.TemplateResponse(
            request, "partials/agent_reply.html",
            {"reply": "Escribí una pregunta."},
            status_code=422,
        )

    created_by = int(user.id) if user is not None else None
    # Persistir la pregunta del usuario ANTES del rate-limit (B5): la burbuja
    # optimista ya la mostró, así el historial de BD queda consistente aunque la
    # petición sea rechazada. El commit además libera la conexión del request
    # durante el run lento del agente (el runner usa su propia sesión thread-local).
    chat_store.add_message(db, business_id, "user", message, user_id=created_by)

    if not _check_agent_rate(rate_key):
        return templates.TemplateResponse(
            request, "partials/agent_reply.html",
            {"reply": "Demasiadas peticiones. Esperá un momento."},
            status_code=429,
        )

    # Contexto conversacional: turnos previos (sin el mensaje recién agregado)
    # para que el modelo responda con memoria de la conversación.
    prior = chat_store.list_messages(db, business_id)[:-1]
    history = [{"role": m.role, "content": m.content} for m in prior]
    # Memoria de tool previa (REQ-3): solo widgets creados en o después del
    # primer turno de ESTA conversación — evita anclar a un widget de una
    # conversación anterior (H-001: `prior` no vacío no implica que esta
    # conversación haya producido su propio widget, hay que comparar fechas).
    last_tool = (
        widget_store.latest_widget_meta(db, business_id, since=prior[0].created_at)
        if prior else None
    )
    result = await runner.run_agent(business_id, message, history=history, last_tool=last_tool)
    for w in result["widgets"]:
        widget_store.add_widget(db, business_id, type=w["type"], title=w["title"],
                                tool=w["tool"], tool_args=w["tool_args"], payload=w.get("payload", {}),
                                unit=w.get("unit"), created_by=created_by)

    widgets = widget_store.widgets_with_payload(db, business_id)
    # El reply del plan siempre es no-vacío (PlanModel min_length=1 y fallbacks
    # con mensaje amigable), así que el error va en el propio reply (E3).
    reply = result["reply"] or "No pude responder."
    chat_store.add_message(db, business_id, "assistant", reply, user_id=created_by)

    return templates.TemplateResponse(
        request, "partials/agent_chat_turn.html",
        {"user_content": message, "reply": reply, "widgets": widgets, "currency_symbol": currency_symbol}
    )


@router.post("/agent/widget/{widget_id}/delete", response_class=HTMLResponse)
def delete(request: Request, widget_id: int, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    widget_store.delete_widget(db, business_id, widget_id)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(
        request, "partials/widget_grid.html", {"widgets": widgets, "currency_symbol": _currency_symbol(db, request)}
    )


@router.get("/agent/widgets", response_class=HTMLResponse)
def widgets_view(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    ensure_admin_or_owner(db, request)
    business_id = require_active_business_id(db, request)
    widgets = widget_store.widgets_with_payload(db, business_id)
    return templates.TemplateResponse(request, "partials/widget_grid.html", {"widgets": widgets, "currency_symbol": _currency_symbol(db, request)})
