from __future__ import annotations

import asyncio
from collections import defaultdict
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.auth import hash_password
from app.agent import widgets as widget_store
from app.models import User
from app.routers import ui_agent


def _request(username: str, business_id: int) -> MagicMock:
    req = MagicMock()
    req.session = {"username": username, "active_business_id": business_id}
    req.client = MagicMock()
    req.client.host = "127.0.0.1"
    req.url = MagicMock()
    req.url.path = "/ui/tabs/agent"
    return req


def _user(db: Session, username: str, role: str, business_id: int) -> User:
    u = User(username=username, password_hash=hash_password("x"), role=role,
             is_active=True, business_id=business_id)
    db.add(u)
    db.commit()
    return u


def test_tab_agent_requires_auth():
    from fastapi.testclient import TestClient
    from app.main import app

    client = TestClient(app, follow_redirects=False)
    resp = client.get("/ui/tabs/agent")
    assert resp.status_code in (302, 401)


def test_agent_routes_single_ui_prefix():
    """Regression: the agent router must NOT double the /ui prefix.

    main.py registers ui_agent.router with prefix="/ui"; the router itself
    must not declare one too (would produce /ui/ui/tabs/agent). We inspect
    openapi paths because Starlette 1.3 wraps included routers in
    _IncludedRouter (no direct .path on app.routes).
    """
    from app.main import app

    paths = app.openapi()["paths"]
    assert "/ui/tabs/agent" in paths
    assert "/ui/agent/chat" in paths
    assert "/ui/agent/widgets" in paths
    assert "/ui/ui/tabs/agent" not in paths


def test_tab_agent_operator_forbidden(db_session, business_id):
    """H-003 / G5: operator must NOT read the Copiloto tab (403)."""
    _user(db_session, "operator", "operator", business_id)
    req = _request("operator", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_agent.tab_agent(req, db=db_session)
    assert exc.value.status_code == 403


def test_delete_operator_forbidden(db_session, business_id):
    _user(db_session, "operator", "operator", business_id)
    req = _request("operator", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_agent.delete(req, widget_id=1, db=db_session)
    assert exc.value.status_code == 403


def test_chat_empty_message_returns_422(db_session, business_id, monkeypatch):
    """H-003 / B3: a blank message must not fire LLM calls."""
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)

    captured = {}

    def _fake_tmpl(request, name, context=None, status_code=None, **kw):
        captured["status_code"] = status_code
        return "RENDERED"

    monkeypatch.setattr(ui_agent.templates, "TemplateResponse", _fake_tmpl)
    asyncio.run(ui_agent.chat(req, message="   ", db=db_session))
    assert captured["status_code"] == 422


def test_chat_rate_limit_429(db_session, business_id, monkeypatch):
    """H-003 / B4: the chat endpoint enforces a per-user rate limit."""
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)

    monkeypatch.setattr(ui_agent, "AGENT_RATE_LIMIT", 1)
    monkeypatch.setattr(ui_agent, "AGENT_RATE_WINDOW", 60)
    monkeypatch.setattr(ui_agent, "_agent_attempts", defaultdict(list))

    captured = {}

    def _fake_tmpl(request, name, context=None, status_code=None, **kw):
        captured["status_code"] = status_code
        return "RENDERED"

    monkeypatch.setattr(ui_agent.templates, "TemplateResponse", _fake_tmpl)

    # Primera petición: pasa el límite (se registra el intento).
    assert ui_agent._check_agent_rate("admin") is True
    # Segunda petición en la ventana: supera el límite -> 429.
    asyncio.run(ui_agent.chat(req, message="ventas del mes", db=db_session))
    assert captured["status_code"] == 429


def test_tab_agent_admin_renders_widgets(db_session, business_id):
    """H-003: admin sees its widgets in the tab (payload + currency symbol)."""
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    widget_store.add_widget(db_session, business_id, type="kpi", title="Valor",
                            tool="inventory_value_total", tool_args={},
                            payload={"value": 1234.5, "label": None, "currency": True},
                            unit=None, created_by=None)

    rendered = {}
    import app.routers.ui_common as uic

    def _fake_tmpl(request, name, context=None, **kw):
        rendered["name"] = name
        rendered["context"] = context
        return "RENDERED"

    original = uic.templates.TemplateResponse
    uic.templates.TemplateResponse = _fake_tmpl
    try:
        ui_agent.tab_agent(req, db=db_session)
    finally:
        uic.templates.TemplateResponse = original

    assert rendered["name"] == "partials/tab_agent.html"
    # El símbolo depende de la config del negocio (archivo en disco); solo
    # verificamos que se pase al contexto y que el widget esté presente.
    assert rendered["context"]["currency_symbol"]
    assert len(rendered["context"]["widgets"]) == 1
