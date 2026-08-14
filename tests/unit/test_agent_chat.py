from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.agent import chat_store
from app.auth import hash_password
from app.models import User
from app.routers import ui_agent


def test_add_and_list_messages(db_session, business_id):
    chat_store.add_message(db_session, business_id, "user", "¿cuánto vendí?")
    chat_store.add_message(db_session, business_id, "assistant", "Vendiste 100.")
    msgs = chat_store.list_messages(db_session, business_id)
    assert len(msgs) == 2
    assert msgs[0].role == "user"
    assert msgs[0].content == "¿cuánto vendí?"
    assert msgs[1].role == "assistant"


def test_messages_capped(db_session, business_id, monkeypatch):
    monkeypatch.setattr(chat_store, "MAX_MESSAGES", 3)
    for i in range(5):
        chat_store.add_message(db_session, business_id, "user", f"msg {i}")
    msgs = chat_store.list_messages(db_session, business_id)
    assert len(msgs) == 3
    # se recortaron las más viejas; queda lo más reciente
    assert [m.content for m in msgs] == ["msg 2", "msg 3", "msg 4"]


def test_clear_messages(db_session, business_id):
    chat_store.add_message(db_session, business_id, "user", "hola")
    chat_store.add_message(db_session, business_id, "assistant", "¡Hola!")
    assert len(chat_store.list_messages(db_session, business_id)) == 2
    deleted = chat_store.clear_messages(db_session, business_id)
    assert deleted == 2
    assert chat_store.list_messages(db_session, business_id) == []


def test_messages_view_operator_forbidden(db_session, business_id):
    u = User(username="op", password_hash=hash_password("x"), role="operator",
             is_active=True, business_id=business_id)
    db_session.add(u)
    db_session.commit()
    req = MagicMock()
    req.session = {"username": "op", "active_business_id": business_id}
    with pytest.raises(HTTPException) as exc:
        ui_agent.messages_view(req, db=db_session)
    assert exc.value.status_code == 403
