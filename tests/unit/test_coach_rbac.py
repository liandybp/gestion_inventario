from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from app.auth import hash_password
from app.models import User
from app.routers import ui_coach


def _request(username: str, business_id: int) -> MagicMock:
    req = MagicMock()
    req.session = {"username": username, "active_business_id": business_id}
    req.client = MagicMock()
    req.client.host = "127.0.0.1"
    req.url = MagicMock()
    req.url.path = "/ui/tabs/coach"
    return req


def _user(db_session, username: str, role: str, business_id: int) -> User:
    u = User(username=username, password_hash=hash_password("x"), role=role,
             is_active=True, business_id=business_id)
    db_session.add(u)
    db_session.commit()
    return u


def test_operator_gets_403_on_tab(db_session, business_id):
    _user(db_session, "operator", "operator", business_id)
    req = _request("operator", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_coach.tab_coach(req, db=db_session)
    assert exc.value.status_code == 403


def test_admin_gets_200_on_tab(db_session, business_id):
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    resp = ui_coach.tab_coach(req, db=db_session)
    assert resp.status_code == 200
    assert "tab_coach" in resp.template.name


def test_operator_gets_403_on_cash_baseline(db_session, business_id):
    _user(db_session, "operator", "operator", business_id)
    req = _request("operator", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_coach.set_cash_baseline(
            req, baseline_date="2026-01-01T00:00:00", amount=100, db=db_session
        )
    assert exc.value.status_code == 403


def test_admin_post_cash_baseline(db_session, business_id):
    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    resp = ui_coach.set_cash_baseline(
        req, baseline_date="2026-01-01T00:00:00", amount=100, db=db_session
    )
    assert resp.status_code == 200
    baseline = db_session.query(ui_coach.CashBaseline).one()
    assert baseline.amount == 100


def test_invalid_cash_amount_422(db_session, business_id):
    from fastapi import HTTPException

    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_coach.set_cash_baseline(
            req, baseline_date="2026-01-01T00:00:00", amount=-1, db=db_session
        )
    assert exc.value.status_code == 422


def test_invalid_action_status_422(db_session, business_id):
    from fastapi import HTTPException

    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    with pytest.raises(HTTPException) as exc:
        ui_coach.update_action(
            req, rule_key="margin_drop", status="invalid", db=db_session
        )
    assert exc.value.status_code == 422


def test_admin_post_action_done(db_session, business_id):
    from app.models import User

    _user(db_session, "admin", "admin", business_id)
    req = _request("admin", business_id)
    resp = ui_coach.update_action(
        req, rule_key="margin_drop", status="done", db=db_session
    )
    assert resp.status_code == 200
    user = db_session.query(User).filter_by(username="admin").one()
    log = db_session.query(ui_coach.CashBaseline).all()
    from app.models import CoachActionLog

    action = db_session.query(CoachActionLog).one()
    assert action.rule_key == "margin_drop"
    assert action.status == "done"
    assert action.marked_by == user.id


def test_coach_router_registered_in_main():
    """main.py registra ui_coach con prefix /ui: rutas visibles y sin duplicar."""
    from app.main import app

    paths = app.openapi()["paths"]
    assert "/ui/tabs/coach" in paths
    assert "/ui/coach/cash-baseline" in paths
    assert "/ui/coach/action" in paths
    assert "/ui/ui/tabs/coach" not in paths