"""REQ-3: como máximo un SELECT sobre `users` por petición UI.

Lote 0 (modo medida): recorre las rutas de la tabla de REQ-3 con un usuario logueado,
afirma status 200 y registra los recuentos. `STRICT_ROUTES` se llena al cerrar el Lote D
(el envelope que afirma todo REQ-3); hasta entonces no se afirma el máximo.
"""
from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.auth import hash_password
from app.db import Base
from app.deps import session_dep
from app.main import app
from app.models import Business, User, UserBusiness

ROUTES: list[tuple[str, str]] = [
    ("admin", "/ui/tabs/sales"),
    ("owner", "/ui/tabs/transfers"),
    ("operator", "/ui/tabs/sales"),
    ("owner", "/ui/tabs/documents"),
    ("admin", "/ui/tabs/inventory"),
    ("owner", "/ui/dashboard"),
]

# Todas las rutas de REQ-3 cumplen `users == 1` tras REQ-4 (auxiliares a ctx).
STRICT_ROUTES: set[str] = {
    "/ui/tabs/sales",
    "/ui/tabs/transfers",
    "/ui/tabs/documents",
    "/ui/tabs/inventory",
    "/ui/dashboard",
}

_COUNTS: dict[str, int] = {"users": 0, "user_businesses": 0}


def _reset_counts() -> None:
    _COUNTS["users"] = 0
    _COUNTS["user_businesses"] = 0


def _count_statement(conn, cursor, statement, parameters, context, executemany):  # noqa: ARG001
    text = str(statement)
    if re.search(r"\bFROM\s+users\b", text, flags=re.IGNORECASE):
        _COUNTS["users"] += 1
    if re.search(r"\bFROM\s+user_businesses\b", text, flags=re.IGNORECASE):
        _COUNTS["user_businesses"] += 1


@pytest.fixture
def counter_client():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    db = Session()
    biz = Business(code="test", name="Test Business")
    db.add(biz)
    db.flush()
    admin = User(username="admin", password_hash=hash_password("admin"), role="admin",
                 is_active=True, business_id=biz.id)
    owner = User(username="owner", password_hash=hash_password("owner"), role="owner",
                 is_active=True, business_id=biz.id)
    operator = User(username="operator", password_hash=hash_password("operator"), role="operator",
                    is_active=True, business_id=biz.id)
    db.add_all([admin, owner, operator])
    db.flush()
    db.add(UserBusiness(user_id=owner.id, business_id=biz.id))
    db.commit()
    db.close()

    def _override():
        s = Session()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[session_dep] = _override
    event.listen(engine, "before_cursor_execute", _count_statement)
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()
        event.remove(engine, "before_cursor_execute", _count_statement)
        engine.dispose()


@pytest.mark.parametrize("role,route", ROUTES)
def test_route_returns_200_and_records_counts(counter_client, role: str, route: str) -> None:
    client = counter_client
    resp = client.post("/ui/login", data={"username": role, "password": role},
                       follow_redirects=False)
    assert resp.status_code in (302, 303), f"login {role}: {resp.status_code}"
    _reset_counts()
    page = client.get(route)
    assert page.status_code == 200, f"{role} {route} -> {page.status_code}"
    print(f"[query-count] {role} {route}: users={_COUNTS['users']} "
          f"user_businesses={_COUNTS['user_businesses']}")
    if route in STRICT_ROUTES:
        assert _COUNTS["users"] == 1, f"{role} {route}: users={_COUNTS['users']}"
        max_ub = 2 if route == "/ui/dashboard" else 1
        assert _COUNTS["user_businesses"] <= max_ub, (
            f"{role} {route}: user_businesses={_COUNTS['user_businesses']} (máx {max_ub})"
        )
