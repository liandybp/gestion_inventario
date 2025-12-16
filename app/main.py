from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.db import Base, engine
from app.db import get_session
from app.auth import hash_password
from app.routers.health import router as health_router
from app.routers.inventory import router as inventory_router
from app.routers.products import router as products_router
from app.routers.ui import router as ui_router
from app.models import User
from app.utils import get_session_secret


def _run_startup_tasks() -> None:
    """Ejecuta tareas de inicialización de la base de datos."""
    Base.metadata.create_all(bind=engine)

    with engine.connect() as conn:
        try:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_inventory_lots_lot_code ON inventory_lots(lot_code)"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear el índice UNIQUE para lot_code (hay lotes duplicados en la BD)."
            ) from e

        try:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_products_sku ON products(sku)"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear el índice UNIQUE para SKU (hay SKUs duplicados en la BD)."
            ) from e

        try:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_users_username ON users(username)"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear el índice UNIQUE para users.username (hay usuarios duplicados en la BD)."
            ) from e

        user_cols = {
            row[1] for row in conn.exec_driver_sql("PRAGMA table_info(users)").fetchall()
        }
        if "role" not in user_cols:
            conn.exec_driver_sql("ALTER TABLE users ADD COLUMN role VARCHAR(16)")
            conn.exec_driver_sql(
                "UPDATE users SET role='admin' WHERE role IS NULL OR role=''"
            )

        cols = {
            row[1]
            for row in conn.exec_driver_sql("PRAGMA table_info(products)").fetchall()
        }
        if "unit_of_measure" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN unit_of_measure VARCHAR(32)"
            )
        if "default_purchase_cost" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN default_purchase_cost NUMERIC(14, 4)"
            )
        if "image_url" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN image_url VARCHAR(512)"
            )

    db = get_session()
    try:
        users_to_ensure = [
            {
                "username": os.getenv("ADMIN_USERNAME", "admin"),
                "password": os.getenv("ADMIN_PASSWORD", "admin"),
                "role": "admin",
                "is_active": True,
            },
            {
                "username": os.getenv("OPERATOR_USERNAME", "operator"),
                "password": os.getenv("OPERATOR_PASSWORD", "operator"),
                "role": "operator",
                "is_active": True,
            },
        ]

        for spec in users_to_ensure:
            username = (spec.get("username") or "").strip()
            if not username:
                continue
            existing = db.scalar(select(User).where(User.username == username))
            if existing is None:
                db.add(
                    User(
                        username=username,
                        password_hash=hash_password(str(spec.get("password") or "")),
                        role=str(spec.get("role") or "operator"),
                        is_active=bool(spec.get("is_active", True)),
                    )
                )
            else:
                existing.role = str(spec.get("role") or existing.role or "operator")
                existing.is_active = bool(spec.get("is_active", True))
        db.commit()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifecycle manager para FastAPI."""
    _run_startup_tasks()
    yield


app = FastAPI(title="Inventario", lifespan=lifespan)


@app.middleware("http")
async def ui_auth_middleware(request, call_next):
    path = request.url.path or ""
    if path.startswith("/static") or path == "/health":
        return await call_next(request)
    if path.startswith("/ui") and path not in ("/ui/login", "/ui/logout"):
        session = getattr(request, "session", None) or {}
        if not session.get("username"):
            if request.headers.get("HX-Request") == "true":
                resp = RedirectResponse(url="/ui/login", status_code=302)
                resp.headers["HX-Redirect"] = "/ui/login"
                return resp
            return RedirectResponse(url="/ui/login", status_code=302)
    return await call_next(request)


app.add_middleware(
    SessionMiddleware,
    secret_key=get_session_secret(),
    session_cookie="inventario_session",
    max_age=60 * 60 * 24 * 7,
    same_site="lax",
    https_only=False,
)

app.include_router(health_router)
app.include_router(products_router)
app.include_router(inventory_router)
app.include_router(ui_router)

os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "8000")),
        reload=os.getenv("RELOAD", "1") == "1",
    )
