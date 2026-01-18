from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.routers.health import router as health_router
from app.routers.inventory import router as inventory_router
from app.routers.products import router as products_router
from app.routers.ui import router as ui_router
from app.routers import (
    inventory,
    products,
    ui_auth,
    ui_expenses,
    ui_extractions,
    ui_products,
    ui_purchases,
    ui_sales,
    ui_sales_documents,
    ui_tabs,
    ui_transfers,
    ui_users,
)
from app.migrations import run_startup_tasks
from app.utils import get_session_secret


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifecycle manager para FastAPI."""
    run_startup_tasks()
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
    https_only=os.getenv("SESSION_HTTPS_ONLY", "0") == "1",
)

app.include_router(health_router)
app.include_router(products_router)
app.include_router(inventory_router)
app.include_router(ui_router)
app.include_router(ui_tabs.router, prefix="/ui")
app.include_router(ui_auth.router, prefix="/ui")
app.include_router(ui_products.router, prefix="/ui")
app.include_router(ui_purchases.router, prefix="/ui")
app.include_router(ui_sales.router, prefix="/ui")
app.include_router(ui_transfers.router, prefix="/ui")
app.include_router(ui_expenses.router, prefix="/ui")
app.include_router(ui_extractions.router, prefix="/ui")
app.include_router(ui_sales_documents.router, prefix="/ui")
app.include_router(ui_users.router, prefix="/ui")

os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
async def root(request: Request):
    """Redirige a dashboard si hay sesión activa, sino a login."""
    session = getattr(request, "session", None) or {}
    if session.get("username"):
        return RedirectResponse(url="/ui/dashboard", status_code=302)
    return RedirectResponse(url="/ui/login", status_code=302)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "10000")),
        reload=os.getenv("RELOAD", "1") == "1",
    )
