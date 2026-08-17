from __future__ import annotations

import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

import app.settings as settings  # H-045: env centralizada
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.logger import get_logger, request_id_var, set_request_id

_log = get_logger(__name__)

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
    ui_businesses,
    ui_agent,
    ui_coach,
    ui_expenses,
    ui_extractions,
    ui_products,
    ui_purchases,
    ui_sales,
    ui_sales_documents,
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
async def security_headers_middleware(request, call_next):
    """H-006: security headers básicos (CSP, frame, sniffing, referrer)."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "connect-src 'self'; "
        "font-src 'self' data:; "
        "object-src 'none'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'"
    )
    return response


@app.middleware("http")
async def ui_auth_middleware(request, call_next):
    path = request.url.path or ""
    if path.startswith("/static") or path == "/health":
        return await call_next(request)

    # CSRF protection: require HX-Request header for state-changing UI requests
    # Login, logout, and active-business switch are exempt (regular HTML forms, not HTMX)
    _csrf_exempt_paths = {"/ui/login", "/ui/logout", "/ui/active-business"}
    if (
        path.startswith("/ui")
        and path not in _csrf_exempt_paths
        and request.method in ("POST", "PUT", "PATCH", "DELETE")
        and request.headers.get("HX-Request") != "true"
    ):
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse("CSRF validation failed", status_code=403)

    if path.startswith("/ui") and path not in ("/ui/login", "/ui/logout"):
        session = getattr(request, "session", None) or {}
        if session.get("username"):
            now = int(time.time())
            now_ms = int(time.time() * 1000)
            last_activity = session.get("last_activity")
            try:
                last_activity = int(last_activity) if last_activity is not None else None
            except (TypeError, ValueError):
                last_activity = None

            if last_activity is not None and (now - last_activity) > (60 * 60):
                session.clear()
                if request.headers.get("HX-Request") == "true":
                    resp = RedirectResponse(url="/ui/login", status_code=302)
                    resp.headers["HX-Redirect"] = "/ui/login"
                    return resp
                return RedirectResponse(url="/ui/login", status_code=302)

            should_bump_activity = True
            if request.headers.get("HX-Request") == "true":
                # HTMX can fire background polling requests (every Xm) that should NOT keep the session alive.
                # app.js attaches X-User-Activity (ms timestamp of last real user interaction).
                raw_ts = request.headers.get("X-User-Activity")
                if raw_ts is not None:
                    try:
                        ts = int(raw_ts)
                        # Only count as "activity" if the user interacted recently.
                        if ts > 0 and (now_ms - ts) > (30 * 1000):
                            should_bump_activity = False
                    except (TypeError, ValueError):
                        pass

            if last_activity is None:
                session["last_activity"] = now
            elif should_bump_activity:
                session["last_activity"] = now
        if not session.get("username"):
            if request.headers.get("HX-Request") == "true":
                resp = RedirectResponse(url="/ui/login", status_code=302)
                resp.headers["HX-Redirect"] = "/ui/login"
                return resp
            return RedirectResponse(url="/ui/login", status_code=302)
    return await call_next(request)


@app.middleware("http")
async def correlation_id_middleware(request, call_next):
    """H-043: X-Request-ID + log de request (método, path, status, duración)."""
    rid = (request.headers.get("X-Request-ID") or "").strip() or uuid.uuid4().hex[:16]
    request.state.request_id = rid
    set_request_id(rid)
    start = time.monotonic()
    response = await call_next(request)
    duration_ms = int((time.monotonic() - start) * 1000)
    response.headers["X-Request-ID"] = rid
    path = getattr(request, "url", None)
    _log.info(
        "request %s %s -> %s (%sms)",
        request.method, path.path if path else request.url, response.status_code, duration_ms,
    )
    return response


app.add_middleware(
    SessionMiddleware,
    secret_key=get_session_secret(),
    session_cookie="inventario_session",
    max_age=60 * 60 * 24,
    same_site="lax",
    https_only=settings.SESSION_HTTPS_ONLY,
)

app.include_router(health_router)
app.include_router(products_router)
app.include_router(inventory_router)
app.include_router(ui_router)
app.include_router(ui_auth.router, prefix="/ui")
app.include_router(ui_products.router, prefix="/ui")
app.include_router(ui_purchases.router, prefix="/ui")
app.include_router(ui_sales.router, prefix="/ui")
app.include_router(ui_transfers.router, prefix="/ui")
app.include_router(ui_expenses.router, prefix="/ui")
app.include_router(ui_extractions.router, prefix="/ui")
app.include_router(ui_sales_documents.router, prefix="/ui")
app.include_router(ui_users.router, prefix="/ui")
app.include_router(ui_businesses.router, prefix="/ui")
app.include_router(ui_agent.router, prefix="/ui")
app.include_router(ui_coach.router, prefix="/ui")

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
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.RELOAD,
    )
