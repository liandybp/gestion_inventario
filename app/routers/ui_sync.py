from __future__ import annotations

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.logger import get_logger
from app.security import get_current_user_from_session
from app.settings import app_mode_label, is_desktop
from app.sync.client import get_sync_state, provision as provision_state, sync as run_sync
from app.routers.ui_common import ensure_admin, templates

router = APIRouter(tags=["ui-sync"])

_log = get_logger(__name__)


@router.post("/sync", response_class=HTMLResponse)
def do_sync(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    if not is_desktop():
        raise HTTPException(status_code=404, detail="No disponible en modo servidor")
    try:
        result = run_sync(db)
    except Exception:  # noqa: BLE001
        _log.error("Sync manual (/ui/sync) falló inesperadamente", exc_info=True)
        result = {"ok": False, "error": "Fallo interno durante la sincronización, ver logs."}
    return templates.TemplateResponse(
        request=request, name="partials/sync_status.html",
        context={"result": result, "state": get_sync_state(db), "app_mode": "desktop", "user": user},
    )


@router.get("/sync/status", response_class=HTMLResponse)
def sync_status(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    user = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request, name="partials/sync_status.html",
        context={"result": None, "state": get_sync_state(db), "app_mode": app_mode_label(), "user": user},
    )


@router.post("/sync/provision", response_class=HTMLResponse)
def provision(
    request: Request,
    server_url: str = Form(...),
    device_token: str = Form(...),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    """Guarda en `sync_state` las credenciales que un admin ya obtuvo llamando
    `POST /sync/register` en el servidor central (P2). No hay, todavía, un flujo
    de registro remoto automático desde este nodo — ver H-008 de la auditoría
    2026-08-29 (`docs/auditorias/2026-08-29-p3-sync-client-offline-sync.md`)."""
    ensure_admin(db, request)
    if not is_desktop():
        raise HTTPException(status_code=404, detail="No disponible en modo servidor")
    if not server_url.strip() or not device_token.strip():
        raise HTTPException(status_code=422, detail="server_url y device_token son obligatorios")
    provision_state(db, server_url=server_url, device_token=device_token)
    user = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request, name="partials/sync_status.html",
        context={"result": None, "state": get_sync_state(db), "app_mode": "desktop", "user": user},
    )
