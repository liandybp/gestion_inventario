from __future__ import annotations

import secrets

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import DeviceToken, SyncConflict, User
from app.repositories.location_repository import LocationRepository
from app.security import get_user_business_ids, require_active_business_id, require_admin_or_owner_api
from app.sync.auth import require_device
from app.sync.schemas import (
    ConflictOut,
    PushRequest,
    PushResponse,
    RegisterRequest,
    RegisterResponse,
    SyncPullData,
)
from app.sync.service import SyncService

router = APIRouter(tags=["sync"])


@router.post("/sync/register", response_model=RegisterResponse)
def register_device(
    payload: RegisterRequest,
    user: User = Depends(require_admin_or_owner_api),
    db: Session = Depends(session_dep),
) -> RegisterResponse:
    # H-001: require_admin_or_owner_api solo valida el ROL, no la pertenencia del
    # negocio. Un owner solo puede aprovisionar dispositivos para sus propios
    # negocios asignados (mismo criterio que require_active_business_id); un
    # admin tiene acceso global, igual que en el resto de la app.
    if (user.role or "").lower() == "owner":
        allowed = get_user_business_ids(db, int(user.id))
        if int(payload.business_id) not in allowed:
            raise HTTPException(status_code=403, detail="No tiene acceso a este negocio")

    # H-010: reusar la resolución de ubicación existente (evita el patrón de
    # bug histórico de ubicaciones huérfanas por lógica de resolución duplicada).
    loc_id = LocationRepository(db, int(payload.business_id)).location_id_for_code(
        payload.location_code, auto_create=False
    )

    token = secrets.token_urlsafe(32)
    device = DeviceToken(
        token=token, business_id=int(payload.business_id),
        location_id=loc_id, label=payload.label,
    )
    db.add(device)
    db.commit()
    db.refresh(device)
    pull = SyncService(db, device).build_pull()
    return RegisterResponse(token=token, device_id=int(device.id), pull=SyncPullData(**pull))


@router.post("/sync/push", response_model=PushResponse)
def push(
    payload: PushRequest,
    device: DeviceToken = Depends(require_device),
    db: Session = Depends(session_dep),
) -> PushResponse:
    service = SyncService(db, device)
    result = service.push(payload.app_schema_version, [e.model_dump() for e in payload.events])
    conflicts = db.scalars(
        select(SyncConflict).where(
            SyncConflict.business_id == int(device.business_id), SyncConflict.resolved == False  # noqa: E712
        )
    )
    conflict_out = [ConflictOut(event_id=c.event_id, kind=c.kind, detail=c.detail, created_at=c.created_at) for c in conflicts]
    pull = service.build_pull()
    return PushResponse(applied_event_ids=result["applied_event_ids"], conflicts=conflict_out, pull=SyncPullData(**pull))


@router.get("/sync/conflicts", response_model=list[ConflictOut])
def list_conflicts(
    request: Request,
    user: User = Depends(require_admin_or_owner_api),
    db: Session = Depends(session_dep),
) -> list[ConflictOut]:
    bid = require_active_business_id(db, request)
    rows = db.scalars(select(SyncConflict).where(
        SyncConflict.business_id == bid, SyncConflict.resolved == False  # noqa: E712
    ))
    return [ConflictOut(event_id=c.event_id, kind=c.kind, detail=c.detail, created_at=c.created_at) for c in rows]
