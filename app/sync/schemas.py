from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class SyncEventIn(BaseModel):
    event_id: str
    event_type: str
    payload: dict[str, Any]
    seq: int
    prev_hash: Optional[str] = None
    event_hash: str
    location_id: Optional[int] = None


class SyncPullData(BaseModel):
    snapshot: dict[str, list[dict[str, Any]]]
    server_time: datetime


class ConflictOut(BaseModel):
    event_id: str
    kind: str
    detail: str
    created_at: Optional[datetime] = None


class RegisterRequest(BaseModel):
    business_id: int
    location_code: str
    label: str


class RegisterResponse(BaseModel):
    token: str
    device_id: int
    pull: SyncPullData


class PushRequest(BaseModel):
    app_schema_version: int
    events: list[SyncEventIn] = []


class PushResponse(BaseModel):
    applied_event_ids: list[str]
    conflicts: list[ConflictOut]
    pull: SyncPullData
