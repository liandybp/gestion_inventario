from __future__ import annotations

import json
import secrets
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import Date, DateTime, select
from sqlalchemy.orm import Session

from app.auth import hash_password
from app.logger import get_logger
from app.models import (
    AuditLog, Business, CashBaseline, ChatMessage, CoachActionLog, Customer,
    DashboardWidget, InventoryLot, InventoryMovement, Location, MoneyExtraction,
    MovementAllocation, OperatingExpense, Product, SalesDocument, SalesDocumentItem,
    SyncEvent, SyncState, User, UserBusiness,
)
from app.sync.outbox import rehash_chain
from app.sync.tables import SYNC_TABLE_ORDER

_log = get_logger(__name__)

_MODEL_BY_TABLE: dict[str, type] = {
    "businesses": Business,
    "locations": Location,
    "products": Product,
    "customers": Customer,
    "users": User,
    "sales_documents": SalesDocument,
    "inventory_movements": InventoryMovement,
    "inventory_lots": InventoryLot,
    "movement_allocations": MovementAllocation,
    "sales_document_items": SalesDocumentItem,
    "money_extractions": MoneyExtraction,
    "operating_expenses": OperatingExpense,
    "cash_baselines": CashBaseline,
}
assert set(_MODEL_BY_TABLE) == set(SYNC_TABLE_ORDER), (
    "app/sync/client.py::_MODEL_BY_TABLE desincronizado de "
    "app/sync/tables.py::SYNC_TABLE_ORDER — agregá la tabla nueva a ambos lados."
)

# Orden de dependencia FK (padres antes que hijos), derivado de la fuente única
# app/sync/tables.py::SYNC_TABLE_ORDER. apply_pull inserta en este orden y borra
# en el orden inverso — evita IntegrityError bajo PRAGMA foreign_keys=ON
# (H-001 de la auditoría 2026-08-29).
_PULL_ORDER: list[tuple[str, type]] = [
    (table, _MODEL_BY_TABLE[table]) for table in SYNC_TABLE_ORDER
]


def get_sync_state(db: Session) -> SyncState | None:
    return db.get(SyncState, 1)


def provision(db: Session, *, server_url: str, device_token: str) -> SyncState:
    """Guarda las credenciales de un nodo ya registrado en el servidor central
    (POST /sync/register, P2) en la fila única sync_state (id=1), creándola si
    la migración v7 todavía no la sembró."""
    state = get_sync_state(db)
    if state is None:
        state = SyncState(id=1)
        db.add(state)
    state.server_url = server_url.strip()
    state.device_token = device_token.strip()
    db.commit()
    return state


def _pending_events(db: Session) -> list[dict]:
    rows = list(
        db.scalars(select(SyncEvent).where(SyncEvent.status == "pending").order_by(SyncEvent.seq.asc()))
    )
    out: list[dict] = []
    for e in rows:
        out.append({
            "event_id": e.event_id, "event_type": e.event_type,
            "payload": json.loads(e.payload), "seq": e.seq,
            "prev_hash": e.prev_hash, "event_hash": e.event_hash,
            "location_id": e.location_id,
        })
    return out


# Tablas locales (NO sincronizadas — no están en SYNC_TABLE_ORDER) que igual
# referencian `users.id` por FK. Si tienen filas, bloquean `db.query(User).delete()`
# bajo PRAGMA foreign_keys=ON. Descubierto por el e2e de sync (login real escribe
# un audit_logs.user_id antes del primer sync) — el mismo patrón que ya se usaba
# para el borrado de usuario (ui_users.py) se generaliza acá para las 4 tablas
# nullable + user_businesses, que no es nullable y por eso se borra entera (sus
# filas quedan de todas formas obsoletas: los ids de `users` cambian con el pull).
def _detach_local_user_references(db: Session) -> None:
    db.query(AuditLog).filter(AuditLog.user_id.isnot(None)).update(
        {AuditLog.user_id: None}, synchronize_session=False
    )
    db.query(DashboardWidget).filter(DashboardWidget.created_by.isnot(None)).update(
        {DashboardWidget.created_by: None}, synchronize_session=False
    )
    db.query(ChatMessage).filter(ChatMessage.user_id.isnot(None)).update(
        {ChatMessage.user_id: None}, synchronize_session=False
    )
    db.query(CoachActionLog).filter(CoachActionLog.marked_by.isnot(None)).update(
        {CoachActionLog.marked_by: None}, synchronize_session=False
    )
    db.query(UserBusiness).delete()


def _coerce_datetimes(model: type, data: dict) -> dict:
    """Parsea de vuelta a `date`/`datetime` los valores que llegaron como string
    ISO por el viaje JSON (push/pull cruzan HTTP: `resp.json()` no reconstruye
    tipos Python, solo str/int/float/bool/None). Sin esto, `model(**data)`
    fallaba con `TypeError: SQLite DateTime type only accepts Python datetime
    and date objects` en cualquier tabla con una columna DateTime/Date — es
    decir, en un pull real (encontrado por el e2e de sync, no por los tests
    unitarios: estos siempre pasaban dicts con objetos datetime nativos, nunca
    el snapshot ya serializado/deserializado que realmente cruza la red)."""
    columns = model.__table__.columns
    out = dict(data)
    for key, value in out.items():
        if not isinstance(value, str):
            continue
        col = columns.get(key)
        if col is None:
            continue
        if isinstance(col.type, DateTime):
            out[key] = datetime.fromisoformat(value)
        elif isinstance(col.type, Date):
            out[key] = date.fromisoformat(value)
    return out


def apply_pull(db: Session, snapshot: dict) -> None:
    """Reemplazo transaccional de las tablas de lectura con las filas del snapshot.

    Borra en orden inverso de `_PULL_ORDER` y reinserta en orden directo — ambos
    respetan la dependencia de FKs (H-001). `users` es un caso especial: el pull
    nunca trae `password_hash` (excluido a propósito por P2, ver
    `SyncService._SENSITIVE_COLUMNS`); se preserva el hash local existente por
    `username` a través del reemplazo en vez de perderlo (H-002) — un usuario que
    llega por primera vez en un pull (nunca visto localmente) se crea con un hash
    aleatorio inutilizable, ya que este nodo nunca recibe su contraseña real; no
    podrá loguear en este nodo hasta que se le asigne una localmente.
    NO toca pending_sync_events ni sync_state (estado local del nodo).
    """
    try:
        present = [(table, model) for table, model in _PULL_ORDER if table in snapshot]
        preserved_hashes: dict[str, str] = {
            u.username: u.password_hash for u in db.scalars(select(User))
        }
        if any(model is User for _table, model in present):
            _detach_local_user_references(db)
        for _table, model in reversed(present):
            db.query(model).delete()
        for table, model in present:
            rows = snapshot.get(table) or []
            cols = {c.name for c in model.__table__.columns}
            for row in rows:
                data = _coerce_datetimes(model, {k: v for k, v in row.items() if k in cols})
                if model is User:
                    data["password_hash"] = preserved_hashes.get(
                        data.get("username"), hash_password(secrets.token_urlsafe(32))
                    )
                db.add(model(**data))
            # Flush por tabla (no uno solo al final): sin `relationship()` ORM
            # declaradas entre estos modelos, el unit-of-work de SQLAlchemy NO
            # reordena los INSERT por dependencia FK entre tablas distintas en
            # un único flush — encontrado por el e2e de sync (inventory_lots
            # referenciando un inventory_movements recién insertado en el
            # mismo flush fallaba con IntegrityError pese a que _PULL_ORDER ya
            # tenía el orden correcto).
            db.flush()
    except Exception:  # noqa: BLE001
        db.rollback()
        _log.error("apply_pull falló; rollback", exc_info=True)
        raise


def sync(db: Session) -> dict:
    """Push de pendientes + apply_pull + last_synced_at."""
    state = get_sync_state(db)
    if state is None or not state.server_url or not state.device_token:
        return {"ok": False, "error": "Nodo no aprovisionado (falta server_url/device_token)."}
    events = _pending_events(db)
    try:
        resp = httpx.post(
            f"{state.server_url.rstrip('/')}/sync/push",
            json={"app_schema_version": 7, "events": events},
            headers={"X-Sync-Token": state.device_token},
            timeout=30.0,
        )
    except Exception as exc:  # noqa: BLE001
        _log.error("Sync: fallo de red", exc_info=True)
        return {"ok": False, "error": f"Sin conexión: {exc}"}
    if resp.status_code != 200:
        return {"ok": False, "error": f"Servidor respondió {resp.status_code}: {resp.text[:200]}"}
    data = resp.json()
    applied = set(data.get("applied_event_ids", []))
    touched_business_ids: set[int] = set()
    for e in db.scalars(select(SyncEvent).where(SyncEvent.status == "pending")):
        if e.event_id in applied:
            e.status = "synced"
            touched_business_ids.add(int(e.business_id))
    for bid in touched_business_ids:
        rehash_chain(db, bid)
    apply_pull(db, data.get("pull", {}).get("snapshot", {}))
    state.last_synced_at = datetime.now(timezone.utc)
    db.commit()
    return {"ok": True, "applied": len(applied), "conflicts": len(data.get("conflicts", []))}
