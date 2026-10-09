# Task Envelopes — P2: Backend de sincronización

- **Slice:** P2 (`offline-desktop-sync`) · **Rama base:** `develop` · **Depende de:** P1 (verde)
- **Insumo:** `design.md` (mismo dir) · **REQs:** 10, 11, 12, 13, 14, 16, 17(parcial)
- **Test runner:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest`
- **Batches:** T1-T3 · T4-T6 · T7-T9 · T10-T13 (`/compact` entre batches)

> Mapeo: REQ-10→T6,T13 · REQ-11→T7 · REQ-12→T5,T8 · REQ-13→T9 · REQ-14→T1,T10 · REQ-16→T6,T13 · REQ-17→T9.

---

### T1: Modelos de sync (device_tokens, applied_sync_events, sync_conflicts)

**Depends on:** none · **Skill:** data-engineer · **Agent:** data
**Files:** `app/models.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/models.py — imports ya presentes (líneas 3-9): datetime, Optional, Boolean, DateTime,
# ForeignKey, Index, Integer, Numeric, String, Text, func, Mapped, mapped_column, Base.
# Patrón: class X(Base): __tablename__ = "..."; id: Mapped[int] = mapped_column(primary_key=True)
```

#### Objective
Añadir 3 modelos al final de `app/models.py`.

#### Instructions
**Step 1:** `Read app/models.py`
**Step 2:** Append al final:
```python
class DeviceToken(Base):
    __tablename__ = "device_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    token: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    business_id: Mapped[int] = mapped_column(Integer, index=True)
    location_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    label: Mapped[str] = mapped_column(String(128))
    revoked: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class AppliedSyncEvent(Base):
    __tablename__ = "applied_sync_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    device_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    business_id: Mapped[int] = mapped_column(Integer, index=True)
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class SyncConflict(Base):
    __tablename__ = "sync_conflicts"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(Integer, index=True)
    device_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    event_id: Mapped[str] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(32))
    detail: Mapped[str] = mapped_column(Text)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.models import DeviceToken, AppliedSyncEvent, SyncConflict; print('ok')"
# Expected: ok
```
#### Expected Report
- [ ] `app/models.py` — 3 clases añadidas al final · Import imprime `ok`

---

### T2: Migración v6 (3 tablas de sync)

**Depends on:** T1 · **Skill:** data-engineer · **Agent:** data
**Files:** `app/migrations.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/migrations.py — _LATEST_SCHEMA_VERSION = 5 (tras P1)
# Ambas ramas terminan con: if schema_version < 1: _set_schema_version(conn, 1); return schema_version
# Patrón: conn.exec_driver_sql("CREATE TABLE IF NOT EXISTS ...")
```
#### Objective
Crear device_tokens, applied_sync_events, sync_conflicts y subir versión a 6.

#### Instructions
**Step 1:** `Read app/migrations.py`
**Step 2:** `_LATEST_SCHEMA_VERSION = 5` → `_LATEST_SCHEMA_VERSION = 6`
**Step 3:** En `_run_sqlite_schema_migrations`, antes de `if schema_version < 1:`, añadir:
```python
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS device_tokens ("
            "id INTEGER PRIMARY KEY, token VARCHAR(128), business_id INTEGER, "
            "location_id INTEGER, label VARCHAR(128), revoked BOOLEAN DEFAULT false, "
            "created_at TIMESTAMP, last_seen_at TIMESTAMP)"
        )
        conn.exec_driver_sql("CREATE UNIQUE INDEX IF NOT EXISTS ux_device_tokens_token ON device_tokens(token)")
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS applied_sync_events ("
            "id INTEGER PRIMARY KEY, event_id VARCHAR(64), device_id INTEGER, "
            "business_id INTEGER, applied_at TIMESTAMP)"
        )
        conn.exec_driver_sql("CREATE UNIQUE INDEX IF NOT EXISTS ux_applied_sync_events_event_id ON applied_sync_events(event_id)")
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS sync_conflicts ("
            "id INTEGER PRIMARY KEY, business_id INTEGER, device_id INTEGER, "
            "event_id VARCHAR(64), kind VARCHAR(32), detail TEXT, "
            "resolved BOOLEAN DEFAULT false, created_at TIMESTAMP)"
        )
```
**Step 4:** En `_run_postgresql_schema_migrations`, antes de `if schema_version < 1:`, añadir lo mismo con `id SERIAL PRIMARY KEY` y `TIMESTAMPTZ` en vez de `TIMESTAMP`.

**Edge cases:** `IF NOT EXISTS` idempotente; `Base.metadata.create_all` ya crea las tablas en BD nuevas.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "import app.migrations as m; print(m._LATEST_SCHEMA_VERSION)"
# Expected: 6
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```
#### Expected Report
- [ ] `_LATEST_SCHEMA_VERSION = 6` + 3 tablas en ambas ramas · suite unit PASS

---

### T3: Schemas de sync

**Depends on:** none · **Skill:** api-designer · **Agent:** coder
**Files:** `app/sync/schemas.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# Pydantic v2 (BaseModel, Field). app/sync/__init__.py ya existe (P1).
```
#### Objective
Crear los schemas Pydantic del protocolo de sync.

#### Instructions
**Step 1: Create** `app/sync/schemas.py`:
```python
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
```
#### Verification
```bash
python3 -c "from app.sync.schemas import PushRequest, PushResponse, RegisterResponse, SyncPullData; print('ok')"
# Expected: ok
```
#### Expected Report
- [ ] `app/sync/schemas.py` creado · import `ok`

---

### T4: Auth de dispositivo (`require_device`)

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/auth.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/deps.py — def session_dep() -> Generator[Session] (usar con Depends)
# app/models.py — DeviceToken(token:str unique, revoked:bool, last_seen_at)
# FastAPI: Request, HTTPException, Depends
```
#### Objective
Crear la dependency `require_device` que autentica por header `X-Sync-Token`.

#### Instructions
**Step 1: Create** `app/sync/auth.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import DeviceToken


def require_device(
    request: Request, db: Session = Depends(session_dep)
) -> DeviceToken:
    token = request.headers.get("X-Sync-Token", "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="X-Sync-Token requerido")
    device = db.scalar(select(DeviceToken).where(DeviceToken.token == token))
    if device is None or device.revoked:
        raise HTTPException(status_code=401, detail="Token de dispositivo inválido o revocado")
    device.last_seen_at = datetime.now(timezone.utc)
    db.commit()
    return device
```
**Edge cases:** header vacío → 401; token inexistente o revocado → 401.

#### Verification
```bash
python3 -c "from app.sync.auth import require_device; print('ok')"
# Expected: ok
```
#### Expected Report
- [ ] `app/sync/auth.py` creado · import `ok`

---

### T5: `CustomerRepository.find_by_contact` (dedup)

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/repositories/customer_repository.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/repositories/customer_repository.py — ACTUAL
class CustomerRepository:
    def __init__(self, db: Session, business_id: int):
        self._db = db; self._business_id = int(business_id)
    def get(self, customer_id: int) -> Optional[Customer]: ...
# from sqlalchemy import and_, func, select  (ya importado)
# Customer(business_id, client_id, name, phone, email, created_at)
```
#### Objective
Añadir `find_by_contact(phone, email)` al repositorio.

#### Instructions
**Step 1:** `Read app/repositories/customer_repository.py`
**Step 2:** Añadir el método (tras `get`):
```python
    def find_by_contact(self, phone: Optional[str], email: Optional[str]) -> Optional[Customer]:
        """Cliente del negocio cuyo teléfono O email coincide (no vacío). El más antiguo si varios."""
        p = (phone or "").strip()
        e = (email or "").strip()
        if not p and not e:
            return None
        conds = []
        if p:
            conds.append(Customer.phone == p)
        if e:
            conds.append(Customer.email == e)
        from sqlalchemy import or_
        stmt = (
            select(Customer)
            .where(Customer.business_id == self._business_id)
            .where(or_(*conds))
            .order_by(Customer.created_at.asc(), Customer.id.asc())
            .limit(1)
        )
        return self._db.scalar(stmt)
```
**Edge cases:** ambos vacíos → `None`; varios match → el más antiguo (created_at asc).

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_repositories.py -q
# Expected: PASS
```
#### Expected Report
- [ ] `customer_repository.py` — método `find_by_contact` añadido · tests repos PASS

---

### T6: `SyncService` — init, versión, idempotencia, dispatch

**Depends on:** T1, T3 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/service.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/models.py — DeviceToken(business_id, location_id, id), AppliedSyncEvent(event_id, device_id, business_id), SyncConflict(...)
# app/logger.py — get_logger(__name__)
# from sqlalchemy import select
```
#### Objective
Crear `SyncService` con versión, ledger de idempotencia y bucle de despacho (métodos de replay se añaden en T7/T8/T9).

#### Instructions
**Step 1: Create** `app/sync/service.py`:
```python
from __future__ import annotations

import json
from datetime import datetime, timezone

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.logger import get_logger
from app.models import AppliedSyncEvent, DeviceToken, SyncConflict

_log = get_logger(__name__)

MIN_APP_SCHEMA_VERSION = 6


class SyncService:
    def __init__(self, db: Session, device: DeviceToken):
        self._db = db
        self._device = device
        self._business_id = int(device.business_id)

    def _already_applied(self, event_id: str) -> bool:
        return self._db.scalar(
            select(AppliedSyncEvent).where(AppliedSyncEvent.event_id == event_id)
        ) is not None

    def _mark_applied(self, event_id: str) -> None:
        self._db.add(AppliedSyncEvent(
            event_id=event_id, device_id=int(self._device.id), business_id=self._business_id
        ))

    def _record_conflict(self, event_id: str, kind: str, detail: dict) -> None:
        self._db.add(SyncConflict(
            business_id=self._business_id, device_id=int(self._device.id),
            event_id=event_id, kind=kind, detail=json.dumps(detail, ensure_ascii=False),
        ))
        self._db.commit()

    def push(self, app_schema_version: int, events: list[dict]) -> dict:
        if int(app_schema_version) < MIN_APP_SCHEMA_VERSION:
            raise HTTPException(status_code=409, detail="versión incompatible, actualice la app")
        applied: list[str] = []
        for ev in sorted(events, key=lambda e: int(e.get("seq", 0))):
            eid = str(ev.get("event_id") or "")
            if not eid or self._already_applied(eid):
                continue
            try:
                if ev.get("event_type") == "sale":
                    ok = self._apply_sale(ev)
                elif ev.get("event_type") == "customer":
                    ok = self._apply_customer(ev)
                else:
                    ok = False
                if ok:
                    applied.append(eid)
            except Exception:  # noqa: BLE001
                self._db.rollback()
                _log.error("Fallo aplicando evento %s", eid, exc_info=True)
                self._record_conflict(eid, "apply_error", {"event_type": ev.get("event_type")})
        return {"applied_event_ids": applied}
```
**Edge cases:** versión vieja → 409; evento ya aplicado o sin id → skip; error inesperado → rollback + conflicto, no rompe el batch.

#### Verification
```bash
python3 -c "from app.sync.service import SyncService, MIN_APP_SCHEMA_VERSION; print(MIN_APP_SCHEMA_VERSION)"
# Expected: 6
```
#### Expected Report
- [ ] `app/sync/service.py` creado con `SyncService` (init, versión, idempotencia, push loop)

---

### T7: `SyncService._apply_sale` (replay vía InventoryService)

**Depends on:** T6 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/service.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/services/inventory_service.py — InventoryService(db, business_id).sale(SaleCreate) -> MovementResult
#   COMMITEA internamente; 409 HTTPException si stock insuficiente.
# app/schemas.py — SaleCreate(sku, quantity, unit_price?, movement_date?, note?, location_code?)
# payload de venta esperado: {"sku","quantity","unit_price"?,"movement_date"?,"note"?,"location_code"?}
```
#### Objective
Añadir `_apply_sale` a `SyncService`: marca aplicado en la misma sesión y reaplica la venta.

#### Instructions
**Step 1:** `Read app/sync/service.py`
**Step 2:** Add imports al inicio:
```python
from app.schemas import SaleCreate
from app.services.inventory_service import InventoryService
```
**Step 3:** Añadir método a la clase:
```python
    def _apply_sale(self, ev: dict) -> bool:
        eid = str(ev.get("event_id") or "")
        p = ev.get("payload") or {}
        # Marca de idempotencia ANTES de sale(): el commit interno de sale() los persiste juntos.
        self._mark_applied(eid)
        try:
            payload = SaleCreate(
                sku=str(p.get("sku")),
                quantity=float(p.get("quantity")),
                unit_price=p.get("unit_price"),
                movement_date=p.get("movement_date"),
                note=p.get("note"),
                location_code=p.get("location_code"),
            )
            InventoryService(self._db, business_id=self._business_id).sale(payload)
            return True
        except HTTPException as e:
            self._db.rollback()  # descarta el AppliedSyncEvent pendiente
            self._record_conflict(eid, "sale_stock", {"sku": p.get("sku"), "quantity": p.get("quantity"), "detail": str(e.detail)})
            return False
```
**Edge cases:** 409 stock → rollback (no queda marcado) + conflicto `sale_stock`; éxito → venta + marca commiteadas atómicamente por `sale()`.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.service import SyncService; print(hasattr(SyncService,'_apply_sale'))"
# Expected: True
```
#### Expected Report
- [ ] `service.py` — imports + `_apply_sale` añadidos · imprime `True`

---

### T8: `SyncService._apply_customer` (dedup conservar-el-primero)

**Depends on:** T5, T6 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/service.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/repositories/customer_repository.py — CustomerRepository(db, business_id)
#   find_by_contact(phone, email) -> Optional[Customer]   (T5)
#   create(*, client_id, name, address=None, phone=None, email=None) -> Customer
# app/models.py — Customer(client_id unique global)
# payload cliente: {"client_id","name","phone"?,"email"?,"address"?}
```
#### Objective
Añadir `_apply_customer`: dedup por contacto (conservar-el-primero), colisión de client_id → conflicto.

#### Instructions
**Step 1:** `Read app/sync/service.py`
**Step 2:** Add import:
```python
from app.models import Customer
from app.repositories.customer_repository import CustomerRepository
```
**Step 3:** Añadir método:
```python
    def _apply_customer(self, ev: dict) -> bool:
        eid = str(ev.get("event_id") or "")
        p = ev.get("payload") or {}
        repo = CustomerRepository(self._db, self._business_id)
        # Dedup por contacto: si ya existe uno con el mismo teléfono/email, conservar el primero.
        existing = repo.find_by_contact(p.get("phone"), p.get("email"))
        if existing is not None:
            self._mark_applied(eid)
            self._db.commit()
            return True
        client_id = str(p.get("client_id") or "").strip()
        clash = self._db.scalar(select(Customer).where(Customer.client_id == client_id))
        if clash is not None:
            # Mismo client_id pero contacto distinto: personas distintas → sufijo + conflicto.
            new_cid = f"{client_id}-{eid[:6]}"
            repo.create(client_id=new_cid, name=str(p.get("name") or ""), address=p.get("address"), phone=p.get("phone"), email=p.get("email"))
            self._mark_applied(eid)
            self._record_conflict(eid, "customer_client_id", {"client_id": client_id, "reassigned": new_cid})
            self._db.commit()
            return True
        repo.create(client_id=client_id, name=str(p.get("name") or ""), address=p.get("address"), phone=p.get("phone"), email=p.get("email"))
        self._mark_applied(eid)
        self._db.commit()
        return True
```
**Edge cases:** match de contacto → descartar duplicado (no crea); client_id colisiona sin match → sufijo `-<eid6>` + conflicto; sin colisión → crea normal.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.service import SyncService; print(hasattr(SyncService,'_apply_customer'))"
# Expected: True
```
#### Expected Report
- [ ] `service.py` — imports + `_apply_customer` añadidos · imprime `True`

---

### T9: `SyncService.build_pull` (foto completa + usuarios acotados)

**Depends on:** T6 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/service.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/models.py — Business, Location, Product, Customer, InventoryMovement, InventoryLot,
#   MovementAllocation, SalesDocument, SalesDocumentItem, OperatingExpense, MoneyExtraction,
#   CashBaseline, User. Todos con business_id salvo SalesDocumentItem/MovementAllocation
#   (hijos: se traen por su tabla completa del negocio vía join implícito → v1 trae todos los del negocio por sus padres; simplificación: traer todas las filas de esas 2 tablas).
# from sqlalchemy import select
```
#### Objective
Añadir `build_pull` y el helper `_dump` que serializa filas business-scoped a dicts.

#### Instructions
**Step 1:** `Read app/sync/service.py`
**Step 2:** Add imports:
```python
from app.models import (
    Business, CashBaseline, Location, MoneyExtraction, MovementAllocation,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User,
)
from app.models import InventoryLot, InventoryMovement
```
**Step 3:** Añadir métodos:
```python
    def _dump(self, model, *filters) -> list[dict]:
        rows = list(self._db.scalars(select(model).where(*filters))) if filters else list(self._db.scalars(select(model)))
        out: list[dict] = []
        for r in rows:
            d = {c.name: getattr(r, c.name) for c in model.__table__.columns}
            out.append(d)
        return out

    def build_pull(self) -> dict:
        bid = self._business_id
        snap = {
            "businesses": self._dump(Business, Business.id == bid),
            "locations": self._dump(Location, Location.business_id == bid),
            "products": self._dump(Product, Product.business_id == bid),
            "customers": self._dump(Customer, Customer.business_id == bid),
            "inventory_movements": self._dump(InventoryMovement, InventoryMovement.business_id == bid),
            "inventory_lots": self._dump(InventoryLot, InventoryLot.business_id == bid),
            "movement_allocations": self._dump(MovementAllocation),
            "sales_documents": self._dump(SalesDocument, SalesDocument.business_id == bid),
            "sales_document_items": self._dump(SalesDocumentItem),
            "operating_expenses": self._dump(OperatingExpense, OperatingExpense.business_id == bid),
            "money_extractions": self._dump(MoneyExtraction, MoneyExtraction.business_id == bid),
            "cash_baselines": self._dump(CashBaseline, CashBaseline.business_id == bid),
            "users": self._dump(User, User.business_id == bid),
        }
        return {"snapshot": snap, "server_time": datetime.now(timezone.utc)}
```
**Edge cases:** `users` se acota por `business_id` (REQ-17: solo usuarios del negocio). Hijos sin business_id (`movement_allocations`, `sales_document_items`) se traen completos (simplificación v1 documentada en design §2.9 dim2).

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.service import SyncService; print(hasattr(SyncService,'build_pull'))"
# Expected: True
```
#### Expected Report
- [ ] `service.py` — `_dump` + `build_pull` añadidos · imprime `True`

---

### T10: Router de sync (`/sync/register`, `/sync/push`, `/sync/conflicts`)

**Depends on:** T3, T4, T6, T9 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/routers/sync.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/deps.py — session_dep()
# app/security.py — require_admin_or_owner_api(...) -> User ; require_active_business_id(db, request) -> int
# app/models.py — DeviceToken, Location, SyncConflict
# app/sync/auth.py — require_device(request, db) -> DeviceToken
# app/sync/service.py — SyncService(db, device); .push(app_schema_version, events) -> {"applied_event_ids":[...]}; .build_pull() -> {snapshot, server_time}
# app/sync/schemas.py — RegisterRequest/Response, PushRequest/Response, ConflictOut, SyncPullData
```
#### Objective
Crear el router con registro, push y lectura de conflictos.

#### Instructions
**Step 1: Create** `app/routers/sync.py`:
```python
from __future__ import annotations

import json
import secrets

from fastapi import APIRouter, Depends, Request
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import DeviceToken, Location, SyncConflict, User
from app.security import require_admin_or_owner_api
from app.sync.auth import require_device
from app.sync.schemas import (
    ConflictOut, PushRequest, PushResponse, RegisterRequest, RegisterResponse, SyncPullData,
)
from app.sync.service import SyncService

router = APIRouter(tags=["sync"])


@router.post("/sync/register", response_model=RegisterResponse)
def register_device(
    payload: RegisterRequest,
    user: User = Depends(require_admin_or_owner_api),
    db: Session = Depends(session_dep),
) -> RegisterResponse:
    loc = db.scalar(select(Location).where(
        Location.business_id == payload.business_id, Location.code == payload.location_code
    ))
    token = secrets.token_urlsafe(32)
    device = DeviceToken(
        token=token, business_id=int(payload.business_id),
        location_id=int(loc.id) if loc is not None else None, label=payload.label,
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
    from app.security import require_active_business_id
    bid = require_active_business_id(db, request)
    rows = db.scalars(select(SyncConflict).where(
        SyncConflict.business_id == bid, SyncConflict.resolved == False  # noqa: E712
    ))
    return [ConflictOut(event_id=c.event_id, kind=c.kind, detail=c.detail, created_at=c.created_at) for c in rows]
```
**Edge cases:** register sin ubicación encontrada → `location_id=None` (no falla); push valida versión dentro del service.

#### Verification
```bash
python3 -c "from app.routers.sync import router; print(len(router.routes))"
# Expected: 3
```
#### Expected Report
- [ ] `app/routers/sync.py` creado con 3 rutas · imprime `3`

---

### T11: Registrar `sync_router` en main.py

**Depends on:** T10 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/main.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/main.py:24-27 — from app.routers.health import router as health_router  (etc.)
# app/main.py:170-173 — app.include_router(health_router) ; ... ; app.include_router(ui_router)
```
#### Objective
Importar y registrar el router de sync (API JSON, sin prefix).

#### Instructions
**Step 1:** `Read app/main.py`
**Step 2:** Junto a los imports de routers (línea ~26), añadir:
```python
from app.routers.sync import router as sync_router
```
**Step 3:** Junto a los `include_router` (tras `app.include_router(inventory_router)`, línea ~172), añadir:
```python
app.include_router(sync_router)
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.main import app; print(any(r.path.startswith('/sync') for r in app.routes))"
# Expected: True
```
#### Expected Report
- [ ] `app/main.py` — import + include_router de sync · imprime `True`

---

### T12: Tests de `SyncService`

**Depends on:** T7, T8, T9 · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/unit/test_sync_service.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# conftest: db_session (crea Business code="test"), business_id -> int
# app/models.py — DeviceToken, Product, InventoryMovement, InventoryLot, Location, Customer
# app/sync/service.py — SyncService(db, device); push(version, events); MIN_APP_SCHEMA_VERSION=6
# Para una venta con stock hay que sembrar producto + lote + ubicación (ver test_inventory_service_unit.py como referencia de fixtures de stock).
```
#### Objective
Tests de idempotencia, rechazo por versión y dedup de clientes (no requieren stock complejo).

#### Instructions
**Step 1: Create** `tests/unit/test_sync_service.py`:
```python
from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.models import Customer, DeviceToken
from app.sync.service import SyncService


def _device(db, business_id) -> DeviceToken:
    d = DeviceToken(token="t-test", business_id=business_id, label="POS test")
    db.add(d); db.commit(); db.refresh(d)
    return d


def test_version_too_old_raises_409(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    with pytest.raises(HTTPException) as ei:
        svc.push(app_schema_version=1, events=[])
    assert ei.value.status_code == 409


def test_customer_created_when_no_clash(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e1", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-1", "name": "Ana", "phone": "600111222"}}
    res = svc.push(6, [ev])
    assert "e1" in res["applied_event_ids"]
    assert db_session.query(Customer).filter(Customer.client_id == "C-1").count() == 1


def test_customer_dedup_keeps_first_by_contact(db_session, business_id):
    db_session.add(Customer(business_id=business_id, client_id="C-1", name="Ana", phone="600111222"))
    db_session.commit()
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e2", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-2", "name": "Ana B", "phone": "600111222"}}
    svc.push(6, [ev])
    # No se crea un segundo cliente: se conserva el primero por contacto
    assert db_session.query(Customer).filter(Customer.phone == "600111222").count() == 1


def test_idempotent_replay_does_not_duplicate(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e3", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-3", "name": "Leo", "email": "leo@x.com"}}
    svc.push(6, [ev])
    svc.push(6, [ev])  # reenvío del mismo event_id
    assert db_session.query(Customer).filter(Customer.client_id == "C-3").count() == 1
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sync_service.py -v
# Expected: 4 passed
```
#### Expected Report
- [ ] `tests/unit/test_sync_service.py` — 4 tests PASS

---

### T13: Tests del router de sync

**Depends on:** T10, T11 · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/unit/test_sync_router.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# Patrón de test de router: TestClient(app). Ver tests/unit/test_routers_api.py para login/sesión.
# /sync/push exige header X-Sync-Token → sin él, 401.
# app/models.py — DeviceToken (se puede insertar directo en la BD de test para simular registro).
```
#### Objective
Tests del router: push sin token → 401; push con token y versión vieja → 409.

#### Instructions
**Step 1: Create** `tests/unit/test_sync_router.py`:
```python
from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app
from app.db import get_session
from app.models import Business, DeviceToken


def _seed_device() -> str:
    db = get_session()
    try:
        biz = db.query(Business).first()
        if biz is None:
            biz = Business(code="test", name="Test"); db.add(biz); db.commit(); db.refresh(biz)
        d = DeviceToken(token="tok-abc", business_id=int(biz.id), label="POS")
        db.add(d); db.commit()
        return "tok-abc"
    finally:
        db.close()


def test_push_without_token_returns_401():
    client = TestClient(app)
    r = client.post("/sync/push", json={"app_schema_version": 6, "events": []})
    assert r.status_code == 401


def test_push_old_version_returns_409():
    token = _seed_device()
    client = TestClient(app)
    r = client.post("/sync/push", json={"app_schema_version": 1, "events": []},
                    headers={"X-Sync-Token": token})
    assert r.status_code == 409
```
**Edge cases:** este test usa la BD real de `get_session` (no la fixture in-memory) — igual que `test_routers_api.py`. Si el runner usa `DATABASE_URL=:memory:`, `app` y `get_session` comparten el engine del proceso.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sync_router.py -v
# Expected: 2 passed
```
#### Expected Report
- [ ] `tests/unit/test_sync_router.py` — 2 tests PASS

---

## Checklist de los 7 mandamientos (Fase 3)

- [x] Claude Code piensa, OpenCode escribe (before/after exactos)
- [x] ≤ 800 tokens por envelope (T10 es el mayor; dentro de presupuesto)
- [x] Before/after, nunca descripciones
- [x] Máx 2 archivos por envelope
- [x] Verificación mecánica (comando → PASS/FAIL)
- [x] Dependencias explícitas con firmas reales
- [x] Batches de 3-4 (T1-T3 · T4-T6 · T7-T9 · T10-T13)

> **Nota de version bump:** el bump a v0.4.7 lo hace P1 (T9). P2 sigue en v0.4.7 (mismo ciclo
> de release del épico); no se vuelve a bumpear por slice salvo que el usuario lo pida.
