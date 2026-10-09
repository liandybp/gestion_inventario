# Task Envelopes — P3: Cliente de sync + escritura offline

- **Slice:** P3 (`offline-desktop-sync`) · **Rama base:** `develop` · **Depende de:** P1, P2 (verdes)
- **Insumo:** `design.md` · **REQs:** 5, 6, 7, 8, 17, 18, 19 (+ REQ-1 APP_MODE)
- **Test runner:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest`
- **Batches:** T1-T3 · T4-T6 · T7-T9 · T10-T12

> Mapeo: REQ-1→T1 · REQ-5→T4 · REQ-6→T5 · REQ-18→T4,T5 · REQ-17→T7 · REQ-8→T10,T11 · REQ-19→T11 · sync→T6,T7,T8,T9.

---

### T1: Setting `APP_MODE` + `is_desktop()`

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/settings.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/settings.py — def env(name, default) -> str ; ya define DATABASE_URL, HOST, PORT, etc.
```
#### Objective
Añadir `APP_MODE` y el helper `is_desktop()`.

#### Instructions
**Step 1:** `Read app/settings.py`
**Step 2:** Añadir al final del archivo:
```python
# ── Modo de ejecución: "server" (central) | "desktop" (nodo offline) ──
APP_MODE = env("APP_MODE", "server")


def is_desktop() -> bool:
    return APP_MODE.strip().lower() == "desktop"
```
#### Verification
```bash
APP_MODE=desktop python3 -c "from app.settings import is_desktop; print(is_desktop())"
python3 -c "from app.settings import is_desktop; print(is_desktop())"
# Expected: True  luego  False
```
#### Expected Report
- [ ] `app/settings.py` — `APP_MODE` + `is_desktop()` añadidos · verificación True/False

---

### T2: Modelo `SyncState`

**Depends on:** none · **Skill:** data-engineer · **Agent:** data
**Files:** `app/models.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/models.py — imports ya presentes (datetime, Optional, DateTime, Integer, String, func, Mapped, mapped_column, Base)
```
#### Objective
Añadir `SyncState` (fila única) al final de `app/models.py`.

#### Instructions
**Step 1:** `Read app/models.py`
**Step 2:** Append:
```python
class SyncState(Base):
    """Estado local de sincronización del nodo desktop (fila única, id=1)."""

    __tablename__ = "sync_state"

    id: Mapped[int] = mapped_column(primary_key=True)
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    server_url: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    device_token: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.models import SyncState; print(SyncState.__tablename__)"
# Expected: sync_state
```
#### Expected Report
- [ ] `app/models.py` — `SyncState` añadido · imprime `sync_state`

---

### T3: Migración v7 (`sync_state`)

**Depends on:** T2 · **Skill:** data-engineer · **Agent:** data
**Files:** `app/migrations.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/migrations.py — _LATEST_SCHEMA_VERSION = 6 (tras P2). Ambas ramas terminan con `if schema_version < 1:`.
```
#### Objective
Crear `sync_state` en ambas ramas y subir versión a 7.

#### Instructions
**Step 1:** `Read app/migrations.py`
**Step 2:** `_LATEST_SCHEMA_VERSION = 6` → `_LATEST_SCHEMA_VERSION = 7`
**Step 3:** En `_run_sqlite_schema_migrations`, antes de `if schema_version < 1:`:
```python
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS sync_state ("
            "id INTEGER PRIMARY KEY, last_synced_at TIMESTAMP, server_url VARCHAR(255), "
            "device_token VARCHAR(128), updated_at TIMESTAMP)"
        )
```
**Step 4:** En `_run_postgresql_schema_migrations`, antes de `if schema_version < 1:`:
```python
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS sync_state ("
            "id INTEGER PRIMARY KEY, last_synced_at TIMESTAMPTZ, server_url VARCHAR(255), "
            "device_token VARCHAR(128), updated_at TIMESTAMPTZ)"
        )
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "import app.migrations as m; print(m._LATEST_SCHEMA_VERSION)"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
# Expected: 7 ; suite PASS
```
#### Expected Report
- [ ] `_LATEST_SCHEMA_VERSION = 7` + tabla en ambas ramas · suite PASS

---

### T4: Hook de outbox en venta (desktop)

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/routers/ui_sales.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/routers/ui_sales.py:192 (sale) y :272 (sale_barcode):
#   result = service.sale(SaleCreate(sku=sku, quantity=quantity, unit_price=..., movement_date=..., note=..., location_code=selected_location_code))
# bid = require_active_business_id(db, request) ya en scope; `sku`, `quantity`, `unit_price`, `movement_date`, `note`, `selected_location_code`/location_code en scope.
# app/sync/outbox.py — append_event(db, *, business_id, event_type, payload, location_id=None) (P1)
# app/settings.py — is_desktop() (T1)
```
#### Objective
Tras una venta exitosa, encolar un evento en el outbox si el nodo es desktop.

#### Instructions
**Step 1:** `Read app/routers/ui_sales.py`
**Step 2:** Add imports (junto a los del módulo):
```python
from app.settings import is_desktop
from app.sync.outbox import append_event
```
**Step 3:** En `sale`, JUSTO DESPUÉS de `result = service.sale(SaleCreate(...))` (antes del
`return`), añadir:
```python
        if is_desktop():
            append_event(
                db, business_id=bid, event_type="sale",
                payload={"sku": sku, "quantity": quantity,
                         "unit_price": parse_optional_float(unit_price),
                         "movement_date": movement_date, "note": note or None,
                         "location_code": selected_location_code},
            )
            db.commit()
```
**Step 4:** En `sale_barcode`, JUSTO DESPUÉS de su `result = service.sale(SaleCreate(...))`,
añadir el mismo bloque adaptando los nombres de variable reales de ese endpoint (usar `sku`,
`quantity`, `unit_price`, `movement_date`, `note`, y el `location_code` que ese endpoint pase a
`SaleCreate`).

**Edge cases:** en modo server `is_desktop()` es False → no-op (comportamiento actual intacto).
`movement_date` se guarda como el string del form (JSON-serializable); el server lo parsea.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_sales_context.py -q
# Expected: PASS (sin regresión)
```
#### Expected Report
- [ ] `ui_sales.py` — imports + hook en `sale` y `sale_barcode` · tests PASS

---

### T5: Hook de outbox en alta de cliente (desktop)

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/routers/ui_customers.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/routers/ui_customers.py:45 customer_create — tras CustomerRepository(db, bid).create(...) y db.commit()
#   variables en scope: bid, client_id, name, address, phone, email
# app/sync/outbox.py — append_event ; app/settings.py — is_desktop()
```
#### Objective
Tras crear un cliente, encolar un evento en el outbox si el nodo es desktop.

#### Instructions
**Step 1:** `Read app/routers/ui_customers.py`
**Step 2:** Add imports:
```python
from app.settings import is_desktop
from app.sync.outbox import append_event
```
**Step 3:** JUSTO DESPUÉS del `db.commit()` que persiste el cliente (dentro del try, tras crear),
añadir:
```python
    if is_desktop():
        append_event(
            db, business_id=bid, event_type="customer",
            payload={"client_id": client_id, "name": name,
                     "address": (address or "").strip() or None,
                     "phone": (phone or "").strip() or None,
                     "email": (email or "").strip() or None},
        )
        db.commit()
```
**Edge cases:** modo server → no-op. El evento se encola solo si la creación fue exitosa.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
# Expected: PASS
```
#### Expected Report
- [ ] `ui_customers.py` — imports + hook tras alta · suite PASS

---

### T6: Cliente de sync — transporte (push)

**Depends on:** T2 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/client.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/models.py — SyncState(id=1, server_url, device_token, last_synced_at), SyncEvent(status, seq, event_id, event_type, payload, prev_hash, event_hash, location_id)
# httpx (requirements). app/logger.py — get_logger(__name__)
# El outbox guarda payload como str JSON; para /sync/push se manda como dict → json.loads(payload).
```
#### Objective
Crear `client.py` con `_pending_events`, `_load_state` y `sync()` (push; apply_pull llega en T7).

#### Instructions
**Step 1: Create** `app/sync/client.py`:
```python
from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.logger import get_logger
from app.models import SyncEvent, SyncState

_log = get_logger(__name__)


def _load_state(db: Session) -> SyncState | None:
    return db.get(SyncState, 1)


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


def sync(db: Session) -> dict:
    """Push de pendientes + apply_pull + last_synced_at."""
    state = _load_state(db)
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
    for e in db.scalars(select(SyncEvent).where(SyncEvent.status == "pending")):
        if e.event_id in applied:
            e.status = "synced"
    apply_pull(db, data.get("pull", {}).get("snapshot", {}))
    state.last_synced_at = datetime.now(timezone.utc)
    db.commit()
    return {"ok": True, "applied": len(applied), "conflicts": len(data.get("conflicts", []))}
```
**Edge cases:** nodo sin aprovisionar → `{"ok": False}`; sin red → `{"ok": False}` con mensaje;
solo se marcan `synced` los `event_id` confirmados por el servidor.

#### Verification
```bash
python3 -c "from app.sync.client import sync, _pending_events; print('ok')"
# Expected: ok  (apply_pull se añade en T7; el import fallará hasta entonces si se ejecuta sync — el import del módulo debe funcionar)
```
> Nota: `apply_pull` se referencia pero se define en T7 (misma clase de módulo). El import del
> módulo funciona; `sync()` no se ejecuta hasta T7. No correr `sync()` en este envelope.

#### Expected Report
- [ ] `app/sync/client.py` creado con `_load_state`, `_pending_events`, `sync`

---

### T7: Cliente de sync — `apply_pull` transaccional

**Depends on:** T6 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/sync/client.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/sync/client.py — sync() ya llama apply_pull(db, snapshot) (T6)
# app/models.py — Business, Location, Product, Customer, InventoryMovement, InventoryLot,
#   MovementAllocation, SalesDocument, SalesDocumentItem, OperatingExpense, MoneyExtraction,
#   CashBaseline, User. El snapshot usa los nombres de tabla de P2 (build_pull).
```
#### Objective
Añadir `apply_pull` (reemplazo transaccional de las tablas de lectura).

#### Instructions
**Step 1:** `Read app/sync/client.py`
**Step 2:** Add imports:
```python
from app.models import (
    Business, CashBaseline, Customer, InventoryLot, InventoryMovement, Location,
    MovementAllocation, MoneyExtraction, OperatingExpense, Product, SalesDocument,
    SalesDocumentItem, User,
)
```
**Step 3:** Añadir la función:
```python
_PULL_MODELS = {
    "businesses": Business, "locations": Location, "products": Product,
    "customers": Customer, "inventory_movements": InventoryMovement,
    "inventory_lots": InventoryLot, "movement_allocations": MovementAllocation,
    "sales_documents": SalesDocument, "sales_document_items": SalesDocumentItem,
    "operating_expenses": OperatingExpense, "money_extractions": MoneyExtraction,
    "cash_baselines": CashBaseline, "users": User,
}


def apply_pull(db: Session, snapshot: dict) -> None:
    """Reemplazo transaccional de las tablas de lectura con las filas del snapshot.

    NO toca pending_sync_events ni sync_state (estado local del nodo).
    """
    try:
        for table, model in _PULL_MODELS.items():
            rows = snapshot.get(table)
            if rows is None:
                continue
            db.query(model).delete()
            for row in rows:
                cols = {c.name for c in model.__table__.columns}
                db.add(model(**{k: v for k, v in row.items() if k in cols}))
        db.flush()
    except Exception:  # noqa: BLE001
        db.rollback()
        _log.error("apply_pull falló; rollback", exc_info=True)
        raise
```
**Edge cases:** tabla ausente en el snapshot → se salta (no borra); error → rollback completo
(nunca deja tablas de lectura a medias). No borra el outbox ni `sync_state`.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.client import apply_pull, sync; print('ok')"
# Expected: ok
```
#### Expected Report
- [ ] `client.py` — `_PULL_MODELS` + `apply_pull` añadidos · import `ok`

---

### T8: Router `ui_sync` + partial

**Depends on:** T6, T7 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/routers/ui_sync.py` (CREATE), `app/templates/partials/sync_status.html` (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context
```python
# Patrón de router UI: APIRouter(prefix... via main), templates = Jinja2Templates(...) — ver otros ui_*.py.
# app/deps.py — session_dep ; app/security.py — get_current_user_from_session(db, request)
# app/sync/client.py — sync(db) -> dict ; app/models.py — SyncState ; app/settings.py — is_desktop()
# Los ui_*.py usan: from app.routers.ui_common import templates  (verificar el import real de templates en un ui_*.py)
```
#### Objective
Crear el router con `POST /sync` y `GET /sync/status`, y el partial del banner.

#### Instructions
**Step 1:** `Read app/routers/ui_customers.py` (para copiar el import real de `templates` y el patrón `router = APIRouter(...)`).
**Step 2: Create** `app/routers/ui_sync.py`:
```python
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import SyncState
from app.security import get_current_user_from_session
from app.settings import is_desktop
from app.sync.client import sync as run_sync
from app.routers.ui_common import templates

router = APIRouter(tags=["ui-sync"])


def _state(db: Session) -> SyncState | None:
    return db.get(SyncState, 1)


@router.post("/sync", response_class=HTMLResponse)
def do_sync(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    if not is_desktop():
        raise HTTPException(status_code=404, detail="No disponible en modo servidor")
    result = run_sync(db)
    return templates.TemplateResponse(
        request=request, name="partials/sync_status.html",
        context={"result": result, "state": _state(db), "app_mode": "desktop"},
    )


@router.get("/sync/status", response_class=HTMLResponse)
def sync_status(request: Request, db: Session = Depends(session_dep)) -> HTMLResponse:
    _ = get_current_user_from_session(db, request)
    return templates.TemplateResponse(
        request=request, name="partials/sync_status.html",
        context={"result": None, "state": _state(db), "app_mode": "desktop" if is_desktop() else "server"},
    )
```
**Step 3: Create** `app/templates/partials/sync_status.html`:
```html
<div id="sync-banner" class="sync-banner">
  {% if state and state.last_synced_at %}
    <span>Última sincronización: {{ state.last_synced_at.strftime('%Y-%m-%d %H:%M') }}</span>
  {% else %}
    <span class="warn">Nunca sincronizado</span>
  {% endif %}
  {% if app_mode == 'desktop' %}
    <button hx-post="/ui/sync" hx-target="#sync-banner" hx-swap="outerHTML">Sincronizar</button>
  {% endif %}
  {% if result %}
    <span class="{{ 'ok' if result.ok else 'error' }}">
      {% if result.ok %}Aplicados: {{ result.applied }} · Conflictos: {{ result.conflicts }}{% else %}{{ result.error }}{% endif %}
    </span>
  {% endif %}
</div>
```
**Edge cases:** `is_desktop()` False → `POST /ui/sync` da 404; el botón solo aparece en desktop.

#### Verification
```bash
python3 -c "from app.routers.ui_sync import router; print(len(router.routes))"
# Expected: 2
```
#### Expected Report
- [ ] `ui_sync.py` (2 rutas) + `partials/sync_status.html` creados · imprime `2`

---

### T9: Registrar `ui_sync` en main.py

**Depends on:** T8 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/main.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/main.py — app.include_router(ui_sales.router, prefix="/ui")  (patrón de routers UI)
# los ui_* se importan en el bloque `from app.routers import (...)` (líneas 28-...)
```
#### Objective
Importar y registrar `ui_sync` con prefix `/ui`.

#### Instructions
**Step 1:** `Read app/main.py`
**Step 2:** Añadir `ui_sync` al bloque de import `from app.routers import (...)`.
**Step 3:** Junto a los `include_router(..., prefix="/ui")`, añadir:
```python
app.include_router(ui_sync.router, prefix="/ui")
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.main import app; print(any(r.path=='/ui/sync' for r in app.routes))"
# Expected: True
```
#### Expected Report
- [ ] `app/main.py` — import + include_router de ui_sync · imprime `True`

---

### T10: Contexto `app_mode` + `last_synced_at` en el dashboard

**Depends on:** T1, T2 · **Skill:** backend-developer · **Agent:** coder
**Files:** `app/routers/ui_home.py` · **Pillars:** P1, P4, P5, P6

#### Context
```python
# app/routers/ui_home.py:184 — return templates.TemplateResponse(request=request, name="dashboard.html",
#   context={"user": user, "businesses": businesses, "active_business_id": active_business_id, "active_business": active_business})
# app/settings.py — is_desktop() ; app/models.py — SyncState
```
#### Objective
Pasar `app_mode` y `last_synced_at` al contexto de `dashboard.html`.

#### Instructions
**Step 1:** `Read app/routers/ui_home.py`
**Step 2:** Add imports (junto a los del módulo):
```python
from app.settings import is_desktop
from app.models import SyncState
```
**Step 3:** En `dashboard`, antes del `return`, añadir:
```python
    _sync_state = db.get(SyncState, 1)
    _last_sync = _sync_state.last_synced_at if _sync_state is not None else None
```
**Step 4:** Añadir dos claves al `context` del `TemplateResponse`:
```python
            "app_mode": "desktop" if is_desktop() else "server",
            "last_synced_at": _last_sync,
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_auth.py -q
# Expected: PASS
```
#### Expected Report
- [ ] `ui_home.py` — imports + contexto `app_mode`/`last_synced_at` · tests PASS

---

### T11: Banner + botón + gate de pestañas offline en `dashboard.html`

**Depends on:** T10 · **Skill:** frontend-developer · **Agent:** coder
**Files:** `app/templates/dashboard.html` · **Pillars:** P1, P4, P5, P6

#### Context
```html
<!-- app/templates/dashboard.html — sidebar con {% set user_role = (user.role or '')|lower %} y
     botones tab-* (tab-purchase, tab-transfers, tab-agent/copiloto). Existe #tab-content.
     Ahora el contexto trae app_mode y last_synced_at (T10). -->
```
#### Objective
Añadir el banner de última sync (persistente) y ocultar compra/traspaso/copiloto en desktop.

#### Instructions
**Step 1:** `Read app/templates/dashboard.html`
**Step 2:** Justo al inicio del contenido principal (antes del `#tab-content` o del `<main>`),
insertar el banner:
```html
    {% if app_mode == 'desktop' %}
    <div id="sync-banner" class="sync-banner">
      {% if last_synced_at %}
        <span>Última sincronización: {{ last_synced_at.strftime('%Y-%m-%d %H:%M') }}</span>
      {% else %}
        <span class="warn">Nunca sincronizado</span>
      {% endif %}
      <button hx-post="/ui/sync" hx-target="#sync-banner" hx-swap="outerHTML">Sincronizar</button>
    </div>
    {% endif %}
```
**Step 3:** Envolver los botones del sidebar de **compra**, **traspaso** y **copiloto/agent**
con un gate de modo. Para CADA uno de esos botones (`tab-purchase`, `tab-transfers`, y el de
copiloto), envolver así:
```html
      {% if app_mode != 'desktop' %}
      <!-- ...botón existente sin cambios... -->
      {% endif %}
```
**Edge cases:** en modo server (`app_mode == 'server'`) no aparece el banner y las pestañas se
muestran como hoy (comportamiento intacto). No tocar el gating por rol existente (RBAC).

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_auth.py tests/unit/test_ui_regressions.py -q
# Expected: PASS (el dashboard sigue renderizando)
```
#### Expected Report
- [ ] `dashboard.html` — banner añadido + 3 pestañas gated a server · tests PASS

---

### T12: Tests del cliente y de los hooks offline

**Depends on:** T4, T5, T7 · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/unit/test_sync_client.py` (CREATE) · **Pillars:** P1, P4, P5, P6

#### Context
```python
# conftest: db_session (Business code="test"), business_id
# app/sync/client.py — _pending_events(db), apply_pull(db, snapshot), sync(db)
# app/sync/outbox.py — append_event(db, *, business_id, event_type, payload)
# app/models.py — SyncEvent, SyncState, Product, Customer
```
#### Objective
Tests de `_pending_events`, `apply_pull` y `sync` sin aprovisionar (rama de error).

#### Instructions
**Step 1: Create** `tests/unit/test_sync_client.py`:
```python
from __future__ import annotations

from app.models import Customer, Product, SyncState
from app.sync.client import _pending_events, apply_pull, sync
from app.sync.outbox import append_event


def test_pending_events_serialized_in_seq_order(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"sku": "A", "quantity": 1})
    append_event(db_session, business_id=business_id, event_type="customer", payload={"client_id": "C1", "name": "Ana"})
    db_session.commit()
    evs = _pending_events(db_session)
    assert [e["seq"] for e in evs] == [1, 2]
    assert evs[0]["payload"]["sku"] == "A"


def test_sync_without_provisioning_returns_error(db_session):
    res = sync(db_session)
    assert res["ok"] is False


def test_apply_pull_replaces_read_tables(db_session, business_id):
    db_session.add(Product(business_id=business_id, sku="OLD", name="Old", min_stock=0,
                           default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0))
    db_session.commit()
    snapshot = {"products": [
        {"id": 999, "business_id": business_id, "sku": "NEW", "name": "New", "min_stock": 0,
         "default_purchase_cost": 1.0, "default_sale_price": 2.0, "lead_time_days": 0}
    ]}
    apply_pull(db_session, snapshot)
    db_session.commit()
    skus = {p.sku for p in db_session.query(Product).all()}
    assert skus == {"NEW"}


def test_apply_pull_does_not_touch_outbox(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"sku": "A", "quantity": 1})
    db_session.commit()
    apply_pull(db_session, {"products": []})
    db_session.commit()
    from app.models import SyncEvent
    assert db_session.query(SyncEvent).count() == 1
```
#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sync_client.py -v
# Expected: 4 passed
```
#### Expected Report
- [ ] `tests/unit/test_sync_client.py` — 4 tests PASS

---

## Checklist de los 7 mandamientos (Fase 3)
- [x] Before/after exactos · ≤800 tokens · ≤2 archivos · verificación mecánica
- [x] Dependencias explícitas con firmas reales · batches de 3

> **Version:** el épico va en v0.4.7 (bump en P1). P3 no re-bumpea.
> **e2e:** el flujo offline→sync end-to-end multi-nodo es P5 (REQ-20). P3 se cubre con unit.
