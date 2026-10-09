# Task Envelopes — P5: e2e multi-nodo + endurecimiento

- **Slice:** P5 (`offline-desktop-sync`) · **Rama base:** `develop` · **Depende de:** P1-P4 (verdes)
- **Insumo:** `design.md` · **REQs:** 20 + verificación integral
- **Test runner unit:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest`
- **Test runner e2e:** `python3 -m pytest tests/e2e -m e2e --no-cov`
- **Batches:** T1-T3 · T4-T5

> Mapeo: REQ-20→T2,T3 · endurecimiento→T1 · verificación integral→T4 · doc viva→T5.

---

### T1: Single-source de la versión de protocolo (endurecimiento)

**Depends on:** none · **Skill:** backend-developer · **Agent:** refactor
**Files:** `app/sync/client.py` · **Pillars:** P1, P5, P6

#### Context
```python
# app/sync/client.py — dentro de sync():
#   resp = httpx.post(f"{...}/sync/push", json={"app_schema_version": 7, "events": events}, ...)
# app/migrations.py — _LATEST_SCHEMA_VERSION  (= 7 tras P3)
```
#### Objective
Enviar la versión de esquema real en vez del literal `7`.

#### Instructions
**Step 1:** `Read app/sync/client.py`
**Step 2:** Add import (junto a los del módulo):
```python
from app.migrations import _LATEST_SCHEMA_VERSION
```
**Step 3:** En `sync()`, reemplazar:
```python
# BEFORE:
            json={"app_schema_version": 7, "events": events},
# AFTER:
            json={"app_schema_version": _LATEST_SCHEMA_VERSION, "events": events},
```
**Edge cases:** el server (`MIN_APP_SCHEMA_VERSION` en service.py) sigue siendo el piso de
compatibilidad; enviar el valor real evita drift al próximo bump.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.client import sync; import app.migrations as m; print(m._LATEST_SCHEMA_VERSION)"
# Expected: 7
```
#### Expected Report
- [ ] `client.py` — import + `app_schema_version` single-sourced · imprime `7`

---

### T2: Fixture `central_server` (base_url + db_path)

**Depends on:** none · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/e2e/conftest.py` · **Pillars:** P1, P5, P6

#### Context
```python
# tests/e2e/conftest.py — ya define _free_port(), _wait_ready(base_url), _seed_stock(db_path),
#   y @pytest.fixture(scope="function") def live_server() -> str: (crea db_path, arranca uvicorn
#   subproceso con DATABASE_URL=sqlite file, _seed_stock, yield base_url).
# NO modificar live_server. Añadir una fixture nueva que replique su arranque y exponga db_path.
```
#### Objective
Añadir una fixture `central_server` que devuelva `(base_url, db_path)`.

#### Instructions
**Step 1:** `Read tests/e2e/conftest.py` (copiar el patrón EXACTO de arranque de `live_server`:
tmpdir, env `DATABASE_URL`/`SESSION_SECRET`/`ADMIN_*`/`OPERATOR_*`, `subprocess.Popen`,
`_wait_ready`, `_seed_stock`, y el teardown que mata el proceso y borra el tmp).
**Step 2:** Añadir al final del archivo una fixture que haga lo mismo que `live_server` pero
`yield (base_url, db_path)` en vez de solo `base_url`. Reutilizar `_free_port`, `_wait_ready`,
`_seed_stock`. Mantener `scope="function"` y el mismo teardown (terminar proceso + `shutil.rmtree`).

**Edge cases:** no alterar `live_server` (otros e2e la usan). La nueva fixture es independiente.

#### Verification
```bash
python3 -c "import ast; ast.parse(open('tests/e2e/conftest.py').read()); print('syntax ok')"
# Expected: syntax ok
```
#### Expected Report
- [ ] `tests/e2e/conftest.py` — fixture `central_server` añadida (sin tocar `live_server`)

---

### T3: e2e multi-nodo con conflicto (REQ-20)

**Depends on:** T2 · **Skill:** test-automation-engineer · **Agent:** coder
**Files:** `tests/e2e/test_sync_multinode_e2e.py` (CREATE) · **Pillars:** P1, P5, P6

#### Context
```python
# central_server fixture -> (base_url, db_path). Central sembrado: business 'recambios',
#   central location, E2E-SKU1 qty 10.
# HTTP: POST {base_url}/sync/push  headers {"X-Sync-Token": token}
#   body {"app_schema_version": <int>, "events": [ {event_id, event_type, payload, seq, event_hash} ]}
#   payload venta: {"sku","quantity","location_code"?}  payload cliente: {"client_id","name","phone"}
# GET {base_url}/sync/conflicts NO (requiere sesión); aseverar por db_path.
# Sembrar tokens + aseverar abriendo db_path con create_engine(sqlite file).
# app/models.py — DeviceToken, InventoryMovement, Customer, SyncConflict, Business, Location
```
#### Objective
Test e2e: dos nodos sincronizan contra el central con un conflicto; verificar convergencia.

#### Instructions
**Step 1: Create** `tests/e2e/test_sync_multinode_e2e.py`:
```python
from __future__ import annotations

import httpx
import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import Business, Customer, DeviceToken, InventoryMovement, Location, SyncConflict

pytestmark = pytest.mark.e2e


def _session(db_path: str):
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    return sessionmaker(bind=engine)(), engine


def _seed_tokens(db_path: str) -> tuple[str, int, str]:
    db, engine = _session(db_path)
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        central = db.scalar(select(Location).where(Location.business_id == biz.id, Location.role == "central"))
        a = DeviceToken(token="tok-A", business_id=int(biz.id), location_id=int(central.id), label="Nodo A")
        b = DeviceToken(token="tok-B", business_id=int(biz.id), location_id=int(central.id), label="Nodo B")
        db.add_all([a, b]); db.commit()
        return "tok-A", int(biz.id), central.code
    finally:
        db.close(); engine.dispose()


def _push(base_url: str, token: str, events: list[dict]) -> dict:
    r = httpx.post(f"{base_url}/sync/push", headers={"X-Sync-Token": token},
                   json={"app_schema_version": 7, "events": events}, timeout=30.0)
    assert r.status_code == 200, r.text
    return r.json()


def test_two_nodes_sync_with_conflict(central_server):
    base_url, db_path = central_server
    _seed_tokens(db_path)
    central_code = None
    db, engine = _session(db_path)
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        central = db.scalar(select(Location).where(Location.business_id == biz.id, Location.role == "central"))
        central_code = central.code
    finally:
        db.close(); engine.dispose()

    # Nodo A: venta válida + cliente
    _push(base_url, "tok-A", [
        {"event_id": "A1", "event_type": "sale", "seq": 1, "event_hash": "h1",
         "payload": {"sku": "E2E-SKU1", "quantity": 2, "location_code": central_code}},
        {"event_id": "A2", "event_type": "customer", "seq": 2, "event_hash": "h2",
         "payload": {"client_id": "C-A", "name": "Ana", "phone": "600X"}},
    ])
    # Nodo B: cliente duplicado (mismo phone) + venta sin stock
    _push(base_url, "tok-B", [
        {"event_id": "B1", "event_type": "customer", "seq": 1, "event_hash": "h3",
         "payload": {"client_id": "C-B", "name": "Ana B", "phone": "600X"}},
        {"event_id": "B2", "event_type": "sale", "seq": 2, "event_hash": "h4",
         "payload": {"sku": "E2E-SKU1", "quantity": 999, "location_code": central_code}},
    ])
    # Idempotencia: reenviar A no duplica
    _push(base_url, "tok-A", [
        {"event_id": "A1", "event_type": "sale", "seq": 1, "event_hash": "h1",
         "payload": {"sku": "E2E-SKU1", "quantity": 2, "location_code": central_code}},
    ])

    db, engine = _session(db_path)
    try:
        sales = db.scalars(select(InventoryMovement).where(InventoryMovement.type == "sale")).all()
        assert len(sales) == 1  # solo la venta de A; la de 999 fue conflicto; reenvío no duplicó
        custs = db.scalars(select(Customer).where(Customer.phone == "600X")).all()
        assert len(custs) == 1  # dedup conservó el primero
        conflicts = db.scalars(select(SyncConflict).where(SyncConflict.kind == "sale_stock")).all()
        assert len(conflicts) == 1  # la venta sin stock
    finally:
        db.close(); engine.dispose()
```
**Edge cases:** el `event_hash` es dummy (el server usa `event_id` para idempotencia; la cadena
se valida en el cliente/P1). La venta de 999 excede el stock (10) → conflicto, no se aplica.

#### Verification
```bash
python3 -m pytest tests/e2e/test_sync_multinode_e2e.py -m e2e --no-cov -v
# Expected: 1 passed
```
#### Expected Report
- [ ] `tests/e2e/test_sync_multinode_e2e.py` creado · e2e PASS (convergencia + dedup + conflicto + idempotencia)

---

### T4: Script de verificación integral del épico

**Depends on:** T3 · **Skill:** devops-engineer · **Agent:** coder
**Files:** `specs/offline-desktop-sync/verify.sh` (CREATE) · **Pillars:** P1, P6

#### Objective
Crear el script que verifica todo el épico (unit + e2e de sync + scope).

#### Instructions
**Step 1: Create** `specs/offline-desktop-sync/verify.sh`:
```bash
#!/bin/bash
# Verificación integral del épico offline-desktop-sync (P1-P5).
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:

echo "=== Suite unit completa ==="
python3 -m pytest tests/unit -q

echo "=== Unit específicos del épico ==="
python3 -m pytest \
  tests/unit/test_sku_range.py tests/unit/test_sync_outbox.py \
  tests/unit/test_sync_service.py tests/unit/test_sync_router.py \
  tests/unit/test_sync_client.py tests/unit/test_desktop_keyring.py -v

echo "=== e2e multi-nodo ==="
python3 -m pytest tests/e2e/test_sync_multinode_e2e.py -m e2e --no-cov -v

echo "=== Endpoints de sync registrados ==="
python3 -c "from app.main import app; assert any(r.path.startswith('/sync') for r in app.routes); assert any(r.path=='/ui/sync' for r in app.routes)"

echo "=== Schema version ==="
python3 -c "import app.migrations as m; assert m._LATEST_SCHEMA_VERSION >= 7, m._LATEST_SCHEMA_VERSION"

echo "=== TODO VERDE (build Windows: manual, ver packaging/README-desktop.md) ==="
```
**Step 2:** `chmod +x specs/offline-desktop-sync/verify.sh` (o documentar `bash verify.sh`).

#### Verification
```bash
bash specs/offline-desktop-sync/verify.sh
# Expected: termina con "TODO VERDE"
```
#### Expected Report
- [ ] `specs/offline-desktop-sync/verify.sh` creado · corre verde de punta a punta

---

### T5: Documentación viva (CLAUDE.md)

**Depends on:** T4 · **Skill:** technical-writer · **Agent:** docs
**Files:** `CLAUDE.md` · **Pillars:** P1

#### Context
```markdown
# CLAUDE.md — sección "## Estado actual" (checklist [x]/[ ]) y "## Sesiones".
# Versión actual documentada: 0.4.6 → pasa a 0.4.7 (bump en P1).
```
#### Objective
Registrar el épico offline-desktop-sync en el `CLAUDE.md` raíz.

#### Instructions
**Step 1:** `Read CLAUDE.md`
**Step 2:** En "## Resumen del proyecto", actualizar `Versión actual: 0.4.6` → `0.4.7`.
**Step 3:** En "## Estado actual", añadir una entrada:
```markdown
- [x] **App de escritorio offline-first + sync bidireccional** (épico `offline-desktop-sync`,
      5 slices P1-P5, v0.4.7) — nodo Windows (pywebview) con el mismo backend contra SQLite
      local cifrado (SQLCipher + DPAPI); escritura offline de venta + alta de cliente vía outbox
      con hash encadenado; sync manual (token por dispositivo, replay idempotente vía services,
      dedup de clientes, pull foto completa); banner de última sync; e2e multi-nodo. Specs en
      `specs/offline-desktop-sync/` (requirements + P1..P5). Build Windows: `packaging/`.
```
**Step 4:** En "## Sesiones", añadir una línea con fecha 2026-08-28 resumiendo la generación del
épico (5 PAEs vía SDD-Agent, pendiente de ejecución con OpenCode).

**Edge cases:** no tocar otras secciones; mantener el estilo de checklist existente.

#### Verification
```bash
grep -q "offline-desktop-sync" CLAUDE.md && grep -q "0.4.7" CLAUDE.md && echo "doc ok"
# Expected: doc ok
```
#### Expected Report
- [ ] `CLAUDE.md` — versión 0.4.7 + entrada del épico + línea de sesión · `doc ok`

---

## Checklist de los 7 mandamientos (Fase 3)
- [x] Before/after exactos · ≤800 tokens · ≤2 archivos · verificación mecánica
- [x] Dependencias con firmas reales · batches de 3 (T1-T3 · T4-T5)

> **Cierre del épico.** Con P5 verde, los 5 slices están completos. Version del épico: v0.4.7
> (PATCH, bump en P1). El build del instalador Windows se valida manualmente (P4/README).
