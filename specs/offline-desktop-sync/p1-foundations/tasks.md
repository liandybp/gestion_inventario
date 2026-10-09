# Task Envelopes — P1: Fundamentos de sincronización

- **Slice:** P1 (`offline-desktop-sync`) · **Rama base:** `develop`
- **Insumo:** `design.md` (mismo directorio) · **REQs:** REQ-9, REQ-15
- **Ejecuta:** OpenCode + Qwen3.6-35B-A3B · **Test runner:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest`
- **Batches:** T1-T3 · T4-T6 · T7-T9 (correr `/compact` entre batches)

> Mapeo REQ↔envelope: REQ-15 → T1,T2,T3,T4 · REQ-9 → T5,T6,T7,T8 · version → T9.

---

### T1: Helper de rango de SKU

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/sku_range.py` (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context

```python
# No hay dependencias externas. Módulo puro (sin BD, sin FastAPI).
# El formato de SKU existente es: prefix "SKU" + número zfill(6). Ej: "SKU000509".
```

#### Objective

Crear el helper puro de numeración de SKU por rango de negocio en `app/services/sku_range.py`.

#### Instructions

**Step 1: Create the file** `app/services/sku_range.py` con EXACTAMENTE este contenido:

```python
from __future__ import annotations

SKU_RANGE_SIZE = 10000


def sku_range_base(business_id: int) -> int:
    """Base inclusiva del rango de SKU reservado a un negocio."""
    return int(business_id) * SKU_RANGE_SIZE


def next_sku_in_range(
    existing_skus: list[str],
    business_id: int,
    prefix: str = "SKU",
    width: int = 6,
) -> str:
    """Siguiente SKU autogenerado dentro del rango [base, base + SKU_RANGE_SIZE).

    Ignora sufijos no numéricos y números fuera del rango. Rango vacío -> base + 1.
    """
    base = sku_range_base(business_id)
    ceiling = base + SKU_RANGE_SIZE
    max_n = base
    for sku in existing_skus:
        s = str(sku or "")
        if not s.startswith(prefix):
            continue
        suffix = s[len(prefix):]
        if suffix.isdigit():
            n = int(suffix)
            if base <= n < ceiling:
                max_n = max(max_n, n)
    return f"{prefix}{str(max_n + 1).zfill(width)}"
```

**Edge cases (ya resueltos):**
- Lista vacía o sin números en rango: devuelve `base + 1` (ej. business_id=2 → "SKU020001").
- Sufijo no numérico ("SKUABC"): se ignora.
- Número fuera de rango (SKU de otro negocio): se ignora.

#### Verification

```bash
python3 -c "from app.services.sku_range import next_sku_in_range; print(next_sku_in_range([], 2))"
# Expected: SKU020001
```

#### Expected Report

- [ ] `app/services/sku_range.py` creado con las 2 funciones
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación imprime `SKU020001`

---

### T2: Tests del helper de rango de SKU

**Depends on:** T1
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_sku_range.py` (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/sku_range.py — YA EXISTE (T1)
SKU_RANGE_SIZE = 10000
def sku_range_base(business_id: int) -> int: ...
def next_sku_in_range(existing_skus: list[str], business_id: int, prefix: str = "SKU", width: int = 6) -> str: ...
```

#### Objective

Crear tests unitarios del helper en `tests/unit/test_sku_range.py`.

#### Instructions

**Step 1: Create the file** `tests/unit/test_sku_range.py` con EXACTAMENTE:

```python
from __future__ import annotations

from app.services.sku_range import SKU_RANGE_SIZE, next_sku_in_range, sku_range_base


def test_base_is_business_times_range_size():
    assert sku_range_base(1) == 10000
    assert sku_range_base(2) == 20000


def test_empty_list_starts_at_base_plus_one():
    assert next_sku_in_range([], 2) == "SKU020001"


def test_continues_from_max_in_range():
    assert next_sku_in_range(["SKU020001", "SKU020005"], 2) == "SKU020006"


def test_ignores_skus_of_other_business_ranges():
    # SKUs del negocio 1 no afectan la numeración del negocio 2
    assert next_sku_in_range(["SKU010500", "SKU000509"], 2) == "SKU020001"


def test_ignores_non_numeric_suffix():
    assert next_sku_in_range(["SKUABC", "SKU020003"], 2) == "SKU020004"


def test_ranges_are_disjoint_between_businesses():
    a = next_sku_in_range(["SKU010001"], 1)
    b = next_sku_in_range(["SKU020001"], 2)
    assert a.startswith("SKU01") and b.startswith("SKU02")
    assert a != b
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sku_range.py -v
# Expected: 6 passed
```

#### Expected Report

- [ ] `tests/unit/test_sku_range.py` creado — 6 tests
- [ ] `pytest tests/unit/test_sku_range.py` — 6 PASS

---

### T3: `ProductService._generate_sku` usa el helper

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/product_service.py:25 — ACTUAL
class ProductService:
    # self._business_id: int  (ya existe)
    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        rows = self._db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
        existing = [str(sku or "") for (sku,) in rows]
        max_n = 0
        for sku in existing:
            suffix = sku[len(prefix):]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"
```

#### Objective

Reemplazar el cuerpo de `_generate_sku` para numerar por rango de negocio vía el helper.

#### Instructions

**Step 1: Read the file**
```
Read app/services/product_service.py
```

**Step 2: Add import** (junto a los imports existentes, tras `from app.schemas import ...`):

```python
from app.services.sku_range import next_sku_in_range
```

**Step 3: Replace `_generate_sku` (líneas 25-33)**

```python
# BEFORE:
    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        rows = self._db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
        existing = [str(sku or "") for (sku,) in rows]
        max_n = 0
        for sku in existing:
            suffix = sku[len(prefix):]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"

# AFTER:
    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        rows = self._db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
        existing = [str(sku or "") for (sku,) in rows]
        return next_sku_in_range(existing, self._business_id, prefix=prefix, width=width)
```

**Edge cases (ya resueltos):** el retry loop de `create()` (5 intentos) se mantiene intacto.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_product_service.py -v
# Expected: todos PASS (los tests existentes siguen verdes)
```

#### Expected Report

- [ ] `app/services/product_service.py` — import añadido + `_generate_sku` reemplazado
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_product_service.py` — PASS

---

### T4: `_generate_auto_sku` (compra) usa el helper

**Depends on:** T1
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_purchases.py:345 — ACTUAL (closure dentro del endpoint POST /ui/purchase)
# `db` y `bid` están en scope. PurchaseRepository(db, bid).list_sku_prefixes(prefix) -> list[str]
    def _generate_auto_sku(prefix: str = "SKU", width: int = 6) -> str:
        rows = PurchaseRepository(db, bid).list_sku_prefixes(prefix)
        existing = [str(sku or "") for sku in rows]
        max_n = 0
        for s in existing:
            suffix = s[len(prefix):]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"
```

#### Objective

Reemplazar el cuerpo del closure `_generate_auto_sku` para usar el helper de rango.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_purchases.py
```

**Step 2: Add import** (junto a los imports existentes del router):

```python
from app.services.sku_range import next_sku_in_range
```

**Step 3: Replace el closure `_generate_auto_sku` (líneas 345-353)**

```python
# BEFORE:
    def _generate_auto_sku(prefix: str = "SKU", width: int = 6) -> str:
        rows = PurchaseRepository(db, bid).list_sku_prefixes(prefix)
        existing = [str(sku or "") for sku in rows]
        max_n = 0
        for s in existing:
            suffix = s[len(prefix):]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"

# AFTER:
    def _generate_auto_sku(prefix: str = "SKU", width: int = 6) -> str:
        rows = PurchaseRepository(db, bid).list_sku_prefixes(prefix)
        existing = [str(sku or "") for sku in rows]
        return next_sku_in_range(existing, bid, prefix=prefix, width=width)
```

**Edge cases (ya resueltos):** el retry loop de 5 intentos que envuelve el closure se mantiene.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_repositories.py tests/unit/test_ui_regressions.py -v
# Expected: todos PASS
```

#### Expected Report

- [ ] `app/routers/ui_purchases.py` — import añadido + closure reemplazado
- [ ] Ningún otro archivo modificado
- [ ] pytest de repositorios/regresiones — PASS

---

### T5: Modelo `SyncEvent` (tabla `pending_sync_events`)

**Depends on:** none
**Skill:** data-engineer
**Agent:** data
**Files:** `app/models.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/models.py — ACTUAL (patrón de modelo con Index en __table_args__)
from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column
from app.db import Base
# Ejemplo de índice existente: __table_args__ = (Index("ix_coach_action_business_rule", ...),)
```

#### Objective

Añadir el modelo `SyncEvent` al final de `app/models.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/models.py
```

**Step 2: Append al final del archivo** (tras la clase `CoachActionLog`):

```python
class SyncEvent(Base):
    """Outbox local de eventos offline pendientes de sincronizar (append-only).

    En el nodo desktop guarda ventas/altas de cliente hechas sin conexión, con un
    hash encadenado que detecta manipulación. En el servidor central la tabla queda
    inerte hasta que P2/P3 la usen.
    """

    __tablename__ = "pending_sync_events"
    __table_args__ = (
        Index("ix_pending_sync_events_business_seq", "business_id", "seq"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    business_id: Mapped[int] = mapped_column(Integer, index=True)
    location_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    payload: Mapped[str] = mapped_column(Text)
    seq: Mapped[int] = mapped_column(Integer, index=True)
    prev_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    event_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="pending", server_default="pending", index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
```

**Edge cases (ya resueltos):** `Optional` y `datetime` ya están importados al inicio del
archivo (líneas 3-4). No añadir imports.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.models import SyncEvent; print(SyncEvent.__tablename__)"
# Expected: pending_sync_events
```

#### Expected Report

- [ ] `app/models.py` — clase `SyncEvent` añadida al final
- [ ] Ningún otro archivo modificado
- [ ] Import de `SyncEvent` imprime `pending_sync_events`

---

### T6: Migración v5 (crear `pending_sync_events`)

**Depends on:** T5
**Skill:** data-engineer
**Agent:** data
**Files:** `app/migrations.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/migrations.py — ACTUAL
_LATEST_SCHEMA_VERSION = 4
# _run_sqlite_schema_migrations(): usa conn.exec_driver_sql(...) dentro de `with engine.begin() as conn:`
#   y termina con: if schema_version < 1: _set_schema_version(conn, 1); return schema_version
# _run_postgresql_schema_migrations(): idéntico patrón, termina igual.
```

#### Objective

Crear la tabla `pending_sync_events` en ambas ramas de migración y subir la versión a 5.

#### Instructions

**Step 1: Read the file**
```
Read app/migrations.py
```

**Step 2: Bump de versión (línea 41)**

```python
# BEFORE:
_LATEST_SCHEMA_VERSION = 4
# AFTER:
_LATEST_SCHEMA_VERSION = 5
```

**Step 3: En `_run_sqlite_schema_migrations`**, justo ANTES de `if schema_version < 1:`
(final de la función), añadir:

```python
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS pending_sync_events ("
            "id INTEGER PRIMARY KEY, event_id VARCHAR(64), business_id INTEGER, "
            "location_id INTEGER, event_type VARCHAR(32), payload TEXT, seq INTEGER, "
            "prev_hash VARCHAR(64), event_hash VARCHAR(64), "
            "status VARCHAR(16) DEFAULT 'pending', created_at TIMESTAMP)"
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_sync_events_event_id "
            "ON pending_sync_events(event_id)"
        )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_pending_sync_events_business_seq "
            "ON pending_sync_events(business_id, seq)"
        )
```

**Step 4: En `_run_postgresql_schema_migrations`**, justo ANTES de `if schema_version < 1:`,
añadir:

```python
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS pending_sync_events ("
            "id SERIAL PRIMARY KEY, event_id VARCHAR(64), business_id INTEGER, "
            "location_id INTEGER, event_type VARCHAR(32), payload TEXT, seq INTEGER, "
            "prev_hash VARCHAR(64), event_hash VARCHAR(64), "
            "status VARCHAR(16) DEFAULT 'pending', created_at TIMESTAMPTZ)"
        )
        conn.exec_driver_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_pending_sync_events_event_id "
            "ON pending_sync_events(event_id)"
        )
        conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_pending_sync_events_business_seq "
            "ON pending_sync_events(business_id, seq)"
        )
```

**Edge cases (ya resueltos):** `CREATE TABLE/INDEX IF NOT EXISTS` es idempotente; correr la
migración dos veces no falla. `Base.metadata.create_all` (startup) ya crea la tabla en BD
nuevas — esta migración cubre las BD existentes.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "import app.migrations as m; print(m._LATEST_SCHEMA_VERSION)"
# Expected: 5
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/ -q
# Expected: toda la suite unit PASS
```

#### Expected Report

- [ ] `app/migrations.py` — `_LATEST_SCHEMA_VERSION = 5` + bloque `pending_sync_events` en ambas ramas
- [ ] Ningún otro archivo modificado
- [ ] `_LATEST_SCHEMA_VERSION` imprime `5`; suite unit PASS

---

### T7: Helper de outbox (`app/sync/outbox.py`)

**Depends on:** T5
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/sync/__init__.py` (CREATE), `app/sync/outbox.py` (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/models.py — YA EXISTE (T5)
class SyncEvent(Base):
    __tablename__ = "pending_sync_events"
    id: Mapped[int]; event_id: Mapped[str]; business_id: Mapped[int]
    location_id: Mapped[Optional[int]]; event_type: Mapped[str]; payload: Mapped[str]
    seq: Mapped[int]; prev_hash: Mapped[Optional[str]]; event_hash: Mapped[str]
    status: Mapped[str]; created_at: Mapped[datetime]
# sqlalchemy: from sqlalchemy import select, func; from sqlalchemy.orm import Session
```

#### Objective

Crear el paquete `app/sync/` con el helper de outbox (hash encadenado + verificación).

#### Instructions

**Step 1: Create** `app/sync/__init__.py` VACÍO (0 bytes).

**Step 2: Create** `app/sync/outbox.py` con EXACTAMENTE:

```python
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import SyncEvent


def compute_event_hash(
    prev_hash: Optional[str], event_id: str, event_type: str, payload: str
) -> str:
    """sha256 hex de la concatenación canónica '|'-separada."""
    raw = f"{prev_hash or ''}|{event_id}|{event_type}|{payload}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def append_event(
    db: Session,
    *,
    business_id: int,
    event_type: str,
    payload: dict,
    location_id: Optional[int] = None,
    event_id: Optional[str] = None,
) -> SyncEvent:
    """Encola un evento en pending_sync_events con seq monotónico y hash encadenado.

    No hace commit (lo hace el caller). event_id se genera (uuid4) si no se pasa.
    """
    last = db.scalar(
        select(SyncEvent)
        .where(SyncEvent.business_id == int(business_id))
        .order_by(SyncEvent.seq.desc())
        .limit(1)
    )
    prev_hash = last.event_hash if last is not None else None
    next_seq = (last.seq + 1) if last is not None else 1

    eid = event_id or uuid.uuid4().hex
    payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    ehash = compute_event_hash(prev_hash, eid, event_type, payload_str)

    event = SyncEvent(
        event_id=eid,
        business_id=int(business_id),
        location_id=location_id,
        event_type=event_type,
        payload=payload_str,
        seq=next_seq,
        prev_hash=prev_hash,
        event_hash=ehash,
        status="pending",
    )
    db.add(event)
    db.flush()
    return event


def verify_chain(db: Session, business_id: int) -> bool:
    """Recalcula la cadena de hash de los eventos del negocio en orden de seq.

    True si íntegra, False si alguna fila fue alterada. 0 eventos -> True.
    """
    events = list(
        db.scalars(
            select(SyncEvent)
            .where(SyncEvent.business_id == int(business_id))
            .order_by(SyncEvent.seq.asc())
        )
    )
    prev_hash: Optional[str] = None
    for ev in events:
        expected = compute_event_hash(prev_hash, ev.event_id, ev.event_type, ev.payload)
        if expected != ev.event_hash or ev.prev_hash != prev_hash:
            return False
        prev_hash = ev.event_hash
    return True
```

**Edge cases (ya resueltos):**
- Primer evento del negocio: `prev_hash=None`, `seq=1`.
- 0 eventos en `verify_chain`: devuelve `True`.
- Un solo escritor por nodo (SQLite local) → sin carrera de `seq`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -c "from app.sync.outbox import append_event, verify_chain, compute_event_hash; print('ok')"
# Expected: ok
```

#### Expected Report

- [ ] `app/sync/__init__.py` creado (vacío)
- [ ] `app/sync/outbox.py` creado con 3 funciones
- [ ] Ningún otro archivo modificado
- [ ] Import imprime `ok`

---

### T8: Tests del outbox y la cadena de hash

**Depends on:** T7
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_sync_outbox.py` (CREATE)
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/conftest.py — fixtures: db_session (SQLite en memoria, crea Business code="test"),
#   business_id -> int. Base.metadata.create_all crea pending_sync_events.
# app/sync/outbox.py — append_event(db, *, business_id, event_type, payload, ...) -> SyncEvent
#   verify_chain(db, business_id) -> bool ; compute_event_hash(prev, id, type, payload) -> str
# app/models.py — SyncEvent
```

#### Objective

Crear tests del outbox en `tests/unit/test_sync_outbox.py`.

#### Instructions

**Step 1: Create the file** `tests/unit/test_sync_outbox.py` con EXACTAMENTE:

```python
from __future__ import annotations

from app.models import SyncEvent
from app.sync.outbox import append_event, verify_chain


def test_append_first_event_has_no_prev_and_seq_1(db_session, business_id):
    ev = append_event(
        db_session, business_id=business_id, event_type="sale", payload={"sku": "X"}
    )
    db_session.commit()
    assert ev.seq == 1
    assert ev.prev_hash is None
    assert ev.event_hash


def test_chain_links_events(db_session, business_id):
    e1 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 2})
    db_session.commit()
    assert e2.seq == 2
    assert e2.prev_hash == e1.event_hash


def test_verify_chain_true_when_intact(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    append_event(db_session, business_id=business_id, event_type="customer", payload={"n": 2})
    db_session.commit()
    assert verify_chain(db_session, business_id) is True


def test_verify_chain_false_when_payload_tampered(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 2})
    db_session.commit()
    # Manipular el payload directamente (simula edición del .db)
    row = db_session.get(SyncEvent, e2.id)
    row.payload = '{"n": 999}'
    db_session.commit()
    assert verify_chain(db_session, business_id) is False


def test_verify_chain_true_when_empty(db_session, business_id):
    assert verify_chain(db_session, business_id) is True
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sync_outbox.py -v
# Expected: 5 passed
```

#### Expected Report

- [ ] `tests/unit/test_sync_outbox.py` creado — 5 tests
- [ ] `pytest tests/unit/test_sync_outbox.py` — 5 PASS

---

### T9: Version bump 0.4.6 → 0.4.7

**Depends on:** T1-T8
**Skill:** release-manager
**Agent:** coder
**Files:** `VERSION`
**Pillars:** P1

#### Context

```
# VERSION — ACTUAL
0.4.6
```

#### Objective

Subir la versión un PATCH (regla dura del proyecto: PATCH, no MINOR).

#### Instructions

**Step 1:** Reemplazar el contenido completo de `VERSION` por:

```
0.4.7
```

**Edge cases (ya resueltos):** un PATCH, no MINOR (aunque sea una feature grande) — regla
dura del proyecto salvo que el usuario marque hito explícito.

#### Verification

```bash
cat VERSION
# Expected: 0.4.7
```

#### Expected Report

- [ ] `VERSION` = `0.4.7`
- [ ] Ningún otro archivo modificado

---

## Checklist de los 7 mandamientos (Fase 3)

- [x] Claude Code piensa, OpenCode escribe (before/after exactos)
- [x] ≤ 800 tokens por envelope
- [x] Before/after, nunca descripciones
- [x] Máx 2 archivos por envelope (T7 = __init__ + outbox)
- [x] Verificación mecánica (comando → PASS/FAIL)
- [x] Dependencias explícitas con firmas reales
- [x] Batches de 3-5 (T1-T3 · T4-T6 · T7-T9)
