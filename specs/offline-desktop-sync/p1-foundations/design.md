# Design — P1: Fundamentos de sincronización (outbox + rangos de SKU)

- **Slice:** P1 de 5 (`offline-desktop-sync`)
- **Proyecto:** gestion_inventario · **Rama base:** `develop` · **Versión objetivo:** v0.4.7
- **Fase SDD:** 2 de 4 (Design)
- **REQs cubiertos:** REQ-9 (outbox append-only con hash encadenado), REQ-15 (rangos de 10k de SKU por negocio)
- **Insumo:** `specs/offline-desktop-sync/requirements.md`

> **Refinamiento de alcance vs requirements.md:** REQ-1 (APP_MODE) estaba listado como
> "parcial" en P1. Se **mueve a P4** (desktop runtime): ningún envelope de P1 consume
> `APP_MODE`, y añadir un setting sin consumidor viola la dimensión 1 del pre-mortem (capa
> "por si acaso" sin wiring). P1 queda enfocado en las dos piezas que sí son fundacionales y
> testeables sobre el backend actual: el **outbox** y los **rangos de SKU**.

---

## 2.1 Codebase Grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `app/services/sku_range.py` | **CREATE** | Helper único de numeración de SKU por rango de negocio |
| `app/services/product_service.py` | **MODIFY** | `_generate_sku` pasa a usar el helper (línea 25-33) |
| `app/routers/ui_purchases.py` | **MODIFY** | `_generate_auto_sku` inline pasa a usar el helper (línea 345-353) |
| `app/models.py` | **MODIFY** | Nuevo modelo `SyncEvent` (tabla `pending_sync_events`) |
| `app/migrations.py` | **MODIFY** | Migración v5: crear `pending_sync_events` (SQLite + PG) + bump `_LATEST_SCHEMA_VERSION` |
| `app/sync/__init__.py` | **CREATE** | Paquete nuevo `app/sync/` |
| `app/sync/outbox.py` | **CREATE** | `append_event` (hash encadenado) + `verify_chain` |
| `tests/unit/test_sku_range.py` | **CREATE** | Tests del helper de rangos |
| `tests/unit/test_sync_outbox.py` | **CREATE** | Tests del outbox y la cadena de hash |
| `VERSION` | **MODIFY** | 0.4.6 → 0.4.7 |
| `app/repositories/purchase_repository.py` | REFERENCE | `list_sku_prefixes(prefix)` devuelve SKUs globales — se sigue usando |
| `app/db.py` | REFERENCE | `Base` — el modelo nuevo hereda de aquí |
| `tests/conftest.py` | REFERENCE | fixtures `db_session`, `business_id` (crea `Business(code="test")`) |

**Firmas reales verificadas (no asumidas):**

```python
# app/services/product_service.py:25 — ACTUAL
def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
    rows = self._db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
    existing = [str(sku or "") for (sku,) in rows]
    max_n = 0
    for sku in existing:
        suffix = sku[len(prefix):]
        if suffix.isdigit():
            max_n = max(max_n, int(suffix))
    return f"{prefix}{str(max_n + 1).zfill(width)}"
# self._business_id: int  ya existe en ProductService

# app/routers/ui_purchases.py:345 — ACTUAL (closure dentro del endpoint, usa `bid`)
def _generate_auto_sku(prefix: str = "SKU", width: int = 6) -> str:
    rows = PurchaseRepository(db, bid).list_sku_prefixes(prefix)
    existing = [str(sku or "") for sku in rows]
    max_n = 0
    for s in existing:
        suffix = s[len(prefix):]
        if suffix.isdigit():
            max_n = max(max_n, int(suffix))
    return f"{prefix}{str(max_n + 1).zfill(width)}"

# app/migrations.py — ACTUAL
_LATEST_SCHEMA_VERSION = 4            # → pasa a 5
# run_startup_tasks(): create_all + rama sqlite/pg + backfills por versión + set_schema_version
# patrón de columna nueva: PRAGMA table_info (SQLite) / information_schema.columns (PG)
```

---

## 2.2 Architecture Approach

Dos piezas independientes, ambas sin dependencia de desktop ni de red — corren y se testean
sobre el backend actual con SQLite en memoria.

**(A) Rangos de SKU (REQ-15).** Hoy hay **dos** generadores de SKU automático que hacen
"max global + 1": `ProductService._generate_sku` y el closure `_generate_auto_sku` en
`ui_purchases.py`. Ambos escanean `Product.sku ILIKE 'SKU%'` global (correcto respecto al
constraint `ux_products_sku`, que es único global). Se centraliza la numeración en **un helper
único** `app/services/sku_range.py` que numera dentro del **rango reservado del negocio**:

```
base(business_id) = business_id * 10000          # SKU_RANGE_SIZE = 10000
rango del negocio = [base, base + 10000)         # p.ej. business_id=2 → [20000, 30000)
next = max(numeros_del_prefijo_en_rango, base) + 1   # zfill(6) → "SKU020001"
```

Como todo SKU existente hoy es un número global < 10.000 (máx real ~509) y todo `base` es
≥ 10.000, **ningún SKU existente cae en ningún rango** → los rangos son disjuntos por
construcción y no colisionan ni entre negocios ni con datos previos. El generador conserva el
**retry loop existente** (5 intentos) como red de seguridad ante cualquier borde.

**(B) Outbox (REQ-9).** Tabla nueva `pending_sync_events` + helper `app/sync/outbox.py`. Cada
evento encolado guarda un `event_hash = sha256(prev_hash | event_id | event_type | payload)`,
encadenando el `event_hash` del evento anterior del mismo nodo. `verify_chain` recalcula la
cadena en orden y detecta cualquier alteración posterior. En P1 el outbox **solo se crea y se
testea**; sus escritores reales (venta, alta de cliente) llegan en P3, y su lectura/replay en
el servidor en P2. La tabla vive en el modelo compartido: en el servidor central queda inerte
(vacía) hasta P3.

**Invariantes preservadas:**
- `ux_products_sku` (único global) — el helper produce SKUs globalmente únicos por construcción.
- No se toca `auth.py`, `deps.py`, `db.py`, `docker-compose*`, ni la lógica de FIFO/venta.
- Patrón de migración inline existente (schema_version, PRAGMA/information_schema) sin cambios.

---

## 2.3 Data Model

### Entity: SyncEvent (tabla `pending_sync_events`) — NEW

| Campo | Tipo | Cambio | Default | Notas |
|-------|------|--------|---------|-------|
| id | Integer PK | NEW | — | autoincremental local |
| event_id | String(64) | NEW | — | UUID estable; **unique** (idempotencia de push en P2) |
| business_id | Integer | NEW | — | negocio del nodo |
| location_id | Integer | NEW | NULL | ubicación del nodo (ventas) |
| event_type | String(32) | NEW | — | `"sale"` \| `"customer"` |
| payload | Text | NEW | — | JSON del evento |
| seq | Integer | NEW | — | orden monotónico por nodo (indexado) |
| prev_hash | String(64) | NEW | NULL | `event_hash` del evento anterior (NULL en el primero) |
| event_hash | String(64) | NEW | — | sha256 encadenado |
| status | String(16) | NEW | `"pending"` | `pending` \| `synced` (lo usa P3) |
| created_at | DateTime(tz) | NEW | now() | — |

```
Índices: ix_pending_sync_events_event_id (unique), ix_pending_sync_events_business_id,
         ix_pending_sync_events_seq, ix_pending_sync_events_status
```

**Migración (v5):** crear la tabla en ambas ramas (SQLite `CREATE TABLE IF NOT EXISTS` +
PG equivalente), idempotente, y `_LATEST_SCHEMA_VERSION = 5`. `Base.metadata.create_all`
ya la crea en entornos nuevos (tests, desktop); la migración cubre las BD existentes (prod).
**Rollback:** `DROP TABLE pending_sync_events;` — aditivo, sin pérdida de datos existentes.

**No se añade columna a `businesses`:** el rango de SKU es **derivado** (`business_id * 10000`),
no almacenado — cero migración de columna, cero backfill (ver Trade-off T1).

---

## 2.4 API Contracts

**Ninguno en P1.** No se crean ni modifican endpoints. El closure `_generate_auto_sku` vive
dentro de un endpoint existente (`POST /ui/purchase`), pero su contrato HTTP no cambia: solo
cambia cómo calcula el número interno del SKU.

---

## 2.5 Security Review

| Check | Respuesta |
|-------|-----------|
| ¿Nuevo límite de auth? | No — P1 no toca auth ni endpoints |
| ¿Nuevos datos sensibles? | `pending_sync_events.payload` contendrá datos de venta/cliente **en P3**; en P1 la tabla queda vacía. El cifrado en reposo del payload es responsabilidad de P4 (SQLCipher en el nodo desktop) |
| ¿Validación de input? | El helper de SKU solo procesa strings ya existentes en BD; ignora sufijos no numéricos. El outbox serializa payloads vía `json.dumps` |
| ¿Nueva superficie de ataque? | La tabla `pending_sync_events` es inerte en el servidor hasta P2/P3; sin endpoint que la exponga en P1 |
| ¿Integridad? | El hash encadenado (REQ-9) es precisamente el control de integridad; se testea en P1 |
| ¿OWASP? | N/A directo en P1 (sin nueva superficie HTTP) |

---

## 2.6 Trade-offs

| # | Decisión | Elegido | Alternativa | Por qué no la alternativa |
|---|----------|---------|-------------|---------------------------|
| T1 | Base del rango de SKU | **Derivada `business_id * 10000`** (helper puro, sin estado) | Columna `sku_range_base` en `businesses` + backfill + set en creación | Menos superficie de código y cero riesgo de migración/backfill; los rangos ya quedan disjuntos por construcción. Contra conocido: acopla la numeración al valor de la PK (aceptable — los negocios no se renumeran). Si más adelante se necesita control explícito del rango, la columna es un follow-up limpio |
| T2 | Ubicación del outbox | **Modelo compartido (`models.py` + `app/sync/`)** | Modelo solo-desktop, separado del server | El backend actual usa `create_all`; una tabla compartida inerte en el server es más simple y hace P1 testeable con el conftest existente. Contra: el server tiene una tabla que no usa hasta P3 (inocuo) |
| T3 | Numeración dentro del rango | **`max(en_rango, base) + 1`, zfill(6)** + retry loop existente | Contador persistido por negocio | Sin estado nuevo; determinista; el retry loop ya cubre bordes. Un contador persistido añade otra fuente de verdad a mantener sincronizada |
| T4 | Encadenado de integridad | **sha256(prev_hash \| event_id \| event_type \| payload)** | HMAC con secreto del nodo | HMAC requiere gestión de secreto (llega en P4 con DPAPI); en P1 basta detección de manipulación local vía cadena. El servidor re-valida en P2 con su propia lógica |

---

## 2.7 Migration & Rollback

```
Migración (v5):
1. Base.metadata.create_all → crea pending_sync_events en BD nuevas (tests/desktop)
2. _run_sqlite_schema_migrations / _run_postgresql_schema_migrations → CREATE TABLE IF NOT
   EXISTS pending_sync_events (idempotente) para BD existentes (prod)
3. _LATEST_SCHEMA_VERSION = 5

Rollback:
1. Revertir generadores de SKU al escaneo global (git revert de product_service/ui_purchases)
2. DROP TABLE pending_sync_events  (aditiva, sin pérdida)
3. _LATEST_SCHEMA_VERSION = 4
Sin backfill que revertir (rango de SKU es derivado, no almacenado).
```

**Nota sobre SKUs ya generados:** los productos existentes conservan sus SKUs globales
("ya los que están, están"). El cambio solo afecta la numeración de SKUs **nuevos**
autogenerados. No hay renumeración de datos existentes.

---

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría

| Dim | Riesgo detectado en el diseño | Mitigación → Instruction de envelope |
|-----|-------------------------------|--------------------------------------|
| **1. SoC/SRP** | Duplicar la lógica de numeración en dos sitios (product_service + ui_purchases) reproduce el bug histórico de SKU (v0.4.6 fue exactamente numeración divergente) | **Un solo helper** `sku_range.py`; T4 y T5 lo **importan**, no reimplementan. Ningún componente nuevo queda sin consumidor (el outbox se testea en P1 y se consume en P3 — su wiring está planificado, no es "por si acaso") |
| **2. Cohesión/Acoplamiento** | El helper de SKU podría recibir la lista de SKUs de formas distintas en cada caller | Firma única `next_sku_in_range(existing_skus: list[str], business_id: int, ...)`; cada caller le pasa su propia lista ya obtenida (product_service vía `select`, ui_purchases vía `list_sku_prefixes`). El helper no consulta BD → sin acoplamiento a la sesión |
| **3. Capas/Límites** | Un helper que consultara BD rompería la dirección de dependencias (service → helper → BD) | El helper es **puro** (recibe la lista, no la sesión). La consulta se queda en el caller (capa que ya tiene la sesión) |
| **5. Robustez/Resiliencia** | `append_event` toca BD (lee el último evento, inserta) → debe manejar la cadena vacía (primer evento, `prev_hash = None`) y sufijos no numéricos en el rango | Edge cases resueltos explícitamente en los envelopes: primer evento → `prev_hash=None`; sufijo no dígito → ignorado; rango vacío → `base + 1`. `verify_chain` sobre 0 eventos → `True` |
| **6. Consistencia Transaccional** | El `seq` monotónico y el `prev_hash` deben leerse de forma consistente (dos escrituras concurrentes en el mismo nodo podrían encadenar mal) | En el nodo desktop hay **un solo proceso/escritor** (SQLite local, sin concurrencia multi-nodo sobre el mismo archivo). El diseño documenta esta invariante; `append_event` lee `max(seq)` y `event_hash` del último en la misma transacción del insert |

Sin hallazgos que bloqueen Fase 3. Las mitigaciones ya están reflejadas como instrucciones
concretas en los envelopes (`tasks.md`).

---

## 2.10 Delegation

Ninguna. P1 es acotado y del dominio del SDD Lead (modelo + helpers puros + migración inline).

---

## Interfaces nuevas (contratos exactos para Fase 3)

```python
# app/services/sku_range.py — NEW
SKU_RANGE_SIZE = 10000

def sku_range_base(business_id: int) -> int:
    """Base inclusiva del rango de SKU reservado a un negocio."""

def next_sku_in_range(
    existing_skus: list[str], business_id: int, prefix: str = "SKU", width: int = 6
) -> str:
    """Siguiente SKU autogenerado dentro del rango [base, base+SKU_RANGE_SIZE).
    Ignora sufijos no numéricos y números fuera del rango. Rango vacío → base+1."""

# app/sync/outbox.py — NEW
def compute_event_hash(prev_hash: str | None, event_id: str, event_type: str, payload: str) -> str:
    """sha256 hex de la concatenación canónica '|'-separada."""

def append_event(
    db: Session, *, business_id: int, event_type: str, payload: dict,
    location_id: int | None = None, event_id: str | None = None,
) -> "SyncEvent":
    """Encola un evento en pending_sync_events con seq monotónico y hash encadenado.
    event_id se genera (uuid4) si no se pasa. NO hace commit (lo hace el caller)."""

def verify_chain(db: Session, business_id: int) -> bool:
    """Recalcula la cadena de hash de los eventos del negocio en orden de seq.
    True si íntegra, False si alguna fila fue alterada. 0 eventos → True."""
```
