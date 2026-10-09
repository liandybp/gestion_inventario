# Design — P3: Cliente de sync + escritura offline

- **Slice:** P3 de 5 (`offline-desktop-sync`)
- **Proyecto:** gestion_inventario · **Rama base:** `develop` · **Versión objetivo:** v0.4.7
- **Fase SDD:** 2 de 4 (Design)
- **REQs cubiertos:** REQ-5 (venta offline), REQ-6 (alta cliente offline), REQ-7 (lecturas offline con RBAC), REQ-8 (banner última sync), REQ-17 (credenciales acotadas, lado cliente), REQ-18 (backup local outbox), REQ-19 (compra/traspaso/LLM deshabilitados offline). Aterriza REQ-1 (APP_MODE, primer consumidor).
- **Depende de:** P1 (outbox), P2 (endpoints `/sync/*`) — verdes
- **Insumo:** `specs/offline-desktop-sync/requirements.md`

## Decisión cerrada con el usuario
Bootstrap: **primer sync online obligatorio** (el nodo ya registrado en P2 recibe token + foto + usuarios acotados; P3 los consume y guarda).

---

## 2.1 Codebase Grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `app/settings.py` | MODIFY | `APP_MODE` + `is_desktop()` (primer consumidor de REQ-1) |
| `app/models.py` | MODIFY | Modelo `SyncState` (fila única: last_synced_at, server_url, device_token) |
| `app/migrations.py` | MODIFY | Migración v7: tabla `sync_state` + bump |
| `app/routers/ui_sales.py` | MODIFY | Hook outbox tras venta exitosa (`sale`, `sale_barcode`) en modo desktop |
| `app/routers/ui_customers.py` | MODIFY | Hook outbox tras alta de cliente en modo desktop |
| `app/sync/client.py` | CREATE | `sync()` (push + apply_pull + last_synced_at) sobre httpx |
| `app/routers/ui_sync.py` | CREATE | `POST /ui/sync` (botón) + `GET /ui/sync/status` |
| `app/templates/partials/sync_status.html` | CREATE | Partial del banner/resultado de sync |
| `app/main.py` | MODIFY | Registrar `ui_sync` (prefix `/ui`) |
| `app/routers/ui_home.py` | MODIFY | Pasar `app_mode` + `last_synced_at` al contexto de `dashboard.html` |
| `app/templates/dashboard.html` | MODIFY | Banner de última sync + botón Sincronizar + gate de pestañas offline |
| `tests/unit/test_sync_client.py` | CREATE | Tests del cliente (recolección de pendientes, apply_pull) |
| `tests/unit/test_offline_hooks.py` | CREATE | Venta/cliente en modo desktop encolan evento |
| `app/sync/outbox.py` | REFERENCE | `append_event(db, *, business_id, event_type, payload, location_id)` (P1) |
| `app/sync/schemas.py` | REFERENCE | `PushResponse`, `SyncPullData` (P2) |

**Firmas reales verificadas:**
```python
# app/routers/ui_sales.py:161 sale(...) / :241 sale_barcode(...) — bid = require_active_business_id(db, request)
#   result = service.sale(SaleCreate(sku, quantity, unit_price, movement_date, note, location_code))
# app/routers/ui_customers.py:45 customer_create(...) — CustomerRepository(db, bid).create(...) + db.commit()
# app/routers/ui_home.py:163 dashboard(request, db) → TemplateResponse("dashboard.html", context={user, businesses, active_business_id, active_business})
# httpx==0.27.2 disponible (requirements.txt) — sin dependencia nueva
# app/sync/outbox.py — append_event(db, *, business_id, event_type, payload:dict, location_id=None, event_id=None) -> SyncEvent (NO commitea)
# app/models.py::SyncEvent — status default "pending"
```

---

## 2.2 Architecture Approach

El mismo backend corre como **nodo desktop** cuando `APP_MODE=desktop`. Tres comportamientos
nuevos, todos gated por `is_desktop()`:

1. **Escritura offline (REQ-5/6).** Tras una venta o alta de cliente exitosa, si `is_desktop()`,
   se encola un evento en el outbox (`append_event`) con el payload necesario para que el
   servidor lo reaplique (P2). En modo `server` el hook no hace nada → el comportamiento online
   actual no cambia.
2. **Sincronización (REQ-8/17/18).** `app/sync/client.py::sync()` lee `SyncState`
   (server_url + device_token), recolecta los eventos `pending` del outbox en orden de `seq`,
   hace `POST /sync/push`, y con la respuesta: marca los eventos aplicados como `synced`, aplica
   el pull (reemplazo transaccional de las tablas de lectura, incl. `users` acotados = REQ-17), y
   actualiza `last_synced_at`. Botón "Sincronizar" (`POST /ui/sync`).
3. **Lecturas + gating (REQ-7/19).** El banner de última sync se muestra en `dashboard.html`
   (persistente en todas las pestañas). Las pestañas de compra, traspaso y copiloto se ocultan en
   modo desktop (requieren conexión/servidor). El RBAC por rol existente se conserva intacto.

**Backup local del outbox (REQ-18).** `append_event` ya deja el evento en la BD local; el backup
rotativo es una **copia del archivo `.db`** (o de una exportación del outbox) que se genera tras
cada encolado. En v1 se implementa como una exportación JSON append-only del outbox a un archivo
local rotativo (`sync_outbox_backup.jsonl`), sin red — barato y suficiente para reducir la
ventana de pérdida. (El cifrado de esa copia es responsabilidad de P4 junto con el `.db`.)

**Invariantes preservadas:** en modo `server` (default), **cero cambio de comportamiento** — los
hooks y el banner son no-ops. No se toca `auth.py`/`deps.py`/`db.py`/`docker-compose*`.

---

## 2.3 Data Model

### SyncState (`sync_state`) — NEW (fila única, id=1)
| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | siempre 1 |
| last_synced_at | DateTime(tz) | nullable — alimenta el banner |
| server_url | String(255) | nullable — URL del servidor central |
| device_token | String(128) | nullable — token de este nodo (de /sync/register) |
| updated_at | DateTime(tz) | now(), onupdate |

**Migración v7:** `CREATE TABLE IF NOT EXISTS sync_state ...`, `_LATEST_SCHEMA_VERSION = 7`.
**Rollback:** DROP TABLE + versión a 6. En el servidor la tabla queda inerte (una fila opcional).

---

## 2.4 API Contracts

### POST /ui/sync  (solo desktop; sesión de usuario)
Dispara `sync()`. **Response 200:** partial `sync_status.html` con resultado (aplicados,
conflictos, nueva fecha). Si `is_desktop()` es falso → 404/deshabilitado.

### GET /ui/sync/status  (solo desktop)
Devuelve el partial del banner con `last_synced_at`.

No se modifican los contratos de `/ui/sale`, `/ui/customer/create` (solo se añade el hook interno).

---

## 2.5 Security Review

| Check | Respuesta |
|-------|-----------|
| Nuevo límite de auth | `/ui/sync` usa la sesión de usuario existente (mismo middleware `/ui/*`) |
| Datos sensibles | `device_token` se guarda en `sync_state` (BD local, cifrada en P4). El pull trae PII/hashes acotados al negocio (REQ-17) |
| Validación | `apply_pull` solo inserta filas del snapshot que devolvió el servidor autenticado; no confía en input del usuario |
| Superficie | 2 endpoints UI nuevos, gated a desktop; en server no existen efectos |
| Robustez | `sync()` maneja fallo de red (sin conexión) → mensaje claro, no rompe la app; `apply_pull` transaccional (rollback si falla, no deja tablas a medias) |

---

## 2.6 Trade-offs

| # | Decisión | Elegido | Alternativa | Por qué no |
|---|----------|---------|-------------|-----------|
| T1 | Cliente HTTP | **httpx** (ya en requirements) | urllib stdlib | httpx ya está; API más limpia, sin dependencia nueva |
| T2 | Estado de sync | Tabla `sync_state` de una fila | Archivo JSON junto al .db | Vive dentro del .db (cifrado en P4); una sola fuente |
| T3 | apply_pull | **Reemplazo transaccional** de tablas de lectura | Merge fila-a-fila | v1 = foto completa (design P2); reemplazo es simple y determinista; transaccional evita estado a medias |
| T4 | Gating offline | Ocultar pestañas en desktop + no-op en server | Guard por request en cada router | Ocultar es suficiente en v1 y menos superficie; el server no cambia |
| T5 | Backup outbox | Exportación JSONL rotativa local | Copia completa del .db por evento | Barato, sin red, suficiente para la ventana offline |

---

## 2.7 Migration & Rollback
```
v7: CREATE TABLE IF NOT EXISTS sync_state. _LATEST_SCHEMA_VERSION = 7.
Rollback: DROP TABLE sync_state + versión a 6. Aditivo.
Comportamiento server sin cambios (hooks/banner no-op cuando APP_MODE != desktop).
```

---

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría

| Dim | Riesgo | Mitigación → Instruction de envelope |
|-----|--------|--------------------------------------|
| **1. SoC/SRP** | Meter la lógica de sync en los routers de venta/cliente | El hook de outbox es **una llamada** a `append_event` (helper P1); toda la lógica de transporte vive en `app/sync/client.py`, no en los routers UI |
| **2. Cohesión/Acoplamiento** | `apply_pull` repetir el patrón de borrado/inserción por tabla | Un mapa `{tabla: modelo}` + un bucle único; sin duplicar el borrado/inserción por tabla |
| **3. Capas/Límites** | El router `ui_sync` con lógica de red | El router solo llama a `client.sync()` y renderiza el partial; la red vive en `client.py` |
| **5. Robustez/Resiliencia** | `sync()` (red) y `apply_pull` (BD masiva) pueden fallar a medias | **CADA** función pública de `client.py` en try/except con `_log.error(..., exc_info=True)`: fallo de red → dict `{"ok": False, "error": ...}` (no excepción a la UI); `apply_pull` en **una transacción** con rollback ante error (nunca tablas de lectura a medias). Especificado literal en los envelopes |
| **6. Consistencia Transaccional** | Marcar eventos `synced` y aplicar el pull deben ser coherentes | Se marca `synced` **solo** los `event_id` que el servidor confirmó en `applied_event_ids`; `apply_pull` en su propia transacción tras el push confirmado |

Sin hallazgos que bloqueen Fase 3.

---

## 2.10 Delegation
Ninguna. Del dominio del SDD Lead (routers UI + cliente HTTP + template).

---

## Interfaces nuevas (contratos para Fase 3)
```python
# app/settings.py
APP_MODE = env("APP_MODE", "server")
def is_desktop() -> bool: return APP_MODE == "desktop"

# app/sync/client.py
def sync(db: Session) -> dict:
    """Push de pendientes + apply_pull + last_synced_at. {"ok":bool,"applied":int,"conflicts":int,"error"?:str}."""
def apply_pull(db: Session, snapshot: dict) -> None:
    """Reemplazo transaccional de las tablas de lectura con las filas del snapshot."""
def _pending_events(db: Session) -> list[dict]:
    """Eventos SyncEvent status='pending' en orden de seq, serializados para /sync/push."""
```
