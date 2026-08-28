# Design — P2: Backend de sincronización (servidor central)

- **Slice:** P2 de 5 (`offline-desktop-sync`)
- **Proyecto:** gestion_inventario · **Rama base:** `develop` · **Versión objetivo:** v0.4.7
- **Fase SDD:** 2 de 4 (Design)
- **REQs cubiertos:** REQ-10 (push idempotente), REQ-11 (replay vía services), REQ-12 (dedup clientes), REQ-13 (pull foto completa), REQ-14 (pantalla de conflictos), REQ-16 (rechazo por versión), REQ-17 parcial (usuarios acotados en el pull)
- **Depende de:** P1 (modelo `SyncEvent`, helper outbox) — ya implementado
- **Insumo:** `specs/offline-desktop-sync/requirements.md`

## Decisiones cerradas con el usuario (2026-08-28)

- **Auth de sync:** token por dispositivo (registro de dispositivos, revocable).
- **Bootstrap:** primer sync online obligatorio (el nodo se registra, recibe token + foto inicial + usuarios acotados).

## Hallazgos de grounding (flagged — protocolo SDD)

1. **`InventoryService.sale()` hace `self._db.commit()` internamente** (inventory_service.py:1765).
   → El replay inserta el registro de idempotencia (`AppliedSyncEvent`) en la **misma sesión**
   ANTES de llamar `sale()`; el commit interno de `sale()` persiste ambos atómicamente. En
   conflicto de stock, `sale()` lanza `HTTPException` **antes** de su commit → se hace
   `rollback()` (descarta el `AppliedSyncEvent` pendiente) y se registra un `SyncConflict`.
2. **`InventoryMovement` (venta rápida) NO tiene `customer_id`** — solo `SalesDocument`
   (online-only). → **REQ-12 se simplifica en v1:** dedup de clientes = conservar-el-primero +
   descartar duplicado. **No hay remapeo de ventas** porque ninguna venta offline referencia a
   un cliente. Confirmado con el usuario. La colisión de `client_id` sin match sigue yendo a
   conflicto para revisión manual.

---

## 2.1 Codebase Grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `app/models.py` | MODIFY | Modelos `DeviceToken`, `AppliedSyncEvent`, `SyncConflict` |
| `app/migrations.py` | MODIFY | Migración v6: 3 tablas nuevas + bump `_LATEST_SCHEMA_VERSION` |
| `app/sync/schemas.py` | CREATE | Pydantic: register/push request-response, evento, pull, conflicto |
| `app/sync/auth.py` | CREATE | `require_device` (header `X-Sync-Token`) → `DeviceToken` |
| `app/repositories/customer_repository.py` | MODIFY | `find_by_contact(phone, email)` para dedup |
| `app/sync/service.py` | CREATE | `SyncService`: versión, idempotencia, replay, dedup, pull |
| `app/routers/sync.py` | CREATE | `POST /sync/register`, `POST /sync/push`, `GET /sync/conflicts` |
| `app/main.py` | MODIFY | Registrar `sync_router` |
| `tests/unit/test_sync_service.py` | CREATE | Idempotencia, replay ok/conflicto, dedup match/colisión |
| `tests/unit/test_sync_router.py` | CREATE | register→token, 401 sin token, rechazo por versión, push happy |
| `app/services/inventory_service.py` | REFERENCE | `InventoryService(db, business_id).sale(SaleCreate)` commitea internamente |
| `app/models.py::SyncEvent` | REFERENCE | outbox del cliente (P1); el server no lo usa, recibe eventos por HTTP |
| `app/security.py` | REFERENCE | `require_admin_or_owner_api` (para GET /sync/conflicts) |
| `app/schemas.py::SaleCreate` | REFERENCE | `sku, quantity, unit_price?, movement_date?, note?, location_code?` |

**Firmas reales verificadas:**

```python
# app/services/inventory_service.py:1715 — sale() COMMITEA internamente
class InventoryService:
    def __init__(self, db: Session, business_id: int): ...
    def sale(self, payload: SaleCreate) -> MovementResult: ...  # 409 si stock insuficiente

# app/schemas.py:83
class SaleCreate(BaseModel):
    sku: str; quantity: float; unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None; note: Optional[str] = None
    location_code: Optional[str] = None

# app/repositories/customer_repository.py — CustomerRepository(db, business_id)
#   create(*, client_id, name, address=None, phone=None, email=None) -> Customer
# app/security.py:147 — require_admin_or_owner_api(...) -> User
# app/deps.py — session_dep() -> Session
# app/main.py:170-185 — app.include_router(...) ; los API JSON van sin prefix
```

---

## 2.2 Architecture Approach

Endpoint de sync en el servidor central (mismo repo, `app/routers/sync.py`), autenticado por
**token de dispositivo**. Flujo:

```
POST /sync/register  (bootstrap, auth: credenciales de admin/owner del negocio)
   → crea DeviceToken(business_id, location_id, label) → devuelve token + pull inicial

POST /sync/push  (auth: X-Sync-Token → DeviceToken activo)
   1. version check: si app_schema_version < MIN → 426/409 con mensaje (REQ-16)
   2. por cada evento (en orden de seq):
        - si event_id ya en AppliedSyncEvent → skip (idempotente, REQ-10)
        - "sale":    add AppliedSyncEvent(pending) → InventoryService.sale(SaleCreate(...))
                     (commit atómico interno). 409 stock → rollback + SyncConflict (REQ-11/14)
        - "customer": dedup por teléfono/email (find_by_contact):
                        match → descartar duplicado (conservar-el-primero), marcar aplicado
                        client_id colisiona sin match → sufijo + SyncConflict (REQ-12/14)
                        sin colisión → crear cliente, marcar aplicado
   3. build pull: foto completa business-scoped (REQ-13)
   → SyncPushResponse(applied_event_ids, conflicts, pull)

GET /sync/conflicts  (auth: admin/owner) → lista JSON de SyncConflict sin resolver (REQ-14)
```

**Pull = foto completa (REQ-13).** El servidor serializa las filas business-scoped de las
tablas del alcance de lectura; el **cliente tiene el mismo código**, así que recibe filas
crudas y **recomputa** stock/saldos/utilidades con los mismos services localmente. Tablas del
snapshot: `businesses` (config), `locations`, `products`, `customers`, `inventory_movements`,
`inventory_lots`, `movement_allocations`, `sales_documents`, `sales_document_items`,
`operating_expenses`, `money_extractions`, `cash_baselines`, y `users` **acotados** al negocio
(solo los de ese business_id — REQ-17). Un helper genérico serializa `(modelo, filtro)` → dicts.

**Idempotencia (REQ-10).** Tabla `applied_sync_events(event_id UNIQUE)` como ledger del
servidor. Un evento se considera aplicado solo si su `event_id` está en el ledger; el registro
se inserta en la misma transacción que el efecto (venta/cliente), de modo que un corte deja un
estado consistente y el reintento salta lo ya aplicado.

**Invariantes preservadas:** no se toca `auth.py`, `deps.py`, `db.py`, `docker-compose*`, ni la
lógica de venta/FIFO (se **reutiliza** `InventoryService.sale`). El endpoint es aditivo.

---

## 2.3 Data Model

### DeviceToken (`device_tokens`) — NEW
| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | |
| token | String(128) | **unique**, index — secreto del dispositivo (secrets.token_urlsafe) |
| business_id | Integer | index — negocio del nodo |
| location_id | Integer | nullable — ubicación del nodo |
| label | String(128) | nombre legible ("Bazar Mariela — POS1") |
| revoked | Boolean | default False — revocación sin borrar (auditable) |
| created_at | DateTime(tz) | now() |
| last_seen_at | DateTime(tz) | nullable — última sync |

### AppliedSyncEvent (`applied_sync_events`) — NEW
| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | |
| event_id | String(64) | **unique**, index — ledger de idempotencia |
| device_id | Integer | index — dispositivo origen |
| business_id | Integer | index |
| applied_at | DateTime(tz) | now() |

### SyncConflict (`sync_conflicts`) — NEW
| Campo | Tipo | Notas |
|-------|------|-------|
| id | Integer PK | |
| business_id | Integer | index |
| device_id | Integer | nullable |
| event_id | String(64) | index — evento en conflicto |
| kind | String(32) | `sale_stock` \| `customer_client_id` |
| detail | Text | JSON con el contexto (sku, cantidad, client_id, etc.) |
| resolved | Boolean | default False, index |
| created_at | DateTime(tz) | now() |

**Migración (v6):** crear las 3 tablas en ambas ramas (`CREATE TABLE IF NOT EXISTS`),
`_LATEST_SCHEMA_VERSION = 6`. **Rollback:** `DROP TABLE` de las 3, versión a 5 — aditivo.

---

## 2.4 API Contracts

### POST /sync/register
**Auth:** credenciales admin/owner del negocio (reusa `require_admin_or_owner_api`).
**Request:** `{ "business_id": int, "location_code": str, "label": str }`
**Response 200:** `{ "token": str, "device_id": int, "pull": SyncPullData }`
**Response 403:** no admin/owner.

### POST /sync/push
**Auth:** header `X-Sync-Token` (DeviceToken activo). 401 si falta/inválido/revocado.
**Request:** `{ "app_schema_version": int, "events": [SyncEventIn...] }`
**Response 200:** `{ "applied_event_ids": [str], "conflicts": [ConflictOut], "pull": SyncPullData }`
**Response 409/426:** `{ "detail": "versión incompatible, actualice la app" }` (REQ-16).
**Idempotencia:** reenviar un evento ya aplicado no lo duplica (REQ-10).

### GET /sync/conflicts?business_id=
**Auth:** admin/owner. **Response 200:** `[ConflictOut...]` sin resolver.

```
SyncEventIn:  { event_id, event_type, payload(dict), seq, prev_hash?, event_hash, location_id? }
SyncPullData: { snapshot: { <tabla>: [rows...] }, server_time }
ConflictOut:  { event_id, kind, detail, created_at }
```

---

## 2.5 Security Review

| Check | Respuesta |
|-------|-----------|
| Nuevo límite de auth | Sí: `require_device` (token de dispositivo) para `/sync/push`; admin/owner para register y conflicts |
| Datos sensibles | El pull incluye PII de clientes + hashes de usuarios acotados. Viaja sobre HTTPS (Cloudflare Tunnel ya en prod). El cifrado en reposo del nodo es P4 |
| Validación de input | Pydantic valida todos los cuerpos; `event_type` restringido a `sale`/`customer`; SaleCreate re-valida el payload de venta |
| Superficie de ataque | 3 endpoints nuevos. Token revocable (robo de equipo → revoca su token sin afectar a otros). El replay pasa por los services con sus validaciones |
| Idempotencia/anti-replay | `applied_sync_events.event_id` UNIQUE evita doble aplicación; el `event_hash` del cliente permite validar integridad de la cadena recibida |
| OWASP | AuthN/AuthZ (token + RBAC), IDOR (todo scopeado por `business_id` del token; un token no puede empujar a otro negocio) |

---

## 2.6 Trade-offs

| # | Decisión | Elegido | Alternativa | Por qué no |
|---|----------|---------|-------------|-----------|
| T1 | Idempotencia | Ledger `applied_sync_events(event_id UNIQUE)` | Confiar en `pending_sync_events` del server | El server no tiene el outbox del cliente; necesita su propio registro de lo aplicado |
| T2 | Atomicidad venta+ledger | Insertar `AppliedSyncEvent` en la sesión antes de `sale()` (commit interno los persiste juntos) | Marcar aplicado tras commit de venta | Evita ventana de doble-venta si hay corte entre commit de venta y marca |
| T3 | Dedup de clientes | Conservar-el-primero + descartar; sin remapeo | Remapear ventas al canónico | **Descubierto en grounding:** las ventas offline no referencian cliente → no hay nada que remapear (ver Hallazgos) |
| T4 | Pull | Foto completa de filas crudas; el cliente recomputa | Delta incremental / agregados precomputados | El cliente tiene el mismo código; recibir filas y recomputar es más simple y correcto en v1 |
| T5 | Auth de sync | Token por dispositivo | Sesión de usuario | Revocable por equipo; no mezcla identidad de persona con dispositivo (decisión del usuario) |

---

## 2.7 Migration & Rollback

```
v6: CREATE TABLE IF NOT EXISTS device_tokens / applied_sync_events / sync_conflicts
    (idempotente, ambas ramas). _LATEST_SCHEMA_VERSION = 6.
Rollback: DROP TABLE de las 3 + versión a 5. Aditivo, sin pérdida.
El router es aditivo: revertir = quitar include_router(sync_router) + archivos nuevos.
```

---

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría

| Dim | Riesgo | Mitigación → Instruction de envelope |
|-----|--------|--------------------------------------|
| **1. SoC/SRP** | `SyncService` podría reimplementar la lógica de venta/stock | **Reutiliza `InventoryService.sale`**; no duplica FIFO ni validación. `find_by_contact` vive en `CustomerRepository` (capa de datos), no en el service de sync |
| **2. Cohesión/Acoplamiento** | El pull serializa muchas tablas; tentación de repetir el patrón por tabla | Un helper genérico `_dump(model, *filters)` serializa filas → dicts; el snapshot es un dict de llamadas a ese helper, sin duplicar el bucle |
| **3. Capas/Límites** | El router podría meter lógica de replay | El router solo autentica y delega en `SyncService`; toda la lógica vive en el service. `require_device` en `auth.py`, no en el router |
| **5. Robustez/Resiliencia** | Cada método público de `SyncService` toca BD (replay, dedup, pull) | **CADA método público** envuelve su cuerpo en try/except con `_log.error(..., exc_info=True)` y política definida: un evento que falla inesperado → `SyncConflict` (no rompe el batch entero); el push completo captura y responde error claro. Sin excepción tácita |
| **6. Consistencia Transaccional** | Venta + marca de aplicado deben ser atómicas; el ledger y el efecto no pueden divergir | T2 (design): `AppliedSyncEvent` se añade a la sesión ANTES de `sale()`; el commit interno de `sale()` los persiste juntos; en conflicto, `rollback()` descarta ambos y registra conflicto. Especificado literalmente en el envelope de `_apply_sale` |

Sin hallazgos que bloqueen Fase 3 (los 2 de grounding ya están resueltos en el diseño).

---

## 2.10 Delegation

Ninguna. Del dominio del SDD Lead (router + service + modelos, reusando services existentes).
`security-architect` sería opcional por ser auth nueva, pero el patrón (token revocable +
RBAC + scoping por business_id) es estándar y está cubierto por §2.5.

---

## Interfaces nuevas (contratos para Fase 3)

```python
# app/sync/auth.py
def require_device(request: Request, db: Session = Depends(session_dep)) -> DeviceToken:
    """Lee X-Sync-Token; 401 si falta/no existe/revocado. Actualiza last_seen_at."""

# app/repositories/customer_repository.py  (método nuevo)
def find_by_contact(self, phone: str | None, email: str | None) -> Customer | None:
    """Cliente del negocio cuyo teléfono O email coincide (no vacío). El más antiguo si varios."""

# app/sync/service.py
MIN_APP_SCHEMA_VERSION = 6

class SyncService:
    def __init__(self, db: Session, device: DeviceToken): ...
    def push(self, app_schema_version: int, events: list[dict]) -> dict:
        """Valida versión, aplica eventos idempotentes, arma pull. Devuelve dict de respuesta."""
    def build_pull(self) -> dict:
        """Foto completa business-scoped (snapshot dict + server_time)."""
    # privados: _apply_sale(ev) / _apply_customer(ev) / _dump(model, *filters) / _record_conflict(...)
```
