# PAE-P2: offline-desktop-sync — Backend de sincronización (servidor central)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto.  2. Leer sección 3 completa.
     3. VERIFICACIÓN PRE-IMPLEMENTACIÓN (5.0) — sin excepción.  P1 debe estar verde.
     4. Una tarea a la vez; verificar antes de la siguiente.  5. No inventar: reportar.
     6. Solo archivos del inventario 3.2.  7. /compact entre batches.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/offline-sync-p2-backend` |
| **Fecha** | 2026-08-28 |
| **Versión actual** | v0.4.7 (bumpeada en P1) |
| **Versión objetivo** | v0.4.7 (mismo ciclo del épico) |
| **Slice** | P2 de 5 (`offline-desktop-sync`) |
| **PAE anterior** | P1 — Fundamentos (debe estar verde en dev) |
| **Fase** | planificación (Claude Code + Opus 4.8) → ejecución (OpenCode + Qwen3.6-35B-A3B) |

### Documentos fuente

| Fase | Output |
|------|--------|
| Requirements (épico) | `specs/offline-desktop-sync/requirements.md` |
| Design (P2) | `specs/offline-desktop-sync/p2-sync-backend/design.md` |
| Task Envelopes (P2) | `specs/offline-desktop-sync/p2-sync-backend/tasks.md` |

### Skills asignadas a OpenCode
- [ ] `data-engineer` — 3 modelos + migración v6
- [ ] `api-designer` — schemas del protocolo
- [ ] `backend-developer` — auth de dispositivo, `SyncService`, router
- [ ] `test-automation-engineer` — tests de service y router

### Decisiones cerradas con el usuario
- Auth: **token por dispositivo** (revocable). Bootstrap: **primer sync online obligatorio**.

---

## 1. Objetivo

### 1.1 Qué se construye
El backend de sincronización en el servidor central: endpoint `/sync/register` (bootstrap con
token de dispositivo), `/sync/push` (aplica eventos offline de forma idempotente reusando
`InventoryService.sale`, deduplica clientes, registra conflictos, devuelve la foto completa de
lectura), y `/sync/conflicts` (lista para revisión admin). Nada de cliente ni desktop todavía.

### 1.2 Por qué
El servidor debe saber **recibir y aplicar** los eventos offline antes de que exista un cliente
que los envíe (P3). Reusa la lógica de negocio ya probada en vez de duplicarla.

### 1.3 Valor esperado
- Un servidor que aplica ventas offline con las mismas validaciones (stock/FIFO) que online.
- Idempotencia real (reenviar no duplica) y conflictos explícitos (nunca pérdida silenciosa).
- Base de auth por dispositivo (revocable) para los nodos.

### 1.4 Qué NO se toca
- `auth.py`, `deps.py`, `db.py`, `docker-compose*`, lógica de venta/FIFO (se **reutiliza**).
- Cliente offline / outbox writers / botón sincronizar / banner → **P3**.
- Empaquetado desktop / SQLCipher / DPAPI → **P4**.
- UI HTMX de la pantalla de conflictos → se expone JSON en P2; la pantalla admin visual se
  evalúa en P3 (mismo server). REQ-14 queda cubierto a nivel de datos + endpoint en P2.

---

## 2. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Idempotencia por ledger `applied_sync_events(event_id UNIQUE)` | El server necesita su propio registro de lo aplicado |
| D2 | `AppliedSyncEvent` se marca ANTES de `sale()` (que commitea internamente) | Atomicidad venta+marca; evita doble-venta ante corte |
| D3 | Dedup de clientes = conservar-el-primero, **sin remapeo** | **Grounding:** las ventas offline no referencian cliente (ver design §Hallazgos) |
| D4 | Pull = foto completa de filas crudas; el cliente recomputa | El cliente tiene el mismo código; más simple y correcto en v1 |
| D5 | Auth por token de dispositivo revocable | Decisión del usuario; robo de equipo → revoca solo su token |
| D6 | `users` del pull acotado por `business_id` | REQ-17: el nodo solo conoce sus usuarios |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama
```
★ NUEVO   ✦ MODIFICADO

  Nodo desktop (P3) ──HTTP──►  app/routers/sync.py ★
                                  ├─ POST /sync/register (require_admin_or_owner_api) → DeviceToken ★ + pull
                                  ├─ POST /sync/push (require_device ★) ──► SyncService ★
                                  │         ├─ version check (REQ-16)
                                  │         ├─ idempotencia: AppliedSyncEvent ★
                                  │         ├─ _apply_sale ─► InventoryService.sale (REUSO) · SyncConflict ★
                                  │         ├─ _apply_customer ─► CustomerRepository.find_by_contact ✦
                                  │         └─ build_pull ─► snapshot business-scoped
                                  └─ GET /sync/conflicts (admin/owner)
  app/main.py ✦ (include_router)   ·  app/migrations.py ✦ (v6)  ·  app/models.py ✦ (3 modelos)
```

### 3.2 Inventario de archivos (scope duro)
> `git diff --name-only develop` (respecto al estado post-P1) debe mostrar EXACTAMENTE:

| Archivo | Operación |
|---------|-----------|
| `app/models.py` | MODIFY (3 modelos) |
| `app/migrations.py` | MODIFY (v6) |
| `app/sync/schemas.py` | CREATE |
| `app/sync/auth.py` | CREATE |
| `app/repositories/customer_repository.py` | MODIFY |
| `app/sync/service.py` | CREATE |
| `app/routers/sync.py` | CREATE |
| `app/main.py` | MODIFY |
| `tests/unit/test_sync_service.py` | CREATE |
| `tests/unit/test_sync_router.py` | CREATE |

### 3.3 Dependencias existentes (firmas reales)
```python
# InventoryService(db, business_id).sale(SaleCreate) -> MovementResult  (COMMITEA; 409 sin stock)
# SaleCreate(sku, quantity, unit_price?, movement_date?, note?, location_code?)
# CustomerRepository(db, business_id).create(*, client_id, name, address?, phone?, email?) -> Customer
# require_admin_or_owner_api(...) -> User ; require_active_business_id(db, request) -> int
# session_dep() -> Session ; app.include_router(...) en main.py (API JSON sin prefix)
# _LATEST_SCHEMA_VERSION = 5 (post-P1) → 6
```

### 3.4 Interfaces nuevas
Ver `design.md` §"Interfaces nuevas": `require_device`, `CustomerRepository.find_by_contact`,
`SyncService(db, device)` con `push` / `build_pull` + privados.

---

## 4. Fases de implementación (Task Envelopes)

Los 13 envelopes están en `tasks.md`. OpenCode ejecuta uno a la vez. Batches:
**T1-T3 · T4-T6 · T7-T9 · T10-T13** (`/compact` entre batches).

| # | Envelope | Files | Depends |
|---|----------|-------|---------|
| T1 | Modelos sync | models.py | none |
| T2 | Migración v6 | migrations.py | T1 |
| T3 | Schemas sync | sync/schemas.py | none |
| T4 | `require_device` | sync/auth.py | T1 |
| T5 | `find_by_contact` | customer_repository.py | none |
| T6 | SyncService (init/versión/idempotencia/push) | sync/service.py | T1,T3 |
| T7 | `_apply_sale` | sync/service.py | T6 |
| T8 | `_apply_customer` | sync/service.py | T5,T6 |
| T9 | `build_pull` | sync/service.py | T6 |
| T10 | Router sync | routers/sync.py | T3,T4,T6,T9 |
| T11 | Registrar en main.py | main.py | T10 |
| T12 | Tests service | test_sync_service.py | T7,T8,T9 |
| T13 | Tests router | test_sync_router.py | T10,T11 |

---

## 5. Plan de tests y verificación

### 5.0 Pre-implementación (OBLIGATORIO)
```bash
# P1 debe estar mergeado y verde. Suite unit completa verde antes de empezar.
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```
Si falla, NO empezar.

### 5.1 Verificación por envelope
Cada envelope trae su `#### Verification`. No avanzar si falla.

### 5.3 Tests existentes que deben seguir pasando
- [ ] `tests/unit/test_repositories.py` (T5 añade método, no rompe) · suite completa tras T2/T11.

### 5.4 Tests e2e
**No aplica a P2 de forma aislada.** P2 es API backend; su validación de flujo completo
(offline→sync→estado canónico) es el slice **P5** (e2e multi-nodo, REQ-20). P2 se cubre con
tests unit de `SyncService` (idempotencia, dedup, versión) y del router (401/409). Excepción
explícita y trazada.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Doble aplicación de una venta ante corte | Media | Alto | Venta duplicada en el server | D2: marca de idempotencia atómica con el commit interno de `sale()` | ledger permite detectar; `applied_sync_events` |
| R2 | Un evento corrupto rompe el batch entero | Media | Medio | Push falla completo | `push` captura por evento → conflicto, sigue con el resto | — |
| R3 | Pull expone datos de otro negocio (IDOR) | Baja | Alto | Nodo ve datos ajenos | Todo scopeado por `business_id` del token; `users` acotado | revocar token |
| R4 | Token filtrado (robo de equipo) | Media | Alto | Sync no autorizada | Token revocable por dispositivo (`revoked=true`) sin afectar a otros | revocar token |
| R5 | Migración v6 falla en prod | Baja | Medio | Startup error | `CREATE TABLE IF NOT EXISTS`; probar en dev antes de main | DROP TABLE + versión a 5 |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit
- [ ] **Pre-check:** suite unit verde (con P1) antes de empezar
- [ ] **Tests:** `pytest tests/unit -q` verde tras los cambios (+6 nuevos: 4 service + 2 router)
- [ ] **App arranca:** `uvicorn app.main:app` (migración v6 limpia; `/sync/*` registrado)
- [ ] **Invariantes:** no se tocó `auth.py`/`deps.py`/`db.py`/`docker-compose*`; venta/FIFO reutilizada, no duplicada
- [ ] **Scope:** `git diff --name-only develop` == exactamente los 10 archivos de 3.2
- [ ] **Limpieza:** sin temporales

### 7.2 Script de verificación
```bash
#!/bin/bash
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:
python3 -m pytest tests/unit -q
python3 -m pytest tests/unit/test_sync_service.py tests/unit/test_sync_router.py -v
python3 -c "from app.main import app; assert any(r.path.startswith('/sync') for r in app.routes)"
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados
```
feat(sync): device tokens, applied-events ledger y conflicts (modelos + migración v6)
feat(sync): SyncService — replay idempotente vía services + dedup de clientes
feat(sync): endpoints /sync/register, /sync/push, /sync/conflicts
test(sync): cobertura de service y router
```

### 7.4 Criterios de aceptación (Gherkin de requirements.md)
- [ ] REQ-10: reenviar un `event_id` ya aplicado no duplica (test idempotencia).
- [ ] REQ-11: venta sin stock en el server → `SyncConflict`, no se aplica silenciosamente.
- [ ] REQ-12: mismo teléfono/email en dos nodos → se conserva el primero (dedup).
- [ ] REQ-16: `app_schema_version` viejo → 409 con mensaje claro.
- [ ] REQ-17: el pull solo incluye `users` del negocio del token.

---

> **Fin de P2.** Al cerrar (verde + merge a develop + deploy dev), se genera **P3 — Cliente de
> sync + escritura offline** vía SDD-Agent. No empezar P3 antes de que P2 esté verde en dev.
