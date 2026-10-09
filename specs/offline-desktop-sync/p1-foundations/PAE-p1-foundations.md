# PAE-P1: offline-desktop-sync — Fundamentos (outbox + rangos de SKU)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto.
     2. Leer sección 3 (Arquitectura) completa antes de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (5.0) — sin excepción.
     4. Una tarea a la vez. Verificación de cada una antes de la siguiente.
     5. Si algo no está especificado aquí: no se inventa, se reporta.
     6. Archivos fuera del inventario (3.2): no se tocan.
     7. Al terminar cada batch: /compact.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/offline-sync-p1-foundations` |
| **Fecha** | 2026-08-28 |
| **Versión actual** | v0.4.6 |
| **Versión objetivo** | v0.4.7 (PATCH) |
| **Slice** | P1 de 5 (`offline-desktop-sync`) |
| **PAE anterior** | ninguno (primer slice del épico) |
| **Fase** | planificación (Claude Code + Opus 4.8) → ejecución (OpenCode + Qwen3.6-35B-A3B) |

### Documentos fuente

| Fase SDD | Output |
|----------|--------|
| Brainstorming | `docs/plans/2026-08-28-app-escritorio-offline-sync-design.md` |
| Phase 1 — Requirements (épico) | `specs/offline-desktop-sync/requirements.md` |
| Phase 2 — Design (P1) | `specs/offline-desktop-sync/p1-foundations/design.md` |
| Phase 3 — Task Envelopes (P1) | `specs/offline-desktop-sync/p1-foundations/tasks.md` |

### Skills asignadas a OpenCode

- [ ] `backend-developer` — helper de SKU, wiring de generadores, outbox
- [ ] `data-engineer` — modelo `SyncEvent` + migración v5
- [ ] `test-automation-engineer` — tests unit de SKU y outbox

---

## 1. Objetivo

### 1.1 Qué se construye

Las dos piezas fundacionales del épico offline-sync, testeables sobre el backend actual sin
desktop ni red: (A) **rangos de SKU de 10k por negocio** — un helper único que numera los SKU
autogenerados dentro del rango reservado a cada negocio (`business_id * 10000`), reemplazando
los dos generadores globales duplicados de hoy; (B) **outbox** `pending_sync_events` — tabla
append-only con hash encadenado que detecta manipulación, más su helper `append_event`/
`verify_chain`.

### 1.2 Por qué

El épico requiere (a) que la numeración de SKU no colisione entre negocios cuando se generen
productos (hoy dos generadores divergentes ya causaron el bug de prod v0.4.6) y (b) un registro
íntegro de eventos offline que P2 (backend de sync) y P3 (cliente) consumirán. P1 asienta ambas
bases con cero superficie de red y cero riesgo desktop.

### 1.3 Valor esperado

- Un único punto de numeración de SKU (elimina la duplicación que causó v0.4.6).
- SKUs autogenerados deterministas y disjuntos por negocio, sin retries por colisión cruzada.
- Outbox con integridad verificable, listo para que P2/P3 lo pueblen y lean.

### 1.4 Qué NO se toca

- `app/auth.py`, `app/deps.py`, `app/db.py`, `docker-compose*.yml` — sin cambios.
- Lógica de FIFO, venta, traspaso, compra (más allá de cómo se numera el SKU).
- Ningún endpoint HTTP (contratos sin cambios).
- `APP_MODE` / runtime desktop / SQLCipher / pywebview → **P4**, no P1.
- Escritores del outbox (venta/cliente) → **P3**. Replay/dedup → **P2**.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación |
|---|----------|-------------|---------------|
| D1 | Rango de SKU **derivado** `business_id * 10000` (helper puro, sin estado) | Columna `sku_range_base` + backfill | Cero migración/backfill; rangos disjuntos por construcción (todo SKU existente < 10000 ≤ toda base) |
| D2 | Helper de SKU **puro** (recibe `list[str]`, no la sesión) | Helper que consulta BD | Mantiene la consulta en la capa que ya tiene la sesión; testeable sin BD |
| D3 | Outbox en **modelo compartido** (`models.py` + `app/sync/`) | Tabla solo-desktop | El backend usa `create_all`; tabla inerte en el server es más simple y testeable con el conftest actual |
| D4 | Integridad por **sha256 encadenado** (`prev\|id\|type\|payload`) | HMAC con secreto de nodo | HMAC necesita gestión de secreto (llega en P4/DPAPI); en P1 basta detección de manipulación |
| D5 | Se conserva el **retry loop** existente (5 intentos) en ambos generadores | Quitarlo al tener rangos | Red de seguridad ante bordes; costo nulo |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama

```
★ NUEVO   ✦ MODIFICADO   · sin cambios

  ProductService._generate_sku ✦ ──┐
                                    ├─► sku_range.next_sku_in_range() ★  (helper puro)
  ui_purchases._generate_auto_sku ✦─┘

  models.SyncEvent ★  ──► tabla pending_sync_events ★ (migración v5 ✦ en migrations.py)
        ▲
        │
  app/sync/outbox.py ★  (append_event / verify_chain / compute_event_hash)
        ▲
        └── tests/unit/test_sync_outbox.py ★   · conftest fixtures (db_session, business_id)
```

### 3.2 Inventario de archivos (scope duro)

> `git diff --name-only develop` debe mostrar EXACTAMENTE estos:

| Archivo | Operación |
|---------|-----------|
| `app/services/sku_range.py` | CREATE |
| `tests/unit/test_sku_range.py` | CREATE |
| `app/services/product_service.py` | MODIFY |
| `app/routers/ui_purchases.py` | MODIFY |
| `app/models.py` | MODIFY |
| `app/migrations.py` | MODIFY |
| `app/sync/__init__.py` | CREATE |
| `app/sync/outbox.py` | CREATE |
| `tests/unit/test_sync_outbox.py` | CREATE |
| `VERSION` | MODIFY |

### 3.3 Dependencias existentes (firmas reales)

```python
# app/services/product_service.py:14 — ProductService.__init__ setea self._business_id: int
# app/services/product_service.py:25 — _generate_sku(self, prefix="SKU", width=6) -> str
# app/routers/ui_purchases.py:345 — closure _generate_auto_sku(prefix, width); `db`, `bid` en scope
# app/repositories/purchase_repository.py:31 — list_sku_prefixes(prefix) -> list[str] (SKUs globales)
# app/migrations.py:41 — _LATEST_SCHEMA_VERSION = 4
# app/models.py:1-9 — Optional, datetime, Index, mapped_column, Base ya importados
# tests/conftest.py — db_session (crea Business code="test"), business_id -> int
```

### 3.4 Interfaces nuevas

Ver `design.md` §"Interfaces nuevas". Contratos: `sku_range.sku_range_base` /
`next_sku_in_range`; `outbox.compute_event_hash` / `append_event` / `verify_chain`;
modelo `SyncEvent` (tabla `pending_sync_events`).

---

## 4. Fases de implementación (Task Envelopes)

> Los 9 envelopes están en `specs/offline-desktop-sync/p1-foundations/tasks.md`.
> OpenCode ejecuta UNO a la vez. Batches: **T1-T3 · T4-T6 · T7-T9** (`/compact` entre batches).

| # | Envelope | Files | Depends |
|---|----------|-------|---------|
| T1 | Helper `sku_range.py` | sku_range.py | none |
| T2 | Tests del helper | test_sku_range.py | T1 |
| T3 | `_generate_sku` usa helper | product_service.py | T1 |
| T4 | `_generate_auto_sku` usa helper | ui_purchases.py | T1 |
| T5 | Modelo `SyncEvent` | models.py | none |
| T6 | Migración v5 | migrations.py | T5 |
| T7 | Helper outbox | sync/__init__.py, sync/outbox.py | T5 |
| T8 | Tests outbox | test_sync_outbox.py | T7 |
| T9 | Version bump | VERSION | T1-T8 |

---

## 5. Plan de tests y verificación

### 5.0 Pre-implementación (OBLIGATORIO antes de tocar código)

```bash
# Toda la suite unit debe pasar ANTES de empezar (222 unit esperados verdes)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```
Si falla, NO empezar. Reportar y esperar.

### 5.1 Verificación por envelope

Cada envelope trae su bloque `#### Verification` con comando → PASS/FAIL. No avanzar si falla.

### 5.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit/test_product_service.py` — sin regresión (T3)
- [ ] `tests/unit/test_repositories.py`, `tests/unit/test_ui_regressions.py` — sin regresión (T4)
- [ ] Suite unit completa tras T6 (migración) y al cierre.

### 5.4 Tests e2e

**No aplica a P1.** P1 no añade flujo de usuario ni UI (solo helpers internos y una tabla
inerte). Los e2e multi-nodo obligatorios del épico son el slice **P5** (REQ-20), que valida el
flujo completo offline→sync. Esta excepción es explícita y trazada, no una omisión.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Un SKU manual antiguo cae dentro de un rango de negocio y colisiona con un autogenerado | Baja | Bajo | 409 puntual al crear producto | El retry loop (5 intentos) absorbe el borde; el helper solo cuenta números en rango | git revert generadores |
| R2 | La migración v5 falla en una BD prod con datos raros | Baja | Medio | Startup error en deploy | `CREATE TABLE/INDEX IF NOT EXISTS` idempotente; probar en dev antes de main | `DROP TABLE pending_sync_events` + versión a 4 |
| R3 | `business_id` grande (>99 negocios) rompe zfill(6) | Muy baja | Bajo | SKU con 7 dígitos | Sigue siendo string válido y único; documentado; a ~99 negocios está lejísimos | ninguno necesario |
| R4 | Regresión en tests de product/purchase por el cambio de generador | Baja | Medio | Tests rojos en T3/T4 | Cambio conservador (mismo formato, misma firma); verificación por envelope | git revert del envelope |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit

- [ ] **Pre-check:** suite unit verde antes de empezar (guardar output)
- [ ] **Tests:** `pytest tests/unit -q` — 100% verde tras los cambios (222 previos + 11 nuevos: 6 SKU + 5 outbox)
- [ ] **App arranca:** `uvicorn app.main:app` sin errores (migración v5 corre limpia)
- [ ] **Invariantes:** no se tocó `auth.py`/`deps.py`/`db.py`/`docker-compose*`
- [ ] **Scope:** `git diff --name-only develop` == exactamente los 10 archivos de 3.2
- [ ] **Limpieza:** sin archivos temporales

### 7.2 Script de verificación

```bash
#!/bin/bash
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:
echo "=== unit suite ==="
python3 -m pytest tests/unit -q
echo "=== nuevos tests P1 ==="
python3 -m pytest tests/unit/test_sku_range.py tests/unit/test_sync_outbox.py -v
echo "=== version ==="
grep -q "0.4.7" VERSION && echo "VERSION ok"
echo "=== scope ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados

```
feat(sku): numeración de SKU por rango de negocio (10k) en un helper único
feat(sync): tabla pending_sync_events (outbox) + hash encadenado
chore(release): v0.4.7
```

### 7.4 Criterios de aceptación (de los Gherkin de requirements.md)

- [ ] REQ-15: SKU autogenerado de un negocio cae en `[business_id*10000, +10000)` y no colisiona con otro negocio.
- [ ] REQ-9: manipular el payload de un evento rompe `verify_chain` (devuelve False).

---

> **Fin de P1.** Al cerrar (verde + merge a develop + deploy dev verificado), se genera el
> siguiente slice **P2 — Backend de sincronización** vía SDD-Agent, reusando este mismo
> `requirements.md` como insumo. No empezar P2 antes de que P1 esté verde en dev.
