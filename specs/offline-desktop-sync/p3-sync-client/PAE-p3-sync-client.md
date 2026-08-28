# PAE-P3: offline-desktop-sync — Cliente de sync + escritura offline

<!-- INSTRUCCIONES OPENCODE: leer CLAUDE.md + sección 3; verificación 5.0 (P1+P2 verdes);
     una tarea a la vez; solo archivos del inventario 3.2; /compact entre batches. -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` · **Rama de trabajo** | `feature/offline-sync-p3-client` |
| **Fecha** | 2026-08-28 |
| **Versión** | v0.4.7 (bump en P1; P3 no re-bumpea) |
| **Slice** | P3 de 5 (`offline-desktop-sync`) |
| **PAE anterior** | P2 — Backend de sync (verde en dev) |
| **Fase** | Claude Code (Opus 4.8) planifica → OpenCode + Qwen3.6-35B-A3B ejecuta |

### Documentos fuente
`specs/offline-desktop-sync/requirements.md` · `.../p3-sync-client/design.md` · `.../p3-sync-client/tasks.md`

### Skills asignadas
- [ ] `data-engineer` — `SyncState` + migración v7
- [ ] `backend-developer` — settings, hooks de outbox, cliente httpx, router
- [ ] `frontend-developer` — banner + gate de pestañas en `dashboard.html`
- [ ] `test-automation-engineer` — tests del cliente y hooks

---

## 1. Objetivo

### 1.1 Qué se construye
El comportamiento de **nodo desktop** sobre el mismo backend: escritura offline (venta y alta de
cliente encolan un evento en el outbox), un cliente de sincronización (`app/sync/client.py`) que
empuja los pendientes al servidor central (P2), aplica la foto de lectura devuelta y actualiza la
fecha de última sync; un botón "Sincronizar" y un banner persistente de última sincronización en
todas las pestañas; y el ocultamiento de las pestañas que requieren conexión (compra, traspaso,
copiloto). Todo gated por `APP_MODE=desktop`; en modo `server` es no-op.

### 1.2 Por qué
Cierra el flujo funcional offline: el nodo opera sin conexión (vende, da de alta clientes,
consulta lecturas de la última sync) y sincroniza a demanda contra el servidor que P2 ya sabe
recibir.

### 1.3 Valor esperado
- Ventas y altas de cliente funcionan sin internet y quedan encoladas íntegras (hash de P1).
- Un botón sincroniza push+pull; el banner muestra siempre cuándo fue la última sync.
- Cero cambio de comportamiento en el servidor central (modo server intacto).

### 1.4 Qué NO se toca
- `auth.py`, `deps.py`, `db.py`, `docker-compose*`, RBAC por rol existente.
- Empaquetado desktop, SQLCipher, DPAPI, cifrado del backup → **P4**.
- e2e multi-nodo → **P5**.
- Comportamiento online del servidor (los hooks/banner son no-op si `APP_MODE != desktop`).

---

## 2. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | `APP_MODE` aterriza en P3 (primer consumidor) | Los hooks, el banner y el gating lo necesitan; sin consumidor en P1 era prematuro |
| D2 | Cliente HTTP con **httpx** | Ya está en requirements; sin dependencia nueva |
| D3 | `apply_pull` **transaccional** (reemplazo completo) | Foto completa (P2); rollback evita tablas de lectura a medias |
| D4 | Gating por ocultar pestañas en desktop | Suficiente en v1; el server no cambia |
| D5 | Estado en tabla `sync_state` (fila única) | Vive en el .db (cifrado en P4); una sola fuente |
| D6 | Backup del outbox = export JSONL rotativo local | Barato, sin red, reduce ventana de pérdida (REQ-18) |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama
```
★ NUEVO   ✦ MODIFICADO

  Venta (ui_sales) ✦ ─┐
  Alta cliente (ui_customers) ✦ ─┴─► append_event (outbox P1) ──► pending_sync_events
                                                                        │
  Botón "Sincronizar" (dashboard.html ✦) ──POST /ui/sync──► ui_sync.py ★ ──► client.sync() ★
        │                                                                        ├─ httpx POST /sync/push (P2)
  Banner última sync (dashboard.html ✦ · ui_home.py ✦ · sync_state ★)           ├─ marca eventos synced
                                                                                 └─ apply_pull() ★ (reemplaza tablas de lectura)
  settings.APP_MODE ✦ / is_desktop()   ·   migración v7 (sync_state) ✦
```

### 3.2 Inventario de archivos (scope duro)
| Archivo | Operación |
|---------|-----------|
| `app/settings.py` | MODIFY |
| `app/models.py` | MODIFY (`SyncState`) |
| `app/migrations.py` | MODIFY (v7) |
| `app/routers/ui_sales.py` | MODIFY (hook) |
| `app/routers/ui_customers.py` | MODIFY (hook) |
| `app/sync/client.py` | CREATE |
| `app/routers/ui_sync.py` | CREATE |
| `app/templates/partials/sync_status.html` | CREATE |
| `app/main.py` | MODIFY |
| `app/routers/ui_home.py` | MODIFY |
| `app/templates/dashboard.html` | MODIFY |
| `tests/unit/test_sync_client.py` | CREATE |

### 3.3 Dependencias existentes (firmas reales)
Ver `design.md` §2.1 (ui_sales `sale`/`sale_barcode`, ui_customers `customer_create`,
ui_home `dashboard`, `append_event` de P1, `PushResponse` de P2, httpx disponible).

### 3.4 Interfaces nuevas
`settings.is_desktop()`, `client.sync/apply_pull/_pending_events`, router `ui_sync`
(`POST /ui/sync`, `GET /ui/sync/status`), partial `sync_status.html`. Ver `design.md`.

---

## 4. Fases de implementación (Task Envelopes)

12 envelopes en `tasks.md`. Batches: **T1-T3 · T4-T6 · T7-T9 · T10-T12**.

| # | Envelope | Files | Depends |
|---|----------|-------|---------|
| T1 | `APP_MODE` + `is_desktop` | settings.py | none |
| T2 | Modelo `SyncState` | models.py | none |
| T3 | Migración v7 | migrations.py | T2 |
| T4 | Hook outbox en venta | ui_sales.py | T1 |
| T5 | Hook outbox en cliente | ui_customers.py | T1 |
| T6 | Cliente sync (push) | sync/client.py | T2 |
| T7 | `apply_pull` transaccional | sync/client.py | T6 |
| T8 | Router `ui_sync` + partial | ui_sync.py, sync_status.html | T6,T7 |
| T9 | Registrar en main.py | main.py | T8 |
| T10 | Contexto dashboard | ui_home.py | T1,T2 |
| T11 | Banner + gate pestañas | dashboard.html | T10 |
| T12 | Tests cliente/hooks | test_sync_client.py | T4,T5,T7 |

---

## 5. Plan de tests y verificación

### 5.0 Pre-implementación (OBLIGATORIO)
```bash
# P1 y P2 mergeados y verdes.
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```

### 5.1 Verificación por envelope
Cada envelope trae su `#### Verification`. No avanzar si falla.

### 5.3 Tests que deben seguir pasando
- [ ] `test_ui_sales_context.py`, `test_ui_auth.py`, `test_ui_regressions.py` — sin regresión (hooks/banner no-op en modo server).

### 5.4 Tests e2e
El flujo offline→sync end-to-end es **P5** (REQ-20, multi-nodo). P3 se valida con unit
(hooks encolan, cliente recolecta/aplica). Excepción explícita y trazada.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación | Rollback |
|---|--------|-------|---------|------------|----------|
| R1 | `apply_pull` deja tablas de lectura a medias ante error | Baja | Alto | Transaccional con rollback (T7) | re-sync |
| R2 | Hook de outbox rompe el flujo de venta online | Baja | Alto | Gated por `is_desktop()`; no-op en server; verificación sin regresión | git revert hook |
| R3 | Sync sin conexión bloquea la UI | Media | Medio | `sync()` captura fallo de red → dict de error, no excepción | — |
| R4 | Se marca `synced` un evento que el server no aplicó | Baja | Alto | Solo se marcan los `event_id` en `applied_event_ids` | evento sigue `pending` |
| R5 | Migración v7 falla en prod | Baja | Bajo | `IF NOT EXISTS`; tabla inerte en server | DROP + versión a 6 |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit
- [ ] Pre-check verde (P1+P2) · Tests verdes tras cambios (+4 nuevos) · App arranca (v7 limpia)
- [ ] **Modo server intacto:** render del dashboard y flujo de venta sin cambios con `APP_MODE` ausente
- [ ] Invariantes: no se tocó `auth.py`/`deps.py`/`db.py`/`docker-compose*`
- [ ] Scope: `git diff --name-only develop` == los 12 archivos de 3.2
- [ ] Limpieza: sin temporales

### 7.2 Script de verificación
```bash
#!/bin/bash
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:
python3 -m pytest tests/unit -q
python3 -m pytest tests/unit/test_sync_client.py -v
python3 -c "from app.main import app; assert any(r.path=='/ui/sync' for r in app.routes)"
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados
```
feat(desktop): APP_MODE + is_desktop + sync_state (migración v7)
feat(offline): encolar venta y alta de cliente en el outbox (modo desktop)
feat(sync): cliente httpx push+apply_pull + endpoint /ui/sync
feat(ui): banner de última sync + gate de pestañas offline
test(sync): cobertura del cliente y hooks
```

### 7.4 Criterios de aceptación (Gherkin)
- [ ] REQ-5/6: en modo desktop, una venta / alta de cliente encola un evento en `pending_sync_events`.
- [ ] REQ-8: el banner muestra `last_synced_at` en todas las pestañas.
- [ ] REQ-17: `apply_pull` escribe solo los `users` del negocio (los que devolvió el server).
- [ ] REQ-19: en modo desktop, las pestañas de compra/traspaso/copiloto no se muestran.

---

> **Fin de P3.** Al cerrar (verde + merge + deploy dev), se genera **P4 — Empaquetado desktop +
> cifrado** vía SDD-Agent. No empezar P4 antes de que P3 esté verde.
