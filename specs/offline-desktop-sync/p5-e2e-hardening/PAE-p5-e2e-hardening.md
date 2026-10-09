# PAE-P5: offline-desktop-sync — e2e multi-nodo + endurecimiento (cierre)

<!-- INSTRUCCIONES OPENCODE: leer CLAUDE.md + sección 3; P1-P4 verdes; una tarea a la vez;
     el e2e se corre con -m e2e --no-cov; solo archivos del inventario 3.2. -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` · **Rama de trabajo** | `feature/offline-sync-p5-e2e` |
| **Fecha** | 2026-08-28 |
| **Versión** | v0.4.7 (bump en P1; P5 no re-bumpea) |
| **Slice** | P5 de 5 (`offline-desktop-sync`) — **cierre del épico** |
| **PAE anterior** | P4 — Desktop + cifrado (verde en dev) |
| **Fase** | Claude Code (Opus 4.8) planifica → OpenCode + Qwen3.6-35B-A3B ejecuta |

### Documentos fuente
`specs/offline-desktop-sync/requirements.md` · `.../p5-e2e-hardening/design.md` · `.../p5-e2e-hardening/tasks.md`

### Skills asignadas
- [ ] `backend-developer` — single-source de versión
- [ ] `test-automation-engineer` — fixture + e2e multi-nodo
- [ ] `devops-engineer` — verify.sh
- [ ] `technical-writer` — documentación viva

---

## 1. Objetivo

### 1.1 Qué se construye
La validación end-to-end del épico y su endurecimiento de cierre: un test **e2e multi-nodo**
(dos nodos + servidor central, con un conflicto de venta y una deduplicación de cliente) que
verifica la convergencia canónica (REQ-20); el **single-source** de la versión de protocolo de
sync; el script `verify.sh` integral del épico; y la actualización de la documentación viva.

### 1.2 Por qué
Los slices P1-P4 construyen las piezas; P5 prueba que **juntas convergen correctamente** bajo el
escenario real (dos nodos, conflicto) y deja el épico verificable de un comando.

### 1.3 Valor esperado
- Un e2e que falla si la convergencia (idempotencia/dedup/conflicto) se rompe en el futuro.
- Cero número mágico de versión de protocolo (no drifta al próximo bump de esquema).
- `verify.sh`: un comando valida todo el épico (unit + e2e).

### 1.4 Qué NO se toca
- Lógica de negocio, endpoints de sync (P2), cliente (P3), empaquetado (P4) — solo se prueban.
- La fixture `live_server` existente (se **añade** `central_server`, no se modifica).
- Build Windows: su validación es manual (P4/README), no CI.

---

## 2. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Nodos = emisores de eventos HTTP contra el central | El cliente ya está en unit (P3); el valor de REQ-20 es la convergencia del central |
| D2 | Fixture `central_server` nueva `(base_url, db_path)` | No romper los e2e que usan `live_server` |
| D3 | `app_schema_version` = `_LATEST_SCHEMA_VERSION` | Elimina el literal `7` que driftaría (dim6) |
| D4 | `verify.sh` corre unit + e2e de sync + checks de scope | DoD del épico exige verificación integral |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama
```
★ NUEVO   ✦ MODIFICADO

  central_server ★ (subproceso uvicorn + db_path, siembra stock)
        ▲ HTTP POST /sync/push (X-Sync-Token)
  Nodo A (eventos) ──┐
  Nodo B (eventos) ──┴──► /sync/push (P2) ──► convergencia en db_path
                                                  │
  test_sync_multinode_e2e.py ★ ── aserciones sobre db_path (ventas, dedup, conflicto, idempotencia)
  client.py ✦ (versión single-source)   ·   verify.sh ★   ·   CLAUDE.md ✦ (doc viva)
```

### 3.2 Inventario de archivos (scope duro)
| Archivo | Operación |
|---------|-----------|
| `app/sync/client.py` | MODIFY |
| `tests/e2e/conftest.py` | MODIFY (fixture nueva) |
| `tests/e2e/test_sync_multinode_e2e.py` | CREATE |
| `specs/offline-desktop-sync/verify.sh` | CREATE |
| `CLAUDE.md` | MODIFY |

### 3.3 Dependencias existentes (firmas reales)
Ver `design.md` §2.1 (`live_server`/`_seed_stock`/`_wait_ready` en e2e conftest, `/sync/push`,
`DeviceToken`/`Customer`/`InventoryMovement`/`SyncConflict`, `_LATEST_SCHEMA_VERSION`).

### 3.4 Interfaces nuevas
Fixture `central_server -> (base_url, db_path)`; `client.sync()` con versión single-source.

---

## 4. Fases de implementación (Task Envelopes)

5 envelopes en `tasks.md`. Batches: **T1-T3 · T4-T5**.

| # | Envelope | Files | Depends |
|---|----------|-------|---------|
| T1 | Single-source versión | sync/client.py | none |
| T2 | Fixture `central_server` | tests/e2e/conftest.py | none |
| T3 | e2e multi-nodo | test_sync_multinode_e2e.py | T2 |
| T4 | verify.sh del épico | specs/.../verify.sh | T3 |
| T5 | Doc viva | CLAUDE.md | T4 |

---

## 5. Plan de tests y verificación

### 5.0 Pre-implementación
```bash
# P1-P4 verdes. Suite unit verde antes de empezar.
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
```

### 5.1 Verificación por envelope
Cada envelope trae su `#### Verification`.

### 5.3 Tests que deben seguir pasando
- [ ] Los e2e existentes (usan `live_server`, no modificada) — sin regresión.

### 5.4 Tests e2e
**Este slice ES el e2e obligatorio del épico (REQ-20).** `test_sync_multinode_e2e.py` valida el
flujo completo dos-nodos→central→convergencia. Se corre con `-m e2e --no-cov`. La UI desktop y el
cifrado se validan manualmente (P4/README).

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación | Rollback |
|---|--------|-------|---------|------------|----------|
| R1 | e2e flaky por timing del subproceso | Media | Bajo | Reusar `_wait_ready` (ya endurecido en el repo) | reintento/ajuste de espera |
| R2 | La fixture nueva duplica lógica de `live_server` y diverge | Baja | Bajo | Reutiliza `_free_port`/`_wait_ready`/`_seed_stock`; misma estructura | unificar en follow-up |
| R3 | Aserciones acopladas a los datos del seed | Baja | Bajo | Usa E2E-SKU1 (stock 10) del seed estándar; venta 999 garantiza conflicto | ajustar cantidades |

---

## 7. Definición de hecho (DoD del épico)

### 7.1 Checklist pre-commit
- [ ] Pre-check verde (P1-P4) · unit verde tras cambios · e2e multi-nodo verde
- [ ] `bash specs/offline-desktop-sync/verify.sh` termina en "TODO VERDE"
- [ ] Invariantes: `live_server` intacta; sin cambios de negocio
- [ ] Scope: `git diff --name-only develop` == los 5 archivos de 3.2
- [ ] Doc viva: CLAUDE.md refleja v0.4.7 + el épico

### 7.2 Script de verificación
`specs/offline-desktop-sync/verify.sh` (T4) — es el script oficial del épico.

### 7.3 Commits esperados
```
refactor(sync): single-source de la versión de protocolo
test(e2e): sincronización multi-nodo con conflicto (REQ-20)
chore(epic): verify.sh integral offline-desktop-sync
docs: registrar el épico offline-desktop-sync (v0.4.7)
```

### 7.4 Criterios de aceptación (Gherkin REQ-20 + cierre del épico)
- [ ] REQ-20: dos nodos sincronizan; venta válida aplicada, venta sin stock → conflicto, cliente deduplicado, reenvío idempotente.
- [ ] Épico: `verify.sh` verde (unit + e2e). Build Windows validado manualmente (P4/README).
- [ ] Todos los REQ Must-Have (1-14,17,19,20) tienen ≥1 envelope implementado y verde entre P1-P5.

---

> **Cierre del épico `offline-desktop-sync`.** Los 5 PAEs (P1-P5) están completos y listos para
> ejecución con OpenCode + Qwen local, uno tras otro, cada uno verde antes del siguiente.
> Versión del épico: **v0.4.7** (PATCH). El instalador Windows se construye y valida a mano
> (`packaging/README-desktop.md`).
