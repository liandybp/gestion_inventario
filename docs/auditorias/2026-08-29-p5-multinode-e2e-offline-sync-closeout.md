# Cierre P5 offline-desktop-sync — reescritura completa tras auditoría 1.40/5.00

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop` (working tree, **sin commitear**)
**Responsable:** Claude Code (code-review + auditoria-ingenieria + verificación empírica)

---

## Contexto

`docs/auditorias/2026-08-29-p5-multinode-e2e-offline-sync.md` encontró 10 hallazgos (8
confirmados por ejecución directa) en el P5 implementado por OpenCode — score **1.40/5.00 🔴**,
el más bajo del épico. El hallazgo raíz: el e2e (`tests/e2e/test_sync_multinode_e2e.py`) nunca
había corrido con éxito una sola vez (`NameError` en la primera línea real), y la
documentación (`CLAUDE.md`) igual lo declaraba "🟢 Verde" porque `verify.sh` —el script que
debía gatear esa afirmación— nunca ejecutaba el e2e, solo comprobaba que el archivo existiera.

Esta sesión corrigió los 10 hallazgos sin excepciones.

---

## Hallazgos corregidos

| ID | Título | Fix |
|----|--------|-----|
| H-001 | `NameError` inmediato (imports faltantes) | Reescritura completa del archivo con imports correctos |
| H-002 | Fixture `central_server` apuntaba a la BD real de dev + puerto 10000 (colisión con el servidor de dev real) | Reescrita para usar SQLite temporal aislado + puerto libre dinámico, mismo patrón que `live_server` (helper compartido `_boot_temp_server()`) |
| H-003 | `CLAUDE.md` declaraba "P5 🟢 Verde" sin que el test hubiera corrido | Sección reescrita con el estado real por slice, scores auditados, y un checklist de pendientes explícito |
| H-004 | `verify.sh` nunca ejecutaba el e2e pese a afirmarlo | Nuevo paso 2 que corre `pytest tests/e2e/test_sync_e2e.py tests/e2e/test_sync_multinode_e2e.py -m e2e`; `set -e` como único gate (sin el `if [ $? -ne 0 ]` redundante/muerto que tenía antes) |
| H-005 | `db_path` roto para URLs Postgres → aserciones contra una BD fantasma | Resuelto de raíz junto con H-002 (la fixture nueva nunca necesita parsear una URL Postgres) |
| H-006 | Aserción sobre `resp["ok"]`, key inexistente en `PushResponse` | Reescrito: se verifica `applied_event_ids` (el campo real del schema) |
| H-007 | Aserción sobre `SyncEvent` (modelo cliente) en vez de `AppliedSyncEvent` (lo que el servidor realmente escribe) | Reescrito para consultar `AppliedSyncEvent` y `SyncConflict`, los modelos que `SyncService.push()` de verdad toca |
| H-008 | `mv.id` leído antes de `flush()` | No reintroducido en la reescritura — el nuevo test no construye movimientos vía ORM directo, usa el protocolo HTTP real (`POST /sync/push`) para todo |
| H-009 | Segundo `NameError` enmascarado (`client_id-a`) | No reintroducido — reescritura completa |
| H-010 | Colisión de nombre `central_server` con el fixture ya existente y seguro de `test_sync_e2e.py` (P3) | Renombrado el de `test_sync_e2e.py` a `p3_central_server` (semántica más específica: requiere `app_mode="server"` explícito) — el `central_server` de `conftest.py` queda como el genérico, reusable por cualquier e2e de sync |
| H-011 | Fixtures muertas (`node_a_live_server`/`node_b_live_server`) que desempaquetaban mal | No reintroducidas — reescritura completa |

---

## Diseño del test reescrito

Sigue la decisión de diseño original del PAE (T1: "nodos = emisores de eventos HTTP contra el
central", no procesos-nodo completos) mejor que la versión original, porque ahora sí es
ejecutable: un único servidor central real (subproceso uvicorn + SQLite temporal aislado) recibe
`POST /sync/push` de dos `DeviceToken` distintos ("nodo A"/"nodo B"), y el test verifica:

- Venta con stock (nodo A) → se aplica, movimiento de inventario real con cantidad negativa.
- Venta sin stock (nodo B, mismo SKU) → conflicto `sale_stock` registrado, no se aplica.
- Cliente nuevo (nodo A) + cliente con mismo teléfono, distinto client_id (nodo B) → dedup: el
  evento de B se marca aplicado pero no crea un segundo `Customer`.
- Reenvío del evento de venta de A (mismo `event_id`) → no se re-aplica (`AppliedSyncEvent`
  ya existente, idempotencia real).

Todas las aserciones consultan la BD real (SQLite temporal) que el servidor bajo prueba
efectivamente usa — no una copia fantasma.

---

## Verificación

- `pytest tests/e2e/test_sync_multinode_e2e.py -m e2e` → verde, primera corrida real del test
  en su historia.
- `pytest tests/e2e -m e2e` (suite completa, 22 tests incluyendo el reescrito y el de P3
  renombrado) → 22/22 verdes.
- `pytest tests/unit --ignore=tests/unit/test_desktop_keyring.py` → 277/277 verdes (sin cambios
  respecto a antes de esta sesión).
- `bash specs/offline-desktop-sync/verify.sh` → **falla correctamente** en el paso 1 (suite
  unitaria) por el test roto preexistente de P4 (`test_desktop_keyring.py`, fuera de alcance de
  esta sesión, el usuario decidió dejarlo sin commitear) — comportamiento correcto y esperado
  del gate, no se ocultó el fallo.
- Verificado manualmente que no quedaron procesos uvicorn huérfanos ni archivos temporales tras
  las corridas.

## Pendiente (fuera de alcance de esta sesión, documentado en CLAUDE.md)

- Re-auditar P5 con el código reescrito para asignarle un score formal (no se re-corrió
  `auditoria-ingenieria` sobre la versión nueva en esta sesión, solo se verificó que los 10
  hallazgos originales estén resueltos).
- Auditar P4 (nunca se hizo) y arreglar `test_desktop_keyring.py`.

---

*Informe generado por Claude Code (skills: code-review, auditoria-ingenieria).*
