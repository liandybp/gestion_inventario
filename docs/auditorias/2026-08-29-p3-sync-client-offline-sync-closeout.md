# Cierre P3 offline-desktop-sync — code-review + e2e + 3 bugs adicionales

**Fecha:** 2026-08-29
**Proyecto:** gestion_inventario
**Rama:** `develop`, commit `13f7af1` (sigue a `41adf1d`)
**Responsable:** Claude Code (code-review + auditoria-ingenieria + verificación empírica)

---

## Contexto

Tras la re-auditoría de P3 (`2026-08-29-p3-sync-client-offline-sync-reaudit.md`, 3.95/5.00 🟡),
se corrió un `/code-review` (medium effort, 8 ángulos) sobre el commit `41adf1d` completo. Encontró
5 hallazgos — 1 crítico (falta e2e, regla dura del `CLAUDE.md` raíz), 3 medios (hardcoded
`app_mode`, `_PULL_ORDER` sin fuente compartida, guards `is_desktop()` dispersos), 1 bajo (dead
init). La re-auditoría posterior bajó el score a **3.55/5.00 🟡** por el peso de la dimensión
Pruebas/Calidad.

Esta sesión cerró los 3 quick wins y escribió el e2e que la auditoría exigía. El e2e —
deliberadamente un test de dos servidores reales (central + desktop) sobre un browser real, no
un mock — encontró **3 bugs de producción adicionales** que ninguno de los 15 tests unitarios de
`apply_pull`/`sync` había detectado, porque todos pasaban datos Python nativos directo a la
función en vez de datos que realmente cruzan HTTP o coexisten con el resto del schema.

---

## Quick wins cerrados

| # | Hallazgo | Fix |
|---|----------|-----|
| 1 | `ui_sync.py`: `do_sync`/`provision` hardcodeaban `"app_mode": "desktop"` en vez de `app_mode_label()` (inconsistente con `sync_status`) | Ambos usan `app_mode_label()` ahora |
| 2 | `_PULL_ORDER` (client.py) y `build_pull()` (service.py) mantenían el mismo set de 13 tablas en dos listas independientes, sin nada que fallara si una se desincronizaba | Nuevo `app/sync/tables.py::SYNC_TABLE_ORDER` como fuente única; `client.py` deriva `_PULL_ORDER` de ahí con un `assert`, `service.py::build_pull()` valida sus keys contra la misma lista con otro `assert` |
| 3 | `ui_home.py` pasaba `"result": None` siempre al dashboard, sin uso real (`sync_status.html` solo lo consulta cuando `do_sync`/`provision` lo pasan) | Eliminado |

---

## e2e agregado (`tests/e2e/test_sync_e2e.py`)

Levanta **dos procesos uvicorn reales** (`central_server` en modo server con datos sembrados,
`desktop_server` en modo desktop, cada uno con su propio SQLite temporal) y conduce, vía
Playwright, el flujo completo: login en central → `POST /sync/register` → login en desktop →
aprovisionar (pegando el token, UI de H-008) → primer sync (pull) → venta offline sin fecha
explícita → segundo sync (push) → assert de que la venta se replicó en la BD del **central**
(no solo local) → logout/relogin en desktop para confirmar que el password local sobrevivió.

## 3 bugs nuevos encontrados y corregidos (H-e2e-1/2/3)

Todos en `app/sync/client.py::apply_pull`. Los tres son del mismo tipo: código que pasaba los 15
tests unitarios porque esos tests nunca reprodujeron las condiciones reales de un pull —
(a) un nodo con historial de uso previo (audit_logs), (b) un snapshot que de verdad cruzó JSON por
HTTP, (c) tablas dependientes insertadas en el mismo pull sin `relationship()` ORM.

### H-e2e-1 — `db.query(User).delete()` bloqueado por FKs locales no sincronizadas

Cualquier login real escribe una fila en `audit_logs.user_id` **antes** de que corra el primer
sync. `audit_logs` no está en `SYNC_TABLE_ORDER` (es local al nodo), pero su FK a `users.id`
igual bloqueaba el `DELETE FROM users` bajo `PRAGMA foreign_keys=ON` — el mismo problema existe
para `dashboard_widgets.created_by`, `chat_messages.user_id`, `coach_action_logs.marked_by`
(nullable, se ponen a `NULL`) y `user_businesses` (no nullable, se borra entera — sus filas
quedan obsoletas de todas formas porque los ids de `users` cambian con cada pull).

**Fix:** `_detach_local_user_references()`, llamada antes del delete de `users` — mismo patrón
que ya usaba `ui_users.py` para el borrado manual de un usuario.

### H-e2e-2 — snapshot cruza HTTP como JSON, no como objetos Python

`sync()` manda el push con `httpx.post(json=...)` y lee el pull con `resp.json()`. Cualquier
columna `DateTime`/`Date` en el snapshot llega como string ISO (`"2026-08-29T14:52:36"`), no como
`datetime`. `model(**data)` con ese string fallaba con
`TypeError: SQLite DateTime type only accepts Python datetime and date objects` — en la
**primera** tabla con una columna de fecha (`businesses.created_at`), es decir, en todo pull real.

**Fix:** `_coerce_datetimes()`, aplicada a cada fila antes de `model(**data)` — inspecciona el
tipo real de cada columna vía `model.__table__.columns` y parsea con
`datetime.fromisoformat`/`date.fromisoformat` solo cuando corresponde.

### H-e2e-3 — un solo `flush()` al final no reordena INSERTs cross-tabla por FK

`_PULL_ORDER` ya lista `inventory_movements` antes que `inventory_lots` (dependencia FK
correcta), pero como los modelos no declaran `relationship()` ORM entre sí, el unit-of-work de
SQLAlchemy no reordena los `INSERT` por dependencia cuando ambos viven en el mismo `flush()` —
insertar un lot que referencia un movement recién agregado **en el mismo flush** fallaba con
`IntegrityError` pese a que el orden de *tablas* era correcto. Reproducido en aislamiento con un
script mínimo de 15 líneas antes de tocar el código (ver commit para el detalle).

**Fix:** mover `db.flush()` a adentro del loop, una vez por tabla, en vez de una sola vez al
final. Verificado empíricamente que soluciona el caso mínimo y el flujo completo.

---

## Verificación

- Cada uno de los 3 bugs se reprodujo primero con un script standalone contra SQLite real
  (`PRAGMA foreign_keys=ON`), confirmado el fallo, aplicado el fix, y re-confirmado que
  desaparece — no se asumió que el fix funcionaba por lectura de código.
- El flujo completo se corrió también **manualmente vía curl** contra dos servidores reales
  (sin Playwright) antes de correr la versión Playwright, para aislar si un fallo era del
  flujo HTTP/DB o de un selector de UI.
- 277 unit tests verdes (274 + 3 regresiones nuevas para H-e2e-1/2/3).
- 21/21 e2e verdes (20 preexistentes + `test_sync_e2e.py`), corridos con navegador real
  (chromium, Playwright).

## Pendiente fuera de alcance de esta sesión

- Guards `is_desktop()` dispersos en 9 endpoints de compras/traspasos (hallazgo medio del
  code-review, no bloqueante): se dejó como está — es codebase-wide, no específico de esta
  sesión, y ya está probado (cada guard tiene su propio test). Candidato a middleware/decorator
  centralizado si el patrón se repite para una tercera feature online-only.
- Se encontró trabajo **no relacionado y no comiteado** en el working tree
  (`app/desktop/`, `packaging/`, `requirements-desktop.txt`, `tests/unit/test_desktop_keyring.py`,
  `app/db.py` modificado) — aparenta ser el arranque de P4 (empaquetado desktop + SQLCipher).
  No se tocó ni se comiteó: es trabajo en progreso de otra sesión/rama de trabajo, con un test
  fallando (`test_desktop_keyring.py::TestGetDbKey::test_fallback_deterministic_from_db_path`)
  que no guarda relación con los cambios de esta sesión. Queda para que el usuario decida cómo
  proceder.

---

*Informe generado por Claude Code (skills: code-review, auditoria-ingenieria).*
