# Re-auditoría — PAE-015: copiloto, ventas por punto de venta mes a mes

- **Fecha:** 2026-10-24
- **Alcance:** `2638bf8..79bb240` (rama `feature/frontend-modular`, 8 commits): `11fd748` specs regenerados +
  7 commits de implementación. Sustituye a la implementación auditada en
  `2026-10-23-copiloto-ventas-ubicacion-mensual.md` (3.10 🔴, conservada en la rama `backup/pae015-haiku`).
- **Score:** 3.10 🔴 → 4.35 🟢 → **4.70 / 5.00 🟢** tras corregir H-601..H-604 (§7)
- **Responsable:** Claude Code (Opus 5.5) — `/code-review` + `auditoria-ingenieria`, una pasada
- **Estado:** H-601..H-604 corregidos el mismo día (`09ef478`, `7e1e7fe`); queda el DoD con datos reales (§7.3).

## 1. Contexto y método

Se descartó la implementación anterior: se reconstruyeron los specs originales del 2026-10-18 desde el transcript
de aquella sesión (byte a byte con el PAE que había en el repo), se regeneraron sobre la fase 2 del frontend y la
implementación se hizo **aplicando literalmente** los bloques BEFORE/AFTER de cada envelope, como haría cc_local.
Así la implementación también valida el PAE: solo hizo falta insertar a mano el paso 2b de T5 ("insertar antes
de…", que no es un par BEFORE/AFTER).

| Herramienta | Uso |
|---|---|
| `/code-review` (skill en segundo plano) | 3 hallazgos (H-601..H-603); estado de H-501..H-512. Ejecutó `_synth_title` contra las 34 tools. No usó Serena/codebase-memory. |
| Ejecución | Script de verificación del PAE (§7.2): 433 unit (cov 86,26 %), 72 e2e, eval colectado, versión sin cambios, 40 snapshots equivalentes. Mutación: con `widget-chart.js` de `2638bf8`, el e2e multi-serie falla (1 dataset en vez de 3). |
| Eval contra el LLM local (`qwen3_8b_instruct`) | Rama: tool 23/25 (92 %), type 24/25 (96 %), args 23/25 (92 %); el caso nuevo `sales_by_location_monthly_6m` acierta tool, type `bar` y `months=6`. Base `2638bf8` (worktree desechable, ya borrado): tool 23/24 (96 %), type 96 %, args 19/24 (79 %). |
| **Fuera de cobertura** | Serena/codebase-memory no se usaron en esta pasada (el review leyó con `git diff`/`grep`). DoD pendiente: ejecutar la tool contra una copia de la BD de producción y probar la frase del usuario en la app levantada. |

## 2. Estado de los hallazgos anteriores

| ID | Estado | Evidencia |
|---|---|---|
| H-501 tool rota | ✅ | `tools.py:259` usa `ReportingService(service.db, service.business_id)`; `test_sales_by_location_monthly_payload` la ejecuta |
| H-502 bug de prod (título, aviso, honestidad) | ✅ | `graph.py:163` `_synth_title`, `graph.py:230-237` aviso; reglas en `prompts.py`; 5 tests |
| H-503 specs a mano | ✅ | Regenerados desde el original (`11fd748`); la implementación los aplicó tal cual |
| H-504 commit con ficheros ajenos | ✅ | Historia rehecha; los 8 commits solo contienen los 12 ficheros del PAE + specs |
| H-505 falso verde | ✅ | +10 unit, +2 e2e validados por mutación, caso del eval ejecutado |
| H-506 series por nombre | ✅ | Agrupa por `InventoryMovement.location_id`; test de dos ubicaciones con el mismo nombre |
| H-507 central al final | ✅ | `coalesce(Location.position, 9999)` en SQL: `position=0` se respeta |
| H-508 leyenda de radar | ✅ | La leyenda solo se toca en bar/line, como antes |
| H-509 pie multi-serie | ✅ | `multi` exige `bar`/`line` (`widget-chart.js:50`) |
| H-510 tipo no acotado | ⚠️ parcial | La regla del prompt lo acota; el código no (H-603) |
| H-511 `months: null` | ✅ | `int(args.get("months") or 6)`; pero ver H-601 |
| H-512 requirements incoherente | ✅ | Se restauró el `requirements.md` original |

## 3. Checklist y scorecard

| # | Dimensión | Peso | Score | Evidencia |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 5 | Lógica en `ReportingService`; la tool solo da formato; el grafo solo titula y avisa |
| 2 | Acoplamiento | 10 | 4 | Mismo patrón que las tools hermanas; el contrato tool↔type solo vive en el prompt (H-603) |
| 3 | Capas | 10 | 5 | `tools → ReportingService → ORM` |
| 4 | Seguridad | 15 | 4 | Filtro `business_id` con test de aislamiento; nombres por `tojson` + canvas + CSP `script-src 'self'`; aviso sin detalle de la excepción |
| 5 | Robustez | 15 | 4 | Errores capturados en `_execute_node` y avisados; `months=0` no se acota como pide REQ-2 (H-601); aviso de más con varios widgets (H-602) |
| 6 | Consistencia | 10 | 5 | Misma ventana para todas las series; clave por id; misma fórmula de importe que `sales_by_location` |
| 7 | Observabilidad | 10 | 4 | `agent_plan` loguea tool y args; el usuario ve el aviso; eval como vara de medir |
| 8 | Configuración | 5 | 5 | `_MAX_MONTHS_WINDOW = 24` |
| 9 | Pruebas | 10 | 4 | Unit + e2e con mutación + eval; falta el escenario `months` fuera de rango a nivel de tool (H-601) |
| 10 | Escalabilidad | 5 | 4 | Agregación en Python acotada a 24 meses, índice `(business_id, type, movement_date)` |
| | **Total** | 100 | **4.35 🟢** | |

## 4. Hallazgos nuevos

### Medio

**H-601 — `{"months": 0}` devuelve 6 meses; REQ-2 pide 1.**
- Evidencia: `app/agent/tools.py:260` (`int(args.get("months") or 6)`); `requirements.md` REQ-2, escenario
  "months fuera de rango se acota". El envelope T2 decía "0/None → 6" y contradecía al REQ (incoherencia ya presente
  en el PAE original); el test de acotado de T1b llama al servicio, no a la tool.
- Riesgo: bajo en la práctica (el LLM no pide 0 meses), pero el comportamiento contradice la especificación.
- Recomendación: `months = args.get("months"); months = 6 if months is None else int(months)` y un test de la tool
  con `0` y `500`; corregir el edge case de T2 en `tasks.md`.

### Bajo

**H-602 — El aviso aparece aunque el widget mostrado tenga datos.** `graph.py:236`: `_execute_node` ejecuta todos
los widgets del plan, pero `_build_node` solo muestra el primero. Si falla la tool del segundo, el usuario ve un
widget correcto junto con "No pude obtener los datos de este widget". Recomendación: avisar solo si el error es del
widget que se muestra (o limitar `_execute_node` al primero).

**H-603 — Nada impide pintar esta tool como pie/doughnut/radar/kpi.** El payload no trae `values`; con esos tipos
`multi=false` y el gráfico sale vacío sin aviso (`widget-chart.js:50`). Hoy solo lo evita la regla del prompt.
Recomendación: en `_build_node`, si el payload trae `datasets` y no `values`, forzar `bar` cuando el tipo elegido no
sea `bar`/`line`/`table`.

**H-604 — Eval: un fallo nuevo sin confirmar.** `dividends_pending` eligió `extractions_by_party` en la rama y
acertó en la base; `low_stock` falla en ambas (previo). Con una ejecución por lado y la variación observada (args
88 → 79 → 92 % entre ejecuciones), no se puede atribuir a las reglas nuevas del prompt. Recomendación: repetir el
eval 3 veces en cada lado antes de pasar a `main`.

## 5. Matriz y backlog

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-601, H-602 |
| 📋 Baja prioridad | H-603, H-604 |

1. H-601 — acotado de `months` en la tool + test + edge case de T2.
2. H-602 — aviso solo para el widget mostrado.
3. H-603 — tipo forzado a `bar` para payloads multi-serie sin `values`.
4. H-604 — eval repetido 3× en rama y base.
5. DoD pendiente: tool contra copia de la BD de producción (tabla de `requirements.md`) y prueba en la app.

## 6. Plan

- **Antes de pasar la rama a `develop`:** puntos 1-5. Cierre: tests verdes, eval sin regresiones confirmadas y la
  petición del 2026-10-18 reproducida con datos reales.
- **Después:** H-003 del copiloto (routing en dos pasos) usando el eval repetido como baseline.

## 7. Correcciones (2026-10-24)

### 7.1 Hallazgos

| ID | Corrección | Test (validado por mutación: revertir el fix hace fallar el test) |
|---|---|---|
| H-601 | `tools.py`: solo `months` ausente/`None` usa 6; `0`/`500` los acota el servicio. Edge case de T2 corregido en `tasks.md` | `test_sales_by_location_monthly_months_bounds` |
| H-602 | `graph.py`: `_execute_node` guarda `tool_failed` alineado con `tool_results`; `_narrate_node` avisa solo si falló el widget mostrado | `test_reply_no_warning_when_only_discarded_widget_fails` |
| H-603 | `graph.py::_build_node`: payload con `datasets` y sin `values` → `bar` si el planner no eligió `bar`/`line`/`table` | `test_multiseries_payload_gets_a_drawable_type` (7 casos) |
| H-604 | Confirmado y corregido, ver 7.2 | `test_prompt_separates_pending_dividends_from_extractions` |

### 7.2 Eval repetido (LLM local `qwen3_8b_instruct`, 3 ejecuciones por lado)

Las 3 ejecuciones de cada lado dieron resultados idénticos: el eval es determinista con este modelo, así que la
diferencia es real y no ruido.

| | Tool | Type | Args | Fallos de tool |
|---|---|---|---|---|
| Base `2638bf8` (24 casos) | 96 % | 96 % | 79 % | `low_stock` |
| Rama antes del fix (25 casos) | 92 % | 96 % | 92 % | `low_stock`, **`dividends_pending`** |
| Rama tras el fix (25 casos) | **96 %** | **96 %** | **92 %** | `low_stock` (previo) |

Con las reglas nuevas, "¿Cuánto tiene pendiente cada socio?" pasó a resolverse con `extractions_by_party`. La regla
de dividendos ahora nombra la pregunta y separa "pendiente" de "retirado", como ya se hacía con
`sales_by_location` frente a `profit_by_location`. La rama mejora los argumentos (+4 casos: el rango de "este mes"
ya no empieza el mes anterior) y el caso nuevo `sales_by_location_monthly_6m` acierta tool, type y `months=6`.

### 7.3 Verificación final y pendientes

- 443 unit (cobertura 86,45 %), 72 e2e, 40 snapshots equivalentes.
- **Pendiente (DoD del PAE):** ejecutar la tool contra una copia de la BD de producción y comprobar la tabla de
  `requirements.md` (validado el 2026-10-18 con el código original; desde entonces solo cambió la clave de las
  series, de nombre a id), y probar en la app levantada la frase original del usuario.

### 7.4 Scorecard tras las correcciones

| # | Dimensión | Peso | Antes | Después |
|---|---|---|---|---|
| 2 | Acoplamiento | 10 | 4 | 5 (el contrato tool↔type se impone en código) |
| 5 | Robustez | 15 | 4 | 5 (límites de REQ-2, aviso preciso, tipo dibujable) |
| 9 | Pruebas | 10 | 4 | 5 (límites a nivel de tool, eval repetido en rama y base) |
| | **Total** | | 4.35 | **4.70 🟢** |
