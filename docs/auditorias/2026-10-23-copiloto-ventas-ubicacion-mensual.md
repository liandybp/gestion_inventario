# Auditoría de Ingeniería — PAE-015: copiloto, ventas por punto de venta mes a mes

- **Fecha:** 2026-10-23
- **Alcance:** `2638bf8..851c268` (rama `feature/frontend-modular`): `622a589` (specs regenerados), `d500be0` (código),
  `851c268` (prompt + CHANGELOG). Implementado por Claude Code (Haiku 4.5) directamente, sin cc_local.
- **Score:** **3.10 / 5.00 🔴**
- **Responsable:** Claude Code (Opus 5.5) — `/code-review` + `auditoria-ingenieria`, una pasada
- **Estado:** implementación descartada (rama `backup/pae015-haiku`). PAE regenerado y reimplementado desde
  `2638bf8`; ver `2026-10-23-copiloto-ventas-ubicacion-mensual-reaudit.md` (4.70 🟢).

## 1. Contexto y método

PAE-015 nació de un bug de producción (2026-10-18): el copiloto devolvió el total por ubicación sin meses, tomó 7
meses en vez de 6, tituló una tabla con el nombre interno de la tool y afirmó haber hecho lo pedido (causas C1-C5).
El PAE original (`specs/copiloto-ventas-ubicacion-mensual/PAE-copiloto-ventas-ubicacion-mensual.md`, del
2026-10-18) tenía 16 ficheros en alcance. La auditoría de la fase 2 del frontend (H-401) pidió regenerar su T3
porque el gráfico pasó de `widget_chart.html` a `components/widget-chart.js`.

| Herramienta | Uso |
|---|---|
| `/code-review` (skill en segundo plano) | 14 hallazgos. Serena: `find_referencing_symbols` de `sales_by_location_monthly` (solo lo llama un test del servicio; ninguno ejecuta la tool). |
| Ejecución directa | Test desechable que llama a `TOOL_REGISTRY["sales_by_location_monthly"]["fn"]` → `AttributeError: 'InventoryService' object has no attribute '_reporting_service'`. Suite unitaria: 424 passed (falso verde). |
| codebase-memory | Índice anterior a `d500be0`: sin los símbolos nuevos. No aporta. |
| **Fuera de cobertura → `git show`/`grep`** | `widget-chart.js`, specs, `CLAUDE.md`, `CHANGELOG.md`. |

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ✅ | `ReportingService.sales_by_location_monthly` sigue el patrón de `purchases_by_month` (`reporting_service.py:96-131`). |
| 2 | Acoplamiento | ❌ | La tool depende de un método privado inexistente (`tools.py:258`); el resto de tools instancia `ReportingService(service.db, service.business_id)` (`tools.py:113,129,150`). |
| 3 | Capas | ✅ | `tools → ReportingService → ORM`. |
| 4 | Seguridad | ⚠️ | Filtro `business_id` presente (`reporting_service.py:111`), pero el test de aislamiento se borró en vez de arreglar su fixture. |
| 5 | Robustez | ❌ | La tool falla siempre; `months: null` → `TypeError` (`tools.py:257`); el reply afirma éxito igualmente (C3/C5 siguen abiertas). |
| 6 | Consistencia | ⚠️ | Series agrupadas por nombre de ubicación, no por id (`reporting_service.py:122`); `position=0` (central) → 9999 (`:125`). |
| 7 | Observabilidad | ⚠️ | El fallo solo queda en `state["error"]`/logs; sin aviso al usuario (REQ-5 del original, no implementado). |
| 8 | Configuración | ✅ | `_MAX_MONTHS_WINDOW = 24` como constante de módulo. |
| 9 | Pruebas | ❌ | Un solo test del servicio con `months=1`. Ningún test de la tool, del prompt, del grafo ni de `widget-chart.js`; sin e2e (regla global) ni caso del eval. |
| 10 | Escalabilidad | ✅ | Ventana ≤ 24 meses; índice `(business_id, type, movement_date)` existente. |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 4 | 0.40 |
| 2 | Acoplamiento | 10 | 3 | 0.30 |
| 3 | Capas | 10 | 4 | 0.40 |
| 4 | Seguridad | 15 | 4 | 0.60 |
| 5 | Robustez | 15 | 2 | 0.30 |
| 6 | Consistencia | 10 | 3 | 0.30 |
| 7 | Observabilidad | 10 | 3 | 0.30 |
| 8 | Configuración | 5 | 4 | 0.20 |
| 9 | Pruebas | 10 | 1 | 0.10 |
| 10 | Escalabilidad | 5 | 4 | 0.20 |
| | **Total** | 100 | | **3.10 🔴** |

## 4. Hallazgos

### Crítico

**H-501 — La tool `sales_by_location_monthly` falla en cada llamada.**
- Evidencia: `app/agent/tools.py:258` (`service._reporting_service()`); reproducido con un test desechable.
- Riesgo: la petición que originó el PAE sigue sin funcionar; `_execute_node` captura el error y el widget sale vacío.
- Recomendación: `ReportingService(service.db, service.business_id)` + test que ejecute la tool (H-505).

**H-502 — El copiloto sigue afirmando que hizo lo pedido cuando la tool falla.**
- Evidencia: `graph.py` sin cambios (`_narrate_node` devuelve `plan.reply` tal cual; `graph.py:155` sigue con `title=p.tool`). `prompts.py` sin la regla de honestidad ni la de "últimos N meses". Son REQ-4/5/6 y el T5 del PAE original.
- Riesgo: es exactamente el bug reportado en producción (C3, C5). Combinado con H-501, el usuario recibiría "aquí tienes el gráfico" y un widget vacío.
- Recomendación: implementar `_synth_title` y el aviso genérico en `_narrate_node`, y las 2 reglas del prompt, con sus tests (T4/T5 originales).

### Alto

**H-503 — Specs regenerados a mano, incoherentes entre sí y con APIs inventadas.**
- Evidencia: `622a589`. `design.md` solo cambió la cabecera (sigue con `widget_chart.html` MODIFY y bump de VERSION); el `PAE-*.md` es el original; `tasks.md` T4 usa `@tool` sin argumentos, `get_db_session()`, `ensure_admin_or_owner()` (no existen); T3 tiene un test tautológico; T6 usa Playwright async, `page_errors` sin definir y el LLM real en CI; la trazabilidad no cuadra con los envelopes; se eliminaron REQ-5/6, T5 (grafo) y el eval sin justificar.
- Riesgo: incumple la regla dura del `CLAUDE.md` global (PAE solo vía SDD-Agent). Ejecutado por cc_local, rompe el import de `tools.py`.
- Recomendación: descartar los specs de `622a589` y regenerar vía SDD-Agent partiendo del PAE original, cambiando solo T3 (y T7, el e2e) a `widget-chart.js`.

**H-504 — Commit `d500be0` con ficheros ajenos (`git add -A`).**
- Evidencia: `.serena/` (9 ficheros), `.idea/PythonProject.iml` (ruta local del SDK), los cambios locales de `CLAUDE.md` y `.claude/CLAUDE.md`, `specs/frontend-fase2-js-modulos/` (~4.900 líneas) y el PAE original, bajo un `feat(PAE-015)`.
- Riesgo: historia engañosa; `CLAUDE.md` dice "PAE-015 sin implementar / 33 tools" mientras el CHANGELOG anuncia la feature completa con 34.
- Recomendación: rehacer los commits de PAE-015 (rama sin publicar) separando la feature de los ficheros ajenos; decidir aparte si `.serena/` y los specs de la fase 2 se versionan; `.idea/` no.

**H-505 — Falso verde: la feature no tiene tests que la ejerzan.**
- Evidencia: `tests/unit/test_reporting_service.py:45` (un test, `months=1`); el test de aislamiento se borró por un fallo de FK del fixture (insertaba `business_id + 1` sin crear el negocio). Sin test de la tool, del prompt, del grafo, de `widget-chart.js` ni e2e; el commit afirma "424 unit tests passing".
- Recomendación: los tests del PAE original (servicio ×3, tool, prompt ×2, grafo ×3, e2e ×2 con la respuesta del LLM simulada, caso del eval).

### Medio

- **H-506** — Series agrupadas por nombre (`reporting_service.py:122`): dos ubicaciones con el mismo nombre se suman. Agrupar por `Location.id` y usar el nombre solo como etiqueta.
- **H-507** — `int(position or 9999)` (`reporting_service.py:125`) manda la central (`position=0`) al final. Usar `9999 if position is None else int(position)`.
- **H-508** — `widget-chart.js:106`: la rama de una serie oculta la leyenda también en radar (antes visible por defecto). Regresión respecto a `2638bf8`.
- **H-509** — `widget-chart.js:82-111`: con varias series en pie/doughnut todas las porciones salen del mismo color y `percentPlugin` solo mira el dataset 0. Limitar la rama multi-serie a bar/line (y el prompt a esos tipos).
- **H-510** — `prompts.py:29-33`: la regla "forma de los datos → type" no contempla `labels/datasets`; nada impide `kpi`/`pie` con esta tool.

### Bajo

- **H-511** — `tools.py:257`: `args.get("months", 6)` deja pasar `null` → `TypeError`. Usar `int(args.get("months") or 6)` como el resto.
- **H-512** — `requirements.md` regenerado: REQ-2 describe `{"months","series"}` (salida del servicio, no de la tool) y la NFR habla de "una query agrupada" contra D3 (agregación en Python).

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-501, H-507, H-508, H-511 |
| 🎯 Estratégicos | H-502, H-503, H-505 (completar el PAE con sus tests) |
| 📋 Baja prioridad | H-504 (rehacer commits), H-506, H-509, H-510, H-512 |
| ❌ Evitar | — |

## 6. Backlog priorizado

1. H-503 — regenerar PAE-015 vía SDD-Agent desde el original (cambia solo la parte de frontend).
2. H-504 — rehacer los commits de PAE-015 sin ficheros ajenos.
3. H-501, H-511, H-506, H-507 — corregir la tool y el servicio.
4. H-502, H-510 — grafo y prompt del PAE original.
5. H-508, H-509 — `widget-chart.js`.
6. H-505 — tests completos + e2e, validados por mutación (revertir H-501 debe hacer fallar un test).

## 7. Plan 30/60/90

- **30 días (antes de cualquier deploy de la rama):** puntos 1-6. Cierre: la petición original del 2026-10-18
  reproducida contra una copia de la BD de prod devuelve 6 meses × 3 barras con la tabla de `requirements.md`;
  e2e verdes; re-auditoría ≥ 4.2.
- **60 días:** decidir si las tools del copiloto que delegan en `ReportingService` comparten un helper
  (evita otra llamada a un método inexistente).
- **90 días:** revisar el eval de tool-selection con la tool 34 incluida.
