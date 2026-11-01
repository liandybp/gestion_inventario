# Auditoría de Ingeniería — Frontend modular, fase 2: JS a módulos ES, sin JS inline, CSP estricta (PAE-017)

- **Fecha:** 2026-10-23
- **Alcance:** `b7a9881..e462650` (rama `feature/frontend-modular`, release 0.5.0 sin publicar), 7 commits:
  lotes A-I (`f17e3dd`, `5b8d8c9`, `ef207cd`, `ae411b1`) + 2 correcciones del `/code-review` (`b684b71`, `e462650`)
- **Score:** **4.35 / 5.00 🟢**
- **Responsable:** Claude Code (Opus 5.5) — `/code-review` + `auditoria-ingenieria`, una pasada
- **Estado:** hallazgos presentados al usuario, sin corregir (salvo los 2 del code-review, ya corregidos)

## 1. Contexto y método

La fase saca todo el JS inline a 19 módulos ES nativos (`main.js` + `core/`, `components/`, `features/`, `pages/`,
`theme-init.js` clásico), elimina `app.js` (767 líneas) y el código de depuración, quita `'unsafe-inline'` de
`script-src`, sirve `/static/js/**` con `Cache-Control: no-cache` y corrige el XSS del tooltip del pastel de Inicio.

**Cómo se implementó:** el código se validó antes en un worktree (`/tmp/sdd-fase2-wt`) y se copió a la rama con
`rsync` en 4 commits. Esa copia introdujo 2 divergencias que encontró el `/code-review` (ver H-403).

**Herramientas:**

| Herramienta | Uso |
|---|---|
| `/code-review` (skill en segundo plano) | `diff -rq` contra el worktree: 2 hallazgos (`app.js` sin borrar; `ui_common.py` copiado a `app/ui_common.py` en vez de `app/routers/`). Confirmó orden de carga, ausencia de `hx-on`/`hx-vars`/filtros que pidan `unsafe-eval`, `tojson` en atributo con comillas simples, OOB en `afterSwap`. No usó Serena/codebase-memory (diff casi todo JS/Jinja). |
| Serena | `activate_project` (7 memorias). `find_referencing_symbols(_static_asset_version)`: un solo llamador de producción (`templates.env.globals["static_version"]`, `ui_common.py:49`) + el test de regresión. |
| codebase-memory | `detect_changes --since b7a9881`: impacto Python limitado a `_static_asset_version`, el arnés (`canonical`, `_get`) y un test. Índice anterior al borrado de `app.js`. |
| **Fuera de cobertura → `grep`/`Read`/`git diff`** | Los 19 módulos JS (Serena solo tiene LSP de Python), las 18 plantillas Jinja, `.gitea/workflows/`, `CHANGELOG.md`. |
| Ejecución | 423 unit, 40 snapshots equivalentes, suite e2e completa (ver §1.1). |

### 1.1 Verificación ejecutada sobre HEAD

| Comprobación | Resultado |
|---|---|
| Unit (`tests/unit`, suite completa) | 423 passed |
| Arnés `ui_snapshot.py check` | OK: 40 snapshots equivalentes |
| e2e (`tests/e2e -m e2e`, suite completa) | 70 passed (47 previos + 23 nuevos) |
| `VERSION` / `.bumpversion.cfg` | sin cambios en el diff (REQ-12) |

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ✅ | Un módulo por responsabilidad, cada uno con `init()`; `main.js` solo importa e inicializa. `core/content.js` centraliza el "contenido nuevo" (DOMContentLoaded + `afterSwap`). |
| 2 | Cohesión / Acoplamiento | ⚠️ | Sin ciclos (grafo validado por `test_module_graph_resolves_and_has_no_orphans`). Acoplamiento a ids del DOM y a `window.Chart`/`window.htmx` (inherente sin build). PAE-015 quedó acoplado al JS inline que ya no existe (H-401). |
| 3 | Capas / Límites | ✅ | Plantillas solo declaran `data-js-*`; el comportamiento vive en `static/js`. El único cambio Python es de infraestructura (cabeceras, hash de assets). |
| 4 | Seguridad | ✅ | `script-src 'self'` (`main.py:77`); tooltip con `textContent` (`home-charts.js:103-123`); `tojson` en atributo con comillas simples (`widget_chart.html:5`). `style-src 'unsafe-inline'` sigue, planificado en fase 3. |
| 5 | Robustez | ⚠️ | Un enhancer que lanza corta los siguientes (H-404). `fetch` de stock sin comprobar `res.ok` (H-405, previo). |
| 6 | Consistencia transaccional | ✅ N/A | La fase no toca datos ni transacciones. |
| 7 | Observabilidad | ⚠️ | Los errores de JS en producción no se reportan a ningún sitio, y hay ~20 `catch (e) {}` silenciosos heredados de `app.js`. En desarrollo los cubre la guarda `js_errors`, solo en los e2e nuevos (H-402). |
| 8 | Configuración | ✅ | Sin hardcodes nuevos. Cache-busting por contenido de todos los assets (`ui_common.py:26-46`, `_static_asset_version`). |
| 9 | Pruebas | ⚠️ | 9 unit de guarda + 23 e2e nuevos validados por mutación + arnés. Huecos: guarda solo en los e2e nuevos (H-402); la copia por rsync se dio por verificada sin la suite completa (H-403). |
| 10 | Escalabilidad | ⚠️ | `static_version()` lee y hashea 23 ficheros (~430 KB) en cada llamada, 6 veces por carga del dashboard (H-406). |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 | 5 | 0.50 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 0.40 |
| 3 | Capas / Límites | 10 | 5 | 0.50 |
| 4 | Seguridad | 15 | 4 | 0.60 |
| 5 | Robustez | 15 | 4 | 0.60 |
| 6 | Consistencia transaccional | 10 | 5 | 0.50 |
| 7 | Observabilidad | 10 | 4 | 0.40 |
| 8 | Configuración | 5 | 5 | 0.25 |
| 9 | Pruebas | 10 | 4 | 0.40 |
| 10 | Escalabilidad | 5 | 4 | 0.20 |
| | **Total** | 100 | | **4.35 🟢** |

## 4. Hallazgos

### Corregidos durante el `/code-review`

- **CR-1 (Alto) — `app.js` sin borrar.** `rsync` no propaga borrados. Rompía `test_module_graph_resolves_and_has_no_orphans` y `test_no_debug_code_left`. Corregido en `b684b71`.
- **CR-2 (Alto) — `_static_asset_version` nuevo en `app/ui_common.py` en vez de `app/routers/ui_common.py`.** HEAD seguía hasheando una lista fija que incluía `app.js`: el `?v=` no cambiaba al editar un módulo, y `test_static_asset_version_changes_with_any_nested_module` fallaba (el CI lo habría parado). Corregido en `e462650`.

### Alto

**H-401 — PAE-015 (pendiente) edita el JS inline de `widget_chart.html`, que ya no existe.**
- Dimensión: Cohesión / Acoplamiento (entre PAEs)
- Evidencia: `specs/copiloto-ventas-ubicacion-mensual/tasks.md:355-474` (T3 "Gráfico multi-serie en `widget_chart.html`", Step 1 `Read app/templates/partials/widget_chart.html`). Tras esta fase, `widget_chart.html` es un `<canvas data-js-widget-chart>` de 6 líneas y el dibujo vive en `components/widget-chart.js` (`chartConfig`, un solo dataset).
- Riesgo: cc_local ejecuta T3 sobre un fichero que no tiene lo que el envelope describe. En el mejor caso falla la verificación; en el peor reintroduce un `<script>` inline, que la CSP bloquea y `test_script_tags_per_template` rechaza.
- Recomendación: regenerar T3 de PAE-015 vía SDD-Agent sobre esta rama. El cambio multi-serie va en `widget-chart.js::chartConfig` (`payload.datasets`) y su test en un e2e o en el unit de atributos. Revisar también el e2e de PAE-015 (ya se había ajustado a la CSP antigua).

### Medio

**H-402 — La guarda de errores JS/CSP solo cubre los 23 e2e nuevos.**
- Dimensión: Pruebas / Observabilidad
- Evidencia: `tests/e2e/conftest.py:282` (`js_errors`), usada solo por `test_ui_tour_csp_e2e.py` y `test_js_behaviors_e2e.py`. Los 47 e2e previos (compras, ventas, traspaso masivo, swaps de error 4xx, sugerencias) no la usan.
- Riesgo: el recorrido nuevo abre y cierra pestañas y modales, pero no envía formularios. Una excepción en un listener de envío (p. ej. `modals.js::onAfterRequest`, `swap.js`) o una violación de CSP en una respuesta de POST no haría fallar ningún test.
- Recomendación: fixture `autouse` en `tests/e2e/conftest.py` que enganche `pageerror`/`console.error` a `page` y falle en el teardown, con una lista blanca explícita si hiciera falta.

**H-403 — La copia por rsync se dio por verificada ejecutando solo los tests nuevos.**
- Dimensión: Pruebas (proceso)
- Evidencia: CR-1 y CR-2. Al cerrar la implementación se corrieron los 9 unit nuevos y los 23 e2e nuevos, no la suite unitaria completa, que habría fallado en `test_ui_regressions.py`.
- Riesgo: un "verde" parcial ocultó una regresión que el gate de CI habría parado al desplegar.
- Recomendación: trasladar trabajo validado de un worktree con commits (`git cherry-pick`/`git format-patch`), no con `rsync`; y antes de declarar una fase terminada, `git diff --stat <worktree-HEAD> HEAD` vacío + suite completa (unit + arnés + e2e).

### Bajo

**H-404 — Un enhancer que lanza deja sin ejecutar los siguientes.** `core/content.js:12` (`enhancers.forEach(fn => fn(root, fromSwap))`) no aísla cada enhancer. Un spec de widget con `type` que Chart.js no conoce, o un fallo de JsBarcode fuera de su `try`, deja sin notas o sin gráficos el contenido de ese swap. Igual que el IIFE de `app.js`. Recomendación: `try { fn(...) } catch (e) { console.error(e) }` por enhancer; `console.error` lo atrapa la guarda de los e2e (habría que ajustar `test_no_debug_code_left`, que prohíbe `console.`).

**H-405 — `features/transfers.js:33` no comprueba `res.ok`.** Si `/ui/transfers/stock/{sku}` devuelve 404 o la sesión caducó (redirección al login), el HTML de la respuesta acaba en el campo cantidad. Heredado tal cual de `app.js:175`. Recomendación: `if (!res.ok || res.redirected) return '0'`.

**H-406 — `static_version()` hashea todos los assets en cada llamada.** `ui_common.py:26`: 23 ficheros (~430 KB), 6 llamadas por carga del dashboard (`layouts/app_shell.html`), unos 2,5 MB de lectura + SHA-1 por render (antes ~1,7 MB). Despreciable en LAN y con un usuario; recomendable memoizar por `(ruta, mtime)` si crece.

**H-407 — Nota para la fase 3: htmx inyecta un `<style>` inline.** htmx 1.9 inserta los estilos de `.htmx-indicator` con `includeIndicatorStyles` (true por defecto). Hoy lo permite `style-src 'unsafe-inline'`; al quitarlo en la fase 3, añadir `<meta name="htmx-config" content='{"includeIndicatorStyles":false}'>` y mover esos estilos a `app.css`.

**H-408 — El arnés ignora todo `data-js-*`.** `tests/snapshots/ui_snapshot.py` (`_is_js_wiring_attr`): renombrar o quitar un `data-js-*` no cambia el snapshot. Es una decisión de diseño (D del PAE) y lo cubren los e2e de comportamiento, pero solo los cableados que esos e2e ejercitan. Sin acción; dejarlo explícito para la fase 5 (reagrupar `partials/`).

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-401 (regenerar T3 de PAE-015), H-402 (fixture autouse), H-405 |
| 🎯 Estratégicos | — |
| 📋 Baja prioridad | H-403 (proceso, ya corregido el efecto), H-404, H-406, H-407 (en la fase 3) |
| ❌ Evitar | H-408 (no compensa: los e2e cubren el comportamiento) |

## 6. Backlog priorizado

1. H-401 — regenerar T3 de PAE-015 antes de ejecutarlo (bloquea PAE-015, no esta fase).
2. H-402 — fixture `autouse` de errores JS en todos los e2e.
3. H-405 — `res.ok` en el stock de traspasos.
4. H-404 — aislar enhancers.
5. H-407 — al empezar la fase 3.
6. H-406 — memoizar `static_version()` si el dashboard crece.

## 7. Plan 30/60/90

- **30 días:** H-401 antes de lanzar PAE-015; H-402, H-404 y H-405 en un commit de mantenimiento dentro del release 0.5.0. Cierre: los 70 e2e con la guarda activa en verde y `grep -c js_errors` innecesario (autouse).
- **60 días:** fase 3 con H-407 incluido en su PAE. Cierre: `style-src 'self'` y 0 violaciones en el recorrido.
- **90 días:** H-406 si el número de assets o de usuarios crece; revisar H-408 en la fase 5.
