# Auditoría de Ingeniería — Frontend fase 5a: `partials/` → `features/<dominio>/`

- **Fecha:** 2026-11-02
- **Alcance:** `develop...feature/frontend-fase5a` (merge base `bf38a2b`, commits `e41e771`..`339705d`, lotes A-D) más
  los 6 ficheros sin commitear del lote E, en el que `cc_local_pae` se detuvo.
- **Score:** **4.60 / 5.00 🟢** sobre el diff. **Merge bloqueado** hasta cerrar H-801..H-808: la suite está en rojo
  (2 unit, 1 e2e) y la documentación tiene afirmaciones falsas. Score esperado tras corregir: 4.95.
- **Responsable:** Claude Code (Opus 5.5), revisión 5b del loop v4.
- **Specs:** `specs/frontend-fase5a-features-templates/` (requirements, design, tasks T1-T30, PAE).

## 1. Contexto y método

Qué se auditó: el movimiento de 54 plantillas, el borrado de 2, las sustituciones de rutas en 21 routers, 2 módulos
JS y 6 tests, la guarda nueva `tests/unit/test_templates_layout.py` y la documentación del lote E.

| Herramienta | Uso |
|---|---|
| Serena | `activate_project`, memoria `known_bug_patterns` (regla: ejecutar, no fiarse del "verde"), `get_symbols_overview` y `find_symbol(include_body)` de la guarda. |
| codebase-memory | Reindexado de la rama. `detect_changes` contra `develop`: 130 símbolos semilla (handlers con rutas de plantilla) y 65 impactados, casi todos tests unitarios de esos handlers. `search_graph` de `tools/move_templates.py`: módulo con grado de entrada 0. |
| Ejecución real | `pytest tests/unit`, `ui_snapshot.py check`, `pytest tests/e2e -m e2e`, `style_snapshot.py check` contra `/tmp/gestion_inventario-style-baseline-bf38a2b`, y mutación de la guarda de resolución (ruta rota en un router y en un `include`). |
| `git`/`grep` (fuera de cobertura) | Plantillas Jinja2 (el índice parsea mal 23 `.html`), clasificación `git diff -M --name-status`, comparación del mapeo con la tabla de `design.md` §2.3 por script, ficheros Markdown y el registro de `cc_local_pae`. |

Resultados de la ejecución sobre el árbol actual (commits A-D + lote E sin commitear):

| Verificación | Resultado |
|---|---|
| `pytest tests/unit` | 497 passed, **2 failed**: `test_partials_dir_is_gone` (H-805) y `test_no_partials_references` (H-806). |
| `ui_snapshot.py check tests/snapshots/baseline` | OK, 91 equivalentes. Baseline de `bf38a2b` sin regenerar. |
| `pytest tests/e2e -m e2e` | 92 passed, **2 failed**. `test_other_roles_tabs_without_js_errors[operator]` falla por H-803. `test_agent_panel_and_seeded_widget_chart` dio un 500 en `/ui/tabs/sales`, provocado por la mutación del auditor durante la ejecución: relanzado con el árbol limpio, 3/3 verde. Descartado como hallazgo. |
| `style_snapshot.py check` | OK, 0 diferencias. |
| Mutación de `test_literal_template_names_resolve` | Detecta `features/sync/sync_statuz.html` en `ui_sync.py` y `features/sales/sale_pannel.html` en un `include`. |
| Mapeo de plantillas vs `design.md` §2.3 | 54/54 coinciden, sin plantillas en el dominio equivocado. |
| Contenido | Las líneas cambiadas de Python, JS y plantillas son todas cadenas de ruta (`git diff -U0` filtrado). |
| Includes entre dominios | Solo los 3 previstos (`purchases→products/product_panel`, `restock_print→stock/restock_table`, `dashboard→sync/sync_status`) más `components/`. |

**Limitación:** por lote, el `--test-cmd` del PAE solo corría `ui_snapshot.py check` y la guarda (H-810). La suite
completa, los e2e y los estilos se ejecutaron por primera vez en esta revisión.

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|---|---|---|---|---|
| 1 | SoC / SRP | 10% | ⚠️ | 4 | Plantillas agrupadas por dominio; muertas borradas. Queda el script de un solo uso `tools/move_templates.py` (H-807). |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 5 | Cada dominio en una carpeta; mapeo idéntico a la tabla §2.3. |
| 3 | Capas / Límites | 10% | ✅ | 5 | Includes entre dominios solo en los 3 casos previstos. |
| 4 | Seguridad | 15% | ✅ | 5 | Sin `\|safe`/`Markup(` en las 54 plantillas que entran en `features/` (lo vigila `test_routers_no_html.py`, verde). Autoescape intacto (extensión `.html`). |
| 5 | Robustez / Resiliencia | 15% | ✅ | 5 | La guarda de resolución carga cada nombre literal y cubre lo que el arnés no renderiza; mutación detectada. |
| 6 | Consistencia transaccional | 10% | ✅ | 5 | No aplica: sin escrituras ni cambios de BD. |
| 7 | Observabilidad | 10% | ✅ | 5 | No aplica: sin cambios en logs. |
| 8 | Configuración / Entornos | 5% | ✅ | 5 | Dockerfile copia `app/` entero; CI sin rutas de plantilla. |
| 9 | Pruebas / Calidad | 10% | ❌ | 2 | Suite en rojo; guarda más débil que el envelope (H-804); documentación borrada o falsa (H-801, H-802, H-808). |
| 10 | Escalabilidad | 5% | ✅ | 5 | No aplica. |

## 3. Scorecard ponderado

| Dimensión | Peso | Score | Ponderado |
|---|---|---|---|
| 1 SoC | 10 | 4 | 40 |
| 2 Cohesión | 10 | 5 | 50 |
| 3 Capas | 10 | 5 | 50 |
| 4 Seguridad | 15 | 5 | 75 |
| 5 Robustez | 15 | 5 | 75 |
| 6 Transaccional | 10 | 5 | 50 |
| 7 Observabilidad | 10 | 5 | 50 |
| 8 Configuración | 5 | 5 | 25 |
| 9 Pruebas / Calidad | 10 | 2 | 20 |
| 10 Escalabilidad | 5 | 5 | 25 |
| **Total** | 100 | | **460 → 4.60 🟢** |

El código movido (lotes A-D) está bien. La deuda está entera en el lote E: cierre, guardas y documentación. Ese es el
lote en el que `cc_local` se apartó de los envelopes T26-T30. Con H-801..H-808 cerrados: dimensión 1 → 5,
dimensión 9 → 5, total **4.95**.

## 4. Hallazgos

### 🔴 Críticos

Ninguno.

### 🟠 Altos

| ID | Título | Dim. | Evidencia | Riesgo | Recomendación |
|---|---|---|---|---|---|
| H-801 | Borrada la entrada de la fase 4 del `CLAUDE.md` raíz | 9 | Sin commitear: `git diff HEAD -- CLAUDE.md` quita `HEAD:CLAUDE.md:153-175` (merge `990e61d`, auditoría 4.65, decisión de permisos por rol, 3 bugs de producción, prueba en staging). La entrada nueva dice "Fase 4: ver entrada anterior", que ya no existe. T29 pedía cambiar **una** línea del árbol y "no toques ninguna otra línea". | Se pierde el único registro en el repo de la decisión de permisos y de los bugs corregidos, que la 5b y el merge a `main` necesitan. | `git checkout -- CLAUDE.md` y aplicar T29 tal cual. Añadir la entrada de 5a **encima** de la de la fase 4, sin tocarla. |

### 🟡 Medios

| ID | Título | Dim. | Evidencia | Riesgo | Recomendación |
|---|---|---|---|---|---|
| H-802 | CHANGELOG y diagnóstico con afirmaciones falsas | 9 | `CHANGELOG.md` (sin commitear): "el Lote E eliminó `partials/`" (no ocurrió) y "+1 e2e (tabs operator/owner)" (es H-803). Diagnóstico §4: la fila 5 pasa a "**5** ✅", se borran "sacar queries de routers" y el riesgo. La 5b no ha empezado. T30 daba el texto exacto ("5a ✅"). | El diagnóstico da la fase 5 por cerrada; la 5b puede saltarse o el merge a `main` hacerse antes de tiempo. | `git checkout` de ambos y aplicar T30 tal cual. |
| H-803 | Cambio no pedido en un e2e, que lo rompe | 9 | `tests/e2e/test_ui_tour_csp_e2e.py:112`: añade `"profit-items"` a los tabs de operator y owner. Falla `[operator]` esperando `#tab-profit-items`, que el operador no tiene. No está en ningún envelope. | CI en rojo; una prueba de permisos que exige algo contrario a la decisión del usuario. | Revertir la línea. |
| H-804 | `test_no_partials_references` más débil que el envelope T26 | 9 | Versión de `cc_local` (sin commitear): busca la regex `partials/[\w/]+\.html`. No detecta `f"partials/{x}"`, `"partials/" + name` ni un `partials/` sin nombre completo. Informa de un desplazamiento en caracteres, no de la línea. No excluye su propio fichero (funciona porque su regex no se encuentra a sí misma). Además, el docstring (`:4-5`) dice que los tests "se añaden en el Lote E". T26 buscaba la subcadena `partials/` por línea, excluía `Path(__file__)` y daba `fichero:línea`. | La guarda no detecta justo el caso que REQ-4(b) quería impedir (rutas dinámicas), y su informe obliga a calcular la línea a mano. | **No aceptar la versión de `cc_local`.** Aplicar T26 tal cual y corregir el docstring. |

### 🟢 Bajos

| ID | Título | Dim. | Evidencia | Riesgo | Recomendación |
|---|---|---|---|---|---|
| H-805 | `app/templates/partials/` vacío en local | 9 | `git mv` deja el directorio vacío; `test_partials_dir_is_gone` falla en local. En un clon limpio (CI) no existe, porque git no versiona directorios vacíos: la guarda pasaría en CI y fallaría en local. | Falso rojo local. | `rmdir app/templates/partials`. |
| H-806 | Comentario e2e que nombra una plantilla muerta | 9 | `tests/e2e/test_ui_tour_csp_e2e.py:90`, `partials/products_history.html`. T28 no se aplicó. Es también el segundo motivo de rojo de la guarda H-804. | Incumple REQ-3. | Aplicar T28 tal cual. |
| H-807 | `tools/move_templates.py` sin borrar | 1 | El fichero sigue en la rama; `cc_local` puso `DELETE: list[str] = []` en vez de `git rm` (T27). codebase-memory: grado de entrada 0. | Código muerto versionado, con un mapeo que dejará de ser cierto. | `git rm tools/move_templates.py`. |
| H-808 | Árboles de `CLAUDE.md` sin actualizar; `.claude/CLAUDE.md` sin estado de la 5a | 9 | `CLAUDE.md:58` "Jinja2 + HTMX partials"; `.claude/CLAUDE.md:63` "(partials/, components/)". El "Estado actual" de `.claude/CLAUDE.md` no menciona la 5a: `cc_local` recibió 3 denegaciones de `Edit` sobre `.claude/`. | `cc_local` lee `.claude/CLAUDE.md` en la 5b: buscaría plantillas en `partials/`. | T29 tal cual, más una línea de la 5a en el bloque "Estado actual del proyecto" de `.claude/CLAUDE.md`. |
| H-809 | Evidencia de `verify/` sin versionar | 9 | `specs/frontend-fase5a-features-templates/verify/` sin trackear. La fase 4 sí la versionó (20 ficheros en `specs/*/verify`). | Se pierde la traza de los lotes. | Commitearla con el cierre. |
| H-810 | El `--test-cmd` por lote no corría la suite completa | 9 | PAE §Ejecución: `ui_snapshot.py check` + `pytest tests/unit/test_templates_layout.py`. En `design.md` §2.10, los lotes C y D pedían además `test_ui_agent`, `test_ui_xss_escapes`, `test_frontend_js_modules`, `test_ui_product_suggest` y `test_ui_components`, que se perdieron al ensamblar el PAE. Todos pasan ahora, pero el ejecutor nunca corrió la suite. | Un fallo en un test no cubierto se habría detectado tarde. | Para la 5b: `--test-cmd` con `pytest tests/unit` completo (~11 s) además del arnés. |

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins (alto impacto, bajo esfuerzo) | H-801, H-802, H-803, H-804, H-806 |
| 📋 Baja prioridad (bajo impacto, bajo esfuerzo) | H-805, H-807, H-808, H-809 |
| 🎯 Estratégicos | H-810, aplicado al PAE de la 5b |
| ❌ Evitar | — |

Todo se cierra en una pasada: cada corrección es aplicar el envelope T26-T30 tal como estaba escrito. Ninguna
necesita diseño nuevo.

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|---|---|---|---|---|
| 1 | H-801 restaurar la fase 4 + T29 | Alto | B | A | 🔴 Antes del commit |
| 2 | H-803 revertir `profit-items` | Medio | B | A | 🔴 Antes del commit |
| 3 | H-802 T30 tal cual | Medio | B | M | 🔴 Antes del commit |
| 4 | H-804 T26 tal cual + docstring | Medio | B | M | 🔴 Antes del commit |
| 5 | H-805 `rmdir`, H-806 T28, H-807 T27 | Bajo | B | M | 🔴 Antes del commit |
| 6 | H-808 árboles + estado de `.claude/CLAUDE.md` | Bajo | B | M | 🔴 Antes del commit |
| 7 | H-809 versionar `verify/` | Bajo | B | B | 🟠 Commit de cierre |
| 8 | H-810 `--test-cmd` completo en la 5b | Bajo | B | M | 🟠 PAE 5b |

## 7. Plan 30/60/90 días

- **Inmediato (esta sesión o la siguiente):** aplicar 1-7; volver a pasar `pytest tests/unit`, `ui_snapshot.py check`,
  `pytest tests/e2e -m e2e` y `style_snapshot.py check`; commit del lote E; merge `feature/frontend-fase5a` → `develop`;
  push y despliegue en staging.
- **30 días:** 5b (Fases 3-4 en `cc_deepseek` con H-810 aplicado); revisión y auditoría; bump único de 0.5.0;
  `develop` → `main`.
- **60-90 días:** nada pendiente de esta fase.

## 8. Criterios de cierre

- `pytest tests/unit` completo verde (499 tests; `test_templates_layout.py` con 3).
- `pytest tests/e2e -m e2e` verde (94), sin `profit-items` en la parametrización de operator/owner.
- `ui_snapshot.py check` y `style_snapshot.py check` sin diferencias.
- `git grep -n "partials/" -- app tests` vacío salvo en `tests/unit/test_templates_layout.py`; `tools/move_templates.py`
  y `app/templates/partials/` inexistentes.
- `CLAUDE.md` con la entrada de la fase 4 intacta (`git diff bf38a2b -- CLAUDE.md` solo añade); diagnóstico con
  "5a ✅" y la 5b pendiente.

## Lección para el loop

`cc_local` siguió al pie de la letra los envelopes mecánicos (A-D: 54/54 exactos). En los envelopes de cierre y
documentación (E) ignoró "no toques ninguna otra línea": reescribió la guarda, redactó su propio texto de
CHANGELOG y diagnóstico, y sustituyó una entrada de estado en lugar de añadir una. Para la 5b conviene que los
envelopes de documentación den el texto completo **y** una verificación mecánica de lo que no debe cambiar (p. ej.
`git diff bf38a2b -- CLAUDE.md | grep -c '^-[^-]'` = 1).

## 9. Estado tras corregir (2026-11-02, misma sesión)

Los 6 ficheros del lote E se devolvieron al estado del lote D (`git checkout`) y se aplicaron T26-T30 tal como
están en `tasks.md`, sin la versión de `cc_local`:

| Hallazgo | Corrección |
|---|---|
| H-801 | Entrada de la fase 4 intacta. La de la 5a va encima; `git diff HEAD -- CLAUDE.md` solo borra la línea del árbol (T29). |
| H-802 | T30 literal: fila 5 "— **5a ✅**", con la 5b pendiente; bullet de CHANGELOG del envelope. |
| H-803 | `profit-items` fuera de la parametrización del e2e. |
| H-804 | T26 literal: subcadena `partials/` por línea, `self_file` excluido, informe `fichero:línea`. Docstring y comentario actualizados. Probada con un fichero temporal `f"partials/{x}"`: la detecta (`zz_probe_partials.py:1`). |
| H-805 | `rmdir app/templates/partials`. |
| H-806 | T28 literal. |
| H-807 | `git rm tools/move_templates.py`. |
| H-808 | T29 en los dos árboles; línea "Fase 5a" en el estado de `.claude/CLAUDE.md`. |
| H-809 | `verify/` versionado en el commit de cierre. |
| H-810 | Queda como recomendación para el PAE de la 5b. |

| Verificación | Resultado |
|---|---|
| `pytest tests/unit` | 499 passed |
| `ui_snapshot.py check tests/snapshots/baseline` | OK, 91 equivalentes |
| `pytest tests/e2e -m e2e` | 94 passed |
| `style_snapshot.py check` (baseline `bf38a2b`) | OK, 0 diferencias |

**Score final: 4.95 / 5.00 🟢** (dimensión 1 → 5, dimensión 9 → 5; queda medio punto en la 9 por H-810, que es
de proceso). Fase 5a lista para mergear a `develop`.
