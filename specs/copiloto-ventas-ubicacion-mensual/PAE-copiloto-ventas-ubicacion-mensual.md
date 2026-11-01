# PAE-015: copiloto-ventas-ubicacion-mensual

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     ═══════════════════════════════════════════════════════════════
     1. Leer CLAUDE.md del proyecto (raíz Y .claude/CLAUDE.md). Si no existe, detenerse.
     2. Leer sección 3 (Arquitectura objetivo) completa antes de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 5.0) — sin excepción.
     4. Un task envelope a la vez, en el orden de dependencias de la sección 4.
     5. Si algo no está especificado aquí: no se inventa, se reporta.
     6. Archivos fuera del inventario (3.2): no se tocan bajo ninguna circunstancia.
     7. CADA TAREA incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: ejecutar el script de verificación completa (sección 7.2).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `feature/frontend-modular` @ `2638bf8` (fase 2 del frontend cerrada) |
| **Rama de trabajo** | feature/frontend-modular |
| **Fecha** | 2026-10-18, regenerado 2026-10-23 |
| **Versión actual** | v0.4.15 |
| **Versión objetivo** | sin cambio: entra en el release 0.5.0 (sin publicar), que acumula sin bump por commit |
| **PAE anterior** | PAE-014 agent-copiloto-diagnostico |
| **Fase** | planificación (Claude Code) → ejecución (OpenCode + Qwen3.6-35B-A3B local, GLM-4.7-Flash fallback) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/copiloto-ventas-ubicacion-mensual/requirements.md` | Bug reportado, causas C1–C5, REQ-1..6 con Gherkin, NFRs, fuera de alcance |
| Phase 2 — Design | `specs/copiloto-ventas-ubicacion-mensual/design.md` | Enfoque, payload multi-serie, seguridad, trade-offs D1–D7, pre-mortem de 10 dimensiones |
| Phase 3 — Task Envelopes | `specs/copiloto-ventas-ubicacion-mensual/tasks.md` | 10 envelopes (T1a, T1b, T2–T9; T8 sin cambios) autocontenidos |

### Skills asignadas a OpenCode

- [ ] `backend-developer` — servicio, tool, prompt, grafo (T1a, T2, T4, T5)
- [ ] `frontend-developer` — gráfico multi-serie (T3)
- [ ] `test-automation-engineer` — tests del servicio, eval y e2e Playwright (T1b, T6, T7)
- [ ] `release-manager` — comprobar que la versión no cambia (T8)
- [ ] `technical-writer` — CHANGELOG (T9)

---

## 1. Objetivo

### 1.1 Qué se construye

Una tool nueva del copiloto, `sales_by_location_monthly(months)`, que devuelve las ventas de cada
punto de venta mes a mes (una serie por ubicación, N meses calendario incluyendo el actual),
respaldada por `ReportingService.sales_by_location_monthly`. Los gráficos del Dashboard Dinámico
pasan a admitir varias series (`payload.datasets`), para dibujar barras agrupadas por mes con
leyenda. El prompt del planner aprende la tool nueva, la regla "últimos N meses = N meses" y una
regla de honestidad. El grafo titula el widget de rescate con la descripción de la tool y avisa
en el reply cuando la tool falla.

### 1.2 Por qué

El 2026-10-18 el usuario pidió en producción "barras de ventas de cada punto de venta por mes,
últimos 6 meses, tres barras por mes". El copiloto devolvió el total del período por ubicación
(sin meses), tomó 7 meses en vez de 6, tituló una tabla con el nombre interno `sales_by_location`
y afirmó haber hecho lo pedido. Causas C1–C5 en `requirements.md`.

### 1.3 Valor esperado

- La petición del usuario produce 6 meses × 3 barras con los datos reales de producción
  (validado: may–oct 2026 = tabla de `requirements.md`, exacto al céntimo).
- "Últimos N meses" se resuelve sin aritmética del LLM (parámetro `months`).
- El copiloto deja de afirmar que hizo algo que no hizo.

### 1.4 Qué NO se toca

- `sales_by_location`, `profit_by_location` y el resto de tools (salvo añadir una).
- `app/agent/widgets.py`, `app/routers/ui_agent.py`, `widget_grid.html`, `widget_table.html`.
- Esquema de BD, migraciones, seed, auth, contrato de `POST /ui/agent/chat`.
- Widgets ya guardados en producción.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Ventana por `months` (int) | `start`/`end` ISO | El LLM ya calculó mal la ventana (7 meses); `_month_series` es determinista | La tool no admite rangos arbitrarios |
| D2 | Tool nueva | Flag `group_by_month` en `sales_by_location` | No cambia el payload de una tool existente; menos ambigüedad para un modelo pequeño | 34 tools en el prompt |
| D3 | Agregación en Python | `date_trunc` en SQL | SQLite (tests, desktop offline) no tiene `date_trunc`; mismo patrón que `purchases_by_month` | Volumen bajo, acotado a 24 meses |
| D4 | Payload `datasets` + `columns`/`rows` | Un widget por ubicación | Regla de 1 widget por turno; comparación en la misma gráfica | `widget-chart.js::chartConfig` gana una rama multi-serie (solo bar/line) |
| D5 | Excluir ubicaciones sin ventas | Series a cero | "En el mes que aplique"; Almacén Central no vende | Una ubicación que deja de vender desaparece de la ventana |
| D6 | Aviso genérico si falla la tool | Mostrar la excepción | No filtrar detalles internos | El detalle sigue en `state["error"]` y en los logs |
| D7 | Título sintetizado desde la descripción | Diccionario manual | La descripción ya existe y está en español | Si una descripción cambia, cambia el título |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
Usuario ─► POST /ui/agent/chat ─► graph: planner ─► execute ─► build ─► narrate✦
                                     │ prompt✦        │
                                     │                ▼
                                     │      TOOL_REGISTRY["sales_by_location_monthly"]★
                                     │                │
                                     │                ▼
                                     │      ReportingService.sales_by_location_monthly★
                                     │                │  (_month_series · inventory_movements ⟕ locations)
                                     ▼                ▼
                         widget sintetizado✦   payload {labels, datasets, columns, rows}
                                                      │
                                                      ▼
                              widget_grid.html · ─► widget_chart.html · ─► widget-chart.js✦ (N datasets + leyenda)
★ nuevo   ✦ modificado   · sin cambios
```

### 3.2 Inventario de archivos

> **Scope duro.** `git diff --name-only 2638bf8` debe mostrar exactamente estos 12 archivos (+ `specs/copiloto-ventas-ubicacion-mensual/`; `CLAUDE.md` y `.claude/CLAUDE.md` los actualiza Claude Code al cerrar, en un commit aparte).

| Archivo | Operación | Envelope |
|---------|-----------|----------|
| `app/services/reporting_service.py` | MODIFY | T1a |
| `tests/unit/test_reporting_service.py` | MODIFY | T1b |
| `app/agent/tools.py` | MODIFY | T2 |
| `tests/unit/test_agent_tools.py` | MODIFY | T2 |
| `app/static/js/components/widget-chart.js` | MODIFY | T3 |
| `app/agent/prompts.py` | MODIFY | T4 |
| `tests/unit/test_agent_prompts.py` | MODIFY | T4 |
| `app/agent/graph.py` | MODIFY | T5 |
| `tests/unit/test_agent_graph.py` | MODIFY | T5 |
| `tests/eval/test_agent_tool_selection_eval.py` | MODIFY | T6 |
| `tests/e2e/test_agent_multiseries_chart_e2e.py` | CREATE | T7 |
| `CHANGELOG.md` | MODIFY | T9 |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/services/reporting_service.py — NO MODIFICAR estas firmas
class ReportingService:
    def __init__(self, db: Session, business_id: int)
    def _month_series(self, months: int) -> tuple[datetime, list[str]]
    def purchases_by_month(self, months: int = 12) -> list[dict]      # patrón a imitar

# app/agent/tools.py
TOOL_REGISTRY: dict[str, dict[str, Any]]
def tool(name: str, description: str, params: dict) -> Callable
def tool_schemas() -> list[dict]

# app/agent/graph.py
def _resolve_widget_tool_args(plan: PlanModel, widget: WidgetSpec) -> dict
def _narrate_node(state: AgentState) -> AgentState
# _execute_node: state["error"] = f"Tool {tool_name}: ..." cuando una tool falla

# app/agent/prompts.py
SYSTEM_PROMPT_TEMPLATE: str   # .format(schemas=..., today=...) — sin llaves sueltas
def build_system_prompt(last_tool: Optional[dict] = None) -> str

# tests/e2e/conftest.py
@pytest.fixture
def central_server() -> tuple[str, str]   # (base_url, db_path)
```

### 3.4 Interfaces nuevas

```python
ReportingService.sales_by_location_monthly(self, months: int = 6) -> dict
#   -> {"months": ["YYYY-MM", ...], "series": [{"location": str, "values": [float, ...]}]}
TOOL_REGISTRY["sales_by_location_monthly"]   # args {"months": int}
#   -> {"labels": [...], "datasets": [{"label", "data"}], "columns": ["Mes", ...], "rows": [[mes, ...]]}
graph._synth_title(tool_name: str) -> str
graph._TOOL_ERROR_NOTICE: str
```

---

## 4. Fases de implementación (Task Envelopes)

> Ver `specs/copiloto-ventas-ubicacion-mensual/tasks.md` para los 10 envelopes completos (T1a, T1b, T2–T9),
> cada uno con Context/Objective/Instructions/Verification/Expected Report. OpenCode ejecuta
> un envelope a la vez, en este orden:

**Batch 1 (independientes):** T1a (servicio), T3 (gráfico), T4 (prompt)
**Batch 2:** T1b (tests del servicio, depende de T1a), T2 (tool, depende de T1a), T5 (grafo)
**Batch 3:** T6 (eval, depende de T2+T4), T7 (e2e, depende de T3)
**Batch 4:** T8 (comprobar versión), T9 (CHANGELOG)

`/compact` de OpenCode entre batches.

> **Pre-validación del SDD Lead (2026-10-18):** T1–T7 (antes del split de T1 en T1a/T1b, mismo código) se aplicaron mecánicamente desde
> `tasks.md` en un worktree desechable. Resultado: 396 unit passed (cobertura del 86 %), 2 e2e
> passed y el eval se colecta. Mutación: sin T3, el e2e falla (1 dataset en vez de 3). La tool
> se ejecutó contra la BD de producción en una transacción `READ ONLY` y dio exactamente la tabla
> de `requirements.md`. Esta validación encontró y corrigió en T7 que la CSP de la app no admite
> `wait_for_function` con reintentos (usa polling con `page.evaluate`).
>
> **Regeneración 2026-10-23:** tras la fase 2 del frontend, T3 pasa a `components/widget-chart.js` (sin
> test de plantilla: el JS lo fija el e2e de T7), T1a agrupa por id de ubicación (+1 test en T1b), T8 no
> cambia la versión y T9 escribe solo el CHANGELOG. Implementación previa descartada (rama
> `backup/pae015-haiku`, auditada en `docs/auditorias/2026-10-23-copiloto-ventas-ubicacion-mensual.md`, 3.10 🔴).

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO)

```bash
git checkout feature/frontend-modular && git log -1 --format=%h   # Expected: 2638bf8 (o un hijo suyo con solo specs/)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest
# Expected: 423 passed, cobertura >= 80 %
```
Si falla, **no empezar**: reportar y esperar instrucciones.

### 5.1 Verificación por envelope

Cada envelope trae su comando (`tasks.md`). No avanzar si falla. Tras 3 intentos fallidos:
reportar el error, el diff y lo intentado.

### 5.2 Tests nuevos

| Envelope | Archivo | Tests |
|----------|---------|-------|
| T1b | `tests/unit/test_reporting_service.py` | 4 (ventana + orden, aislamiento por negocio, mismo nombre, acotado de `months`) |
| T2 | `tests/unit/test_agent_tools.py` | 1 nuevo + conteo 33→34 |
| T4 | `tests/unit/test_agent_prompts.py` | 2 |
| T5 | `tests/unit/test_agent_graph.py` | 3 |
| T6 | `tests/eval/test_agent_tool_selection_eval.py` | 1 golden case (a demanda) |
| T7 | `tests/e2e/test_agent_multiseries_chart_e2e.py` | 2 e2e Playwright |

### 5.3 Tests existentes que deben seguir pasando

- [ ] Suite unitaria completa: 385 → **396** passed, cobertura ≥ 80 %
- [ ] `tests/unit/test_agent_graph.py::test_plan_synthesizes_widget_when_retries_exhausted` — sin cambios
- [ ] `tests/unit/test_agent_graph.py::test_graph_degrades_when_tool_fails` — sin cambios
- [ ] Suite e2e completa (43 → **45**), en especial `test_tabs_smoke_e2e.py` (pestaña agent)

### 5.4 Tests e2e

**Herramienta: Playwright.** El proyecto no usa `behave`, y REQ-3 depende del render real de
Chart.js. El e2e siembra el widget directamente en la BD del servidor temporal: el LLM no se puede
invocar en CI. La selección de tool por el LLM la cubre el eval (5.5).

```bash
.venv/bin/python -m pytest tests/e2e -m e2e --no-cov -v
```

### 5.5 Eval de tool-selection (no es gate de CI — lo ejecuta Claude Code)

```bash
.venv/bin/python -m pytest tests/eval -m agent_eval -v -s -o addopts=""
# Esperado: el caso sales_by_location_monthly_6m acierta tool, type bar y months=6
```

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | El modelo pequeño sigue eligiendo `sales_by_location` para peticiones mensuales | Media | Medio | Gráfico sin meses | Regla explícita "NO uses sales_by_location para eso" + descripción de la tool + golden case en el eval | Afinar el prompt en un PAE posterior (H-003: routing en 2 pasos / few-shot) |
| R2 | Llaves `{}` en el texto del prompt rompen `.format()` | Baja | Alto | `KeyError` en cada chat | Prohibido en T4 + test de T4 que construye el prompt | Revertir T4 |
| R3 | Regresión visual en los gráficos de una serie | Baja | Medio | Leyenda visible o colores distintos | La rama `else` es idéntica al código actual + e2e `test_single_series_bar_chart_unchanged` | Revertir T3 |
| R4 | Fechas de venta en zona horaria local en el borde de mes | Baja | Bajo | Una venta del día 1 a las 00:xx cae en el mes anterior | Mismo criterio UTC que `purchases_by_month` y el resto de reportes (consistente) | — |
| R5 | e2e inestable por la CSP | Baja | Bajo | `EvalError unsafe-eval` | Polling con `page.evaluate` (ya corregido y validado) | — |
| R6 | XSS por el nombre de una ubicación | Baja | Alto | Script inyectado | `tojson` + canvas + CSP `script-src 'self'` (fase 2) | — |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (en orden)

- [ ] **Pre-check:** suite unitaria verde antes de empezar (5.0)
- [ ] **Tests:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest` → 433 passed (423 + 10 nuevos), cov ≥ 80 %
- [ ] **e2e:** `.venv/bin/python -m pytest tests/e2e -m e2e --no-cov -v` → todos pasan (incluye los 2 nuevos)
- [ ] **Mutación e2e (Claude Code):** revertir solo `widget-chart.js` → `test_multiseries_bar_chart_one_dataset_per_location` FALLA; restaurar → PASA
- [ ] **Eval (Claude Code):** 5.5 contra el LLM de dev; anotar el % de aciertos
- [ ] **Datos reales (Claude Code):** ejecutar la tool contra una copia desechable de la BD de producción (o en una transacción `READ ONLY`): los meses 2026-05..09 coinciden con la tabla de `requirements.md`
- [ ] **App:** `uvicorn app.main:app --port 10000` arranca; en Copiloto, la frase del usuario produce 6 meses × 3 barras con leyenda
- [ ] **Invariantes:** filtro `business_id`, 1 widget por turno, contrato del chat sin cambios
- [ ] **Scope:** `git diff --name-only 2638bf8` = archivos de 3.2 + `specs/copiloto-ventas-ubicacion-mensual/`
- [ ] **Versión:** `VERSION` y `.bumpversion.cfg` sin cambios
- [ ] **Docs (Claude Code, al cerrar):** "Estado actual" de `CLAUDE.md`; `.claude/CLAUDE.md` ("33 tools" → "34 tools" en las 2 apariciones + Estado actual); nota Obsidian `05-Proyectos/gestion-inventario.md`

### 7.2 Script de verificación completa

```bash
#!/bin/bash
set -e
export SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory:
echo "=== Unit (gate de cobertura) ==="
.venv/bin/python -m pytest
echo "=== e2e ==="
unset DATABASE_URL
.venv/bin/python -m pytest tests/e2e -m e2e --no-cov -v
echo "=== Eval colecta ==="
.venv/bin/python -m pytest tests/eval -m agent_eval --collect-only --no-cov -o addopts="" -q
echo "=== Versión ==="
test -z "$(git diff --name-only 2638bf8 -- VERSION .bumpversion.cfg)"
echo "=== Snapshots ==="
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
echo "=== Scope ==="
git diff --name-only 2638bf8
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados

```
feat(reporting): sales_by_location_monthly — ventas por ubicación y mes
feat(agent): tool sales_by_location_monthly (payload multi-serie)
feat(ui): gráficos del dashboard con varias series y leyenda
fix(agent): prompt — ventas mensuales por ubicación, "últimos N meses", honestidad del reply
fix(agent): título legible del widget sintetizado y aviso cuando falla la tool
test(agent): golden case del eval + e2e de barras agrupadas
docs: CHANGELOG 0.5.0 — ventas por punto de venta mes a mes
```

---

## 8. Seguridad (resumen de `design.md` §2.5)

Sin endpoints ni fronteras de auth nuevas. `months` acotado a 1..24. Aislamiento por
`business_id` cubierto por test. Los nombres de ubicación llegan al JS vía `tojson` y se pintan en
canvas. El aviso de error no expone la excepción.

## 9. Fuera de alcance (recordatorio)

Ganancia mensual por ubicación · filtro por ubicación en la tool nueva · routing en 2 pasos /
few-shot (H-003) · cambiar el `type` del widget sintetizado · migrar widgets ya guardados.
