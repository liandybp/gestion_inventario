# PAE-copiloto-dashboard-ia: Copiloto de negocio (dashboard dinámico con IA)

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer sección 5 (Arquitectura objetivo) completa antes de tocar archivos.
     3. Una fase a la vez (T1→T19 en orden). Sin fase N verificada, no empieza N+1.
     4. Si algo no está especificado aquí o en tasks.md: no se inventa, se reporta.
     5. Archivos fuera del inventario (5.2): no se tocan.
     6. CADA tarea incluye verificación. Si falla: corregir, no avanzar.
     7. Envelopes completos (código exacto) están en specs/copiloto-dashboard-ia/tasks.md
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/copiloto-dashboard-ia` |
| **Fecha** | 2026-08-14 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.5.0 |
| **PAE anterior** | ninguno |
| **Fase** | ejecución (OpenCode + `qwen36_35b_coder_mtp` local) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Phase 1 | `specs/copiloto-dashboard-ia/requirements.md` | REQ-1..12 con Gherkin + MoSCoW + NFRs |
| Phase 2 | `specs/copiloto-dashboard-ia/design.md` | modelo, API, seguridad, trade-offs, migración |
| Phase 3 | `specs/copiloto-dashboard-ia/tasks.md` | **envelopes T1..T19 con código exacto** |
| Diseño previo | `docs/plans/2026-08-14-copiloto-ia-dashboard-design.md` | decisiones de alto nivel (modelo LLM, alcance) |

## 3. Objetivo

Añadir una pestaña **"Copiloto"** al dashboard. El usuario escribe en lenguaje natural y un agente
LangGraph interpreta, consulta los datos reales del negocio activo vía `InventoryService`, y construye
**widgets** (KPI, gráfico, tabla) que se persisten por negocio y se acumulan en esa pestaña.

**Qué NO se toca:** `InventoryService` (solo lectura), `security.py`, `deps.py`, `db.py`, `auth.py`,
los routers UI existentes, y los modelos existentes (salvo añadir `DashboardWidget`).

## 4. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Agente LangGraph (plan→execute→build→narrate, retry ≤2 en plan) | usuario eligió agente completo; retry compensa fiabilidad del LLM |
| D2 | El LLM devuelve JSON (`PlanModel`) y Python despacha vía `TOOL_REGISTRY` (no `bind_tools`) | más robusto y testeable que tool-calling mágico |
| D3 | `business_id` inyectado por el router (`require_active_business_id`) | el LLM nunca decide el scoping multi-negocio |
| D4 | Widget guarda **spec** (`tool` + `tool_args`), renderer re-ejecuta la tool | datos frescos en cada carga, params pequeño |
| D5 | Render con Chart.js (ya en el stack) + partials Jinja | reutiliza lo existente |
| D6 | Concurrencia: threadpool + `asyncio.Semaphore` + timeout | el grafo/`InventoryService` son síncronos; no bloquear el event loop |
| D7 | Roles: admin/owner construyen (`ensure_admin_or_owner`), operator ve | coherente con multi-negocio |
| D8 | Modelo LLM: env vars `AGENT_LLM_*` (API externa OpenAI-compatible; default `gpt-4o-mini`) | cambiar proveedor = cambiar env var; el runtime NUNCA usa el modelo local |

## 5. Arquitectura objetivo

### 5.1 Diagrama de componentes

```
[UI HTMX] ── POST /ui/agent/chat ──▶ [ui_agent.py ★]
                                        │ ensure_admin_or_owner + business_id
                                        ▼
                                   [runner.py ★]  (to_thread + semáforo)
                                        │
                                        ▼
                                   [graph.py ★]  LangGraph
                                     plan ─▶ execute ─▶ build ─▶ narrate
                                      │            │
                                      ▼            ▼
                                [prompts.py ★]  [tools.py ★] ──▶ InventoryService (·existente)
                                      │            │
                                   [llm.py ★]   [schemas.py ★] (PlanModel/WidgetSpec)
                                        │
                                   ChatOpenAI (Gemini/GPT-4o mini/DeepSeek vía base_url)
                                        │
                                   [widgets.py ★] ──▶ dashboard_widgets (BD ★)

★ NUEVO    · existente (solo lectura)
```

### 5.2 Inventario de archivos (scope duro)

| Archivo | Operación |
|---------|-----------|
| `app/agent/__init__.py` | CREATE |
| `app/agent/config.py` | CREATE |
| `app/agent/llm.py` | CREATE |
| `app/agent/schemas.py` | CREATE |
| `app/agent/tools.py` | CREATE |
| `app/agent/prompts.py` | CREATE |
| `app/agent/graph.py` | CREATE |
| `app/agent/runner.py` | CREATE |
| `app/agent/widgets.py` | CREATE |
| `app/routers/ui_agent.py` | CREATE |
| `app/templates/partials/tab_agent.html` | CREATE |
| `app/templates/partials/widget_grid.html` | CREATE |
| `app/templates/partials/widget_kpi.html` | CREATE |
| `app/templates/partials/widget_chart.html` | CREATE |
| `app/templates/partials/widget_table.html` | CREATE |
| `app/templates/partials/widget_list.html` | CREATE |
| `app/templates/partials/agent_result.html` | CREATE |
| `app/models.py` | MODIFY (añadir `DashboardWidget`) |
| `app/main.py` | MODIFY (registrar `ui_agent`) |
| `app/templates/dashboard.html` | MODIFY (botón sidebar) |
| `requirements.txt` | MODIFY (3 deps) |
| `tests/unit/test_agent_widgets.py` | CREATE |
| `tests/unit/test_agent_tools.py` | CREATE |
| `tests/unit/test_agent_graph.py` | CREATE |
| `tests/unit/test_ui_agent.py` | CREATE |

**REFERENCE (no tocar):** `app/services/inventory_service.py`, `app/security.py`,
`app/routers/ui_common.py`, `app/db.py`, `app/deps.py`, `app/schemas.py`, `tests/conftest.py`.

### 5.3 Nuevas interfaces

```python
# app/agent/runner.py
async def run_agent(db: Session, business_id: int, message: str, history: list[dict] | None = None) -> dict
#   -> {"widgets": list[dict], "reply": str, "tool_result": dict | None}

# app/agent/widgets.py
def add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by) -> DashboardWidget
def list_widgets(db, business_id) -> list[DashboardWidget]
def widgets_with_payload(db, business_id) -> list[dict]   # [{id, type, title, payload}]
def delete_widget(db, business_id, widget_id) -> bool

# app/agent/tools.py
TOOL_REGISTRY: dict[str, {"description", "params", "fn"}]
def tool_schemas() -> list[dict]

# app/routers/ui_agent.py
GET  /ui/tabs/agent                    # tab (todos los roles)
POST /ui/agent/chat                    # chat (admin/owner)
POST /ui/agent/widget/{id}/delete      # borrar (admin/owner)
GET  /ui/agent/widgets                 # grid (todos)
```

## 6. Fases de implementación (Task Envelopes)

> **Código exacto de cada envelope en `tasks.md`.** Orden de ejecución estricto.

| Envelope | Título | Depends on | Archivos |
|----------|--------|-----------|----------|
| T1 | Dependencias | none | `requirements.txt` |
| T2 | Modelo `DashboardWidget` | T1 | `app/models.py` |
| T3 | `app/agent/config.py` | T1 | `app/agent/__init__.py`, `config.py` |
| T4 | `app/agent/llm.py` | T3 | `llm.py` |
| T5 | `app/agent/schemas.py` | T1 | `schemas.py` |
| T6 | `tools.py` parte 1 (infra + 5 tools KPI) | T5 | `tools.py` |
| T7 | `tools.py` parte 2 (4 tools chart/table) | T6 | `tools.py` |
| T8 | `app/agent/prompts.py` | T7 | `prompts.py` |
| T9 | `app/agent/graph.py` | T4,T5,T8 | `graph.py` |
| T10 | `app/agent/runner.py` | T9 | `runner.py` |
| T11 | `app/agent/widgets.py` | T2,T7 | `widgets.py` |
| T12 | `app/routers/ui_agent.py` | T10,T11 | `ui_agent.py` |
| T13 | Registrar router + botón | T12 | `app/main.py`, `dashboard.html` |
| T14 | `tab_agent` + `widget_grid` | T12 | 2 partials |
| T15 | `widget_kpi` + `widget_chart` | T14 | 2 partials |
| T16 | `widget_table` + `widget_list` | T14 | 2 partials |
| T17 | `agent_result` | T14 | 1 partial |
| T18 | Tests modelo + CRUD | T2,T11 | `test_agent_widgets.py` |
| T19 | Tests tools | T7 | `test_agent_tools.py` |
| T20 | Tests grafo (LLM fake) | T9 | `test_agent_graph.py` |
| T21 | Tests router | T12 | `test_ui_agent.py` |

**Bathes sugeridos (compactar cada 4-5 envelopes):** B1=T1..T5 · B2=T6..T9 · B3=T10..T13 ·
B4=T14..T17 · B5=T18..T21.

## 7. Plan de tests y verificación

### 7.1 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
# Expected: todos los tests existentes PASS (antes de tocar nada)
```

### 7.2 Verificación por envelope

Cada envelope incluye su comando de verificación en `tasks.md`. Tests nuevos:
- `test_agent_widgets.py` — 2 tests (CRUD + aislamiento business)
- `test_agent_tools.py` — 3 tests (count + despacho + scoping)
- `test_agent_graph.py` — 1 test (grafo con LLM fake)
- `test_ui_agent.py` — 1 test (auth del tab)

### 7.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit/` (14 archivos) — sin cambios, todos PASS tras cada batch.

## 8. Riesgos y mitigaciones

| # | Riesgo | Prob | Impacto | Mitigación |
|---|--------|------|---------|------------|
| R1 | LLM devuelve JSON inválido | Media | Medio | retry ≤2 con error inline (T8); fallback `PlanModel(reply=...)` |
| R2 | Versión de deps desactualizada | Media | Bajo | reportar error de instalación, no cambiar pins |
| R3 | Prompt injection (el LLM pide datos de otro negocio) | Baja | Alto | tools read-only + `business_id` forzado por el router (D3) |
| R4 | Redundancia de registro de routers en `main.py` | Media | Bajo | registrar `ui_agent` solo en `main.py` siguiendo el patrón de `ui_businesses` |
| R5 | Payload guardado en `params` puede quedar desactualizado | Baja | Bajo | se re-consulta al volver a preguntar; N de widgets pequeño |
| R6 | LLM caído / timeout | Media | Medio | `runner` degrada a reply "no disponible" (chat no rompe el tab) |

## 9. Definición de hecho (DoD)

- [ ] **Pre-check:** `pytest -v` — 100% tests pasaban antes (guardar output)
- [ ] **Tests:** `pytest -v` — 100% tests pasan después (incluye 7 tests nuevos)
- [ ] **App arranca:** `python -m uvicorn app.main:app` sin errores
- [ ] **Invariantes:** no se viola ninguna de `CLAUDE.md` (Base única, deps aprobadas, auth centralizado)
- [ ] **Scope:** `git diff --name-only develop` muestra exactamente los archivos de 5.2, nada más
- [ ] **Limpieza:** no hay archivos temporales (`_tmp`, `_draft`, `_vN`)

> **Generado por:** SDD Lead (Phase 4 — PAE Assembly) · 2026-08-14
