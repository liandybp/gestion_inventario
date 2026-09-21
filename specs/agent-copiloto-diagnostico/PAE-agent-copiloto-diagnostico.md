# PAE-014: agent-copiloto-diagnostico

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
     8. Al terminar: ejecutar script de verificación completa (sección 5.2 / verify.sh).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | develop |
| **Rama de trabajo** | feature/agent-copiloto-diagnostico |
| **Fecha** | 2026-09-21 |
| **Versión actual** | v0.4.8 |
| **Versión objetivo** | v0.4.9 (PATCH — regla dura del proyecto, ningún MINOR sin pedido explícito) |
| **PAE anterior** | sale-payment-method (generado, no implementado — sin relación con este) |
| **Fase** | planificación (Claude Code) → ejecución (OpenCode + Qwen3.6-35B-A3B local, GLM-4.7-Flash fallback) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Auditoría previa | `docs/auditorias/2026-09-21-agent-copiloto.md` | Origen de H-001/H-004/H-005, score 2.85/5.00 🔴 |
| Phase 1 — Requirements | `specs/agent-copiloto-diagnostico/requirements.md` | REQ-1..5, Gherkin, NFRs, out-of-scope |
| Phase 2 — Design | `specs/agent-copiloto-diagnostico/design.md` | Arquitectura, seguridad, trade-offs, pre-mortem 10 dimensiones |
| Phase 3 — Task Envelopes | `specs/agent-copiloto-diagnostico/tasks.md` | 8 envelopes (T1-T8) autocontenidos |

### Skills asignadas a OpenCode

- [ ] `backend-developer` — logging, memoria de tool previa, wiring de routers (T1-T7)
- [ ] `test-automation-engineer` — eval set de precisión (T8)

---

## 1. Objetivo

### 1.1 Qué se construye

Tres mejoras acotadas al copiloto IA (`app/agent/`), tomadas de la auditoría
`docs/auditorias/2026-09-21-agent-copiloto.md`: **(1)** logging estructurado del plan
que el LLM elige en cada turno de chat (hoy solo hay una línea de log en todo el
módulo), **(2)** paso del contexto de la tool/args del turno anterior de la misma
conversación al planner, para que preguntas de seguimiento ("¿y por ubicación?") se
puedan anclar correctamente, y **(3)** un eval set de 24 preguntas que corre contra el
LLM real (no mockeado) y reporta el % de aciertos de tool/tipo de widget/argumentos
clave elegidos.

### 1.2 Por qué

El usuario reportó que el copiloto "en ocasiones da resultados que no pide y lo que
pide no lo da como lo pide". La auditoría encontró que hoy es imposible diagnosticar
ese síntoma después de que ocurre (cero logging del plan elegido), que las preguntas
de seguimiento pierden todo contexto de qué se mostró antes, y que no existe ninguna
forma de medir objetivamente si un cambio al prompt (`app/agent/prompts.py`) mejora o
empeora la precisión — bloqueando cualquier decisión informada sobre el hallazgo H-003
(reescribir el prompt heurístico / routing en dos pasos), que es el cambio de fondo
que el usuario quiere evaluar a continuación.

### 1.3 Valor esperado

- La próxima vez que el usuario reporte "me dio algo que no pedí", hay un log con el
  mensaje exacto, la tool elegida, los args y los reintentos — diagnóstico en minutos,
  no reproducción a ciegas
- Preguntas de seguimiento como "¿y por ubicación?" o "¿y el mes pasado?" tienen más
  probabilidad de anclarse a la tool correcta
- Existe una vara de medir (`pytest tests/eval -m agent_eval -v -s`) para comparar
  objetivamente cualquier cambio futuro al prompt o al modelo, antes de decidir sobre
  H-003

### 1.4 Qué NO se toca

- El mecanismo de retry-with-feedback existente (`_plan_node`, graph.py:92-150) — se
  le agregan logs en los puntos de retorno ya existentes, su lógica de control no cambia
- `PlanModel`/`WidgetSpec` (schemas.py) — sin campos nuevos
- El texto de `SYSTEM_PROMPT_TEMPLATE` (las ~30 reglas heurísticas de H-003) — el
  contexto de `last_tool` se agrega como párrafo aparte, no se reescriben las reglas
- `_execute_node`/`_build_node`/`_narrate_node` — sin cambios
- Ningún endpoint nuevo ni cambio de contrato en `POST /ui/agent/chat`
- Ningún archivo de `app/routers/` fuera de `ui_agent.py`
- Ninguna migración de base de datos — cero cambios de schema

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|----------------|
| D1 | Memoria de tool previa vía heurístico `history vacío → sin last_tool` + último `DashboardWidget` del negocio | FK explícita `ChatMessage.widget_id` con migración | Evita migración + tocar `chat_store.py`/templates para un fix que se evalúa junto a H-003 (pendiente de decisión); revisitar si el heurístico resulta insuficiente con datos reales | No es un vínculo exacto conversación↔widget: si hubiera dos administradores chateando simultáneamente sobre el mismo negocio, podrían cruzarse el `last_tool` — riesgo aceptado y documentado, caso de uso actual es un solo operador por negocio |
| D2 | Logs con formato `%s` plano (`_log_agent.info("msg %s", arg)`) | `extra={...}` estructurado | `app/logger.py::_JsonFormatter` solo serializa `record.getMessage()`, no lee `extra` — usarlo sería silenciosamente ignorado; cambiar el formatter es un cambio más amplio fuera de alcance | Los logs son greppables por texto (`agent_plan`, `agent_plan_retry`, etc.) pero no queryables como JSON estructurado por campo — suficiente para el diagnóstico manual que se necesita ahora |
| D3 | Eval set inline en un único archivo de test, sin dataset en módulo separado | Dataset importado vía `tests.eval.golden_tool_selection` | El proyecto no tiene `tests/__init__.py` (pytest rootless import mode) — un import de paquete cruzado es frágil en ese modo; mismo patrón que el resto de `tests/`, cada archivo autocontenido | El archivo de test es más largo (~200 líneas), aceptable para una herramienta de eval que se edita raramente |
| D4 | Eval reporta % de aciertos, solo falla ante fallos de conectividad al LLM | Assert de precisión mínima (ej. `>= 80%`) | El propósito es comparar antes/después al decidir H-003, no bloquear CI con un umbral arbitrario | El eval nunca "falla" por baja precisión — hay que leer el reporte impreso/JSON manualmente para juzgar si mejoró o empeoró |
| D5 | Marker `agent_eval` nuevo, excluido por default (mismo patrón que `e2e`) | Reusar el marker `e2e` existente | La descripción de `e2e` en pytest.ini dice explícitamente "live server + Playwright browser" — el eval no es eso, mezclar los dos confundiría el propósito de cada marker | Un comando nuevo (`pytest tests/eval -m agent_eval`) que hay que recordar; documentado en el docstring del propio archivo de test |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
POST /ui/agent/chat (ui_agent.py::chat)
  │
  ├─ chat_store.list_messages()          · sin cambios
  ├─ ✦ widget_store.latest_widget_meta()  ★ NUEVO — solo si prior (history) no vacío
  │
  └─ runner.run_agent(business_id, message, history, ✦ last_tool)
        │
        └─ graph.invoke({..., ✦ last_tool})
              │
              └─ _plan_node (graph.py)
                    ├─ ✦ build_system_prompt(✦ last_tool=...)   [prompts.py]
                    ├─ _PLANNER.invoke(messages)                 · sin cambios
                    └─ ★ _log_agent.info/warning/error(...)      ★ NUEVO — 5 puntos

tests/eval/test_agent_tool_selection_eval.py  ★ NUEVO
  └─ build_graph().invoke(...) contra el LLM real, 24 casos, marker agent_eval
```

### 3.2 Inventario de archivos

> **Scope duro:** Solo estos archivos. Ninguno más.
> **Verificación:** `git diff --name-only develop` debe mostrar exactamente estos archivos.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/agent/prompts.py` | MODIFY | `build_system_prompt(last_tool=None)` — párrafo extra de contexto |
| `app/agent/widgets.py` | MODIFY | `latest_widget_meta(db, business_id)` — última tool usada por el negocio |
| `app/agent/graph.py` | MODIFY | 5 puntos de logging en `_plan_node` + `AgentState.last_tool` + consumo |
| `app/agent/runner.py` | MODIFY | `run_agent(..., last_tool=None)` |
| `app/routers/ui_agent.py` | MODIFY | `chat()` obtiene y pasa `last_tool` condicionado a `prior` |
| `pytest.ini` | MODIFY | Marker `agent_eval` + exclusión en `addopts` |
| `.gitignore` | MODIFY | `tests/eval/results_latest.json` |
| `tests/unit/test_agent_prompts.py` | CREATE | Tests de `build_system_prompt(last_tool=...)` |
| `tests/unit/test_agent_widgets.py` | MODIFY | Tests de `latest_widget_meta` |
| `tests/unit/test_agent_graph.py` | MODIFY | Tests de logging (caplog) + consumo de `last_tool` |
| `tests/unit/test_agent_runner.py` | MODIFY | Test de propagación de `last_tool` |
| `tests/unit/test_ui_agent.py` | MODIFY | Test de wiring condicional en `chat()` |
| `tests/eval/test_agent_tool_selection_eval.py` | CREATE | Eval set de 24 casos contra el LLM real |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/agent/schemas.py — YA EXISTE, NO MODIFICAR
class WidgetSpec(BaseModel):
    type: Literal["kpi", "line", "bar", "pie", "doughnut", "radar", "table"]
    title: str
    tool: str
    tool_args: dict
    unit: Optional[str] = None

class PlanModel(BaseModel):
    tool: Optional[str] = None
    tool_args: dict
    widgets: list[WidgetSpec]
    reply: str

# app/agent/graph.py — YA EXISTE, NO MODIFICAR (salvo lo descrito en T3/T4)
def build_graph():  # compila el StateGraph: planner -> execute -> build -> narrate
    ...

# app/models.py — YA EXISTE, NO MODIFICAR
class DashboardWidget(Base):
    business_id: Mapped[Optional[int]]
    type: Mapped[str]           # "kpi" | "line" | "bar" | ... (mismos valores que WidgetSpec.type)
    params: Mapped[Optional[str]]  # JSON: {"tool": str, "tool_args": dict, "payload": dict, "unit": ...}
    position: Mapped[int]

# app/logger.py — YA EXISTE, NO MODIFICAR
def get_logger(name: str) -> logging.Logger: ...
# _log_agent = get_logger(__name__) ya existe en graph.py:22
```

### 3.4 Interfaces nuevas a implementar

Ver `specs/agent-copiloto-diagnostico/tasks.md` T1-T8 — cada envelope contiene la
firma exacta y el before/after completo. Resumen de firmas nuevas:

```python
# app/agent/prompts.py
def build_system_prompt(last_tool: Optional[dict] = None) -> str: ...

# app/agent/widgets.py
def latest_widget_meta(db: Session, business_id: int) -> Optional[dict]: ...
# retorna {"tool": str, "tool_args": dict, "type": str} o None

# app/agent/runner.py
async def run_agent(
    business_id: int, message: str, history: Optional[list[dict]] = None,
    last_tool: Optional[dict] = None,
) -> dict: ...
```

---

## 4. Fases de implementación (Task Envelopes)

> Ver `specs/agent-copiloto-diagnostico/tasks.md` para los 8 envelopes completos
> (T1-T8), cada uno autocontenido con Context/Objective/Instructions/Verification/
> Expected Report. OpenCode ejecuta un envelope a la vez, en este orden:

**Batch 1 (independientes entre sí):** T1 (prompts.py), T2 (widgets.py), T7 (pytest.ini)
**Batch 2:** T3 (graph.py — logging, depende de nada nuevo, pero se secuencia después
del batch 1 para mantener sesiones de 3-5 envelopes)
**Batch 3:** T4 (graph.py — last_tool, depende de T1+T3)
**Batch 4:** T5 (runner.py, depende de T4), T6 (ui_agent.py, depende de T2+T5)
**Batch 5:** T8 (eval set, depende de T7)

Compactar contexto de OpenCode entre batches (`/compact`), per §"Sesiones cortas" de
`opencode-optimization`.

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO)

```bash
# 1. Todos los tests existentes deben pasar ANTES de empezar
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v

# 2. Schema drift: N/A — este PAE no modifica app/models.py, el proyecto no tiene
#    tests/test_schema_drift.py (no aplica al stack de este repo)

# 3. Seed: N/A — no hay script de seed standalone; el seed corre inline en
#    app/migrations.py::run_migrations() al arrancar la app (ver 5.1 "App arranca")
```

Si el paso 1 falla, **NO empezar la implementación**. Reportar y esperar instrucciones.

### 5.1 Verificación por task envelope

Cada envelope de `tasks.md` (T1-T8) incluye su propio bloque `#### Verification` con
comandos `pytest` específicos del archivo tocado. Protocolo si falla: ver
`Templates/template-pae.md` §5.2 (leer error completo → verificar diff → corregir
código, no el test → re-ejecutar → si persiste tras 3 intentos, reportar sin avanzar).

### 5.2 Tests existentes que deben seguir pasando

- [ ] `tests/unit/test_agent_graph.py` — 12 tests existentes, sin cambios de comportamiento
- [ ] `tests/unit/test_agent_runner.py` — 2 tests existentes
- [ ] `tests/unit/test_agent_widgets.py` — 2 tests existentes
- [ ] `tests/unit/test_ui_agent.py` — todos los existentes (empty message 422, rate limit 429, tab_agent)
- [ ] `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` — 100% verde antes y después

### 5.3 Tests e2e

**No aplica.** Ver "e2e tool decision" al inicio de `tasks.md`: ninguno de los 3
hallazgos tiene un criterio de aceptación de UI/flujo de usuario nuevo (logging
interno, contexto invisible del LLM, eval que no pasa por el navegador). El contrato
de `POST /ui/agent/chat` no cambia. La suite e2e existente
(`tests/e2e/test_flows_e2e.py` y demás) debe seguir en verde sin tocarla:

```bash
python3 -m pytest tests/e2e -m e2e --no-cov -v
```

### 5.4 Eval de tool-selection (no es parte del gate de CI)

```bash
# Requiere AGENT_LLM_BASE_URL alcanzable (dev: llama-swap en :8080)
pytest tests/eval -m agent_eval -v -s
```

No forma parte de la checklist pre-commit (§7.1) — es una herramienta de comparación
a demanda, tal como se documenta en D4 y en el docstring del propio test.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | El heurístico de D1 cruza `last_tool` entre dos conversaciones simultáneas del mismo negocio (dos admins chateando a la vez) | Baja | Bajo | El copiloto "recuerda" una tool que no corresponde a la pregunta de seguimiento del usuario actual | Documentado como limitación conocida (D1); si se observa en la práctica, migrar a FK `ChatMessage.widget_id` (fuera de alcance de este PAE) | Revertir T2/T4/T5/T6 — `last_tool` vuelve a no pasarse nunca |
| R2 | Los logs nuevos (REQ-1) aumentan el volumen de log en producción de forma no trivial | Baja | Bajo | Ruido en agregadores de log si el copiloto se usa muy intensivamente | Un log por turno de chat, ya acotado por `AGENT_RATE_LIMIT`; nivel `INFO` para el caso exitoso (filtrable), `WARNING`/`ERROR` solo en casos anómalos | Bajar el nivel del log exitoso a `DEBUG` si resulta ruidoso — cambio de una línea |
| R3 | El eval set (T8) da falsos "FALLO" en casos donde el LLM elige un tool/type razonable pero distinto al esperado (ambigüedad real, no bug) | Media | Bajo | El % de aciertos reportado subestima la calidad real del copiloto | Es la naturaleza de un eval con juicio binario sobre un problema con más de una respuesta razonable — el propio hallazgo H-003 es sobre esa ambigüedad; el reporte detallado (`details`) permite revisar caso por caso, no solo el agregado | N/A — es una limitación aceptada del enfoque, no un bug a revertir |
| R4 | El LLM local (dev) no está corriendo cuando alguien ejecuta el eval | Alta (si no se arranca antes) | Bajo | El test falla con "N/24 casos fallaron por conectividad" | Mensaje de error explícito con la URL configurada (`AGENT_LLM_BASE_URL`); no bloquea el run normal de CI (marker excluido) | N/A — no requiere rollback, solo arrancar el LLM local y reintentar |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (OBLIGATORIO — ejecutar en orden)

- [ ] **Pre-check:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` — 100% verde ANTES de empezar (guardar output)
- [ ] **Schema drift:** N/A (sin cambios de modelos ORM en este PAE)
- [ ] **Tests:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` — 100% verde DESPUÉS de los cambios
- [ ] **e2e:** `python3 -m pytest tests/e2e -m e2e --no-cov -v` — verde, sin cambios respecto al estado previo
- [ ] **Seed:** N/A (sin script de seed standalone en este proyecto)
- [ ] **App:** `uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload` — arranca sin errores
- [ ] **Invariantes:** ninguna de las invariantes de `.claude/CLAUDE.md` (docker-compose, `.env`, `app/auth.py`, `app/db.py`, `app/deps.py`) se toca
- [ ] **Scope:** `git diff --name-only develop` muestra exactamente los 13 archivos de §3.2, nada más
- [ ] **Limpieza:** sin archivos `_tmp`/`_test`/`_draft`/`_vN` sueltos
- [ ] **Versión:** `VERSION` bumpeado a `0.4.9` (PATCH) — regla dura del proyecto

### 7.2 Script de verificación completa

```bash
#!/bin/bash
# specs/agent-copiloto-diagnostico/verify.sh
set -e
echo "=== Pre-check: tests before changes ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v 2>&1 | tee /tmp/pre-tests.log
echo "=== Post-check: all unit tests ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
echo "=== Post-check: e2e ==="
python3 -m pytest tests/e2e -m e2e --no-cov -v
echo "=== Scope check ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
echo "(Opcional, no bloqueante) Eval de tool-selection: pytest tests/eval -m agent_eval -v -s"
```

### 7.3 Commits esperados

```
feat(agent): log structured plan decisions in _plan_node (H-001)
feat(agent): thread last-tool context into follow-up planner prompts (H-005)
test(agent): add real-LLM tool-selection accuracy eval (H-004)
chore: bump VERSION to 0.4.9
```

---

## 8. Riesgos y mitigaciones de seguridad (resumen de design.md §2.5)

Sin nueva superficie de ataque, sin nuevo endpoint, sin dato sensible nuevo expuesto.
`latest_widget_meta` scoped por `business_id` igual que el resto de `app/agent/widgets.py`.
Detalle completo en `specs/agent-copiloto-diagnostico/design.md` §2.5.

---

## 9. Fuera de alcance (recordatorio)

H-002 (reply desacoplado de datos), H-003 (routing en dos pasos / few-shot — **decisión
pendiente, a tomar con los datos que produzca T8**), H-006 (validación semántica
post-plan), H-007 (SQL directo en 3 tools), H-008 (transacción única). Ver
`requirements.md` §4 para el detalle de cada exclusión.

---

**PAE complete at `specs/agent-copiloto-diagnostico/PAE-agent-copiloto-diagnostico.md`.**
**8 task envelopes ready for OpenCode + Qwen local (GLM fallback).**
**Awaiting approval before any implementation begins.**
