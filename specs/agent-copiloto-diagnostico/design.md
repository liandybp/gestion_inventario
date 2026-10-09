# Design — Diagnóstico y memoria del copiloto IA

## 2.1 Codebase Grounding

| Archivo | Rol |
|---------|-----|
| `app/agent/graph.py` | MODIFY — `_plan_node` gana 5 puntos de logging (REQ-1) + consume `last_tool` (REQ-3); `AgentState` gana el campo `last_tool` |
| `app/agent/prompts.py` | MODIFY — `build_system_prompt()` gana parámetro opcional `last_tool` |
| `app/agent/widgets.py` | MODIFY — nueva función `latest_widget_meta(db, business_id)` |
| `app/agent/runner.py` | MODIFY — `run_agent()` gana parámetro opcional `last_tool`, lo mete en el state |
| `app/routers/ui_agent.py` | MODIFY — `chat()` obtiene `latest_widget_meta` cuando hay historial previo y lo pasa a `run_agent` |
| `app/logger.py` | REFERENCE — `get_logger(name)` y el `_JsonFormatter` (mensaje como string único vía `record.getMessage()`, sin `extra=` estructurado) ya están en uso en `graph.py:22` (`_log_agent = get_logger(__name__)`). Los logs nuevos siguen el mismo patrón `%s`-style que el resto del repo (ej. `app/migrations.py:616`), no `extra=` |
| `app/models.py` | REFERENCE — `ChatMessage` (líneas 363-379, solo `role`/`content`, sin campo de tool) y `DashboardWidget` (líneas 342-361, `params` es `Text` con JSON `{tool, tool_args, payload, unit}`) — **sin cambios de schema** |
| `pytest.ini` | MODIFY — nuevo marker `agent_eval`, excluido del run por defecto (mismo patrón que `e2e`) |
| `tests/unit/test_agent_graph.py` | MODIFY — nuevos tests para REQ-1/REQ-3 (logging + `last_tool` en `_plan_node`) |
| `tests/unit/test_agent_widgets.py` | MODIFY — nuevo test para `latest_widget_meta` |
| `tests/unit/test_agent_runner.py` | MODIFY — nuevo test para el paso de `last_tool` |
| `tests/unit/test_ui_agent.py` | MODIFY — nuevo test para el wiring en `chat()` |
| `tests/eval/test_agent_tool_selection_eval.py` | CREATE — eval set de 24 casos + lógica de reporte (REQ-4) |
| `tests/conftest.py` | REFERENCE — fixtures `db_session`/`business_id` (SQLite en memoria) ya visibles para `tests/eval/` sin cambios (no hay `tests/__init__.py`, pytest usa rootless import mode; por eso el dataset del eval va INLINE en el mismo archivo de test, no en un módulo separado importado vía `tests.eval.xxx`) |

## 2.2 Architecture Approach

Tres cambios independientes que comparten el mismo submódulo (`app/agent/`):

1. **Observabilidad (REQ-1/REQ-2):** `_plan_node` ya es el único punto de decisión del
   agente (graph.py:64-151). Se le agregan 5 líneas de log (`_log_agent`, ya importado)
   en los puntos de retorno existentes — éxito, retry por formato, fallo duro, widget
   sintetizado, plan no interpretable. No se crea infraestructura nueva: `app/logger.py`
   ya centraliza formato JSON + `request_id` vía `contextvars`. `asyncio.to_thread`
   (usado en `runner.py:42`) copia el contexto activo al hilo del executor por diseño
   de la librería estándar (`contextvars.copy_context()` + `ctx.run(...)`), así que el
   `request_id` seteado por el middleware de la request HTTP llega intacto a los logs
   del hilo sin ningún cambio de código — REQ-2 es una verificación, no una tarea.

2. **Memoria de tool previa (REQ-3):** en vez de modificar el schema de `ChatMessage`
   (que forzaría migración + tocar `chat_store.py` + el render de la burbuja de chat,
   que muestra `content` tal cual), se reutiliza `DashboardWidget.params` — ya contiene
   `tool`/`tool_args` en JSON (`widgets.py:24`). Se agrega `latest_widget_meta()` que
   trae el widget más reciente del negocio. El router decide SI pasarlo al planner
   usando una señal ya disponible: `history` (los turnos previos de la conversación
   actual, ya reconstruidos en `ui_agent.py:113`). Si `history` está vacío — primera
   pregunta de una conversación nueva, o justo después de "nueva conversación"
   (`clear_conversation()` borra `chat_messages` pero NO `dashboard_widgets`, ver
   `ui_agent.py:69-76`) — no se pasa `last_tool`, evitando anclar a un widget de una
   conversación distinta o ya cerrada. Es un heurístico a nivel de negocio, no un
   vínculo real conversación↔widget (eso requeriría FK nueva, fuera de alcance — ver
   `requirements.md` §4).

3. **Eval set (REQ-4):** un test de `pytest`, marcado `agent_eval` (excluido del run
   normal, igual que `e2e`), que invoca `build_graph()` REAL (sin mockear el planner,
   a diferencia de los 12 tests existentes en `test_agent_graph.py`) contra 24
   preguntas fijas cubriendo los grupos ambiguos de H-003. Corre contra
   `AGENT_LLM_BASE_URL` tal como está configurado (dev: Qwen3-8B local). No es un gate
   de CI — es una herramienta de comparación a demanda para cuando se decida sobre
   H-003. Reporta % de aciertos (tool / tipo de widget / argumentos clave) y persiste
   un JSON, en vez de un simple assert binario, tal como se acordó con el usuario.

**Invariantes preservados:**
- `PlanModel`/`WidgetSpec` (schemas.py) sin cambios — ningún campo nuevo en el JSON que
  el LLM produce (`last_tool` viaja por fuera, en el system prompt como texto).
- El mecanismo de retry-with-feedback existente (graph.py:92-150) no se toca en su
  lógica, solo se le agregan logs en los puntos de retorno ya existentes.
- `_execute_node`/`_build_node`/`_narrate_node` sin cambios.
- Ningún archivo de `app/routers/` fuera de `ui_agent.py` se toca.

## 2.3 Data Model

**Sin cambios de schema.** No se agregan columnas ni tablas. `latest_widget_meta` lee
`DashboardWidget` (existente) parseando su columna `params` (ya JSON con `tool`/
`tool_args`), igual que ya hace `widgets_with_payload` (widgets.py:43-53) para `payload`.

**Migración:** ninguna.
**Rollback:** ninguno — revertir el código es suficiente.
**Índice needed:** No — `DashboardWidget.business_id` ya tiene `index=True` (models.py:347)
y la query usa `ORDER BY position DESC, id DESC LIMIT 1`, cubierta por el índice existente
más el filtro de igualdad.

## 2.4 API Contracts

No se agregan ni cambian endpoints. `POST /ui/agent/chat` (`ui_agent.py:79-130`) mantiene
su contrato — mismo request (`message: str = Form(...)`), misma respuesta HTML parcial.
El único cambio interno es qué argumentos recibe `runner.run_agent()` (función Python,
no endpoint).

## 2.5 Security Review

| Check | Answer |
|-------|--------|
| **New auth boundary?** | No — `chat()` ya está detrás de `ensure_admin_or_owner` (ui_agent.py:81); `latest_widget_meta` no es un endpoint, es una función interna llamada con el `business_id` ya resuelto por `require_active_business_id` |
| **New sensitive data?** | No — los logs nuevos exponen mensaje del usuario + tool/args, mismos datos que ya se persisten en `chat_messages`/`dashboard_widgets` sin cifrar; no hay PII nueva expuesta |
| **Input validation?** | `latest_widget_meta` recibe `business_id` ya validado por la capa de router (mismo patrón que el resto de `widgets.py`), y filtra por `business_id` explícitamente (sin riesgo de fuga cross-negocio, igual que `list_widgets`) |
| **New attack surface?** | Ninguna — no hay endpoint nuevo, no hay input de usuario nuevo (el eval set es un test, no llega a producción) |
| **Secrets needed?** | Ninguno |
| **OWASP risks?** | N/A — cambio de observabilidad + lectura read-only ya scopeada por `business_id`, mismo patrón de aislamiento multi-tenant que el resto de `app/agent/` |

## 2.6 Trade-offs

| Decisión | Elegido | Alternativa | Por qué no la alternativa |
|----------|---------|-------------|---------------------------|
| Memoria de tool previa | Heurístico basado en `history` vacío/no-vacío + último `DashboardWidget` del negocio | FK explícita `ChatMessage.widget_id` con migración | Migración + tocar `chat_store.py`/templates para un fix que se está evaluando junto al resto del prompt (H-003 pendiente); si el heurístico resulta insuficiente en la práctica, se revisita con datos reales del eval, no a priori |
| Formato de logs | `_log_agent.info/warning/error("msg %s", args)` (mensaje plano, mismo patrón que el resto del repo) | `extra={...}` con campos estructurados | `app/logger.py::_JsonFormatter` (línea 25-35) solo serializa `record.getMessage()`, no lee `extra` — usar `extra` sería silenciosamente ignorado por el formatter actual; cambiar el formatter es un cambio más amplio, fuera de alcance de este PAE |
| Ubicación del eval set | Inline en un único archivo `tests/eval/test_agent_tool_selection_eval.py` | Dataset en módulo separado importado (`tests.eval.golden_tool_selection`) | El proyecto no tiene `tests/__init__.py` (pytest rootless import mode); un import de paquete cruzado (`from tests.eval.x import Y`) es frágil en ese modo. Mismo patrón que el resto de `tests/`: cada archivo es autocontenido, sin imports cruzados entre archivos de test |
| Assertion del eval | Reporta % de aciertos (no gatea), solo falla ante fallos de **conectividad** al LLM | Assert de precisión mínima (ej. `>= 80%`) | El propósito explícito (acordado con el usuario) es comparar antes/después al decidir H-003, no bloquear CI con un umbral arbitrario sobre un modelo que puede variar por reintentos/temperatura=0 pero aun así no es 100% determinístico entre versiones de prompt |
| Cuándo pasar `last_tool` | Solo si `history` (turnos previos de la conversación actual) no está vacío | Siempre el último widget del negocio, sin importar la conversación | `clear_conversation()` borra mensajes pero no widgets (ui_agent.py:69-76) — pasar siempre el último widget filtraría contexto de una conversación ya cerrada por el usuario como "nueva" |

## 2.7 Migration & Rollback Strategy

```
Migración: ninguna (sin cambios de schema).
Rollback: revertir los 6 archivos de código + 4 archivos de test + pytest.ini a su estado
previo. Sin pasos de datos.
```

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría (obligatorio)

| Dimensión | Respuesta del diseño |
|-----------|----------------------|
| **1. SoC/SRP** | `latest_widget_meta` vive en `widgets.py` (módulo de acceso a datos de `DashboardWidget` ya existente) — mismo lugar que `widgets_with_payload`/`list_widgets`. Tiene un consumidor real y único: `ui_agent.py::chat()` (T7). No se crea ninguna capa "por si acaso" |
| **2. Cohesión/Acoplamiento** | `latest_widget_meta` se llama **una sola vez** por request de chat, condicionado a `if prior:` — no hay cómputo duplicado ni N+1 |
| **3. Capas/Límites** | Dirección de dependencias sin cambios: `ui_agent.py` (router) → `widgets.py`/`runner.py` (agent layer) → `graph.py` (orquestación) → `prompts.py` (construcción de prompt). Ningún salto de capa nuevo |
| **5. Robustez/Resiliencia** | `latest_widget_meta` sigue el MISMO patrón defensivo que `widgets_with_payload` (widgets.py:43-53): `json.loads` envuelto en `try/except (TypeError, ValueError)`, devuelve `None` ante `params` corrupto — nunca lanza. Los 5 logs nuevos en `_plan_node` no introducen ningún `try/except` nuevo porque las llamadas a `_log_agent.*` no pueden fallar por datos arbitrarios (logging estándar, formato lazy `%s`) — consistente con el resto del repo, donde ninguna llamada a `_log.*` está envuelta en su propio try/except |
| **6. Consistencia Transaccional** | No aplica — este PAE no combina cifras en una fórmula/ratio. `latest_widget_meta` es una lectura simple sin agregación |

Sin riesgos detectados que requieran una instrucción adicional en los envelopes más allá
de lo ya especificado arriba (el patrón defensivo de REQ-3 ya está explícito en la tabla
de dimensión 5, y se traslada literal al envelope T2).

## 2.10 Optional Delegation

Ninguna — alcance acotado, sin necesidad de `spec-writer`/`software-architect`/
`api-designer`/`data-engineer`/`security-architect`. El propio SDD Lead cubre las 3
dimensiones (observabilidad, memoria conversacional, testing) en este documento.
