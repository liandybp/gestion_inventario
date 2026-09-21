# Requirements — Diagnóstico y memoria del copiloto IA (H-001, H-005, H-004)

**Fuente:** `docs/auditorias/2026-09-21-agent-copiloto.md` (auditoría de `app/agent/`,
score 2.85/5.00 🔴). El usuario aprobó abordar 3 de los 8 hallazgos como paquete inicial
— los quick wins de observabilidad/memoria más el habilitador de medición — antes de
decidir sobre H-003 (reescritura del prompt / routing en dos pasos), que depende de los
datos que este mismo trabajo va a producir.

---

## 1. Functional Requirements

### REQ-1: El planner registra el plan elegido en cada turno
**Priority:** Must Have
**Scenario:** El LLM produce un plan válido en el primer intento
  GIVEN el copiloto recibe un mensaje de un usuario
  WHEN el planner obtiene un `PlanModel` válido (con o sin tool)
  THEN se emite un log `INFO` con el mensaje del usuario, la tool elegida, los
    `tool_args`, la cantidad de widgets y el número de intento en que se logró

**Scenario:** El LLM devuelve un JSON inválido y se reintenta
  GIVEN el planner recibe una respuesta que no cumple `PlanModel`
  WHEN el nodo reintenta con el mensaje de error como feedback
  THEN se emite un log `WARNING` con el número de intento y el error descartado

**Scenario:** El LLM falla de forma dura (red/timeout/auth)
  GIVEN el circuit breaker está CLOSED o HALF-OPEN
  WHEN la llamada al LLM lanza una excepción no relacionada con formato
  THEN se emite un log `ERROR` con la excepción (`exc_info=True`) antes de degradar

**Scenario:** Se agotan los reintentos sin `widgets`
  GIVEN el modelo insiste en dejar `widgets` vacío tras `MAX_RETRIES` intentos
  WHEN se sintetiza el widget de rescate con la tool que sí eligió
  THEN se emite un log `WARNING` indicando que el widget fue sintetizado

### REQ-2: El correlation_id existente cubre los logs nuevos sin cableado adicional
**Priority:** Must Have
**Scenario:** Un log del planner incluye el request_id de la petición HTTP
  GIVEN una petición POST /ui/agent/chat con su `X-Request-ID` / correlation_id ya
    seteado por el middleware existente (`app/logger.py::set_request_id`)
  WHEN `runner.run_agent` ejecuta el grafo dentro de `asyncio.to_thread`
  THEN los logs emitidos por REQ-1 dentro del hilo llevan el mismo `request_id`
    (verificar que `asyncio.to_thread` propaga `contextvars`, no requiere código nuevo)

### REQ-3: El planner recibe la tool/args del turno anterior en la misma conversación
**Priority:** Must Have
**Scenario:** Pregunta de seguimiento con conversación previa
  GIVEN el negocio activo tiene al menos un widget persistido Y la conversación
    actual tiene al menos un turno previo (`history` no vacío)
  WHEN el usuario envía un nuevo mensaje
  THEN el system prompt que recibe el LLM incluye la tool, `tool_args` y `type` del
    último widget persistido del negocio, como contexto adicional

**Scenario:** Primer mensaje de una conversación nueva (o tras "nueva conversación")
  GIVEN la conversación actual no tiene turnos previos (`history` vacío, ya sea por
    ser la primera pregunta o por haber usado "nueva conversación")
  WHEN el usuario envía un mensaje
  THEN el system prompt NO incluye contexto de tool anterior, aunque el negocio
    tenga widgets de una conversación previa (evita anclar a contexto obsoleto)

**Scenario:** Negocio sin ningún widget todavía
  GIVEN el negocio activo no tiene ningún `DashboardWidget` persistido
  WHEN se solicita el contexto de la última tool
  THEN se obtiene `None` sin error, y el system prompt se genera igual que hoy

### REQ-4: Existe un eval set de precisión de tool-selection contra el LLM real
**Priority:** Must Have
**Scenario:** Se ejecuta el eval con el LLM local disponible
  GIVEN el endpoint `AGENT_LLM_BASE_URL` configurado está disponible
  WHEN se corre `pytest tests/eval -m agent_eval -v -s`
  THEN se ejecutan los 24 casos del set dorado contra `build_graph()` real (planner
    NO mockeado), se imprime el detalle por caso y el % de aciertos agregado de
    tool elegida, tipo de widget y argumentos clave, y se persiste un reporte JSON

**Scenario:** El eval no corre en el run normal de CI/unit
  GIVEN el comando estándar del proyecto (`pytest -v` o el de CI)
  WHEN se ejecuta sin flags adicionales
  THEN los casos marcados `agent_eval` se excluyen (mismo patrón que `e2e`)

**Scenario:** El LLM configurado no es alcanzable
  GIVEN `AGENT_LLM_BASE_URL` apunta a un endpoint caído
  WHEN se corre el eval
  THEN el test falla con una aserción clara de fallos de conectividad (no se queda
    colgado indefinidamente — cada llamada respeta `AGENT_LLM_TIMEOUT_S`)

### REQ-5: Cobertura de test para los cambios de REQ-1 y REQ-3
**Priority:** Must Have
**Scenario:** Regresión de logging
  GIVEN el planner produce un plan válido con un `_FakePlanner` en test
  WHEN se invoca `_plan_node`
  THEN un test con `caplog` confirma que se emitió un registro `agent_plan` con la
    tool esperada

**Scenario:** Regresión de memoria de tool previa
  GIVEN `build_system_prompt(last_tool={...})` se invoca con un dict de ejemplo
  WHEN se compara contra `build_system_prompt()` sin argumento
  THEN el prompt con `last_tool` contiene la tool/args/type indicados y el prompt
    sin `last_tool` no contiene ese bloque

---

## 2. MoSCoW Prioritization

| REQ | Prioridad |
|-----|-----------|
| REQ-1 (logging del plan) | Must Have |
| REQ-2 (correlation_id gratis) | Must Have (verificación, no código nuevo) |
| REQ-3 (memoria de tool previa) | Must Have |
| REQ-4 (eval set) | Must Have |
| REQ-5 (tests de regresión) | Must Have |

No hay Should/Could/Won't en este PAE — es un paquete de 3 hallazgos ya acotado por
la auditoría, sin margen de negociación de alcance dentro de él.

---

## 3. Non-Functional Requirements (NFRs)

| Category | Question | Answer |
|----------|----------|--------|
| **Performance** | ¿Impacto en latencia del chat? | Despreciable — logging es I/O asíncrono del logger estándar; la consulta de `latest_widget_meta` es un SELECT indexado (`business_id` ya tiene índice) con `LIMIT 1`, solo cuando `history` no está vacío |
| **Security** | ¿Nuevo límite de auth o dato sensible? | No — los logs nuevos exponen `tool`/`tool_args`/mensaje del usuario (ya visibles en la UI del propio chat), ningún dato de otro negocio ni credenciales |
| **Scalability** | ¿Volumen esperado? | Un log por turno de chat (ya limitado por `AGENT_RATE_LIMIT`); sin cambio de escalabilidad |
| **Observability** | ¿Nuevos logs/métricas? | Sí — es el objetivo central de REQ-1/REQ-2 |
| **Reliability** | ¿Comportamiento ante fallo? | `latest_widget_meta` debe degradar a `None` ante JSON corrupto (mismo patrón que `widgets_with_payload` existente), nunca lanzar excepción que rompa el chat |

---

## 4. Out of Scope

- **H-002** (reply narrado antes de ejecutar la tool, desacoplado de los datos reales) — PAE futuro
- **H-003** (routing en dos pasos / few-shot para reemplazar el prompt heurístico) — depende de
  los datos que produzca el eval set de REQ-4; se decide en una sesión posterior
- **H-006** (capa de validación semántica post-plan) — PAE futuro
- **H-007** (mover `total_purchases`/`purchases_by_product`/`sales_by_location` de SQL directo
  a `ReportingService`) — PAE futuro, sin relación con el síntoma reportado por el usuario
- **H-008** (transacción única widget+mensaje) — bajo impacto, backlog
- Vincular `ChatMessage` y `DashboardWidget` por FK explícita en el schema — REQ-3 usa el
  heurístico "hay historial → hay tool anterior en esta conversación" sin migración de BD;
  una relación explícita queda para si el heurístico demuestra ser insuficiente en la práctica
- Ampliar el eval set más allá de los 24 casos iniciales, o automatizarlo en CI — es una
  herramienta de comparación a demanda, no un gate de despliegue

---

## 5. Dependencies

- LLM local (Qwen3-8B vía llama-swap, `AGENT_LLM_BASE_URL` default `http://localhost:8080/v1`)
  debe estar corriendo en dev para que REQ-4 produzca resultados — no es una dependencia de
  build/deploy, solo de la ejecución manual del eval
- Ninguna migración de BD (confirmado en el diseño — ver `design.md` §2.3)
- No depende de ningún otro PAE en curso

---

## 6. Open Questions

Ninguna — el alcance quedó cerrado por la auditoría (3 hallazgos concretos, con evidencia
archivo:línea ya identificada) y por la conversación previa con el usuario. Procediendo a
Fase 2.
