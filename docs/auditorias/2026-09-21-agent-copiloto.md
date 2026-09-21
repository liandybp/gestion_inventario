# Auditoría de Ingeniería — Copiloto IA (dashboard dinámico, `app/agent/`)

**Fecha:** 2026-09-21
**Proyecto:** gestion_inventario
**Rama:** develop
**Score:** 2.85 / 5.00 🔴
**Responsable:** Claude Code (auditoria-ingenieria)

---

## 1. Contexto y método

**Alcance:** submódulo `app/agent/` (copiloto IA / dashboard dinámico) + integración en
`app/routers/ui_agent.py`. NO incluye `app/services/coach_service.py` ni el resto del
dashboard (fuera de alcance explícito del usuario).

**Archivos auditados:** `graph.py`, `prompts.py`, `tools.py`, `llm.py`, `config.py`,
`runner.py`, `schemas.py`, `widgets.py`, `chat_store.py`, `circuit_breaker.py` (10
archivos), más `routers/ui_agent.py` y los 6 archivos de test en
`tests/unit/test_agent_*.py` / `test_ui_agent.py`.

**Motivo de la auditoría:** el usuario reporta que el copiloto "en ocasiones da
resultados que no pide y lo que pide no lo da como lo pide". Se pidió foco especial en
4 hipótesis ya identificadas en una revisión previa informal: (1) ambigüedad del prompt
heurístico de mapeo texto→tool, (2) `reply` desacoplado de los datos reales, (3) sin
memoria de qué tool se usó en turnos anteriores, (4) sin capa de validación semántica
post-plan.

**Criterios:** 10 dimensiones estándar de la skill `auditoria-ingenieria`. Dado que el
síntoma reportado es específico de calidad de decisión del agente (no de seguridad ni
escalabilidad), las dimensiones 5 (Robustez), 7 (Observabilidad) y 9 (Pruebas) se
evaluaron con especial atención a si permiten diagnosticar y prevenir ese síntoma
concreto, no solo a si el código "no crashea".

**Limitaciones:** no se ejecutó el LLM real (ni el local Qwen3-8B ni la API de prod)
durante esta auditoría — es un análisis estático del código y los tests existentes, no
un eval empírico de precisión de tool-selection. Esa carencia es en sí misma uno de los
hallazgos (H-004).

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ⚠️ | 3 | `tools.py` mezcla tools que delegan en la capa de servicio con tools que ejecutan SQL directo (H-007) |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 3 | Imports de `sqlalchemy`/`app.models` repetidos dentro de funciones individuales en `tools.py` en vez de a nivel de módulo |
| 3 | Capas / Límites | 10% | ⚠️ | 3 | Mismo hallazgo que dim 1: 3 tools saltan la capa de servicio (`total_purchases`, `purchases_by_product`, `sales_by_location`) |
| 4 | Seguridad | 15% | ✅ | 4 | RBAC (`ensure_admin_or_owner`) en las 6 rutas de `ui_agent.py`; rate limit in-memory; `_resolve_location_id` nunca autocrea (evita ubicaciones fantasma por args alucinados) |
| 5 | Robustez / Resiliencia | 15% | ❌ | 2 | Circuit breaker + retry con backoff son sólidos para fallos *estructurales*, pero **no existe ningún mecanismo para fallos *semánticos*** (tool/fecha/args equivocados) — exactamente el síntoma reportado (H-002, H-006) |
| 6 | Consistencia transaccional | 10% | ⚠️ | 3 | Commits separados para cada widget y para el mensaje assistant en `ui_agent.py:116-125` (H-008) |
| 7 | Observabilidad | 10% | ❌ | 1 | Una sola línea de log en todo `app/agent/` (circuit breaker OPEN); el plan elegido por el LLM en cada turno nunca se registra (H-001) |
| 8 | Configuración / Entornos | 5% | ✅ | 5 | Todas las `AGENT_*` centralizadas en `settings.py`, sin hardcodes, paridad dev/prod documentada |
| 9 | Pruebas / Calidad | 10% | ❌ | 2 | 6 archivos de test cubren bien el grafo *determinístico* (retries, indexado, degradación) con planner mockeado, pero **cero tests ejercitan el LLM real contra `prompts.py`** (H-004) |
| 10 | Escalabilidad | 5% | ✅ | 4 | Semáforo de concurrencia, trimming de widgets/mensajes por límite, sesión DB propia por hilo |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3       0.30
2. Cohesión / Acoplamiento       10%     3       0.30
3. Capas / Límites               10%     3       0.30
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     2       0.30
6. Consistencia transaccional    10%     3       0.30
7. Observabilidad                10%     1       0.10
8. Configuración / Entornos       5%     5       0.25
9. Pruebas / Calidad             10%     2       0.20
10. Escalabilidad                 5%     4       0.20
────────────────────────────────────────────────────────────
TOTAL                          100%            28.5 / 100
SCORE FINAL                                      2.85 / 5.00 🔴
```

**Interpretación:** 🔴 — no es un módulo mal construido (la seguridad, el manejo de
fallos duros y la config están bien resueltos), pero está en rojo específicamente en las
tres dimensiones que determinan si el agente **acierta lo que le piden y si alguien
puede diagnosticarlo cuando falla**. Eso coincide con el síntoma reportado por el
usuario, no es una casualidad del peso ponderado.

---

## 4. Hallazgos

### 🔴 Críticos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-001 | Cero observabilidad del plan elegido por turno | 7. Observabilidad | `app/agent/graph.py` — única línea de log en todo el módulo es la línea 80 (circuit breaker OPEN). `_plan_node` (64-151) nunca loguea `plan.tool`, `plan.tool_args`, `plan.widgets` ni el número de reintentos | Cada vez que el usuario reporta "me dio algo que no pedí" no hay ningún rastro para reconstruir qué mensaje llegó, qué plan generó el LLM real (dev/prod) ni cuántos reintentos hizo — hay que reproducir el bug a ciegas o pedirle al usuario que repita la pregunta exacta | Loguear (info/debug) mensaje del usuario, plan final (tool/tool_args/widgets), intentos usados y errores de formato descartados, con un correlation_id por request. Bajo esfuerzo, alto impacto — es el primer paso antes de tocar el prompt |

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-002 | `reply` se redacta antes de ejecutar la tool, nunca se reconcilia con los datos reales | 5. Robustez | `graph.py:64-151` (`_plan_node` genera `plan.reply` en la misma llamada que decide `tool`/`tool_args`) + `graph.py:204-206` (`_narrate_node` copia `plan.reply` tal cual, sin ver `tool_results`) | El texto que lee el usuario puede describir datos distintos a los que el widget termina mostrando (p. ej. decir "acá tienes julio" mientras el widget trae agosto por una fecha relativa mal resuelta) — el usuario percibe esto como "no me dio lo que pedí" aunque la tool haya ejecutado bien | Mover la narración a después de `execute` (segundo LLM call grounded en `tool_results`, o al menos una plantilla determinística que resuma el payload real en vez de un reply libre generado a ciegas) |
| H-003 | Prompt heurístico de ~30 reglas de prosa para 33 tools, sin few-shot, con tools semánticamente cercanas | 1/9 | `prompts.py:12-74` — reglas como `sales_by_location` vs `profit_by_location`, o `sales_by_product` vs `profit_by_product` vs `profit_by_category`, dependen de que el modelo interprete matices de prosa. Modelo de dev es Qwen3-8B (chico) | Es la causa más probable del síntoma "no me da lo que pido": confusión entre tools cercanas. Ningún test ejercita esto contra el LLM real (ver H-004), así que cualquier ambigüedad nueva pasa desapercibida hasta que el usuario la reporta en producción | Evaluar las dos vías ya discutidas con el usuario: (a) routing en dos pasos — clasificar la tool con un enum corto primero, rellenar `tool_args` en una segunda llamada acotada al schema de esa tool; (b) reemplazar reglas de prosa por pares few-shot query→tool/args concretos, que generalizan mejor en modelos chicos que las instrucciones abstractas |
| H-004 | Sin eval set de precisión de tool-selection contra el LLM real | 9. Pruebas | `tests/unit/test_agent_graph.py` — los 12 tests usan `_FakePlanner`/`_CapturePlanner`/`_FlakyPlanner` (líneas 6-11, 101-107, 155-164): prueban el grafo determinístico (reintentos, indexado de resultados, degradación), nunca el comportamiento real del modelo contra `prompts.py` | Se puede reescribir `prompts.py` (o cambiar de Qwen3-8B a otro modelo) sin ninguna señal automática de si la precisión mejoró o empeoró — exactamente el tipo de cambio que el usuario quiere hacer ahora. El "mejoramos el prompt" queda a puro ojo/reporte manual del usuario | Crear un eval set (20-40 pares pregunta→tool/args esperado, priorizando los casos ambiguos de H-003) corrido contra el LLM real como test separado (marcado, tipo `@pytest.mark.e2e`, no en el run normal por costo/latencia pero ejecutable a demanda al tocar el prompt o el modelo) |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-005 | Sin memoria de qué tool/widget se usó en turnos anteriores | 3 (diseño agente) | `chat_store.py` (`ChatMessage` solo persiste `role`+`content` de texto plano) + `graph.py:71-78` y `ui_agent.py:113-114` reconstruyen el historial solo como texto. `widgets.py` (`DashboardWidget`) persiste widgets aparte, sin vínculo al mensaje que los generó | Preguntas de seguimiento ("¿y por ubicación?", "¿y el mes pasado?") fuerzan al modelo a reinterpretar desde cero sin saber qué tool/args usó el turno anterior, aumentando la chance de elegir una tool distinta a la esperada por continuidad conversacional | Incluir en el historial que recibe el planner un resumen de la última tool+args+tipo de widget (no solo el texto), para que el modelo pueda anclar preguntas de seguimiento al contexto correcto |
| H-006 | Sin capa de validación semántica post-plan | 5. Robustez | `graph.py:92-150` — el único retry-with-feedback es para errores *estructurales* (JSON inválido, `widgets` vacío). Nada confronta `tool_args` (fechas, location, limit) contra la intención del mensaje antes de ejecutar | Si el usuario pide "ventas de este mes" y el modelo resuelve `start`/`end` del mes pasado, nada lo detecta antes de ejecutar y narrar — llega directo al usuario como si fuera correcto | Ya existe el mecanismo de retry-with-feedback (probado y funcionando para errores de formato) — extenderlo con una verificación ligera (heurística de rango de fechas razonable, o una segunda pasada LLM barata) antes de comprometerse al plan final |
| H-007 | `tools.py` mezcla acceso a datos vía servicio y SQL directo | 1/3 | `tools.py:156-174` (`total_purchases`), `177-212` (`purchases_by_product`), `215-251` (`sales_by_location`) ejecutan SQLAlchemy crudo dentro del `@tool`, mientras el resto (`sales_by_product`, `profit_by_product`, etc.) delega en `ReportingService`/`FinanceService` | Lógica de negocio duplicada/divergente entre capas — un cambio en la regla de "qué cuenta como venta" hay que recordar aplicarlo en `ReportingService` Y en estas 3 tools sueltas | Mover esas 3 tools a métodos de `ReportingService`, consistente con el resto del archivo |

### 🟢 Bajos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|---------------|
| H-008 | Commits separados para widget y mensaje assistant | 6. Transaccional | `ui_agent.py:116-125` — `widget_store.add_widget` (commit interno por widget) y `chat_store.add_message` (commit propio) no comparten transacción | Si el proceso muere entre medio, puede quedar un widget sin su mensaje de chat correspondiente (o viceversa) — bajo impacto porque es una feature de conveniencia, no dato crítico | No bloqueante; envolver en una única transacción si se observa en la práctica |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS           🎯 ESTRATÉGICOS
             │  H-001 (logging)         H-002 (reply grounded post-tool)
             │  H-005 (tool en historial) H-003 (routing 2 pasos / few-shot)
             │                          H-004 (eval set LLM real)
             │                          H-006 (validación semántica)
Bajo impacto │  📋 BAJA PRIORIDAD       ❌ EVITAR
             │  H-008 (transacción)     (ninguno)
             │  H-007 (mover SQL a servicio)
             └──────────────────────────────────────────────
               Bajo esfuerzo              Alto esfuerzo
```

**Quick wins recomendados (hacer YA):**
1. **H-001** — logging del plan por turno: ~1-2h, y es prerequisito práctico de todo lo
   demás (sin esto, no hay forma de confirmar si H-003/H-006 realmente mejoraron algo).
2. **H-005** — pasar tool+args del último turno al contexto del planner: ~2-3h, cambio
   acotado a `graph.py`/`ui_agent.py`, no toca el prompt todavía.

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 Logging del plan por turno | Crítico | Bajo | Alto | 🔴 Inmediata |
| 2 | H-004 Eval set contra LLM real | Alto | Medio | Alto | 🟠 Este sprint (habilita medir H-003) |
| 3 | H-003 Routing en dos pasos / few-shot | Alto | Alto | Alto | 🟠 Este sprint (con H-004 como red de seguridad) |
| 4 | H-002 Reply grounded post-tool | Alto | Medio | Alto | 🟠 Este sprint |
| 5 | H-005 Memoria de tool previa en historial | Medio | Bajo | Medio | 🟡 Próximo ciclo |
| 6 | H-006 Validación semántica post-plan | Medio | Alto | Medio | 🟡 Próximo ciclo |
| 7 | H-007 Mover SQL directo a ReportingService | Medio | Medio | Bajo | 🟡 Próximo ciclo |
| 8 | H-008 Transacción única widget+mensaje | Bajo | Bajo | Bajo | 🟢 Backlog |

---

## 7. Plan 30/60/90 días

### 30 días (corto plazo)
- [ ] H-001: logging estructurado del plan/tool/args/reintentos por turno → visibilidad
      real la próxima vez que el usuario reporte el síntoma
- [ ] H-005: incluir tool+args del turno anterior en el contexto del planner
- [ ] H-004 (arranque): construir el eval set de 20-40 preguntas con tool/args esperado,
      priorizando los pares ambiguos identificados en H-003
**Criterio de cierre:** logging en producción confirmando qué plan elige el LLM en casos
reportados por el usuario; eval set corrible a demanda.

### 60 días (medio plazo)
- [ ] H-003: implementar y comparar (vía el eval set de H-004) routing en dos pasos vs.
      prompt actual — decisión basada en datos, no en intuición
- [ ] H-002: mover la narración del reply a después de `execute`, grounded en
      `tool_results`
**Criterio de cierre:** eval set muestra mejora medible de precisión de tool-selection;
score de re-auditoría >= 3.5.

### 90 días (largo plazo)
- [ ] H-006: capa de validación semántica ligera post-plan (fechas/location coherentes)
- [ ] H-007: consolidar las 3 tools con SQL directo en `ReportingService`
**Criterio de cierre:** score de re-auditoría >= 4.0; cero regresiones en el eval set
tras cambios de prompt.

---

## 8. Criterios de cierre

- [ ] H-001 y H-005 resueltos (quick wins)
- [ ] Eval set (H-004) existe y corre contra el LLM real de dev
- [ ] Decisión tomada y documentada sobre H-003 (routing en dos pasos vs. few-shot vs.
      ambos), con datos del eval set respaldándola
- [ ] Score de re-auditoría >= 3.5

---

*Informe generado por Claude Code (skill: auditoria-ingenieria).*
*Re-auditoría recomendada: tras implementar H-001/H-004/H-003, o en 60 días.*
