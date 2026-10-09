# Auditoría de Ingeniería — Copiloto IA: verificación post-implementación (PAE agent-copiloto-diagnostico)

**Fecha:** 2026-09-21
**Proyecto:** gestion_inventario
**Rama:** develop (sin commitear)
**Score:** 3.90 / 5.00 🟡
**Responsable:** Claude Code (auditoria-ingenieria)

---

## 1. Contexto y método

**Alcance:** el diff recién implementado del PAE `specs/agent-copiloto-diagnostico/` —
**no** el módulo `app/agent/` completo (ya auditado el 2026-09-21 en
`docs/auditorias/2026-09-21-agent-copiloto.md`, score 2.85/5.00 🔴). Esta es una
auditoría de **verificación**: ¿la implementación de H-001 (logging), H-005 (memoria
de tool previa) y H-004 (eval set) cumple lo que `design.md` prometió, especialmente
el pre-mortem de §2.9 (dimensiones 1, 2, 3, 5, 6)?

**Archivos auditados:** `app/agent/graph.py`, `app/agent/prompts.py`,
`app/agent/runner.py`, `app/agent/widgets.py`, `app/routers/ui_agent.py`,
`pytest.ini`, `tests/unit/test_agent_{graph,prompts,runner,widgets}.py`,
`tests/unit/test_ui_agent.py`, `tests/eval/test_agent_tool_selection_eval.py`.

**Insumo previo:** `/code-review` (medium, 8 ángulos + verify de 1 voto) ya corrió
sobre este mismo diff y reportó **5 hallazgos CONFIRMADOS**. Esta auditoría los
integra en el marco de 10 dimensiones (no los repite en detalle) y agrega lo que
`/code-review` no evalúa: si la cobertura de tests da **confianza real** sobre el
hallazgo más grave, y si el diseño cumplió su propio pre-mortem.

**Limitaciones:** alcance deliberadamente angosto (un diff de 13 archivos, no el
sistema completo) — no se repiten las 10 dimensiones a la profundidad de la
auditoría base, solo las que este diff mueve la aguja.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia |
|---|-----------|------|-----------|-------|-----------|
| 1 | SoC / SRP | 10% | ✅ | 4 | `latest_widget_meta` tiene un único consumidor real (`ui_agent.py::chat`), tal como exigía el pre-mortem §2.9 dim.1 — verificado, no quedó código muerto |
| 2 | Cohesión / Acoplamiento | 10% | ✅ | 4 | `last_tool` se calcula una sola vez por request (`if prior else None`), sin recomputo — pre-mortem dim.2 cumplido |
| 3 | Capas / Límites | 10% | ✅ | 4 | Dirección de dependencias sin cambios: router → agent → graph → prompts, sin saltos nuevos |
| 4 | Seguridad | 15% | ✅ | 4 | Sin endpoints nuevos, sin cambio de RBAC; el hallazgo de fuga de contexto (ver H-001 abajo) es de negocio/UX, no de aislamiento multi-tenant (queda scoped a `business_id`, nunca cruza negocios) |
| 5 | Robustez / Resiliencia | 15% | ⚠️ | 3 | El pre-mortem §2.9 dim.5 exigía "mismo patrón en el 100% de los métodos" — `latest_widget_meta` sí lo sigue (defensivo, igual que `widgets_with_payload`), **pero** la guarda de `last_tool` en `ui_agent.py:117` no cubre el caso que el propio REQ-3 dijo resolver más allá del primer turno (H-001, CONFIRMED por `/code-review`) |
| 6 | Consistencia transaccional | 10% | ✅ | 4 | Sin operaciones multi-tabla nuevas; pre-mortem dim.6 no aplicaba y se confirma que no se introdujo ninguna |
| 7 | Observabilidad | 10% | ✅ | 5 | H-001 entregado y **verificado en vivo**: corrí el eval contra el LLM real y los 5 puntos de log (`agent_plan`, `agent_plan_retry`, etc.) aparecieron con datos correctos — pasa de 1 línea de log en todo el módulo a diagnóstico turno-por-turno real |
| 8 | Configuración / Entornos | 5% | ✅ | 5 | Marker `agent_eval` registrado sin warnings, `VERSION`/`.bumpversion.cfg` bumpeados a PATCH (0.4.9) correctamente, sin hardcodes nuevos |
| 9 | Pruebas / Calidad | 10% | ⚠️ | 3 | Cantidad buena (8 tests nuevos, todos verdes), pero **calidad con gaps reales**: (a) el test de regresión de REQ-3 no ejercita el escenario que rompió H-001 — falsa confianza (hallazgo nuevo de esta auditoría, ver H-002); (b) el eval set duplica lógica existente y tiene un hueco de detección (H-003/H-004, ya CONFIRMED por `/code-review`) |
| 10 | Escalabilidad | 5% | ✅ | 4 | El SELECT extra en `chat()` es indexado y `LIMIT 1`, impacto despreciable (ya evaluado así en `design.md` NFR); la recompilación del grafo en el eval es costo de test, no de producción |

**Leyenda:** ✅ OK (4-5) · ⚠️ PARCIAL (2-3) · ❌ NO (<2)

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     4       0.40
2. Cohesión / Acoplamiento       10%     4       0.40
3. Capas / Límites               10%     4       0.40
4. Seguridad                     15%     4       0.60
5. Robustez / Resiliencia        15%     3       0.45
6. Consistencia transaccional    10%     4       0.40
7. Observabilidad                10%     5       0.50
8. Configuración / Entornos       5%     5       0.25
9. Pruebas / Calidad             10%     3       0.30
10. Escalabilidad                 5%     4       0.20
────────────────────────────────────────────────────────────
TOTAL                          100%            39.0 / 100
SCORE FINAL                                      3.90 / 5.00 🟡
```

**Interpretación:** 🟡 — mejora clara sobre la base (2.85 🔴 del módulo completo),
consistente con haber resuelto H-001 casi ejemplarmente y H-004 funcionalmente. Pero
**no llega a verde** porque H-005 (memoria de tool previa) quedó con una brecha real
entre lo prometido en `requirements.md` y lo entregado, y esa brecha además **no está
cubierta por ningún test** — ambas cosas en la misma dimensión (Robustez/Pruebas),
que es exactamente donde el pre-mortem de `design.md` puso el foco.

---

## 4. Hallazgos

### 🟠 Altos

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|----------------|
| H-001 | `last_tool` filtra contexto de conversaciones ya cerradas más allá del primer turno | 5. Robustez | `app/routers/ui_agent.py:117` — `last_tool = widget_store.latest_widget_meta(...) if prior else None` solo previene la fuga en el turno inmediatamente posterior a "nueva conversación"; cualquier turno siguiente con `prior` no vacío (aunque esta conversación nunca haya producido un widget propio) vuelve a traer el widget más reciente del negocio, sin importar de qué conversación viene. **CONFIRMED por `/code-review`** con secuencia de repro completa (ver reporte de code-review). | El copiloto puede anclar una pregunta de seguimiento ("¿y por ubicación?") a la tool/args de una conversación distinta y ya cerrada, produciendo exactamente el síntoma original que el usuario reportó ("me da algo que no pedí") — la feature pensada para arreglar ese síntoma puede, en este caso puntual, reproducirlo. | Cambiar el guard para que dependa de si **esta conversación** ya produjo un widget, no de si tiene turnos previos. Opción de bajo esfuerzo: en vez de `if prior`, verificar si algún turno de `prior` tiene un widget asociado — pero eso requiere el vínculo que D1 (design.md) explícitamente pospuso. Alternativa más simple y ya disponible: comparar el `created_at` del último `DashboardWidget` contra el `created_at` del primer mensaje de `prior` (si el widget es más viejo que el primer turno de esta conversación, es de otra conversación → no pasarlo) |
| H-002 | El test de regresión de REQ-3 no cubre el escenario que rompió H-001 | 9. Pruebas | `tests/unit/test_ui_agent.py::test_chat_passes_last_tool_only_when_history_exists` — solo prueba: (a) primer mensaje sin historial → `last_tool=None` (correcto y trivial), (b) segundo mensaje **después de que la propia prueba agregó un widget manualmente** → `last_tool` presente. Nunca prueba el caso real que rompe: un segundo turno cuyo `prior` es no-vacío pero donde NINGÚN turno de la conversación actual generó un widget (el escenario exacto de H-001). | Este test pasa hoy y seguirá pasando incluso si H-001 nunca se arregla — da falsa sensación de cobertura sobre precisamente el requisito (REQ-3) que más le importa al usuario. Cualquier refactor futuro que "verifique tests en verde" no detectará esta regresión porque nunca estuvo cubierta. | Agregar un caso de test que reproduzca exactamente la secuencia de H-001: negocio con un widget preexistente (de "otra conversación", simulado insertándolo antes de cualquier mensaje de chat) + primer turno Q&A sin tool + segundo turno → assert de que `last_tool` es `None` (hoy fallaría, correctamente, hasta que se corrija H-001) |
| H-003 | Eval set (H-004) con 3 gaps de calidad ya confirmados por `/code-review` | 9. Pruebas | `tests/eval/test_agent_tool_selection_eval.py`: (a) línea ~162, el contador de fallos de conectividad no detecta `"LLM en cooldown"` (circuit breaker abierto), solo `"LLM no disponible"` — un breaker abierto a mitad de run puede pasar el assert mientras reporta accuracy artificialmente baja; (b) línea ~82, `_this_month_bounds` duplica `app/utils.py::month_range` ya usado en `reporting_service.py`; (c) línea ~100, `_resolve_tool_args` duplica literal el fallback de `_execute_node` en `graph.py`. Los 3 con verdict **CONFIRMED**, detalle completo en el reporte de `/code-review` de esta sesión. | El eval —pensado como la "vara de medir" objetiva para decidir sobre H-003 (routing en dos pasos)— puede dar lecturas engañosas si el LLM se degrada a mitad de run, y arrastra divergencia silenciosa si la lógica de fechas o de resolución de args cambia en el código de producción sin que el eval se entere. | Importar `month_range`/`_execute_node`'s fallback en vez de reimplementarlos; ampliar el filtro de conectividad a `("LLM no disponible" in err) or ("LLM en cooldown" in err)`. Esfuerzo bajo (~20 min), ya especificado con precisión en el reporte de code-review |

### 🟡 Medios

| ID | Título | Dimensión | Evidencia | Riesgo | Recomendación |
|----|--------|-----------|-----------|--------|----------------|
| H-004 | Eval recompila el grafo 24 veces | 10. Escalabilidad (impacto solo en tiempo de test) | `tests/eval/test_agent_tool_selection_eval.py:90` — `build_graph()` dentro de `_run_case`, llamado una vez por caso, en vez de una sola vez fuera del loop (mismo patrón que `runner.py::_GRAPH` ya documenta) | Alarga el eval sin necesidad (grafo es stateless y reutilizable); impacto real bajo porque el cuello de botella es la latencia del LLM, no la compilación del grafo | Mover `graph = build_graph()` fuera de `_run_case`, a nivel de módulo o al inicio del test |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS           🎯 ESTRATÉGICOS
             │  H-003 (eval: fechas,     H-001 (fix real del guard
             │  args, conectividad)      last_tool — requiere decidir
             │  H-002 (test que cubra    cómo vincular widget↔turno
             │  el escenario real)       sin la FK que D1 pospuso)
Bajo impacto │  📋 BAJA PRIORIDAD       ❌ EVITAR
             │  H-004 (build_graph 24x)  (ninguno)
             └──────────────────────────────────────────────
               Bajo esfuerzo              Alto esfuerzo
```

**Quick wins recomendados (hacer YA, antes de commitear):**
1. **H-003** — 3 fixes puntuales en el eval (imports + ampliar filtro), ~20-30 min
2. **H-002** — agregar el caso de test que hoy falta, ~15 min (fallará hasta que H-001 se arregle, lo cual es el punto)
3. **H-004** — mover `build_graph()` fuera del loop, ~2 min

**Antes de decidir qué hacer con H-001:** no es un quick win — arreglarlo bien
requiere decidir si el heurístico "primer turno tras reset" se reemplaza por algo
más robusto (comparar timestamps, como se sugiere arriba) o si se acepta el riesgo
documentado y se deja para cuando el heurístico "resulte insuficiente en la
práctica" (tal como D1 en `design.md` ya anticipaba, aunque subestimó el alcance del
caso).

---

## 6. Backlog priorizado de remediación

| # | Hallazgo | Severidad | Esfuerzo | Impacto | Prioridad |
|---|----------|-----------|----------|---------|-----------|
| 1 | H-001 last_tool filtra conversaciones cerradas | Alto | Medio | Alto | 🟠 Antes de commitear (afecta directamente el síntoma que motivó todo este trabajo) |
| 2 | H-002 test sin cobertura del escenario real | Alto | Bajo | Alto | 🟠 Antes de commitear (junto con H-001 — el test debe fallar primero, luego el fix lo pone en verde) |
| 3 | H-003 gaps de calidad del eval | Alto | Bajo | Medio | 🟠 Antes de commitear |
| 4 | H-004 build_graph 24x | Medio | Bajo | Bajo | 🟡 Opcional, no bloqueante |

---

## 7. Plan 30/60/90 días

### Ahora (antes de commit)
- [ ] H-002: escribir el test que reproduce el escenario real de H-001 (debe fallar)
- [ ] H-001: corregir el guard de `last_tool` para que el test de H-002 pase
- [ ] H-003: los 3 fixes puntuales del eval
- [ ] H-004: mover `build_graph()` fuera del loop
**Criterio de cierre:** re-correr `/code-review` sobre el diff corregido — 0 hallazgos CONFIRMED pendientes; re-correr el eval (`pytest tests/eval -m agent_eval -v -s`) y confirmar que sigue en 96%+ de tool accuracy.

### 30 días
- [ ] Decidir sobre H-003 de la auditoría original (routing en dos pasos / few-shot) usando el eval como vara de medir — ya hay una primera lectura real (96% tool, 88% args) que sirve de baseline
**Criterio de cierre:** decisión documentada + si se implementa, comparación before/after con el mismo eval set

---

## 8. Criterios de cierre

- [ ] H-001 y H-002 resueltos (el test nuevo pasa)
- [ ] H-003 y H-004 resueltos
- [ ] Suite completa (`pytest -v` + `pytest tests/e2e -m e2e --no-cov -v`) en verde
- [ ] Re-auditoría opcional tras los fixes: objetivo ≥4.2 (verde)

---

*Informe generado por Claude Code (skill: auditoria-ingenieria).*
*Complementa el reporte de `/code-review` (medium, 5 hallazgos CONFIRMED) de la misma sesión — no lo duplica, lo integra en el marco de 10 dimensiones y aporta H-002 como hallazgo adicional.*
