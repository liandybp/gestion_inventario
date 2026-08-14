# Auditoría de Ingeniería — Gestión de Inventario + Copiloto completo

**Fecha:** 2026-08-14
**Proyecto:** gestion_inventario
**Rama:** develop (`e731cfb`)
**Score:** 3.13 / 5.00 🔴 (Alto riesgo — con fixes pendientes del code-review)
**Responsable:** Claude Code (auditoria-ingenieria)

> **Nota:** esta auditoría refleja el estado PRE-fixes. El `/code-review` detectó
> 2 críticos (C1: planner json_schema rompe prod en OpenAI; B2: FK de borrado de
> usuario) que afectan Robustez y Transaccional. Los fixes están en el backlog y
> se aplicarán de inmediato; el score subirá al aplicarlos.

---

## 1. Contexto y método

**Alcance:** proyecto completo (v0.4.0) + copiloto IA terminado (burbuja, historial,
diagramas, tema claro/oscuro, Qwen3-8B en dev).

**Archivos auditados:** ~46 archivos en `app/` (agent, routers, templates, models,
services) + tests.

**Criterios:** 10 dimensiones. **Limitaciones:** no se auditaron los templates de las
tablas (fuera del copiloto) a fondo; el frontend JS se evaluó por inspección.

---

## 2. Checklist por dimensión

| # | Dimensión | Peso | Veredicto | Score | Evidencia clave |
|---|-----------|------|-----------|-------|-----------------|
| 1 | SoC / SRP | 10% | ⚠️ | 3.0 | `inventory_service.py` 3069 ln (god class); `ui_tabs.py` 2437 ln; `graph.py` crece (_Planner + nodos + fallbacks) |
| 2 | Cohesión / Acoplamiento | 10% | ⚠️ | 3.0 | Duplicados: rate limiter (D5), `chat_store`/`widgets` CRUD (D3), `_iso`/`_parse_end` vs `parse_dt` (D1/D2), `_currency_symbol` ×3 (D4) |
| 3 | Capas / Límites | 10% | ⚠️ | 3.5 | repos→services→routers correcto; pero `ui_agent.py` hace persistencia+config+history inline |
| 4 | Seguridad | 15% | ✅ | 4.0 | CSRF, PBKDF2, roles, `business_id` forzado, rate limit (duplicado pero presente), API key en env |
| 5 | Robustez / Resiliencia | 15% | ❌ | 2.5 | **C1** prod 400 (OpenAI schema); **B1** errores 429/422 no se muestran; **A1** fecha congelada; **A6** chart roto por `\n`; degradaciones silenciosas |
| 6 | Consistencia transaccional | 10% | ❌ | 2.5 | **B2** FK de borrado de usuario (chat_messages/dashboard_widgets); **G4** commits por widget sin atomicidad; **B5** ghost messages sin persistir |
| 7 | Observabilidad | 10% | ⚠️ | 3.0 | logging plano, sin correlation_id, agente no loguea errores de tool |
| 8 | Configuración / Entornos | 5% | ⚠️ | 3.0 | **B4** prod sin `AGENT_LLM_*` cae a dev local; `env()` vs `os.getenv` disperso (D6); **H3** doc vs código (modelo) |
| 9 | Pruebas / Calidad | 10% | ⚠️ | 3.5 | 88 tests, 86% cov, CI verde; pero los críticos (C1/B2/B1/A2) **no tienen test** que los cace |
| 10 | Escalabilidad | 5% | ⚠️ | 3.0 | pooling + threadpool ✓; `_trim` O(n) (F1/E5), re-parseo, `position` vía len (F3) |

---

## 3. Scorecard ponderado

```
Dimensión                        Peso    Score   Peso×Score
────────────────────────────────────────────────────────────
1. SoC / SRP                     10%     3.0     0.30
2. Cohesión / Acoplamiento       10%     3.0     0.30
3. Capas / Límites               10%     3.5     0.35
4. Seguridad                     15%     4.0     0.60
5. Robustez / Resiliencia        15%     2.5     0.38
6. Consistencia transaccional    10%     2.5     0.25
7. Observabilidad                10%     3.0     0.30
8. Configuración / Entornos       5%     3.0     0.15
9. Pruebas / Calidad             10%     3.5     0.35
10. Escalabilidad                 5%     3.0     0.15
────────────────────────────────────────────────────────────
TOTAL                          100%             3.13 / 5.00
```

**Interpretación:** 🔴 **3.13** — Alto riesgo, acción inmediata. Bajó de 3.45 (auditoría
intermedia) por los críticos del copiloto en Robustez/Transaccional. **Los fixes del
code-review subirán el score.**

---

## 4. Hallazgos

### 🔴 Críticos
| ID | Título | Dim | Evidencia | Riesgo | Recomendación |
|----|--------|-----|-----------|--------|---------------|
| C1 | Planner json_schema rompe prod (OpenAI 400) | 5 | `graph.py` schema sin `additionalProperties:false`/`required` completo | Todo chat en prod degrada a "no disponible" | Añadir `additionalProperties:false` + todas las props en `required` al schema, o usar `with_structured_output` con un schema compatible |
| B2 | FK de borrado de usuario | 6 | `ui_users.py` no limpia `chat_messages.user_id`/`dashboard_widgets.created_by` | Borrar usuario que usó el copiloto → 500 | Anular esas FKs antes del delete (como `bf2b315` hizo con audit_logs) |

### 🟠 Altos
| ID | Título | Dim | Evidencia | Riesgo | Recomendación |
|----|--------|-----|-----------|--------|---------------|
| A2 | `tool_results` colisionan por tool | 5 | `graph.py:132,153` | Widgets con misma tool/args distintos muestran datos del último | Keyear por índice de widget, no por tool |
| A1 | Fecha del prompt congelada | 5 | `graph.py:21` `_SYSTEM_PROMPT` en import | "Mes pasado" mal tras días | `build_system_prompt()` por request |
| B1 | Errores 429/422 no se muestran | 5 | htmx no swapea ≥400 | Sin feedback de rate-limit | Insertar `xhr.responseText` en afterRequest si status ≥400 |

### 🟡 Medios
| ID | Título | Dim | Evidencia | Riesgo | Recomendación |
|----|--------|-----|-----------|--------|---------------|
| B4 | Prod sin `AGENT_LLM_*` cae a dev | 8 | `docker-compose ${VAR:-}` → default local | Copiloto degradado en prod silenciosamente | Fail-fast o default de prod explícito |
| A6 | Título con `\n` rompe chart JS | 9 | `widget_chart.html` `label: "{{ w.title }}"` | Gráfico nunca se dibuja | Escapar el título o pasar por `tojson` |
| B5 | Ghost bubble en 429/422 | 6 | mensaje no persistido | Historial diverge del contexto LLM | Persistir el mensaje antes de validar rate-limit, o avisar |
| C4 | Fallback `tool_args` inyecta args ajenos | 5 | `graph.py:118` | KeyError en widget con otra tool | Solo usar `plan.tool_args` si `w.tool == plan.tool` |
| B6 | FAB visible al inicio | 2 | `dashboard.html` | Parpadea sobre Inicio | `display:none` por defecto, mostrar solo en dashboard |

---

## 5. Matriz impacto vs esfuerzo

```
Alto impacto │  🔥 QUICK WINS                 🎯 ESTRATÉGICOS
             │  C1 schema OpenAI             H-001 dividir inventory_service
             │  B2 FK delete                 structlog + correlation
             │  A2 tool collision            circuit breaker LLM
             │  A1 fecha prompt
             │  B1 error swap htmx
             │
Bajo impacto │  📋 BAJA PRIORIDAD            ❌ EVITAR
             │  B5/B6 ghost/FAB              Redis rate-limit (hasta
             │  C4 tool_args                 multi-worker)
             │  A6 newline chart
             └────────────────────────────────────────────────
               Bajo esfuerzo                  Alto esfuerzo
```

**Quick wins (hacer YA, ~3h):** C1, B2, A2, A1, B1, C4, B6, A6 — todos de bajo
esfuerzo y alto impacto (los críticos).

---

## 6. Backlog priorizado

| # | ID | Acción | Prioridad | Esfuerzo |
|---|----|--------|-----------|----------|
| 1 | C1 | Schema OpenAI-compatible (additionalProperties:false + required) | Crítica | Bajo |
| 2 | B2 | Limpiar FKs chat_messages/dashboard_widgets en user_delete | Crítica | Bajo |
| 3 | A2 | tool_results keyeados por índice de widget | Alta | Bajo |
| 4 | A1 | build_system_prompt() por request (fecha fresca) | Alta | Bajo |
| 5 | B1 | Mostrar errores 4xx en el chat (afterRequest insert) | Alta | Bajo |
| 6 | C4 | tool_args fallback solo si tool == plan.tool | Media | Bajo |
| 7 | B6 | FAB display:none por defecto | Media | Bajo |
| 8 | A6 | Escapar título del chart (tojson) | Media | Bajo |
| 9 | B4 | Fail-fast o default prod para AGENT_LLM_* | Media | Medio |
| 10 | B5 | Persistir mensaje user antes de validar | Media | Bajo |
| 11 | E1-F6 | Limpiezas de eficiencia/duplicación (tool_result, trims, currency cache, etc.) | Baja | Medio |
| 12 | H3 | Corregir doc (modelo qwen3_8b_instruct en CLAUDE.md) | Baja | Bajo |
| 13 | G6 | Tema sin reload (redraw charts) | Baja | Medio |
| 14 | H-001 | Dividir inventory_service/ui_tabs | Baja | Alto |

---

## 7. Plan 30/60/90

### Días 1-7 — Críticos + altos (aplicar YA, como pidió el usuario)
- [ ] C1 schema OpenAI + B2 FK delete + A2 tool collision + A1 fecha + B1 error swap
- **Criterio:** ninguno de los críticos/altos del code-review abierto; suite verde

### Días 8-30 — Medios + limpieza
- [ ] C4, B6, A6, B4, B5
- [ ] E1-E6, F1-F6 (tool_result, trims O(n), currency cache, Chart destroy, dedupe)
- [ ] H3 doc
- **Criterio:** score ≥ 3.5; sin degradaciones silenciosas en el copiloto

### Días 31-90 — Estructural
- [ ] G6 tema sin reload · D5 rate limiter compartido · G5 json_schema en llm.py
- [ ] H-001 dividir god classes
- **Criterio:** score objetivo ≥ 3.9; deuda estructural planificada

---

> **Generado por:** auditoria-ingenieria · 2026-08-14
> **Siguiente:** aplicar los fixes del code-review (críticos+altos primero) según pedido del usuario.
