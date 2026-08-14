# Design — Copiloto de negocio: dashboard dinámico con IA

- **Fecha:** 2026-08-14
- **Estado:** diseño aprobado (pendiente plan de implementación)
- **Rama:** `develop`
- **Proyecto:** gestion_inventario (FastAPI + HTMX + SQLAlchemy)

---

## 1. Contexto y objetivo

Añadir una pestaña **"Copiloto"** al dashboard. El usuario escribe en lenguaje natural y un
agente (LangGraph + LLM) interpreta la petición, consulta los datos reales del negocio y
**añade widgets** (KPI, gráfico, tabla) a un grid que se va construyendo en esa misma pestaña.
El tablero es **persistente por negocio** y compartido entre sus usuarios.

Objetivo de valor: un dueño/encargado **no técnico** obtiene respuestas y visualizaciones
sobre su negocio (ventas, profit, gastos, inventario) sin navegar 14 tabs ni armar queries.

**Principio rector:** el LLM **nunca calcula números**. Solo interpreta, elige tools y narra.
Los datos los produce `InventoryService` (lógica determinista ya probada). El LLM no genera SQL.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación |
|---|----------|-------------|---------------|
| D1 | Escenario: Q&A + insights + **dashboard dinámico** en pestaña dedicada | Widget flotante (plan original) | El usuario pidió "todo en una sola pestaña que se va construyendo" |
| D2 | Vocabulario **híbrido semi-abierto** | Set finito / SQL libre | Set finito + widget genérico "métrica × dimensión × tiempo"; sin SQL generado por LLM |
| D3 | Orquestación **LangGraph** (grafo multi-nodo con validación + retry) | Intérprete single-shot | El usuario quiere el agente completo; se compensa el riesgo con schema+retry |
| D4 | Persistencia **por negocio** (tabla `dashboard_widgets`) | Por usuario / efímero | Tablero vivo compartido; `business_id` aisla datos |
| D5 | Modelo primario **Gemini 2.5 Flash-Lite** ($0.10/$0.40) | GPT-4o mini / DeepSeek v4-flash | Más barato entre los capaces; single-tool (su fortaleza) encaja con grafo de 1 tool por nodo. ⚠️ verificar precio en `ai.google.dev/pricing` |
| D6 | Fallback fiabilidad **GPT-4o mini** ($0.15/$0.60) | — | Si Flash-Lite produce tool-calls raros, cambiar env var |
| D7 | **Runtime LLM = API externa, NUNCA el modelo local** | — | La app usa `AGENT_LLM_*` (OpenAI-compatible). El modelo local `qwen36_35b_coder_mtp` es SOLO para OpenCode (generar código), no para las peticiones del copiloto |
| D8 | Roles: **admin y owner** construyen/borran; **operator** solo ve | admin-only | Coherente con multi-negocio owners |
| D9 | Contexto conversacional: últimos ~5 widgets en el prompt | Sin contexto / historial completo | Permite "compara con el mes pasado" sin inflar tokens |

---

## 3. Arquitectura

```
┌─ UI (HTMX) ──────────────────────────────────────────────┐
│  pestaña /tabs/agent  =  chat (arriba) + grid de widgets  │
└──────────────────────────┬───────────────────────────────┘
                           │ POST /ui/agent/chat  (HTMX)
┌──────────────────────────▼───────────────────────────────┐
│  app/agent/  ★ NUEVO                                      │
│   graph.py      → grafo LangGraph (estado, nodos, retry)  │
│   llm.py        → cliente OpenAI-compatible (httpx.Async) │
│   tools.py      → 9 tools read-only (MVP) sobre InventoryService│
│   prompts.py    → system prompts + schemas (cacheados)     │
│   schemas.py    → schema de widget (validación)            │
│   runner.py     → exec en threadpool + semáforo + timeout  │
└───────────────┬───────────────────────────────────────────┘
                │  llama servicios EXISTENTES (sin reescribir)
┌───────────────▼───────────────────────────────────────────┐
│  InventoryService (sales_metrics_table, monthly_profit, …) │
│  + modelo DashboardWidget (BD)                             │
└───────────────────────────────────────────────────────────┘
```

### Env vars

```
# Runtime LLM del copiloto — SIEMPRE API externa (OpenAI-compatible), NUNCA el modelo local.
AGENT_LLM_BASE_URL=https://generativelanguage.googleapis.com/v1beta/openai/  # Gemini (primario, pendiente verificar precio)
AGENT_LLM_MODEL=gemini-2.5-flash-lite
AGENT_LLM_API_KEY=<key>
# Alternativa/fallback: GPT-4o mini (cambiar las 2 vars de arriba):
# AGENT_LLM_BASE_URL=https://api.openai.com/v1
# AGENT_LLM_MODEL=gpt-4o-mini
# El modelo local qwen36_35b_coder_mtp es SOLO para OpenCode (escribir código), no para el copiloto.
```

---

## 4. Grafo LangGraph (~6 nodos)

```
plan (interpreta intención → elige tool(s)/widget(s))
  → tool_execute (llama tools, resultados estructurados)
  → validate (¿el widget spec cumple schema?)
      ├─ falla → error_recovery (retry acotado, máx 2, error inline) → tool_execute
      └─ ok    → build_widgets (arma 1..N widgets)
                → narrate (respuesta en lenguaje natural)
                → END
```

Estado del grafo: `business_id`, historial (últimos ~5 widgets), mensaje del usuario,
lista de tool calls, lista de widgets producidos, texto de respuesta.

---

## 5. Tools (vocabulario híbrido semi-abierto)

**a) Tools finitas read-only** (envuelven métodos reales de `InventoryService`,
todas reciben `business_id` inyectado desde la sesión — el LLM no decide el scoping):

`sales_metrics_table`, `sales_by_product`, `daily_sales_series`, `monthly_profit_report`,
`monthly_profit_items_report`, `monthly_overview`, `monthly_dividends_report`,
`top_expense_concept`, `inventory_value_total`, `inventory_sale_value_total`, `stock_list`,
`stock_for_location`, `available_lots`, `recent_sales`, `recent_purchases`,
`movement_history`, `total_expenses`, `total_extractions_by_party`.

**b) Widget genérico** `build_generic_widget(metric, dimension, time_range)`:
resuelve `metric` (whitelist: ventas, unidades, costo, margen, gastos, extracciones,
inventario) × `dimension` (tiempo, producto, ubicación) contra los mismos servicios.
Sin SQL libre.

**c) Mutación del tablero (único write):** `add_widget`, `remove_widget` — solo tocan
`DashboardWidget`, nunca datos de negocio.

Regla: tools **no solapadas** (DeepSeek/Flash-Lite pueden "equivocarse con confianza"
cuando las tools se solapan) + validación de schema + retry.

---

## 6. Concurrencia y escalabilidad

1. **No bloquear el event loop:** el grafo (síncrono) corre en threadpool vía `anyio.to_thread`;
   el cliente LLM usa `httpx.AsyncClient`.
2. **Backpressure:** semáforo global (tope 5–10 LLM concurrentes). Si se satura →
   respuesta "ocupado, reintentá" o spinner HTMX (patrón de polling ya existe en el proyecto).
3. **Timeout:** cada llamada LLM ~30–60s. Al expirar → degradación sin afectar a otros usuarios.
4. **Rate limiting por usuario** sobre `/ui/agent/chat` (reutilizar patrón de login).
5. **Caché de prompt:** system prompt + schemas de tools son prefijo compartido por todos
   los usuarios → cache hit masivo. El costo con 8 usuarios ≈ $3/mes (ver §8).

---

## 7. Persistencia

Nuevo modelo `DashboardWidget`:

```
id, business_id (FK), type (kpi|line|bar|table|list), title,
params (JSON — métrica, dimensión, rango, filtros), position (int),
created_by (FK user), created_at
```

Migración inline en `migrations.py`. El grid se re-renderiza ordenado por `position`.
Soporte para borrar y (fase 2) reordenar.

---

## 8. Coste estimado (a tu escala)

~8 usuarios × ~15 peticiones/día ≈ 3.600 peticiones/mes, con caché de prompt activo:

- Salida: 4,3M tok × $0.40 = ~$1,7
- Entrada fresh: 10,8M × $0.10 = ~$1,1
- Entrada cacheada: ~$0,01
- **Total ≈ $3/mes** (Gemini Flash-Lite). Aun ×3, un solo dígito.

---

## 9. Rendering de widgets

Tipos: `kpi` (tarjeta), `line` / `bar` (Chart.js, ya en el stack), `table`, `list`.
Cada tipo = partial Jinja (`partials/widget_kpi.html`, `widget_chart.html`, `widget_table.html`).
La respuesta HTMX hace swap dentro del grid. Widgets con datos vacíos → estado "sin datos".

---

## 10. Seguridad

- `business_id` forzado en cada tool (no se confía en el LLM para el aislamiento).
- Read-only salvo `add/remove_widget`.
- Sin SQL generado por LLM (genérico usa resolver parametrizado con whitelist).
- Roles: admin+owner construyen/borran; operator ve.
- Rate limiting + timeout.
- API key del LLM en env var, nunca en código.

---

## 11. Error handling

- LLM caído/timeout → "copiloto no disponible" sin romper el tab.
- Schema inválido tras retries → chips de sugerencia ("¿querés ver ventas por producto?").
- Tool devuelve vacío → widget "sin datos".
- Saturación del semáforo → "ocupado, reintentá en unos segundos".

---

## 12. Testing

- LLM **mockeado** (fake determinista) para tests del grafo (plan/tool/validate/retry/narrate).
- Aislamiento `business_id` en tools.
- Persistencia y rendering de `DashboardWidget`.
- Path de error: retry agotado, timeout, semáforo saturado.
- Concurrencia: test de N peticiones simultáneas no bloquean el event loop.

---

## 13. Fases

- **MVP:** grafo LangGraph + 9 tools finitas (payload canónico) + dashboard persistente + rendering. El resto del vocabulario (profit mensual, overview, historial) se difiere a una fase posterior (sus formas de retorno se verificarán al implementarlas).
  (KPI/chart/table) + Q&A + concurrencia (threadpool/semáforo/timeout/rate-limit).
- **Fase 2:** widget genérico semi-abierto + reordenar/editar widgets + insights proactivos
  (auto-widgets programados/on-demand) + presupuesto mensual real con métricas de uso.

---

## 14. Fuera de alcance (por ahora)

- Mutaciones de negocio por chat (registrar ventas/compras) — requiere HITL, riesgo alto.
- SQL generado por LLM.
- Reentrenamiento/fine-tuning.
- Múltiples proveedores con routing automático por complejidad (solo primario + fallback).
