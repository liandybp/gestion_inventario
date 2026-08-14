# Phase 1 — Requirements: Copiloto de negocio (dashboard dinámico con IA)

- **Slug:** `copiloto-dashboard-ia`
- **Fecha:** 2026-08-14
- **Fuente del diseño:** `docs/plans/2026-08-14-copiloto-ia-dashboard-design.md`

---

## 1.1 Functional Requirements (EARS + Gherkin)

### REQ-1: Pestaña "Copiloto" accesible para todos los roles
**Priority:** Must Have
**WHEN** un usuario autenticado (admin/owner/operator) navega al tab "Copiloto",
**THE SYSTEM SHALL** mostrar una pestaña con un campo de texto de chat y el grid de
widgets persistidos del negocio activo.

```gherkin
**Scenario:** Cualquier rol ve la pestaña Copiloto
  GIVEN un usuario autenticado con rol admin, owner u operator
  WHEN el usuario hace GET /ui/tabs/agent
  THEN la respuesta es 200
  AND contiene el grid de widgets del negocio activo (por posición)
  AND contiene el formulario de chat
```

### REQ-2: El agente construye widgets desde lenguaje natural
**Priority:** Must Have
**WHEN** un usuario admin/owner envía un mensaje en lenguaje natural,
**THE SYSTEM SHALL** ejecutar el agente LangGraph, consultar los datos reales del negocio
activo vía `InventoryService`, y producir uno o más widgets + una narración en lenguaje natural.

```gherkin
**Scenario:** Petición que produce un widget de gráfico
  GIVEN un admin autenticado con negocio activo "recambios"
  WHEN el admin envía POST /ui/agent/chat con mensaje "ventas por producto este mes"
  THEN la respuesta es 200
  AND persiste al menos un DashboardWidget con type en (line, bar, table, kpi, list)
  AND la respuesta incluye HTML del widget y una narración
```

### REQ-3: Tipos de widget renderizables
**Priority:** Must Have
**WHEN** un widget es renderizado,
**THE SYSTEM SHALL** soportar los tipos `kpi`, `line`, `bar`, `table`, `list`
(gráficos vía Chart.js, tablas/tarjetas vía HTML).

```gherkin
**Scenario:** Widget tipo kpi se renderiza como tarjeta
  GIVEN un DashboardWidget con type "kpi" y title "Valor de inventario"
  WHEN se renderiza el grid
  THEN aparece una tarjeta con el valor formateado
```

### REQ-4: Persistencia por negocio
**Priority:** Must Have
**WHEN** un widget se crea,
**THE SYSTEM SHALL** persistirlo en `dashboard_widgets` con `business_id` del negocio activo,
de modo que sobreviva recargas y sesiones y sea compartido entre usuarios del mismo negocio.

```gherkin
**Scenario:** Widget persiste y es compartido por negocio
  GIVEN un widget creado en el negocio "recambios"
  WHEN otro usuario del negocio "recambios" carga la pestaña
  THEN el widget aparece
  AND un usuario del negocio "ropa" NO lo ve
```

### REQ-5: Borrar widget
**Priority:** Must Have
**WHEN** un admin/owner borra un widget,
**THE SYSTEM SHALL** eliminar su fila en `dashboard_widgets`.

```gherkin
**Scenario:** Admin borra widget
  GIVEN un DashboardWidget con id 5 del negocio activo
  WHEN el admin envía POST /ui/agent/widget/5/delete
  THEN la fila se elimina y el grid se re-renderiza sin él
```

### REQ-6: Aislamiento multi-negocio en las tools
**Priority:** Must Have
**WHEN** el agente ejecuta cualquier tool de datos,
**THE SYSTEM SHALL** inyectar el `business_id` activo desde la sesión (el LLM NO decide el scoping).

```gherkin
**Scenario:** Tool no filtra por negocio
  GIVEN un usuario con negocio activo A
  WHEN el agente llama la tool sales_by_product
  THEN la consulta filtra InventoryMovement.business_id == A
  AND no incluye datos de otros negocios
```

### REQ-7: Control de roles
**Priority:** Must Have
**WHEN** un operator intenta construir/borrar widgets,
**THE SYSTEM SHALL** responder 403 (operator solo puede ver).

```gherkin
**Scenario:** Operator no puede construir
  GIVEN un usuario operator autenticado
  WHEN envía POST /ui/agent/chat
  THEN la respuesta es 403
```

### REQ-8: Narración en lenguaje natural
**Priority:** Must Have
**WHEN** el agente produce widgets,
**THE SYSTEM SHALL** devolver una narración legible que explique qué se consultó y qué se encontró.

### REQ-9: Degradación ante fallo del LLM
**Priority:** Must Have
**WHEN** el LLM no responde (timeout/error),
**THE SYSTEM SHALL** devolver un mensaje amigable ("copiloto no disponible") sin romper la pestaña
ni afectar a otras peticiones.

### REQ-10: Concurrencia no bloqueante
**Priority:** Must Have
**WHEN** varios usuarios envían peticiones simultáneas,
**THE SYSTEM SHALL** ejecutar el agente en threadpool con semáforo (máx 5–10 LLM concurrentes),
timeout por llamada, y no bloquear el event loop de FastAPI.

```gherkin
**Scenario:** Peticiones simultáneas no bloquean la app
  GIVEN 5 peticiones concurrentes a /ui/agent/chat
  WHEN se procesan
  THEN el event loop sigue sirviendo /health y otras rutas
```

### REQ-11: Widget genérico semi-abierto (métrica × dimensión × tiempo)
**Priority:** Should Have
**WHEN** el usuario pide una combinación no cubierta por una tool finita,
**THE SYSTEM SHALL** resolverla vía un resolver parametrizado (métrica ∈ whitelist, dimensión ∈
{tiempo, producto, ubicación}, rango de tiempo) SIN SQL generado por LLM.

### REQ-12: Rate limiting del endpoint del agente
**Priority:** Should Have
**WHEN** un usuario excede el límite de peticiones al agente,
**THE SYSTEM SHALL** responder 429 (reutilizando el patrón de rate limiting de login).

---

## 1.2 MoSCoW Prioritization

| Priority | REQs | % |
|----------|------|---|
| **Must Have** | REQ-1..REQ-10 | ~60% |
| **Should Have** | REQ-11 (widget genérico), REQ-12 (rate limit) | ~25% |
| **Could Have** | reordenar widgets, editar título, insights proactivos programados | ~10% |
| **Won't Have** (esta iteración) | mutaciones de negocio por chat (ventas/compras), SQL generado por LLM, routing multi-proveedor | ~5% |

---

## 1.3 Non-Functional Requirements

| Category | Question | Answer |
|----------|----------|--------|
| **Performance** | Tiempo de respuesta esperado | La petición del agente (varias llamadas LLM) puede tardar 2–15s; debe ser asíncrona/threadpool y no degradar el resto de la app |
| **Security** | Nuevos límites de auth | Sí — admin/owner construyen, operator ve (`ensure_admin_or_owner`); `business_id` forzado en cada tool; API key en env var |
| **Scalability** | Volumen esperado | Pocos usuarios internos (~8 concurrentes); semáforo acota el paralelismo LLM; caché de prompt compartido |
| **Observability** | Logs/metrics | Loggear inicio/fin de cada run del agente, timeout, y errores de tool (reusar `app/logger.py`) |
| **Reliability** | Tasa de error aceptable / recuperación | Retry acotado (máx 2) ante schema inválido; degradación amigable ante LLM caído; timeout 30–60s |

---

## 1.4 Out of Scope (esta iteración)

- Mutaciones de negocio por chat (registrar venta/compra/traspaso) — requiere HITL.
- SQL generado por el LLM.
- Routing automático entre proveedores (solo primario + fallback vía env var).
- Reentrenamiento/fine-tuning de modelos.
- Servidor MCP para exponer el inventario a agentes externos.

---

## 1.5 Dependencies

- **Nuevas dependencias Python (aprobadas 2026-08-14):**
  - `langgraph` — orquestación del grafo.
  - `langchain-openai` — cliente `ChatOpenAI` con `base_url` (Gemini / GPT-4o mini / DeepSeek — todos OpenAI-compatible).
  - `httpx` — cliente HTTP async.
- **Modelo LLM primario:** Gemini 2.5 Flash-Lite vía endpoint OpenAI-compatible (pendiente confirmar precio en `ai.google.dev/pricing`).
- **Fallback:** GPT-4o mini (env var). ⚠️ El modelo local `qwen36_35b_coder_mtp` es SOLO para OpenCode (implementación), NO para el runtime del copiloto — la app usa APIs LLM externas.
- **Servicios internos ya existentes:** `InventoryService` (firmas reales en Phase 2), `ensure_admin_or_owner`, `require_active_business_id`.

---

## 1.6 Open Questions

- Precio exacto de Gemini Flash-Lite (las fuentes difieren) — **no bloquea el diseño**, solo la env var `AGENT_LLM_MODEL`.
- Ninguna otra ambigüedad. All requirements are unambiguous. Proceeding to Phase 2.
