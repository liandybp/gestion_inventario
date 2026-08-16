# PAE-coach-financiero-operativo: Coach financiero y operativo

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     1. Leer CLAUDE.md del proyecto. Si no existe, detenerse y reportar.
     2. Leer sección 5 (Arquitectura objetivo) completa antes de tocar archivos.
     3. Una fase a la vez, en el orden de la tabla de la sección 6. Sin fase N
        verificada (PASS), no se empieza N+1 — respetar "Depends on" de cada envelope.
     4. Si algo no está especificado aquí o en tasks.md: no se inventa, se reporta.
     5. Archivos fuera del inventario (5.2): no se tocan.
     6. CADA tarea incluye verificación mecánica (pytest u otro comando). Si falla:
        corregir esa tarea, no avanzar a la siguiente.
     7. El código exacto (before/after) de cada envelope está en
        specs/coach-financiero-operativo/tasks.md — leer SOLO el envelope activo,
        no el documento completo.
     ═══════════════════════════════════════════════════════════════ -->

## 1. Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `develop` |
| **Rama de trabajo** | `feature/coach-financiero-operativo` |
| **Fecha** | 2026-08-16 |
| **Versión actual** | v0.4.0 |
| **Versión objetivo** | v0.4.1 |
| **PAE anterior** | `PAE-copiloto-dashboard-ia` |
| **Fase** | ejecución (OpenCode + Qwen3.6-35B-A3B `qwen36_35b_coder_mtp` local; GLM-4.7-Flash `glm_coder` fallback) |
| **Test runner** | `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v` |
| **E2E runner** | `python3 -m pytest tests/e2e -m e2e --no-cov -v` |
| **Generado por** | SDD-Agent (4 fases: requirements → design → task envelopes → PAE assembly) |

## 2. Documentos fuente

| Fase SDD | Output | Contenido |
|----------|--------|-----------|
| Diseño previo (brainstorming, aprobado por el usuario) | `docs/plans/2026-08-16-coach-financiero-operativo-design.md` | decisiones de alto nivel, UX, alcance |
| Phase 1 | `specs/coach-financiero-operativo/requirements.md` | REQ-1..13 con Gherkin + MoSCoW + NFRs + out-of-scope |
| Phase 2 | `specs/coach-financiero-operativo/design.md` | codebase grounding, modelo de datos, API, seguridad, trade-offs, migración |
| Phase 3 | `specs/coach-financiero-operativo/tasks.md` | **24 task envelopes con código exacto (before/after)**, validados contra `pae-estructura.md` (≤800 tokens, ≤2 archivos, logging) |

## 3. Objetivo

Añadir un **coach financiero y operativo** a la app. Un `CoachService` **determinista** (sin
LLM) calcula indicadores financieros, operativos y de clientes hoy inexistentes, evalúa reglas
contra umbrales declarativos, estima el impacto en dinero de cada acción, ajusta por
estacionalidad y respeta el círculo cerrado de acciones ya marcadas — produciendo un **score de
salud 0–100** y una **lista priorizada de recomendaciones accionables** (síntoma → causa
operativa → acción → impacto $) para usuarios sin conocimientos de finanzas ni operaciones. Se
expone en un tab **"Coach"** (solo admin/owner, sin acceso para operator) con una burbuja de
chat que reutiliza el agente LangGraph existente para explicar el diagnóstico en lenguaje
natural, sin inventar cifras.

**Qué NO se toca:** lógica existente de `InventoryService`/`ReportingService`/`FinanceService`/
`SalesDocumentService` (solo lectura o extensión aditiva), `security.py`, `deps.py`, `db.py`,
`auth.py`, routers UI existentes, el agente LangGraph (`app/agent/graph.py`, `runner.py`, etc. —
solo se añaden 2 tools nuevas a `tools.py`), modelos existentes (solo se añaden tablas nuevas).

## 4. Decisiones de diseño

| # | Decisión | Justificación |
|---|----------|---------------|
| D1 | Híbrido: núcleo determinista + chat vía agente LLM existente | recomendación financiera no puede depender de que el LLM "adivine bien" |
| D2 | Diagnóstico/score/alertas en `CoachService` (Python puro, sin LLM) | testeable con pytest, funciona con el LLM caído |
| D3 | Chat reutiliza agente LangGraph + 2 tools nuevas read-only | el LLM explica el diagnóstico ya calculado, nunca lo inventa |
| D4 | Cálculo on-demand al abrir el tab | sin scheduler nuevo; datos siempre frescos |
| D5 | Caja real vía `CashBaseline` (saldo puntual) + cálculo en vivo desde el más reciente | el modelo no tiene balance; un baseline puntual desbloquea saldo real |
| D6 | Deuda/pasivos fuera de alcance | sin ningún dato de origen en el sistema |
| D7 | Recomendación = síntoma + causa operativa + acción + impacto $ | usuario no experto sabe exactamente qué hacer |
| D8 | Umbrales/textos/estimadores en `coach_rules.py` declarativo, función pura `evaluate()` | P5 configurabilidad; testeable sin BD |
| D9 | RBAC solo admin/owner; operator sin acceso (tab oculto + 403 en los 3 endpoints) | diagnóstico financiero sensible, más estricto que el copiloto |
| D10 | Incluye A (círculo cerrado), B (estacionalidad), C (salud de clientes) en el alcance | blindan confianza y evitan falsas alarmas; D/E/F quedan para fase siguiente |
| D11 | `try/except` + `logging.error(exc_info=True)` en cada método público de `CoachService`/`coach_rules` | REQ-13: ningún error interno debe romper el tab con un 500 |

## 5. Arquitectura objetivo

### 5.1 Diagrama de componentes

```
[UI HTMX] ── GET /ui/tabs/coach ──▶ [ui_coach.py ★]
                                      │ ensure_admin_or_owner + require_active_business_id
                                      ▼
                                 [CoachService ★]  (determinista, sin LLM, con logging)
                                   │  ├─▶ ReportingService (·existente, read)
                                   │  ├─▶ FinanceService   (·existente, read)
                                   │  ├─▶ InventoryService (·existente, read + 2 métodos nuevos)
                                   │  ├─▶ SalesDocumentService (·existente, read)
                                   │  ├─▶ coach_rules.py ★ (umbrales, textos, impacto — función pura)
                                   │  ├─▶ CashBaseline (BD ★)  → caja real
                                   │  └─▶ CoachActionLog (BD ★) → círculo cerrado [A]
                                   ▼
                          score + recomendaciones priorizadas + "lo que va bien"
                                   ▼
                          [partials/tab_coach.html ★]

[Burbuja chat] ── POST /ui/agent/chat (·existente, sin modificar) ──▶ agente LangGraph
                                                                        + 2 tools coach ★ (tools.py)
```

### 5.2 Inventario de archivos (scope duro)

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/models.py` | MODIFY | añadir `CashBaseline` (T1) y `CoachActionLog` (T2) |
| `app/services/coach_rules.py` | CREATE | umbrales, textos, estimadores de impacto (T4a/T4b) |
| `app/services/coach_service.py` | CREATE | indicadores + reglas + estacionalidad + scoring + círculo cerrado (T6a-T11) |
| `app/services/inventory_service.py` | MODIFY | +`dead_stock()`, +`inventory_turnover_days()` (T5, aditivo, solo lectura) |
| `app/schemas.py` | MODIFY | +`CashBaselineCreate`, +`CoachActionUpdate` (T3) |
| `app/routers/ui_coach.py` | CREATE | GET tab (T12), POST caja (T13), POST acción (T14) |
| `app/templates/partials/tab_coach.html` | CREATE | score + alertas + recomendaciones (T15a) |
| `app/templates/dashboard.html` | MODIFY | botón sidebar "Coach" (T15b, dentro del bloque admin/owner ya existente) |
| `app/main.py` | MODIFY | registrar `ui_coach.router` (T15b) |
| `app/agent/tools.py` | MODIFY | +`coach_health_score`, +`coach_recommendations` (T16, read-only, scoped) |
| `tests/unit/test_coach_models.py` | CREATE | tests de `CashBaseline`/`CoachActionLog` (T1, T2) |
| `tests/unit/test_coach_schemas.py` | CREATE | tests de validación Pydantic (T3) |
| `tests/unit/test_coach_rules.py` | CREATE | tests del motor de reglas (T4a, T4b) |
| `tests/unit/test_inventory_service_coach.py` | CREATE | tests de `dead_stock`/`turnover_days` (T5) |
| `tests/unit/test_coach_service.py` | CREATE | tests de `CoachService` (T6a-T11) |
| `tests/unit/test_coach_rbac.py` | CREATE | RBAC operator 403 + validación 422 (T12-T14) |
| `tests/unit/test_coach_tools.py` | CREATE | tests de las tools del agente (T16) |
| `tests/e2e/test_coach.py` | CREATE | flujo admin completo + operator sin acceso (T17) |
| `VERSION`, `CLAUDE.md`, `.claude/CLAUDE.md` | MODIFY | estado del proyecto (T18a, T18b) |
| `~/obsidian-lab/05-Proyectos/gestion-inventario.md` | MODIFY | nota Obsidian del proyecto (T18b) |
| `ReportingService`, `FinanceService`, `SalesDocumentService`, `security.py`, `ui_common.py`, `db.py`, `app/agent/graph.py`, `app/agent/runner.py` | REFERENCE | solo lectura, no se modifican |

### 5.3 Firmas reales del repo (grounding — ver `design.md` §2.1 para el detalle completo)

```python
# app/services/reporting_service.py — YA EXISTE
class ReportingService:
    def __init__(self, db: Session, business_id: int): ...
    def monthly_profit_report(self, now=None, ...) -> tuple[dict, list[dict]]
        # summary: sales_total, cogs_total, gross_total, expenses_total, net_total, gross_margin_pct
    def monthly_overview(self, months=12, now=None, ...) -> list[dict]
    def sales_metrics_table(self, now, ...) -> list[dict]  # name, stock_qty, reorder_shortage

# app/services/finance_service.py / inventory_service.py / sales_document_service.py — YA EXISTEN
class FinanceService:
    def monthly_dividends_report(self, now=None, ...) -> dict
class InventoryService:
    def __init__(self, db, business_id=None): ...
    def inventory_value_total(self, location_id=None) -> float
class SalesDocumentService:
    def sales_by_customer(self, start=None, end=None, limit=20) -> list[dict]

# app/security.py / ui_common.py / deps.py — YA EXISTEN
def require_active_business_id(db, request) -> int
def ensure_admin_or_owner(db, request) -> None
def session_dep() -> Generator[Session, None, None]

# app/agent/tools.py — YA EXISTE, patrón de tool
def tool(name: str, description: str, params: dict) -> Callable: ...
```

### 5.4 Interfaces nuevas

> **Nota (2026-08-16, post-implementación):** esta sección documentaba el diseño original
> (`diagnose()` único, score 0-100, `seasonal_note`). La implementación real de OpenCode
> divergió y quedó así tras la corrección de code-review (F-004, F-006):

```python
# app/services/coach_service.py — firmas reales tras implementación + fixes
class CoachService:
    def __init__(self, db: Session, business_id: int): ...
    def health_score(self, now: datetime | None = None) -> dict
        # {"score": float|None (escala 1.0-10.0, no 0-100), "level": "red|yellow|green|unknown",
        #  "message"?: str, "whats_good": list[str],
        #  "financial": dict, "operational": dict, "customers": dict, "cash": dict}
    def recommendations(self, now: datetime | None = None) -> list[dict]
        # lista de {"rule_key","severity","symptom","cause","action","impact_amount","impact_text"}
        # ambos métodos comparten cómputo cacheado internamente (_compute_indicators) —
        # no se llama dos veces el pipeline de indicadores por request (fix F-004)
    def current_cash(self, now: datetime | None = None) -> dict  # {"amount": float|None, "mode": "real|relative"}
    def mark_action(self, rule_key: str, status: str, user_id: int) -> None  # [A]

# app/services/coach_rules.py
def evaluate(indicators: dict) -> list[dict]  # función pura, sin BD
```

**Deriva de scope conocida:** la estacionalidad [B] (REQ-8, `_seasonal_context`) especificada
en `requirements.md` y en los envelopes T9 originales **no se implementó** — no existe en
`coach_service.py`. No hay `seasonal_note` en ningún lado del reporte real. Pendiente de
decisión: implementarlo en una fase posterior o marcar REQ-8 formalmente como no entregado
en esta iteración.

## 6. Fases de implementación (Task Envelopes)

> Orden estricto respetando "Depends on". Código exacto (before/after) para cada uno en
> `specs/coach-financiero-operativo/tasks.md`. Todos verificados ≤800 tokens de Instructions y
> ≤2 archivos (T19 exento — operación git mecánica, ver su nota en tasks.md).

| T | Título | Depends on | Files | Pillars |
|---|--------|------------|-------|---------|
| **T1** | Modelo `CashBaseline` | none | `models.py` | P1,P4,P5,P6 |
| **T2** | Modelo `CoachActionLog` [A] | T1 | `models.py` | P1,P4,P5,P6 |
| **T3** | Schemas `CashBaselineCreate` + `CoachActionUpdate` | T2 | `schemas.py` | P1,P5,P6 |
| **T4a** | `coach_rules.py` — reglas financieras básicas + logging | none | `coach_rules.py` | P1,P2,P4,P5,P6 |
| **T4b** | `coach_rules.py` — reglas operativas + impacto | T4a | `coach_rules.py` | P1,P4,P5 |
| **T5** | `InventoryService.dead_stock` + `inventory_turnover_days` | none | `inventory_service.py` | P1,P4,P8 |
| **T6a** | `CoachService` — indicadores financieros básicos + logging | T4a | `coach_service.py` | P1,P2,P4 |
| **T6b** | `CoachService` — crecimiento y ratio de extracciones | T6a | `coach_service.py` | P1,P4 |
| **T6c** | `CoachService` — caja real desde `CashBaseline` | T6a, T1 | `coach_service.py` | P1,P4 |
| **T7** | `CoachService` — indicadores operativos | T5, T6a | `coach_service.py` | P1,P4 |
| **T8** | `CoachService` — salud de clientes [C] | T7 | `coach_service.py` | P1,P4 |
| **T9** | `CoachService` — estacionalidad [B] | T7 | `coach_service.py` | P1,P4 |
| **T10** | `CoachService.diagnose()` — orquestación y scoring | T6b, T6c, T8, T9 | `coach_service.py` | P1,P2,P4 |
| **T11** | `CoachService` — círculo cerrado [A] | T2, T10 | `coach_service.py` | P1,P2,P4,P6 |
| **T12** | Router `ui_coach.py` — GET tab | T3, T10 | `ui_coach.py` | P1,P6 |
| **T13** | Router `ui_coach.py` — POST caja | T3, T10 | `ui_coach.py` | P1,P6 |
| **T14** | Router `ui_coach.py` — POST acción | T11, T13 | `ui_coach.py` | P1,P6 |
| **T15a** | Template `tab_coach.html` | T14 | `tab_coach.html` | P1,P4 |
| **T15b** | Registrar router + botón sidebar | T15a | `main.py`, `dashboard.html` | P1,P4 |
| **T16** | Tools del agente `coach_health_score` + `coach_recommendations` | T10 | `tools.py` | P1,P6 |
| **T17** | E2E: admin flujo completo + operator sin acceso | T15b, T16 | `test_coach.py` | P1 |
| **T18a** | Actualizar VERSION + `CLAUDE.md` raíz | T17 | `VERSION`, `CLAUDE.md` | P4 |
| **T18b** | Actualizar `.claude/CLAUDE.md` + nota Obsidian | T18a | `.claude/CLAUDE.md`, Obsidian | P4 |
| **T19** | Commit final | T18b | (git, exento del límite de 2 archivos) | P4 |

## 7. Plan de tests y verificación

### 7.1 Pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
# Expected: 149 unit PASS (antes de tocar nada)
```

### 7.2 Verificación por envelope
Cada envelope (T1-T18b) cierra con su comando de verificación mecánico (`pytest ...`, o
`python3 -c "from app.main import app"` para arranque) → PASS/FAIL. Sin PASS, no avanza al
siguiente. T19 verifica con `git log --oneline -1`.

### 7.3 Suite completa al cerrar

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v   # 149 + ~25 nuevos
python3 -m pytest tests/e2e -m e2e --no-cov -v                                      # 16 + 2 coach
```

### 7.4 Tests existentes que deben seguir pasando

Los 149 unit + 16 e2e actuales. El feature es **aditivo**: no modifica lógica existente, solo
añade 2 métodos read-only a `InventoryService`, 2 tablas nuevas y 2 tools read-only al agente.

## 8. Riesgos y mitigaciones

| Riesgo | Prob | Impacto | Síntoma | Mitigación |
|--------|------|---------|---------|------------|
| Estimadores de impacto poco fiables | Media | Medio | Usuario desconfía de los "$" | Etiquetados como estimación; fórmulas simples sobre datos reales; unit tests con montos exactos |
| Falsas alarmas por no considerar estacionalidad | Media | Alto | Coach "grita" sin contexto | T9 estacionalidad; requiere ≥13 meses de historial, si no se omite el ajuste (REQ-8) |
| Negocio nuevo sin datos → división por cero | Alta | Medio | Crash o números sin sentido | Guard explícito en `diagnose()`: `sales_total <= 0` → `level="unknown"` (T10, REQ-13) |
| Excepción interna en cualquier indicador | Media | Alto | 500 al usuario | `try/except` + logging en cada método (T4a, T6a, T10, T11 — D11/REQ-13) |
| Caja calculada se desvía de la real | Media | Medio | Usuario ve saldo incorrecto | Recalibración vía nuevo `CashBaseline`; siempre se usa el más reciente (T6c) |
| LLM caído tumba el tab | Baja | Alto | Tab no carga | Diagnóstico determinista independiente del LLM; solo el chat degrada (circuit breaker existente, sin tocar) |
| Envelope excede tokens o archivos | Baja | Medio | OpenCode se lía | Ya auditado: 24/24 envelopes ≤800 tokens y ≤2 archivos (verificado 2026-08-16, ver nota en tasks.md) |

## 9. Definición de hecho (DoD)

- [ ] T1–T19 verificados (cada uno PASS, en orden de dependencias)
- [ ] `CoachService.diagnose()` calcula los indicadores de `design.md` §2.1 con unit tests de valor exacto
- [ ] Recomendaciones con síntoma + causa + acción + impacto $, ordenadas por severidad × impacto
- [ ] Círculo cerrado [A]: marcar hecha/descartada suprime la recomendación en `diagnose()` siguiente
- [ ] Estacionalidad [B]: sin historial suficiente no se aplica ajuste (no falsos positivos)
- [ ] Salud de clientes [C]: recurrentes/nuevos + fuga detectada
- [ ] Caja real desde `CashBaseline` + modo relativo sin baseline (REQ-7)
- [ ] RBAC: operator sin acceso a los 3 endpoints (403); caja/acción validan inputs (422)
- [ ] Chat reutiliza el agente existente y responde grounded; degrada elegante si el LLM cae
- [ ] Ningún método de `CoachService`/`coach_rules` propaga una excepción sin loggear
- [ ] Suite completa verde: 149+~25 unit + 16+2 e2e
- [ ] Docs actualizadas: `VERSION` → 0.4.1, `CLAUDE.md` raíz, `.claude/CLAUDE.md`, nota Obsidian `05-Proyectos/gestion-inventario.md`
- [ ] Todos los criterios Gherkin de `requirements.md` (REQ-1..13) verificados por al menos un test
