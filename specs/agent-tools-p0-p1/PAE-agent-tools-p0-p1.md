# PAE-008: agent-tools-p0-p1

<!-- ═══════════════════════════════════════════════════════════════
     INSTRUCCIONES PARA OPENCODE — LEER ANTES DE CUALQUIER ACCIÓN
     ═══════════════════════════════════════════════════════════════
     1. Leer CLAUDE.md del proyecto (raíz y .claude/CLAUDE.md). Si no existe, detenerse.
     2. Leer sección 3 (Arquitectura objetivo) completa antes de tocar archivos.
     3. EJECUTAR VERIFICACIÓN PRE-IMPLEMENTACIÓN (sección 5.0) — sin excepción.
     4. Una tarea a la vez, en orden T1→T11. Sin T(N) verificada, no empieza T(N+1).
     5. Si algo no está especificado aquí: no se inventa, se reporta.
     6. Archivos fuera del inventario (3.2): no se tocan bajo ninguna circunstancia.
     7. CADA TAREA incluye verificación. Si falla: corregir, no avanzar.
     8. Al terminar: ejecutar script de verificación completa (sección 7.2).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | develop |
| **Rama de trabajo** | feature/agent-tools-p0-p1 |
| **Fecha** | 2026-08-17 |
| **Versión actual** | v0.4.2 |
| **Versión objetivo** | v0.4.3 (PATCH — regla dura del proyecto, nunca MINOR) |
| **PAE anterior** | ninguno específico (continúa sobre el fix de sesión de `profit_by_product` limit/sort, ya mergeado fuera de este PAE) |
| **Fase** | planificación (Claude Code) → ejecución (OpenCode + Qwen3.6-35B-A3B local, GLM-4.7-Flash fallback) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/agent-tools-p0-p1/requirements.md` | 5 REQs con Gherkin, MoSCoW, NFRs, out-of-scope |
| Phase 2 — Design | `specs/agent-tools-p0-p1/design.md` | Contratos de tool, trade-offs (D1-D4), pre-mortem 10 dimensiones |
| Phase 3 — Task Envelopes | `specs/agent-tools-p0-p1/tasks.md` | 11 envelopes autocontenidos para OpenCode |

### Skills asignadas a OpenCode

- [ ] `backend-developer` — implementación de las 4 tools nuevas + extensión de 3 tools existentes (T1-T8)
- [ ] `test-automation-engineer` — tests unitarios (T9)
- [ ] `release-manager` — version bump PATCH (T10)
- [ ] `technical-writer` — CHANGELOG (T11)

---

## 1. Objetivo

### 1.1 Qué se construye

Cuatro tools nuevas para el copiloto IA (`app/agent/tools.py`) — `dead_stock`,
`inventory_turnover`, `profit_by_category`, `profit_by_location` — y un parámetro
opcional `location` en tres tools existentes (`sales_by_product`, `profit_by_product`,
`daily_sales_series`) para filtrar por ubicación/POS. Ninguna requiere SQL nueva de
fondo: las 4 tools nuevas reusan métodos de servicio (`InventoryService.dead_stock`,
`InventoryService.inventory_turnover_days`, `ReportingService.monthly_profit_items_report`)
que ya existían pero no estaban expuestos al agente.

### 1.2 Por qué

Auditoría de cobertura de tools realizada en sesión de Claude Code (2026-08-17): se
cruzaron las 29 tools existentes contra las entidades del modelo y los métodos ya
escritos en la capa de servicios, y se encontró que 4 capacidades de negocio con lógica
YA implementada (stock muerto, rotación, ganancia por categoría, ganancia por ubicación)
no eran preguntables desde el copiloto — el usuario tenía que ir a la UI o no podía
consultarlas en absoluto vía chat.

### 1.3 Valor esperado

- El copiloto responde correctamente a "qué productos no se han vendido", "cada cuánto
  rota mi inventario", "qué categoría/ubicación es más rentable" y "ventas/ganancia de
  X en la central" sin que el usuario tenga que navegar la UI.
- 33 tools registradas (29 → 33), 9 tests unitarios nuevos, 0 tests existentes rotos.

### 1.4 Qué NO se toca

- `app/models.py` — cero migraciones, esta feature es 100% read-path.
- `app/agent/graph.py`, `app/agent/widgets.py`, `app/agent/runner.py` — el contrato de
  ejecución/degradación de errores y de persistencia de widgets ya soporta esto sin cambios.
- Ningún router HTTP ni plantilla Jinja/HTMX — no hay superficie de UI nueva.
- `tests/e2e/` — ver 1.4 de `requirements.md` (Out of Scope) para la justificación.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Resolución de `location` con query de solo lectura nueva, 404 si no existe | Reusar `InventoryService._location_id_for_code` / `LocationRepository.location_id_for_code(auto_create=True)` | Ese método auto-crea una `Location` si el código no existe — inaceptable en una tool de lectura invocada por un LLM que puede alucinar un código | Un código de ubicación mal escrito por el usuario/LLM da un error claro en el chat, nunca una fila fantasma en BD |
| D2 | Agregación de categoría/ubicación en Python sobre `monthly_profit_items_report` | Nueva query SQL con `GROUP BY` en `ReportingService` | El método ya trae los campos exactos por línea; una query nueva duplicaría el cálculo de `profit`/`margin_pct` ya resuelto ahí | Coherente con el patrón ya usado por `dividends_report`/`extractions_by_party` |
| D3 | `inventory_turnover` normaliza `None` → `{"value": 0.0, "label": "Sin datos suficientes"}` | Propagar `None` tal cual | Un widget `kpi` con `value: None` rompe el contrato (todas las demás tools devuelven `float`) | El service mantiene su contrato actual (usado también fuera del agente); la normalización vive solo en el borde de la tool |
| D4 | `category`/`location` vacíos se agrupan bajo `"Sin categoría"`/`"Sin ubicación"` | Descartar esas líneas | Descartar rompería la cuadratura con `monthly_profit_summary` (perdería ganancia real del período) | El total de `profit_by_category`/`profit_by_location` siempre suma igual que `monthly_profit_summary` para el mismo rango |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
Usuario → Chat (ui_agent.py, sin cambios)
            → LangGraph (_plan_node → _execute_node → _build_node, sin cambios)
                → TOOL_REGISTRY["dead_stock" ★] → InventoryService.dead_stock() ·
                → TOOL_REGISTRY["inventory_turnover" ★] → InventoryService.inventory_turnover_days() ·
                                                          → ReportingService.monthly_profit_report() ·
                → TOOL_REGISTRY["profit_by_category" ★] → ReportingService.monthly_profit_items_report() ·
                → TOOL_REGISTRY["profit_by_location" ★] → ReportingService.monthly_profit_items_report() ·
                → TOOL_REGISTRY["sales_by_product" ✦] → _resolve_location_id() ★ → Location ·
                → TOOL_REGISTRY["profit_by_product" ✦] → _resolve_location_id() ★
                → TOOL_REGISTRY["daily_sales_series" ✦] → _resolve_location_id() ★
            → SYSTEM_PROMPT_TEMPLATE ✦ (reglas de selección actualizadas)

★ nuevo   ✦ modificado   · sin cambios (solo contexto)
```

### 3.2 Inventario de archivos

> **Scope duro:** Solo estos archivos. Ninguno más.
> **Verificación:** `git diff --name-only develop` debe mostrar exactamente estos archivos.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/agent/tools.py` | MODIFY | 4 tools nuevas + helper `_resolve_location_id` + 3 tools extendidas con `location` |
| `app/agent/prompts.py` | MODIFY | Reglas de selección de tool para las 4 tools nuevas + filtro `location` |
| `tests/unit/test_agent_tools.py` | MODIFY | Conteo 29→33 + 9 tests nuevos |
| `VERSION` | MODIFY | `0.4.2` → `0.4.3` |
| `.bumpversion.cfg` | MODIFY | `current_version = 0.4.2` → `0.4.3` |
| `CHANGELOG.md` | MODIFY | Sección `## 0.4.3 (2026-08-17)` |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
def dead_stock(self, days: int = 90, now: Optional[datetime] = None) -> list[dict]: ...
def inventory_turnover_days(self, monthly_cogs: float) -> Optional[float]: ...

# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def monthly_profit_report(self, now=None, location_id=None, *, start=None, end=None) -> tuple[dict, list[dict]]: ...
def monthly_profit_items_report(self, now=None, *, start=None, end=None, location_id=None) -> tuple[dict, list[dict]]: ...
def sales_by_product(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> tuple[float, list[dict]]: ...
def daily_sales_series(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> list[dict]: ...

# app/models.py — YA EXISTE, NO MODIFICAR
class Location(Base):
    id: Mapped[int]; business_id: Mapped[Optional[int]]; code: Mapped[str]; name: Mapped[str]

# app/agent/graph.py — YA EXISTE, NO MODIFICAR
# _execute_node ya envuelve cada tool en try/except HTTPException + except Exception,
# degradando el turno sin romper el chat.
```

### 3.4 Interfaces nuevas a implementar

```python
# app/agent/tools.py
def _resolve_location_id(service: InventoryService, location: str) -> int:
    """Código/nombre -> id, solo lectura, scoped a business_id. Lanza HTTPException(404)
    si no existe. Nunca autocrea."""

@tool("dead_stock", ...)
def dead_stock(service: InventoryService, args: dict) -> dict: ...

@tool("inventory_turnover", ...)
def inventory_turnover(service: InventoryService, args: dict) -> dict: ...

@tool("profit_by_category", ...)
def profit_by_category(service: InventoryService, args: dict) -> dict: ...

@tool("profit_by_location", ...)
def profit_by_location(service: InventoryService, args: dict) -> dict: ...
```

Ver `tasks.md` para el código completo de cada función (before/after exacto).

---

## 4. Fases de implementación (Task Envelopes)

> Los 11 task envelopes completos (T1-T11) están en `specs/agent-tools-p0-p1/tasks.md`.
> OpenCode ejecuta ese archivo directamente, un envelope a la vez, en orden T1→T11.

| Envelope | Qué hace | Archivo(s) | Depende de |
|---|---|---|---|
| T1 | Tool `dead_stock` | `app/agent/tools.py` | ninguno |
| T2 | Tool `inventory_turnover` | `app/agent/tools.py` | T1 |
| T3 | Tool `profit_by_category` | `app/agent/tools.py` | T2 |
| T4 | Tool `profit_by_location` | `app/agent/tools.py` | T3 |
| T5 | Helper `_resolve_location_id` + `location` en `sales_by_product` | `app/agent/tools.py` | T4 |
| T6 | `location` en `profit_by_product` | `app/agent/tools.py` | T5 |
| T7 | `location` en `daily_sales_series` | `app/agent/tools.py` | T5 |
| T8 | Reglas del system prompt | `app/agent/prompts.py` | T7 |
| T9 | Tests unitarios (9 nuevos + conteo 29→33) | `tests/unit/test_agent_tools.py` | T8 |
| T10 | Suite completa + VERSION bump PATCH | `VERSION`, `.bumpversion.cfg` | T9 |
| T11 | CHANGELOG | `CHANGELOG.md` | T10 |

**Checkpoints de `/compact` recomendados:** después de T4 y después de T8 (batches de
≤5 envelopes, regla del harness).

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO — antes de tocar código)

```bash
# 1. Todos los tests existentes deben pasar ANTES de empezar
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v

# 2. Seed: no aplica — este proyecto no tiene script `seed.py` standalone; el seed de
#    usuarios admin/operator corre inline en `app/migrations.py` durante el startup de
#    la app (ver Step "App" en 7.1).

# 3. Schema drift: no aplica — este proyecto no tiene `test_schema_drift.py` y este PAE
#    no modifica `app/models.py` (0 migraciones, ver 1.4).
```

Si el paso 1 falla, **NO empezar la implementación**. Reportar el fallo y esperar instrucciones.

### 5.1 Verificación por task envelope

Cada envelope en `tasks.md` incluye su propio bloque `#### Verification` con comandos
mecánicos (`python3 -c "..."` con `assert` + `print("OK")`, o `pytest -k <nombre>`).

### 5.2 Protocolo cuando una verificación falla

1. Leer el error completo — no asumir la causa.
2. `git diff` para ver exactamente qué se cambió.
3. Corregir el código (no el test).
4. Re-ejecutar la verificación hasta que pase.
5. Si tras 3 intentos sigue fallando: reportar el error completo, el diff, y lo
   intentado. NO seguir al siguiente envelope.

### 5.3 Tests existentes que deben seguir pasando

- [ ] `tests/unit/test_agent_tools.py` — los 20 tests existentes, sin cambios de comportamiento
- [ ] `tests/unit/test_coach_tools.py` — sin cambios (no toca `CoachService`)
- [ ] `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit` — 100% pasan antes y después

### 5.4 Tests e2e

**No aplica a este PAE.** Ver `requirements.md` §1.4 (Out of Scope): esta feature no
añade superficie de UI nueva — el flujo chat → widget ya está cubierto por los e2e
existentes del copiloto (`tests/e2e/`), y ninguna de las 29 tools actuales tiene un e2e
dedicado por tool (serían redundantes con el e2e genérico "el copiloto responde con un
widget"). Cobertura de comportamiento vía tests unitarios de tool (T9).

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | `_resolve_location_id` crea falsos negativos si el LLM pasa un nombre con acentos/typos distinto al guardado | Media | Bajo | El copiloto responde "Ubicación no encontrada" para una ubicación que sí existe | Fuera de alcance explícito (ver requirements.md 1.4) — degrada con gracia (404 manejado por `_execute_node`, no rompe el chat), el usuario puede reformular | Ninguno necesario — no es un bug, es un límite documentado |
| R2 | Category `None` en `profit_by_category` se agrupa distinto a como el usuario esperaba ("Sin categoría" vs excluir) | Baja | Bajo | Un producto sin categoría aparece agrupado con otros sin categoría en vez de solo | Documentado explícitamente en D4 — decisión consciente para no romper la cuadratura del total | Revertir a excluir esas líneas si el usuario lo pide explícitamente (cambio de 1 línea) |
| R3 | `test_tool_schemas_count` (hardcodea el número) se vuelve a desincronizar en el futuro si se añaden más tools sin actualizar el test | Media | Bajo | Test falla tras un cambio no relacionado | Ya es el patrón existente del proyecto (era 29, ahora 33) — no se introduce riesgo nuevo, solo se hereda uno ya presente | El test falla ruidosamente (no silenciosamente), que es el comportamiento deseado |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (OBLIGATORIO — ejecutar en orden)

- [ ] **Pre-check:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v` — 100% pasaban antes de empezar (guardar output)
- [ ] **Tests:** mismo comando — 100% pasan después de los cambios (149+9 = ≥158 tests unitarios de la suite, más los ya existentes de otros módulos)
- [ ] **App:** `uvicorn app.main:app --host 0.0.0.0 --port 10000 --reload` — arranca sin errores (equivalente a la verificación de seed, ya que el seed corre en el startup)
- [ ] **Invariantes:** ninguna violada — sin migraciones, sin tocar `app/deps.py`/`app/auth.py`/`docker-compose*.yml`/`.env` (ver `.claude/CLAUDE.md` "Invariantes")
- [ ] **Scope:** `git diff --name-only develop` muestra exactamente los 6 archivos de 3.2, nada más
- [ ] **Limpieza:** sin archivos temporales (`_tmp`, `_test`, `_draft`, `_vN`)

### 7.2 Script de verificación completa

```bash
#!/bin/bash
# specs/agent-tools-p0-p1/verify.sh
set -e
echo "=== Pre-check: tests before changes ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit 2>&1 | tee /tmp/pre-tests.log
echo "=== Post-check: all unit tests ==="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
echo "=== Tool registry sanity ==="
python3 -c "
from app.agent.tools import TOOL_REGISTRY, tool_schemas
assert len(tool_schemas()) == 33
for name in ('dead_stock','inventory_turnover','profit_by_category','profit_by_location'):
    assert name in TOOL_REGISTRY, name
print('OK — 33 tools, 4 nuevas registradas')
"
echo "=== Scope check ==="
git diff --name-only develop
echo "=== ALL CHECKS PASSED ==="
```

### 7.3 Commits esperados

```
feat(agent): add dead_stock and inventory_turnover tools
feat(agent): add profit_by_category and profit_by_location tools
feat(agent): add optional location filter to sales_by_product, profit_by_product, daily_sales_series
docs(agent): update system prompt rules for new tools
test(agent): add unit tests for 4 new tools and location filter
chore(release): bump version 0.4.2 → 0.4.3
docs: update CHANGELOG for 0.4.3
```

---

> Generado por: SDD Lead (Phase 4 — PAE Assembly)
> Fuentes: `specs/agent-tools-p0-p1/{requirements,design,tasks}.md`

---

**PAE complete at `specs/agent-tools-p0-p1/PAE-agent-tools-p0-p1.md`.**
**11 task envelopes ready for OpenCode + Qwen local (GLM fallback).**
**Awaiting approval before any implementation begins.**
