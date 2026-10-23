# Design — Copiloto: ventas por punto de venta mes a mes

> SDD Phase 2 · PAE-015 · 2026-10-18 · Fuente: `requirements.md` (REQ-1..6)
> Regenerado 2026-10-23 sobre `feature/frontend-modular` (tras PAE-017: gráfico en `components/widget-chart.js`;
> release 0.5.0 sin bump; series por id de ubicación, D8; multi-serie solo bar/line, D9)

## 2.1 Codebase grounding

| Archivo | Operación | Rol |
|---------|-----------|-----|
| `app/services/reporting_service.py` | MODIFY | Nuevo método `sales_by_location_monthly(months)` junto a `purchases_by_month` (mismo patrón `_month_series` + agregación en Python) — REQ-1 |
| `app/agent/tools.py` | MODIFY | Nueva tool `sales_by_location_monthly` justo después de `sales_by_location` — REQ-2 |
| `app/static/js/components/widget-chart.js` | MODIFY | `chartConfig`: soporte `payload.datasets` (multi-serie, solo bar/line) + leyenda — REQ-3 |
| `app/agent/prompts.py` | MODIFY | Reglas: tool nueva, "últimos N meses", honestidad — REQ-4, REQ-5 |
| `app/agent/graph.py` | MODIFY | `_synth_title()` para el widget sintetizado (REQ-6) + aviso en `_narrate_node` (REQ-5) |
| `tests/unit/test_reporting_service.py` | MODIFY | Tests REQ-1 |
| `tests/unit/test_agent_tools.py` | MODIFY | Conteo 33→34 + test payload REQ-2 |
| `tests/unit/test_agent_prompts.py` | MODIFY | Tests REQ-4/REQ-5 (prompt) |
| `tests/unit/test_agent_graph.py` | MODIFY | Tests REQ-5 (narrate) y REQ-6 |
| `tests/eval/test_agent_tool_selection_eval.py` | MODIFY | Golden case REQ-4 (LLM real, a demanda) |
| `tests/e2e/test_agent_multiseries_chart_e2e.py` | CREATE | Playwright REQ-3 |
| `CHANGELOG.md` | MODIFY | Viñetas bajo `## 0.5.0 (sin publicar …)`; sin tocar `VERSION` ni `.bumpversion.cfg` |
| `CLAUDE.md`, `.claude/CLAUDE.md` | MODIFY (Claude Code, al cerrar) | Estado actual + conteo de tools vigente (34) |
| `app/templates/partials/widget_chart.html` | REFERENCE | Sin cambios: solo declara el `<canvas data-js-widget-chart>` |
| `app/models.py` (`Location`, `InventoryMovement`, `DashboardWidget`) | REFERENCE | Sin cambios |
| `app/agent/widgets.py`, `app/routers/ui_agent.py`, `widget_grid.html`, `widget_table.html` | REFERENCE | Sin cambios: la tabla ya pinta `columns`/`rows` genéricos |
| `tests/e2e/conftest.py` | REFERENCE | Fixture `central_server` → `(base_url, db_path)` |

## 2.2 Enfoque

Flujo nuevo: `planner → sales_by_location_monthly(months=6) → ReportingService.sales_by_location_monthly
→ payload {labels, datasets, columns, rows} → widget bar → widget_chart.html (canvas) → widget-chart.js (N datasets)`.

- **Servicio (REQ-1):** ventana = `_month_series(months)` (ya existente: N meses calendario
  incluyendo el actual, `start` = día 1 del mes más antiguo, UTC). Una query:
  `InventoryMovement` (type `sale`, `business_id`, `movement_date >= start`) LEFT JOIN `Location`.
  Importe por fila = `abs(quantity) * coalesce(unit_price, 0)` (misma fórmula que
  `sales_by_location` y `profit_by_location` — las ventas se guardan con cantidad negativa).
  Agregación en Python por `(ubicación, 'YYYY-MM')`, igual que `purchases_by_month`.
  Se descartan las fechas cuyo mes no esté en `keys` (ventas con fecha futura).
  Orden de series: `Location.position` (NULL → 9999), luego nombre. Valores redondeados a 2 decimales.
  Nombre de la serie: `coalesce(Location.name, Location.code, "Sin ubicación")` (igual que `profit_by_location`).
- **Tool (REQ-2):** recibe `months` (default 6, acotado a 1..24 en el servicio). **No** recibe
  start/end: así el LLM no hace aritmética de fechas (causa C4).
- **Chart (REQ-3):** `multi = Array.isArray(payload.datasets) && payload.datasets.length > 0`.
  Si `multi`, un dataset por serie con `palette[i]`; si no, el dataset único actual sin cambios.
  Leyenda cartesiana: `display: multi`.
- **Prompt (REQ-4/5):** 3 reglas nuevas + una línea en "datos que condicionan el type".
- **Graph (REQ-5/6):** `_synth_title(tool)` = descripción hasta el primer `" ("` y el primer `"."`;
  `_narrate_node` añade un aviso genérico si `state["error"]` empieza por `"Tool "`.

**Invariantes que se preservan:**
- Multi-tenant: toda query filtra `InventoryMovement.business_id == self._business_id`.
- Un widget por turno (`_build_node` → `plan.widgets[:1]`), sin cambios.
- Contrato de `POST /ui/agent/chat` sin cambios.
- Widgets existentes (payload `values`) se dibujan igual que antes (rama `else` idéntica).
- `SYSTEM_PROMPT_TEMPLATE` usa `.format()`: el texto nuevo **no** lleva `{` ni `}`.

## 2.3 Data model

Sin cambios de esquema. Nueva forma de payload (JSON en `dashboard_widgets.params`):

```json
{
  "labels": ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"],
  "datasets": [
    {"label": "Punto de venta Playa", "data": [2690.5, 1292.5, 2741.9, 2001.0, 873.0, 2834.0]},
    {"label": "Taller Boyeros", "data": [980.5, 90.0, 545.4, 1453.5, 791.2, 152.5]},
    {"label": "Taller Cerro", "data": [967.5, 976.0, 971.0, 1181.7, 413.2, 0.0]}
  ],
  "columns": ["Mes", "Punto de venta Playa", "Taller Boyeros", "Taller Cerro"],
  "rows": [["2026-05", 2690.5, 980.5, 967.5], ["...", 0, 0, 0]]
}
```

**Migración:** ninguna. **Rollback:** revertir el commit; los widgets multi-serie ya guardados
se pintarían con 0 datasets (gráfico vacío, sin error) y se borran con ✕.

## 2.4 API contracts

Sin endpoints nuevos ni cambiados. Contrato interno nuevo:

```python
ReportingService.sales_by_location_monthly(self, months: int = 6) -> dict
# -> {"months": list[str], "series": list[{"location": str, "values": list[float]}]}

TOOL_REGISTRY["sales_by_location_monthly"]["fn"](service: InventoryService, args: dict) -> dict
# args: {"months": int (opcional, default 6)}
```

## 2.5 Security review

| Check | Respuesta |
|-------|-----------|
| Nueva frontera de auth | No — corre dentro del chat existente (admin/owner, `ensure_admin_or_owner`) |
| Datos sensibles nuevos | No — importes de venta ya visibles en otras tools |
| Validación de entrada | `months` → `int(...)` y acotado a `[1, 24]`; un `months` no numérico → `ValueError` capturado en `_execute_node` |
| Superficie de ataque | Nombres de ubicación al JS vía `tojson` (escapa `<`, `>`, `&`, `'`); Chart.js los pinta en `<canvas>` (sin HTML) |
| IDOR / multi-tenant | Filtro `business_id` obligatorio en la query (test de aislamiento en REQ-1) |
| Fuga de detalles internos | El aviso de REQ-5 es genérico, **no** incluye el texto de la excepción |
| Secretos | Ninguno |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|----------|---------|-------------|------------|
| D1 Parámetro de ventana | `months` (int) | `start`/`end` ISO | El LLM ya falló la aritmética (C4: 7 meses en vez de 6). `months` + `_month_series` es determinista |
| D2 Tool nueva vs ampliar `sales_by_location` | Tool nueva | Añadir `group_by_month` a `sales_by_location` | Cambiaría el payload de una tool existente según un flag → más ambigüedad para el modelo pequeño y rompe a quien espera `values` |
| D3 Agregación | Python (como `purchases_by_month`) | `date_trunc` en SQL | `date_trunc` no existe en SQLite (tests + desktop offline); el volumen es bajo |
| D4 Formato de payload | `datasets` (+ `columns`/`rows`) | Varios widgets, uno por ubicación | La regla de 1 widget por turno lo impide, y no permitiría comparar en la misma gráfica |
| D5 Ubicaciones sin ventas | Excluidas | Incluir todas con ceros | El usuario pidió las barras "en el mes que aplique"; Almacén Central no vende y sería una serie vacía |
| D6 Aviso de error de tool | Texto genérico fijo | Incluir `exc` | Puede filtrar detalles internos (SQL, rutas) |
| D7 Título sintetizado | Primer tramo de la descripción | Diccionario manual tool→título | 34 entradas que mantener a mano; la descripción ya existe y está en español |
| D8 Clave de las series | `Location.id` (etiqueta = nombre) | Nombre de la ubicación | `Location.name` no es único: dos POS con el mismo nombre se sumarían en una serie (auditoría 2026-10-23, H-506) |
| D9 Multi-serie por tipo | Solo `bar`/`line` | Cualquier tipo | En pie/doughnut cada serie tendría un solo color para todas sus porciones y `percentPlugin` solo mira el dataset 0 (H-509) |

## 2.7 Migración y rollback

Despliegue estándar (Gitea Actions CD, `develop` → dev, `main` → prod). Sin migración de datos.
Rollback = `git revert` del merge; sin estado persistente nuevo salvo widgets (borrables con ✕).

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría

| Dimensión | Riesgo detectado | Mitigación (texto literal en el envelope) |
|-----------|------------------|-------------------------------------------|
| 1. SoC/SRP | Método de servicio sin consumidor | T1a crea el método, T2 lo conecta a la tool en el mismo PAE. La tool solo formatea, la lógica vive en `ReportingService` |
| 2. Cohesión/Acoplamiento | — | Un solo cálculo por request (la tool llama al servicio una vez). Reutiliza `_month_series`, no duplica la lógica de meses |
| 3. Capas | — | `tools.py → ReportingService → ORM`, la misma dirección que `monthly_overview` |
| 4. Seguridad | XSS por nombre de ubicación | `tojson` + canvas (ver 2.5). `months` acotado |
| 5. Robustez | ¿Try/except en el método nuevo? | Mismo contrato que sus hermanos (`purchases_by_month`, `expenses_by_month`): sin try/except en el servicio. La frontera de errores es `_execute_node` (captura `Exception` por widget). Se añade un aviso visible al usuario (REQ-5). Consistente al 100% con los métodos de reporting existentes |
| 6. Consistencia | ¿Dos ubicaciones con el mismo nombre? | Series agrupadas por `Location.id` (D8); test en T1b |
| 6. Consistencia | ¿Misma ventana en todas las series? | Todas las series usan las mismas `keys` y el mismo `start`; los meses fuera de `keys` se descartan (fechas futuras). La fórmula del importe es idéntica a la de `sales_by_location` → el total de 6 meses cuadra con `sales_by_location(start=día 1 de hace 5 meses)` |
| 7. Observabilidad | — | `agent_plan` ya loguea tool y args |
| 8. Configuración | `24` hardcodeado | Constante de módulo `_MAX_MONTHS_WINDOW = 24` en `reporting_service.py` (límite técnico, no de negocio) |
| 9. Pruebas | Comportamiento del LLM no testeable en CI; sin test runner de JS | Unit (servicio, tool, prompt, graph) + e2e Playwright del render real (fija el JS, validado por mutación; guarda `js_errors` autouse) + golden case en eval (a demanda) + verificación manual con copia desechable de la BD de prod |
| 10. Escalabilidad | — | Ventana ≤ 24 meses, índice `(business_id, type, movement_date)` ya existente |

## 2.10 Delegación

Ninguna (alcance acotado, sin auth ni modelo de datos nuevo).
