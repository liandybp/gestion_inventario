# Design — Tools P0/P1 del copiloto IA

## 2.1 Codebase Grounding

Archivos existentes verificados leyendo el repo real (no se asume ninguna firma):

| Archivo | Rol | Firmas relevantes verificadas |
|---|---|---|
| `app/agent/tools.py` | MODIFY | `@tool(name, description, params)` decorator (línea 34); `TOOL_REGISTRY` dict; helpers `_iso(value)`, `_parse_end(value)` (líneas 15-31); patrón dual labels/values+columns/rows en `sales_by_product` (94-110) y `profit_by_product` (263-285) |
| `app/agent/prompts.py` | MODIFY | `SYSTEM_PROMPT_TEMPLATE` (líneas 12-62), reglas de selección de tool en bullets 20-56 |
| `app/services/inventory_service.py` | REFERENCE | `dead_stock(self, days: int = 90, now: Optional[datetime] = None) -> list[dict]` (línea 1269), retorna `[{"sku": str, "name": str, "value": float}]`; `inventory_turnover_days(self, monthly_cogs: float) -> Optional[float]` (línea 1292) |
| `app/services/reporting_service.py` | REFERENCE | `monthly_profit_report(self, now=None, location_id=None, *, start=None, end=None) -> tuple[dict, list[dict]]` (línea 465), `summary["cogs_total"]`; `monthly_profit_items_report(self, now=None, *, start=None, end=None, location_id=None) -> tuple[dict, list[dict]]` (línea 577), cada item trae `sku, name, category, location_code, location_name, qty, cogs, sales, profit, margin_pct` |
| `app/models.py` | REFERENCE | `Location(id, business_id, code, name, ...)` (línea 107), `code` es `unique=True` global |
| `app/agent/graph.py` | REFERENCE | `_execute_node` (línea 124) ya envuelve cada llamada a tool en `try/except HTTPException` + `except Exception`, degradando a `state["error"]` sin romper el turno — **no se añade manejo de errores nuevo, se reusa este contrato** |
| `app/agent/widgets.py` | REFERENCE | payload-agnóstico (`json.dumps` de lo que la tool devuelva) — sin cambios necesarios |
| `tests/unit/test_agent_tools.py` | MODIFY | `test_tool_schemas_count` hardcodea `len(tool_schemas()) == 29` (línea 9) — debe actualizarse a 33 |
| `VERSION` / `.bumpversion.cfg` | MODIFY | `current_version = 0.4.2` → next patch `0.4.3` |
| `CHANGELOG.md` | MODIFY | sección `## Unreleased` existente (no se reordena histórico, solo se añade sección nueva) |

**No se toca:** `app/models.py` (sin migraciones — esta feature es 100% read-path sobre
datos existentes), `app/agent/widgets.py`, `app/agent/runner.py`, `app/agent/graph.py`,
ningún router, ninguna plantilla Jinja/HTMX.

## 2.2 Architecture Approach

Cuatro tools nuevas + 3 tools existentes extendidas con un parámetro opcional, todas
dentro de `app/agent/tools.py`, siguiendo el patrón ya establecido en el archivo: cada
tool es una función decorada con `@tool(name, description, params_schema)` que recibe
`(service: InventoryService, args: dict)` y devuelve un dict con alguna combinación de
`value/label/currency` (KPI), `labels/values` (chart) y `columns/rows` (tabla).

Ninguna tool nueva escribe SQL adicional más allá de lo estrictamente necesario:
- `dead_stock` e `inventory_turnover` **no tienen SQL propia** — son wrappers finos sobre
  métodos de `InventoryService` que ya existen y ya filtran por `business_id`
  internamente (el service se instancia con `business_id` en `_execute_node`).
- `profit_by_category` y `profit_by_location` reusan `monthly_profit_items_report`
  (ya trae `category`/`location_name` por línea) y agregan client-side en Python con un
  simple `dict` acumulador — mismo enfoque que ya usa `dividends_report` (línea 302-317)
  y `extractions_by_party` (320-333), que agregan diccionarios devueltos por el service
  sin SQL adicional en la tool.
- El filtro `location` de REQ-5 se resuelve con **una única query de solo lectura nueva**
  (helper `_resolve_location_id`, ver 2.4), reusada por las 3 tools que lo necesitan.

**Invariantes que el diseño preserva:**
- Multi-tenant: toda query nueva filtra por `service.business_id` (igual que
  `sales_by_location`, `purchases_by_product`, etc. — patrón `True if service.business_id
  is None else (Model.business_id == service.business_id)`).
- Contrato de tool: cada tool sigue devolviendo un dict plano JSON-serializable (ya
  serializado por `widgets.py` sin cambios).
- Contrato de error: ninguna tool nueva atrapa excepciones que no atrapaban las
  existentes — se deja que `_execute_node` las capture (invariante ya probada).

## 2.3 Data Model

**Ninguno.** No se crean ni modifican columnas, tablas ni índices. Todas las tools leen
datos ya persistidos vía métodos de servicio existentes. No aplica migración ni rollback
de esquema.

## 2.4 Tool Contracts (equivalente a "API Contracts" para este proyecto — no hay HTTP nuevo)

### Tool: `dead_stock`
```
Params: {"days": integer (opcional, default 90)}
Return:
  {
    "labels": [str, ...],       # nombre de producto
    "values": [float, ...],     # valor inmovilizado
    "columns": ["SKU", "Nombre", "Valor inmovilizado"],
    "rows": [[sku, name, value], ...]   # ordenado desc por value
  }
```
**Multi-tenant:** heredado de `InventoryService.dead_stock`, que ya filtra por
`self._business_id` internamente (verificado línea 1279: `InventoryLot.business_id ==
self._business_id`).

### Tool: `inventory_turnover`
```
Params: {"start": str ISO (opcional), "end": str ISO (opcional)}
Return: {"value": float, "label": Optional[str], "currency": false}
```
**Sin datos suficientes (cogs_total == 0):** `{"value": 0.0, "label": "Sin datos
suficientes", "currency": false}` — nunca `None` (rompería el widget kpi).

### Tool: `profit_by_category`
```
Params: {"start": str ISO (opc.), "end": str ISO (opc.), "limit": integer (opc.)}
Return:
  {
    "labels": [str, ...],   # categoría (o "Sin categoría")
    "values": [float, ...], # ganancia
    "columns": ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
    "rows": [[category, qty, sales, cogs, profit, margin_pct], ...]  # desc por profit
  }
```

### Tool: `profit_by_location`
```
Params: {"start": str ISO (opc.), "end": str ISO (opc.)}
Return:
  {
    "labels": [str, ...],   # ubicación
    "values": [float, ...], # ganancia
    "columns": ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
    "rows": [[location, qty, sales, cogs, profit, margin_pct], ...]  # desc por profit
  }
```

### Extensión: `sales_by_product`, `profit_by_product`, `daily_sales_series`
```
Nuevo param opcional: {"location": str}   # código o nombre exacto (case-insensitive)
Si se resuelve: se pasa location_id al service method correspondiente (ya soportado).
Si NO se resuelve: HTTPException(404, "Ubicación no encontrada: <valor>")
Si no se pasa "location": comportamiento idéntico al actual (sin filtro).
```

## 2.5 Security Review

| Check | Answer |
|-------|--------|
| **New auth boundary?** | No. El copiloto ya está protegido por `require_active_business_id` a nivel de router (`ui_agent.py`); las tools nuevas no introducen ningún endpoint ni cambian quién puede llamarlas. |
| **New sensitive data?** | No. Mismos datos (ventas/costos/stock) que las tools existentes ya exponen agregados. |
| **Input validation?** | `days`/`limit` se castean con `int(...)` igual que las tools existentes (`purchases_by_product`, `expenses_list`); `location` se compara contra `Location.code`/`Location.name` filtrados por `business_id` — no hay concatenación de SQL (SQLAlchemy `select().where()` parametrizado, igual que el resto del archivo). |
| **New attack surface?** | Ninguna — no hay input de usuario final libre nuevo más allá de lo que el LLM ya construye para las tools existentes (fechas ISO, enteros, ahora un string de ubicación comparado por igualdad, no por LIKE con wildcards de usuario). |
| **Secrets needed?** | Ninguno. |
| **OWASP risks?** | Cross-tenant data leak (mitigado: todo filtro nuevo incluye `business_id`, verificado explícitamente en cada task envelope de Fase 3). |

## 2.6 Trade-offs

| Decisión | Elegido | Alternativa | Por qué no la alternativa |
|---|---|---|---|
| D1 — Resolución de `location` | Query de solo lectura nueva (`_resolve_location_id`), 404 si no existe | Reusar `InventoryService._location_id_for_code(code)` / `LocationRepository.location_id_for_code(auto_create=True)` | Ese método **autocrea** una ubicación si el código no existe (línea 50-79 de `location_repository.py`). En una tool de solo lectura invocada por un LLM, un código mal formado o alucinado terminaría **creando ubicaciones fantasma** en la BD. Se prefiere fallar con 404 (ya manejado con gracia por `_execute_node`) a mutar estado desde una tool de lectura. |
| D2 — Agregación de categoría/ubicación | Reusar `monthly_profit_items_report` y agregar en Python dentro de la tool | Nueva query SQL con `GROUP BY category` / `GROUP BY location_id` en `ReportingService` | El método ya trae exactamente los campos necesarios (`category`, `location_name`, `sales`, `cogs`, `profit`) por línea; añadir dos queries SQL nuevas al service duplicaría lógica de negocio (cálculo de `profit`/`margin_pct` ya resuelto ahí) por un ahorro marginal de CPU en agregación Python sobre, como mucho, unos cientos de líneas de venta al mes. Consistente con cómo ya se agregan `dividends_report`/`extractions_by_party`. |
| D3 — `inventory_turnover` cuando COGS=0 | Devolver `{"value": 0.0, "label": "Sin datos suficientes"}` | Propagar `None` tal cual devuelve el service | Un widget `kpi` con `value: None` no es un contrato válido para el resto de tools (todas devuelven `float` en `value`); se normaliza en el borde de la tool, no en el service (el service mantiene su contrato actual, usado también fuera del agente). |
| D4 — Categoría/ubicación vacía | Agrupar bajo `"Sin categoría"` / `"Sin ubicación"` | Descartar esas líneas del agregado | Descartar perdería ganancia real del período (el summary ya no cuadraría con `monthly_profit_summary`); agrupar bajo una etiqueta explícita mantiene la suma total consistente — ver 2.9 dimensión 6. |

## 2.7 Migration & Rollback Strategy

No aplica migración de datos. Rollback = revertir el commit (no hay estado persistente
nuevo, las tools son puro read-path).

## 2.9 Pre-mortem contra las 10 dimensiones de auditoría (obligatorio)

| Dimensión | Resolución en este diseño |
|---|---|
| **1. SoC/SRP** | Las 4 tools nuevas tienen un único consumidor real: `TOOL_REGISTRY`/`_execute_node` (ya existente, sin wiring nuevo que crear — a diferencia de una capa repo/service nueva, aquí no hay capa "por si acaso": la función decorada YA es su propio punto de entrada, se registra sola al importar el módulo). |
| **2. Cohesión/Acoplamiento** | `profit_by_category` y `profit_by_location` llaman **una sola vez** a `monthly_profit_items_report` por invocación (no se llama dos veces el mismo método pesado dentro del mismo turno) — igual que `profit_by_product` llama una sola vez a `monthly_profit_report`. `_resolve_location_id` se define una sola vez (helper compartido) y se reusa en las 3 tools de REQ-5, no se copia 3 veces. |
| **3. Capas/Límites** | Dirección de dependencia sin cambios: `tools.py` → `services/*.py` → `models.py`. Las tools no acceden a repositorios directamente salvo el propio patrón ya usado en el archivo para queries puntuales (`sales_by_location`, `purchases_by_product` ya hacen `select()` directo con `service.db`); `_resolve_location_id` sigue ese mismo patrón exacto, no introduce una nueva capa. |
| **5. Robustez/Resiliencia** | Las 6 funciones nuevas/modificadas (`dead_stock`, `inventory_turnover`, `profit_by_category`, `profit_by_location`, y los 3 wrappers de `location`) **no** llevan try/except propio — **decisión explícita y consistente**: ninguna de las 29 tools existentes en `tools.py` tiene try/except individual; todas delegan el manejo de errores a `_execute_node` (verificado línea 124-154 de `graph.py`). Añadir try/except solo a las tools nuevas rompería la consistencia (el patrón real del 100% del archivo es "deja que la excepción suba"). Los `HTTPException(404, ...)` de `_resolve_location_id` siguen exactamente el mismo patrón que `_get_product` (línea 210-214 de `inventory_service.py`, ya usado indirectamente por la tool `stock`). |
| **6. Consistencia Transaccional** | `profit_by_category`/`profit_by_location` y `inventory_turnover` deben usar el **mismo rango de fechas** en todos los términos de su cálculo: el `start`/`end` que el usuario pida (o el mes actual por default, igual que `monthly_profit_summary`) se pasa completo a `monthly_profit_items_report`/`monthly_profit_report` — nunca se mezcla "este mes" en un término con "histórico" en otro. `inventory_turnover` usa `cogs_total` del MISMO rango que se le pidió a la tool (no el mes actual si el usuario pidió otro rango) — esto se especifica literalmente en el Task Envelope T2 de Fase 3. |

## 2.10 Optional Delegation

Ninguna — el diseño cabe completo dentro del SDD Lead (reutiliza contratos ya
existentes, no requiere modelado de datos nuevo, ni threat model nuevo, ni >5 endpoints).
