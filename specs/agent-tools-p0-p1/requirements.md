# Requirements — Tools P0/P1 del copiloto IA

> Fuente: auditoría de cobertura de tools realizada en sesión de Claude Code (2026-08-17),
> a raíz de un bug reportado por el usuario donde `profit_by_product` no soportaba `limit`
> ni ordenaba por ganancia (ya corregido en esa misma sesión, fuera de este PAE). La
> auditoría cruzó las 29 tools existentes (`app/agent/tools.py`) contra las entidades del
> modelo (`app/models.py`) y los métodos ya escritos en `app/services/*.py`, e identificó
> huecos de cobertura priorizados P0 (reusan lógica de servicio ya existente, cero SQL
> nueva) y P1 (agregación nueva sobre datos que el service ya trae).

## 1.1 Functional Requirements

### REQ-1: Tool `dead_stock` — productos sin venta reciente
**Priority:** Must Have

Expone `InventoryService.dead_stock(days, now)` (`app/services/inventory_service.py:1269`),
que ya calcula productos con lotes remanentes (`qty_remaining > 0`) recibidos antes del
corte y sin ninguna venta desde entonces, con su valor inmovilizado a costo.

```gherkin
### REQ-1: Copiloto responde "qué productos no se han vendido"
**Scenario: Usuario pide productos sin venta con horizonte default**
  GIVEN el copiloto está activo para un negocio con productos y movimientos
  WHEN el LLM invoca la tool "dead_stock" sin argumentos
  THEN la tool usa days=90 por defecto
  AND devuelve columns ["SKU", "Nombre", "Valor inmovilizado"] y rows con los productos
      sin venta en los últimos 90 días, ordenados de mayor a menor valor inmovilizado
  AND devuelve también labels/values (nombre, valor) para permitir un widget de tipo bar

**Scenario: Usuario especifica el horizonte**
  GIVEN el mismo contexto
  WHEN el LLM invoca "dead_stock" con {"days": 30}
  THEN la tool usa days=30 como corte

**Scenario: Sin productos estancados**
  GIVEN un negocio donde todos los productos tuvieron venta reciente
  WHEN se invoca "dead_stock"
  THEN devuelve rows=[] y labels=[]/values=[] (no excepción)
```

### REQ-2: Tool `inventory_turnover` — rotación de inventario en días
**Priority:** Must Have

Expone `InventoryService.inventory_turnover_days(monthly_cogs)`
(`app/services/inventory_service.py:1292`). El `monthly_cogs` se obtiene internamente
llamando a `ReportingService.monthly_profit_report()` (ya usado por `monthly_profit_summary`,
`app/agent/tools.py:239-260`) y tomando `summary["cogs_total"]` del rango pedido (o del
mes actual si no se pasa rango — mismo default que `monthly_profit_summary`).

```gherkin
### REQ-2: Copiloto responde "cada cuánto rota mi inventario"
**Scenario: Rotación calculable**
  GIVEN un negocio con costo de ventas (COGS) > 0 en el rango pedido
  WHEN el LLM invoca "inventory_turnover" con start/end opcionales
  THEN la tool calcula cogs_total del rango vía monthly_profit_report
  AND devuelve {"value": <días>, "label": None, "currency": False}

**Scenario: COGS es cero (sin ventas en el rango)**
  GIVEN cogs_total == 0 para el rango pedido
  WHEN se invoca "inventory_turnover"
  THEN inventory_turnover_days devuelve None (comportamiento ya existente del método)
  AND la tool devuelve {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    en vez de propagar None al widget (un widget kpi con value=None rompe el render)
```

### REQ-3: Tool `profit_by_category` — ganancia agrupada por categoría de producto
**Priority:** Must Have

Agrega los items de `ReportingService.monthly_profit_items_report`
(`app/services/reporting_service.py:577`, ya trae `category`, `sales`, `cogs`, `profit`
por línea de venta) por `Product.category`, sumando sales/cogs/profit por categoría,
ordenado de mayor a menor ganancia. Sigue el mismo patrón de respuesta dual
(labels/values + columns/rows) que `profit_by_product` (`app/agent/tools.py:263-285`).

```gherkin
### REQ-3: Copiloto responde "qué categoría genera más ganancia"
**Scenario: Top categorías por ganancia**
  GIVEN ventas registradas en 3 categorías distintas en el rango pedido
  WHEN el LLM invoca "profit_by_category" con start/end y limit=2
  THEN la tool agrupa por category, suma profit, ordena desc
  AND devuelve solo las 2 categorías con mayor ganancia
  AND columns == ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]

**Scenario: Producto sin categoría asignada**
  GIVEN un producto vendido con Product.category == NULL
  WHEN se invoca "profit_by_category"
  THEN esas líneas se agrupan bajo la etiqueta "Sin categoría" (no se descartan,
    no se agrupan como None/null crudo)
```

### REQ-4: Tool `profit_by_location` — ganancia agrupada por ubicación/POS
**Priority:** Must Have

Agrega los mismos items de `monthly_profit_items_report` por `location_code`/
`location_name`, sumando sales/cogs/profit por ubicación. Complementa a
`sales_by_location` (`app/agent/tools.py:180-216`), que solo da ingresos, no ganancia.

```gherkin
### REQ-4: Copiloto responde "qué ubicación genera más ganancia"
**Scenario: Ganancia por ubicación**
  GIVEN ventas en CENTRAL y POS1 en el rango pedido
  WHEN el LLM invoca "profit_by_location"
  THEN la tool agrupa por location_name (fallback a location_code, fallback a
    "Sin ubicación" si ambos vacíos), suma profit, ordena desc
  AND columns == ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]
```

### REQ-5: Filtro opcional de ubicación en tools existentes
**Priority:** Should Have

`ReportingService.sales_by_product`, `monthly_profit_report` y `daily_sales_series`
YA aceptan `location_id: Optional[int]` (verificado en el service), pero las tools del
agente (`sales_by_product`, `profit_by_product`, `daily_sales_series`,
`app/agent/tools.py`) no lo exponen. Se añade un parámetro `location` (string: código o
nombre de ubicación) a las tres, resuelto a `location_id` vía una consulta de solo
lectura contra `Location` (nunca autocrear — ver design.md D1).

```gherkin
### REQ-5: Copiloto responde "ventas de este producto en la central"
**Scenario: Location code válido**
  GIVEN una ubicación con code="CENTRAL" existe para el negocio activo
  WHEN el LLM invoca "sales_by_product" con {"start":..., "end":..., "location": "CENTRAL"}
  THEN la tool resuelve location_id y filtra el reporte a esa ubicación únicamente

**Scenario: Location no existe para este negocio**
  GIVEN "location": "POS_INEXISTENTE" no coincide con ningún Location.code ni
    Location.name de este business_id
  WHEN se invoca cualquiera de las 3 tools con ese argumento
  THEN la tool lanza HTTPException(404, "Ubicación no encontrada: POS_INEXISTENTE")
  AND el grafo del agente (_execute_node, ya existente) captura la excepción y
    degrada a un mensaje de error en el chat, sin romper el turno completo
    (comportamiento ya probado para SKU inexistente — mismo patrón, sin código nuevo
    de manejo de errores)

**Scenario: Sin argumento location (comportamiento actual, sin cambios)**
  GIVEN el LLM no pasa "location"
  WHEN se invoca cualquiera de las 3 tools
  THEN el comportamiento es idéntico al actual (todas las ubicaciones del negocio)
```

## 1.2 MoSCoW Prioritization

| REQ | Título | Prioridad |
|-----|--------|-----------|
| REQ-1 | `dead_stock` | Must Have |
| REQ-2 | `inventory_turnover` | Must Have |
| REQ-3 | `profit_by_category` | Must Have |
| REQ-4 | `profit_by_location` | Must Have |
| REQ-5 | filtro `location` en 3 tools existentes | Should Have |

REQ-5 es "Should Have" (no "Must") porque el pedido original del usuario fue "P0 y P1";
P0/P1 de la auditoría cubren REQ-1..4 tal cual. El punto 5 del pedido de esta ronda
(filtro de ubicación) estaba catalogado como P1 en la auditoría original también — se
incluye en el mismo PAE porque reusa exactamente el mismo dato ya expuesto
(`location_id` en los service methods) y el usuario pidió explícitamente "implementar
P0 y P1", que incluía este punto. Se marca Should Have (no Must) únicamente porque, a
diferencia de REQ-1..4, su ausencia no deja una pregunta de negocio completamente sin
respuesta (el usuario aún puede pedir el desglose sin filtrar y leerlo manualmente).

## 1.3 Non-Functional Requirements (NFRs)

| Category | Question | Answer |
|----------|----------|--------|
| **Performance** | ¿Tiempo de respuesta esperado? | Mismo orden que las tools existentes (`profit_by_product`, `sales_by_location`): una query agregada sobre `InventoryMovement`/`MovementAllocation` con los índices compuestos ya existentes (`ix_inventory_movements_business_type_date`, H-053). Sin negocios de miles de líneas/mes esto es sub-100ms. |
| **Security** | ¿Nuevos límites de auth? ¿Datos sensibles nuevos? | No. Mismo patrón multi-tenant que todas las tools existentes: filtro obligatorio por `business_id` en cada query nueva. Ninguna tool nueva expone datos de otro negocio ni PII adicional. |
| **Scalability** | ¿Volumen esperado? | Igual que el resto del copiloto — un negocio, consultas puntuales bajo demanda del chat, no hay fan-out. |
| **Observability** | ¿Logs/métricas nuevos? | No se añaden — las tools existentes no loguean por tool individual (el circuit breaker y el runner ya loguean a nivel de turno del agente); consistente no añadir logging ad-hoc solo a las tools nuevas. |
| **Reliability** | ¿Tasa de error aceptable / recuperación? | Todo error de tool (SKU/ubicación no encontrada, args mal formados) ya degrada de forma controlada vía `_execute_node` (`app/agent/graph.py:124-154`, try/except existente) — las tools nuevas deben seguir exactamente ese contrato (lanzar `HTTPException` o dejar que un `ValueError` de parseo de fecha se propague, nunca `except: pass`). |

## 1.4 Out of Scope

- **Tests e2e nuevos (Playwright).** Esta feature no añade superficie de UI nueva: el
  chat, el flujo de creación de widgets y el render de gráfica/tabla son exactamente
  los mismos que ya ejercitan los e2e existentes del copiloto (`tests/e2e/`). Lo único
  que cambia es qué datos puede pedir el LLM — eso se verifica con tests unitarios de
  la tool (`tests/unit/test_agent_tools.py`), igual que las 29 tools actuales, ninguna
  de las cuales tiene un e2e dedicado por tool. Añadir un e2e por tool nueva sería
  redundante con el e2e genérico "el copiloto responde con un widget" que ya existe.
- **Margen por cliente** (candidata P2 de la auditoría) — requiere verificar si
  `SalesDocument` está enlazado de forma confiable a `InventoryMovement`/COGS; no
  investigado en esta ronda.
- **Ventas por vendedor/usuario** (candidata P2) — requeriría correlación con
  `AuditLog` o añadir `created_by` a `InventoryMovement`; cambio de modelo, fuera de
  alcance de "reusar datos existentes".
- **Compras/ventas por proveedor** — no existe la entidad `Supplier` en el modelo
  (roadmap de evolución, `docs/roadmap/2026-08-17-oportunidades-evolucion.md`).
- **Resolución fuzzy de nombres de ubicación** (typos, acentos) — REQ-5 resuelve por
  código exacto o nombre exacto (case-insensitive); no se implementa fuzzy matching.
- **Cambiar `sales_by_location` para que también devuelva ganancia** — se deja como
  tool separada (`profit_by_location`, REQ-4) para no romper el contrato/columns de la
  tool existente ni sus tests (`test_sales_by_location_dual_format`).

## 1.5 Dependencies

- Ninguna dependencia externa nueva (no se añaden paquetes a `requirements.txt`).
- Depende de que `ReportingService.monthly_profit_items_report` y
  `InventoryService.dead_stock`/`inventory_turnover_days` sigan con las firmas actuales
  (verificadas en esta sesión, ver design.md 2.1).
- No depende de ningún otro PAE en curso.

## 1.6 Open Questions

Ninguna. Todos los requerimientos están fundamentados en métodos de servicio ya
existentes y verificados leyendo el código real (`app/services/inventory_service.py`,
`app/services/reporting_service.py`). Procediendo a Phase 2.
