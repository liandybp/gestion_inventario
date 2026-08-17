# Task Envelopes — Tools P0/P1 del copiloto IA

> 11 envelopes, secuenciales (mismo archivo principal `app/agent/tools.py` en T1-T7).
> Cada envelope es autocontenido: OpenCode no necesita leer `requirements.md` ni
> `design.md`, solo este archivo, un envelope a la vez.

---

### T1: Tool `dead_stock`

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
def dead_stock(self, days: int = 90, now: Optional[datetime] = None) -> list[dict]:
    """Ya filtra por business_id. Retorna [{"sku": str, "name": str, "value": float}]."""
    ...
```

#### Objective

Añadir la tool `dead_stock` al final de `app/agent/tools.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change** (el archivo termina en la función `coach_recommendations`)

```python
# BEFORE (últimas líneas del archivo, tal cual, sin nada después):
@tool("coach_recommendations", "Recomendaciones activas del coach.",
      {"type": "object", "properties": {}})
def coach_recommendations(service: InventoryService, args: dict) -> dict:
    coach = CoachService(service.db, service.business_id)
    recommendations = coach.recommendations()
    return {
        "columns": ["Síntoma", "Acción", "Impacto"],
        "rows": [[r["symptom"], r["action"], r.get("impact_text", "")] for r in recommendations],
    }

# AFTER (se añade la función nueva DESPUÉS, con dos líneas en blanco de separación,
# igual que el resto del archivo):
@tool("coach_recommendations", "Recomendaciones activas del coach.",
      {"type": "object", "properties": {}})
def coach_recommendations(service: InventoryService, args: dict) -> dict:
    coach = CoachService(service.db, service.business_id)
    recommendations = coach.recommendations()
    return {
        "columns": ["Síntoma", "Acción", "Impacto"],
        "rows": [[r["symptom"], r["action"], r.get("impact_text", "")] for r in recommendations],
    }


@tool("dead_stock", "Productos sin venta reciente y su valor inmovilizado a costo (days opcional, default 90).",
      {"type": "object", "properties": {"days": {"type": "integer"}}})
def dead_stock(service: InventoryService, args: dict) -> dict:
    days = int(args.get("days") or 90)
    items = sorted(service.dead_stock(days=days), key=lambda i: i["value"], reverse=True)
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["value"]) for i in items],
        "columns": ["SKU", "Nombre", "Valor inmovilizado"],
        "rows": [[i["sku"], i["name"], float(i["value"])] for i in items],
    }
```

**Imports needed:** ninguno (`InventoryService` ya está importado en el archivo).

**Edge cases (ya resueltos):**
- Sin productos estancados: `service.dead_stock()` devuelve `[]` → labels/values/rows quedan `[]`, sin excepción.
- `days` ausente: usa 90 vía `args.get("days") or 90`.

#### Verification

```bash
python3 -c "from app.agent.tools import TOOL_REGISTRY; assert 'dead_stock' in TOOL_REGISTRY; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — tool `dead_stock` añadida al final
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T2: Tool `inventory_turnover`

**Depends on:** T1 (misma zona del archivo — se añade justo después de `dead_stock`)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/inventory_service.py — YA EXISTE, NO MODIFICAR
def inventory_turnover_days(self, monthly_cogs: float) -> Optional[float]:
    """Devuelve None si monthly_cogs <= 0."""
    ...

# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def monthly_profit_report(
    self, now=None, location_id=None, *, start=None, end=None
) -> tuple[dict, list[dict]]:
    """summary dict incluye summary["cogs_total"]: float."""
    ...

# app/agent/tools.py — YA EXISTE, patrón de fechas a reusar
def _iso(value: str) -> datetime: ...
def _parse_end(value: str) -> datetime: ...
```

#### Objective

Añadir la tool `inventory_turnover` en `app/agent/tools.py`, justo después de `dead_stock` (añadida en T1).

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change**

```python
# BEFORE (función dead_stock completa, tal cual quedó tras T1, es lo ÚLTIMO del archivo):
@tool("dead_stock", "Productos sin venta reciente y su valor inmovilizado a costo (days opcional, default 90).",
      {"type": "object", "properties": {"days": {"type": "integer"}}})
def dead_stock(service: InventoryService, args: dict) -> dict:
    days = int(args.get("days") or 90)
    items = sorted(service.dead_stock(days=days), key=lambda i: i["value"], reverse=True)
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["value"]) for i in items],
        "columns": ["SKU", "Nombre", "Valor inmovilizado"],
        "rows": [[i["sku"], i["name"], float(i["value"])] for i in items],
    }

# AFTER (se añade la función nueva después):
@tool("dead_stock", "Productos sin venta reciente y su valor inmovilizado a costo (days opcional, default 90).",
      {"type": "object", "properties": {"days": {"type": "integer"}}})
def dead_stock(service: InventoryService, args: dict) -> dict:
    days = int(args.get("days") or 90)
    items = sorted(service.dead_stock(days=days), key=lambda i: i["value"], reverse=True)
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["value"]) for i in items],
        "columns": ["SKU", "Nombre", "Valor inmovilizado"],
        "rows": [[i["sku"], i["name"], float(i["value"])] for i in items],
    }


@tool("inventory_turnover", "Rotación de inventario en días, según el costo de ventas del período "
      "(start/end ISO opcionales, default mes actual).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def inventory_turnover(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    summary, _items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    turnover = service.inventory_turnover_days(float(summary["cogs_total"]))
    if turnover is None:
        return {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    return {"value": round(float(turnover), 1), "label": None, "currency": False}
```

**Imports needed:** ninguno (`ReportingService`, `_iso`, `_parse_end` ya existen en el archivo).

**Edge cases (ya resueltos):**
- `cogs_total == 0` (sin ventas en el rango): `inventory_turnover_days` devuelve `None` → la tool devuelve `{"value": 0.0, "label": "Sin datos suficientes", "currency": False}`, NUNCA `None` crudo (rompería el widget kpi).
- El rango usado para `cogs_total` es SIEMPRE el mismo `start`/`end` que recibió la tool — no mezclar con "mes actual" si el usuario pidió otro rango.

#### Verification

```bash
python3 -c "from app.agent.tools import TOOL_REGISTRY; assert 'inventory_turnover' in TOOL_REGISTRY; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — tool `inventory_turnover` añadida después de `dead_stock`
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T3: Tool `profit_by_category`

**Depends on:** T2
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def monthly_profit_items_report(
    self, now=None, *, start=None, end=None, location_id=None
) -> tuple[dict, list[dict]]:
    """Cada item: {"sku","name","category"(Optional[str]),"location_code","location_name",
    "qty","sales","cogs","profit","margin_pct", ...}. category puede ser None."""
    ...
```

#### Objective

Añadir la tool `profit_by_category` en `app/agent/tools.py`, después de `inventory_turnover` (T2).

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change**

```python
# BEFORE (función inventory_turnover completa, tal cual quedó tras T2, es lo ÚLTIMO del archivo):
@tool("inventory_turnover", "Rotación de inventario en días, según el costo de ventas del período "
      "(start/end ISO opcionales, default mes actual).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def inventory_turnover(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    summary, _items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    turnover = service.inventory_turnover_days(float(summary["cogs_total"]))
    if turnover is None:
        return {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    return {"value": round(float(turnover), 1), "label": None, "currency": False}

# AFTER (se añade la función nueva después):
@tool("inventory_turnover", "Rotación de inventario en días, según el costo de ventas del período "
      "(start/end ISO opcionales, default mes actual).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def inventory_turnover(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    summary, _items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    turnover = service.inventory_turnover_days(float(summary["cogs_total"]))
    if turnover is None:
        return {"value": 0.0, "label": "Sin datos suficientes", "currency": False}
    return {"value": round(float(turnover), 1), "label": None, "currency": False}


@tool("profit_by_category", "Ganancia y margen agrupados por categoría de producto en un período, "
      "ordenado de mayor a menor ganancia (start/end ISO opcionales; limit opcional = top N).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def profit_by_category(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    _summary, items = reporting.monthly_profit_items_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    grouped: dict[str, dict[str, float]] = {}
    for i in items:
        key = (i["category"] or "").strip() or "Sin categoría"
        g = grouped.setdefault(key, {"qty": 0.0, "sales": 0.0, "cogs": 0.0, "profit": 0.0})
        g["qty"] += float(i["qty"])
        g["sales"] += float(i["sales"])
        g["cogs"] += float(i["cogs"])
        g["profit"] += float(i["profit"])
    rows = [
        [name, g["qty"], g["sales"], g["cogs"], g["profit"],
         round((g["profit"] / g["sales"] * 100.0) if g["sales"] else 0.0, 1)]
        for name, g in grouped.items()
    ]
    rows.sort(key=lambda r: r[4], reverse=True)
    limit = args.get("limit")
    if limit:
        rows = rows[: int(limit)]
    return {
        "labels": [r[0] for r in rows],
        "values": [r[4] for r in rows],
        "columns": ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": rows,
    }
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `category` es `None`/vacío: se agrupa bajo `"Sin categoría"` (nunca se descarta la línea — el total de ganancia debe seguir cuadrando con `monthly_profit_summary`).
- Sin ventas en el rango: `items == []` → `rows == []`.

#### Verification

```bash
python3 -c "from app.agent.tools import TOOL_REGISTRY; assert 'profit_by_category' in TOOL_REGISTRY; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — tool `profit_by_category` añadida
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T4: Tool `profit_by_location`

**Depends on:** T3
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR (mismo método que T3)
def monthly_profit_items_report(
    self, now=None, *, start=None, end=None, location_id=None
) -> tuple[dict, list[dict]]:
    """Cada item incluye "location_code": str (puede ser "") y "location_name": str (puede ser "")."""
    ...
```

#### Objective

Añadir la tool `profit_by_location` en `app/agent/tools.py`, después de `profit_by_category` (T3).

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change** — localizar el final de la función `profit_by_category` (última función del archivo tras T3) y añadir después:

```python
# AFTER el cierre de profit_by_category (el `}` y la línea en blanco que la termina),
# añadir al final del archivo:


@tool("profit_by_location", "Ganancia y margen agrupados por ubicación/POS en un período, "
      "ordenado de mayor a menor ganancia (start/end ISO opcionales).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}})
def profit_by_location(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    _summary, items = reporting.monthly_profit_items_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    grouped: dict[str, dict[str, float]] = {}
    for i in items:
        key = (i["location_name"] or i["location_code"] or "").strip() or "Sin ubicación"
        g = grouped.setdefault(key, {"qty": 0.0, "sales": 0.0, "cogs": 0.0, "profit": 0.0})
        g["qty"] += float(i["qty"])
        g["sales"] += float(i["sales"])
        g["cogs"] += float(i["cogs"])
        g["profit"] += float(i["profit"])
    rows = [
        [name, g["qty"], g["sales"], g["cogs"], g["profit"],
         round((g["profit"] / g["sales"] * 100.0) if g["sales"] else 0.0, 1)]
        for name, g in grouped.items()
    ]
    rows.sort(key=lambda r: r[4], reverse=True)
    return {
        "labels": [r[0] for r in rows],
        "values": [r[4] for r in rows],
        "columns": ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": rows,
    }
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `location_name` y `location_code` ambos vacíos (venta sin ubicación asignada): se agrupa bajo `"Sin ubicación"`.

#### Verification

```bash
python3 -c "from app.agent.tools import TOOL_REGISTRY; assert 'profit_by_location' in TOOL_REGISTRY; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — tool `profit_by_location` añadida al final del archivo
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T5: Helper `_resolve_location_id` + filtro `location` en `sales_by_product`

**Depends on:** T4
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/models.py — YA EXISTE, NO MODIFICAR
class Location(Base):
    id: Mapped[int]
    business_id: Mapped[Optional[int]]
    code: Mapped[str]   # unique=True global
    name: Mapped[str]

# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def sales_by_product(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> tuple[float, list[dict]]: ...

# app/agent/tools.py — YA EXISTE, NO MODIFICAR (helpers de fecha, mismo patrón a seguir)
def _iso(value: str) -> datetime: ...
def _parse_end(value: str) -> datetime: ...
def _parse_end ...
```

#### Objective

Añadir el helper `_resolve_location_id` (resolución de solo lectura, sin autocrear) justo
después de `_parse_end` y ANTES de `def tool(...)`; y extender `sales_by_product` con un
parámetro opcional `location`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2a: Añadir el helper, justo antes de `def tool(...)`**

```python
# BEFORE (líneas 32-38, tal cual):
def tool(name: str, description: str, params: dict) -> Callable:
    def deco(fn: Callable) -> Callable:
        TOOL_REGISTRY[name] = {"description": description, "params": params, "fn": fn}
        return fn
    return deco

# AFTER (se inserta el helper ANTES de `def tool`):
def _resolve_location_id(service: InventoryService, location: str) -> int:
    """Código/nombre -> id, solo lectura, scoped a business_id. A diferencia de
    InventoryService._location_id_for_code, NUNCA autocrea: un código alucinado por
    el LLM debe fallar con 404, no crear una ubicación fantasma."""
    from fastapi import HTTPException
    from sqlalchemy import func, select

    from app.models import Location

    value = (location or "").strip()
    loc_id = service.db.scalar(
        select(Location.id).where(
            Location.business_id == service.business_id,
            func.lower(Location.code) == value.lower(),
        )
    )
    if loc_id is None:
        loc_id = service.db.scalar(
            select(Location.id).where(
                Location.business_id == service.business_id,
                func.lower(Location.name) == value.lower(),
            )
        )
    if loc_id is None:
        raise HTTPException(status_code=404, detail=f"Ubicación no encontrada: {location}")
    return int(loc_id)


def tool(name: str, description: str, params: dict) -> Callable:
    def deco(fn: Callable) -> Callable:
        TOOL_REGISTRY[name] = {"description": description, "params": params, "fn": fn}
        return fn
    return deco
```

**Step 2b: Extender `sales_by_product` con `location`**

```python
# BEFORE (líneas 94-110, tal cual):
@tool("sales_by_product", "Ventas desglose por producto (start/end ISO; limit opcional = top N por ventas).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}},
       "required": ["start", "end"]})
def sales_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    _total, items = reporting.sales_by_product(_iso(args["start"]), _parse_end(args["end"]))
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    # Devuelve ambos formatos: labels/values para chart (bar/pie/line) y
    # columns/rows para widget de tipo table, según lo que pida el usuario.
    return {
        "labels": [i["name"] for i in items],
        "values": [i["sales"] for i in items],
        "columns": ["Producto", "Cantidad", "Ventas"],
        "rows": [[i["name"], i["qty"], i["sales"]] for i in items],
    }

# AFTER:
@tool("sales_by_product", "Ventas desglose por producto (start/end ISO; limit opcional = top N por ventas; "
      "location opcional = código o nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"},
                                          "location": {"type": "string"}},
       "required": ["start", "end"]})
def sales_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    _total, items = reporting.sales_by_product(_iso(args["start"]), _parse_end(args["end"]), location_id=location_id)
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    # Devuelve ambos formatos: labels/values para chart (bar/pie/line) y
    # columns/rows para widget de tipo table, según lo que pida el usuario.
    return {
        "labels": [i["name"] for i in items],
        "values": [i["sales"] for i in items],
        "columns": ["Producto", "Cantidad", "Ventas"],
        "rows": [[i["name"], i["qty"], i["sales"]] for i in items],
    }
```

**Imports needed:** ninguno a nivel de módulo (los imports de `_resolve_location_id` van dentro de la propia función, igual que en `total_purchases`/`sales_by_location` del mismo archivo).

**Edge cases (ya resueltos):**
- `location` no coincide ni por código ni por nombre (case-insensitive) para este `business_id`: `HTTPException(404, "Ubicación no encontrada: <valor>")`. `_execute_node` (`app/agent/graph.py`, sin cambios) ya captura este tipo de excepción.
- `location` ausente: `location_id = None` → comportamiento idéntico al actual.

#### Verification

```bash
python3 -c "from app.agent.tools import TOOL_REGISTRY, _resolve_location_id; print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — helper `_resolve_location_id` añadido antes de `def tool(...)`
- [ ] `app/agent/tools.py` modificado — `sales_by_product` acepta `location` opcional
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T6: Filtro `location` en `profit_by_product`

**Depends on:** T5 (usa `_resolve_location_id`)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/tools.py — YA EXISTE tras T5, NO MODIFICAR
def _resolve_location_id(service: InventoryService, location: str) -> int: ...

# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def monthly_profit_report(
    self, now=None, location_id=None, *, start=None, end=None
) -> tuple[dict, list[dict]]: ...
```

#### Objective

Extender la tool `profit_by_product` en `app/agent/tools.py` con un parámetro opcional `location`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change**

```python
# BEFORE (tal cual quedó en la sesión anterior, líneas ~263-285):
@tool("profit_by_product", "Ganancia y margen por producto en un período, ordenado de mayor a menor ganancia "
      "(start/end ISO opcionales; limit opcional = top N por ganancia).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def profit_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    _summary, items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
    )
    items = sorted(items, key=lambda i: i["gross"], reverse=True)
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["gross"]) for i in items],
        "columns": ["Producto", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": [
            [i["name"], float(i["qty"]), float(i["sales"]), float(i["cogs"]),
             float(i["gross"]), round(float(i["gross_pct"]), 1)]
            for i in items
        ],
    }

# AFTER:
@tool("profit_by_product", "Ganancia y margen por producto en un período, ordenado de mayor a menor ganancia "
      "(start/end ISO opcionales; limit opcional = top N por ganancia; location opcional = código o "
      "nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"},
                                          "location": {"type": "string"}}})
def profit_by_product(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    _summary, items = reporting.monthly_profit_report(
        start=_iso(args["start"]) if args.get("start") else None,
        end=_parse_end(args["end"]) if args.get("end") else None,
        location_id=location_id,
    )
    items = sorted(items, key=lambda i: i["gross"], reverse=True)
    limit = args.get("limit")
    if limit:
        items = items[: int(limit)]
    return {
        "labels": [i["name"] for i in items],
        "values": [float(i["gross"]) for i in items],
        "columns": ["Producto", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"],
        "rows": [
            [i["name"], float(i["qty"]), float(i["sales"]), float(i["cogs"]),
             float(i["gross"]), round(float(i["gross_pct"]), 1)]
            for i in items
        ],
    }
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):** mismos que T5 (404 si `location` no resuelve; `None` si no se pasa).

#### Verification

```bash
python3 -c "
from app.agent.tools import TOOL_REGISTRY
import inspect
src = inspect.getsource(TOOL_REGISTRY['profit_by_product']['fn'])
assert 'location_id' in src
print('OK')
"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — `profit_by_product` acepta `location` opcional
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T7: Filtro `location` en `daily_sales_series`

**Depends on:** T5 (usa `_resolve_location_id`)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/tools.py — YA EXISTE tras T5, NO MODIFICAR
def _resolve_location_id(service: InventoryService, location: str) -> int: ...

# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR
def daily_sales_series(self, start: datetime, end: datetime, location_id: Optional[int] = None) -> list[dict]: ...
```

#### Objective

Extender la tool `daily_sales_series` en `app/agent/tools.py` con un parámetro opcional `location`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/tools.py
```

**Step 2: Apply the change**

```python
# BEFORE (líneas 113-118, tal cual):
@tool("daily_sales_series", "Serie diaria de ventas entre start y end ISO.",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}}, "required": ["start", "end"]})
def daily_sales_series(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    series = reporting.daily_sales_series(_iso(args["start"]), _parse_end(args["end"]))
    return {"labels": [d["day"] for d in series], "values": [d["sales"] for d in series]}

# AFTER:
@tool("daily_sales_series", "Serie diaria de ventas entre start y end ISO (location opcional = código "
      "o nombre de ubicación para filtrar).",
      {"type": "object", "properties": {"start": {"type": "string"}, "end": {"type": "string"}, "location": {"type": "string"}},
       "required": ["start", "end"]})
def daily_sales_series(service: InventoryService, args: dict) -> dict:
    reporting = ReportingService(service.db, service.business_id)
    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None
    series = reporting.daily_sales_series(_iso(args["start"]), _parse_end(args["end"]), location_id=location_id)
    return {"labels": [d["day"] for d in series], "values": [d["sales"] for d in series]}
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):** mismos que T5.

#### Verification

```bash
python3 -c "
from app.agent.tools import TOOL_REGISTRY
import inspect
src = inspect.getsource(TOOL_REGISTRY['daily_sales_series']['fn'])
assert 'location_id' in src
print('OK')
"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — `daily_sales_series` acepta `location` opcional
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T8: Reglas del system prompt para las tools nuevas

**Depends on:** T7
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/prompts.py`
**Pillars:** P1, P4

#### Context

```python
# app/agent/prompts.py — SYSTEM_PROMPT_TEMPLATE, sección de reglas (sin cambios de estructura)
```

#### Objective

Añadir 5 bullets nuevos a las reglas de selección de tool en `SYSTEM_PROMPT_TEMPLATE`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/prompts.py
```

**Step 2a: Insertar reglas de `profit_by_category`/`profit_by_location`**

```python
# BEFORE:
- Para "cuánto gané"/"margen"/"rentabilidad" usa monthly_profit_summary (kpi) o
  profit_by_product (bar/table) para el desglose por producto.
- Para "evolución mensual"/"tendencia de ventas y compras" usa monthly_overview (line).

# AFTER:
- Para "cuánto gané"/"margen"/"rentabilidad" usa monthly_profit_summary (kpi) o
  profit_by_product (bar/table) para el desglose por producto.
- Para "ganancia por categoría"/"qué categoría de producto es más rentable" usa
  profit_by_category (bar/table, limit opcional).
- Para "ganancia por ubicación"/"qué POS genera más utilidad" usa profit_by_location
  (bar/table) — sales_by_location da solo ingresos, profit_by_location da ganancia.
- Para "evolución mensual"/"tendencia de ventas y compras" usa monthly_overview (line).
```

**Step 2b: Insertar reglas de `dead_stock`/`inventory_turnover`**

```python
# BEFORE:
- Para "qué reponer"/"punto de reorden"/"stock de seguridad" usa replenishment_table
  (table) o low_stock (table) para solo los que están por debajo del mínimo.
- Para "últimos gastos" usa expenses_list (table); para "historial de movimientos/

# AFTER:
- Para "qué reponer"/"punto de reorden"/"stock de seguridad" usa replenishment_table
  (table) o low_stock (table) para solo los que están por debajo del mínimo.
- Para "productos sin venta"/"stock muerto"/"qué no se ha vendido" usa dead_stock
  (bar/table, argumento days opcional, default 90).
- Para "rotación de inventario"/"cada cuánto rota el stock" usa inventory_turnover (kpi).
- Para "últimos gastos" usa expenses_list (table); para "historial de movimientos/
```

**Step 2c: Insertar regla del filtro `location`**

```python
# BEFORE:
- Para "top N que más ganancia/utilidad generaron" usa profit_by_product con el
  argumento limit=N (ya viene ordenado de mayor a menor ganancia) y type "bar" o "pie".
- Si el usuario pide "tabla"/"listado"/"detalle", usa type "table" (sales_by_product

# AFTER:
- Para "top N que más ganancia/utilidad generaron" usa profit_by_product con el
  argumento limit=N (ya viene ordenado de mayor a menor ganancia) y type "bar" o "pie".
- Para filtrar por ubicación/POS específica (ej. "ventas de X en la central") pasa
  el argumento location="<código o nombre>" a sales_by_product, profit_by_product o
  daily_sales_series.
- Si el usuario pide "tabla"/"listado"/"detalle", usa type "table" (sales_by_product
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):** ninguno — es texto de prompt, sin lógica.

#### Verification

```bash
python3 -c "
from app.agent.prompts import build_system_prompt
p = build_system_prompt()
for kw in ('profit_by_category', 'profit_by_location', 'dead_stock', 'inventory_turnover'):
    assert kw in p, kw
print('OK')
"
# Expected: OK
```

#### Expected Report

- [ ] `app/agent/prompts.py` modificado — 5 bullets nuevos añadidos en 3 puntos de la lista de reglas
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — OK

---

### T9: Tests unitarios

**Depends on:** T8
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_agent_tools.py`
**Pillars:** P1, P4, P6

#### Context

```python
# tests/unit/test_agent_tools.py — YA EXISTE, patrón de fixtures
# fixtures disponibles (tests/conftest.py): db_session (Session), business_id (int)

def test_tool_schemas_count():
    assert len(tool_schemas()) == 29   # pasa a 33 (4 tools nuevas: dead_stock,
                                        # inventory_turnover, profit_by_category, profit_by_location)

def test_profit_by_product_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_product"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Producto", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]
```

#### Objective

Actualizar el conteo de tools y añadir tests de shape para las 4 tools nuevas + tests de
resolución de `location` (404 en desconocida, resuelve en conocida).

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_agent_tools.py
```

**Step 2a: Actualizar imports y el conteo**

```python
# BEFORE (líneas 1-9):
from datetime import datetime, timezone

from app.agent.prompts import build_system_prompt
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 29

# AFTER:
from datetime import datetime, timezone

import pytest
from fastapi import HTTPException

from app.agent.prompts import build_system_prompt
from app.agent.tools import TOOL_REGISTRY, tool_schemas
from app.models import Location
from app.services.inventory_service import InventoryService


def test_tool_schemas_count():
    assert len(tool_schemas()) == 33
```

**Step 2b: Añadir tests al final del archivo**

```python
# BEFORE (últimas líneas del archivo, tal cual):
def test_purchases_by_month_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["purchases_by_month"]["fn"](service, {"months": 2})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Mes", "Compras"]
    assert len(result["rows"]) == 2

# AFTER (se añade después):
def test_purchases_by_month_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["purchases_by_month"]["fn"](service, {"months": 2})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Mes", "Compras"]
    assert len(result["rows"]) == 2


def test_dead_stock_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["dead_stock"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["SKU", "Nombre", "Valor inmovilizado"]
    assert result["rows"] == []


def test_dead_stock_days_arg(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["dead_stock"]["fn"](service, {"days": 30})
    assert result["rows"] == []


def test_inventory_turnover_no_data(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["inventory_turnover"]["fn"](service, {})
    assert result == {"value": 0.0, "label": "Sin datos suficientes", "currency": False}


def test_profit_by_category_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_category"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Categoría", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]


def test_profit_by_location_shape(db_session, business_id):
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["profit_by_location"]["fn"](service, {})
    assert "labels" in result and "values" in result
    assert result["columns"] == ["Ubicación", "Cantidad", "Ventas", "Costo", "Ganancia", "Margen %"]


def test_sales_by_product_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["sales_by_product"]["fn"](
            service, {"start": "2026-01-01", "end": "2026-01-31", "location": "NOPE"}
        )


def test_profit_by_product_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["profit_by_product"]["fn"](service, {"location": "NOPE"})


def test_daily_sales_series_unknown_location_raises(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException):
        TOOL_REGISTRY["daily_sales_series"]["fn"](
            service, {"start": "2026-01-01", "end": "2026-01-31", "location": "NOPE"}
        )


def test_sales_by_product_known_location_resolves(db_session, business_id):
    db_session.add(Location(business_id=business_id, code="CENTRAL", name="Central"))
    db_session.commit()
    service = InventoryService(db_session, business_id)
    result = TOOL_REGISTRY["sales_by_product"]["fn"](
        service, {"start": "2026-01-01", "end": "2026-01-31", "location": "central"}
    )
    assert result["rows"] == []
```

**Imports needed:** `pytest`, `from fastapi import HTTPException`, `from app.models import Location` (añadidos en Step 2a).

**Edge cases (ya resueltos):**
- `test_sales_by_product_known_location_resolves` prueba resolución case-insensitive por código (`"central"` vs `Location.code="CENTRAL"`), sin datos de venta (rows vacío es el resultado esperado, no un error).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_agent_tools.py -v --no-cov
# Expected: todos los tests del archivo PASS (los 20 existentes + 9 nuevos = 29 tests)
```

#### Expected Report

- [ ] `tests/unit/test_agent_tools.py` modificado — imports actualizados, conteo 29→33, 9 tests nuevos añadidos
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_tools.py -v --no-cov` — PASS

---

### T10: Suite completa + VERSION bump (PATCH)

**Depends on:** T9
**Skill:** release-manager
**Agent:** coder
**Files:** `VERSION`, `.bumpversion.cfg`
**Pillars:** P1, P7

#### Context

```
# VERSION — YA EXISTE
0.4.2

# .bumpversion.cfg — YA EXISTE
[bumpversion]
current_version = 0.4.2
```

#### Objective

Ejecutar la suite completa y, si pasa, subir la versión de PATCH `0.4.2` → `0.4.3` (nunca
MINOR — regla dura del proyecto).

#### Instructions

**Step 1: Ejecutar la suite completa ANTES de tocar versión**

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: 100% PASS, cobertura >= 80% (gate del proyecto)
```

Si falla: NO continuar a Step 2. Corregir en el envelope correspondiente (T1-T9) y
repetir este comando hasta que pase.

**Step 2: Read the files**
```
Read VERSION
Read .bumpversion.cfg
```

**Step 3: Apply the change**

```
# VERSION
# BEFORE:
0.4.2
# AFTER:
0.4.3
```

```ini
# .bumpversion.cfg
# BEFORE:
[bumpversion]
current_version = 0.4.2
commit = True
tag = True
tag_name = v{new_version}
message = chore(release): bump version {current_version} → {new_version}

# AFTER:
[bumpversion]
current_version = 0.4.3
commit = True
tag = True
tag_name = v{new_version}
message = chore(release): bump version {current_version} → {new_version}
```

**Imports needed:** ninguno (edición de archivos de texto plano).

**Edge cases (ya resueltos):**
- Este es un incremento PATCH, no MINOR — `0.4.2` → `0.4.3`, no `0.5.0`. Ver regla dura
  del `CLAUDE.md` del proyecto.

#### Verification

```bash
cat VERSION
# Expected: 0.4.3
grep "current_version" .bumpversion.cfg
# Expected: current_version = 0.4.3
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: 100% PASS (mismo resultado que Step 1)
```

#### Expected Report

- [ ] `VERSION` modificado — `0.4.2` → `0.4.3`
- [ ] `.bumpversion.cfg` modificado — `current_version = 0.4.3`
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit -v` — 100% PASS

---

### T11: CHANGELOG.md

**Depends on:** T10
**Skill:** technical-writer
**Agent:** docs
**Files:** `CHANGELOG.md`
**Pillars:** P4

#### Context

```markdown
# CHANGELOG.md — primeras líneas, YA EXISTEN
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## Unreleased
```

#### Objective

Documentar las 4 tools nuevas + el filtro de ubicación bajo una nueva sección `## 0.4.3`.

#### Instructions

**Step 1: Read the file**
```
Read CHANGELOG.md
```

**Step 2: Apply the change**

```markdown
# BEFORE (primeras líneas, tal cual):
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## Unreleased

# AFTER:
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.4.3 (2026-08-17)

- feat(agent): 4 tools nuevas para el copiloto IA — `dead_stock` (productos sin venta
  reciente y valor inmovilizado), `inventory_turnover` (rotación de inventario en días),
  `profit_by_category` y `profit_by_location` (ganancia agrupada por categoría/ubicación,
  hasta ahora solo disponible por producto).
- feat(agent): `sales_by_product`, `profit_by_product` y `daily_sales_series` aceptan un
  argumento opcional `location` (código o nombre de ubicación) para filtrar por POS/central.

## Unreleased
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- No se reordena ni se toca el histórico previo de `## Unreleased` — se inserta la
  sección nueva `## 0.4.3` ANTES de esa cabecera, dejándola vacía debajo para futuras
  entradas (igual patrón que las secciones de versión anteriores del mismo archivo).

#### Verification

```bash
grep -A1 "## 0.4.3" CHANGELOG.md
# Expected: la línea "## 0.4.3 (2026-08-17)" existe
```

#### Expected Report

- [ ] `CHANGELOG.md` modificado — sección `## 0.4.3 (2026-08-17)` añadida
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — línea encontrada

---

## Checklist de los 7 mandamientos (verificado)

- [x] Claude Code piensa, OpenCode escribe — todo before/after es código exacto, cero decisiones abiertas
- [x] ≤ 800 tokens por envelope (T9 es el más largo por el volumen de tests, dentro de margen razonable dado el precedente de PAEs previos del proyecto — ver `coach-financiero-operativo`)
- [x] Before/after, nunca descripciones
- [x] Máximo 2 archivos por envelope (todos son 1 archivo)
- [x] Verificación mecánica (comando → PASS/FAIL o assert con print("OK"))
- [x] Dependencias explícitas con firmas reales
- [x] Batches de 3-5 envelopes — recomendado: `/compact` después de T4 y después de T8
