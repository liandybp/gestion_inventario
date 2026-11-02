# Task Envelopes — Copiloto: movimientos de un artículo en un período (`product_movements`)

Fase 3 de `requirements.md` / `design.md`. Rama base `develop` `9655c8c`. Sin bump de versión (0.5.0 agrupada).

Cada envelope es la única fuente de su tarea. Un lote = una sesión limpia de `cc_local`. No leer el PAE completo.

Lotes: **A** T1–T6 · **B** T7–T9 · **C** T10–T14

---

### T1: `InventoryRepository.quantity_before` — saldo inicial por artículo y ubicación

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/repositories/inventory_repository.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/repositories/inventory_repository.py — YA EXISTE, NO MODIFICAR salvo lo indicado
from datetime import datetime                      # línea 3 (ya importado)
from typing import NamedTuple, Optional            # línea 7 (ya importado)
from sqlalchemy import String, case, cast, func, select   # línea 9 (ya importado)
from app.models import AuditLog, InventoryLot, InventoryMovement, Location, MovementAllocation, Product

class InventoryRepository:
    def __init__(self, db: Session, business_id: Optional[int] = None): ...  # self._db, self._business_id
    def stock_for_product_id(self, product_id: int, location_id: Optional[int] = None) -> float: ...
```

#### Objective

Añadir el método `quantity_before` a `InventoryRepository` en `app/repositories/inventory_repository.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/repositories/inventory_repository.py
```

**Step 2: Insert the new method immediately after `stock_for_product_id` (líneas 74–82)**

```python
# BEFORE (fin de stock_for_product_id, líneas 74–82):
    def stock_for_product_id(self, product_id: int, location_id: Optional[int] = None) -> float:
        stmt = select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0)).where(
            InventoryLot.product_id == product_id
        )
        stmt = stmt.where(InventoryLot.business_id == self._business_id)
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        total = self._db.scalar(stmt)
        return float(total or 0)

# AFTER:
    def stock_for_product_id(self, product_id: int, location_id: Optional[int] = None) -> float:
        stmt = select(func.coalesce(func.sum(InventoryLot.qty_remaining), 0)).where(
            InventoryLot.product_id == product_id
        )
        stmt = stmt.where(InventoryLot.business_id == self._business_id)
        if location_id is not None:
            stmt = stmt.where(InventoryLot.location_id == location_id)
        total = self._db.scalar(stmt)
        return float(total or 0)

    def quantity_before(self, product_id: int, before: datetime, location_id: Optional[int] = None) -> float:
        """Suma con signo de las cantidades del artículo con movement_date < before (saldo inicial)."""
        stmt = select(func.coalesce(func.sum(InventoryMovement.quantity), 0)).where(
            InventoryMovement.business_id == self._business_id,
            InventoryMovement.product_id == int(product_id),
            InventoryMovement.movement_date < before,
        )
        if location_id is not None:
            stmt = stmt.where(InventoryMovement.location_id == int(location_id))
        return float(self._db.scalar(stmt) or 0)
```

**Imports needed:** ninguno nuevo. `func`, `select`, `InventoryMovement`, `datetime`, `Optional` ya están importados.

**Edge cases (ya resueltos):**
- Sin movimientos anteriores: `scalar` devuelve `None`, se devuelve `0.0` (`or 0`).
- Frontera: `movement_date < before` usa estricto menor; un movimiento exactamente en `before` NO cuenta.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: todos PASS (el método nuevo aún no tiene test propio; nada debe romperse)
```

#### Expected Report

- [ ] `app/repositories/inventory_repository.py` modificado — método `quantity_before` insertado tras `stock_for_product_id`
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_repositories.py -q --override-ini="addopts="` — PASS

---

### T2: `LocationRepository.labels` — mapa id/código → nombre

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/repositories/location_repository.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/repositories/location_repository.py — YA EXISTE, NO MODIFICAR salvo lo indicado
from sqlalchemy import and_, select              # ya importado
from sqlalchemy.orm import Session               # ya importado
from app.models import Business, Location        # ya importado

class LocationRepository:
    def __init__(self, db: Session, business_id: int, business_code: Optional[str] = None): ...
    # self._db, self._business_id, self._business_code
```

#### Objective

Añadir el método `labels` a `LocationRepository` en `app/repositories/location_repository.py`.

#### Instructions

**Step 1: Read the file**
```
Read app/repositories/location_repository.py
```

**Step 2: Insert the new method immediately after `__init__` (antes de `location_id_for_code`, línea 18)**

```python
# BEFORE (fin de __init__, líneas 12–16):
class LocationRepository:
    def __init__(self, db: Session, business_id: int, business_code: Optional[str] = None):
        self._db = db
        self._business_id = int(business_id)
        self._business_code = business_code or ""

# AFTER:
class LocationRepository:
    def __init__(self, db: Session, business_id: int, business_code: Optional[str] = None):
        self._db = db
        self._business_id = int(business_id)
        self._business_code = business_code or ""

    def labels(self) -> tuple[dict[int, str], dict[str, str]]:
        """({location_id: nombre}, {code: nombre}); el nombre vacío cae al código."""
        rows = self._db.execute(
            select(Location.id, Location.code, Location.name).where(
                Location.business_id == self._business_id
            )
        ).all()
        by_id: dict[int, str] = {}
        by_code: dict[str, str] = {}
        for loc_id, code, name in rows:
            label = (str(name or "").strip()) or (str(code or "").strip()) or ""
            by_id[int(loc_id)] = label
            by_code[str(code or "").strip()] = label
        return by_id, by_code
```

**Imports needed:** ninguno nuevo. `select`, `Location`, `Session`, `Optional` ya están importados.

**Edge cases (ya resueltos):**
- Nombre vacío: `label` cae al código (`.strip()`).
- Código vacío: `label` queda `""`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: todos PASS
```

#### Expected Report

- [ ] `app/repositories/location_repository.py` modificado — método `labels` insertado tras `__init__`
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_repositories.py -q --override-ini="addopts="` — PASS

---

### T3: `ReportingService.product_movements` + constantes de módulo

**Depends on:** T1 (quantity_before), T2 (labels)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/reporting_service.py`
**Pillars:** P1, P2, P3, P4, P5, P6

#### Context

```python
# app/services/reporting_service.py — YA EXISTE, NO MODIFICAR salvo lo indicado
import math                                     # línea 3
from datetime import datetime, timedelta, timezone   # línea 4
from typing import Optional                      # línea 5
from sqlalchemy.orm import Session               # línea 8

class ReportingService:
    def __init__(self, db: Session, business_id: int): ...  # self._db, self._business_id
    def _finance(self):  # import local de FinanceService dentro del método

# app/repositories/inventory_repository.py — métodos YA EXISTENTES que se usan:
class InventoryRepository:
    def movement_history(self, sku=None, query="", movement_type=None, location_id=None,
                         start_date=None, end_date=None, limit=100) -> list[tuple]:
        # ordena movement_date DESC, id DESC; limit=None = sin límite; devuelve MovementHistoryRow
    def quantity_before(self, product_id: int, before: datetime, location_id=None) -> float: ...
    def stock_for_product_id(self, product_id: int, location_id=None) -> float: ...

# MovementHistoryRow (NamedTuple) campos: id, movement_date, type, location_id, sku, name,
#   unit_of_measure, quantity, unit_cost, unit_price, note, username

# app/services/inventory_service.py — YA EXISTE:
class InventoryService:
    def transfer_codes_from_out_note(self, note: str) -> tuple[str, str, Optional[str]]:
        # regex Transfer\s+([^\s:;]+)->([^\s:;]+); lanza HTTPException(409) si no matchea
```

#### Objective

Añadir `import re`, las constantes `_OUT_ID_RE`, `_IN_FROM_RE`, `PRODUCT_MOVEMENTS_MAX_ROWS`, `_TYPE_LABELS` y el método `product_movements` a `ReportingService`.

#### Instructions

**Step 1: Read the file**
```
Read app/services/reporting_service.py
```

**Step 2: Añadir `import re`**

```python
# BEFORE (líneas 3–4):
import math
from datetime import datetime, timedelta, timezone

# AFTER:
import math
import re
from datetime import datetime, timedelta, timezone
```

**Step 3: Añadir las constantes de módulo tras `_MAX_MONTHS_WINDOW = 24` (línea 22)**

```python
# BEFORE (línea 22):
_MAX_MONTHS_WINDOW = 24

# AFTER:
_MAX_MONTHS_WINDOW = 24

_OUT_ID_RE = re.compile(r"\bout_id=(\d+)")
_IN_FROM_RE = re.compile(r"Transfer in from\s+([^\s:;]+)")
PRODUCT_MOVEMENTS_MAX_ROWS = 500
_TYPE_LABELS = {
    "purchase": "Compra",
    "sale": "Venta",
    "adjustment": "Ajuste",
    "return_supplier": "Devolución a proveedor",
    "transfer_out": "Traspaso",
    "transfer_in": "Traspaso",
}
```

**Step 4: Añadir el método `product_movements` como último método de la clase (tras `monthly_overview`)**

```python
    def product_movements(
        self,
        *,
        product_id: int,
        sku: str,
        start: datetime,
        end: datetime,
        location_id: Optional[int] = None,
    ) -> dict:
        """Movimientos de UN artículo en un período: fila de saldo inicial + una fila por
        movimiento (los traspasos se agrupan en una fila en la vista global), con saldo
        acumulado y columna Costo/Precio. Solo lectura; la nota de REQ-6/REQ-7 va en
        result["note"] cuando aplica."""
        from fastapi import HTTPException

        from app.repositories.inventory_repository import InventoryRepository
        from app.repositories.location_repository import LocationRepository
        from app.services.inventory_service import InventoryService

        inventory = InventoryRepository(self._db, self._business_id)
        location_repo = LocationRepository(self._db, self._business_id)
        names_by_id, names_by_code = location_repo.labels()

        rows = list(reversed(
            inventory.movement_history(sku=sku, location_id=location_id,
                                       start_date=start, end_date=end, limit=None)
        ))
        initial = inventory.quantity_before(int(product_id), start, location_id)

        columns = ["Fecha", "Tipo", "Ubicación", "Cantidad", "Costo/Precio", "Saldo", "Usuario"]

        def _fmt_date(dt: Optional[datetime]) -> str:
            return dt.strftime("%Y-%m-%d %H:%M") if dt else ""

        def _num(v: Optional[float]) -> float:
            return round(float(v or 0), 4)

        out_rows: list[list] = [
            ["", "Saldo inicial",
             "Todas" if location_id is None else names_by_id.get(location_id, ""),
             "", "", _num(initial), ""]
        ]
        saldo = float(initial)
        inv_service = InventoryService(self._db, self._business_id)

        def _cost_price(r) -> object:
            if r.type == "sale":
                return _num(r.unit_price) if r.unit_price is not None else ""
            if r.type in ("purchase", "adjustment", "return_supplier"):
                return _num(r.unit_cost) if r.unit_cost is not None else ""
            return ""

        def _location_name(loc_id: Optional[int]) -> str:
            return names_by_id.get(int(loc_id), "") if loc_id is not None else ""

        if location_id is None:
            out_ids = {r.id for r in rows if r.type == "transfer_out"}
            grouped_ins: set[int] = set()
            for r in rows:
                if r.type != "transfer_in":
                    continue
                m = _OUT_ID_RE.search(str(r.note or ""))
                if m and int(m.group(1)) in out_ids:
                    grouped_ins.add(r.id)

            for r in rows:
                if r.type == "transfer_in" and r.id in grouped_ins:
                    continue
                if r.type == "transfer_out":
                    ins = []
                    for x in rows:
                        if x.type != "transfer_in":
                            continue
                        mm = _OUT_ID_RE.search(str(x.note or ""))
                        if mm and int(mm.group(1)) == r.id:
                            ins.append(x)
                    saldo += float(r.quantity or 0) + sum(float(x.quantity or 0) for x in ins)
                    try:
                        from_code, to_code, _ref = inv_service.transfer_codes_from_out_note(r.note or "")
                    except HTTPException:
                        from_code, to_code = "", ""
                    origen = names_by_code.get(from_code, from_code) or _location_name(r.location_id)
                    destino = names_by_code.get(to_code, to_code) or "?"
                    out_rows.append([_fmt_date(r.movement_date), "Traspaso",
                                     f"{origen} → {destino}", _num(abs(r.quantity or 0)), "",
                                     _num(saldo), r.username or ""])
                elif r.type == "transfer_in":
                    m = _IN_FROM_RE.search(str(r.note or ""))
                    origen = names_by_code.get(m.group(1), m.group(1)) if m else ""
                    label = f"← {origen}" if origen else _location_name(r.location_id)
                    saldo += float(r.quantity or 0)
                    out_rows.append([_fmt_date(r.movement_date), "Traspaso", label,
                                     _num(r.quantity or 0), "", _num(saldo), r.username or ""])
                else:
                    saldo += float(r.quantity or 0)
                    out_rows.append([_fmt_date(r.movement_date),
                                     _TYPE_LABELS.get(r.type or "", r.type or ""),
                                     _location_name(r.location_id), _num(r.quantity or 0),
                                     _cost_price(r), _num(saldo), r.username or ""])
        else:
            for r in rows:
                saldo += float(r.quantity or 0)
                if r.type == "transfer_out":
                    try:
                        _from_code, to_code, _ref = inv_service.transfer_codes_from_out_note(r.note or "")
                    except HTTPException:
                        to_code = ""
                    label = f"→ {names_by_code.get(to_code, to_code)}" if to_code else _location_name(r.location_id)
                elif r.type == "transfer_in":
                    m = _IN_FROM_RE.search(str(r.note or ""))
                    origen = names_by_code.get(m.group(1), m.group(1)) if m else ""
                    label = f"← {origen}" if origen else _location_name(r.location_id)
                else:
                    label = _location_name(r.location_id)
                out_rows.append([_fmt_date(r.movement_date),
                                 _TYPE_LABELS.get(r.type or "", r.type or ""),
                                 label, _num(r.quantity or 0), _cost_price(r),
                                 _num(saldo), r.username or ""])

        final_saldo = float(saldo)
        total_movements = len(out_rows) - 1
        notes: list[str] = []
        if total_movements > PRODUCT_MOVEMENTS_MAX_ROWS:
            out_rows = out_rows[:1 + PRODUCT_MOVEMENTS_MAX_ROWS]
            notes.append(
                f"Se muestran los {PRODUCT_MOVEMENTS_MAX_ROWS} primeros movimientos de {total_movements}."
            )

        now = datetime.now(timezone.utc)
        end_cmp = end if end.tzinfo is not None else end.replace(tzinfo=timezone.utc)
        if end_cmp >= now:
            stock = inventory.stock_for_product_id(int(product_id), location_id)
            if abs(final_saldo - stock) > 0.0001:
                notes.append(
                    f"El saldo calculado ({_num(final_saldo)}) no coincide con el stock actual por lotes ({_num(stock)})."
                )

        result: dict = {"columns": columns, "rows": out_rows}
        if notes:
            result["note"] = " ".join(notes)
        return result
```

**Imports needed:** solo `import re` (Step 2). El resto (InventoryRepository, LocationRepository, InventoryService) se importa localmente dentro del método, como `_finance()`.

**Edge cases (ya resueltos):**
- Nota de salida sin formato `Transfer …->…`: `transfer_codes_from_out_note` lanza `HTTPException`; el `except HTTPException` la atrapa: el origen cae al nombre de la ubicación del movimiento y el destino a "?". Solo se captura `HTTPException` (corrección de la revisión, 2026-11-02): otro error es un bug y debe verse.
- `end` sin tzinfo: `end_cmp` la normaliza antes de comparar con `now` (evita `TypeError`).
- Truncado a 500: los saldos de las filas mostradas siguen correctos (se calculan en orden antes de cortar).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini="addopts="
# Expected: todos PASS (el método nuevo aún no tiene test; nada debe romperse)
```

#### Expected Report

- [ ] `app/services/reporting_service.py` modificado — `import re`, 4 constantes y método `product_movements` añadidos
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit -q --override-ini="addopts="` — PASS

---

### T4: tool `product_movements` + actualizar el conteo de tools (34 → 35)

**Depends on:** T3 (ReportingService.product_movements)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/tools.py`, `tests/unit/test_agent_tools.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/tools.py — YA EXISTE, NO MODIFICAR salvo lo indicado
from datetime import datetime, timedelta, timezone    # línea 3
from app.services.inventory_service import InventoryService     # línea 7
from app.services.reporting_service import ReportingService     # línea 8

def _iso(value: str) -> datetime: ...      # parsea ISO; siempre devuelve tz-aware
def _parse_end(value: str) -> datetime: ...   # fin inclusivo (date-only → end-of-day)
def _resolve_location_id(service: InventoryService, location: str) -> int: ...  # 404 si no existe
def tool(name: str, description: str, params: dict) -> Callable: ...  # registra en TOOL_REGISTRY

# app/services/product_service.py — YA EXISTE:
class ProductService:
    def __init__(self, db: Session, business_id: int | None = None): ...  # self._products (ProductRepository)
    def resolve_sku(self, value: str, *, barcode_fallback: bool = False) -> str: ...  # 422 vacío, 404 inexistente

# app/repositories/product_repository.py — YA EXISTE:
class ProductRepository:
    def get_by_sku(self, sku: str) -> Optional[Product]: ...

# tests/unit/test_agent_tools.py — YA EXISTE, línea 12–13:
def test_tool_schemas_count():
    assert len(tool_schemas()) == 34
```

#### Objective

Añadir la tool `product_movements` a `app/agent/tools.py` y actualizar el conteo de tools a 35 en `tests/unit/test_agent_tools.py`.

#### Instructions

**Step 1: Read the files**
```
Read app/agent/tools.py
Read tests/unit/test_agent_tools.py
```

**Step 2: Añadir los imports de `ProductRepository` y `ProductService` a `app/agent/tools.py`**

```python
# BEFORE (líneas 6–10):
from app.services.finance_service import FinanceService
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService
from app.services.sales_document_service import SalesDocumentService
from app.services.coach_service import CoachService

# AFTER:
from app.repositories.product_repository import ProductRepository
from app.services.finance_service import FinanceService
from app.services.inventory_service import InventoryService
from app.services.product_service import ProductService
from app.services.reporting_service import ReportingService
from app.services.sales_document_service import SalesDocumentService
from app.services.coach_service import CoachService
```

**Step 3: Añadir la tool tras `movement_history` (líneas 426–442)**

```python
# BEFORE (fin de movement_history, líneas 426–442):
@tool("movement_history", "Historial de movimientos recientes (tipo opcional: sale/purchase/adjustment/transfer_out/transfer_in; start/end ISO opcionales).",
      {"type": "object", "properties": {"type": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def movement_history(service: InventoryService, args: dict) -> dict:
    rows = service.movement_history(
        movement_type=args.get("type") or None,
        start_date=_iso(args["start"]) if args.get("start") else None,
        end_date=_parse_end(args["end"]) if args.get("end") else None,
        limit=int(args.get("limit") or 30),
    )
    return {
        "columns": ["Fecha", "Tipo", "SKU", "Producto", "Cantidad", "Usuario"],
        "rows": [
            [r[1].strftime("%Y-%m-%d %H:%M") if r[1] else "", r[2], r[4], r[5],
             float(r[7] or 0), (r[11] or "")]
            for r in rows
        ],
    }

# AFTER (añadir al final):
@tool("movement_history", "Historial de movimientos recientes (tipo opcional: sale/purchase/adjustment/transfer_out/transfer_in; start/end ISO opcionales).",
      {"type": "object", "properties": {"type": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}, "limit": {"type": "integer"}}})
def movement_history(service: InventoryService, args: dict) -> dict:
    rows = service.movement_history(
        movement_type=args.get("type") or None,
        start_date=_iso(args["start"]) if args.get("start") else None,
        end_date=_parse_end(args["end"]) if args.get("end") else None,
        limit=int(args.get("limit") or 30),
    )
    return {
        "columns": ["Fecha", "Tipo", "SKU", "Producto", "Cantidad", "Usuario"],
        "rows": [
            [r[1].strftime("%Y-%m-%d %H:%M") if r[1] else "", r[2], r[4], r[5],
             float(r[7] or 0), (r[11] or "")]
            for r in rows
        ],
    }


@tool("product_movements",
      "Movimientos de UN artículo (sku: código, 'SKU - nombre' o nombre exacto) en un período: compras, ventas, "
      "traspasos, ajustes y devoluciones, con saldo inicial y saldo acumulado. start/end ISO opcionales "
      "(por defecto los últimos 3 meses); location opcional (código o nombre) para un solo punto de venta.",
      {"type": "object", "properties": {"sku": {"type": "string"}, "start": {"type": "string"},
       "end": {"type": "string"}, "location": {"type": "string"}}, "required": ["sku"]})
def product_movements(service: InventoryService, args: dict) -> dict:
    product_service = ProductService(service.db, service.business_id)
    sku = product_service.resolve_sku(args.get("sku") or "")
    product = ProductRepository(service.db, business_id=service.business_id).get_by_sku(sku)

    location_id = _resolve_location_id(service, args["location"]) if args.get("location") else None

    if args.get("start"):
        start = _iso(args["start"])
    else:
        today = datetime.now(timezone.utc)
        start = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        for _ in range(2):
            if start.month == 1:
                start = start.replace(year=start.year - 1, month=12)
            else:
                start = start.replace(month=start.month - 1)
    end = _parse_end(args["end"]) if args.get("end") else _parse_end(datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    return ReportingService(service.db, service.business_id).product_movements(
        product_id=int(product.id), sku=sku, start=start, end=end, location_id=location_id,
    )
```

**Step 4: Actualizar el conteo en `tests/unit/test_agent_tools.py`**

```python
# BEFORE (líneas 12–13):
def test_tool_schemas_count():
    assert len(tool_schemas()) == 34

# AFTER:
def test_tool_schemas_count():
    assert len(tool_schemas()) == 35
```

**Edge cases (ya resueltos):**
- `sku` ausente o vacío: `resolve_sku("")` lanza `HTTPException(422, "product is required")`; `_execute_node` lo gestiona.
- SKU inexistente: `resolve_sku` lanza 404; `_execute_node` lo gestiona.
- `location` ausente o vacío: `location_id = None` (vista global).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_agent_tools.py::test_tool_schemas_count -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/agent/tools.py` modificado — import `ProductService` y tool `product_movements` añadidos
- [ ] `tests/unit/test_agent_tools.py` modificado — `test_tool_schemas_count` pasa a 35
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_tools.py::test_tool_schemas_count -q --override-ini="addopts="` — PASS

---

### T5: `tests/unit/test_reporting_product_movements.py` — camino feliz (REQ-1/2/3)

**Depends on:** T1, T2, T3
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_reporting_product_movements.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/conftest.py — fixtures YA EXISTENTES:
#   db_session -> Session (SQLite en memoria, Business(code="test"))
#   business_id -> int

# app/services/inventory_service.py — métodos YA EXISTENTES que se usan para sembrar:
#   purchase(PurchaseCreate) / transfer(TransferCreate) / sale(SaleCreate)
# PurchaseCreate(sku=..., quantity=..., unit_cost=..., movement_date=..., lot_code=..., note=...)
# TransferCreate(from_location_code=..., to_location_code=..., lines=[TransferLineCreate(sku=..., quantity=...)], movement_date=..., note=...)
# SaleCreate(sku=..., quantity=..., unit_price=..., movement_date=..., location_code=..., note=...)
```

#### Objective

Crear el fichero de tests unitarios del camino feliz (REQ-1, REQ-2, REQ-3) para `ReportingService.product_movements`.

#### Instructions

**Step 1: Create the file**
```
Write tests/unit/test_reporting_product_movements.py
```

**Step 2: Contenido literal completo**

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.models import Business, InventoryLot, InventoryMovement, Location, Product
from app.schemas import PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService
from app.services.reporting_service import ReportingService

COLUMNS = ["Fecha", "Tipo", "Ubicación", "Cantidad", "Costo/Precio", "Saldo", "Usuario"]


def _seed_locations(db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Almacén central")
    pos = Location(business_id=business_id, code="POS1", name="Playa")
    db_session.add_all([central, pos])
    db_session.flush()
    return central, pos


def _seed_product(db_session, business_id, sku):
    product = Product(
        business_id=business_id, sku=sku, name=f"Artículo {sku}", min_stock=0,
        unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
        lead_time_days=0,
    )
    db_session.add(product)
    db_session.commit()
    return product


def test_purchase_transfer_sale_order_and_balance(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M1")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M1", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M1", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M1", quantity=4)],
        movement_date=datetime(2026, 8, 10, tzinfo=timezone.utc), note=None,
    ))
    svc.sale(SaleCreate(sku="SKU-M1", quantity=1, unit_price=2.0,
                        movement_date=datetime(2026, 9, 2, tzinfo=timezone.utc),
                        location_code="POS1", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M1",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    assert result["columns"] == COLUMNS
    assert "note" not in result
    rows = result["rows"]
    assert rows[0] == ["", "Saldo inicial", "Todas", "", "", 0.0, ""]
    assert rows[1][1] == "Compra"
    assert rows[1][2] == "Almacén central"
    assert rows[1][3] == 10.0
    assert rows[1][4] == 1.0
    assert rows[1][5] == 10.0
    assert rows[2][1] == "Traspaso"
    assert rows[2][2] == "Almacén central → Playa"
    assert rows[2][3] == 4.0
    assert rows[2][4] == ""
    assert rows[2][5] == 10.0
    assert rows[3][1] == "Venta"
    assert rows[3][2] == "Playa"
    assert rows[3][3] == -1.0
    assert rows[3][4] == 2.0
    assert rows[3][5] == 9.0


def test_filtered_location_balance(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M1")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M1", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M1", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M1", quantity=4)],
        movement_date=datetime(2026, 8, 10, tzinfo=timezone.utc), note=None,
    ))
    svc.sale(SaleCreate(sku="SKU-M1", quantity=1, unit_price=2.0,
                        movement_date=datetime(2026, 9, 2, tzinfo=timezone.utc),
                        location_code="POS1", note=None))
    pos_id = db_session.query(Location.id).filter(
        Location.code == "POS1", Location.business_id == business_id).scalar()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M1",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
        location_id=pos_id,
    )
    rows = result["rows"]
    assert rows[0][2] == "Playa"
    assert rows[0][5] == 0.0
    assert rows[1][1] == "Traspaso"
    assert rows[1][2] == "← Almacén central"
    assert rows[1][3] == 4.0
    assert rows[1][5] == 4.0
    assert rows[2][1] == "Venta"
    assert rows[2][3] == -1.0
    assert rows[2][5] == 3.0


def test_transfer_with_two_entries_is_one_row(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M2")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M2", quantity=2, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
                                lot_code="L1", note=None))
    svc.purchase(PurchaseCreate(sku="SKU-M2", quantity=3, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 2, tzinfo=timezone.utc),
                                lot_code="L2", note=None))
    svc.transfer(TransferCreate(
        from_location_code="CENTRAL", to_location_code="POS1",
        lines=[TransferLineCreate(sku="SKU-M2", quantity=5)],
        movement_date=datetime(2026, 8, 3, tzinfo=timezone.utc), note=None,
    ))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M2",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert [r[1] for r in rows] == ["Saldo inicial", "Compra", "Compra", "Traspaso"]
    assert rows[3][3] == 5.0
    assert rows[3][5] == 5.0
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_reporting_product_movements.py -q --override-ini="addopts="
# Expected: 3 passed
```

#### Expected Report

- [ ] `tests/unit/test_reporting_product_movements.py` creado con las 3 funciones de test
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_reporting_product_movements.py -q --override-ini="addopts="` — 3 passed

---

### T6: `tests/e2e/test_agent_product_movements_e2e.py` — e2e sembrado (Lote 0)

**Depends on:** T4 (tool product_movements), T3 (ReportingService.product_movements)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_agent_product_movements_e2e.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/e2e/conftest.py — fixtures YA EXISTENTES (NO modificar):
#   central_server -> (base_url, db_path): servidor uvicorn real + _seed_stock (negocio "recambios")
#   js_errors -> autouse, necesita page; falla el test si hay errores reales de JS/CSP
#   page (pytest-playwright)

# app/agent/tools.py — TOOL_REGISTRY["product_movements"]["fn"](service, args) -> dict
# app/agent/widgets.py — add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by) -> DashboardWidget

# Widget tipo "table" se pinta en <div id="widget-{{ w.id }}"> con <table class="widget-table">.
```

#### Objective

Crear el e2e que siembra compra + traspaso + venta, ejecuta la tool real, guarda el widget y abre la pestaña del copiloto en Playwright.

#### Instructions

**Step 1: Create the file**
```
Write tests/e2e/test_agent_product_movements_e2e.py
```

**Step 2: Contenido literal completo**

```python
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.agent.tools import TOOL_REGISTRY
from app.agent.widgets import add_widget
from app.models import Business, Location, Product
from app.schemas import PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate
from app.services.inventory_service import InventoryService

pytestmark = pytest.mark.e2e

SKU = "E2E-MOV"


def _seed_and_run_tool(db_path: str) -> int:
    """Siembra compra + traspaso + venta del SKU, ejecuta la tool real y guarda el
    resultado como widget tipo table. Devuelve el id del widget."""
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        bid = int(biz.id)
        central = db.scalar(
            select(Location).where(Location.business_id == bid, Location.role == "central")
        )
        pos = db.scalar(
            select(Location).where(Location.business_id == bid, Location.id != central.id)
            .order_by(Location.id)
        )
        db.add(Product(business_id=bid, sku=SKU, name="Movimientos E2E", min_stock=0,
                       unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                       lead_time_days=0))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        now = datetime.now(timezone.utc)
        svc.purchase(PurchaseCreate(sku=SKU, quantity=10, unit_cost=1.0, movement_date=now,
                                    lot_code="LOT-E2E-MOV", note=None))
        svc.transfer(TransferCreate(from_location_code=central.code, to_location_code=pos.code,
                                    lines=[TransferLineCreate(sku=SKU, quantity=4)],
                                    movement_date=now, note=None))
        svc.sale(SaleCreate(sku=SKU, quantity=1, unit_price=2.0, movement_date=now,
                            location_code=pos.code, note=None))
        payload = TOOL_REGISTRY["product_movements"]["fn"](svc, {"sku": SKU})
        widget = add_widget(db, bid, type="table", title="Movimientos E2E", tool="product_movements",
                            tool_args={"sku": SKU}, payload=payload, unit=None, created_by=None)
        return int(widget.id)
    finally:
        db.close()
        engine.dispose()


def test_product_movements_table_initial_transfer_and_final_balance(central_server, page: Page) -> None:
    base_url, db_path = central_server
    wid = _seed_and_run_tool(db_path)

    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined'")
    page.locator("#tab-agent").click()

    table = page.locator(f"#widget-{wid} table.widget-table")
    table.wait_for(state="attached", timeout=10000)

    rows = table.locator("tbody tr")
    first_cells = rows.nth(0).locator("td").all_inner_texts()
    assert first_cells[1] == "Saldo inicial"

    all_cells = [c for i in range(rows.count()) for c in rows.nth(i).locator("td").all_inner_texts()]
    assert any("→" in c for c in all_cells)

    last_cells = rows.nth(rows.count() - 1).locator("td").all_inner_texts()
    assert last_cells[5] == "9.0"
```

**Edge cases (ya resueltos):**
- Errores de JS/CSP: el fixture autouse `js_errors` falla el test solo si hay errores reales.
- Los tres movimientos comparten `movement_date=now`; el orden por id da saldo final 9.0.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/e2e/test_agent_product_movements_e2e.py -q --no-cov --override-ini="addopts="
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/e2e/test_agent_product_movements_e2e.py` creado con 1 test
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/e2e/test_agent_product_movements_e2e.py -q --no-cov --override-ini="addopts="` — 1 passed

---

### T7: bordes del servicio en `test_reporting_product_movements.py`

**Depends on:** T5 (mismo fichero, camino feliz)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_reporting_product_movements.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/unit/test_reporting_product_movements.py — YA EXISTE (T5). Sus helpers:
#   _seed_locations(db_session, business_id) -> (central, pos)
#   _seed_product(db_session, business_id, sku) -> Product
# Imports ya presentes: datetime/timedelta/timezone, Business, InventoryLot, InventoryMovement,
#   Location, Product, PurchaseCreate/SaleCreate/TransferCreate/TransferLineCreate,
#   InventoryService, ReportingService
```

#### Objective

Añadir los casos de borde (entrada sin `out_id`, frontera `start`, aislamiento por negocio, límite 500, nota de descuadre) al mismo fichero.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_reporting_product_movements.py
```

**Step 2: Append las 5 funciones al final**

```python
def test_transfer_in_without_out_id_is_own_row(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M3")
    pos_id = db_session.query(Location.id).filter(
        Location.code == "POS1", Location.business_id == business_id).scalar()
    db_session.add(InventoryMovement(
        business_id=business_id, product_id=product.id, location_id=pos_id,
        type="transfer_in", quantity=3.0, unit_cost=1.0, unit_price=None,
        movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
        note="Transfer in from CENTRAL lot=L1",
    ))
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M3",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert rows[0][1] == "Saldo inicial"
    assert rows[1][1] == "Traspaso"
    assert rows[1][2] == "← Almacén central"
    assert rows[1][3] == 3.0


def test_movement_exactly_at_start_is_a_row_not_initial(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M4")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M4", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
                                lot_code="LOT-M4", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M4",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert rows[0][5] == 0.0
    assert rows[1][1] == "Compra"


def test_other_business_movements_invisible(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-ISO")
    other = Business(code="otro", name="Otro Negocio")
    db_session.add(other)
    db_session.commit()
    other_prod = Product(business_id=other.id, sku="SKU-OTRO", name="Compartido", min_stock=0,
                         unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                         lead_time_days=0)
    db_session.add(other_prod)
    db_session.flush()
    other_loc = Location(business_id=other.id, code="CENTRAL", name="Central")
    db_session.add(other_loc)
    db_session.flush()
    db_session.add(InventoryMovement(
        business_id=other.id, product_id=other_prod.id, location_id=other_loc.id,
        type="purchase", quantity=50.0, unit_cost=1.0, unit_price=None,
        movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc), note=None,
    ))
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-ISO",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    assert result["rows"] == [["", "Saldo inicial", "Todas", "", "", 0.0, ""]]


def test_limit_keeps_first_rows_and_adds_note(db_session, business_id, monkeypatch):
    monkeypatch.setattr("app.services.reporting_service.PRODUCT_MOVEMENTS_MAX_ROWS", 2)
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M5")
    svc = InventoryService(db_session, business_id)
    for i, day in enumerate((1, 2, 3), start=1):
        svc.purchase(PurchaseCreate(sku="SKU-M5", quantity=1, unit_cost=1.0,
                                    movement_date=datetime(2026, 8, day, tzinfo=timezone.utc),
                                    lot_code=f"LOT-M5-{i}", note=None))

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M5",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime(2026, 9, 30, tzinfo=timezone.utc),
    )
    rows = result["rows"]
    assert len(rows) == 3  # saldo inicial + 2 compras (de 3)
    assert rows[0][1] == "Saldo inicial"
    assert result["note"] == "Se muestran los 2 primeros movimientos de 3."


def test_note_when_balance_differs_from_lots(db_session, business_id):
    _seed_locations(db_session, business_id)
    product = _seed_product(db_session, business_id, "SKU-M6")
    svc = InventoryService(db_session, business_id)
    svc.purchase(PurchaseCreate(sku="SKU-M6", quantity=10, unit_cost=1.0,
                                movement_date=datetime(2026, 8, 5, tzinfo=timezone.utc),
                                lot_code="LOT-M6", note=None))
    lot = db_session.query(InventoryLot).filter(
        InventoryLot.product_id == product.id).one()
    lot.qty_remaining = 9.0
    db_session.commit()

    result = ReportingService(db_session, business_id).product_movements(
        product_id=product.id, sku="SKU-M6",
        start=datetime(2026, 8, 1, tzinfo=timezone.utc),
        end=datetime.now(timezone.utc) + timedelta(days=1),
    )
    assert result["note"] == "El saldo calculado (10.0) no coincide con el stock actual por lotes (9.0)."
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_reporting_product_movements.py -q --override-ini="addopts="
# Expected: 8 passed
```

#### Expected Report

- [ ] `tests/unit/test_reporting_product_movements.py` modificado — 5 funciones de test añadidas
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_reporting_product_movements.py -q --override-ini="addopts="` — 8 passed

---

### T8: tests de la tool en `test_agent_tools.py`

**Depends on:** T4 (tool product_movements)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_agent_tools.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/unit/test_agent_tools.py — YA EXISTE. Patrón ya presente en el fichero:
#   TOOL_REGISTRY["<name>"]["fn"](service, args)
#   importa: from app.agent.tools import TOOL_REGISTRY, tool_schemas
#            from app.models import InventoryMovement, Location, Product
#            from app.services.inventory_service import InventoryService
#   fixtures: db_session, business_id
```

#### Objective

Añadir los tests de la tool `product_movements` (SKU inexistente 404, ubicación inexistente 404, período por defecto) a `tests/unit/test_agent_tools.py`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_agent_tools.py
```

**Step 2: Append las 3 funciones al final**

```python
def test_product_movements_unknown_sku_raises_404(db_session, business_id):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException) as exc:
        TOOL_REGISTRY["product_movements"]["fn"](service, {"sku": "NOPE"})
    assert exc.value.status_code == 404


def test_product_movements_unknown_location_raises_404(db_session, business_id, sample_product):
    service = InventoryService(db_session, business_id)
    with pytest.raises(HTTPException) as exc:
        TOOL_REGISTRY["product_movements"]["fn"](
            service, {"sku": "SKU100001", "location": "NOPE"}
        )
    assert exc.value.status_code == 404


def test_product_movements_default_period(db_session, business_id):
    from datetime import timedelta

    p = Product(business_id=business_id, sku="PM-DEF", name="P", min_stock=0,
                unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                lead_time_days=0)
    loc = Location(business_id=business_id, code="CENTRAL", name="Central")
    db_session.add_all([p, loc])
    db_session.flush()
    now = datetime.now(timezone.utc)
    db_session.add(InventoryMovement(
        business_id=business_id, product_id=p.id, location_id=loc.id,
        type="purchase", quantity=7.0, unit_cost=1.0, unit_price=None,
        movement_date=now - timedelta(days=120), note=None,
    ))
    db_session.add(InventoryMovement(
        business_id=business_id, product_id=p.id, location_id=loc.id,
        type="purchase", quantity=3.0, unit_cost=1.0, unit_price=None,
        movement_date=now, note=None,
    ))
    db_session.commit()

    result = TOOL_REGISTRY["product_movements"]["fn"](
        InventoryService(db_session, business_id), {"sku": "PM-DEF"})
    rows = result["rows"]
    assert rows[0] == ["", "Saldo inicial", "Todas", "", "", 7.0, ""]
    assert len(rows) == 2
    assert rows[1][1] == "Compra"
    assert rows[1][5] == 10.0
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_agent_tools.py -q --override-ini="addopts="
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_agent_tools.py` modificado — 3 funciones de test añadidas
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_tools.py -q --override-ini="addopts="` — PASS

---

### T9: tests de repos en `test_repositories.py`

**Depends on:** T1 (quantity_before), T2 (labels)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_repositories.py`
**Pillars:** P1, P4, P5, P6, P9

#### Context

```python
# tests/unit/test_repositories.py — YA EXISTE. Cabecera actual (líneas 1–9):
#   from datetime import datetime, timedelta, timezone
#   from sqlalchemy.orm import Session
#   from app.models import Business, InventoryLot, InventoryMovement, Product
#   from app.repositories.inventory_repository import InventoryRepository, _query_match
#   from app.repositories.product_repository import ProductRepository
#   helper _create_product(db, business_id, sku, name) -> Product
```

#### Objective

Añadir imports (`Location`, `LocationRepository`) y los tests de `quantity_before` y `LocationRepository.labels` a `tests/unit/test_repositories.py`.

#### Instructions

**Step 1: Read the file**
```
Read tests/unit/test_repositories.py
```

**Step 2: Cambiar los imports (líneas 7–9)**

```python
# BEFORE (líneas 7–9):
from app.models import Business, InventoryLot, InventoryMovement, Product
from app.repositories.inventory_repository import InventoryRepository, _query_match
from app.repositories.product_repository import ProductRepository

# AFTER:
from app.models import Business, InventoryLot, InventoryMovement, Location, Product
from app.repositories.inventory_repository import InventoryRepository, _query_match
from app.repositories.location_repository import LocationRepository
from app.repositories.product_repository import ProductRepository
```

**Step 3: Append las 4 funciones al final**

```python
def test_quantity_before_frontier_and_sign(db_session: Session, business_id: int) -> None:
    product = _create_product(db_session, business_id, "SKU-QB", "Qb")
    repo = InventoryRepository(db_session, business_id=business_id)
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    db_session.add_all([
        InventoryMovement(business_id=business_id, product_id=product.id, location_id=None,
                          type="purchase", quantity=10, unit_cost=1, unit_price=None,
                          movement_date=base, note=None),
        InventoryMovement(business_id=business_id, product_id=product.id, location_id=None,
                          type="sale", quantity=-3, unit_cost=1, unit_price=5,
                          movement_date=base + timedelta(days=1), note=None),
    ])
    db_session.commit()
    assert repo.quantity_before(product.id, base) == 0.0
    assert repo.quantity_before(product.id, base + timedelta(hours=1)) == 10.0
    assert repo.quantity_before(product.id, base + timedelta(days=2)) == 7.0


def test_quantity_before_by_location(db_session: Session, business_id: int) -> None:
    product = _create_product(db_session, business_id, "SKU-QL", "Ql")
    central = Location(business_id=business_id, code="CENTRAL", name="Central")
    pos = Location(business_id=business_id, code="POS1", name="Pos")
    db_session.add_all([central, pos])
    db_session.flush()
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    db_session.add_all([
        InventoryMovement(business_id=business_id, product_id=product.id, location_id=central.id,
                          type="purchase", quantity=10, unit_cost=1, unit_price=None,
                          movement_date=base, note=None),
        InventoryMovement(business_id=business_id, product_id=product.id, location_id=pos.id,
                          type="purchase", quantity=5, unit_cost=1, unit_price=None,
                          movement_date=base + timedelta(days=1), note=None),
    ])
    db_session.commit()
    repo = InventoryRepository(db_session, business_id=business_id)
    after = base + timedelta(days=2)
    assert repo.quantity_before(product.id, after, location_id=pos.id) == 5.0
    assert repo.quantity_before(product.id, after) == 15.0


def test_quantity_before_scoped_to_business(db_session: Session, business_id: int) -> None:
    product = _create_product(db_session, business_id, "SKU-QISO", "Qiso")
    other = Business(code="otro", name="Otro")
    db_session.add(other)
    db_session.commit()
    other_prod = Product(business_id=other.id, sku="SKU-QOTRO", name="Otro", min_stock=0,
                         unit_of_measure="ud", default_purchase_cost=1.0, default_sale_price=2.0,
                         lead_time_days=0)
    db_session.add(other_prod)
    db_session.flush()
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    db_session.add(InventoryMovement(business_id=other.id, product_id=other_prod.id, location_id=None,
                                     type="purchase", quantity=50, unit_cost=1, unit_price=None,
                                     movement_date=base, note=None))
    db_session.commit()
    repo = InventoryRepository(db_session, business_id=business_id)
    assert repo.quantity_before(product.id, base + timedelta(days=1)) == 0.0


def test_location_repository_labels(db_session: Session, business_id: int) -> None:
    db_session.add_all([
        Location(business_id=business_id, code="CENTRAL", name="Almacén central"),
        Location(business_id=business_id, code="POS1", name=""),
    ])
    db_session.commit()
    repo = LocationRepository(db_session, business_id)
    by_id, by_code = repo.labels()
    central_id = db_session.query(Location.id).filter(
        Location.code == "CENTRAL", Location.business_id == business_id).scalar()
    assert by_id[int(central_id)] == "Almacén central"
    assert by_code["CENTRAL"] == "Almacén central"
    assert by_code["POS1"] == "POS1"
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_repositories.py -q --override-ini="addopts="
# Expected: todos PASS
```

#### Expected Report

- [ ] `tests/unit/test_repositories.py` modificado — imports + 4 funciones de test añadidas
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_repositories.py -q --override-ini="addopts="` — PASS

---

### T10: nota al pie en `widget_table.html` + test de escape

**Depends on:** none
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/features/agent/widget_table.html`, `tests/unit/test_ui_xss_escapes.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/templates/features/agent/widget_table.html — YA EXISTE (líneas 1–9). `w` es un dict con `.payload`.
# tests/unit/test_ui_xss_escapes.py — YA EXISTE; patrón:
#   from app.routers.ui_common import templates
#   html = templates.get_template("features/...").render(...)
#   assert "<script>" not in html / "&lt;script&gt;" in html
```

#### Objective

Mostrar `w.payload.note` bajo la tabla (autoescape) y añadir el test de escape en `test_ui_xss_escapes.py`.

#### Instructions

**Step 1: Read the files**
```
Read app/templates/features/agent/widget_table.html
Read tests/unit/test_ui_xss_escapes.py
```

**Step 2: Añadir la nota al pie en `widget_table.html`**

```html
<!-- BEFORE (líneas 1–9):
<table class="widget-table">
  <thead>
    <tr>{% for c in (w.payload.get('columns') or []) %}<th>{{ c }}</th>{% endfor %}</tr>
  </thead>
  <tbody>
    {% for row in (w.payload.get('rows') or []) %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
    {% if not (w.payload.get('rows') or []) %}<tr><td colspan="10" class="muted text-center">Sin resultados para el rango solicitado.</td></tr>{% endif %}
  </tbody>
</table>
-->

<!-- AFTER (añadir la última línea tras </table>):
<table class="widget-table">
  <thead>
    <tr>{% for c in (w.payload.get('columns') or []) %}<th>{{ c }}</th>{% endfor %}</tr>
  </thead>
  <tbody>
    {% for row in (w.payload.get('rows') or []) %}<tr>{% for cell in row %}<td>{{ cell }}</td>{% endfor %}</tr>{% endfor %}
    {% if not (w.payload.get('rows') or []) %}<tr><td colspan="10" class="muted text-center">Sin resultados para el rango solicitado.</td></tr>{% endif %}
  </tbody>
</table>
{% if w.payload.get('note') %}<p class="muted">{{ w.payload.get('note') }}</p>{% endif %}
-->

<!-- No añadir style= ni <style>: lo prohíbe test_frontend_css.py y la CSP. -->
```

**Step 3: Añadir el test de escape en `tests/unit/test_ui_xss_escapes.py`**

```python
def test_widget_table_note_is_escaped() -> None:
    from app.routers.ui_common import templates

    evil = '<script>alert("pm")</script>'
    w = {"payload": {"columns": ["A"], "rows": [[1]], "note": evil}}
    html = templates.get_template("features/agent/widget_table.html").render(w=w)
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_ui_xss_escapes.py::test_widget_table_note_is_escaped tests/unit/test_frontend_css.py -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/templates/features/agent/widget_table.html` modificado — nota al pie añadida
- [ ] `tests/unit/test_ui_xss_escapes.py` modificado — 1 test añadido
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_ui_xss_escapes.py::test_widget_table_note_is_escaped tests/unit/test_frontend_css.py -q --override-ini="addopts="` — PASS

---

### T11: regla de enrutado del prompt en `prompts.py`

**Depends on:** T4 (tool product_movements registrada)
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/agent/prompts.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/agent/prompts.py — YA EXISTE. Líneas 63–64 (regla actual de movement_history):
# - Para "últimos gastos" usa expenses_list (table); para "historial de movimientos/
#   traspasos/ajustes" usa movement_history (table).
```

#### Objective

Añadir la regla de `product_movements` antes de la regla de `movement_history` en `SYSTEM_PROMPT_TEMPLATE`.

#### Instructions

**Step 1: Read the file**
```
Read app/agent/prompts.py
```

**Step 2: Insertar la regla nueva antes de la línea 63**

```python
# BEFORE (líneas 63–64):
- Para "últimos gastos" usa expenses_list (table); para "historial de movimientos/
  traspasos/ajustes" usa movement_history (table).

# AFTER:
- Para "movimientos"/"historial"/"entradas y salidas"/"kárdex" de UN artículo concreto (código o
  nombre) usa product_movements (table) con sku=<código> y start/end del período pedido; si pide un
  punto de venta, pasa location. movement_history es para los últimos movimientos SIN artículo concreto.
- Para "últimos gastos" usa expenses_list (table); para "historial de movimientos/
  traspasos/ajustes" usa movement_history (table).
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_agent_tools.py::test_system_prompt_contains_current_date -q --override-ini="addopts="
# Expected: PASS
```

#### Expected Report

- [ ] `app/agent/prompts.py` modificado — regla nueva insertada antes de la de movement_history
- [ ] Ningún otro archivo modificado
- [ ] `pytest tests/unit/test_agent_tools.py::test_system_prompt_contains_current_date -q --override-ini="addopts="` — PASS

---

### T12: 3 casos de eval en `test_agent_tool_selection_eval.py`

**Depends on:** T11 (regla del prompt)
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/eval/test_agent_tool_selection_eval.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/eval/test_agent_tool_selection_eval.py — YA EXISTE. Lista GOLDEN_CASES (líneas 28–80).
# Formato de caso: {"id", "message", "expected_tool", "expected_type", "expected_args"?,
#                   "expected_args_present"?}
# El eval NO ejecuta la tool: solo comprueba tool/type/args que elige el LLM.
```

#### Objective

Añadir 3 casos a `GOLDEN_CASES` que verifiquen el enrutado a `product_movements`.

#### Instructions

**Step 1: Read the file**
```
Read tests/eval/test_agent_tool_selection_eval.py
```

**Step 2: Insertar los 3 casos antes del cierre de `GOLDEN_CASES` (línea 80, tras el caso `sales_documents_summary`)**

```python
# BEFORE (último caso + cierre, líneas 78–80):
    {"id": "sales_documents_summary", "message": "¿Cuánto facturé este mes comparado con lo presupuestado?",
     "expected_tool": "sales_documents_summary", "expected_type": {"kpi", "table"}, "date_range": "this_month"},
]

# AFTER:
    {"id": "sales_documents_summary", "message": "¿Cuánto facturé este mes comparado con lo presupuestado?",
     "expected_tool": "sales_documents_summary", "expected_type": {"kpi", "table"}, "date_range": "this_month"},
    {"id": "product_movements_last_3_months", "message": "movimientos del artículo SKU000190 de los últimos 3 meses",
     "expected_tool": "product_movements", "expected_type": {"table"}, "expected_args": {"sku": "SKU000190"}},
    {"id": "product_movements_kardex_period", "message": "kárdex de SKU000190 entre junio y septiembre",
     "expected_tool": "product_movements", "expected_type": {"table"}, "expected_args": {"sku": "SKU000190"}},
    {"id": "product_movements_location", "message": "movimientos de SKU000190 en Playa este mes",
     "expected_tool": "product_movements", "expected_type": {"table"},
     "expected_args": {"sku": "SKU000190"}, "expected_args_present": ["location"]},
]
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/eval/test_agent_tool_selection_eval.py --collect-only -q --override-ini="addopts="
# Expected: recoge 1 test (el eval completo); NO ejecutar contra el LLM aquí (lo corre la revisión)
```

#### Expected Report

- [ ] `tests/eval/test_agent_tool_selection_eval.py` modificado — 3 casos añadidos a `GOLDEN_CASES`
- [ ] Ningún otro archivo modificado
- [ ] `--collect-only` sin error de sintaxis

---

### T13: recuento de tools 34 → 35 en los dos `CLAUDE.md`

**Depends on:** T4 (tool registrada)
**Skill:** technical-writer
**Agent:** docs
**Files:** `.claude/CLAUDE.md`, `CLAUDE.md`
**Pillars:** P1, P4

#### Context

```text
# .claude/CLAUDE.md — línea 287 (Estado actual → Endpoints):
#   UI HTMX: dashboard completo + copiloto IA (34 tools) + tab Coach + sync banner
# CLAUDE.md (raíz) — línea 229 (entrada PAE-015):
#   título interno, reply engañoso). Tool `sales_by_location_monthly(months)` (34 tools), gráfico multi-serie
```

#### Objective

Actualizar el recuento de tools de 34 a 35 en los dos `CLAUDE.md`.

#### Instructions

**Step 1: Read the files**
```
Read .claude/CLAUDE.md
Read CLAUDE.md
```

**Step 2: Sustituir el recuento en ambos ficheros (solo estas dos ocurrencias)**

```text
# .claude/CLAUDE.md — BEFORE (línea 287):
  UI HTMX: dashboard completo + copiloto IA (34 tools) + tab Coach + sync banner
# .claude/CLAUDE.md — AFTER:
  UI HTMX: dashboard completo + copiloto IA (35 tools) + tab Coach + sync banner

# CLAUDE.md — BEFORE (línea 229):
      título interno, reply engañoso). Tool `sales_by_location_monthly(months)` (34 tools), gráfico multi-serie
# CLAUDE.md — AFTER:
      título interno, reply engañoso). Tool `sales_by_location_monthly(months)` (35 tools), gráfico multi-serie
```

**Edge cases (ya resueltos):**
- No tocar ninguna otra línea de ninguno de los dos ficheros.
- La aparición de `CLAUDE.md` línea 321 ("27 tools", histórico) NO se toca.

#### Verification

```bash
grep -n "34 tools" CLAUDE.md .claude/CLAUDE.md || echo "OK: sin '34 tools' restantes"
# Expected: OK: sin '34 tools' restantes
```

#### Expected Report

- [ ] `.claude/CLAUDE.md` modificado — `(34 tools)` → `(35 tools)` en la línea 287
- [ ] `CLAUDE.md` modificado — `(34 tools)` → `(35 tools)` en la línea 229
- [ ] Ningún otro archivo modificado
- [ ] `grep -n "34 tools" CLAUDE.md .claude/CLAUDE.md` no devuelve coincidencias

---

### T14: entrada 0.5.0 en `CHANGELOG.md`

**Depends on:** none
**Skill:** technical-writer
**Agent:** docs
**Files:** `CHANGELOG.md`
**Pillars:** P1, P4

#### Context

```text
# CHANGELOG.md — YA EXISTE. Sección actual (líneas 5–6):
# ## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)
# (bullets de la fase 5b, 5a, 4, permisos, fixes…)
```

#### Objective

Añadir la entrada de la feature a la sección 0.5.0 del `CHANGELOG.md`.

#### Instructions

**Step 1: Read the file**
```
Read CHANGELOG.md
```

**Step 2: Insertar el bullet como PRIMERA entrada de la sección 0.5.0 (tras la línea 5 y su línea en blanco)**

```text
# BEFORE (líneas 5–7):
## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)

- refactor(frontend): fase 5b — las consultas salen de los routers UI hacia repositorios/servicios y

# AFTER:
## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)

- feat(agent): nueva tool `product_movements` — movimientos de un artículo en un período (compras, ventas,
  traspasos, ajustes y devoluciones a proveedor) con fila de saldo inicial y saldo acumulado, filtro por
  ubicación y nota de descuadre contra el stock por lotes. 35 tools.
- refactor(frontend): fase 5b — las consultas salen de los routers UI hacia repositorios/servicios y
```

**Edge cases (ya resueltos):**
- No tocar los bullets existentes ni la línea 51 ("34 tools." del entry de `sales_by_location_monthly`, histórico).

#### Verification

```bash
git diff -- CHANGELOG.md | head -20
# Expected: solo muestra la adición del bullet nuevo
```

#### Expected Report

- [ ] `CHANGELOG.md` modificado — bullet de `product_movements` añadido al inicio de 0.5.0
- [ ] Ningún otro archivo modificado
- [ ] El diff solo añade el bullet nuevo
