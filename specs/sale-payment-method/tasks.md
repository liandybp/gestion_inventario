# Task Envelopes — Método de pago (transferencia) en venta rápida

> 14 envelopes, secuenciales por dependencia (T1→T2→T3/T4→T5→T6/T7→T8/T9/T10→T11/T12→T13/T14).
> Cada envelope es autocontenido: OpenCode no necesita leer `requirements.md` ni
> `design.md`, solo este archivo, un envelope a la vez.

---

### T1: Columna `paid_by_transfer` en el modelo `InventoryMovement`

**Depends on:** none
**Skill:** data-engineer
**Agent:** coder
**Files:** `app/models.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/models.py — ESTO YA EXISTE, NO MODIFICAR
class InventoryMovement(Base):
    __tablename__ = "inventory_movements"
    # ... columnas existentes: id, business_id, product_id, location_id, type,
    # quantity, unit_cost, unit_price, movement_date, note, created_at

# Boolean ya está importado en la línea 6:
from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, func

# Patrón ya usado en este mismo archivo para columnas booleanas (clase User, línea 64):
must_change_password: Mapped[bool] = mapped_column(
    Boolean, nullable=False, default=False, server_default="false"
)
```

#### Objective

Añadir la columna `paid_by_transfer` (booleano, default `False`) a `InventoryMovement`.

#### Instructions

**Step 1: Read the file**
```
Read app/models.py
```

**Step 2: Apply the change** (dentro de `class InventoryMovement`, justo después de `unit_price`)

```python
# BEFORE:
    unit_price: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    movement_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )

# AFTER:
    unit_price: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    paid_by_transfer: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    movement_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
```

**Imports needed:** ninguno (`Boolean` ya está importado en la línea 6 del archivo).

**Edge cases (ya resueltos):**
- Este campo solo tiene significado cuando `type == "sale"`. En otros tipos de movimiento
  (purchase/transfer/adjustment) queda `False` sin uso — mismo patrón que `unit_price`,
  que ya queda `NULL`/sin uso en movimientos que no son venta.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -v
# Expected: todos los tests existentes PASS (el modelo nuevo no rompe nada — Base.metadata.create_all crea la columna)
python3 -c "from app.models import InventoryMovement; assert hasattr(InventoryMovement, 'paid_by_transfer'); print('OK')"
# Expected: OK
```

#### Expected Report

- [ ] `app/models.py` modificado — columna `paid_by_transfer` añadida a `InventoryMovement`
- [ ] Ningún otro archivo modificado
- [ ] Ambos comandos de verificación — PASS

---

### T2: Migración de arranque (SQLite + PostgreSQL)

**Depends on:** T1
**Skill:** data-engineer
**Agent:** data
**Files:** `app/migrations.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/migrations.py — ESTO YA EXISTE, NO MODIFICAR
# Patrón idéntico ya usado para la columna location_id de esta misma tabla,
# en las dos ramas de migración (sqlite y postgresql).
```

#### Objective

Agregar la columna `paid_by_transfer` a `inventory_movements` de forma idempotente en
ambas ramas de migración (SQLite y PostgreSQL).

#### Instructions

**Step 1: Read the file**
```
Read app/migrations.py
```

**Step 2a: Rama SQLite** — dentro de `_run_sqlite_schema_migrations()`, justo después del
bloque que agrega `location_id` a `inventory_movements`:

```python
# BEFORE:
        mv_cols = {
            row[1]
            for row in conn.exec_driver_sql(
                "PRAGMA table_info(inventory_movements)"
            ).fetchall()
        }
        if "location_id" not in mv_cols:
            conn.exec_driver_sql(
                "ALTER TABLE inventory_movements ADD COLUMN location_id INTEGER"
            )

# AFTER:
        mv_cols = {
            row[1]
            for row in conn.exec_driver_sql(
                "PRAGMA table_info(inventory_movements)"
            ).fetchall()
        }
        if "location_id" not in mv_cols:
            conn.exec_driver_sql(
                "ALTER TABLE inventory_movements ADD COLUMN location_id INTEGER"
            )
        if "paid_by_transfer" not in mv_cols:
            conn.exec_driver_sql(
                "ALTER TABLE inventory_movements ADD COLUMN paid_by_transfer BOOLEAN DEFAULT 0"
            )
```

**Step 2b: Rama PostgreSQL** — dentro de `_run_postgresql_schema_migrations()`, justo
después del bloque `try/except` que agrega `location_id` a `inventory_movements`:

```python
# BEFORE:
        try:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns WHERE table_name='inventory_movements' AND column_name='location_id'"
            ).fetchone()
            if exists is None:
                conn.exec_driver_sql(
                    "ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS location_id INTEGER"
                )
                conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_inventory_movements_location_id ON inventory_movements(location_id)"
                )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna location_id en inventory_movements."
            ) from e

# AFTER:
        try:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns WHERE table_name='inventory_movements' AND column_name='location_id'"
            ).fetchone()
            if exists is None:
                conn.exec_driver_sql(
                    "ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS location_id INTEGER"
                )
                conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_inventory_movements_location_id ON inventory_movements(location_id)"
                )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna location_id en inventory_movements."
            ) from e

        try:
            exists = conn.exec_driver_sql(
                "SELECT 1 FROM information_schema.columns WHERE table_name='inventory_movements' AND column_name='paid_by_transfer'"
            ).fetchone()
            if exists is None:
                conn.exec_driver_sql(
                    "ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS paid_by_transfer BOOLEAN NOT NULL DEFAULT FALSE"
                )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna paid_by_transfer en inventory_movements."
            ) from e
```

**Imports needed:** ninguno (`SQLAlchemyError` ya está importado en la línea 8).

**Edge cases (ya resueltos):**
- Ejecutar la migración dos veces (dos arranques seguidos) no falla — ambas ramas
  verifican existencia de la columna antes de agregarla (`not in mv_cols` / `exists is None`).
- Filas existentes quedan con `paid_by_transfer = False` (el `DEFAULT`/`DEFAULT FALSE` de
  la sentencia `ALTER TABLE ADD COLUMN` se aplica a las filas ya existentes, sin backfill manual).

#### Verification

```bash
rm -f /tmp/mig_test_paid_by_transfer.db
DATABASE_URL=sqlite+pysqlite:////tmp/mig_test_paid_by_transfer.db SESSION_SECRET=test \
ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
python3 -c "
from app.migrations import run_startup_tasks
from app.db import engine
run_startup_tasks()
with engine.begin() as conn:
    cols = {row[1] for row in conn.exec_driver_sql('PRAGMA table_info(inventory_movements)').fetchall()}
    assert 'paid_by_transfer' in cols, cols
run_startup_tasks()  # segunda corrida: debe ser idempotente, sin error
print('paid_by_transfer column present after 2 runs: OK')
"
rm -f /tmp/mig_test_paid_by_transfer.db
# Expected: "paid_by_transfer column present after 2 runs: OK"
```

#### Expected Report

- [ ] `app/migrations.py` modificado — bloque SQLite y bloque PostgreSQL, ambos con el
      check idempotente para `paid_by_transfer`
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — "OK" impreso, sin excepción

---

### T3: Schemas `SaleCreate` y `MovementRead`

**Depends on:** T1
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/schemas.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/schemas.py — ESTO YA EXISTE, NO MODIFICAR
class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None
    location_code: Optional[str] = None

class MovementRead(BaseModel):
    id: int
    product_id: int
    location_id: Optional[int] = None
    type: str
    quantity: float
    unit_cost: Optional[float]
    unit_price: Optional[float]
    movement_date: datetime
    note: Optional[str]

    model_config = {"from_attributes": True}
```

#### Objective

Añadir `paid_by_transfer` a `SaleCreate` (default `False`) y a `MovementRead`.

#### Instructions

**Step 1: Read the file**
```
Read app/schemas.py
```

**Step 2: `SaleCreate`**

```python
# BEFORE:
class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None
    location_code: Optional[str] = None

# AFTER:
class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None
    location_code: Optional[str] = None
    paid_by_transfer: bool = False
```

**Step 3: `MovementRead`**

```python
# BEFORE:
    unit_cost: Optional[float]
    unit_price: Optional[float]
    movement_date: datetime
    note: Optional[str]

    model_config = {"from_attributes": True}

# AFTER:
    unit_cost: Optional[float]
    unit_price: Optional[float]
    paid_by_transfer: bool
    movement_date: datetime
    note: Optional[str]

    model_config = {"from_attributes": True}
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `SaleCreate(sku="A1", quantity=1)` sin pasar `paid_by_transfer` es válido y resuelve a
  `False` (default de Pydantic, no requiere validador).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_schemas.py -v
# Expected: todos los tests existentes PASS
```

#### Expected Report

- [ ] `app/schemas.py` modificado — `paid_by_transfer` en `SaleCreate` y `MovementRead`
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T4: `InventoryRepository.recent_sales()` — columna + filtro

**Depends on:** T1
**Skill:** data-engineer
**Agent:** coder
**Files:** `app/repositories/inventory_repository.py`
**Pillars:** P1, P2, P4, P5, P6

#### Context

```python
# app/repositories/inventory_repository.py — ESTO YA EXISTE, NO MODIFICAR
# InventoryMovement.paid_by_transfer ya existe (añadido en T1).
```

#### Objective

Añadir el parámetro `payment_method` y la columna `paid_by_transfer` al método
`recent_sales()`.

#### Instructions

**Step 1: Read the file**
```
Read app/repositories/inventory_repository.py
```

**Step 2: Firma del método**

```python
# BEFORE:
    def recent_sales(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[tuple]:

# AFTER:
    def recent_sales(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
        payment_method: Optional[str] = None,
    ) -> list[tuple]:
```

**Step 3: Añadir la columna al `select()`** (al final de la lista de columnas)

```python
# BEFORE:
                InventoryMovement.unit_price,
                Location.name,
                lot_codes_expr,
            )

# AFTER:
                InventoryMovement.unit_price,
                Location.name,
                lot_codes_expr,
                InventoryMovement.paid_by_transfer,
            )
```

**Step 4: Añadir el filtro** (justo después del filtro `month`/`year`, antes de ejecutar el query)

```python
# BEFORE:
        if month and year:
            stmt = stmt.where(
                func.extract('year', InventoryMovement.movement_date) == year,
                func.extract('month', InventoryMovement.movement_date) == int(month)
            )
        rows = list(self._db.execute(stmt).all())

# AFTER:
        if month and year:
            stmt = stmt.where(
                func.extract('year', InventoryMovement.movement_date) == year,
                func.extract('month', InventoryMovement.movement_date) == int(month)
            )
        if payment_method == "transfer":
            stmt = stmt.where(InventoryMovement.paid_by_transfer.is_(True))
        elif payment_method == "cash":
            stmt = stmt.where(InventoryMovement.paid_by_transfer.is_(False))
        rows = list(self._db.execute(stmt).all())
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `payment_method` en cualquier otro valor (`None`, `""`, o algo distinto de
  `"transfer"`/`"cash"`) no aplica ningún filtro adicional — se listan todas las ventas,
  igual que hoy.
- La columna nueva se agrega AL FINAL de la tupla seleccionada (después de `lot_codes_expr`)
  — no reordena las posiciones ya usadas por `sales_history.html` (verificado: es el único
  template que desempaqueta esta tupla por posición).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -k "recent_sales or sale" -v
# Expected: todos PASS (test_recent_sales_filters_by_payment_method se añade en T11, aún fallará con NameError/AttributeError hasta T5 — ignorar en esta verificación, correr solo tests que ya existen)
```

#### Expected Report

- [ ] `app/repositories/inventory_repository.py` modificado — firma, `select()`, filtro
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS (tests existentes, sin regresión)

---

### T5: `InventoryService` — `sale()`, `update_sale()`, `recent_sales()`

**Depends on:** T3, T4
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/inventory_service.py`
**Pillars:** P1, P2, P4, P5, P6

#### Context

```python
# app/schemas.py — YA EXISTE (T3)
class SaleCreate(BaseModel):
    ...
    paid_by_transfer: bool = False

# app/repositories/inventory_repository.py — YA EXISTE (T4)
def recent_sales(self, ..., payment_method: Optional[str] = None) -> list[tuple]: ...

# app/routers/ui_profit_items.py — YA EXISTE, NO MODIFICAR (fuera de scope de esta feature)
# Llama a service.update_sale(...) SIN pasar paid_by_transfer:
result = service.update_sale(
    movement_id=movement_id, sku=sku, quantity=quantity, unit_price=unit_price,
    movement_date=parse_dt(movement_date), note=note or None,
)
```

#### Objective

Propagar `paid_by_transfer` a través de `sale()`, `update_sale()` (sin romper el caller de
`ui_profit_items.py`) y `recent_sales()`.

#### Instructions

**Step 1: Read the file**
```
Read app/services/inventory_service.py
```

**Step 2: `sale()`** — dentro del constructor de `InventoryMovement`

```python
# BEFORE:
        movement = InventoryMovement(
            business_id=self._business_id,
            product_id=product.id,
            location_id=loc_id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=unit_price,
            movement_date=movement_dt,
            note=payload.note,
        )

# AFTER:
        movement = InventoryMovement(
            business_id=self._business_id,
            product_id=product.id,
            location_id=loc_id,
            type="sale",
            quantity=-payload.quantity,
            unit_cost=None,
            unit_price=unit_price,
            paid_by_transfer=payload.paid_by_transfer,
            movement_date=movement_dt,
            note=payload.note,
        )
```

**Step 3: `update_sale()`** — firma y cuerpo

```python
# BEFORE:
    def update_sale(
        self,
        movement_id: int,
        sku: str,
        quantity: float,
        unit_price: float,
        movement_date: Optional[datetime],
        note: Optional[str],
    ) -> MovementResult:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "sale":
            raise HTTPException(status_code=404, detail="Sale movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")

        old_product_id = mv.product_id
        product = self._get_product(sku)
        mv.product_id = product.id
        mv.quantity = -quantity
        mv.unit_price = unit_price
        mv.movement_date = self._movement_datetime(movement_date)
        mv.note = note

# AFTER:
    def update_sale(
        self,
        movement_id: int,
        sku: str,
        quantity: float,
        unit_price: float,
        movement_date: Optional[datetime],
        note: Optional[str],
        paid_by_transfer: Optional[bool] = None,
    ) -> MovementResult:
        self._invalidate_metrics_cache()
        mv = self._db.get(InventoryMovement, movement_id)
        if mv is None or mv.type != "sale":
            raise HTTPException(status_code=404, detail="Sale movement not found")

        if quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")
        if unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")

        old_product_id = mv.product_id
        product = self._get_product(sku)
        mv.product_id = product.id
        mv.quantity = -quantity
        mv.unit_price = unit_price
        if paid_by_transfer is not None:
            mv.paid_by_transfer = paid_by_transfer
        mv.movement_date = self._movement_datetime(movement_date)
        mv.note = note
```

**Step 4: `recent_sales()`** — passthrough

```python
# BEFORE:
    def recent_sales(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
    ) -> list[tuple]:
        return self._inventory.recent_sales(
            query=query,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            month=month,
            year=year,
            location_id=location_id,
        )

# AFTER:
    def recent_sales(
        self,
        query: str = "",
        limit: int = 20,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        month: Optional[str] = None,
        year: Optional[int] = None,
        location_id: Optional[int] = None,
        payment_method: Optional[str] = None,
    ) -> list[tuple]:
        return self._inventory.recent_sales(
            query=query,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            month=month,
            year=year,
            location_id=location_id,
            payment_method=payment_method,
        )
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `update_sale(..., paid_by_transfer=None)` (default, o el caller de
  `ui_profit_items.py` que ni siquiera pasa el argumento) **NO** toca
  `mv.paid_by_transfer` — el valor existente se conserva. Solo se sobreescribe cuando el
  caller pasa explícitamente `True` o `False`. Esto evita que editar una venta desde el
  tab "Ganancia por artículo" resetee el método de pago a `False` sin que el usuario lo
  haya tocado (ver D3 en `design.md`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -v
# Expected: todos los tests existentes PASS (los tests nuevos de T11 aún no existen)
```

#### Expected Report

- [ ] `app/services/inventory_service.py` modificado — `sale()`, `update_sale()`,
      `recent_sales()`
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T6: `ui_sales.py` — `sale()` y `sale_barcode()` (creación)

**Depends on:** T5
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/schemas.py — YA EXISTE (T3)
class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None
    location_code: Optional[str] = None
    paid_by_transfer: bool = False

# app/routers/ui_products.py — patrón YA EXISTE en el proyecto para checkbox de creación:
initial_inventory: bool = Form(False),
```

#### Objective

Aceptar `paid_by_transfer` en `POST /ui/sale` y `POST /ui/sale/barcode`, y pasarlo a
`SaleCreate`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales.py
```

**Step 2: Firma de `sale()`** (función que empieza en `@router.post("/sale", ...)`, ~línea 161)

```python
# BEFORE:
@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:

# AFTER:
@router.post("/sale", response_class=HTMLResponse)
def sale(
    request: Request,
    product: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
```

**Step 3: `SaleCreate(...)` dentro de `sale()`** — este bloque exacto aparece DOS veces en
el archivo (una en `sale()`, otra en `sale_barcode()`). En esta tarea, edita **solo la
primera ocurrencia** (dentro de la función `sale()`, que ya editaste en el Step 2):

```python
# BEFORE:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
            )
        )

# AFTER:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
                paid_by_transfer=paid_by_transfer,
            )
        )
```

**Step 4: Firma de `sale_barcode()`** (función que empieza en `@router.post("/sale/barcode", ...)`, ~línea 241)

```python
# BEFORE:
@router.post("/sale/barcode", response_class=HTMLResponse)
def sale_barcode(
    request: Request,
    barcode: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:

# AFTER:
@router.post("/sale/barcode", response_class=HTMLResponse)
def sale_barcode(
    request: Request,
    barcode: str = Form(...),
    location_code: str = Form(""),
    quantity: float = Form(...),
    unit_price: str = Form(""),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
```

**Step 5: `SaleCreate(...)` dentro de `sale_barcode()`** — es la **segunda ocurrencia**
del mismo bloque exacto del Step 3 (dentro de `sale_barcode()`, después de la línea
`sku = barcode_to_sku(db, barcode, business_id=bid)`). Aplica el mismo cambio:

```python
# BEFORE (segunda ocurrencia en el archivo):
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
            )
        )

# AFTER:
        result = service.sale(
            SaleCreate(
                sku=sku,
                quantity=quantity,
                unit_price=parse_optional_float(unit_price),
                movement_date=parse_dt(movement_date),
                note=note or None,
                location_code=selected_location_code,
                paid_by_transfer=paid_by_transfer,
            )
        )
```

**Imports needed:** ninguno (`Form` ya está importado en la línea 8).

**Edge cases (ya resueltos):**
- Checkbox no marcado en el HTML ⇒ el navegador no envía el campo en el POST ⇒
  `Form(False)` resuelve a `False` (mismo comportamiento que `initial_inventory` en
  `ui_products.py`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: todos los tests existentes PASS
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` modificado — `sale()` (firma + `SaleCreate`) y
      `sale_barcode()` (firma + `SaleCreate`)
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T7: `ui_sales.py` — `sale_update()` y `tab_sales()` (edición + filtro)

**Depends on:** T6
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/routers/ui_sales.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/services/inventory_service.py — YA EXISTE (T5)
def update_sale(self, movement_id: int, sku: str, quantity: float, unit_price: float,
                 movement_date: Optional[datetime], note: Optional[str],
                 paid_by_transfer: Optional[bool] = None) -> MovementResult: ...
def recent_sales(self, ..., payment_method: Optional[str] = None) -> list[tuple]: ...
```

#### Objective

Aceptar `paid_by_transfer` en `POST /movement/sale/{id}/update` y `payment_method` en
`GET /tabs/sales`.

#### Instructions

**Step 1: Read the file**
```
Read app/routers/ui_sales.py
```

**Step 2: Firma de `sale_update()`** (~línea 366)

```python
# BEFORE:
@router.post("/movement/sale/{movement_id}/update", response_class=HTMLResponse)
def sale_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    db: Session = Depends(session_dep),
) -> HTMLResponse:

# AFTER:
@router.post("/movement/sale/{movement_id}/update", response_class=HTMLResponse)
def sale_update(
    request: Request,
    movement_id: int,
    product: str = Form(...),
    quantity: float = Form(...),
    unit_price: float = Form(...),
    movement_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    paid_by_transfer: bool = Form(False),
    db: Session = Depends(session_dep),
) -> HTMLResponse:
```

**Step 3: Llamada a `service.update_sale(...)` dentro de `sale_update()`**

```python
# BEFORE:
        result = service.update_sale(
            movement_id=movement_id,
            sku=sku,
            quantity=quantity,
            unit_price=unit_price,
            movement_date=parse_dt(movement_date),
            note=note or None,
        )

# AFTER:
        result = service.update_sale(
            movement_id=movement_id,
            sku=sku,
            quantity=quantity,
            unit_price=unit_price,
            movement_date=parse_dt(movement_date),
            note=note or None,
            paid_by_transfer=paid_by_transfer,
        )
```

**Step 4: Firma de `tab_sales()`** (~línea 627)

```python
# BEFORE:
@router.get("/tabs/sales", response_class=HTMLResponse)
def tab_sales(
    request: Request,
    month: Optional[str] = None,
    year: Optional[int] = None,
    show_all: Optional[str] = None,
    location_code: str = "",
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:

# AFTER:
@router.get("/tabs/sales", response_class=HTMLResponse)
def tab_sales(
    request: Request,
    month: Optional[str] = None,
    year: Optional[int] = None,
    show_all: Optional[str] = None,
    location_code: str = "",
    query: str = "",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    payment_method: Optional[str] = None,
    db: Session = Depends(session_dep)
) -> HTMLResponse:
```

**Step 5: Llamada a `inventory_service.recent_sales(...)` dentro de `tab_sales()`**

```python
# BEFORE:
            "sales": inventory_service.recent_sales(
                query=query,
                limit=100,
                start_date=start_dt,
                end_date=end_dt,
                month=filter_month,
                year=filter_year,
                location_id=selected_filter_location_id,
            ),

# AFTER:
            "sales": inventory_service.recent_sales(
                query=query,
                limit=100,
                start_date=start_dt,
                end_date=end_dt,
                month=filter_month,
                year=filter_year,
                location_id=selected_filter_location_id,
                payment_method=(payment_method or "").strip() or None,
            ),
```

**Step 6: Añadir `filter_payment_method` al contexto del template** (mismo `context={...}` del Step 5)

```python
# BEFORE:
            "filter_location_code": selected_filter_location_code,
            "product_options": product_options,

# AFTER:
            "filter_location_code": selected_filter_location_code,
            "filter_payment_method": (payment_method or "").strip(),
            "product_options": product_options,
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):**
- `payment_method=""` o ausente ⇒ `(payment_method or "").strip() or None` resuelve a
  `None` ⇒ sin filtro, se listan todas las ventas.
- Valores fuera de `"transfer"`/`"cash"` se pasan igual al service/repo, que los ignora
  (ver Edge case de T4) — sin error 422 ni excepción.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: todos los tests existentes PASS
```

#### Expected Report

- [ ] `app/routers/ui_sales.py` modificado — `sale_update()` (firma + llamada) y
      `tab_sales()` (firma + llamada + contexto)
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T8: `sale_panel.html` — checkbox de creación + filtro de historial

**Depends on:** T6, T7
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/sale_panel.html`
**Pillars:** P1, P4, P6

#### Context

```
# app/templates/partials/product_edit_form_inventory.html — patrón YA EXISTE en el
# proyecto para checkbox desmarcado por defecto:
<input name="initial_inventory" type="checkbox" value="1" />
```

#### Objective

Añadir el checkbox "Pagado por transferencia" al form de creación y el filtro de método
de pago al form de historial.

#### Instructions

**Step 1: Read the file**
```
Read app/templates/partials/sale_panel.html
```

**Step 2: Checkbox en el form de creación**

```html
<!-- BEFORE: -->
      <div class="row"><label>Nota</label><input name="note" placeholder="(opcional)" /></div>
      <button type="submit">Registrar venta</button>

<!-- AFTER: -->
      <div class="row"><label>Nota</label><input name="note" placeholder="(opcional)" /></div>
      <div class="row"><label>Pagado por transferencia</label><input name="paid_by_transfer" type="checkbox" value="1" /></div>
      <button type="submit">Registrar venta</button>
```

**Step 3: `hx-trigger` del form de filtro** — agregar `change from:#sales-filter-payment`

```html
<!-- BEFORE: -->
        hx-trigger="submit, keyup changed delay:300ms from:#sales-filter-query, change from:#sales-filter-start, change from:#sales-filter-end, change from:#sales-filter-location, change from:#sales-filter-show-all">

<!-- AFTER: -->
        hx-trigger="submit, keyup changed delay:300ms from:#sales-filter-query, change from:#sales-filter-start, change from:#sales-filter-end, change from:#sales-filter-location, change from:#sales-filter-payment, change from:#sales-filter-show-all">
```

**Step 4: Select de filtro** — insertarlo antes del filtro "Desde"

```html
<!-- BEFORE: -->
      <div>
        <label class="filter-label">Desde</label>
        <input id="sales-filter-start" type="date" name="start_date" value="{{ filter_start_date or '' }}" />
      </div>

<!-- AFTER: -->
      <div>
        <label class="filter-label">Método de pago</label>
        <select id="sales-filter-payment" name="payment_method">
          <option value="" {% if not (filter_payment_method or '') %}selected{% endif %}>Todos</option>
          <option value="transfer" {% if (filter_payment_method or '') == 'transfer' %}selected{% endif %}>Transferencia</option>
          <option value="cash" {% if (filter_payment_method or '') == 'cash' %}selected{% endif %}>Efectivo/otro</option>
        </select>
      </div>
      <div>
        <label class="filter-label">Desde</label>
        <input id="sales-filter-start" type="date" name="start_date" value="{{ filter_start_date or '' }}" />
      </div>
```

**Edge cases (ya resueltos):**
- Checkbox de creación SIN `checked` — nace desmarcado (REQ-3), consistente con el patrón
  ya usado en `product_edit_form_inventory.html`.
- `filter_payment_method` no está en el contexto de todos los renders posibles de este
  partial (ej. el primer render de `sale()`/`sale_barcode()` reusa `_sales_doc_context`
  pero no pasa `filter_payment_method`) — el `{% if not (filter_payment_method or '') %}`
  maneja `undefined`/`None`/`""` igual, Jinja2 no lanza error por variable ausente en un
  `{% if %}` con `or`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: todos los tests existentes PASS (los templates no tienen test unitario directo, pero no deben romper el render de ui_sales.py)
```

#### Expected Report

- [ ] `app/templates/partials/sale_panel.html` modificado — checkbox, `hx-trigger`, select
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T9: `sale_edit_form.html` — checkbox de edición

**Depends on:** T7
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/sale_edit_form.html`
**Pillars:** P1, P4, P6

#### Context

```python
# app/routers/ui_sales.py — YA EXISTE, NO MODIFICAR
# sale_edit_form() pasa "movement": mv al template, donde mv es el ORM InventoryMovement
# (con paid_by_transfer ya disponible desde T1) — no MovementRead.
```

#### Objective

Añadir el checkbox "Pagado por transferencia" al form de edición, precargado con el
valor actual del movimiento.

#### Instructions

**Step 1: Read the file**
```
Read app/templates/partials/sale_edit_form.html
```

**Step 2: Apply the change**

```html
<!-- BEFORE: -->
        <div class="row"><label>Nota</label><input name="note" value="{{ movement.note or '' }}" /></div>

        <button type="submit">Guardar cambios</button>

<!-- AFTER: -->
        <div class="row"><label>Nota</label><input name="note" value="{{ movement.note or '' }}" /></div>
        <div class="row"><label>Pagado por transferencia</label><input name="paid_by_transfer" type="checkbox" value="1" {% if movement.paid_by_transfer %}checked{% endif %} /></div>

        <button type="submit">Guardar cambios</button>
```

**Edge cases (ya resueltos):**
- `movement.paid_by_transfer` siempre existe como `True`/`False` tras T1+T2 (columna
  `NOT NULL`) — no hay caso `None` que evaluar.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: todos los tests existentes PASS
```

#### Expected Report

- [ ] `app/templates/partials/sale_edit_form.html` modificado — checkbox añadido
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T10: `sales_history.html` — columna nueva + tupla

**Depends on:** T4
**Skill:** frontend-developer
**Agent:** coder
**Files:** `app/templates/partials/sales_history.html`
**Pillars:** P1, P4, P6

#### Context

```python
# app/repositories/inventory_repository.py — YA EXISTE (T4)
# recent_sales() ahora devuelve tuplas de 11 elementos, con paid_by_transfer al final:
# (id, movement_date, sku, name, unit_of_measure, image_url, quantity, unit_price,
#  location_name, lot_codes, paid_by_transfer)
```

#### Objective

Mostrar el método de pago por fila en la tabla de historial de ventas.

#### Instructions

**Step 1: Read the file**
```
Read app/templates/partials/sales_history.html
```

**Step 2: Header de la tabla**

```html
<!-- BEFORE: -->
      <th>Punto de venta</th>
      <th>Acciones</th>

<!-- AFTER: -->
      <th>Punto de venta</th>
      <th>Pago</th>
      <th>Acciones</th>
```

**Step 3: Desempaquetado de la tupla**

```html
<!-- BEFORE: -->
    {% for id, d, sku, name, uom, image_url, qty, unit_price, pos_name, lot_codes in sales %}
      <tr>

<!-- AFTER: -->
    {% for id, d, sku, name, uom, image_url, qty, unit_price, pos_name, lot_codes, paid_by_transfer in sales %}
      <tr>
```

**Step 4: Celda nueva** (entre la celda de "Punto de venta" y la de "Acciones")

```html
<!-- BEFORE: -->
        <td>
          {{ pos_name or '' }}
        </td>
        <td>
          <button type="button"

<!-- AFTER: -->
        <td>
          {{ pos_name or '' }}
        </td>
        <td>
          {% if paid_by_transfer %}Transferencia{% else %}Efectivo/otro{% endif %}
        </td>
        <td>
          <button type="button"
```

**Edge cases (ya resueltos):**
- Este template es el ÚNICO consumidor de la tupla de `recent_sales()` (verificado con
  grep en todo `app/templates`) — no hay otro lugar que romper por el nuevo elemento al
  final de la tupla.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -v
# Expected: todos los tests existentes PASS
```

#### Expected Report

- [ ] `app/templates/partials/sales_history.html` modificado — header, desempaquetado, celda
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — PASS

---

### T11: Tests unitarios

**Depends on:** T5
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_inventory_service_unit.py`, `tests/unit/test_schemas.py`
**Pillars:** P1, P4, P5, P6, P7

#### Context

```python
# tests/unit/test_inventory_service_unit.py — YA EXISTE, NO MODIFICAR
def _create_product(db: Session, business_id: int) -> Product: ...  # sku="SKU-TST", default_sale_price=2.0
def _create_locations(db: Session, business_id: int) -> tuple[Location, Location]: ...  # (CENTRAL, POS1)

# app/schemas.py — YA EXISTE (T3): SaleCreate(..., paid_by_transfer: bool = False)
# app/services/inventory_service.py — YA EXISTE (T5):
#   sale(payload: SaleCreate) -> MovementResult
#   update_sale(..., paid_by_transfer: Optional[bool] = None) -> MovementResult
#   recent_sales(..., payment_method: Optional[str] = None) -> list[tuple]
```

#### Objective

Cubrir REQ-1, REQ-2, REQ-4, REQ-6 con tests unitarios de `InventoryService`.

#### Instructions

**Step 1: Read both files**
```
Read tests/unit/test_inventory_service_unit.py
Read tests/unit/test_schemas.py
```

**Step 2: `tests/unit/test_inventory_service_unit.py`** — insertar entre
`test_sale_supports_central_location` y `test_sale_fails_if_location_has_no_enough_stock_even_with_stock_elsewhere`

```python
# BEFORE (las dos funciones ya existen, una seguida de la otra):
    assert result.stock_after == 3


def test_sale_fails_if_location_has_no_enough_stock_even_with_stock_elsewhere(db_session: Session, business_id: int) -> None:

# AFTER (se insertan 5 funciones nuevas entre ambas):
    assert result.stock_after == 3


def test_sale_defaults_paid_by_transfer_to_false(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)

    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))
    result = service.sale(SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL"))

    assert result.movement.paid_by_transfer is False


def test_sale_persists_paid_by_transfer_true(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)

    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))
    result = service.sale(
        SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL", paid_by_transfer=True)
    )

    assert result.movement.paid_by_transfer is True


def test_update_sale_changes_paid_by_transfer(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)

    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))
    result = service.sale(SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL"))

    updated = service.update_sale(
        movement_id=result.movement.id,
        sku="SKU-TST",
        quantity=1,
        unit_price=2.0,
        movement_date=None,
        note=None,
        paid_by_transfer=True,
    )

    assert updated.movement.paid_by_transfer is True


def test_update_sale_keeps_paid_by_transfer_when_not_passed(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)

    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))
    result = service.sale(
        SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL", paid_by_transfer=True)
    )

    updated = service.update_sale(
        movement_id=result.movement.id,
        sku="SKU-TST",
        quantity=1,
        unit_price=2.0,
        movement_date=None,
        note=None,
    )

    assert updated.movement.paid_by_transfer is True


def test_recent_sales_filters_by_payment_method(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)

    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))
    service.sale(SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL", paid_by_transfer=True))
    service.sale(SaleCreate(sku="SKU-TST", quantity=1, location_code="CENTRAL", paid_by_transfer=False))

    assert len(service.recent_sales(payment_method="transfer")) == 1
    assert len(service.recent_sales(payment_method="cash")) == 1
    assert len(service.recent_sales()) == 2


def test_sale_fails_if_location_has_no_enough_stock_even_with_stock_elsewhere(db_session: Session, business_id: int) -> None:
```

**Step 3: `tests/unit/test_schemas.py`** — insertar después de `test_sale_create_requires_positive_quantity`

```python
# BEFORE:
def test_sale_create_requires_positive_quantity() -> None:
    try:
        SaleCreate(sku="A1", quantity=-1)
        assert False, "Expected ValidationError"
    except ValidationError as exc:
        assert "quantity must be greater than 0" in str(exc)


def test_supplier_return_requires_positive_quantity() -> None:

# AFTER:
def test_sale_create_requires_positive_quantity() -> None:
    try:
        SaleCreate(sku="A1", quantity=-1)
        assert False, "Expected ValidationError"
    except ValidationError as exc:
        assert "quantity must be greater than 0" in str(exc)


def test_sale_create_paid_by_transfer_defaults_false() -> None:
    payload = SaleCreate(sku="A1", quantity=1)
    assert payload.paid_by_transfer is False

    payload_checked = SaleCreate(sku="A1", quantity=1, paid_by_transfer=True)
    assert payload_checked.paid_by_transfer is True


def test_supplier_return_requires_positive_quantity() -> None:
```

**Imports needed:** ninguno (`SaleCreate`, `PurchaseCreate`, `InventoryService`, `Session`
ya están importados en ambos archivos).

**Edge cases (ya resueltos):**
- Ninguno adicional — todos los tests usan datos sembrados en el propio test (`_create_product`/`_create_locations`), sin dependencias externas.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py tests/unit/test_schemas.py -v
# Expected: 6 tests nuevos PASS + todos los existentes siguen PASS
```

#### Expected Report

- [ ] `tests/unit/test_inventory_service_unit.py` modificado — 5 tests nuevos
- [ ] `tests/unit/test_schemas.py` modificado — 1 test nuevo
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — 6 tests nuevos PASS, 0 regresiones

---

### T12: Test e2e (Playwright)

**Depends on:** T6, T7, T8, T9, T10
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_sale_payment_method_e2e.py`
**Pillars:** P1, P4, P5, P6, P7

#### Context

```python
# tests/e2e/test_flows_e2e.py — YA EXISTE, NO MODIFICAR (patrón a replicar)
def _login(page: Page, base_url: str) -> None: ...  # login admin/admin, espera htmx listo

def test_sale_flow_reduces_stock(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    page.fill('input[name="product"]', "E2E-SKU1")  # E2E-SKU1 sembrado con 10 uds en CENTRAL
    ...

# app/templates/partials/sales_history.html — YA EXISTE (T10):
# botón de edición: hx-get="/ui/movement/sale/{{ id }}/edit" hx-target="body" hx-swap="beforeend"
# → inyecta #sale-edit-modal en el body
```

#### Objective

Crear `tests/e2e/test_sale_payment_method_e2e.py` con 2 tests que cubran REQ-1 a REQ-6.

#### Instructions

**Step 1: Create the file** (no existe todavía)

```python
"""E2E del checkbox "pagado por transferencia" en el panel de venta rápida."""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def test_sale_payment_method_default_unchecked_persists_and_editable(live_server: str, page: Page) -> None:
    """Nace desmarcado, persiste al marcarlo, se ve en el historial y es editable."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()

    create_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    checkbox = create_form.locator('input[name="paid_by_transfer"]')
    expect(checkbox).not_to_be_checked()

    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    create_form.locator('input[name="product"]').fill("E2E-SKU1")
    create_form.locator('input[name="quantity"]').fill("1")
    create_form.locator('input[name="unit_price"]').fill("2.00")
    checkbox.check()
    create_form.locator('button[type="submit"]').click()

    row = page.locator("#sale-panel table tbody tr").first
    expect(row.locator("text=Transferencia")).to_be_visible()

    row.locator('button:has-text("Edit")').click()
    edit_modal = page.locator("#sale-edit-modal")
    edit_checkbox = edit_modal.locator('input[name="paid_by_transfer"]')
    expect(edit_checkbox).to_be_checked()

    edit_checkbox.uncheck()
    edit_modal.locator('button[type="submit"]').click()

    updated_row = page.locator("#sale-panel table tbody tr").first
    expect(updated_row.locator("text=Efectivo/otro")).to_be_visible()


def test_sale_history_filters_by_payment_method(live_server: str, page: Page) -> None:
    """El filtro 'Método de pago' del historial devuelve solo transferencias."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()

    create_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    create_form.locator('input[name="product"]').fill("E2E-SKU1")
    create_form.locator('input[name="quantity"]').fill("1")
    create_form.locator('input[name="unit_price"]').fill("2.00")
    create_form.locator('input[name="paid_by_transfer"]').check()
    create_form.locator('button[type="submit"]').click()

    page.select_option("#sales-filter-payment", "transfer")
    expect(page.locator("#sales-history").locator("text=Transferencia").first).to_be_visible()
```

**Imports needed:** ninguno adicional — ya listados en el archivo completo del Step 1.

**Edge cases (ya resueltos):**
- El stock sembrado de `E2E-SKU1` es 10 uds en `CENTRAL` (ver `tests/e2e/conftest.py`) —
  ambos tests venden 1 unidad, sin riesgo de quedarse sin stock aunque corran en el mismo
  proceso de test (pytest-playwright aísla el server por sesión, pero no la BD entre tests
  del mismo archivo — 2 ventas de 1 unidad cada una sobre 10 uds es seguro).

#### Verification

```bash
python3 -m playwright install chromium
SESSION_SECRET=test python3 -m pytest tests/e2e/test_sale_payment_method_e2e.py -m e2e --no-cov -v
# Expected: 2 tests PASS
```

#### Expected Report

- [ ] `tests/e2e/test_sale_payment_method_e2e.py` creado — 2 tests
- [ ] Ningún otro archivo modificado
- [ ] Comando de verificación — 2 tests PASS

---

### T13: Version bump (PATCH)

**Depends on:** T1-T12 (todo verde)
**Skill:** release-manager
**Agent:** coder
**Files:** `VERSION`, `.bumpversion.cfg`
**Pillars:** P1, P7

#### Context

```
# VERSION — YA EXISTE
0.4.5

# .bumpversion.cfg — YA EXISTE
[bumpversion]
current_version = 0.4.5
```

#### Objective

Ejecutar la suite completa (unit + e2e) y, si pasa, subir la versión de PATCH
`0.4.5` → `0.4.6` (nunca MINOR — regla dura del proyecto).

#### Instructions

**Step 1: Ejecutar la suite completa ANTES de tocar versión**

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v
# Expected: 100% PASS
SESSION_SECRET=test python3 -m pytest tests/e2e -m e2e --no-cov -v
# Expected: 100% PASS
```

Si falla: NO continuar a Step 2. Corregir en el envelope correspondiente (T1-T12) y
repetir hasta que pase.

**Step 2: Read the files**
```
Read VERSION
Read .bumpversion.cfg
```

**Step 3: Apply the change**

```
# VERSION
# BEFORE:
0.4.5
# AFTER:
0.4.6
```

```ini
# .bumpversion.cfg
# BEFORE:
[bumpversion]
current_version = 0.4.5
commit = True
tag = True
tag_name = v{new_version}
message = chore(release): bump version {current_version} → {new_version}

# AFTER:
[bumpversion]
current_version = 0.4.6
commit = True
tag = True
tag_name = v{new_version}
message = chore(release): bump version {current_version} → {new_version}
```

**Imports needed:** ninguno (edición de archivos de texto plano).

**Edge cases (ya resueltos):**
- Este es un incremento PATCH, no MINOR — `0.4.5` → `0.4.6`, no `0.5.0`.

#### Verification

```bash
cat VERSION
# Expected: 0.4.6
grep "current_version" .bumpversion.cfg
# Expected: current_version = 0.4.6
```

#### Expected Report

- [ ] `VERSION` modificado — `0.4.5` → `0.4.6`
- [ ] `.bumpversion.cfg` modificado — `current_version = 0.4.6`
- [ ] Ningún otro archivo modificado

---

### T14: CHANGELOG.md

**Depends on:** T13
**Skill:** technical-writer
**Agent:** coder
**Files:** `CHANGELOG.md`
**Pillars:** P1, P7

#### Context

```markdown
# CHANGELOG.md — YA EXISTE
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.4.5 (2026-08-18)
```

#### Objective

Añadir la entrada de la versión `0.4.6`.

#### Instructions

**Step 1: Read the file**
```
Read CHANGELOG.md
```

**Step 2: Apply the change**

```markdown
# BEFORE:
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.4.5 (2026-08-18)

# AFTER:
# Changelog

Este archivo se genera a partir del historial de Git (`git log`).

## 0.4.6 (2026-08-18)

- feat(sales): nuevo campo `paid_by_transfer` en el panel de venta rápida
  (`InventoryMovement`) — checkbox "Pagado por transferencia", desmarcado por
  defecto, editable después de creada la venta, visible y filtrable en el
  historial de ventas. No aplica a Documentos de venta (facturas/presupuestos).

## 0.4.5 (2026-08-18)
```

**Imports needed:** ninguno.

**Edge cases (ya resueltos):** ninguno.

#### Verification

```bash
grep -A2 "## 0.4.6" CHANGELOG.md
# Expected: la línea "- feat(sales): nuevo campo..." aparece
```

#### Expected Report

- [ ] `CHANGELOG.md` modificado — entrada `0.4.6` añadida antes de `0.4.5`
- [ ] Ningún otro archivo modificado
