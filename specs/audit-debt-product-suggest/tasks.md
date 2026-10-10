# Tasks — audit-debt-product-suggest

27 envelopes en 5 lotes (T1-T26 + T9b). Ejecutar **uno por sesión de `cc_local`, en orden**. Base `6fce595`.
Comandos de test: usar siempre `.venv/bin/python` (el `python3` del sistema no tiene `pytest-cov`).


Fuentes: [requirements.md](requirements.md) · [design.md](design.md).

---

## Lote A — Servicio de productos (REQ-4, REQ-5, REQ-6)

### T1: `list_sku_prefixes` pasa a `ProductRepository`

**Depends on:** none · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/repositories/product_repository.py`, `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Context
```python
# app/repositories/product_repository.py — YA EXISTE (línea 36)
    def list_skus_starting_with(self, prefix: str) -> list[str]:  # filtra por negocio: NO es la que necesitamos
# app/services/product_service.py — líneas 12 y 32-35 (YA EXISTEN)
from app.repositories.purchase_repository import PurchaseRepository
    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        # Numeración global: products.sku es unique=True global (ver PurchaseRepository.list_sku_prefixes).
        existing = PurchaseRepository(self._db, self._business_id).list_sku_prefixes(prefix)
        return next_sku_in_range(existing, self._business_id, prefix=prefix, width=width)
```

#### Instructions
**Step 1:** En `product_repository.py`, justo después del método `list_skus_starting_with` completo, añadir:
```python
    def list_sku_prefixes(self, prefix: str) -> list[str]:
        # products.sku es unique=True GLOBAL (no compuesto por business_id, ver
        # migrations.py ux_products_sku): el candidato autogenerado debe calcularse
        # sobre TODOS los negocios o choca con SKUs de otros negocios (409 recurrente).
        stmt = select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))
        rows = self._db.execute(stmt).all()
        return [str(sku or "") for (sku,) in rows]
```
**Step 2:** En `product_service.py`:
```python
# BEFORE
        # Numeración global: products.sku es unique=True global (ver PurchaseRepository.list_sku_prefixes).
        existing = PurchaseRepository(self._db, self._business_id).list_sku_prefixes(prefix)
# AFTER
        # Numeración global: products.sku es unique=True global (ver ProductRepository.list_sku_prefixes).
        existing = self._products.list_sku_prefixes(prefix)
```
**Step 3:** Borrar la línea `from app.repositories.purchase_repository import PurchaseRepository` de `product_service.py`.

#### Verification
```bash
grep -c "PurchaseRepository" app/services/product_service.py      # Expected: 0
grep -c "def list_sku_prefixes" app/repositories/product_repository.py   # Expected: 1
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py    # Expected: all passed
```

---

### T2: Quitar `list_sku_prefixes` de `PurchaseRepository`

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/repositories/purchase_repository.py`, `tests/unit/test_repositories.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
**Step 1:** En `purchase_repository.py`, borrar el método `list_sku_prefixes` completo (6 líneas desde `def list_sku_prefixes` hasta `return [str(sku or "") for (sku,) in rows]`) y la línea en blanco que lo precede.
**Step 2:** En `tests/unit/test_repositories.py`:
```python
# BEFORE (línea 55)
    repo = PurchaseRepository(db_session, business_id)
# AFTER
    repo = ProductRepository(db_session, business_id=business_id)
```
**Step 3:** Borrar la línea `from app.repositories.purchase_repository import PurchaseRepository` del test (ya no se usa).

#### Verification
```bash
grep -rn "list_sku_prefixes" app/ | grep -c purchase_repository   # Expected: 0
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_repositories.py     # Expected: all passed
```

---

### T3: Helpers `_sku_conflict_detail` e `_insert_with_sku_retry`

**Depends on:** T1 · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-5, REQ-6
**Files:** `app/services/product_service.py`
**Pillars:** P1, P2, P4, P5, P6

#### Instructions
**Step 1:** Añadir tras `import math` (línea 3):
```python
from collections.abc import Callable
```
**Step 2:** Justo después del método `_generate_sku` completo, añadir:
```python
    def _sku_conflict_detail(self, sku_raw: str) -> str:
        """Mensaje del 409 por SKU: products.sku es único GLOBAL (ver list_sku_prefixes)."""
        if not sku_raw:
            return "No se pudo generar un SKU automáticamente"
        if self._products.get_by_sku(sku_raw) is not None:
            return f"El SKU '{sku_raw}' ya existe"
        return f"El SKU '{sku_raw}' ya está en uso en otro negocio"

    def _insert_with_sku_retry(
        self, build: Callable[[str], Product], sku_raw: str, *, commit: bool
    ) -> Product:
        """Inserta `build(sku)` reintentando el SKU automático (5 intentos; explícito: 1).

        commit=False solo hace flush (el llamador confirma). Una colisión hace rollback de
        TODA la transacción: llamar antes de cualquier otra escritura pendiente.
        """
        attempts = 1 if sku_raw else 5
        last_error: Exception | None = None
        for _ in range(attempts):
            try:
                candidate = sku_raw or self._generate_sku()
            except SKURangeExhausted as e:
                raise HTTPException(status_code=409, detail=str(e)) from e
            product = build(candidate)
            self._products.add(product)
            try:
                if commit:
                    self._db.commit()
                    self._db.refresh(product)
                else:
                    self._db.flush()
                return product
            except IntegrityError as e:
                self._db.rollback()
                last_error = e
                _log.warning("SKU collision creating product: %r", candidate)

        _log.error("Could not create product (sku=%r, auto=%s)", sku_raw, not sku_raw)
        raise HTTPException(status_code=409, detail=self._sku_conflict_detail(sku_raw)) from last_error
```

#### Verification
```bash
grep -c "def _insert_with_sku_retry\|def _sku_conflict_detail" app/services/product_service.py  # Expected: 2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py   # Expected: all passed (aún no se usa)
```

---

### T4: `create()` usa `_insert_with_sku_retry`

**Depends on:** T3 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
En `create()`, sustituir **desde** `sku_raw = payload.sku.strip() if payload.sku else ""` **hasta** `raise HTTPException(status_code=409, detail="SKU already exists") from last_error` (incluidos; bucle `for _ in range(attempts)` completo) por:
```python
        sku_raw = payload.sku.strip() if payload.sku else ""

        def _build(sku: str) -> Product:
            return Product(
                business_id=self._business_id,
                sku=sku,
                name=payload.name.strip(),
                category=payload.category.strip() if payload.category else None,
                min_stock=payload.min_stock,
                unit_of_measure=payload.unit_of_measure.strip()
                if payload.unit_of_measure and payload.unit_of_measure.strip()
                else None,
                default_purchase_cost=payload.default_purchase_cost,
                default_sale_price=payload.default_sale_price,
                lead_time_days=int(payload.lead_time_days or 0),
                image_url=payload.image_url.strip()
                if payload.image_url and payload.image_url.strip()
                else None,
            )

        return self._insert_with_sku_retry(_build, sku_raw, commit=True)
```
No tocar las validaciones previas de `create()`.

#### Verification
```bash
grep -c "for _ in range(attempts)" app/services/product_service.py   # Expected: 1 (solo el helper)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py   # Expected: all passed
```

---

### T5: `create_for_purchase()` usa `_insert_with_sku_retry`

**Depends on:** T4 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
En `create_for_purchase()`, sustituir **desde** `auto_sku = not sku_raw` **hasta** `raise HTTPException(status_code=409, detail=detail) from last_error` (incluidos) por:
```python
        def _build(candidate: str) -> Product:
            return Product(
                business_id=self._business_id,
                sku=candidate,
                name=clean_name,
                category=(category or "").strip() or None,
                min_stock=0,
                unit_of_measure=None,
                default_purchase_cost=purchase_cost,
                default_sale_price=sale_price if sale_price is not None else 0,
                lead_time_days=0,
                image_url=None,
            )

        return self._insert_with_sku_retry(_build, sku_raw, commit=False)
```
No tocar las validaciones previas ni el docstring.

#### Verification
```bash
grep -c "for _ in range" app/services/product_service.py   # Expected: 1
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py tests/unit/test_audit_isolated.py   # Expected: all passed
```

---

### T6: `update()` usa `_sku_conflict_detail`

**Depends on:** T5 · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-5
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
En `update()` (única aparición restante del texto):
```python
# BEFORE
            raise HTTPException(status_code=409, detail="SKU already exists")
# AFTER
            raise HTTPException(status_code=409, detail=self._sku_conflict_detail(new_sku or original_sku))
```

#### Verification
```bash
grep -c "SKU already exists" app/services/product_service.py   # Expected: 0
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py   # Expected: all passed
```

---

### T7: `_ensure_finite` en `create()` y `update()`

**Depends on:** T6 · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-4
**Files:** `app/services/product_service.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
**Step 1:** Justo después de `_log = get_logger(__name__)`, añadir (con 2 líneas en blanco antes de `class ProductService`):
```python
def _ensure_finite(**fields: float | None) -> None:
    """422 si algún valor es NaN o infinito: Pydantic y float() los aceptan y `nan < 0` es False."""
    for field, value in fields.items():
        if value is not None and not math.isfinite(float(value)):
            raise HTTPException(status_code=422, detail=f"{field} must be a finite number")
```
**Step 2:** En `create()`:
```python
# BEFORE
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")

        if payload.min_stock < 0:
# AFTER
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        _ensure_finite(
            min_stock=payload.min_stock,
            default_purchase_cost=payload.default_purchase_cost,
            default_sale_price=payload.default_sale_price,
        )

        if payload.min_stock < 0:
```
**Step 3:** En `update()`:
```python
# BEFORE
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        if payload.min_stock < 0:
# AFTER
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        _ensure_finite(
            min_stock=payload.min_stock,
            default_purchase_cost=payload.default_purchase_cost,
            default_sale_price=payload.default_sale_price,
        )
        if payload.min_stock < 0:
```

#### Verification
```bash
grep -c "_ensure_finite(" app/services/product_service.py   # Expected: 3 (def + 2 llamadas)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py   # Expected: all passed
```

---

### T8: Tests de T3-T7

**Depends on:** T7 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-4, REQ-5
**Files:** `tests/unit/test_product_service.py`
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
**Step 1:**
```python
# BEFORE
from app.models import InventoryMovement, Product
# AFTER
from app.models import Business, InventoryMovement, Product
```
**Step 2:** Añadir al FINAL del archivo:
```python


# --- Mensajes de conflicto de SKU (REQ-5) y no finitos (REQ-4) ---


def test_create_explicit_sku_same_business_conflict_message(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    service.create(_payload(sku="DUP-2", name="Uno"))

    with pytest.raises(HTTPException) as exc:
        service.create(_payload(sku="DUP-2", name="Dos"))

    assert exc.value.status_code == 409
    assert exc.value.detail == "El SKU 'DUP-2' ya existe"


def test_sku_in_other_business_conflict_message(db_session: Session, business_id: int) -> None:
    other = Business(code="otro", name="Otro negocio")
    db_session.add(other)
    db_session.commit()
    ProductService(db_session, business_id=other.id).create(_payload(sku="AJENO-1", name="Ajeno"))
    service = ProductService(db_session, business_id=business_id)

    with pytest.raises(HTTPException) as exc:
        service.create_for_purchase(name="Mío", sku="AJENO-1", purchase_cost=1.0, sale_price=None)

    assert exc.value.status_code == 409
    assert exc.value.detail == "El SKU 'AJENO-1' ya está en uso en otro negocio"


def test_update_sku_conflict_message(db_session: Session, business_id: int) -> None:
    service = ProductService(db_session, business_id=business_id)
    service.create(_payload(sku="UPD-A", name="A"))
    service.create(_payload(sku="UPD-B", name="B"))

    with pytest.raises(HTTPException) as exc:
        service.update("UPD-A", ProductUpdate(sku="UPD-B", name="A"))

    assert exc.value.status_code == 409
    assert exc.value.detail == "El SKU 'UPD-B' ya existe"


@pytest.mark.parametrize(
    "overrides",
    [{"min_stock": float("nan")}, {"default_purchase_cost": float("inf")}, {"default_sale_price": float("nan")}],
)
def test_create_and_update_reject_non_finite(db_session: Session, business_id: int, overrides) -> None:
    service = ProductService(db_session, business_id=business_id)
    with pytest.raises(HTTPException) as exc:
        service.create(_payload(name="NoFinito", **overrides))
    assert exc.value.status_code == 422
    assert "must be a finite number" in str(exc.value.detail)

    service.create(_payload(sku="FIN-1", name="Finito"))
    with pytest.raises(HTTPException) as exc2:
        service.update("FIN-1", ProductUpdate(name="Finito", **overrides))
    assert exc2.value.status_code == 422
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_product_service.py   # Expected: all passed (6 tests nuevos)
```

---

## Lote B — No finitos fuera del servicio de productos (REQ-4)

### T9: `parse_optional_float` devuelve `None` para no finitos

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-4
**Files:** `app/routers/ui_common.py`, `tests/unit/test_ui_common_parse.py` (CREATE)
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
**Step 1:** En `ui_common.py`, añadir `import math` tras `import hashlib` (línea 3).
**Step 2:** En `parse_optional_float`:
```python
# BEFORE
    try:
        return float(s)
    except ValueError:
        return None
# AFTER
    try:
        value = float(s)
    except ValueError:
        return None
    # "nan"/"inf" son floats válidos para Python: se tratan como cualquier texto no numérico.
    return value if math.isfinite(value) else None
```
**Step 3:** Crear `tests/unit/test_ui_common_parse.py`:
```python
from __future__ import annotations

import pytest

from app.routers.ui_common import parse_optional_float


@pytest.mark.parametrize("raw", ["nan", "NaN", "inf", "-inf", "Infinity", "1e999"])
def test_parse_optional_float_rejects_non_finite(raw: str) -> None:
    assert parse_optional_float(raw) is None


@pytest.mark.parametrize(("raw", "expected"), [("1,5", 1.5), ("1.234,5", 1234.5), (" 2 ", 2.0), ("", None), ("abc", None)])
def test_parse_optional_float_keeps_existing_behaviour(raw: str, expected) -> None:
    assert parse_optional_float(raw) == expected
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_ui_common_parse.py   # Expected: 11 passed
```

---

### T9b: La compra rechaza un costo o precio tecleado que no es un número

**Depends on:** T9 · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-4
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Context
Tras T9, `parse_optional_float` devuelve `None` tanto para "vacío" como para texto no numérico
(incluido `nan`/`inf`). Sin este envelope, `sale_price="nan"` se trataría como vacío (precio 0) y un
`unit_cost` inválido usaría el costo por defecto, sin avisar.

#### Instructions
En la función `purchase`:
```python
# BEFORE
        try:
            movement_dt = parse_dt(movement_date)
        except ValueError as e:
            raise HTTPException(status_code=422, detail="Fecha inválida") from e
# AFTER
        try:
            movement_dt = parse_dt(movement_date)
        except ValueError as e:
            raise HTTPException(status_code=422, detail="Fecha inválida") from e
        # parse_optional_float da None para vacío Y para texto no numérico (nan/inf incluidos):
        # un valor tecleado que no se puede usar es un error, no un campo vacío.
        if (unit_cost or "").strip() and parsed_cost is None:
            raise HTTPException(status_code=422, detail="El costo unitario debe ser un número mayor o igual que 0")
        if (sale_price or "").strip() and parsed_sale_price is None:
            raise HTTPException(status_code=422, detail="El precio de venta debe ser un número mayor o igual que 0")
```

#### Verification
```bash
grep -c "un valor tecleado que no se puede usar es un error" app/routers/ui_purchases.py   # Expected: 1
.venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q tests/e2e/test_product_creation_in_purchase_e2e.py   # Expected: all passed
```

---

### T10: `InventoryService.sale` rechaza cantidad o precio no finitos

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-4
**Files:** `app/services/inventory_service.py`, `tests/unit/test_inventory_service_unit.py`
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
**Step 1:** En `InventoryService.sale` (≈ línea 1719; `math` ya está importado):
```python
# BEFORE
        self._invalidate_metrics_cache()
        if payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        product = self._get_product(payload.sku)
        unit_price = payload.unit_price
        if unit_price is None:
            unit_price = product.default_sale_price
        if unit_price is None:
            raise HTTPException(status_code=422, detail="unit_price is required")
        if unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")
# AFTER
        self._invalidate_metrics_cache()
        if not math.isfinite(payload.quantity) or payload.quantity <= 0:
            raise HTTPException(status_code=422, detail="quantity must be > 0")

        product = self._get_product(payload.sku)
        unit_price = payload.unit_price
        if unit_price is None:
            unit_price = product.default_sale_price
        if unit_price is None:
            raise HTTPException(status_code=422, detail="unit_price is required")
        if not math.isfinite(unit_price) or unit_price < 0:
            raise HTTPException(status_code=422, detail="unit_price must be >= 0")
```
Atención: el bloque `BEFORE` aparece **solo en `sale()`** (en `purchase()` el texto es distinto: `unit_cost`).
**Step 2:** Añadir al FINAL de `tests/unit/test_inventory_service_unit.py`:
```python


@pytest.mark.parametrize(
    ("quantity", "unit_price"),
    [(float("nan"), 2.0), (float("inf"), 2.0), (1.0, float("nan")), (1.0, float("inf"))],
)
def test_sale_rejects_non_finite_quantity_or_price(
    db_session: Session, business_id: int, quantity: float, unit_price: float
) -> None:
    _create_locations(db_session, business_id)
    _create_product(db_session, business_id)
    service = InventoryService(db_session, business_id=business_id)
    service.purchase(PurchaseCreate(sku="SKU-TST", quantity=5, unit_cost=1.0))

    with pytest.raises(HTTPException) as exc:
        service.sale(SaleCreate(sku="SKU-TST", quantity=quantity, unit_price=unit_price, location_code="CENTRAL"))

    assert exc.value.status_code == 422
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_inventory_service_unit.py   # Expected: all passed (4 nuevos)
```

---

## Lote C — Rutas de compras (REQ-6, REQ-7)

### T11: Quitar `product_options` sin uso en compras ✅ DONE

**Depends on:** none · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-7
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
Borrar la línea exacta
```python
                "product_options": product_service.search(query="", limit=200),
```
(con la sangría que tenga en cada caso) en las funciones `purchase_edit_form`, `purchase_update`,
`purchase_delete` y `purchase_delete_selected` (9 apariciones en total). **NO** borrarla en
`tab_purchases` (la usa el datalist del panel de productos).

#### Verification
```bash
grep -c '"product_options": product_service.search(query="", limit=200),' app/routers/ui_purchases.py   # Expected: 1 (solo tab_purchases)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "import app.main; print('import OK')"
```

---

# T12: Helper `_product_panel_refresh` en compras

**Depends on:** T11 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/routers/ui_purchases.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
**Step 1:** Justo antes de `@router.post("/purchase/from-invoice", response_class=HTMLResponse)`, añadir:
```python
def _product_panel_refresh(product_service: ProductService) -> dict:
    """Contexto para refrescar #product-panel (historial + datalist inicial) vía hx-swap-oob.

    El datalist inicial trae los 200 primeros productos; el recién creado se encuentra al
    teclear gracias a las sugerencias dinámicas (GET /ui/product-suggest).
    """
    return {
        "refresh_product_panel": True,
        "products": product_service.recent(limit=20),
        "product_options": product_service.search(query="", limit=200),
    }


```
**Step 2:** En `purchase_from_invoice`, sustituir el bloque completo
```python
    # Productos nuevos: refrescar #product-panel (datalist + historial) vía hx-swap-oob.
    invoice_refresh: dict = {}
    if created_products:
        new_skus = {p["sku"] for p in created_products}
        options = product_service.search(query="", limit=200)
        seen = {p.sku for p in options}
        options += [p for p in product_service.recent(limit=len(new_skus) + 20) if p.sku in new_skus - seen]
        invoice_refresh = {
            "refresh_product_panel": True,
            "product_options": options,
            "products": product_service.recent(limit=20),
        }
```
por
```python
    # Productos nuevos: refrescar #product-panel vía hx-swap-oob.
    invoice_refresh = _product_panel_refresh(product_service) if created_products else {}
```
**Step 3:** En `purchase`, sustituir
```python
            # Refresca #product-panel (datalist de "Artículo" + historial) vía hx-swap-oob.
            # search("") devuelve los 200 primeros por id: se garantiza el recién creado.
            options = product_service.search(query="", limit=200)
            if all(p.id != created_product.id for p in options):
                options.append(created_product)
            context = {
                "refresh_product_panel": True,
                "product_options": options,
                "products": product_service.recent(limit=20),
            }
```
por
```python
            context = _product_panel_refresh(product_service)
```

#### Verification
```bash
grep -c "_product_panel_refresh(" app/routers/ui_purchases.py   # Expected: 3 (def + 2 usos)
.venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q tests/e2e/test_product_creation_in_purchase_e2e.py   # Expected: all passed
```

---

# T13: `product_from_invoice` sin lógica de "garantizar recién creados"

**Depends on:** T12 · **Skill:** backend-developer · **Agent:** refactor · **REQ:** REQ-6
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
En `product_from_invoice`:
1. Borrar la línea `    created_objs: list[Product] = []`.
2. Borrar la línea `            created_objs.append(product)`.
3. Borrar la línea `    options = product_service.search(query="", limit=200)`.
4. Sustituir
```python
            # search("") devuelve los 200 primeros por id: se garantizan los recién creados.
            "product_options": options + [p for p in created_objs if p.id not in {o.id for o in options}],
```
por
```python
            "product_options": product_service.search(query="", limit=200),
```

#### Verification
```bash
grep -c "created_objs" app/routers/ui_products.py   # Expected: 0
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_ui_product_from_invoice.py   # Expected: 1 passed
```

---

## Lote D — Error dentro del modal de edición de producto (REQ-3)

# T14: `product_update` devuelve el formulario con el error al modal

**Depends on:** T6 · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-3
**Files:** `app/routers/ui_products.py`
**Pillars:** P1, P4, P5, P6

#### Instructions
**Step 1:** Añadir `from types import SimpleNamespace` tras `import html`.
**Step 2:** En la función `product_update` (decorador `@router.post("/product/{sku:path}/update", ...)`),
sustituir su bloque `except HTTPException as e:` completo (el que renderiza
`"message": "Error al actualizar producto"`) por el código de abajo. **NO tocar** el bloque
equivalente de `product_update_inventory`, que tiene el mismo texto (fuera de alcance):
```python
    except HTTPException as e:
        db.rollback()
        # REQ-3: el error se muestra DENTRO del modal con lo tecleado (antes se pintaba en
        # #product-panel detrás del modal y app.js lo cerraba, perdiendo los datos).
        try:
            image_url = product_service.get_by_sku(sku).image_url
        except HTTPException:
            image_url = None
        form_product = SimpleNamespace(
            sku=sku,
            name=name,
            unit_of_measure=unit_of_measure,
            image_url=image_url,
            category=category or "",
            min_stock=min_stock,
            default_purchase_cost=default_purchase_cost,
            default_sale_price=default_sale_price,
        )
        return templates.TemplateResponse(
            request=request,
            name="partials/product_edit_form.html",
            context={"product": form_product, "new_sku_value": new_sku or sku, "form_error": str(e.detail)},
            status_code=e.status_code,
            headers={"HX-Retarget": "#product-edit-modal", "HX-Reswap": "outerHTML"},
        )
```

#### Verification
```bash
grep -c "HX-Retarget" app/routers/ui_products.py   # Expected: 1
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "import app.main; print('import OK')"
```

---

# T15: Plantilla del modal: error y SKU tecleado

**Depends on:** T14 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-3
**Files:** `app/templates/partials/product_edit_form.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
**Step 1:**
```html
<!-- BEFORE -->
      </div>

      <form hx-post="/ui/product/{{ product.sku }}/update"
<!-- AFTER -->
      </div>

      {% if form_error %}
        <div class="error" style="padding:10px;border-radius:8px;margin-bottom:10px;"><strong>Error al actualizar producto</strong><div>{{ form_error }}</div></div>
      {% endif %}

      <form hx-post="/ui/product/{{ product.sku }}/update"
```
**Step 2:**
```html
<!-- BEFORE -->
        <div class="row"><label>SKU</label><input name="new_sku" value="{{ product.sku }}" /></div>
<!-- AFTER -->
        <div class="row"><label>SKU</label><input name="new_sku" value="{{ new_sku_value if new_sku_value is defined else product.sku }}" /></div>
```
(El resto de campos ya leen de `product.*`, que en el error contiene lo tecleado.)

#### Verification
```bash
grep -c "form_error" app/templates/partials/product_edit_form.html   # Expected: 2
grep -c "new_sku_value" app/templates/partials/product_edit_form.html   # Expected: 1
```

---

# T16: e2e del modal (sustituye al test de v0.4.11)

**Depends on:** T15 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-3
**Files:** `tests/e2e/test_product_creation_in_purchase_e2e.py`
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
Sustituir **solo** esta función (exactamente este bloque). **NO tocar** los tests que vienen detrás
(`test_purchase_invalid_quantity_or_date_returns_422_panel` y `test_purchase_quantity_input_blocks_zero`):
```python
# BEFORE
def test_product_edit_modal_closes_and_shows_error(live_server: str, page: Page) -> None:
    """El error 4xx se pinta en #product-panel (detrás del modal): el modal debe cerrarse."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _history_row(page, "E2E-SKU1").locator('button:has-text("Edit")').click()
    modal = page.locator("#product-edit-modal")
    expect(modal).to_be_visible()

    modal.locator('input[name="new_sku"]').fill("E2E-SKU2")
    modal.locator('button[type="submit"]').click()

    expect(page.locator("#product-panel").locator("text=Error al actualizar producto").first).to_be_visible()
    expect(page.locator("#product-edit-modal")).to_have_count(0)
# AFTER
def test_product_edit_modal_keeps_values_and_shows_error(live_server: str, page: Page) -> None:
    """REQ-3: el error de 'Guardar cambios' se ve dentro del modal y no se pierde lo tecleado."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _history_row(page, "E2E-SKU1").locator('button:has-text("Edit")').click()
    modal = page.locator("#product-edit-modal")
    expect(modal).to_be_visible()

    modal.locator('input[name="new_sku"]').fill("E2E-SKU2")
    modal.locator('input[name="name"]').fill("Nombre nuevo E2E")
    modal.locator('button[type="submit"]').click()

    modal = page.locator("#product-edit-modal")
    expect(modal.get_by_text("El SKU 'E2E-SKU2' ya existe")).to_be_visible()
    expect(modal.locator('input[name="name"]')).to_have_value("Nombre nuevo E2E")
    expect(modal.locator('input[name="new_sku"]')).to_have_value("E2E-SKU2")
```

#### Verification
```bash
grep -c "def test_purchase_invalid_quantity_or_date_returns_422_panel\|def test_purchase_quantity_input_blocks_zero" tests/e2e/test_product_creation_in_purchase_e2e.py   # Expected: 2
.venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q tests/e2e/test_product_creation_in_purchase_e2e.py   # Expected: 15 passed
```

---

# T17: Ruta `GET /ui/product-suggest` + plantilla de opciones

**Depends on:** none · **Skill:** backend-developer · **Agent:** coder · **REQ:** REQ-1, REQ-2
**Files:** `app/routers/ui_products.py`, `app/templates/partials/product_suggest_options.html` (CREATE)
**Pillars:** P1, P3, P4, P5, P6

#### Context
```python
# ProductService.search(query: str, limit: int = 20) -> list[Product]  # filtra por negocio, sin tildes
# require_active_business_id(db, request) -> int  (ya importado en ui_products.py)
```

#### Instructions
**Step 1:** Crear `app/templates/partials/product_suggest_options.html`:
```html
{% for p in products %}
  {% if fmt == "sku" %}
    <option value="{{ p.sku }}">{{ p.sku }} - {{ p.name }}</option>
  {% else %}
    <option value="{{ p.sku }} - {{ p.name }}"></option>
    {% if fmt == "sale" %}
      <option value="{{ p.sku }}"></option>
      <option value="{{ p.name }}"></option>
    {% endif %}
  {% endif %}
{% endfor %}
```
**Step 2:** En `ui_products.py`, justo después de la función `product_defaults_sale` completa, añadir:
```python
_SUGGEST_FORMATS = {"label", "sale", "sku"}


@router.get("/product-suggest", response_class=HTMLResponse)
def product_suggest(
    request: Request,
    q: str = "",
    fmt: str = "label",
    db: Session = Depends(session_dep),
) -> HTMLResponse:
    """Sugerencias de producto al teclear (todos los roles; negocio activo).

    Los datalist iniciales solo traen los 200 primeros productos: app.js sustituye su
    contenido con esta respuesta (inputs con data-product-suggest).
    """
    business_id = require_active_business_id(db, request)
    query = (q or "").strip()
    if len(query) < 2:
        return HTMLResponse("")
    products = ProductService(db, business_id=business_id).search(query=query, limit=30)
    return templates.TemplateResponse(
        request=request,
        name="partials/product_suggest_options.html",
        context={"products": products, "fmt": fmt if fmt in _SUGGEST_FORMATS else "label"},
    )
```

#### Verification
```bash
grep -c '"/product-suggest"' app/routers/ui_products.py   # Expected: 1
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -c "import app.main; print('import OK')"
```

---

# T18: Tests unitarios de `product_suggest`

**Depends on:** T17 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-1, REQ-2
**Files:** `tests/unit/test_ui_product_suggest.py` (CREATE)
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
Crear el archivo con este contenido exacto:
```python
"""REQ-1/REQ-2: GET /ui/product-suggest."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from app.models import Business, Product
from app.routers import ui_products


def _suggest(db: Session, business_id: int, monkeypatch, q: str, fmt: str = "label") -> str:
    monkeypatch.setattr(ui_products, "require_active_business_id", lambda _db, _req: business_id)
    request = MagicMock()
    request.session = {}
    resp = ui_products.product_suggest(request=request, q=q, fmt=fmt, db=db)
    return resp.body.decode("utf-8")


def _add(db: Session, business_id: int, sku: str, name: str) -> None:
    db.add(Product(business_id=business_id, sku=sku, name=name, min_stock=0, lead_time_days=0))


def test_finds_product_beyond_first_200(db_session: Session, business_id: int, monkeypatch) -> None:
    for i in range(1, 206):
        _add(db_session, business_id, f"SUG{i:04d}", f"Producto {i:04d}")
    db_session.commit()

    body = _suggest(db_session, business_id, monkeypatch, "0205")

    assert 'value="SUG0205 - Producto 0205"' in body


def test_short_query_returns_empty(db_session: Session, business_id: int, monkeypatch) -> None:
    _add(db_session, business_id, "SUGA", "Alfa")
    db_session.commit()
    assert _suggest(db_session, business_id, monkeypatch, "a").strip() == ""


def test_other_business_not_returned_and_html_escaped(db_session: Session, business_id: int, monkeypatch) -> None:
    other = Business(code="otro", name="Otro negocio")
    db_session.add(other)
    db_session.commit()
    _add(db_session, other.id, "AJENO", "Producto Ajeno")
    _add(db_session, business_id, "XSS1", "<b>Producto</b>")
    db_session.commit()

    assert "Ajeno" not in _suggest(db_session, business_id, monkeypatch, "Producto")
    body = _suggest(db_session, business_id, monkeypatch, "Producto")
    assert "<b>" not in body
    assert "&lt;b&gt;Producto&lt;/b&gt;" in body


@pytest.mark.parametrize(
    ("fmt", "expected"),
    [
        ("sku", '<option value="FMT1">FMT1 - Formato</option>'),
        ("sale", '<option value="FMT1"></option>'),
        ("raro", 'value="FMT1 - Formato"'),
    ],
)
def test_formats(db_session: Session, business_id: int, monkeypatch, fmt: str, expected: str) -> None:
    _add(db_session, business_id, "FMT1", "Formato")
    db_session.commit()
    assert expected in _suggest(db_session, business_id, monkeypatch, "Formato", fmt=fmt)
```

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest --no-cov -p no:warnings -q tests/unit/test_ui_product_suggest.py   # Expected: 6 passed
```

---

# T19: Listener de sugerencias en `app.js`

**Depends on:** T17 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/static/js/app.js`
**Pillars:** P1, P4, P5, P6

#### Instructions
Justo antes de la línea `  document.addEventListener('htmx:configRequest', (evt) => {`, añadir:
```javascript
  // Sugerencias dinámicas de productos (REQ-1): los datalist iniciales solo traen los 200
  // primeros productos; al teclear se piden coincidencias al servidor. No usa hx-get porque
  // varios de estos inputs ya lo usan para cargar los valores por defecto del artículo.
  const __suggestTimers = new WeakMap();
  document.addEventListener('input', (evt) => {
    const input = evt.target;
    if (!(input instanceof HTMLInputElement) || !input.dataset.productSuggest || !input.list) return;
    clearTimeout(__suggestTimers.get(input));
    __suggestTimers.set(input, setTimeout(async () => {
      const q = input.value.trim();
      if (q.length < 2) return;
      const url = '/ui/product-suggest?q=' + encodeURIComponent(q) + '&fmt=' + encodeURIComponent(input.dataset.productSuggest);
      try {
        const resp = await fetch(url, { credentials: 'same-origin' });
        // Sesión caducada (redirige al login), error, o el usuario siguió tecleando: descartar.
        if (!resp.ok || resp.redirected || input.value.trim() !== q || !input.list) return;
        input.list.innerHTML = await resp.text();
      } catch (e) {}
    }, 250));
  });

```

#### Verification
```bash
node --check app/static/js/app.js && echo "JS OK"
grep -c "data-product-suggest\|productSuggest" app/static/js/app.js   # Expected: 2
```

---

# T20: `data-product-suggest` en Compras y Ventas

**Depends on:** T19 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/partials/purchase_panel.html`, `app/templates/partials/sale_panel.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
- `purchase_panel.html`: `list="product-options"` → `list="product-options" data-product-suggest="label"` (1 vez).
- `sale_panel.html`: `list="product-options"` → `list="product-options" data-product-suggest="sale"` (1 vez).

#### Verification
```bash
grep -c 'data-product-suggest="label"' app/templates/partials/purchase_panel.html   # Expected: 1
grep -c 'data-product-suggest="sale"' app/templates/partials/sale_panel.html        # Expected: 1
```

---

# T21: `data-product-suggest` en los modales de edición de compra y venta

**Depends on:** T19 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/partials/sale_edit_form.html`, `app/templates/partials/purchase_edit_form.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
En ambos: `list="product-options"` → `list="product-options" data-product-suggest="label"` (1 vez en cada uno).

#### Verification
```bash
grep -c 'data-product-suggest="label"' app/templates/partials/sale_edit_form.html app/templates/partials/purchase_edit_form.html   # Expected: 1 y 1
```

---

# T22: `data-product-suggest` en Documentos y Devoluciones

**Depends on:** T19 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/components/sales_doc/cart_add.html`, `app/templates/partials/tab_inventory.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
- `cart_add.html`: `list="product-options"` → `list="product-options" data-product-suggest="label"`.
- `tab_inventory.html`: `list="return-products-list"` → `list="return-products-list" data-product-suggest="sku"`.

#### Verification
```bash
grep -c 'data-product-suggest="label"' app/templates/components/sales_doc/cart_add.html   # Expected: 1
grep -c 'data-product-suggest="sku"' app/templates/partials/tab_inventory.html            # Expected: 1
```

---

# T23: `data-product-suggest` en Traspasos

**Depends on:** T19 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/partials/tab_transfers.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
Las 3 apariciones: `list="transfer-products-list"` → `list="transfer-products-list" data-product-suggest="label"`.

#### Verification
```bash
grep -c 'data-product-suggest="label"' app/templates/partials/tab_transfers.html   # Expected: 3
```

---

# T24: `data-product-suggest` en los modales de beneficios

**Depends on:** T19 · **Skill:** frontend-developer · **Agent:** coder · **REQ:** REQ-1
**Files:** `app/templates/partials/profit_items_sale_edit_form.html`, `app/templates/partials/profit_items_purchase_edit_form.html`
**Pillars:** P1, P4, P5, P6

#### Instructions
- `profit_items_sale_edit_form.html`: `list="profit-items-product-options"` → `list="profit-items-product-options" data-product-suggest="label"`.
- `profit_items_purchase_edit_form.html`: `list="profit-items-purchase-product-options"` → `list="profit-items-purchase-product-options" data-product-suggest="label"`.

#### Verification
```bash
grep -rc 'data-product-suggest=' app/templates | awk -F: '{s+=$2} END {print s}'   # Expected: 11
```

---

# T25: e2e de las sugerencias dinámicas

**Depends on:** T20 · **Skill:** test-automation-engineer · **Agent:** coder · **REQ:** REQ-1
**Files:** `tests/e2e/test_product_suggest_e2e.py` (CREATE)
**Pillars:** P1, P4, P5, P6, P7

#### Instructions
Crear el archivo con este contenido exacto (el datalist se vacía antes de teclear para que la opción
solo pueda venir del servidor):
```python
"""E2E REQ-1: al teclear en 'Artículo' el datalist se rellena desde /ui/product-suggest."""

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


def test_sale_product_input_fetches_suggestions(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    page.evaluate("document.getElementById('product-options').innerHTML = ''")

    page.fill('#sale-panel input[name="product"]', "E2E-SKU2")

    datalist = page.locator("#sale-panel datalist#product-options")
    expect(datalist.locator('option[value="E2E-SKU2 - Producto E2E Dos"]')).to_have_count(1)
    expect(datalist.locator('option[value="E2E-SKU2"]')).to_have_count(1)


def test_purchase_product_input_fetches_suggestions(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    page.evaluate("document.getElementById('product-options').innerHTML = ''")

    page.fill('#purchase-panel input[name="product"]', "Producto E2E Uno")

    expect(page.locator('datalist#product-options option[value="E2E-SKU1 - Producto E2E Uno"]')).to_have_count(1)
```

#### Verification
```bash
.venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q tests/e2e/test_product_suggest_e2e.py   # Expected: 2 passed
```

---

# T26: VERSION 0.4.11 → 0.4.12

**Depends on:** T25 · **Skill:** release-manager · **Agent:** coder · **REQ:** — (DoD)
**Files:** `VERSION`, `.bumpversion.cfg`
**Pillars:** P1, P4, P5, P6

#### Instructions
- `VERSION`: `0.4.11` → `0.4.12`.
- `.bumpversion.cfg`: `current_version = 0.4.11` → `current_version = 0.4.12`.

#### Verification
```bash
cat VERSION; grep current_version .bumpversion.cfg   # Expected: 0.4.12 / current_version = 0.4.12
```
