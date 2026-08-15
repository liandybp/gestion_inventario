# Phase 3 — Task Envelopes: Refactor Fase C

> Cada envelope es autocontenido. OpenCode ejecuta UNO a la vez, en orden T1 → T14.
> Test runner: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -v`
> **Regla dura:** mover lógica VERBATIM (sin refactorizar durante la migración). Cero placeholders.

---

## C1 — RBAC API

### T1: `require_admin_or_owner_api` + test

**Depends on:** none
**Skill:** security-architect
**Agent:** coder
**Files:** `app/security.py`, `tests/unit/test_security.py` (si existe, si no crear)

#### Context

```python
# app/security.py — EXISTE
def require_admin_api(request, db=Depends(session_dep)) -> User:
    user = require_user_api(request=request, db=db)
    if (user.role or "").lower() != "admin":
        raise HTTPException(status_code=403, detail="Admin required")
    return user
```

#### Objective

Nueva dependencia que permite admin+owner y rechaza operator.

#### Instructions

**Step 1:** Añadir tras `require_admin_api`:

```python
def require_admin_or_owner_api(
    request: Request,
    db: Session = Depends(session_dep),
) -> User:
    user = require_user_api(request=request, db=db)
    role = (user.role or "").lower()
    if role not in ("admin", "owner"):
        raise HTTPException(status_code=403, detail="Admin or owner required")
    return user
```

**Step 2:** Test (crear `tests/unit/test_security.py` si no existe):

```python
from fastapi import HTTPException
from app.models import User
from app.security import require_admin_or_owner_api


class _Req:
    def __init__(self, username): self.session = {"username": username}


def test_require_admin_or_owner_allows_admin_and_owner(db_session):
    for role in ("admin", "owner"):
        u = User(username=f"{role}_t", password_hash="x", role=role, is_active=True, business_id=1)
        db_session.add(u); db_session.commit()
        got = require_admin_or_owner_api(_Req(f"{role}_t"), db_session)
        assert got.username == f"{role}_t"


def test_require_admin_or_owner_rejects_operator(db_session):
    u = User(username="op_t", password_hash="x", role="operator", is_active=True, business_id=1)
    db_session.add(u); db_session.commit()
    import pytest
    with pytest.raises(HTTPException) as exc:
        require_admin_or_owner_api(_Req("op_t"), db_session)
    assert exc.value.status_code == 403
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_security.py -q --no-cov 2>&1 | tail -2
# Expected: 3 passed
```

---

### T2: Aplicar RBAC en endpoints API

**Depends on:** T1
**Skill:** security-architect
**Agent:** coder
**Files:** `app/routers/inventory.py`, `app/routers/products.py`

#### Context

```python
# app/routers/inventory.py — todos usan require_user_api
from app.security import require_user_api
# app/routers/products.py — idem
```

#### Objective

Los endpoints sensibles pasan a `require_admin_or_owner_api`; sale y lecturas quedan en `require_user_api`.

#### Instructions

**Step 1 — inventory.py:** cambiar el import y reemplazar `Depends(require_user_api)` → `Depends(require_admin_or_owner_api)` SOLO en:
- `create_adjustment` (`POST /movements/adjustment`)
- `create_transfer` (`POST /movements/transfer`)
- `create_purchase` (`POST /movements/purchase`)
- `create_supplier_return_lot` (`POST /movements/return-supplier-lot`)

**NO cambiar:** `create_sale` (`POST /movements/sale`), `get_stock`, `list_stock`.

**Step 2 — products.py:** `create_product` (`POST /products`) → `require_admin_or_owner_api`; `list_all` (`GET /products`) se queda en `require_user_api`.

**Step 3:** añadir `from app.security import require_admin_or_owner_api` en ambos archivos.

#### Verification

```bash
grep -n "Depends(require" app/routers/inventory.py app/routers/products.py
# Expected: adjustment/transfer/purchase/return/product-create → require_admin_or_owner_api; sale/GET → require_user_api
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

## C2/C3 — Paginación + cache SS/ROP

### T3: `limit`/`offset` en stock_list

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/inventory_service.py`, `app/routers/ui_stock.py`

#### Context

```python
# inventory_service.py — EXISTE
def stock_list(self, query: str = "", location_code: Optional[str] = None) -> list[StockRead]:
    loc_id = None
    if location_code:
        loc_id = self._location_id_for_code(location_code)
    base_rows = list(self._inventory.stock_list(query=query, location_id=loc_id))
    ...  # recalcula SS/ROP por SKU
# inventory_repository.py — stock_list ya acepta limit/offset/prefetch_limit
```

#### Objective

`stock_list` acepta `limit`/`offset`; `ui_stock.py` stock-table pagina.

#### Instructions

**Step 1 — inventory_service.py:** cambiar la firma a:

```python
    def stock_list(
        self, query: str = "", location_code: Optional[str] = None,
        limit: Optional[int] = None, offset: int = 0,
    ) -> list[StockRead]:
```

y pasar `limit=limit, offset=offset` a `self._inventory.stock_list(...)`. Aplicar `rows = rows[offset: offset+limit]` si `limit is not None` al final (después de calcular SS/ROP). Los callers que no pasan `limit` reciben todo (comportamiento actual).

**Step 2 — ui_stock.py:** en la ruta que renderiza la tabla de stock (`stock-table`), pasar `limit=200`:

```python
    items = inventory_service.stock_list(query=query, location_code=loc, limit=200)
```

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T4: Cache de SS/ROP (dict + TTL + invalidación)

**Depends on:** T3
**Skill:** backend-developer
**Agent:** coder
**Files:** `app/services/inventory_service.py`, `app/settings.py`

#### Context

```python
# inventory_service.py — stock_list llama a sales_metrics_table por request (costo alto)
# settings.py — T2 de Fase A añadió env comunes
```

#### Objective

Cache en memoria de las métricas de ventas (SS/ROP) con TTL e invalidación en mutaciones.

#### Instructions

**Step 1 — settings.py:** añadir `INVENTORY_METRICS_TTL = int(env("INVENTORY_METRICS_TTL", "300"))`.

**Step 2 — inventory_service.py:** añadir al módulo (fuera de la clase):

```python
# Cache de métricas de ventas (SS/ROP) — H-033: evitar recalcular por request
_metrics_cache: dict[tuple, tuple] = {}
```

Dentro de la clase, métodos:

```python
    def _invalidate_metrics_cache(self) -> None:
        _metrics_cache.clear()

    def _cached_sales_metrics(self, key: tuple, start, end, location_id):
        import time as _time
        import app.settings as _settings
        hit = _metrics_cache.get(key)
        if hit is not None and (_time.monotonic() - hit[0]) < _settings.INVENTORY_METRICS_TTL:
            return hit[1]
        result = self.sales_metrics_table(start, end, location_id=location_id)
        _metrics_cache[key] = (_time.monotonic(), result)
        return result
```

Y en `stock_list`, reemplazar la llamada directa a `sales_metrics_table` por `_cached_sales_metrics((self._business_id, loc_id), start, end, loc_id)`.

**Step 3 — invalidación:** añadir `self._invalidate_metrics_cache()` al INICIO de los métodos que mutan stock: `purchase`, `sale`, `transfer`, `adjustment`, `supplier_return_by_lot`, `delete_purchase_movement`, `delete_sale_movement`, `delete_adjustment_movement`, `delete_transfer_movement`, `update_purchase`, `update_sale`, `update_transfer_shipment`, `update_adjustment_movement`.

> ⚠️ Verificar que el TTL y la invalidación no rompen los tests (los tests de stock verifican valores frescos tras mutaciones → la invalidación los mantiene correctos).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_inventory_service_unit.py tests/unit/test_inventory_repository_queries.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

## C4 — Migrar ORM a repos (H-028)

### T5: `UserRepository` + migrar `ui_users.py`

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/repositories/user_repository.py` (CREATE), `app/routers/ui_users.py` (MODIFY)

#### Context

```python
# ui_users.py — EXISTE (44 ops ORM). Patrones:
list(db.scalars(select(User).order_by(User.username.asc())))
list(db.scalars(select(Business).order_by(Business.name.asc())))
db.scalar(select(User).where(User.username == username))
db.query(UserBusiness).filter(...).delete()
db.add(UserBusiness(...))
```

#### Objective

`UserRepository` con las consultas/mutaciones de usuarios; `ui_users.py` delega.

#### Instructions

**Step 1:** Crear `app/repositories/user_repository.py`:

```python
"""Acceso a datos de usuarios (extraído de ui_users, H-028)."""
from __future__ import annotations
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.models import Business, User, UserBusiness


class UserRepository:
    def __init__(self, db: Session):
        self._db = db

    def list_users(self) -> list[User]:
        return list(self._db.scalars(select(User).order_by(User.username.asc())))

    def list_businesses(self) -> list[Business]:
        return list(self._db.scalars(select(Business).order_by(Business.name.asc())))

    def get_by_username(self, username: str) -> Optional[User]:
        return self._db.scalar(select(User).where(User.username == username))

    def get(self, user_id: int) -> Optional[User]:
        return self._db.get(User, int(user_id))

    def add(self, user: User) -> None:
        self._db.add(user)

    def delete(self, user: User) -> None:
        self._db.delete(user)

    def set_user_businesses(self, user_id: int, business_ids: list[int]) -> None:
        self._db.query(UserBusiness).filter(UserBusiness.user_id == int(user_id)).delete()
        for bid in business_ids:
            self._db.add(UserBusiness(user_id=int(user_id), business_id=int(bid)))
```

**Step 2:** En `ui_users.py`, sustituir los `select(User)`/`select(Business)`/`db.query(UserBusiness)` por llamadas al repo. Instanciar `users_repo = UserRepository(db)` al inicio de cada handler que lo use. Mantener la lógica HTTP/template intacta.

#### Verification

```bash
grep -c "select(User)\|select(Business)\|UserBusiness).filter" app/routers/ui_users.py
# Expected: 0 (todas delegadas al repo)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_users.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T6: `CustomerRepository` + migrar `ui_customers.py`

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/repositories/customer_repository.py` (CREATE), `app/routers/ui_customers.py` (MODIFY)

#### Context

```python
# ui_customers.py — EXISTE (27 ops ORM). Patrones:
select(Customer).where(Customer.business_id == int(business_id))
db.get(Customer, customer_id)
db.add(customer)
select(SalesDocument).where(...)
select(func.coalesce(func.sum(SalesDocument.total), 0)).where(...)
```

#### Objective

`CustomerRepository(db, business_id)` con las consultas de clientes; `ui_customers.py` delega.

#### Instructions

**Step 1:** Crear `app/repositories/customer_repository.py` con métodos: `list(query="")`, `get(id)`, `create(...)`, `update(...)`, `delete(id)`, `count()`, `recent_documents(customer_id, limit)`, `total_spent(customer_id)`.
**Step 2:** En `ui_customers.py`, instanciar `repo = CustomerRepository(db, business_id)` y sustituir los `select(...)` de negocio por llamadas al repo.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_customers.py -q --no-cov 2>&1 | tail -2 2>/dev/null || echo "(sin test propio; suite)"
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T7: `PurchaseRepository` + migrar `ui_purchases.py`

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/repositories/purchase_repository.py` (CREATE), `app/routers/ui_purchases.py` (MODIFY)

#### Context

```python
# ui_purchases.py — EXISTE (28 ops ORM): consultas de compras recientes, filtros, métricas del tab
```

#### Objective

`PurchaseRepository(db, business_id)` con las consultas de compras; `ui_purchases.py` delega.

#### Instructions

**Step 1:** Crear `app/repositories/purchase_repository.py` con los métodos que `ui_purchases.py` usa (list_recent, filter_by_date/query, métricas del tab). Mover VERBATIM las consultas.
**Step 2:** En `ui_purchases.py`, sustituir los `select(...)` de negocio por llamadas al repo.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T8: Ampliar `SalesDocumentService` + migrar `ui_sales_documents.py`

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/services/sales_document_service.py` (MODIFY), `app/routers/ui_sales_documents.py` (MODIFY)

#### Context

```python
# app/services/sales_document_service.py — EXISTE (reporting de documentos)
# ui_sales_documents.py — EXISTE (39 ops ORM): list_recent, get_document, customer lookups, totals
```

#### Objective

Ampliar `SalesDocumentService` con las consultas de `ui_sales_documents.py`; el router delega.

#### Instructions

**Step 1:** En `sales_document_service.py`, añadir métodos: `list_recent(limit)`, `get(doc_id)`, `list_by_customer(customer_id, limit)`, `customer_total_spent(customer_id)`, `totals_for_range(start, end)` (mover VERBATIM las consultas de ui_sales_documents).
**Step 2:** En `ui_sales_documents.py`, sustituir los `select(...)` de negocio por llamadas al service.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_sales_document_service.py tests/unit/test_ui_sales_context.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

### T9: Ampliar `BusinessRepository` + migrar `ui_businesses.py`

**Depends on:** none
**Skill:** backend-developer
**Agent:** refactor
**Files:** `app/repositories/business_repository.py` (MODIFY), `app/routers/ui_businesses.py` (MODIFY)

#### Context

```python
# business_repository.py — EXISTE (get_business_config/get_business_locations)
# ui_businesses.py — EXISTE (45 ops ORM): list_all, get, create/update/delete negocio, dependency counts
```

#### Objective

Ampliar `BusinessRepository` con el CRUD de negocios; `ui_businesses.py` delega.

#### Instructions

**Step 1:** Añadir a `business_repository.py` funciones: `list_businesses(db)`, `get_business(db, id)`, `create_business(db, code, name)`, `delete_business(db, id)`, `count_related(db, business_id)` (users/products/locations/user_businesses). Mover VERBATIM.
**Step 2:** En `ui_businesses.py`, sustituir los `select(Business)...`/`db.add(Business...)`/dependency counts por llamadas a `business_repository`.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_business_repository.py tests/unit/test_ui_businesses.py -q --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -q -m "not e2e" --no-cov 2>&1 | tail -2
```

---

## C5 — e2e en CI

### T10: Playwright en el test-gate

**Depends on:** none
**Skill:** devops-engineer
**Agent:** coder
**Files:** `.gitea/workflows/deploy.yml` (MODIFY)

#### Context

```yaml
# .gitea/workflows/deploy.yml — job test (EXISTE)
steps:
  - run: apt-get update -qq && apt-get install -y -qq python3 python3-pip
  - run: pip install -r requirements.txt --break-system-packages -q
  - run: pip install pytest pytest-asyncio --break-system-packages -q
  - run: python3 -m pytest tests/ -v --override-ini="addopts="
```

#### Objective

El gate de CI instala Playwright y ejecuta los e2e (o falla si no puede).

#### Instructions

**Step 1:** Añadir tras el `pip install pytest...`:

```yaml
  - run: pip install playwright pytest-playwright --break-system-packages -q
  - run: python3 -m playwright install chromium --with-deps
```

**Step 2:** Añadir paso de e2e DESPUÉS del pytest unitario:

```yaml
  - run: python3 -m pytest tests/e2e -m e2e --no-cov -q
```

> ⚠️ Si `--with-deps` falla en el runner (sin permisos apt), usar `python3 -m playwright install chromium` y documentar la dependencia. El job DEBE fallar si los e2e no corren (no skip silencioso).

#### Verification

```bash
# Push a develop → el job test de CI debe incluir pasos de Playwright + e2e.
# Verificar que el run en Gitea Actions muestra 11 passed en el paso e2e.
```

---

### T11: Verificación completa de la Fase C

**Depends on:** T1-T10
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** (verificación)

#### Instructions

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest -m "not e2e" --no-cov 2>&1 | tail -2
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov 2>&1 | tail -2
grep -n "require_admin_or_owner_api" app/routers/inventory.py app/routers/products.py | wc -l  # >= 5
grep -c "select(User)\|select(Business)\|select(Customer)\|select(SalesDocument)" app/routers/ui_users.py app/routers/ui_customers.py app/routers/ui_sales_documents.py  # reducido
git diff --name-only develop
```

Reportar: RBAC aplicado (≥5 endpoints), repos creados, e2e en CI, suite verde, scope = design.md §5.
