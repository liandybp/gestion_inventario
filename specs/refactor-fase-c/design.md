# Fase C — Design (SDD Phase 2)

> Decisiones cerradas. OpenCode NO decide nada de esta sección.

## C1 — RBAC en API JSON (Seguridad)

- Nueva dependencia `require_admin_or_owner_api(request, db) -> User` en `app/security.py`
  (admin+owner pasan, operator → 403).
- Se aplica a los endpoints que cambian stock/gestionan el negocio en la API:

| Endpoint | Antes | Después |
|----------|-------|---------|
| `POST /movements/adjustment` | require_user_api | require_admin_or_owner_api |
| `POST /movements/transfer` | require_user_api | require_admin_or_owner_api |
| `POST /movements/purchase` | require_user_api | require_admin_or_owner_api |
| `POST /movements/return-supplier-lot` | require_user_api | require_admin_or_owner_api |
| `POST /products` (create) | require_user_api | require_admin_or_owner_api |
| `PUT/DELETE /products/{sku}` (si existen) | require_user_api | require_admin_or_owner_api |
| `POST /movements/sale` | require_user_api | require_user_api (cajero operator-OK) |
| GET (stock, products list) | require_user_api | require_user_api (lectura autenticada) |

## C2 — Paginación en stock list (H-033)

- `InventoryService.stock_list(query="", location_code=None, limit=None, offset=0)`:
  - `limit`/`offset` se pasan a `InventoryRepository.stock_list` (ya soporta `prefetch_limit`/`limit`).
  - Si `limit` es None → comportamiento actual (todo) para callers que necesitan el catálogo completo
    (home, transfers bulk, ventas datalist). Los callers de la TABLA de stock pasan `limit=200` (default).
- `ui_stock.py` stock-table: pasa `limit=200` (o el que esté activo) y se paginiza el render.
- **No-regresión:** los callers que no pasan `limit` (home, ventas, transfers) siguen recibiendo todo.

## C3 — Cache de SS/ROP (H-033)

- El costo es `sales_metrics_table` (mean/std semanal por SKU en Python) llamado por `stock_list`.
- **Cache en memoria (dict módulo)** en `inventory_service.py`:
  - Clave: `(business_id, location_id)`.
  - Valor: `(timestamp, resultado_de_sales_metrics_table)`.
  - TTL: `300s` (configurable `INVENTORY_METRICS_TTL` en settings).
  - Invalidación: `_invalidate_metrics_cache()` llamada tras TODA mutación de inventario
    (purchase, sale, transfer, adjustment, supplier_return, delete_*movement, update_*).
- `stock_list` consulta el cache antes de recalcular SS/ROP; si es stale o no está, recalcula y guarda.

## C4 — Repositories + migrar ORM de los 5 routers top (H-028)

Se crean/amplían repos y se migra la lógica ORM de negocio de los routers:

| Router (ops ORM) | Target repository | Migrar |
|------------------|-------------------|--------|
| `ui_users.py` (44) | `UserRepository` (nuevo, `app/repositories/user_repository.py`) | list_users, list_businesses, get_by_username, create/update/delete, set_businesses, sync user_businesses |
| `ui_customers.py` (27) | `CustomerRepository` (nuevo, `app/repositories/customer_repository.py`) | list_by_business, get, create, update, delete, count, recent |
| `ui_purchases.py` (28) | `PurchaseRepository` (nuevo, `app/repositories/purchase_repository.py`) | list_recent, get_movement, filter_queries, dashboard metrics de compras |
| `ui_sales_documents.py` (39) | `SalesDocumentService` (ampliar, `app/services/sales_document_service.py`) | list_recent, get, customer lookups, totals |
| `ui_businesses.py` (45) | `BusinessRepository` (ampliar, `app/repositories/business_repository.py`) | list_all, get, create, update, delete de negocio, dependency counts |

**Regla de migración:** mover el QUERY al repo (método con firma clara), el router llama al repo
y conserva la lógica HTTP/template. NUNCA cambiar el comportamiento. El router puede conservar
`db` para lo que no sea lógica de negocio (p.ej. transacciones simples), pero los `select(...)`
de negocio pasan al repo.

**Prioridad:** ui_users → ui_customers → ui_purchases → ui_sales_documents → ui_businesses.
Cada migración = 1-2 envelopes con verificación de tests.

## C5 — e2e en CI (test-gate)

- `.gitea/workflows/deploy.yml` job `test`:
  - `pip install playwright` + `python3 -m playwright install chromium --with-deps`.
  - Ejecutar `python3 -m pytest tests/e2e -m e2e --no-cov -q` como paso del gate.
  - Si Playwright no se puede instalar → fallar el job (no skip silencioso).

## Fuera de scope (W)

- Migrar el ORM de TODOS los routers (solo los 5 top en C4).
- `docker-compose*.yml` (H-046) — invariante, requiere decisión.
- Credenciales `admin/admin` de prod (ops manual).
- Redis — cache en memoria es suficiente.

---

## 4. Firmas reales del repo (NO modificar salvo la tarea que lo indica)

```python
# app/security.py — EXISTE
def require_user_api(request, db=Depends(session_dep)) -> User: ...
def require_admin_api(request, db=Depends(session_dep)) -> User: ...  # solo admin, 403 si no
def is_admin(user) -> bool / is_owner(user) / is_operator(user)

# app/services/inventory_service.py — EXISTE
def stock_list(self, query: str = "", location_code: Optional[str] = None) -> list[StockRead]:
    # recalcula SS/ROP por SKU (costo alto) — T3/T4 añaden cache + limit/offset
def sales_metrics_table(self, start, end, location_id=None): ...  # mean/std semanal
def _consume_fifo(...) / _rebuild_product_fifo(...): ...          # mutan stock

# app/repositories/inventory_repository.py — EXISTE
def stock_list(self, query="", location_id=None, limit=None, offset=None, ...): ...  # ya tiene prefetch_limit

# app/repositories/business_repository.py — EXISTE (ampliar)
def get_business_locations(db, business_id) -> LocationsConfig
def get_business_config(db, business_id) -> BusinessConfig

# app/services/sales_document_service.py — EXISTE (ampliar)

# app/routers/inventory.py — EXISTE (API)
@router.post("/movements/adjustment", response_model=MovementResult)
def create_adjustment(payload, user=Depends(require_user_api), service=Depends(...)): ...

# app/routers/ui_users.py — EXISTE (44 ops ORM a migrar)
def list_users(db): return list(db.scalars(select(User).order_by(User.username.asc())))
```

## 5. Inventario de archivos (scope duro)

### CREATE
- `app/repositories/user_repository.py`
- `app/repositories/customer_repository.py`
- `app/repositories/purchase_repository.py`

### MODIFY
- `app/security.py` (require_admin_or_owner_api)
- `app/routers/inventory.py` (RBAC en adjustment/transfer/purchase/return)
- `app/routers/products.py` (RBAC en create)
- `app/services/inventory_service.py` (limit/offset + cache SS/ROP + invalidación)
- `app/routers/ui_stock.py` (paginación en stock-table)
- `app/repositories/business_repository.py` (ampliar: CRUD negocio + dependency counts)
- `app/services/sales_document_service.py` (ampliar: list_recent, get, totals)
- `app/routers/ui_users.py`, `ui_customers.py`, `ui_purchases.py`, `ui_sales_documents.py`, `ui_businesses.py` (delegar en repos)
- `app/settings.py` (INVENTORY_METRICS_TTL)
- `.gitea/workflows/deploy.yml` (Playwright + e2e en test)

### NO SE TOCAN
`docker-compose*.yml`, `app/auth.py`, `app/deps.py`, `app/models.py`, `app/schemas.py`,
`app/templates/*`, `app/agent/*`, `app/routers/ui_auth.py`.
