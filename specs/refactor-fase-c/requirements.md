# Fase C — Requirements (SDD Phase 1)

> Refactor final para acercar el score de la auditoría de **3.85 → ~4.0+** atacando
> Capas (H-028), Escalabilidad (H-033), Seguridad (RBAC API) y Testing (e2e en CI).
> Fuente: `docs/auditorias/2026-08-15-fase-ab.md`.

## REQs (MoSCoW)

### Must (M)

| ID | Requisito | Criterio de aceptación (Gherkin) |
|----|-----------|-----------------------------------|
| REQ-1 | **RBAC en API JSON (operador no gestiona inventario)** | Given un usuario `operator` autenticado, When hace `POST /movements/adjustment`, `/movements/transfer`, `/movements/purchase`, `/movements/return-supplier-lot` o `POST /products`, Then recibe 403. Cuando hace `POST /movements/sale`, Then puede (cajero). |
| REQ-2 | **`require_admin_or_owner_api`** | Given la security layer, When un endpoint sensible se protege, Then usa la nueva dependencia que permite admin+owner y rechaza operator. |
| REQ-3 | **Paginación en stock list (H-033)** | Given `GET /stock` o la tabla de stock, When hay N productos, Then soporta `limit`/`offset` y NO materializa todo el catálogo para renderizar. |
| REQ-4 | **Cache de SS/ROP (H-033)** | Given el cálculo de safety stock/reorder point, When se renderiza stock/restock, Then las estadísticas semanales se cachean con TTL y se invalidan al cambiar inventario (no se recalculan por request para TODO el catálogo). |
| REQ-5 | **Migrar ORM de los 5 routers top a repos/services (H-028)** | Given `ui_users`, `ui_businesses`, `ui_sales_documents`, `ui_customers`, `ui_purchases`, When ejecutan consultas de negocio, Then delegan en repositories/services (sin `select(...)`/`db.add` directos de lógica de negocio). |
| REQ-6 | **e2e en CI (test-gate)** | Given el pipeline Gitea Actions, When corre el job test, Then instala Playwright y ejecuta `tests/e2e -m e2e` (o falla explícitamente si los e2e se skipan). |

### Should (S)

| ID | Requisito | Criterio de aceptación |
|----|-----------|------------------------|
| REQ-7 | **Repositories nuevos con firmas estables** | Given `UserRepository`/`CustomerRepository`/`PurchaseRepository`, When se usan, Then exponen métodos de consulta/mutación reutilizables y testeables. |
| REQ-8 | **No-regresión de comportamiento** | Given el refactor, When corren los tests, Then 143 unit + 11 e2e siguen pasando (o más). |

### Won't (W)

| ID | Excluido | Razón |
|----|----------|-------|
| W-1 | Migrar el ORM de TODOS los 13 routers | Scope de C3 = 5 routers top (los que mueven dim3). El resto queda como backlog. |
| W-2 | `docker-compose*.yml` (H-046 compose env obligatoria) | Invariante; requiere decisión del usuario. |
| W-3 | Rotación de credenciales `admin/admin` de prod (H-036 legacy) | Ops manual, fuera de código. |
| W-4 | Caché Redis | Se usa cache en memoria (TTL) — suficiente para la escala actual; Redis es overkill. |
| W-5 | Dividir más `inventory_service.py` (1934 ln) | Fase D (ya está < 2300; la división adicional no mueve el score). |

## NFRs

- **Rendimiento:** paginación con `limit` default (p.ej. 200) y `offset`; el cache de SS/ROP reduce el recálculo a ≤1 por ventana TTL; invalidation inmediata tras purchase/sale/transfer/adjustment.
- **Seguridad:** los endpoints que cambian stock (adjustment/transfer/purchase/return/product) requieren admin/owner; sale queda operator-OK; GET de lectura queda autenticado.
- **Compatibilidad:** las respuestas de la API y la UI son idénticas (paginación con default que cubre el caso actual); los 143 unit + 11 e2e pasan.
- **Consistencia:** el cache de SS/ROP se invalida en los mismos puntos donde `_rebuild_product_fifo`/movements cambian stock.

## Fuentes de verdad

- Firmas reales: `specs/refactor-fase-c/design.md` §4.
- Envelopes: `specs/refactor-fase-c/tasks.md`.
- Invariantes: `CLAUDE.md` (no tocar docker-compose, auth.py hash, deps.py).
