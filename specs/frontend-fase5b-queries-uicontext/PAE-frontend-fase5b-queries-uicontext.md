# PAE: Frontend fase 5b — consultas fuera de los routers UI y `UIContext` completo

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:  cc_local_pae <repo> specs/frontend-fase5b-queries-uicontext/tasks.md
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     Si un humano o cc_local lo ejecuta a mano:
     1. Sesión nueva por lote. Leer SOLO los envelopes del lote (Read offset/limit sobre tasks.md).
     2. El envelope es la única fuente: código exacto, sin consultar notas ni docs genéricas.
     3. Verificación de cada envelope; si falla 3 veces, parar y reportar.
     4. Si el contexto se compacta a mitad de lote: parar.
     5. Un commit por lote; nunca push, sudo, systemctl ni scripts de despliegue.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestión de inventario |
| **Rama base** | `develop` (`a44338e`, merge de la fase 5a) |
| **Rama de trabajo** | `feature/frontend-fase5b` (ya creada) |
| **Fecha** | 2026-11-02 |
| **Versión actual** | `VERSION` = 0.4.15; release en curso **0.5.0 agrupada (sin bump por commit)** |
| **PAE anterior** | `specs/frontend-fase5a-features-templates/` |
| **Fase** | planificación (Claude Code + DeepSeek) → ejecución (cc_local) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/frontend-fase5b-queries-uicontext/requirements.md` | REQ-1..13 con Gherkin, MoSCoW, NFRs, fuera de alcance |
| Phase 2 — Design | `specs/frontend-fase5b-queries-uicontext/design.md` | Arquitectura, componentes §2.3, pre-mortem §2.9, plan de lotes §2.10 |
| Phase 3 — Task Envelopes | `specs/frontend-fase5b-queries-uicontext/tasks.md` | 60 envelopes autocontenidos en 9 lotes |

### Skills asignadas a cc_local

- [x] `backend-developer` — código y repositorios/servicios
- [x] `test-automation-engineer` — guardas y tests unitarios
- [x] `technical-writer` — CLAUDE.md, CHANGELOG, memoria Serena (REQ-12)

---

## 1. Objetivo

### 1.1 Qué se construye

Sacar las consultas a BD de los routers UI (`app/routers/ui_*.py`) hacia repositorios/servicios, y
completar `UIContext` para que resuelva usuario/negocio/código/configuración **una sola vez por
petición**. En concreto: (1) `get_active_business_id` gana un parámetro opcional `user=`; (2)
`UIContext` añade `active_business_id`, `business_code` y `config` perezosos; (3) las ~50 llamadas
directas a `get_active_business_config/code/id` de los routers pasan a `ctx.config`/`ctx.business_code`/
`ctx.active_business_id` y los auxiliares reciben `ctx`; (4) las consultas ORM directas se mueven a
métodos nuevos o existentes de repositorios/servicios; (5) dos guardas nuevas
(`test_routers_no_orm.py`, `test_ui_query_count.py`) impiden regresiones.

### 1.2 Por qué

La auditoría de arquitectura del frontend (`docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`
§3-§4) señaló que los routers UI ejecutan consultas directas y resuelven el negocio 1-4 veces por
petición (4 `SELECT` sobre `users` en `tab_sales`). La fase 4 dejó `UIContext` a medias: `business_id`
vuelve a consultar el usuario por dentro.

### 1.3 Valor esperado

- De 4 a **1** `SELECT` sobre `users` por petición UI (REQ-3, medido por `test_ui_query_count.py`).
- Una única implementación por consulta duplicada (REQ-6) y una única comprobación "el movimiento es
  de este negocio" (REQ-7).
- **El HTML no cambia**: `ui_snapshot.py check` y `style_snapshot.py check` con 0 diferencias contra la
  baseline de la fase 5a.

### 1.4 Qué NO se toca

- Routers API JSON (`products.py`, `inventory.py`, `sync.py`) y `business_repository.get_active_business_config`:
  siguen sin `user=`.
- `ui_auth.py` no migra a `UIContext` (el login corre antes de que haya sesión); sí entra en la guarda ORM.
- Los `commit()`/`rollback()` no se mueven: la frontera transaccional de cada handler es la misma.
- REQ-13 (partir routers grandes), SKU único compuesto, bug de fecha/FIFO de compras/traspasos/gastos.
- Mensajes, códigos HTTP y HTML: idénticos.

---

## 2. Decisiones de diseño

| # | Decisión | Alternativa | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Solo `get_active_business_id` gana `user=` | También `require_active_business_id` y `get_active_business_code` | `UIContext` deriva el 409 y el código del id cacheado; tocar más del invariante no ahorra consultas | `security.py` queda casi intacto; test de REQ-1 para 3 roles |
| D2 | `ctx.business_code` usa `business_repository.get_business_code(db, id)` | `get_active_business_code(db, request)` | Esta volvería a resolver el negocio (una consulta más a `user_businesses` para owners) | Nueva función en `business_repository` |
| D3 | `commit` en el router, como hoy | Mover `commit` a servicios | Cambia la semántica de ~70 handlers | Ningún método nuevo hace `commit()` |
| D4 | Parseo de ubicaciones en el router, BD en el repositorio | Todo al repositorio | `_parse_spec`/`_prefix_location_code` son capa UI | `reconcile_locations` recibe códigos finales |
| D5 | Reutilizar `PurchaseRepository.get_product` (sin filtro) | `ProductRepository.get_by_id` con filtro | El `product_id` sale de un movimiento ya validado por `get_movement` (REQ-10) | Un método nuevo duplicaría uno existente |
| D6 | `extract_sku` se mueve a `product_service.py` | Dejarlo en `ui_common.py` | El servicio importaría de la capa de routers (dirección prohibida, dimensión 3) | 6 importadores actualizados, sin reexport |
| D7 | Guarda ORM por regex por línea | AST | Mismo patrón que `test_routers_no_html.py`; las llamadas ORM son siempre `db.x(` | `test_routers_no_orm.py` |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
[Handler UI] ──ctx=ui_context(request,db)──▶ ★UIContext (user, active_business_id, business_id, business_code, config)
      │                                          │
      │ usa ctx.config/ctx.business_code/ctx.user
      │                                          ▼
      ├──▶ ✦business_repository.get_business_code / get_business_config (ya existentes)
      ├──▶ ★ProductService.resolve_sku / barcode_to_sku ──▶ ✦ProductRepository (first_by_name, list_categories, get_by_lot_code, deletable_skus)
      ├──▶ ★InventoryRepository.get_movement / transfer_out_by_ref / transfer_items_by_ref / skus_with_movements / skus_with_location_history
      ├──▶ ✦SalesDocumentService.delete_document / delete_item / add_item / add_document (ya existía list_recent/customers_list)
      ├──▶ ✦UserRepository.detach_references + add/delete (ya existían)
      ├──▶ ✦CoachService.add_cash_baseline
      ├──▶ ★audit_repository.list_auth_events
      └──▶ ✦business_repository location_has_data / reconcile_locations / create_locations / delete_locations / list_businesses_by_ids

★ = NUEVO   ✦ = MODIFICADO   · = sin cambios (reutilizado)
```

### 3.2 Inventario de archivos

> **Scope duro:** solo estos archivos. **Verificación:** `git diff --name-only a44338e` debe mostrar
> exactamente este conjunto (más `specs/frontend-fase5b-queries-uicontext/`).

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/security.py` | MODIFY | `get_active_business_id` acepta `user=` (REQ-1) |
| `app/routers/ui_context.py` | MODIFY | `UIContext` completo: `active_business_id`/`business_code`/`config` (REQ-2) |
| `app/repositories/business_repository.py` | MODIFY | `get_business_code`, `list_businesses_by_ids`, `location_has_data`, `reconcile_locations`, `create_locations`, `delete_locations` |
| `app/repositories/product_repository.py` | MODIFY | `first_by_name`, `list_categories`, `get_by_lot_code`, `deletable_skus` |
| `app/repositories/inventory_repository.py` | MODIFY | `get_movement`, `skus_with_location_history`, `transfer_out_by_ref`, `transfer_items_by_ref`, `skus_with_movements` |
| `app/repositories/user_repository.py` | MODIFY | `detach_references` |
| `app/repositories/audit_repository.py` | CREATE | `list_auth_events` |
| `app/services/product_service.py` | MODIFY | `extract_sku` (módulo), `barcode_to_sku`, `resolve_sku` |
| `app/services/inventory_service.py` | MODIFY | renombra `_transfer_out_id_for_movement_id`/`_transfer_codes_from_out_note` a públicos |
| `app/services/sales_document_service.py` | MODIFY | `delete_document`, `delete_item`, `add_item`, `add_document` |
| `app/services/coach_service.py` | MODIFY | `add_cash_baseline` |
| `app/routers/ui_common.py` | MODIFY | elimina `extract_sku` y `barcode_to_sku` |
| `app/routers/ui_tabs_common.py` | MODIFY | `_home_locations_context(ctx)`, `_location_name_for_code(ctx)`, `_restock_items_with_location_history` vía repo, borra `_deletable_skus` |
| `app/routers/ui_agent.py` | MODIFY | `_currency_symbol(ctx)` |
| `app/routers/ui_home.py` | MODIFY | `ctx.*` + dashboard vía `business_repository` |
| `app/routers/ui_profit.py` | MODIFY | `ctx.business_code` + `_home_locations_context(ctx)` |
| `app/routers/ui_dividends.py` | MODIFY | `ctx.config` |
| `app/routers/ui_extractions.py` | MODIFY | `ctx.config` (4) |
| `app/routers/ui_customers.py` | MODIFY | `ctx.config` + `CustomerRepository.delete` |
| `app/routers/ui_stock.py` | MODIFY | `ctx.*` + `ProductRepository.list_categories`/`deletable_skus` |
| `app/routers/ui_products.py` | MODIFY | `ctx.*` + `ProductRepository.first_by_name`/`list_categories` + `ProductService.barcode_to_sku` |
| `app/routers/ui_sales.py` | MODIFY | auxiliares a `ctx` + `resolve_sku`/`barcode_to_sku` + `get_movement` + `SalesDocumentService` |
| `app/routers/ui_purchases.py` | MODIFY | auxiliares a `ctx` + `ctx.user` en `_render_error` |
| `app/routers/ui_sales_documents.py` | MODIFY | `ctx.config` (10) + `SalesDocumentService` escrituras |
| `app/routers/ui_transfers.py` | MODIFY | `ctx.*` + `resolve_sku` + `get_movement` + `transfer_*` + métodos públicos |
| `app/routers/ui_profit_items.py` | MODIFY | `_render_profit_items_tab(ctx)` + `get_movement`/`get_product`/`get_lot_by_movement` |
| `app/routers/ui_businesses.py` | MODIFY | `create_locations`/`reconcile_locations`/`delete_locations` |
| `app/routers/ui_activity.py` | MODIFY | `audit_repository.list_auth_events` |
| `app/routers/ui_users.py` | MODIFY | `UserRepository.add`/`detach_references`/`delete` |
| `app/routers/ui_coach.py` | MODIFY | `CoachService.add_cash_baseline` |
| `app/routers/ui_auth.py` | MODIFY | `UserRepository.get` + `get_business` |
| `tests/unit/test_routers_no_orm.py` | CREATE | guarda ORM + resolución de negocio (REQ-5/REQ-4) |
| `tests/unit/test_ui_query_count.py` | CREATE | guarda de recuento de `users` (REQ-3) |
| `tests/unit/test_security.py` | MODIFY | casos de REQ-1 (3 roles) |
| `tests/unit/test_ui_context.py` | MODIFY | casos de REQ-2 |
| `tests/unit/test_ui_sales_context.py` | MODIFY | `_sale_pos_context(ctx)` |
| `tests/unit/test_profit_items_date_filter.py` | MODIFY | `_home_locations_context(ctx)` |
| `tests/unit/test_inventory_service_unit.py` | MODIFY | métodos públicos (REQ-8) |
| `tests/unit/test_repositories.py` | MODIFY | aislamiento de `ProductRepository` nuevos |
| `tests/unit/test_inventory_repository_queries.py` | MODIFY | aislamiento de `get_movement`/`transfer_out_by_ref` |
| `CHANGELOG.md` | MODIFY | entrada fase 5b (REQ-12) |
| `CLAUDE.md` | MODIFY | Estado actual (REQ-12) |
| `.claude/CLAUDE.md` | MODIFY | invariante de `app/security.py` (REQ-12, versionado) |
| `.serena/memories/conventions.md` | MODIFY | guarda ORM (REQ-12) |
| `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` | MODIFY | fila "5" ✅ (REQ-12) |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/security.py — YA EXISTE (REQ-1 lo extiende)
def get_current_user_from_session(db: Session, request: Request) -> Optional[User]: ...
def get_active_business_id(db: Session, request: Request) -> Optional[int]: ...
def require_active_business_id(db: Session, request: Request) -> int: ...
def get_active_business_code(db: Session, request: Request) -> Optional[str]: ...

# app/repositories/business_repository.py — YA EXISTE
def get_business_config(db: Session, business_id: Optional[int]) -> BusinessConfig: ...
def get_business(db: Session, business_id: int) -> Optional[Business]: ...
def list_businesses(db: Session) -> list[Business]: ...

# app/repositories/product_repository.py — YA EXISTE
class ProductRepository:
    def get_by_sku(self, sku: str) -> Optional[Product]: ...

# app/repositories/purchase_repository.py — YA EXISTE
class PurchaseRepository:
    def get_product(self, product_id: int) -> Optional[Product]: ...
    def get_lot_by_movement(self, movement_id: int) -> Optional[InventoryLot]: ...

# app/services/sales_document_service.py — YA EXISTE
class SalesDocumentService:
    def list_recent(self, limit=10, start=None, end=None) -> list[SalesDocument]: ...
    def customers_list(self, limit: int = 200) -> list[Customer]: ...
```

### 3.4 Interfaces nuevas a implementar

```python
# app/repositories/business_repository.py
def get_business_code(db, business_id: Optional[int]) -> Optional[str]: ...
def list_businesses_by_ids(db, ids: list[int]) -> list[Business]: ...
def location_has_data(db, location_id: int) -> bool: ...
def reconcile_locations(db, business_id, central, pos, default_pos_code) -> Optional[str]: ...
def create_locations(db, business_id, central, pos, default_pos_code) -> None: ...
def delete_locations(db, business_id) -> None: ...

# app/repositories/product_repository.py (métodos de ProductRepository)
def first_by_name(value) -> Optional[Product] / list_categories() / get_by_lot_code(code) / deletable_skus(skus)

# app/repositories/inventory_repository.py (métodos de InventoryRepository)
def get_movement(movement_id, types) -> Optional[InventoryMovement]
def skus_with_location_history(location_id, skus) / transfer_out_by_ref(ref) / transfer_items_by_ref(ref) / skus_with_movements(...)

# app/repositories/audit_repository.py
def list_auth_events(db, *, username, start, end, limit=2000) -> list[AuditLog]: ...

# app/services/product_service.py
def extract_sku(product_field: str) -> str: ...
ProductService.barcode_to_sku(barcode) -> str
ProductService.resolve_sku(value, *, barcode_fallback=False) -> str
```

---

## 4. Fases de implementación (Task Envelopes)

> Los 60 envelopes están en `specs/frontend-fase5b-queries-uicontext/tasks.md`, agrupados en 9 lotes:
> `Lotes: **A** T1–T4 · **B** T5–T11 · **C** T12–T18 · **D** T19–T25 · **E** T26–T32 · **F** T33–T39 · **G** T40–T46 · **H** T47–T53 · **I** T54–T60`.
> `cc_local_pae` lee esa línea y ejecuta un lote por sesión limpia, pasando solo los rangos de líneas.

| Lote | Contenido | Cierre |
|------|-----------|--------|
| **A (Lote 0)** | `test_ui_query_count.py` en modo medida (camino feliz: login → ruta → 200 + recuento); REQ-1 (`security.py` + tests); REQ-2 (`get_business_code` + `UIContext` + tests) | unit verdes; `ui_snapshot.py check` OK |
| **B** | REQ-4: auxiliares a `ctx` (tabs_common, agent, home, profit, dividends, extractions, customers) | tras el lote: unit + `check` |
| **C** | REQ-4: auxiliares a `ctx` (stock, products, sales ×2, purchases ×2, sales_documents) | tras el lote: unit + `check` |
| **D** | REQ-4: auxiliares a `ctx` (transfers ×2, profit_items) + movimiento de `extract_sku` + 6 importadores | tras el lote: unit + `check` |
| **E** | REQ-6/REQ-9: `ProductService.barcode_to_sku`/`resolve_sku`, `ProductRepository` nuevos, routers products/stock/sales/transfers | unit + `check` |
| **F** | REQ-7/REQ-8: `InventoryRepository` nuevos + renombrado a públicos + routers transfers/sales/profit_items | unit + `check` + e2e de traspasos y Utilidades |
| **G** | Escrituras: `business_repository` (ubicaciones), dashboard, `audit_repository`, `ui_tabs_common` | unit + `check` |
| **H** | Escrituras: `SalesDocumentService`, `UserRepository`, `CoachService`, customers, auth, `_sales_doc_context` | unit + `check` + e2e de negocios/usuarios/documentos/coach |
| **I** | Guarda `test_routers_no_orm.py`; afirmar todo REQ-3; imports sin uso; docs (REQ-12); cierre REQ-11 | suite completa: `pytest tests/unit`, `ui_snapshot.py check`, `pytest tests/e2e -m e2e`, `style_snapshot.py check` (0 diffs) |

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini="addopts="
```

Si alguna falla antes de empezar, reportar y no implementar.

### 5.1 Verificación por envelope

Cada envelope lleva `#### Verification` con el comando exacto (test o `ui_snapshot.py check`). El gate
de cada lote es `--test-cmd` (suite unitaria completa + snapshots). Dentro de un lote, un envelope
puede dejar temporalmente en rojo tests que arregla otro envelope del MISMO lote (cambio de firma de
auxiliar); el gate es el final del lote.

### 5.2 e2e (comportamiento real)

Este PAE es de **equivalencia de datos**: los REQ se verifican contra la capa de servicio/API sin
renderizar UI nueva, así que no se añaden e2e nuevos. El REQ-11 exige que los e2e existentes sigan en
verde (cubren traspasos, Utilidades por artículo, negocios, usuarios, documentos y coach — los flujos
que este PAE mueve). El test de comportamiento del Lote 0 es `test_ui_query_count.py` (login → ruta →
200 + recuento de SQL real), que recorre las 6 rutas de REQ-3.

### 5.3 Tests existentes que deben seguir pasando

- `tests/unit/` — 499 unit (base de 5a); ninguno puede romperse salvo los 4 que el propio PAE
  actualiza (`test_security.py`, `test_ui_context.py`, `test_ui_sales_context.py`,
  `test_profit_items_date_filter.py`, `test_inventory_service_unit.py`).
- `tests/e2e -m e2e` — 94 e2e; fixture `js_errors` autouse.
- `tests/snapshots/ui_snapshot.py check` — 91 snapshots, reloj de Inicio congelado.
- `tests/snapshots/style_snapshot.py check` — 74 capturas, fuera de CI.

### 5.4 Cobertura de los métodos nuevos (dimensión 9)

- REQ-3 (recuento) → `test_ui_query_count.py`.
- REQ-5/REQ-4 (guardas) → `test_routers_no_orm.py`.
- Aislamiento por negocio de `ProductRepository` nuevos → `test_repositories.py` (T28).
- Aislamiento de `get_movement`/`transfer_out_by_ref` (REQ-7) → `test_inventory_repository_queries.py` (T33).
- El resto de los métodos nuevos ("cuerpo tal cual" movido) queda ejercitado por el arnés de snapshots
  (renderiza cada ruta contra una BD sembrada) y por los e2e de flujo.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Un `ctx.*` cambia el orden de errores (409 antes que 403) | Media | Medio | Un handler que hoy comprueba rol antes que negocio da otro código | `active_business_id`/`business_code`/`config` no lanzan; el 409 sale solo de `business_id` (REQ-2). El e2e de roles lo cubre | `git revert` |
| R2 | `get_movement` olvida comprobar el tipo y un handler acepta otro tipo | Baja | Alto | Un POST acepta un movimiento de tipo distinto | `types` es obligatorio (sin default); test de tipo en T33 | `git revert` |
| R3 | Un servicio importa de `app/routers/` (`extract_sku`) | Media | Medio | Dependencia de capa invertida (dimensión 3) | `extract_sku` se mueve a `product_service.py` (T22) | `git revert` |
| R4 | `ctx.config` cacheado queda obsoleto si el handler escribe el negocio | Baja | Medio | Config vieja tras editar negocio | Verificado: `ui_businesses.py` (único que escribe Business/Location) no lee config tras escribir; comentario en `ui_context.py` | `git revert` |
| R5 | Se pierde un `try/except`/`_log.` al mover un cuerpo | Media | Medio | Un error deja de loguearse | Cada sitio conserva su `try/except`+`_log.*` en el router (dimensión 5); recuento de `_log.` en T60 | `git revert` |
| R6 | cc_local se aparta de los envelopes de documentación (lección 5a) | Media | Medio | CLAUDE.md/CHANGELOG mal editados | Los envelopes de docs dan el texto completo + verificación mecánica `git diff ... | grep -c "^-"` == 0 | re-hacer el envelope |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit

- [ ] **Pre-check:** suite unitaria en verde ANTES de empezar (guardar output)
- [ ] **Guardas:** `test_routers_no_orm.py` y `test_ui_query_count.py` verdes
- [ ] **Tests:** `pytest tests/unit` verde (499+ nuevos)
- [ ] **e2e:** `pytest tests/e2e -m e2e --no-cov` verde (94)
- [ ] **Snapshots:** `ui_snapshot.py check` OK (0 diffs) y `style_snapshot.py check` OK (0 diffs)
- [ ] **Invariantes:** `app/deps.py`, `app/auth.py`, `app/db.py` no tocados; `security.py` solo
  `get_active_business_id` con `user=`
- [ ] **Scope:** `git diff --name-only a44338e` = archivos de §3.2 + `specs/…/`, nada más
- [ ] **Docs:** CHANGELOG, ambos CLAUDE.md, diagnóstico fila 5 y memoria Serena actualizados
- [ ] **Sin bump de versión** (0.5.0 agrupada)

### 7.2 Script de verificación completa (REQ-11)

```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini="addopts="
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov -q
python3 tests/snapshots/style_snapshot.py check /tmp/gestion_inventario-style-baseline-bf38a2b --allow tests/snapshots/style_allowlist.txt
git grep -n "db\.\(get\|scalar\|scalars\|execute\|query\|add\|delete\)(" app/routers/ui_*.py   # debe estar vacío
git diff --name-only a44338e
```

### 7.3 Commits esperados (uno por lote)

```
refactor(frontend-fase5b): lote A — query-count (medida) + REQ-1 security + REQ-2 UIContext
refactor(frontend-fase5b): lote B — auxiliares a ctx (tabs_common/agent/home/profit/dividends/extractions/customers)
refactor(frontend-fase5b): lote C — auxiliares a ctx (stock/products/sales/purchases/sales_documents)
refactor(frontend-fase5b): lote D — transfers/profit_items a ctx + extract_sku a product_service
refactor(frontend-fase5b): lote E — ProductService.barcode_to_sku/resolve_sku + ProductRepository
refactor(frontend-fase5b): lote F — InventoryRepository + renombrado público (REQ-7/REQ-8)
refactor(frontend-fase5b): lote G — business_repository ubicaciones + dashboard + audit_repository
refactor(frontend-fase5b): lote H — SalesDocumentService/UserRepository/CoachService + customers/auth
docs(frontend-fase5b): lote I — guardas + REQ-3 aserción + docs (REQ-12)
```

---

## Ejecución

> Comando que ejecuta el paso 4 del loop (lo lanza `cc_deepseek` tras la aprobación, en segundo plano).
> Tomado de H-810 y del `CLAUDE.md` del proyecto. La suite unitaria completa corre ~11 s.

```bash
cc_local_pae /home/liandy/PyCharmMiscProject/gestion_inventario specs/frontend-fase5b-queries-uicontext/tasks.md \
  --test-cmd "python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q --override-ini=\"addopts=\"" \
  --lot0-test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_ui_query_count.py tests/unit/test_security.py tests/unit/test_ui_context.py -q --override-ini=\"addopts=\" && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline"
```

Notas para `cc_deepseek` (paso 4):
- Rama ya creada: `feature/frontend-fase5b` desde `a44338e` (árbol limpio).
- No hace falta `--free-port`: los e2e usan puerto libre dinámico (`tests/e2e/conftest.py::_free_port`).
- Sin bump de versión: `VERSION` se queda en 0.4.15; el release 0.5.0 es agrupado.
- El Lote 0 (A) es el gate duro: si `test_ui_query_count.py` no cierra el flujo (ningún handler
  devuelve 200), PARAR y devolver a `cc_claude_code`.

---

> **PAE complete at `specs/frontend-fase5b-queries-uicontext/PAE-frontend-fase5b-queries-uicontext.md`.**
> **60 task envelopes ready for cc_local.**
> **Awaiting approval before any implementation begins.**

## Revisión de cc_claude_code antes del lanzamiento (2026-11-02)

Cuatro correcciones en `tasks.md`; el resto de los 60 envelopes se aprueba tal cual:

1. **T3/T4:** T3 quita `require_active_business_id` de `ui_context.py`, y dos tests existentes de
   `test_ui_context.py` lo parchean (`AttributeError`: el Lote 0 habría fallado). T4 Step 0 los sustituye por
   versiones que parchean `get_active_business_id`.
2. **T5/T12/T21:** T5 cambiaba la firma de dos auxiliares cuyos llamadores `ui_stock.py` (T12, lote C) y
   `ui_profit_items.py` (T21, lote D) no se migraban hasta lotes posteriores. Además, `test_ui_regressions.py` y
   `test_profit_items_date_filter.py` no estaban en ningún `**Files:**`. El lote B habría cerrado en rojo. T5 Step 4
   cambia esos 3 llamadores (el de `ui_profit_items` de forma transitoria, con `ui_context(request, db)`) y los
   2 tests; T12 y T21 se ajustan a ese estado.
3. **T54:** la guarda numeraba las líneas después de filtrar los comentarios (líneas citadas desplazadas). Ahora
   numera antes.
4. **T55:** se añade la aserción de REQ-3 sobre `user_businesses` (≤ 1; ≤ 2 en `/ui/dashboard`).
