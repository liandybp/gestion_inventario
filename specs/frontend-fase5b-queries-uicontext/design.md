# Design — Frontend fase 5b: consultas fuera de los routers UI y `UIContext` completo

Requisitos: `requirements.md` (REQ-1..13). Rama base: `develop` tras el merge de 5a. Las líneas citadas son de
`bf38a2b`; 5a solo cambia cadenas de ruta de plantilla, así que los números de línea de Python no se mueven.
**Fase 3: re-verificar cada línea con `find_symbol` antes de escribir el envelope.**

## 2.1 Anclaje en el código (verificado 2026-11-02 sobre `bf38a2b`)

### Resolución de usuario y negocio

- `app/security.py:46` `get_active_business_id(db, request)`: su primera línea es
  `user = get_current_user_from_session(db, request)`. Según el rol:
  - operator: hace `pop` de `active_business_id` en la sesión.
  - owner: llama a `get_user_business_ids` (una consulta a `user_businesses`).
  - admin: puede escribir `session["active_business_id"]`.
- `security.py:120` `require_active_business_id` = `get_active_business_id` + 409 `"Active business_id is required"`.
- `security.py:195` `get_active_business_code` = `get_active_business_id` + `select(Business.code)`, con la
  normalización `(str(code).strip() if code is not None else None) or None`.
- `app/repositories/business_repository.py:132` `get_active_business_config(db, request)` =
  `get_business_config(db, get_active_business_id(db, request))`. Con negocio `None` devuelve `BusinessConfig()` y
  **nunca lanza** (`:77-83`).
- `app/routers/ui_context.py`: `UIContext.user` cachea `get_current_user_from_session`; `business_id` llama a
  `require_active_business_id`, que **vuelve a consultar el usuario**.
- `app/main.py:93` `ui_auth_middleware`: solo lee la sesión, sin BD. El mínimo alcanzable es 1 SELECT a `users`.
- Traza de `tab_sales` (`ui_sales.py:690`): 4 resoluciones de usuario (`ctx.business_id`:703, `ctx.user`:705,
  `get_active_business_code`:742, `get_active_business_config`:743). Para un owner, 3 consultas a `user_businesses`.
- Llamadas `get_active_business_config(db, request)`/`get_active_business_code(db, request)` en `app/routers/ui_*.py`:
  50. Otras directas: `ui_home.py:165` `get_active_business_id`, `ui_sales.py:116` `require_active_business_id`,
  `ui_profit_items.py:52-53`, `ui_purchases.py:87` y `ui_transfers.py:169` `get_current_user_from_session`.

### Acceso ORM en routers UI (sitios a mover)

| Fichero:línea (`bf38a2b`) | Qué hace | Destino (§2.3) |
|---|---|---|
| `ui_home.py:169` | `db.get(Business, id)` | `business_repository.get_business` (existe) |
| `ui_home.py:173` | todos los negocios por código | `business_repository.list_businesses` (existe, mismo orden) |
| `ui_home.py:178-183` | negocios de una lista de ids, por código | `business_repository.list_businesses_by_ids` (NUEVO) |
| `ui_businesses.py:62-75` `_location_has_data` | count en 3 tablas por `location_id` | `business_repository.location_has_data` (NUEVO, cuerpo tal cual) |
| `ui_businesses.py:78-146` `_reconcile_locations` | lee/crea/borra `Location` | `business_repository.reconcile_locations` (NUEVO) |
| `ui_businesses.py:452-472` | crea central + POS de un negocio nuevo | `business_repository.create_locations` (NUEVO) |
| `ui_businesses.py:786` | borra las ubicaciones del negocio | `business_repository.delete_locations` (NUEVO) |
| `ui_activity.py:46-59` | eventos `auth` login/logout filtrados | `app/repositories/audit_repository.py::list_auth_events` (CREATE) |
| `ui_products.py:603-607` | producto por nombre `ilike` | `ProductRepository.first_by_name` (NUEVO) |
| `ui_products.py:679-685`, `ui_stock.py:277-283` | categorías distintas | `ProductRepository.list_categories` (NUEVO, REQ-6) |
| `ui_common.py:175-249` `barcode_to_sku` | SKU por código/lote | `ProductService.barcode_to_sku` (NUEVO) + `ProductRepository.get_by_lot_code` (NUEVO) |
| `ui_tabs_common.py:15-49` `_deletable_skus` | SKUs sin movimientos ni lotes | `ProductRepository.deletable_skus` (NUEVO, cuerpo tal cual) |
| `ui_tabs_common.py:129-141` | SKUs con historial en una ubicación | `InventoryRepository.skus_with_location_history` (NUEVO) |
| `ui_transfers.py:33-55` `_resolve_transfer_sku` | SKU por SKU o nombre | `ProductService.resolve_sku` (NUEVO, REQ-6) |
| `ui_sales.py:142-172` `_resolve_sale_sku` | SKU por SKU, nombre o código de barras | `ProductService.resolve_sku(…, barcode_fallback=True)` |
| `ui_transfers.py:105,206,424,478,708`, `ui_sales.py:404`, `ui_profit_items.py:146,220,258,305,500` | `db.get(InventoryMovement, id)` | `InventoryRepository.get_movement` (NUEVO, REQ-7) |
| `ui_transfers.py:110,479`, `ui_sales.py:409`, `ui_profit_items.py:151,225,310,505` | `db.get(Product, mv.product_id)` | `PurchaseRepository.get_product` (existe, REQ-10) |
| `ui_profit_items.py:506` | lote del movimiento | `PurchaseRepository.get_lot_by_movement` (existe, REQ-10) |
| `ui_transfers.py:721-761` | salidas y líneas de un traspaso por `ref` | `InventoryRepository.transfer_out_by_ref`, `transfer_items_by_ref` (NUEVOS) |
| `ui_transfers.py:847-863` | SKUs con salidas en un rango | `InventoryRepository.skus_with_movements` (NUEVO) |
| `ui_sales.py:124-129` (`_sales_doc_context`) | 10 documentos recientes + 200 clientes | `SalesDocumentService.list_recent(limit=10)` + `customers_list()` (existen, mismo orden) |
| `ui_sales.py:757-774` | 200 documentos en rango + 200 clientes | `SalesDocumentService.list_recent(limit=200, start=…, end=…)` + `customers_list()` |
| `ui_sales_documents.py:274-279` | borra líneas, `flush`, borra documento | `SalesDocumentService.delete_document` (NUEVO) |
| `ui_sales_documents.py:373` | borra una línea | `SalesDocumentService.delete_item` (NUEVO) |
| `ui_sales_documents.py:410` | añade una línea | `SalesDocumentService.add_item` (NUEVO) |
| `ui_sales_documents.py:981-986` | añade documento, `flush`, añade líneas | `SalesDocumentService.add_document` (NUEVO) |
| `ui_users.py:151` | `db.add(new_user)` | `UserRepository.add` (existe) |
| `ui_users.py:348-358` | anula FKs de `audit_logs`/`chat_messages`/`dashboard_widgets` y borra | `UserRepository.detach_references` (NUEVO) + `UserRepository.delete` (existe) |
| `ui_customers.py:258` | `db.delete(customer)` | `CustomerRepository.delete` (existe) |
| `ui_coach.py:55-61` | `db.add(CashBaseline(...))` | `CoachService.add_cash_baseline` (NUEVO) |
| `ui_auth.py:167` | `db.get(User, id)` | `UserRepository.get` (existe) |
| `ui_auth.py:216` | `db.get(Business, bid)` | `business_repository.get_business` (existe) |

### Métodos privados del servicio usados desde routers (REQ-8)

`InventoryService._transfer_out_id_for_movement_id` (`inventory_service.py:684`) y `_transfer_codes_from_out_note`:
9 usos en `ui_transfers.py`, 7 internos en `inventory_service.py`, 2 en `tests/unit/test_inventory_service_unit.py`.

### Dependencia de capa a corregir

`extract_sku` vive en `app/routers/ui_common.py:124` y lo importan `ui_sales_documents.py:23`, `ui_transfers.py:23` y
`ui_sales.py:46` (import multilínea). `ProductService.resolve_sku` lo necesita, y un servicio no puede importar de `app/routers/`.
Se mueve a `app/services/product_service.py` (función de módulo) y se actualizan sus importadores, sin reexportarlo.

### Tests que importan auxiliares que cambian de firma

`tests/unit/test_ui_regressions.py` (`_location_name_for_code`, 4 usos), `test_ui_sales_context.py` (2),
`test_profit_items_date_filter.py` (1), `test_inventory_service_unit.py` (1 auxiliar + 2 métodos privados). Fase 3:
lista exacta con `find_referencing_symbols` de cada símbolo antes de escribir su envelope.

## 2.2 Enfoque

1. **`security.py` (REQ-1):** `_UNSET = object()` de módulo y `get_active_business_id(db, request, *, user: object =
   _UNSET)`. La primera línea pasa a ser `if user is _UNSET: user = get_current_user_from_session(db, request)`. El
   resto de la función no cambia. `require_active_business_id` y `get_active_business_code` no se tocan: siguen
   sirviendo a los routers API y a `business_repository.get_active_business_config`.
2. **`UIContext` (REQ-2):** caché perezosa por campo, todo a partir de `self.user`:
   - `active_business_id -> Optional[int]`: `get_active_business_id(self.db, self.request, user=self.user)`.
   - `business_id -> int`: `self.active_business_id` o `HTTPException(409, "Active business_id is required")` (mismo
     texto que `security.py:123`).
   - `business_code -> Optional[str]`: `business_repository.get_business_code(self.db, self.active_business_id)`
     (NUEVO, misma normalización que `security.py:199-200`; `None` si el id es `None`).
   - `config -> BusinessConfig`: `get_business_config(self.db, self.active_business_id)`. No lanza.
   El sentinel `_UNSET` de `ui_context.py` ya existe para `_user`; se reutiliza para los cuatro campos.
3. **Auxiliares (REQ-4):** cada auxiliar de la lista de REQ-4 cambia `(db, request, …)` por `(ctx, …)` y usa
   `ctx.db`, `ctx.request`, `ctx.config`, `ctx.business_code`, `ctx.user`. Los handlers les pasan su `ctx`. Las
   funciones locales `_render_error`/`_tab_error` usan el `ctx` del handler que las define.
4. **Consultas (REQ-5..10):** cada sitio de §2.1 se sustituye por la llamada destino. Los métodos nuevos llevan el
   **cuerpo actual tal cual** (mismas consultas, mismo orden, mismos límites); solo cambia dónde viven.
5. **Guardas:** `tests/unit/test_routers_no_orm.py` (REQ-5, REQ-4) y `tests/unit/test_ui_query_count.py` (REQ-3).

### Invariantes que el diseño preserva

- **Orden de errores:** `ctx.config`/`ctx.business_code`/`ctx.active_business_id` no lanzan; el 409 solo sale de
  `ctx.business_id`. Un handler que hoy lee la config antes de comprobar el rol sigue dando 403 antes que 409.
- **Efectos de sesión** de `get_active_business_id` (`pop` del operador, escritura del admin): ocurren una vez, en
  la primera lectura de `ctx.active_business_id`, en vez de 1-4 veces. Son idempotentes, así que el resultado es el
  mismo.
- **Frontera transaccional:** ningún método nuevo hace `commit()`. Los que hoy hacen `flush()` en el router
  (`ui_sales_documents.py:277,982`, `_reconcile_locations:145`) lo hacen dentro del método destino, en el mismo punto.
  Los `commit`/`rollback` siguen en el router.
- **Aislamiento por negocio:** todo método nuevo de repositorio o servicio con constructor `business_id` filtra por
  él. Las funciones de `business_repository` reciben `business_id` explícito.
- **Mensajes y códigos HTTP:** idénticos. Los `HTTPException` siguen en el router salvo en `resolve_sku` y
  `barcode_to_sku`, que ya los lanzaban desde una función compartida.

## 2.3 Componentes

### `app/security.py` (MODIFY)

```python
_UNSET = object()

def get_active_business_id(db: Session, request: Request, *, user: object = _UNSET) -> Optional[int]:
    if user is _UNSET:
        user = get_current_user_from_session(db, request)
    if user is None:
        return None
    ...  # resto sin cambios
```

### `app/routers/ui_context.py` (MODIFY)

Campos `_active_bid`, `_business_code`, `_config` con `default=_UNSET`. Propiedades `active_business_id`,
`business_id`, `business_code` y `config` según §2.2. `business_id` deja de importar `require_active_business_id`.

### `app/repositories/business_repository.py` (MODIFY)

| Función nueva | Firma | Notas |
|---|---|---|
| `get_business_code` | `(db, business_id: Optional[int]) -> Optional[str]` | `None` si id `None`; normalización de `security.py:199-200`. |
| `list_businesses_by_ids` | `(db, ids: list[int]) -> list[Business]` | `where(Business.id.in_(ids)).order_by(Business.code.asc())`; `[]` si `ids` vacío. |
| `location_has_data` | `(db, location_id: int) -> bool` | Cuerpo de `_location_has_data`, con su `try/except` + `_log.exception` por tabla. `business_repository.py` no tiene logger: añadir `_log = get_logger(__name__)` con el mismo import que `ui_businesses.py`. |
| `reconcile_locations` | `(db, business_id: int, central: tuple[str, str], pos: list[tuple[str, str]], default_pos_code: str) -> Optional[str]` | El router hace el parseo (`_parse_spec`) y el prefijo (`_prefix_location_code`) y pasa códigos finales. Cuerpo y mensaje de error idénticos; `flush()` al final. |
| `create_locations` | `(db, business_id: int, central: tuple[str, str], pos: list[tuple[str, str]], default_pos_code: str) -> None` | Bucle de `ui_businesses.py:452-472`. |
| `delete_locations` | `(db, business_id: int) -> None` | `delete(Location).where(Location.business_id == business_id)` con `synchronize_session=False`. |

### `app/repositories/audit_repository.py` (CREATE)

`list_auth_events(db, *, username: str, start: Optional[datetime], end: Optional[datetime], limit: int = 2000) ->
list[AuditLog]`, con la consulta de `ui_activity.py:46-59` tal cual (`username` vacío = sin filtro). Consumidor:
`ui_activity.py`. Primer y único fichero nuevo de `repositories/`.

### `app/repositories/product_repository.py` (MODIFY)

| Método nuevo | Notas |
|---|---|
| `first_by_name(value: str) -> Optional[Product]` | `Product.name.ilike(value)`, `order_by(Product.id.asc())`, del negocio. |
| `list_categories() -> list[str]` | Consulta + limpieza `[str(c or "").strip() … if …]` de `ui_products.py:679-685`. Los dos routers conservan su `try/except` + `_log.error(...)`. |
| `get_by_lot_code(code: str) -> Optional[Product]` | Lote del negocio por `lot_code == code` y, si no y `code` no acaba en `"00000"`, por `code + "00000"`. Después, el producto de ese lote si es del negocio. Lógica de `ui_common.py:234-248`. |
| `deletable_skus(skus: list[str]) -> set[str]` | Cuerpo de `_deletable_skus`. |

### `app/repositories/inventory_repository.py` (MODIFY)

| Método nuevo | Notas |
|---|---|
| `get_movement(movement_id: int, types: tuple[str, ...]) -> Optional[InventoryMovement]` | `db.get`; `None` si no existe, si `business_id` no coincide o si `type not in types`. Cada handler conserva su `raise HTTPException(404, "<su detail>")`. |
| `skus_with_location_history(location_id: int, skus: list[str]) -> set[str]` | Consulta de `ui_tabs_common.py:129-141`. |
| `transfer_out_by_ref(ref: str) -> list[InventoryMovement]` | Consulta de `ui_transfers.py:721-731` con filtro de negocio **en SQL**. Hoy se filtra en Python tras la consulta; el resultado y el orden son los mismos. |
| `transfer_items_by_ref(ref: str) -> list[tuple[str, str, float, float]]` | Consulta de `ui_transfers.py:744-761`. |
| `skus_with_movements(movement_type: str, start: Optional[datetime], end: Optional[datetime]) -> set[str]` | Consulta de `ui_transfers.py:847-863`. |

`InventoryRepository` exige `business_id` en el constructor (`inventory_repository.py:60-63`).

### `app/services/product_service.py` (MODIFY)

- `extract_sku(product_field: str) -> str`: función de módulo movida desde `ui_common.py:124`, cuerpo tal cual.
- `ProductService.barcode_to_sku(barcode: str) -> str`: lógica de `ui_common.py:175-249`. Usa `self` como negocio:
  `business_id` deja de ser opcional (REQ-9). Mismos 422/404 y textos.
- `ProductService.resolve_sku(value: str, *, barcode_fallback: bool = False) -> str`:
  1. Valor vacío → 422 `"product is required"`.
  2. Candidatos `extract_sku(value)`, `value` → `ProductRepository.get_by_sku`.
  3. Después `first_by_name(value)`.
  4. Si `barcode_fallback`, `self.barcode_to_sku(value)`, ignorando su `HTTPException`.
  5. Si nada encaja, 404 con `"Producto no encontrado. Usa SKU, nombre exacto o código de barras válido."` (con
     `barcode_fallback`) o `"Producto no encontrado"` (sin él).
  Comprobar en Fase 3 que `ProductService` expone su `ProductRepository` (o crearlo con `self.db`/`self.business_id`).

### `app/services/inventory_service.py` (MODIFY, REQ-8)

Renombrar `_transfer_out_id_for_movement_id` → `transfer_out_id_for_movement_id` y `_transfer_codes_from_out_note` →
`transfer_codes_from_out_note` con `rename_symbol` de Serena. Ese renombrado actualiza los 7 usos internos, los 9 de
`ui_transfers.py` y los 2 de tests.

### `app/services/sales_document_service.py` (MODIFY)

| Método nuevo | Notas |
|---|---|
| `delete_document(doc: SalesDocument) -> None` | Borra `get_items(doc.id)`, `flush()`, borra `doc`. Lleva el comentario de la FK sin `relationship` (`ui_sales_documents.py:276-277`). |
| `delete_item(item: SalesDocumentItem) -> None` | `self._db.delete(item)`. |
| `add_item(item: SalesDocumentItem) -> None` | `self._db.add(item)`. |
| `add_document(doc: SalesDocument, items: list[SalesDocumentItem]) -> None` | `add(doc)`, `flush()`, `it.document_id = doc.id` + `add(it)` por línea. |

### `app/repositories/user_repository.py` (MODIFY)

`detach_references(user_id: int) -> None`: los 3 `update(... = None)` de `ui_users.py:348-357`
(`synchronize_session=False`). El router queda con `set_user_businesses(id, [])`, `detach_references(id)`,
`delete(user)`, `commit()`, en ese orden.

### `app/services/coach_service.py` (MODIFY)

`add_cash_baseline(baseline_date: date, amount: float, created_by: Optional[int]) -> CashBaseline`: crea y añade la
fila sin `commit()`. El router sigue haciendo `db.commit()`.

### Guardas (CREATE)

- `tests/unit/test_routers_no_orm.py` (REQ-5, REQ-4): por cada `app/routers/ui_*.py`, lista `fichero:línea` de:
  - las regex `\bselect\(`, `\bdb\.(get|scalar|scalars|execute|query|add|delete)\(`;
  - `from sqlalchemy import` con `select` o `func`;
  - fuera de `ui_context.py` y `ui_auth.py`, las llamadas a `get_active_business_config(`,
    `get_active_business_code(`, `get_active_business_id(`, `require_active_business_id(` y
    `get_current_user_from_session(`.
  Se ignoran las líneas de comentario.
- `tests/unit/test_ui_query_count.py` (REQ-3): fixture con `event.listen(engine, "before_cursor_execute", …)` que
  cuenta las sentencias con `FROM users` y `FROM user_businesses` (regex sobre `statement`, sin distinguir
  mayúsculas). Recorre la tabla de REQ-3 con el `TestClient` y los usuarios sembrados de los tests existentes. Antes
  de escribirlo, Fase 3 debe comprobar qué fixture de `tests/conftest.py` da `TestClient` + login por rol.
- `tests/unit/test_ui_context.py` (MODIFY, existe): casos de REQ-2 (config sin negocio, 409 solo en `business_id`, una sola
  llamada a `get_current_user_from_session` aunque se lean las 5 propiedades).
- `tests/unit/test_security.py` (MODIFY, existe): casos de REQ-1 para los 3 roles.
- Unit de cada método nuevo de repositorio/servicio, en `tests/unit/test_repositories.py`, `test_business_repository.py`,
  `test_inventory_repository_queries.py` y los tests de servicio existentes. Al menos un caso de aislamiento por negocio en cada método que filtra por negocio.

## 2.4 Contratos de API

Sin cambios: mismas rutas, códigos, `detail` y HTML.

## 2.5 Seguridad

| Comprobación | Respuesta |
|---|---|
| Frontera de auth nueva | No. `get_active_business_id` gana `user=`; con el mismo usuario que resolvería él mismo, devuelve lo mismo. Riesgo: pasar un usuario que no es el de la sesión. Mitigación: solo `UIContext` lo pasa, y siempre `self.user`. La guarda de REQ-4 impide que los routers lo llamen. |
| IDOR | `get_movement` centraliza la comprobación de negocio de 11 sitios. `barcode_to_sku` deja de admitir negocio `None` (hoy busca en todos). `transfer_out_by_ref` filtra por negocio en SQL. Cada método nuevo lleva su test de aislamiento. |
| Datos sensibles | Ninguno nuevo. |
| Validación de entrada | Sin cambios (los parseos se quedan en los routers). |
| OWASP | A01 (control de acceso) mejora por centralización; ningún punto empeora. |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Alcance del cambio en `security.py` | Solo `get_active_business_id` gana `user=` | También `require_active_business_id` y `get_active_business_code` | `UIContext` deriva el 409 y el código del id cacheado; tocar más funciones del invariante no ahorra ninguna consulta. |
| Código de negocio en `UIContext` | `business_repository.get_business_code(db, id)` | `get_active_business_code(db, request)` | Esta volvería a resolver el negocio (una consulta más a `user_businesses` para owners). |
| Dueño de la transacción | `commit` en el router, como hoy | Mover `commit` a servicios | Cambia la semántica transaccional de ~70 handlers; fuera de una release de equivalencia. |
| Lógica de reconciliar ubicaciones | Parseo en router, BD en repositorio | Todo al repositorio | El parseo de specs de formulario es capa UI; `_prefix_location_code` lo usan tests desde el router. |
| `PurchaseRepository.get_product` (sin filtro de negocio) | Reutilizar | `ProductRepository.get_by_id` con filtro | El `product_id` sale de un movimiento ya validado por `get_movement`; un método nuevo duplicaría uno existente (REQ-10). |
| `extract_sku` | A `product_service.py` | Dejarlo en `ui_common.py` | El servicio importaría de la capa de routers (dirección prohibida, dimensión 3). |
| Guarda ORM | Regex por línea | AST | Mismo patrón que `test_routers_no_html.py`; las llamadas ORM del proyecto siempre tienen la forma `db.x(`. |

## 2.7 Migración y rollback

Sin cambios de BD ni de configuración. Rollback: `git revert` del merge.

## 2.8 Contexto externo (Vía 1)

No hace falta. Todo lo que necesitan los envelopes está en el repo. La API de eventos de SQLAlchemy
(`event.listen(engine, "before_cursor_execute", fn)`, firma `fn(conn, cursor, statement, parameters, context,
executemany)`) es estable en 2.0.36, la versión del proyecto.

## 2.9 Pre-mortem contra las 10 dimensiones

| Dimensión | Riesgo | Mitigación (texto literal para los envelopes) |
|---|---|---|
| **1. SoC/SRP** | Métodos de repositorio creados sin consumidor, o métodos existentes que siguen sin usarse. | Cada método nuevo de §2.3 lo crea el **mismo envelope** que cambia su router consumidor. REQ-10 conecta los existentes. Lote final: `search_graph` de grado de entrada 0 sobre los métodos nuevos. |
| **2. Cohesión/Acoplamiento** | Un handler sigue leyendo config/código varias veces por caminos distintos. | Todo pasa por `ctx` (REQ-4), que cachea. La guarda de REQ-4 lo hace verificable. |
| **3. Capas** | Un servicio importa de `app/routers/` (`extract_sku`), o un repositorio lanza `HTTPException`. | `extract_sku` se mueve (§2.1). Los métodos nuevos de **repositorio** devuelven `None`/`[]`/`set()` y nunca lanzan `HTTPException`. Solo `ProductService.resolve_sku`/`barcode_to_sku` lanzan, como hoy. |
| **4. Seguridad** | `get_movement` olvida comprobar el tipo y un handler acepta otro tipo de movimiento. | `types` es obligatorio (sin default). Test: tipo distinto → `None`. |
| **5. Robustez** | Mover un `try/except` cambia qué se captura. | Cada sitio conserva sus `try/except` y `_log.*` **en el router**, envolviendo la llamada nueva, salvo `location_has_data`, cuyo `try` por tabla viaja con el cuerpo. Los métodos nuevos no añaden `try/except` propios: la convención de `repositories/` es propagar. Ningún envelope puede añadir ni quitar un `except`. |
| **6. Transaccional / caché** | `ctx.config` cacheado queda obsoleto si el handler modifica el negocio y luego vuelve a leer la config. | Verificado en `bf38a2b`: `ui_businesses.py` (único router que escribe `Business`/`Location`) no llama a `get_active_business_config` ni a `get_active_business_code`. `ui_auth.py` (cambio de negocio activo) no usa `UIContext`. Fase 3: repetir este `grep` y añadir la regla a `ui_context.py` como comentario de una línea: "valores de la petición: no releer tras modificar el negocio". |
| **7. Observabilidad** | Se pierden logs al mover cuerpos. | Ver dimensión 5: los `_log.*` se quedan donde están o viajan con su cuerpo. Lote final: comparar el `grep -c "_log\."` de `app/` antes y después; debe ser el mismo. |
| **8. Configuración** | No aplica. | — |
| **9. Pruebas** | Tests que llaman auxiliares con la firma `(db, request)` vieja; un test de recuento que pasa porque no ejecuta el handler. | §2.1 lista los tests afectados; se actualizan en el envelope del auxiliar. `test_ui_query_count.py` también exige `status_code == 200` y que el contador de `users` sea ≥ 1, para que no pase en vacío. Lote 0 mide la cifra antes del cambio. |
| **10. Escalabilidad** | No aplica (mismas consultas). | — |

## 2.10 Plan de lotes para la Fase 3

| Lote | Contenido | Cierre |
|---|---|---|
| **A (Lote 0)** | `test_ui_query_count.py` en modo medida: registra recuentos sin afirmar el máximo, falla si ningún handler devuelve 200. Después, REQ-1 (`security.py` + tests) y REQ-2 (`UIContext` + `test_ui_context.py`). Al cerrar el lote, el test pasa a afirmar `users == 1` en las rutas de REQ-3 que ya lo cumplen. | unit verdes; `ui_snapshot.py check` OK. |
| **B** | REQ-4: auxiliares a `ctx` y las 50 llamadas a `ctx.config`/`ctx.business_code`, un envelope por router (≤7 por lote; los de >8 sustituciones, en dos). | Tras cada envelope: unit + `check`. Al final: `test_ui_query_count.py` afirma todo REQ-3. |
| **C** | `extract_sku` y `barcode_to_sku` a `product_service.py`; `resolve_sku`; métodos de `ProductRepository`; routers `ui_products`, `ui_stock`, `ui_sales` (resolución), `ui_transfers` (resolución), `ui_tabs_common`. | unit + `check`. |
| **D** | `InventoryRepository` (`get_movement`, `transfer_*`, `skus_*`) + REQ-8 (renombrado) + routers `ui_transfers`, `ui_sales`, `ui_profit_items`. | unit + `check` + e2e de traspasos y Utilidades por artículo. |
| **E** | Escrituras: `business_repository` (ubicaciones), `SalesDocumentService`, `UserRepository.detach_references`, `CoachService.add_cash_baseline`, `audit_repository`, `ui_home`, `ui_auth`, `ui_customers`, `ui_users`, `ui_coach`, `ui_activity`. | unit + `check` + e2e de negocios, usuarios, documentos y coach. |
| **F** | `test_routers_no_orm.py`; imports sin uso; invariante 6 y docs (REQ-12); memoria de Serena `conventions`; `search_graph` de métodos sin uso; recuento de `_log.`. | Suite completa: `pytest tests/unit`, `ui_snapshot.py check`, `pytest tests/e2e -m e2e`, `style_snapshot.py check` (0 diferencias). |

Verificación global de cierre: la de REQ-11. Además, `test_ui_query_count.py` verde y `git grep -n "db\.\(get\|scalar\|scalars\|execute\|query\|add\|delete\)(" app/routers/ui_*.py` vacío.
