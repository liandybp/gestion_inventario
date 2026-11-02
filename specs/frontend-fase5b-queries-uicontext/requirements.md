# Requirements — Frontend fase 5b: consultas fuera de los routers UI y `UIContext` completo

**Release:** 0.5.0 (agrupado, sin bump por commit). **Rama base:** `develop` tras el merge de la fase 5a
(`specs/frontend-fase5a-features-templates/`). Todas las rutas de plantilla de este documento son las de después
de 5a (`features/<dominio>/…`).
**Insumo:** `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §3 ("Routers UI con queries directas a
BD … deberían construir el contexto vía `services/`") y §4 fila "5". Deuda que la fase 4 dejó para aquí:
`specs/frontend-fase4-oob-uicontext/design.md` §2.6 (auxiliares `(db, request)`) y
`docs/auditorias/2026-11-01-frontend-fase4-oob-uicontext.md` §5.3 (`_render_profit_items_tab`, `_render_error`,
`_tab_error`, `_sales_doc_context`).
**Decisiones del usuario (2026-11-02):**
- Se autoriza tocar `app/security.py`, invariante del proyecto, con un parámetro opcional `user`. Sin ese parámetro,
  el comportamiento es idéntico.
- Se mueven lecturas **y** escrituras.
- Los `commit`/`rollback` se quedan donde están.
**Principio rector de la release:** el HTML que ve el usuario no cambia (arnés `tests/snapshots/ui_snapshot.py`
contra la baseline del cierre de 5a, sin regenerarla).

## 1. Requisitos funcionales

### REQ-1: `app/security.py` acepta el usuario ya resuelto
**Priority:** Must Have
WHEN se llama a `get_active_business_id` con el argumento de palabra clave `user`, THE SYSTEM SHALL usar ese usuario
y no llamar a `get_current_user_from_session`. WHEN no se pasa `user`, THE SYSTEM SHALL comportarse exactamente como en
`bf38a2b`. `user=None` significa "no hay usuario en sesión" (devuelve `None`), distinto de no pasar el argumento. Solo
cambia esta función: `require_active_business_id`, `get_active_business_code` y los `require_*_api` no se tocan.

```gherkin
Scenario: con usuario pasado no se consulta la tabla users
  GIVEN un owner "o1" con negocios asignados [2, 3] y active_business_id=3 en sesión
  WHEN se llama a get_active_business_id(db, request, user=o1)
  THEN devuelve 3
  AND no se ejecuta ninguna sentencia SELECT sobre "users"

Scenario: sin el argumento nada cambia
  GIVEN el mismo owner en sesión
  WHEN se llama a get_active_business_id(db, request)
  THEN devuelve 3 y consulta "users" una vez, como en bf38a2b

Scenario: user=None equivale a sesión sin usuario
  GIVEN una petición con "username" en sesión
  WHEN se llama a get_active_business_id(db, request, user=None)
  THEN devuelve None sin consultar "users"
```

### REQ-2: `UIContext` resuelve una sola vez usuario, negocio, código y configuración
**Priority:** Must Have
WHEN un handler UI lee `ctx.user`, `ctx.active_business_id`, `ctx.business_id`, `ctx.business_code` o `ctx.config`,
THE SYSTEM SHALL resolver cada valor como mucho una vez por petición, reutilizando el usuario de `ctx.user`.
`ctx.active_business_id` (nuevo, `Optional[int]`) no lanza; `ctx.business_id` conserva el 409 actual;
`ctx.business_code` (nuevo) equivale a `get_active_business_code(db, request)`, y `ctx.config` (nuevo) a
`get_active_business_config(db, request)`. Ninguno de los dos lanza, porque las funciones originales tampoco lanzan.

```gherkin
Scenario: ctx.config no adelanta el 409
  GIVEN un admin sin negocios en BD
  WHEN un handler lee ctx.config antes de ctx.business_id
  THEN ctx.config devuelve BusinessConfig() por defecto
  AND el 409 solo aparece al leer ctx.business_id

Scenario: el orden de errores de cada handler no cambia
  GIVEN un operador sin negocio asignado
  WHEN pide un endpoint que hoy comprueba el rol antes que el negocio
  THEN recibe el mismo código (403 o 409) que en bf38a2b
```

### REQ-3: Una sola consulta a `users` por petición UI
**Priority:** Must Have
WHEN un usuario autenticado pide una pestaña o fragmento UI, THE SYSTEM SHALL ejecutar como mucho **una** sentencia
SELECT sobre la tabla `users`. Hoy son 4 en `tab_sales` (`ui_sales.py:690`): `ctx.business_id`, `ctx.user`,
`get_active_business_code` y `get_active_business_config`. Para un owner, además, como mucho **una** sobre
`user_businesses` (hoy 3 en el mismo handler). Excepción: `GET /ui/dashboard`, que lista los negocios asignados para el
selector, admite 2 sobre `user_businesses`.

```gherkin
Scenario Outline: consultas a users por petición
  GIVEN un usuario <rol> autenticado
  WHEN pide GET <ruta>
  THEN el contador de sentencias sobre "users" vale 1
  Examples:
    | rol      | ruta                  |
    | admin    | /ui/tabs/sales        |
    | owner    | /ui/tabs/transfers    |
    | operator | /ui/tabs/sales        |
    | owner    | /ui/tabs/documents    |
    | admin    | /ui/tabs/inventory    |
    | owner    | /ui/dashboard         |
```

### REQ-4: Los auxiliares de los routers reciben `ctx`, no `(db, request)`
**Priority:** Must Have
WHEN un auxiliar de `app/routers/ui_*.py` necesita usuario, negocio o configuración, THE SYSTEM SHALL recibir el
`UIContext` del handler en lugar de `(db, request)`. Afecta a: `_sale_pos_context`, `_sales_doc_context`
(`ui_sales.py`), `_purchase_config_values`, `_render_error` (`ui_purchases.py`), `_tab_error` (`ui_transfers.py`),
`_currency_symbol` (`ui_agent.py`), `_render_profit_items_tab` (`ui_profit_items.py`), `_home_locations_context` y
`_location_name_for_code` (`ui_tabs_common.py`). Las 50 llamadas `get_active_business_config(db, request)` /
`get_active_business_code(db, request)` de los routers UI pasan a `ctx.config` / `ctx.business_code`.

```gherkin
Scenario: ningún router UI resuelve el negocio por su cuenta
  WHEN se buscan "get_active_business_config(", "get_active_business_code(", "get_active_business_id(",
       "require_active_business_id(" y "get_current_user_from_session(" en app/routers/ui_*.py
  THEN solo aparecen en ui_context.py y ui_auth.py
```

### REQ-5: Ningún router UI accede a la BD con el ORM directamente
**Priority:** Must Have
WHEN termina la fase, THE SYSTEM SHALL no contener en `app/routers/ui_*.py` (incluidos `ui_common.py`,
`ui_tabs_common.py` y `ui_auth.py`) llamadas `select(`, `db.get(`, `db.scalar(`, `db.scalars(`, `db.execute(`,
`db.query(`, `db.add(` ni `db.delete(`, ni imports de `select`/`func` desde `sqlalchemy`. Siguen permitidos
`db.commit()`, `db.rollback()` y `db.flush()`. Cada consulta pasa a un repositorio o servicio según `design.md` §2.3.

```gherkin
Scenario: la guarda detecta una consulta nueva en un router
  GIVEN un handler de ui_sales.py con "db.scalar(select(Product)...)"
  WHEN corre "pytest tests/unit/test_routers_no_orm.py"
  THEN falla y lista "app/routers/ui_sales.py:<línea>"
```

### REQ-6: Las consultas duplicadas quedan en un solo sitio
**Priority:** Must Have
WHEN dos routers hacen la misma consulta, THE SYSTEM SHALL tener una única implementación:
- Categorías de producto: `ui_products.py:679` y `ui_stock.py:277`.
- SKU por SKU o nombre: `_resolve_transfer_sku` en `ui_transfers.py:33` y `_resolve_sale_sku` en `ui_sales.py:142`.
  La versión de ventas conserva su intento por código de barras y su mensaje de 404.
- Documentos recientes y clientes del negocio: `ui_sales.py:124-129` y `:757-774`. Ya existen en
  `SalesDocumentService.list_recent`/`customers_list` y se reutilizan.
- Producto por nombre: `ui_products.py:603`.

### REQ-7: La comprobación "el movimiento es de este negocio" vive en el repositorio
**Priority:** Must Have
WHEN un handler carga un `InventoryMovement` por id, THE SYSTEM SHALL obtenerlo con un método de repositorio que
devuelve `None` si no existe, si es de otro negocio o si su tipo no está en los admitidos. Hoy hay 11
`db.get(InventoryMovement, …)`: 5 en `ui_profit_items.py`, 1 en `ui_sales.py` y 5 en `ui_transfers.py`. Nueve van
seguidos de `if mv.business_id != bid` repetido a mano. Los dos de `ui_transfers.py:424,478` no lo llevan: cargan el
`out_id` que ya validó `InventoryService` y no son explotables, pero quedan cubiertos igual. El handler sigue
respondiendo el mismo 404 con el mismo `detail`.

```gherkin
Scenario: un movimiento de otro negocio da 404
  GIVEN una venta id=10 del negocio 2 y un admin con negocio activo 1
  WHEN pide GET /ui/movement/sale/10/edit
  THEN la respuesta es 404 "Sale movement not found"
```

### REQ-8: Los routers no llaman a métodos privados de los servicios
**Priority:** Must Have
WHEN `ui_transfers.py` necesita el id de salida de un traspaso o sus códigos de ubicación, THE SYSTEM SHALL llamar a
métodos públicos de `InventoryService`. Hoy llama 5 veces a `service._transfer_out_id_for_movement_id` y 3 a
`service._transfer_codes_from_out_note`. Se renombran a públicos (sin el `_`) y se actualizan todos sus llamadores,
también los internos del servicio, sin alias.

### REQ-9: `barcode_to_sku` exige el negocio
**Priority:** Should Have
WHEN se resuelve un código de barras, THE SYSTEM SHALL exigir `business_id`. Hoy es `Optional` y con `None` busca en
todos los negocios (`ui_common.py:175`). Los 3 llamadores (`ui_products.py:611`, `ui_sales.py:166`, `:299`) ya lo
pasan.

### REQ-10: Los métodos de repositorio que ya existen se usan
**Priority:** Should Have
WHEN un router borra o añade un usuario o un cliente, THE SYSTEM SHALL usar `UserRepository.add/delete` y
`CustomerRepository.delete`, que existen sin llamadores desde los routers (`ui_users.py:151,358`,
`ui_customers.py:258`). Para las lecturas de movimiento y lote se usa `PurchaseRepository.get_product`/
`get_lot_by_movement` o su sustituto de §2.3, no un método nuevo con la misma función.

### REQ-11: Equivalencia completa
**Priority:** Must Have
WHEN termina la fase, THE SYSTEM SHALL pasar `ui_snapshot.py check` contra la baseline del cierre de 5a (sin
regenerarla), `pytest tests/unit`, `pytest tests/e2e -m e2e` y `style_snapshot.py check` con 0 diferencias.

### REQ-12: Documentación y memorias
**Priority:** Should Have
WHEN termina la fase, THE SYSTEM SHALL tener actualizados los dos `CLAUDE.md`. En el de `.claude/`, el invariante 6 pasa
a decir "`app/security.py`: `get_active_business_id` acepta `user=` opcional; no cambiar su lógica ni la de los `require_*`". También: la fila
"5" del diagnóstico (✅), el `CHANGELOG.md` de 0.5.0 y la memoria de Serena `conventions` ("los routers UI no usan el
ORM; la guarda es `test_routers_no_orm.py`").

### REQ-13: Partir los routers de más de 900 líneas
**Priority:** Won't Have (esta iteración). Diagnóstico §3; es reorganización, no equivalencia de datos.

## 2. Requisitos no funcionales

| Categoría | Respuesta |
|---|---|
| **Performance** | Mejora medible: de 4 a 1 SELECT sobre `users` por petición (REQ-3) y, en owners, de 3 a 1 sobre `user_businesses`. El test de REQ-3 es la medida; Lote 0 registra la cifra de antes para el informe. |
| **Security** | `get_active_business_id` gana un parámetro opcional (REQ-1), sin cambiar lógica: el test de REQ-1 recorre los 3 roles con y sin `user=`. Se cierra el `business_id` opcional de `barcode_to_sku` (REQ-9). La comprobación de negocio de los movimientos pasa a un único sitio (REQ-7): menos lugares donde olvidarla. Sin fronteras nuevas. |
| **Scalability** | No aplica: mismas consultas, mismo volumen. |
| **Observability** | Se conservan los `_log.error(...)`/`_log.exception(...)` de los `try/except` de cada sitio movido, en el router o en el método destino según `design.md` §2.9. Sin logs nuevos. |
| **Reliability** | Arnés + e2e + guarda `test_routers_no_orm.py`. Los `commit` no se mueven: la frontera transaccional de cada handler es la misma. |

## 3. Fuera de alcance

- Routers API JSON (`products.py`, `inventory.py`, `sync.py`) y `get_active_business_config` de `business_repository.py`:
  siguen llamando a `security.py` sin `user=`.
- Migrar `ui_auth.py` a `UIContext` (el login trabaja antes de que haya sesión). Sí entra en la guarda ORM de REQ-5.
- Mover los `commit()`/`rollback()` a servicios o unificar quién es dueño de la transacción.
- REQ-13 (partir routers grandes), SKU único compuesto, bug de fecha/FIFO de compras/traspasos/gastos.
- Cambiar mensajes, códigos de estado o HTML.

## 4. Dependencias

- Fase 5a mergeada en `develop` (rutas `features/<dominio>/`).
- Baseline del arnés y de `style_snapshot.py` escritas en el cierre de 5a.

## 5. Preguntas abiertas

Ninguna. Alcance (lecturas y escrituras), autorización de `security.py` y partición en 5a/5b decididos con el usuario
el 2026-11-02.
