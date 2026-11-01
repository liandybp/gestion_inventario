# Requirements — Frontend fase 4: plantillas OOB, `UIContext` y sidebar data-driven

**Release:** 0.5.0 (agrupado, sin bump por commit). **Rama base:** `develop` `b741a46`.
**Insumo:** `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §1 (HTML en Python), §2 (principio 5,
sidebar), §3 (`UIContext`), §4 fila "4" y nota H-303.
**Principio rector de la release:** el HTML que ve el usuario no cambia. Cada fase lo demuestra con el arnés
`tests/snapshots/ui_snapshot.py` contra una baseline generada desde el commit anterior a la fase.

## 1. Requisitos funcionales

### REQ-1: El arnés captura el dashboard y las pestañas de `operator` y `owner` (H-303.1)
**Priority:** Must Have
WHEN el arnés corre `write` o `check`, THE SYSTEM SHALL capturar, además de lo actual (admin), el dashboard y cada
pestaña visible para un usuario `operator` y para un usuario `owner` sembrados por el propio arnés.

```gherkin
Scenario: el sidebar del operador queda en la baseline
  GIVEN el arnés siembra un usuario "snap-operator" con rol operator y un "snap-owner" con rol owner
  WHEN corre "ui_snapshot.py write <dir>"
  THEN existen "<dir>/operator_dashboard.txt" y "<dir>/owner_dashboard.txt"
  AND existe un fichero "<rol>_tab_<slug>.txt" por cada pestaña visible para ese rol

Scenario: un cambio de visibilidad del sidebar hace fallar el check
  GIVEN una baseline escrita con el código actual
  WHEN se muestra "tab-coach" también al rol owner
  THEN "ui_snapshot.py check <dir>" termina con código 1 y lista "owner_dashboard"
```

### REQ-2: El arnés captura POST con error que re-renderizan un modal o un panel (H-303.2)
**Priority:** Must Have
WHEN el arnés envía cada POST con error de la tabla, THE SYSTEM SHALL guardar el canónico de la respuesta precedido
de su código de estado (`status=<NNN>`), sin exigir 200.

| Clave | Petición | Error provocado |
|---|---|---|
| `post_purchase_qty0` | `POST /ui/purchase` | `quantity=0` |
| `post_user_duplicate` | `POST /ui/users/create` | `username` ya existente |
| `post_product_sku_other_business` | `POST /ui/product/SNAP-1/update` | `new_sku` de un producto del otro negocio |
| `post_transfer_over_stock` | `POST /ui/transfers` | cantidad mayor que el stock en origen |
| `post_must_change_password_mismatch` | `POST /ui/must-change-password` | `confirm_password` distinta |

```gherkin
Scenario: cambiar el texto de un error hace fallar el check
  GIVEN una baseline con "post_purchase_qty0"
  WHEN cambia el mensaje de error de la compra con cantidad 0
  THEN el check falla y lista "post_purchase_qty0"
```

### REQ-3: El arnés captura los fragmentos OOB y las listas que hoy se arman en Python (H-303.3)
**Priority:** Must Have
WHEN el arnés pide cada fragmento, THE SYSTEM SHALL guardar su canónico con el código de estado, también si la
respuesta tiene menos de 4 líneas.

| Clave | Petición |
|---|---|
| `frag_defaults_purchase` | `GET /ui/product-defaults/purchase?product=SNAP-1` |
| `frag_defaults_sale` | `GET /ui/product-defaults/sale?product=SNAP-1` |
| `frag_salesdoc_product` | `GET /ui/sales-doc/product-defaults?product=SNAP-1` |
| `frag_salesdoc_customer` | `GET /ui/sales-doc/customer-defaults?client_name=<nombre del cliente sembrado>` |
| `frag_sale_options` | `GET /ui/sale/product-options?location_code=<POS sembrado>` |
| `frag_transfer_options` | `GET /ui/transfers/product-options?from_location_code=<central>` |
| `frag_lots_empty` / `frag_lots_none` / `frag_lots_ok` | `GET /ui/return-supplier/lots` sin SKU, con SKU sin lotes y con `SNAP-1` |
| `post_purchase_new_product` | `POST /ui/purchase` de un producto nuevo (respuesta con `#product-panel` OOB) |
| `post_sale_ok` | `POST /ui/sale` válida |

Los nombres exactos de los parámetros de query y de formulario se toman de la firma real de cada endpoint (ver
`design.md` §2.1); la tabla fija qué caso se captura, no el nombre del parámetro.

```gherkin
Scenario: quitar un hx-swap-oob hace fallar el check
  GIVEN una baseline con "frag_defaults_sale"
  WHEN la respuesta de /ui/product-defaults/sale pierde el atributo hx-swap-oob del input de precio
  THEN el check falla y lista "frag_defaults_sale"
```

### REQ-4: El arnés captura los estados vacíos (H-303.4)
**Priority:** Must Have
WHEN el arnés cambia al segundo negocio sembrado sin datos, THE SYSTEM SHALL capturar cada pestaña de admin con la
clave `empty_tab_<slug>`.

### REQ-5: La baseline de la fase se genera desde el commit anterior y el criterio de cierre se prueba (H-303.6)
**Priority:** Must Have
WHEN se cierra el lote del arnés, THE SYSTEM SHALL tener en `tests/snapshots/baseline/` la baseline generada desde
`b741a46` (código sin migrar) en un worktree, y un test de mutación documentado que muestre que quitar un
`hx-swap-oob` y cambiar un texto de error hacen fallar el `check`.

### REQ-6: Los fragmentos OOB y las listas se renderizan con plantillas, sin HTML en Python
**Priority:** Must Have
WHEN un endpoint de la tabla de REQ-3 (salvo los POST) responde con marcado, THE SYSTEM SHALL renderizarlo con una
plantilla Jinja2, y el marcado compartido con su panel (`label`/`input` con id) SHALL salir de una sola macro usada
por el panel y por el fragmento.

```gherkin
Scenario: el precio de venta se rellena igual que antes
  GIVEN un producto "SNAP-1" con default_sale_price 2.0 y unidad "ud"
  WHEN el navegador pide /ui/product-defaults/sale?product=SNAP-1
  THEN la respuesta tiene un label#sale-qty-label "Cantidad (ud)" y un input#sale-unit-price con value "2.0", ambos con hx-swap-oob="true"
  AND el canónico es idéntico al de la baseline

Scenario: no queda marcado en los routers
  WHEN corre el unit test de guarda de routers
  THEN ningún fichero de app/routers/*.py contiene "hx-swap-oob" ni literales "<option", "<input", "<select" o "<label"
```

### REQ-7: Dependencia `UIContext` en los handlers UI
**Priority:** Must Have
WHEN un handler de `app/routers/ui_*.py` (salvo `ui_auth.py`) necesita el usuario, el negocio activo o un guardia de
rol, THE SYSTEM SHALL obtenerlos de un `UIContext` creado con `ctx = ui_context(request, db)` al inicio del cuerpo
(llamada directa, no `Depends`: enmienda `5cb168e`), que resuelve cada dato una
sola vez por petición y bajo demanda, con las mismas excepciones y en el mismo orden que hoy.

```gherkin
Scenario: sin negocio activo, el error sigue siendo 409
  GIVEN un usuario owner sin negocios asignados
  WHEN pide /ui/tabs/expenses
  THEN la respuesta es 409 con detail "Active business_id is required"

Scenario: el orden de los guardias no cambia
  GIVEN un operador sin negocio activo
  WHEN envía POST /ui/expenses/create (que comprueba el rol antes del negocio)
  THEN la respuesta es 403, igual que antes de la migración

Scenario: cada dato se resuelve una vez a través del contexto
  GIVEN un UIContext de una petición
  WHEN se lee ctx.user dos veces y ctx.business_id dos veces
  THEN UIContext llama una vez a get_current_user_from_session y una vez a require_active_business_id
  (require_active_business_id sigue consultando el usuario por dentro: app/security.py no se modifica)
```

### REQ-8: Sidebar data-driven
**Priority:** Must Have
WHEN se renderiza el dashboard, THE SYSTEM SHALL construir los botones del sidebar desde una lista `NAV_ITEMS` en
Python (slug, etiqueta, roles, modos) filtrada por rol y modo, con el mismo HTML que hoy para cada combinación.

```gherkin
Scenario Outline: botones visibles por rol y modo
  GIVEN un usuario con rol <rol> y APP_MODE <modo>
  WHEN se renderiza el dashboard
  THEN los ids de botón del sidebar son exactamente <ids>, en ese orden

  Examples:
    | rol      | modo    | ids |
    | admin    | server  | home agent inventory purchases transfers coach sales documents customers expenses profit-items profit dividends history users businesses activity |
    | owner    | server  | home agent inventory purchases transfers sales documents customers expenses profit-items profit dividends history |
    | operator | server  | sales documents customers |
    | admin    | desktop | home inventory coach sales documents customers expenses profit-items profit dividends history users businesses activity |
    | owner    | desktop | home inventory sales documents customers expenses profit-items profit dividends history |
    | operator | desktop | sales documents customers |
```
(Cada id lleva el prefijo `tab-`. `aria-selected="true"` solo en `tab-home` para admin/owner y en `tab-sales` para
operator; el resto `false`.)

> **Enmienda 2026-11-01 (decisión del usuario tras la auditoría):** el operador ve también Documentos y Clientes
> (crea/edita clientes sin borrarlos; crea/edita/borra documentos de venta). La tabla de arriba ya lo refleja.

### REQ-9: e2e de los flujos afectados
**Priority:** Must Have
WHEN corre la suite e2e, THE SYSTEM SHALL cubrir en navegador: (a) sidebar por rol (admin, owner, operator) con los
ids de REQ-8 en modo server; (b) autorrelleno OOB de coste en Compras, precio en Ventas, precio y cliente en
Documentos; (c) lista de lotes de devolución a proveedor en Inventario. Los e2e se escriben primero y pasan con el
código sin migrar (caracterización).

### REQ-10: Guarda contra HTML nuevo en routers
**Priority:** Should Have
THE SYSTEM SHALL incluir el unit test de REQ-6 ("no queda marcado en los routers") para que ninguna fase posterior
reintroduzca HTML en Python.

### REQ-11: Documentación viva
**Priority:** Should Have
WHEN se cierra la fase, THE SYSTEM SHALL tener actualizados `CHANGELOG.md` (sección 0.5.0), `CLAUDE.md`,
`.claude/CLAUDE.md` y el diagnóstico (§4 fila 4 marcada ✅).

### REQ-12: Divergencias heredadas del HTML en Python
**Priority:** Won't Have (esta iteración)
Las diferencias actuales entre fragmento y panel se conservan tal cual (p. ej. el OOB de `sales-doc-client-id`
añade `required` que el panel no tiene). Unificarlas cambia comportamiento y queda fuera de una release de
equivalencia.

**MoSCoW:** Must 9 (REQ-1..9) · Should 2 (REQ-10, 11) · Won't 1 (REQ-12).

## 2. Requisitos no funcionales

| Categoría | Respuesta |
|---|---|
| **Performance** | Mejora: hoy un handler típico consulta el usuario 3-4 veces (cada `ensure_*`, cada `get_current_user_from_session` y la que hace `require_active_business_id` por dentro). Con `UIContext` quedan 2 (la de `ctx.user` y la interna de `require_active_business_id`). Sin objetivo numérico. |
| **Security** | Sin fronteras nuevas. `UIContext` envuelve las funciones de `app/security.py` y `ensure_*` de `ui_common.py` sin cambiar su lógica. El escape pasa de `html.escape` manual a autoescape de Jinja2 (igual o más seguro). `NAV_ITEMS` solo decide qué botones se pintan; el RBAC de cada pestaña sigue en su endpoint. |
| **Scalability** | No aplica: no cambia volumen ni consultas de datos de negocio. |
| **Observability** | No aplica: sin logs nuevos; los existentes en los handlers se mantienen. |
| **Reliability** | El arnés ampliado y los e2e de caracterización son la red: cualquier cambio de HTML, de código de estado o de orden de guardias se detecta antes del merge. |

## 3. Fuera de alcance

- Fase 5: reagrupar `partials/` en `features/<dominio>/` y sacar las consultas SQL de los routers.
- Helpers que reciben `(db, request)` y resuelven el negocio por su cuenta (`_home_locations_context`,
  `_location_name_for_code`, `_location_id_for_code`, etc.): siguen igual; se tocan en la fase 5.
- Routers API JSON (`products.py`, `inventory.py`, `sync.py`) y `ui_auth.py`.
- `GET /ui/transfers/stock/{sku}`: devuelve un número en texto plano, no marcado.
- Respuestas vacías `HTMLResponse("")`: no son marcado.
- Captura del arnés en modo `desktop` (el modo se fija al importar `settings.py`); REQ-8 lo cubre con unit test.
- Unificar las divergencias de REQ-12.

## 4. Dependencias

- Fases 1-3 en `develop` (macros `components/forms.html` y `components/modal.html`, JS en módulos, CSP estricta).
- Usuarios `operator` y `owner` en la siembra de los e2e (`tests/e2e/conftest.py`, ya existen).
- Sin migraciones de BD.

## 5. Preguntas abiertas

Ninguna bloqueante. Decisiones tomadas por el principio de equivalencia de la release (REQ-12) y por el diagnóstico
(alcance de `UIContext` a todos los handlers UI salvo `ui_auth.py`). Se procede a la Fase 2.
