# Design — Frontend fase 4: plantillas OOB, `UIContext` y sidebar data-driven

Requisitos: `requirements.md` (REQ-1..12). Rama base `develop` `b741a46`. Sin migraciones de BD.

## 2.1 Anclaje en el código (verificado 2026-11-01)

### HTML construido en Python (REQ-6)

| Endpoint | Router:línea | Qué arma | Plantilla con el mismo marcado |
|---|---|---|---|
| `GET /ui/product-defaults/purchase` | `ui_products.py:555` `product_defaults_purchase` | `label#purchase-qty-label` + `input#purchase-unit-cost`, ambos `hx-swap-oob='true'` | `partials/purchase_panel.html:44-45` |
| `GET /ui/product-defaults/sale` | `ui_products.py:578` `product_defaults_sale` | `label#sale-qty-label` + `input#sale-unit-price` OOB | `partials/sale_panel.html:26-27` |
| `GET /ui/sales-doc/product-defaults` | `ui_sales_documents.py:456` `sales_doc_product_defaults` | `input#sales-doc-unit-price` OOB | `components/sales_doc/cart_add.html:16` |
| `GET /ui/sales-doc/customer-defaults` | `ui_sales_documents.py:474` `sales_doc_customer_defaults` | `input#sales-doc-client-id` (con `required`) + `input#sales-doc-client-address` OOB | `components/sales_doc/issue_form.html:26-27` (sin `required`) |
| `GET /ui/sale/product-options` | `ui_sales.py:91` `sale_product_options` | `<option>` × 3 por producto (`SKU - nombre`, `SKU`, `nombre`) | `partials/sale_panel.html:35-40` (mismo bucle, idéntico) |
| `GET /ui/transfers/product-options` | `ui_transfers.py:641` `transfer_product_options` | `<option>` (`label`, `sku`, `name` opcional) | `partials/tab_transfers.html:84-88` (**distinto**: solo `SKU - nombre`) |
| `GET /ui/return-supplier/lots` | `ui_stock.py:342` `ui_supplier_return_lots` | `<select name='lot_id'>` en 4 variantes: sin SKU, error (`Error: <msg[:50]>`), sin lotes, lista | ninguna (solo el `hx-get` de `partials/tab_inventory.html:62`) |

Precedente a seguir: `GET /ui/product-suggest` (`ui_products.py:622`) ya responde con
`partials/product_suggest_options.html` vía `templates.TemplateResponse`.

Fuera: `HTMLResponse("")` (vacías), `GET /ui/transfers/stock/{sku}` (número en texto plano).

### Resolución de usuario/negocio (REQ-7)

- `app/security.py:17` `get_current_user_from_session(db, request) -> Optional[User]`: lee `session["username"]`; si
  el usuario no existe o está inactivo, limpia la sesión y devuelve `None`.
- `app/security.py:48` `get_active_business_id(db, request) -> Optional[int]`: llama otra vez a
  `get_current_user_from_session`; para admin puede escribir `session["active_business_id"]`.
- `app/security.py:120` `require_active_business_id(db, request) -> int`: `HTTPException(409, "Active business_id is required")`.
- `app/routers/ui_common.py:69` `ensure_admin(db, request)`: 403 `"Admin required"` si no es admin (`(role or "").lower()`).
- `app/routers/ui_common.py:75` `ensure_admin_or_owner(db, request)`: 403 `"Admin required"` si no es admin/owner
  (`(role or "").strip().lower()`).
- Escriben la sesión solo `app/routers/ui_auth.py` (login, cambio de negocio en la línea 228, logout) y
  `app/security.py`. Ningún otro router.

Llamadas por router (`get_current_user_from_session(db, request)` / `require_active_business_id(db, request)` /
`ensure_*(db, request)`), handlers con `@router`:

| Router | handlers | user | bid | ensure |
|---|---|---|---|---|
| ui_activity | 1 | 0 | 0 | 1 |
| ui_agent | 6 | 1 | 6 | 6 |
| ui_businesses | 5 | 3 | 0 | 5 |
| ui_coach | 3 | 2 | 3 | 3 |
| ui_customers | 7 | 7 | 7 | 1 |
| ui_dividends | 1 | 0 | 1 | 1 |
| ui_expenses | 5 | 3 | 5 | 4 |
| ui_extractions | 4 | 3 | 4 | 4 |
| ui_history | 2 | 2 | 2 | 2 |
| ui_home | 4 | 2 | 2 | 2 |
| ui_products | 10 | 5 | 10 | 7 |
| ui_profit_items | 11 | 7 | 11 | 11 |
| ui_profit | 1 | 1 | 1 | 1 |
| ui_purchases | 8 | 11 | 8 | 8 |
| ui_sales_documents | 13 | 7 | 13 | 1 |
| ui_sales | 8 | 13 | 9 | 3 |
| ui_stock | 7 | 4 | 7 | 7 |
| ui_sync | 3 | 3 | 0 | 1 |
| ui_transfers | 11 | 7 | 11 | 11 |
| ui_users | 7 | 4 | 0 | 7 |

Los recuentos incluyen llamadas dentro de funciones auxiliares (sin `@router`); esas no se migran (fuera de
alcance, fase 5). El orden de las llamadas varía entre handlers: `expense_create` comprueba el rol y después el
negocio; `expense_delete` resuelve el negocio primero y comprueba el rol dentro de un `try`.

### Sidebar (REQ-8)

`app/templates/dashboard.html:34-129`: 17 `<button class="sidebar-item" id="tab-<slug>" aria-selected="…"
hx-get="/ui/tabs/<slug>" hx-target="#tab-content" hx-swap="innerHTML">Etiqueta</button>`, con `{% set user_role =
(user.role or '')|lower %}` y estas condiciones:

| slug | Etiqueta | Roles | Solo server |
|---|---|---|---|
| home | Inicio | admin, owner | no |
| agent | Dashboard Dinámico | admin, owner | sí |
| inventory | Inventario | admin, owner | no |
| purchases | Compras | admin, owner | sí |
| transfers | Traspasos | admin, owner | sí |
| coach | Coach | admin | no |
| sales | Ventas | cualquiera (sin condición) | no |
| documents | Documentos | admin, owner | no |
| customers | Clientes | admin, owner | no |
| expenses | Costos operativos | admin, owner | no |
| profit-items | Utilidades por artículo | admin, owner | no |
| profit | Utilidades | admin, owner | no |
| dividends | Dividendos | admin, owner | no |
| history | Historial | admin, owner | no |
| users | Usuarios | admin | no |
| businesses | Negocios | admin | no |
| activity | Actividad | admin | no |

`aria-selected="true"` en `tab-home` (literal) y en `tab-sales` si `user_role == 'operator'`; el resto `"false"`.
`#tab-content` carga `/ui/tabs/sales` si el rol es operator y `/ui/tabs/home` en otro caso (`dashboard.html:136`).
El contexto lo arma `ui_home.py:162` `dashboard` con `app_mode = app_mode_label()`.

### Arnés (REQ-1..5)

`tests/snapshots/ui_snapshot.py`: un `TestClient`, login admin, `_seed()` (un negocio, `SNAP-1`, compra, traspaso,
venta, ajuste, cliente, gasto, extracción, documento, widget; textos hostiles con `_hostile`), `_pages()` (40
claves GET), `_get()` exige 200 y ≥ 4 líneas. El canonizador ordena atributos y normaliza espacios y comillas, así
que mover un fragmento de Python a Jinja2 no cambia el canónico, y quitar un atributo (`hx-swap-oob`) sí.
`_FrozenDatetime` congela el reloj de Inicio (`b201f64`).

Usuarios de e2e: `tests/e2e/conftest.py:86-104` siembra `operator` y `owner` (contraseña = usuario).

## 2.2 Enfoque

Cuatro bloques, en este orden, cada uno en commits propios sobre `feature/frontend-fase4`:

1. **Red primero (Lote 0).** Ampliar el arnés (H-303) y escribir la baseline **antes de tocar `app/`**, más los e2e
   de caracterización, que pasan con el código actual. Todo cambio posterior se juzga contra esa red.
2. **`UIContext`.** Módulo nuevo con tests unitarios; después la migración router a router.
3. **Fragmentos a plantillas.** Macros compartidas por panel y fragmento; los 7 fragmentos pasan a plantillas.
4. **Sidebar data-driven** y cierre (guarda de routers, limpieza, docs).

### Invariantes que el diseño preserva

- Ids, `data-*`, `aria-selected`, `hx-*` y textos visibles: canónico idéntico a la baseline en las ~120 claves.
- `app/security.py`, `app/deps.py` y `app/routers/ui_auth.py` no se modifican.
- Mismas excepciones (código y `detail`) y mismo orden de comprobación en cada handler.
- RBAC de cada pestaña en su endpoint; `NAV_ITEMS` solo decide qué botones se pintan.
- Divergencias heredadas (REQ-12) se conservan: el fragmento de cliente sigue con `required`, el panel sin él.

## 2.3 Componentes

### `app/routers/ui_context.py` (CREATE)

```python
from dataclasses import dataclass, field
from typing import Optional

from fastapi import Depends, Request
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.models import User
from app.security import get_current_user_from_session, require_active_business_id

_UNSET = object()


@dataclass
class UIContext:
    request: Request
    db: Session
    _user: object = field(default=_UNSET, repr=False)
    _business_id: Optional[int] = field(default=None, repr=False)

    @property
    def user(self) -> Optional[User]: ...          # get_current_user_from_session una vez; cachea también None
    @property
    def business_id(self) -> int: ...              # require_active_business_id una vez; cachea solo el éxito
    def ensure_admin(self) -> None: ...            # misma condición y HTTPException(403, "Admin required") que ui_common.ensure_admin
    def ensure_admin_or_owner(self) -> None: ...   # ídem ensure_admin_or_owner (con .strip())


def ui_context(request: Request, db: Session = Depends(session_dep)) -> UIContext:
    return UIContext(request=request, db=db)
```

- **Perezoso:** nada se resuelve al construirlo. Cada handler lee `ctx.user`, `ctx.business_id` o llama
  `ctx.ensure_*()` en el mismo punto donde hoy llama a la función, así el orden de errores no cambia.
- **Misma sesión de BD:** `ui_context` depende de `session_dep`; FastAPI cachea la dependencia por petición, así que
  `ctx.db` es el mismo objeto que el `db` del handler. Un unit test lo fija.
- `business_id` no cachea el fallo: si lanza 409, una segunda lectura vuelve a llamar (mismo comportamiento que hoy
  si un handler llamaba dos veces).

### Reglas de migración por handler (REQ-7)

Solo en funciones decoradas con `@router.*` de los 20 routers de la tabla. Se añade el parámetro
`ctx: UIContext = Depends(ui_context)` y se sustituye, en el mismo sitio:

| Hoy | Después |
|---|---|
| `<var> = get_current_user_from_session(db, request)` | `<var> = ctx.user` |
| `<var> = require_active_business_id(db, request)` | `<var> = ctx.business_id` |
| `ensure_admin(db, request)` | `ctx.ensure_admin()` |
| `ensure_admin_or_owner(db, request)` | `ctx.ensure_admin_or_owner()` |

- `request` y `db` se mantienen como parámetros si el cuerpo los sigue usando.
- Las funciones auxiliares sin `@router` no se tocan, aunque reciban `(db, request)`.
- Los imports que queden sin uso en cada router se eliminan en el mismo envelope.
- Al final, si `ui_common.ensure_admin`/`ensure_admin_or_owner` quedan sin referencias (comprobar con grep en `app/`
  y `tests/`), se eliminan; si alguna función auxiliar los sigue usando, se quedan.

### Macros compartidas (REQ-6) — añadir a `app/templates/components/forms.html`

```jinja
{% macro qty_label(id, uom='', oob=False) -%}
<label id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %}>{% if uom %}Cantidad ({{ uom }}){% else %}Cantidad{% endif %}</label>
{%- endmacro %}

{% macro number_input(id, name, value=none, oob=False) -%}
<input id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %} name="{{ name }}" type="number" step="0.0001" min="0"{% if value is not none %} value="{{ value }}"{% endif %} />
{%- endmacro %}

{% macro text_input(id, name, value=none, oob=False, required=False) -%}
<input id="{{ id }}"{% if oob %} hx-swap-oob="true"{% endif %} name="{{ name }}"{% if value is not none %} value="{{ value }}"{% endif %}{% if required %} required{% endif %} />
{%- endmacro %}
```

Equivalencias verificadas contra el marcado actual:

| Sitio | Llamada |
|---|---|
| `purchase_panel.html:44` label | `qty_label('purchase-qty-label')` |
| `purchase_panel.html:45` input | `number_input('purchase-unit-cost', 'unit_cost', fv.get('unit_cost', ''))` |
| `sale_panel.html:26` label | `qty_label('sale-qty-label')` |
| `sale_panel.html:27` input (hoy **sin** `value`) | `number_input('sale-unit-price', 'unit_price')` |
| `cart_add.html:16` input (`value="0"`) | `number_input('sales-doc-unit-price', 'unit_price', '0')` |
| `issue_form.html:26` | `text_input('sales-doc-client-id', 'client_id', draft.client_id or '')` |
| `issue_form.html:27` | `text_input('sales-doc-client-address', 'client_address', draft.client_address or '')` |

Solo cambian el `<label>`/`<input>` de esas líneas; el `<div class="row">` y la etiqueta de texto que los rodean
quedan igual.

### Plantillas de fragmento (CREATE, en la estructura objetivo `templates/features/`)

| Plantilla | Contenido | Endpoint |
|---|---|---|
| `features/purchases/_defaults_oob.html` | `qty_label('purchase-qty-label', uom, oob=True)` + `number_input('purchase-unit-cost', 'unit_cost', cost, oob=True)` | `product_defaults_purchase` |
| `features/sales/_defaults_oob.html` | `qty_label('sale-qty-label', uom, oob=True)` + `number_input('sale-unit-price', 'unit_price', price, oob=True)` | `product_defaults_sale` |
| `features/sales/_product_options.html` | el bucle de 3 `<option>` sobre `product_options` | `sale_product_options` y el datalist de `sale_panel.html:35-40` (`{% include %}`) |
| `features/sales_documents/_product_defaults_oob.html` | `number_input('sales-doc-unit-price', 'unit_price', price, oob=True)` | `sales_doc_product_defaults` |
| `features/sales_documents/_customer_defaults_oob.html` | `text_input('sales-doc-client-id', …, oob=True, required=True)` + `text_input('sales-doc-client-address', …, oob=True)` | `sales_doc_customer_defaults` |
| `features/transfers/_product_options.html` | el bucle actual (`label`, `sku`, `name` si existe) | `transfer_product_options` (el datalist inicial de `tab_transfers.html` es distinto y no se toca) |
| `features/stock/_supplier_return_lots.html` | `<select name="lot_id">` con las 4 variantes por variables `state` (`empty`/`error`/`none`/`list`), `error_msg`, `lots` | `ui_supplier_return_lots` |

- Los handlers pasan valores ya formateados (`cost`/`price` como hoy: `""` o `str(float(...))`; `uom` sin escapar,
  Jinja2 escapa). Se elimina `html.escape` de esos handlers.
- Cada endpoint responde con `templates.TemplateResponse(request=request, name=..., context=...)` como
  `product_suggest`. Las ramas que hoy devuelven `HTMLResponse("")` siguen igual.
- `ui_supplier_return_lots` conserva su `try/except` y el recorte `str(e)[:50]`; solo cambia cómo se pinta.

### Sidebar: `app/routers/ui_nav.py` (CREATE)

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class NavItem:
    slug: str
    label: str
    roles: Optional[frozenset[str]]  # None = visible para cualquier rol (hoy: solo "sales")
    server_only: bool = False

NAV_ITEMS: tuple[NavItem, ...] = (...)  # los 17 de la tabla de 2.1, en ese orden

def visible_nav_items(role: str, app_mode: str) -> list[NavItem]: ...
def default_tab(role: str) -> str: ...   # "sales" si role == "operator", si no "home"
```

- `role` llega normalizado como en la plantilla: `(user.role or "").lower()`; `""` si no hay usuario.
- `server_only` oculta el botón cuando `app_mode == "desktop"` (misma comparación que `app_mode != 'desktop'`).
- `dashboard` (`ui_home.py`) añade `nav_items` y `default_tab` al contexto. `dashboard.html` sustituye las líneas
  36-128 por un `{% for item in nav_items %}` que pinta el mismo `<button>`; `aria-selected` es `"true"` si
  `item.slug == default_tab`, y `#tab-content` usa `/ui/tabs/{{ default_tab }}`.
- Comprobación de equivalencia de `aria-selected`: hoy `tab-home` es `"true"` literal, pero solo se pinta para
  admin/owner, cuyo `default_tab` es `home`; `tab-sales` es `"true"` solo para operator. Mismo resultado.

### Arnés H-303 (`tests/snapshots/ui_snapshot.py`, MODIFY)

- `_request(client, key, method, url, data=None, hx=True) -> str`: devuelve `f"status={code}\n" + canonical(text)`;
  no exige 200 ni un mínimo de líneas.
- Siembra adicional en `_seed()` (mismo `get_session()`):
  - usuarios `snap-operator` (operator, `business_id` = negocio sembrado) y `snap-owner` (owner, con fila en
    `UserBusiness` para ese negocio), contraseña con `app.auth.hash_password`;
  - un segundo negocio vacío `snap-empty` creado con `POST /ui/businesses/create` como admin (crea su CENTRAL).
- Secuencia de `capture()` (el orden importa):
  1. Lo actual (login, 40 claves admin) sin cambios de claves.
  2. Fragmentos `frag_*` (REQ-3) como admin sobre el negocio sembrado.
  3. POST con error `post_*` de REQ-2 salvo el de SKU de otro negocio y el de contraseña; POST válidos
     `post_purchase_new_product` y `post_sale_ok`.
  4. Cambio al negocio `snap-empty` con el endpoint de `ui_auth.py` que fija `active_business_id`; captura
     `empty_tab_<slug>` de las 17 pestañas; crea un producto en `snap-empty`; vuelve al negocio sembrado.
  5. `post_product_sku_other_business` (`new_sku` = SKU del producto de `snap-empty`).
  6. Un `TestClient(app)` nuevo por rol (sin `with`, para no relanzar el lifespan): login `snap-operator` → captura
     `operator_dashboard` y `operator_tab_<slug>` de sus pestañas visibles; ídem `snap-owner`.
  7. Último, como hoy: `_force_password_change()`, `modal_must_change_password` y
     `post_must_change_password_mismatch`.
- Los nombres de campos de formulario y de query se copian de la firma real de cada endpoint al escribir el envelope
  (`POST /ui/transfers` lee el formulario a mano en `create_transfer`, `ui_transfers.py:495`).
- **Baseline:** se escribe al final del Lote 0, con `git diff --stat b741a46 -- app/` vacío (código sin migrar), y
  se commitea en el mismo commit que la ampliación del arnés.
- **Criterio de cierre (REQ-5):** en el envelope de verificación del Lote 0, borrar temporalmente `hx-swap-oob='true'`
  del input de `product_defaults_sale` y cambiar un texto de error de compra; `check` debe salir con código 1 en ambos
  casos; después `git checkout -- app/`.

### Tests nuevos

| Fichero | Tipo | Qué fija |
|---|---|---|
| `tests/e2e/test_fase4_characterization_e2e.py` | e2e | REQ-9: sidebar por rol (admin, owner, operator; ids y orden de REQ-8 en server), autorrelleno de coste en Compras, precio en Ventas, precio y cliente en Documentos, select de lotes en Inventario. Pasa antes y después. |
| `tests/unit/test_ui_context.py` | unit | perezoso; una llamada por dato (con `unittest.mock.patch` sobre `app.routers.ui_context.get_current_user_from_session` / `require_active_business_id`); 409 y 403 con los mismos `detail`; `ctx.db` es el `db` del handler. |
| `tests/unit/test_ui_nav.py` | unit | tabla de verdad de REQ-8 (6 combinaciones rol × modo) y `default_tab`. |
| `tests/unit/test_routers_no_html.py` | unit | REQ-10: ningún `app/routers/*.py` contiene `hx-swap-oob`, `<option`, `<input`, `<select` ni `<label`; y ninguna plantilla de `app/templates/features/**/*.html` usa `|safe` ni `Markup(` (misma regex que `test_ui_components.py:98`, que solo revisa `components/*.html`). |

## 2.4 Contratos de API

Sin endpoints nuevos ni cambios de contrato. Cuerpos HTML idénticos en canónico; mismos códigos de estado.

## 2.5 Seguridad

| Check | Respuesta |
|---|---|
| ¿Frontera de auth nueva? | No. `UIContext` delega en las funciones existentes; la autenticación sigue en el middleware de `main.py`. |
| ¿Datos sensibles nuevos? | No. |
| ¿Validación de entrada? | Sin cambios; los handlers mantienen sus `Form`/query. |
| ¿Superficie nueva? | No. Menos superficie de XSS: el escape manual (`html.escape`) pasa a autoescape de Jinja2; el arnés siembra textos hostiles y la guarda impide reintroducir HTML en routers. |
| ¿Secretos? | No. El arnés usa contraseñas de prueba en su BD temporal. |
| OWASP | A01 (control de acceso): el orden de guardias se preserva con el contexto perezoso y lo prueban los e2e y `test_ui_context.py`. A03 (inyección/XSS): autoescape. |

## 2.6 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Resolución en `UIContext` | Perezosa, por propiedad | Ansiosa (resolver todo en la dependencia) | Cambia el orden de errores (409 antes que 403) en handlers que comprueban el rol primero. |
| Ubicación de `UIContext` | `app/routers/ui_context.py` | `app/deps.py` o `app/security.py` | Son invariantes de control de acceso que no se tocan; el contexto es de la capa UI. |
| Alcance de la migración | Todos los handlers UI menos `ui_auth.py` | Solo los 5 routers de los fragmentos | El diagnóstico asigna `UIContext` a la fase 4; dejar dos estilos en paralelo es deuda. |
| Funciones auxiliares `(db, request)` | Sin tocar | Pasarles `ctx` | Cambia firmas usadas por varios routers; se resuelve al mover las consultas en la fase 5. |
| Marcado compartido panel/fragmento | Macros en `components/forms.html` | Plantilla de fragmento independiente | Una sola definición es lo que evita que el OOB pise el panel con atributos distintos (la deuda que señala el diagnóstico). |
| Dónde viven los fragmentos | `templates/features/<dominio>/` | `templates/partials/` | Nacen en la estructura objetivo; la fase 5 no tiene que moverlos otra vez. |
| Sidebar | `NAV_ITEMS` en Python + bucle | Macro Jinja2 con la lista dentro | La lista en Python es testeable sin renderizar (tabla de verdad) y reutilizable. |
| Divergencias heredadas | Conservar | Unificar | Cambia comportamiento en una release de equivalencia (REQ-12). |

## 2.7 Migración y rollback

- Sin cambios de BD ni de configuración.
- Cada lote es un commit (o varios) revertible por separado; el arnés y los e2e corren en CI en cada push.
- Rollback: `git revert` del lote. El Lote 0 (red) se mantiene aunque se reviertan los demás.

## 2.8 Contexto externo (Vía 1)

No hace falta MCP. Todo lo necesario está en el repo; las firmas se copian del código real al escribir los envelopes.

## 2.9 Pre-mortem contra las 10 dimensiones

| Dimensión | Riesgo | Mitigación (va como instrucción en los envelopes) |
|---|---|---|
| **1. SoC/SRP** | `UIContext` o `NAV_ITEMS` sin consumidor; `ensure_*` de `ui_common` muertos tras migrar. | `UIContext` lo consumen los 20 routers en el mismo PAE; `NAV_ITEMS` lo consume `dashboard`. Envelope final: grep de referencias de `ensure_admin`/`ensure_admin_or_owner`; si son 0, eliminarlos. |
| **2. Cohesión/acoplamiento** | Recalcular usuario/negocio varias veces por petición (hoy 3-4 consultas de usuario). | Caché por propiedad en `UIContext`; test que cuenta llamadas. Marcado duplicado panel/fragmento → macros únicas. |
| **3. Capas** | Que `UIContext` empuje lógica de seguridad a la UI o al revés. | `ui_context.py` depende de `app.security`; `security.py`/`deps.py` no importan nada de `routers`. Las plantillas no deciden roles: reciben `nav_items` ya filtrados. |
| **4. Seguridad** | Escape perdido al mover f-strings a plantillas; RBAC confundido con visibilidad del sidebar. | Autoescape de Jinja2; `|safe`/`Markup(` prohibidos en `components/` (`test_ui_components.py:98`) y en `features/` (`test_routers_no_html.py`, nuevo); siembra hostil del arnés; RBAC queda en los endpoints, sin cambios. |
| **5. Robustez** | Añadir `try/except` en `UIContext` cambiaría códigos de estado; perder el `try/except` de `ui_supplier_return_lots`. | `UIContext` no captura nada: propaga las `HTTPException` tal cual. Los handlers de fragmentos conservan sus ramas de error; solo cambia el render. |
| **6. Consistencia transaccional** | Caché de `business_id` desfasada si un handler cambia el negocio de la sesión. | Solo `ui_auth.py` escribe la sesión y queda fuera. Sin escrituras de datos nuevas. |
| **7. Observabilidad** | Ninguno nuevo. | Los logs actuales de los handlers se conservan sin cambios. |
| **8. Configuración** | `app_mode` leído distinto en Python y plantilla. | `visible_nav_items` recibe `app_mode_label()`, el mismo valor que hoy recibe la plantilla. |
| **9. Pruebas** | Arnés ciego a POST/roles/vacíos (H-303); e2e que solo pasan después del cambio. | Lote 0 antes de tocar `app/`; e2e de caracterización verdes con el código sin migrar; mutación documentada. |
| **10. Escalabilidad** | No aplica. | — |

## 2.10 Plan de lotes para la Fase 3

| Lote | Contenido | Cierre |
|---|---|---|
| **A (Lote 0)** | Arnés H-303 (helpers, siembra, secuencia), baseline desde código sin migrar, e2e de caracterización. | `check` OK; mutación falla como se espera; e2e nuevos verdes; `git diff --stat b741a46 -- app/` vacío. |
| **B** | `ui_context.py` + `test_ui_context.py`. | unit verdes. |
| **C** | Migración a `UIContext`, un envelope por router (los de >10 sustituciones se parten en dos). `dashboard` incluido. | Tras cada envelope: unit + `ui_snapshot.py check`. Al final: e2e completos. |
| **D** | Macros en `forms.html` + panels; 7 plantillas de fragmento + sus endpoints. | `check` idéntico; e2e de caracterización verdes. |
| **E** | `ui_nav.py` + `test_ui_nav.py` + bucle en `dashboard.html`. | `check` idéntico (dashboards de los 3 roles); `test_ui_nav` verde. |
| **F** | `test_routers_no_html.py`; eliminar `ensure_*` si quedan sin uso; CHANGELOG (0.5.0), `CLAUDE.md`, `.claude/CLAUDE.md`, diagnóstico §4 fila 4 ✅. | Suite completa + `style_snapshot.py check` contra baseline de `b741a46` con 0 diferencias. |

Verificación global de cierre: `pytest tests/unit`, `python3 tests/snapshots/ui_snapshot.py check
tests/snapshots/baseline`, `pytest tests/e2e -m e2e`, `style_snapshot.py` (fuera de CI).
