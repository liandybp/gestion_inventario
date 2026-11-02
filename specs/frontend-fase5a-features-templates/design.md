# Design — Frontend fase 5a: plantillas de `partials/` a `features/<dominio>/`

Requisitos: `requirements.md` (REQ-1..7). Rama base `develop` `bf38a2b`.

## 2.1 Anclaje en el código (verificado 2026-11-02 sobre `bf38a2b`)

- Cargador único: `app/routers/ui_common.py:22` `templates = Jinja2Templates(directory="app/templates")`. Cualquier
  ruta bajo `app/templates/` se resuelve sin configuración extra.
- `app/templates/partials/`: 56 ficheros planos. 54 tienen consumidor; 2 no (REQ-3, ver tabla).
- `app/templates/features/`: 7 fragmentos de la fase 4 en `purchases/`, `sales/`, `sales_documents/`, `stock/`,
  `transfers/`. Convención: prefijo `_` = fragmento que solo se devuelve como respuesta parcial. No se tocan.
- Todas las referencias son **cadenas literales completas** `partials/<nombre>.html`. No hay nombres dinámicos
  (`f"partials/{x}"` solo aparece en `tests/unit/test_ui_components.py:194`, REQ-5).
- Los `{% from 'components/…' import … %}` de las plantillas son absolutos: mover la plantilla no los rompe.
- Ninguna plantilla de `partials/` usa `|safe` ni `Markup(` (`grep` vacío en `bf38a2b`). Importa porque
  `tests/unit/test_routers_no_html.py:51` recorre `features/**/*.html` con `rglob` y los prohíbe.
- Guardas que recorren plantillas con `rglob` desde `app/templates` (`test_frontend_css.py:39`,
  `test_frontend_js_modules.py:54`): siguen cubriendo todo tras el movimiento.
- El arnés `tests/snapshots/ui_snapshot.py` indexa por ruta HTTP, no por nombre de plantilla: su baseline vale tal
  cual. El CI lo corre (`.gitea/workflows/deploy.yml:36-38`).

### Referencias por fichero (fuera de `partials/`)

| Fichero | Líneas con `partials/` |
|---|---|
| `app/routers/ui_purchases.py` | 16 (2 son comentarios: 59, 327) |
| `app/routers/ui_sales.py` | 15 |
| `app/routers/ui_transfers.py`, `ui_products.py` | 13 cada uno |
| `app/routers/ui_users.py`, `ui_sales_documents.py`, `ui_businesses.py` | 12 cada uno |
| `app/routers/ui_expenses.py` | 10 |
| `app/routers/ui_extractions.py` | 9 |
| `app/routers/ui_customers.py`, `ui_agent.py` | 8 cada uno |
| `app/routers/ui_stock.py`, `ui_profit_items.py` | 5 cada uno |
| `app/routers/ui_sync.py`, `ui_coach.py`, `ui_auth.py` | 3 cada uno |
| `app/routers/ui_home.py`, `ui_history.py` | 2 cada uno |
| `app/routers/ui_profit.py`, `ui_dividends.py`, `ui_activity.py` | 1 cada uno |
| `app/templates/dashboard.html:45`, `restock_print.html:22` | 1 cada uno |
| Plantillas dentro de `partials/` (se mueven con ellas) | 16 `{% include %}` |
| `tests/unit/test_ui_product_suggest.py:73` | 1 línea, 3 rutas |
| `tests/unit/test_ui_components.py:156,172,194` | 3 (194 es la f-string de REQ-5) |
| `tests/unit/test_ui_agent.py:142`, `test_ui_xss_escapes.py:100`, `test_frontend_js_modules.py:163` | 1 cada uno |
| `tests/e2e/test_js_behaviors_e2e.py:202` | 1 |
| `tests/e2e/test_ui_tour_csp_e2e.py:90` | 1 (comentario sobre `products_history.html`, REQ-3) |
| `app/static/js/components/home-charts.js:1`, `widget-chart.js:1,3` | 3 (comentarios) |

## 2.2 Enfoque

Movimiento mecánico, sin cambiar contenido:

1. `git mv` de cada plantilla a su destino (conserva el historial de `git log --follow`).
2. Sustitución exacta de la cadena `partials/<nombre>.html` por `features/<dominio>/<nombre>.html`. La cadena
   incluye `.html`, así que `partials/product_edit_form.html` no toca `partials/product_edit_form_inventory.html`, ni
   `partials/sale_panel.html` toca `partials/sales_document_panel.html`.
3. Se hace **por dominio**, con `ui_snapshot.py check` tras cada uno: si una ruta queda mal, el fallo se localiza en el
   dominio que se acaba de mover.

**Recomendación para la Fase 3 (envelopes de transcripción, no de decisión):** un script de un solo uso
`tools/move_templates.py` con la tabla de §2.3 como `dict` literal. Hace `git mv` y la sustitución en todos los `*.py`,
`*.html` y `*.js` bajo `app/` y `tests/` (no en una lista fija: las plantillas que incluyen a otras cambian de ruta
durante la propia migración), con un argumento `--domain <d>`. Aborta si una plantilla de origen no existe o si su
cadena no aparece en ningún fichero. Así
`cc_local` no edita a mano ~190 líneas. El script no se versiona: el último lote lo borra. Si DeepSeek prefiere
envelopes por fichero, la tabla de §2.3 es la fuente única igualmente.

### Invariantes que el diseño preserva

- El contenido de cada plantilla movida solo cambia en las rutas de `{% include %}` (`git diff -M` muestra renombrado
  con similitud ≥ 95 %).
- Los 7 fragmentos de la fase 4 no cambian de ruta ni de contenido.
- No cambia ningún fichero Python más allá de cadenas de ruta y los 2 comentarios de `ui_purchases.py`.
- La baseline del arnés no se regenera en ningún lote.

## 2.3 Tabla de mapeo (fuente única)

Destino = `app/templates/features/<dominio>/<mismo nombre>`. El dominio sigue al router que la renderiza; si la
renderizan dos routers, al dominio de la pestaña que la muestra.

| Dominio | Plantillas (desde `partials/`) | Consumidores |
|---|---|---|
| `activity` | `tab_activity.html` | `ui_activity` |
| `agent` | `agent_chat_turn.html`, `agent_messages.html`, `agent_reply.html`, `tab_agent.html`, `widget_chart.html`, `widget_grid.html`, `widget_kpi.html`, `widget_list.html`, `widget_table.html` | `ui_agent`; `widget_grid` incluye los 4 `widget_*`; `tab_agent` y `agent_chat_turn` incluyen `widget_grid` |
| `auth` | `must_change_password_form.html` | `ui_auth` |
| `businesses` | `business_edit_form.html`, `tab_businesses.html` | `ui_businesses` |
| `coach` | `tab_coach.html` | `ui_coach` |
| `customers` | `customer_detail.html`, `customer_edit_form.html`, `customers_table.html`, `tab_customers.html` | `ui_customers`; `tab_customers` incluye `customers_table` |
| `dividends` | `tab_dividends.html` | `ui_dividends`, `ui_extractions` |
| `expenses` | `expense_edit_form.html`, `tab_expenses.html` | `ui_expenses` |
| `extractions` | `extraction_edit_form.html` | `ui_extractions` |
| `history` | `tab_history.html` | `ui_history` |
| `home` | `home_charts.html`, `tab_home.html` | `ui_home`; `tab_home` incluye `home_charts` |
| `products` | `product_edit_form.html`, `product_edit_form_inventory.html`, `product_panel.html`, `product_suggest_options.html` | `ui_products`, `ui_purchases`; `tab_purchases` y `purchase_panel` incluyen `product_panel` |
| `profit` | `tab_profit.html` | `ui_profit` |
| `profit_items` | `profit_items_edit_form.html`, `tab_profit_items.html` | `ui_profit_items` |
| `purchases` | `purchase_edit_form.html`, `purchase_panel.html`, `purchases_history.html`, `tab_purchases.html` | `ui_purchases`; `purchase_panel` incluye `purchases_history` |
| `sales` | `sale_edit_form.html`, `sale_panel.html`, `sales_history.html`, `tab_sales.html` | `ui_sales`; `sale_panel` incluye `sales_history` |
| `sales_documents` | `sales_document_edit_form.html`, `sales_document_panel.html`, `sales_document_preview.html`, `tab_documents.html` | `ui_sales_documents`; `tab_documents` incluye `sales_document_panel` |
| `stock` | `restock_table.html`, `stock_table.html`, `tab_inventory.html` | `ui_stock`, `ui_products`; `restock_print.html` incluye `restock_table` |
| `sync` | `sync_status.html` | `ui_sync`; `dashboard.html` la incluye |
| `transfers` | `tab_transfers.html`, `transfer_bulk_modal.html`, `transfer_edit_form.html` | `ui_transfers` |
| `users` | `tab_users.html`, `user_edit_form.html`, `user_reset_password_form.html` | `ui_users` |
| **(borrar)** | `action_result.html`, `products_history.html` | ninguno (REQ-3) |

Total: 54 movidas en 21 dominios (16 directorios nuevos + los 5 de la fase 4) + 2 borradas = 56.

## 2.4 Componentes

### Guarda `tests/unit/test_templates_layout.py` (CREATE, REQ-4)

- `test_partials_dir_is_gone`: `not (ROOT / "app/templates/partials").exists()`.
- `test_no_partials_references`: recorre `app/**/*.py`, `app/templates/**/*.html`, `app/static/js/**/*.js` y
  `tests/**/*.py` (excluido `Path(__file__)`), y falla listando `fichero:línea` de cada `partials/`.
- `test_literal_template_names_resolve`: extrae con la regex
  `["']((?:features|layouts|components)/[\w/]+\.html|[\w]+\.html)["']` los nombres de los ficheros Python de
  `app/routers/` y de los `{% include|extends|from|import %}` de `app/templates/**/*.html`. Para cada nombre llama a
  `templates.env.get_template(name)` y lista los que lanzan `TemplateNotFound`. Excluye los nombres construidos con
  `{` (f-strings). Esta parte pasa ya en `bf38a2b` con las rutas `partials/` incluidas en la regex mientras dure la
  migración. El último lote quita `partials` de la regex.

### Test `tests/unit/test_ui_components.py::test_old_profit_items_templates_are_gone` (MODIFY, REQ-5)

La ruta pasa a `f"features/profit_items/profit_items_{kind}_edit_form.html"`.

## 2.5 Contratos de API

Sin cambios: mismas rutas HTTP, mismos códigos, mismo HTML.

## 2.6 Seguridad

| Comprobación | Respuesta |
|---|---|
| Frontera de auth nueva | No. |
| Datos sensibles nuevos | No. |
| Escape | Sin cambios: autoescape de Jinja2 depende de la extensión `.html`, que se conserva. `test_routers_no_html.py` pasa a vigilar `\|safe`/`Markup(` en 54 plantillas más. |
| Superficie de ataque | Ninguna nueva. |

## 2.7 Trade-offs

| Decisión | Elegida | Alternativa | Por qué no |
|---|---|---|---|
| Nombres | Mismo nombre, otra carpeta | Quitar prefijo de dominio (`features/sales/tab.html`) | Decisión del usuario: diff revisable, sustitución mecánica sin decisiones. |
| Plantillas compartidas por 2 routers | Al dominio de la pestaña que las muestra | Directorio `shared/` | Solo hay 3 casos (`product_panel`, `tab_dividends`, `tab_inventory`) y todos tienen pestaña dueña clara. |
| Plantillas muertas | Borrar | Moverlas también | Mover código muerto lo hace parecer vivo en la estructura nueva. |
| Método | Script de un solo uso por dominio | Edición a mano por fichero | ~190 líneas casi idénticas: la edición a mano es donde `cc_local` se equivoca de carácter. |
| Raíz de `templates/` | Sin tocar (`dashboard`, `login`, impresiones) | Moverlas a `features/` | Son páginas completas, no partes de un dominio; fuera del principio 1 del diagnóstico. |

## 2.8 Migración y rollback

- Sin cambios de BD ni de configuración. El Dockerfile copia `app/` entero (sin rutas de plantillas).
- Rollback: `git revert` del merge; ningún dato depende de las rutas.

## 2.9 Pre-mortem contra las 10 dimensiones

| Dimensión | Riesgo | Mitigación (texto para los envelopes) |
|---|---|---|
| **1. SoC/SRP** | Plantillas muertas trasladadas a la estructura nueva. | REQ-3: se borran en el mismo lote que su dominio (`products`), no se mueven. |
| **2. Cohesión** | Plantillas de un dominio repartidas en dos carpetas. | La tabla §2.3 es la única fuente; el script aborta si una plantilla no está en la tabla. |
| **3. Capas** | Una plantilla de `features/` incluye otra de un dominio distinto. | Permitido solo en los casos de la tabla (`purchases`→`products/product_panel`, `print`→`stock/restock_table`, `dashboard`→`sync/sync_status`). Sin dependencias nuevas. |
| **4. Seguridad** | Una plantilla con `\|safe` entra en `features/`. | Verificado: ninguna. `test_routers_no_html.py` lo vigila. |
| **5. Robustez** | Ruta mal escrita en un handler que el arnés no captura (modal de error, impresión) → 500 en producción. | `test_literal_template_names_resolve` carga cada nombre literal: cubre lo que el arnés no renderiza. |
| **6. Transaccional** | No aplica: sin escrituras. | — |
| **7. Observabilidad** | No aplica. | — |
| **8. Configuración** | Ruta de plantillas en Docker/CI. | Verificado: el Dockerfile copia `app/`; el CI no nombra plantillas. |
| **9. Pruebas** | Un test que pasa sin comprobar nada tras el movimiento (`test_old_profit_items_templates_are_gone`). | REQ-5. La guarda de REQ-4 detecta cualquier otra ruta `partials/` residual en tests. |
| **10. Escalabilidad** | No aplica. | — |

## 2.10 Plan de lotes para la Fase 3

| Lote | Contenido | Cierre |
|---|---|---|
| **A (Lote 0)** | `test_templates_layout.py` con `test_literal_template_names_resolve` (regex con `partials` aún admitido); baseline de `style_snapshot.py` escrita desde `bf38a2b` (fuera de git, ruta en el PAE); `tools/move_templates.py`. | Test nuevo verde; `ui_snapshot.py check` OK; `git diff --stat bf38a2b -- app/` vacío. |
| **B** | Dominios sin dependencias cruzadas: `activity`, `auth`, `businesses`, `coach`, `history`, `profit`, `sync`, `users`. | Tras cada dominio: `ui_snapshot.py check` + `pytest tests/unit/test_templates_layout.py`. |
| **C** | `agent` (9 plantillas con includes internos), `home`, `customers`, `expenses`, `extractions`, `dividends`. | Igual que B; además `test_ui_agent.py`, `test_ui_xss_escapes.py`, `test_frontend_js_modules.py`. |
| **D** | `products` (+ borrar las 2 muertas), `purchases`, `sales`, `sales_documents`, `stock`, `transfers`, `profit_items` (+ REQ-5). | Igual que B; además `test_ui_product_suggest.py`, `test_ui_components.py`. |
| **E** | Activar `test_partials_dir_is_gone` y `test_no_partials_references`; quitar `partials` de la regex; borrar `tools/move_templates.py`; comentarios JS/e2e; docs de REQ-6. | `pytest tests/unit`, `ui_snapshot.py check`, `style_snapshot.py check` (0 diferencias), `pytest tests/e2e -m e2e`. |

Verificación global de cierre: `pytest tests/unit`, `python3 tests/snapshots/ui_snapshot.py check
tests/snapshots/baseline`, `pytest tests/e2e -m e2e`, `style_snapshot.py check` contra la baseline de `bf38a2b`, y
`git diff -M --stat bf38a2b -- app/templates` con 54 renombrados y 2 borrados.
