# Design — Frontend modular, fase 1: layouts + macros `modal`/`field`

> SDD Phase 2 · PAE-016 · Fuente: `requirements.md` (REQ-1..9)

## 2.1 Codebase grounding

Verificado leyendo el repo en `276b411` (rama `feature/frontend-modular`).

| Fichero | Rol | Operación |
|---|---|---|
| `tests/snapshots/ui_snapshot.py` | Arnés de snapshots (REQ-1) | CREATE |
| `tests/snapshots/baseline/*.txt` (40) | Baseline del código previo (REQ-2) | CREATE (generados) |
| `app/templates/components/modal.html` | Macro `modal()` (REQ-5) | CREATE |
| `app/templates/components/forms.html` | Macro `field()` (REQ-6) | CREATE |
| `app/templates/base.html` | Esqueleto HTML sin assets (REQ-3) | CREATE |
| `app/templates/layouts/app_shell.html` | Tema + 5 assets de la app (REQ-3) | CREATE |
| `app/templates/layouts/print.html` | Layout de impresión sin assets (REQ-3) | CREATE |
| `app/templates/dashboard.html` | `extends layouts/app_shell.html` (REQ-4) | MODIFY |
| `app/templates/login.html` | `extends base.html` (REQ-4) | MODIFY |
| `app/templates/{transfer,restock,label_single}_print.html` | `extends layouts/print.html` (REQ-4) | MODIFY |
| `app/templates/partials/` 14 modales | `{% call modal %}` + `field()` (REQ-6/7) | MODIFY |
| `app/templates/partials/profit_items_edit_form.html` | Modal unificado (REQ-8) | CREATE |
| `app/templates/partials/profit_items_{sale,purchase,adjustment,transfer_in}_edit_form.html` | Sustituidos | DELETE |
| `app/routers/ui_profit_items.py` | 4 handlers GET `/edit`: nombre de plantilla + `"kind"` en el contexto | MODIFY |
| `tests/unit/test_ui_components.py` | 22 tests de macros/layouts/unificado | CREATE |
| `tests/e2e/test_modal_components_e2e.py` | 3 e2e (modal cliente, cierre, Utilidades) | CREATE |
| `CHANGELOG.md` | Entrada bajo 0.5.0 (REQ-9) | MODIFY |
| `app/routers/ui_common.py` (`templates`, global `static_version`) | Entorno Jinja2 (autoescape activo) | REFERENCE |
| `app/static/js/app.js:288-307` (`closeModal`, `data-modal-close`, `data-modal-static`) | Contrato JS de los modales: depende de `.modal-overlay`, `id`, `data-modal-close`, `data-modal-static` | REFERENCE (no cambia) |
| `tab_profit_items.html:131-150` (`hx-select=".modal-overlay"`) | Abre los modales de Utilidades | REFERENCE (no cambia) |

Hechos verificados que condicionan el diseño:

- Los 18 modales comparten el esqueleto `modal-overlay > modal-container > modal-content > modal-header`,
  con 4 variantes: `business_edit_form` (`modal-container-wide`, `<h2>` sin clase, `aria-label="Cerrar"`),
  `sales_document_edit_form` (`wide`, título con `<code>{{ doc.code }}</code>`),
  `must_change_password_form` (`data-modal-static="1"`, sin botón de cierre) y `transfer_bulk_modal`
  (`style="max-width: 760px;"` en el container). `transfer_edit_form` importa `flash` dentro de `modal-content`.
- Las 5 plantillas con `<head>` propio comparten exactamente las líneas 1-5 (doctype, `<html lang="es">`,
  `<head>`, 2 `<meta>`) y la 6 es `<title>`. Solo `dashboard.html` carga htmx/Chart.js/JsBarcode/app.css/
  app.js; `label_single_print.html` carga solo JsBarcode; login y las otras 2 impresiones, ninguno.
- Rutas reales (las del prototipo eran inventadas): `/ui/transfers/print?movement_id=` (o `ref=`),
  `/ui/restock-print`, `/ui/purchase/{movement_id}/label`. `/ui/must-change-password-form` devuelve `""` si
  el usuario no tiene `must_change_password` → el arnés lo captura tras activar ese flag.
- `Jinja2 3.1.6` con autoescape: un `{% set x %}…{% endset %}` produce `Markup` (escapando las variables
  internas) y `caller()` devuelve `Markup` → títulos con marcado y cuerpos se pasan sin `|safe`
  (comprobado ejecutándolo).
- `labels_print.html` está vacío (0 líneas) y ninguna ruta lo renderiza → fuera de alcance.

## 2.1b Contexto externo

No se usa ningún MCP en los envelopes. Las firmas y fragmentos van inline.

## 2.2 Architecture approach

1. **Red de seguridad primero (lote A).** Arnés lado servidor: `TestClient(app)` con lifespan real
   (migraciones + seed de usuarios) sobre un SQLite en `tempfile`, login admin, siembra fija
   (producto `SNAP-1`, compra → traspaso CENTRAL→POS → venta en POS → ajuste, cliente, gasto, retiro,
   documento F con 1 línea; fechas fijas `2026-01-15T10:00Z`) y GET de 40 URLs. Cada respuesta se
   canoniza con `html.parser` (stdlib): una línea por token, atributos ordenados, espacios colapsados,
   comentarios HTML descartados, entidades decodificadas, y sustitución de fecha/hora/año-mes/mes por
   marcadores. La baseline se genera **antes** de tocar plantillas y se commitea.
2. **Componentes (lote B).** `components/modal.html`, `components/forms.html`, `base.html` (sin assets),
   `layouts/app_shell.html` (head de dashboard tal cual), `layouts/print.html` (solo `extends base.html` +
   contrato documentado). Migración de las 5 páginas: cabecera → `{% extends %}{% block title %}…`, cuerpo
   del `<head>` → `{% block head %}`, cuerpo del `<body>` → `{% block body %}`.
3. **Modales (lotes C-D).** Cada fichero: imports de macros arriba, `{% call modal(...) %}` sustituye las
   7 líneas de cabecera, `{% endcall %}` las 3 de cierre, y las filas `label + input` simples pasan a
   `field()`. Los 2 modales largos (`business_edit_form`, 267 líneas, y `transfer_bulk_modal`) se migran solo
   en el esqueleto (sustitución de cabecera y cierre); el resto se reescribe completo.
4. **Unificado de Utilidades (lote E).** `profit_items_edit_form.html` recibe `kind`; deriva `slug`
   (`transfer_in`→`transfer-in`), id del modal, título, id del datalist y qué campos son editables. El router
   cambia el nombre de plantilla y añade `"kind"` en los 4 handlers GET `/edit`. Los 4 ficheros se borran.
5. **Tests (lote F)** y **CHANGELOG (lote G)**.

Invariantes que el diseño preserva:
- ids (`#customer-edit-modal`, `#profit-items-sale-edit-modal`…), clases (`modal-overlay`,
  `modal-container[-wide]`, `modal-close`, `row`) y `data-*` (`data-modal-close`, `data-modal-static`,
  `data-product-suggest`) idénticos: `app.js` y los 44 e2e dependen de ellos.
- Autoescape de Jinja2 intacto: ninguna macro usa `|safe`; nada de HTML como string concatenado.
- Las rutas, la lógica de los handlers y los contextos no cambian (salvo `kind` y el nombre de plantilla).

## 2.3 Data model

Sin cambios de modelo ni migraciones.

## 2.4 API contracts

Sin endpoints nuevos ni cambios de contrato. Los 4 GET `/ui/tabs/profit-items/{sale|purchase|adjustment|transfer-in}/{movement_id}/edit`
devuelven el mismo HTML (verificado por el arnés); internamente renderizan `partials/profit_items_edit_form.html`
con `kind` ∈ {`sale`, `purchase`, `adjustment`, `transfer_in`}.

## 2.5 Security review

| Check | Answer |
|---|---|
| New auth boundary? | No. Los handlers conservan `ensure_admin` / `require_active_business_id`. |
| New sensitive data? | No. |
| Input validation? | Sin entradas nuevas. `kind` lo fija el router (literal), nunca el usuario. |
| New attack surface? | Macros que reciben datos de usuario (`title`, `value`, `attrs`). Mitigación: autoescape en todos los `{{ }}`; cuerpo vía `caller()`; títulos con marcado vía `{% set %}…{% endset %}` (escapa variables). Prohibido `|safe` en `components/`. Tests: `test_modal_body_is_markup_and_values_are_escaped`, `test_field_escapes_value_and_renders_flags`, e2e con `Cliente <b>E2E</b>`. |
| Secrets needed? | No. El arnés usa credenciales fijas de test sobre un SQLite temporal (nunca `DATABASE_URL` real: lo fija antes de importar la app y aborta si la URL efectiva no es su SQLite temporal — el `.env` del repo apunta a la BD de dev). |
| OWASP | A03 Injection (XSS almacenado) — cubierto arriba. Sin cambios de CSP (fase 2). |

## 2.6 Trade-offs

| # | Decisión | Elegida | Alternativa | Por qué no la alternativa |
|---|---|---|---|---|
| D1 | Cuerpo del modal | `caller()` | dict de opciones / `body` string | Un string concatenado se autoescapa (el bug del prototipo) y el "arreglo" con `|safe` reabre XSS. |
| D2 | Arnés | **Lado servidor** (TestClient + HTML canónico) | Playwright con DOM en vivo (el del prototipo, y lo que sugería el traspaso: normalizar hora + `wait_for` ante la carrera de Chart.js) | El DOM en vivo depende de JS: la carrera de tamaño de `<canvas>` no se elimina con esperas, solo se hace menos frecuente. El refactor de la fase 1 es de plantillas → comparar el HTML del servidor es la medida exacta, determinista y 10× más rápida (~3 s). La normalización de hora/mes se mantiene. El comportamiento en navegador lo cubren los 44+3 e2e. |
| D3 | Comparación | Tokens canónicos (atributos ordenados, espacios colapsados) | Texto exacto | La macro cambia indentación y orden de atributos sin cambiar el DOM; el texto exacto daría falsos positivos en los 40 ficheros. |
| D4 | `close_label` por defecto | `None` (solo `business_edit_form` pasa `'Cerrar'`) | `'Cerrar'` (firma del traspaso) | Con `'Cerrar'` por defecto, 17 modales ganan `aria-label` → el HTML cambia y la fase deja de ser verificable como "equivalente". Mejora de a11y diferida a fase 3. |
| D5 | Alcance de `field()` | Filas `label + input` de los 18 modales (65) | Las 115 filas del proyecto / ninguna | Ninguna = código muerto (fallo del prototipo). Todas = fase enorme y mezcla dominios que se reorganizan en fase 5. |
| D6 | Unificado de Utilidades | 1 plantilla con `kind` del router | 4 plantillas con las macros | Las 4 solo difieren en URL, título y editabilidad (diagnóstico §2); la duplicación es el problema que la fase ataca. |
| D7 | `layouts/print.html` | Layout mínimo (`extends base.html`) con el contrato "sin assets de la app" | Mover los `<style>` comunes al layout | Los 3 `<style>` difieren; unificarlos cambia el HTML. `print.css` compartido = fase 3. El layout ya da el punto de extensión y el test fija el contrato. |
| D8 | Baseline | Commiteada en `tests/snapshots/baseline/` | Generarla al vuelo desde la rama base | Commiteada, cada envelope y la revisión verifican contra la misma referencia sin `git stash`/worktrees; las fases 2-5 la regeneran explícitamente cuando cambien el HTML a propósito. |
| D9 | Arnés en la suite | Script aparte, fuera de pytest | Test de pytest | Levanta la app completa con su lifespan y siembra; mezclarlo con los unit tests (SQLite en memoria por test) los ralentiza y acopla. Se ejecuta en cada envelope de plantillas. |

## 2.7 Migration & rollback

Sin migraciones de datos. Cada lote es un commit; rollback = `git revert` del lote. El HTML es equivalente
por construcción (arnés), así que no hay estado intermedio visible para el usuario.

## 2.9 Pre-mortem contra las 10 dimensiones

**Riesgos ya materializados en el prototipo descartado (traspaso, "Siguiente etapa") — mitigación obligatoria:**

| Riesgo | Cómo falló | Mitigación en este PAE |
|---|---|---|
| R1. Macro `modal` con dict + HTML concatenado | "Editar cliente" mostraba `&lt;div class="row"&gt;…` | Firma con `caller()` (D1), código literal en T3; test unitario de cuerpo vivo + escape; e2e con nombre `Cliente <b>E2E</b>` que falla si el cuerpo se escapa (validado por mutación). |
| R2. Scripts filtrados a impresión vía `base.html` | `/ui/restock-print` cargaba 5 assets que antes no cargaba | `base.html` sin ningún asset; assets solo en `layouts/app_shell.html`; tests `test_base_and_print_layout_load_no_app_assets`; snapshots de las 3 impresiones con sus rutas reales (mutación "script en base.html" → 5 snapshots fallan). |
| R3. Alcance incompleto (1/18 modales, 3/5 `extends`, `forms.html` sin uso, 4 huérfanos) | El informe decía "✅ verificado" | Un envelope por fichero/par con código literal; DoD con conteos mecánicos: `grep -rl 'class="modal-overlay"' app/templates/partials` vacío, 5 `{% extends` en raíz, `field(` presente en los modales, 4 ficheros borrados (test `TemplateNotFound`). |
| R4. Arnés que compara 404 con 404 y no normaliza hora/canvas | Diff vacío sin probar nada; 8 falsos positivos entre corridas | Rutas reales; cualquier status ≠ 200 o respuesta casi vacía aborta; normalización de fecha/hora/mes; lado servidor (sin canvas, D2). Validado: `write`+`check` consecutivos idénticos. |
| R5. Sin commits | Todo en working tree | `cc_local_pae` exige un commit por lote; los envelopes listan los ficheros exactos para `git add` (nunca `-A`: hay cambios ajenos sin commitear). |

**Dimensiones 1, 2, 3, 5, 6 (las propensas a deuda silenciosa):**

| Dimensión | Resolución en el diseño |
|---|---|
| 1. SoC/SRP | Cada componente nuevo tiene consumidores en este mismo PAE: `modal` (18 modales), `field` (13 ficheros, 65 filas), `base.html` (login + 2 layouts), `app_shell` (dashboard), `print` (3 impresiones). Nada "por si acaso". |
| 2. Cohesión/Acoplamiento | Las macros no conocen dominios; el unificado deriva todo de `kind` en un solo sitio (cabecera del template). El contrato JS (`data-modal-close`) vive en la macro, un solo lugar. |
| 3. Capas/Límites | Dirección única: página → layout → base; partial → componente. Los componentes no importan partials. |
| 5. Robustez | Sin código Python nuevo en `app/` salvo el literal `"kind"`. El arnés falla de forma explícita (clave, URL, status) y nunca compara errores. `kind` desconocido no puede llegar (literal del router). |
| 6. Consistencia | No aplica a cifras; la equivalente aquí es "misma referencia": todas las verificaciones comparan contra la misma baseline commiteada (D8). |
| 4, 7-10 | Seguridad en 2.5; observabilidad/config/escalabilidad no aplican; pruebas: +22 unit, +3 e2e, arnés 40 páginas. |

## 2.10 Delegation

Ninguna.

## Validación del diseño (antes de escribir los envelopes)

El resultado completo de los envelopes se construyó y verificó en un worktree desechable
(`/tmp/sdd-fase1-wt`, desde `276b411`), y `tasks.md` se generó por script a partir de esos ficheros:
- Arnés: `write` + `check` consecutivos → `OK: 40 snapshots equivalentes`.
- Tras todos los cambios: `check` contra la baseline del código original → `OK: 40 snapshots equivalentes`.
- Mutaciones detectadas por el arnés: script en `base.html` (5 snapshots fallan), cuerpo del modal
  escapado (los 18 modales fallan), `readonly` perdido en `field` (2 fallan). El e2e del cliente falla con el
  cuerpo escapado.
- Suites: 409 unit (387 + 22) y 47 e2e (44 + 3) en verde.
- Simulación: aplicar los envelopes de `tasks.md` a una copia limpia de `276b411` reproduce el worktree
  (`diff -r` vacío, salvo la baseline generada y el CHANGELOG).

## Contexto y método

Exploración con `grep`/lectura directa: Serena (LSP solo Python) no cubre plantillas Jinja2 y el índice de
codebase-memory solo tiene cobertura parcial de HTML (diagnóstico, "Contexto y método"). Las firmas Python
(`InventoryService`, esquemas, modelos, handlers) se leyeron del código real.
