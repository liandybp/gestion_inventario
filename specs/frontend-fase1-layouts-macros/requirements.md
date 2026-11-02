# Requirements — Frontend modular, fase 1: layouts + macros `modal`/`field`

> SDD Phase 1 · PAE-016 · Release 0.5.0 (sin bump por commit) · rama `feature/frontend-modular`
> Fuentes: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§2, §4 fase 1) y
> `docs/prototipos/fase1-layouts-macros/traspaso.md` (sección "Estado" — verificación del prototipo
> descartado — y "Siguiente etapa").

## Contexto

El frontend (Jinja2 + HTMX, sin build) no usa herencia de plantillas (0 `{% extends %}`, 5 plantillas con
`<head>` propio) y el esqueleto de modal (overlay/container/header/cierre) está copiado a mano en 18
ficheros. Los 4 `profit_items_*_edit_form.html` solo difieren en URL, título y editabilidad. La fase 1
crea la base de componentes sobre la que se apoyan las fases 2-5, **sin cambiar el HTML renderizado**.

Un prototipo de cc_local (descartado) intentó esta fase y falló en 3 puntos que este PAE trata como
requisitos explícitos: (1) macro `modal()` con dict + HTML concatenado → el modal "Editar cliente" mostraba
el HTML como texto (y el "arreglo" obvio, `|safe`, reabre XSS); (2) `base.html` metía htmx/Chart.js/
JsBarcode/app.css/app.js en el `<head>` común → las impresiones empezaban a cargarlos; (3) alcance real
muy inferior al pedido (1/18 modales, 3/5 `extends`, `forms.html` sin consumidores, 4 ficheros huérfanos)
y un arnés de snapshots que comparaba 404 contra 404 en las rutas de impresión.

## 1.1 Functional Requirements

### REQ-1: Arnés de snapshots determinista como red de seguridad
**Priority:** Must Have
WHEN se ejecuta `tests/snapshots/ui_snapshot.py check <dir>`, THE SYSTEM SHALL renderizar las 40 páginas
de referencia (login, dashboard, 17 pestañas, 18 modales, 3 impresiones) contra un SQLite temporal con
datos fijos, normalizarlas a tokens canónicos y compararlas con la baseline, fallando ante cualquier
diferencia o ante una respuesta distinta de 200.

```gherkin
Scenario: Dos ejecuciones sobre el mismo código son idénticas
  GIVEN el código sin cambios
  WHEN se ejecuta "write /tmp/a" y después "check /tmp/a"
  THEN la salida es "OK: 40 snapshots equivalentes" y el exit code es 0

Scenario: Una ruta que no devuelve 200 aborta el arnés
  GIVEN una URL de la lista que responde 404
  WHEN se ejecuta "write" o "check"
  THEN el arnés termina con error indicando la clave, la URL y el status
  AND nunca compara dos respuestas de error entre sí

Scenario: Las impresiones se capturan desde sus rutas reales
  GIVEN un traspaso, una compra y stock sembrados
  WHEN se captura print_transfer, print_restock y print_label
  THEN las URLs son /ui/transfers/print?movement_id=<id>, /ui/restock-print y /ui/purchase/<id>/label
  AND cada snapshot contiene el documento HTML completo (no un JSON de error)

Scenario: Fechas y horas variables no producen falsos positivos
  GIVEN valores por defecto que dependen de "ahora" (fecha, hora, mes)
  WHEN se compara con una baseline capturada en otro minuto
  THEN fecha, hora, fecha-hora, año-mes y nombres de mes se normalizan y no generan diferencias
```

### REQ-2: Baseline commiteada antes de tocar plantillas
**Priority:** Must Have
WHEN arranca la implementación, THE SYSTEM SHALL generar `tests/snapshots/baseline/` (40 ficheros) desde
el código **sin modificar** y commitearla, de modo que cada envelope posterior verifique equivalencia
contra ella.

```gherkin
Scenario: La baseline refleja el código previo al refactor
  GIVEN app/ sin cambios respecto a la rama base
  WHEN se genera la baseline
  THEN existen 40 ficheros .txt en tests/snapshots/baseline/
  AND modal_customer.txt contiene "modal-overlay"
```

### REQ-3: `base.html` y layouts sin assets heredados
**Priority:** Must Have
THE SYSTEM SHALL proveer `base.html` (doctype, `<head>` con meta/título, bloques `title`, `head`, `body`)
sin ningún asset, `layouts/app_shell.html` (tema + htmx, Chart.js, JsBarcode, app.css, app.js) y
`layouts/print.html` (sin assets de la app).

```gherkin
Scenario: El layout de impresión no carga assets de la app
  WHEN se renderiza base.html o layouts/print.html
  THEN el HTML no contiene htmx.min.js, chart.umd.min.js, JsBarcode.all.min.js, app.css ni app.js

Scenario: El layout de la app carga sus 5 assets
  WHEN se renderiza layouts/app_shell.html
  THEN el HTML contiene los 5 assets
```

### REQ-4: Las 5 plantillas con `<head>` propio usan `{% extends %}`
**Priority:** Must Have
THE SYSTEM SHALL migrar `dashboard.html` (→ `layouts/app_shell.html`), `login.html` (→ `base.html`),
`transfer_print.html`, `restock_print.html` y `label_single_print.html` (→ `layouts/print.html`) sin
cambiar su HTML renderizado.

```gherkin
Scenario: Las impresiones no empiezan a cargar scripts
  GIVEN la baseline de print_restock (solo <style> en el <head>)
  WHEN se renderiza /ui/restock-print tras la migración
  THEN el snapshot es idéntico a la baseline (sin htmx/Chart.js/JsBarcode/app.css/app.js)

Scenario: La etiqueta conserva solo JsBarcode
  WHEN se renderiza /ui/purchase/<id>/label
  THEN su <head> contiene JsBarcode.all.min.js y ningún otro asset de la app
```

### REQ-5: Macro `modal()` con `caller()`
**Priority:** Must Have
THE SYSTEM SHALL proveer `{% macro modal(id, title, wide=False, heading='h3', static=False,
close_label=None, container_style=None) %}` en `components/modal.html`, cuyo cuerpo se pasa con
`{% call %}` (`caller()`), con `title` autoescapado, sin `|safe` ni HTML construido como string.

```gherkin
Scenario: El cuerpo del modal es HTML vivo y los datos se escapan
  GIVEN un cliente con nombre 'Cliente <b>E2E</b>'
  WHEN el admin abre "Editar cliente"
  THEN el modal muestra inputs reales (no texto "<input" ni "&lt;")
  AND el input "name" tiene el valor literal 'Cliente <b>E2E</b>'

Scenario: Modal estático sin botón de cierre
  WHEN se usa modal(..., static=True)
  THEN el overlay lleva data-modal-static="1" y no hay botón .modal-close
```

### REQ-6: Macro `field()` con consumidores reales
**Priority:** Must Have
THE SYSTEM SHALL proveer `{% macro field(label, name=None, type=None, value=None, id=None,
required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` en `components/forms.html`
y usarla en las filas `<div class="row"><label>…</label><input …/></div>` de los modales migrados.
`value=None` omite el atributo; `''` lo emite vacío.

```gherkin
Scenario: field escapa el valor
  WHEN se renderiza field('Nombre', 'name', value='"><script>x</script>')
  THEN el HTML no contiene "<script>" y contiene el valor escapado
```

### REQ-7: Los 18 modales usan la macro
**Priority:** Must Have
THE SYSTEM SHALL migrar al macro `modal()` los 18 modales (`business_edit_form`, `customer_edit_form`,
`expense_edit_form`, `extraction_edit_form`, `must_change_password_form`, `product_edit_form`,
`product_edit_form_inventory`, `purchase_edit_form`, `sale_edit_form`, `sales_document_edit_form`,
`transfer_bulk_modal`, `transfer_edit_form`, `user_edit_form`, `user_reset_password_form` y el unificado
de REQ-8 que sustituye a los 4 `profit_items_*`), conservando ids, clases y `data-*`.

```gherkin
Scenario: Ningún esqueleto de modal queda escrito a mano
  WHEN se busca 'class="modal-overlay"' en app/templates/partials/
  THEN no aparece en ningún fichero (solo en components/modal.html)

Scenario: HTML equivalente
  WHEN se ejecuta el arnés "check tests/snapshots/baseline"
  THEN "OK: 40 snapshots equivalentes"
```

### REQ-8: Un único modal de edición en Utilidades por artículo
**Priority:** Must Have
THE SYSTEM SHALL sustituir los 4 `profit_items_{sale,purchase,adjustment,transfer_in}_edit_form.html` por
`partials/profit_items_edit_form.html` parametrizado por `kind` (lo pasa el router) y **borrar** los 4
ficheros.

```gherkin
Scenario: El admin edita desde Utilidades por artículo
  GIVEN una venta de E2E-SKU1 con lote de compra E2E-SKU1-E2E
  WHEN abre "Edit sale" y luego "Edit buys"
  THEN ve #profit-items-sale-edit-modal con el artículo y el precio editable
  AND ve #profit-items-purchase-edit-modal con el lote "E2E-SKU1-E2E"

Scenario: Los ficheros antiguos ya no existen
  WHEN se pide la plantilla partials/profit_items_sale_edit_form.html
  THEN Jinja2 lanza TemplateNotFound
```

### REQ-9: Documentación del release
**Priority:** Should Have
THE SYSTEM SHALL añadir la entrada de la fase 1 a `CHANGELOG.md` bajo `## 0.5.0 (sin publicar …)` sin
tocar `VERSION` ni `.bumpversion.cfg`.

## 1.2 MoSCoW

| Priority | REQs |
|---|---|
| Must Have | REQ-1..REQ-8 |
| Should Have | REQ-9 |
| Could Have | — |
| Won't Have (esta fase) | ver 1.4 |

## 1.3 Non-Functional Requirements

| Category | Answer |
|---|---|
| **Performance** | Neutro. Jinja2 compila y cachea las plantillas; una macro y un `extends` más por render no son medibles. Las impresiones no cargan JS nuevo (REQ-4). |
| **Security** | Sin nuevas fronteras de auth ni datos. Riesgo principal: XSS al pasar contenido a macros → todo valor se autoescapa, el cuerpo va por `caller()`, prohibido `|safe` en las macros (test de REQ-5/6). |
| **Scalability** | No aplica — cambio de plantillas, sin datos ni consultas nuevas. |
| **Observability** | No aplica — sin rutas ni lógica nueva; el arnés informa la clave/URL de cada fallo. |
| **Reliability** | El HTML renderizado de las 40 páginas debe ser equivalente (arnés). Los 44 e2e existentes siguen verdes (dependen de ids como `#tab-content`, `#sale-edit-modal`). |
| **Coste** | Neutro (sin dependencias ni infraestructura nueva). |

## 1.4 Out of Scope

- Filas `<div class="row">` fuera de los 18 modales (paneles y pestañas, ~50): fase 3/5.
- Filas de modal que no son `label + input` simples (selects, grupos de checkboxes, `<div>` con imagen): siguen a mano.
- Sacar `<style>`/`<script>` inline de login e impresiones, `onclick=`, CSS a tokens: fases 2-3.
- `aria-label="Cerrar"` en los botones de cierre de los 17 modales que hoy no lo tienen (cambiaría el HTML; fase 3, a11y).
- Sustituir `container_style='max-width: 760px;'` de `transfer_bulk_modal` por una clase CSS: fase 3.
- `labels_print.html` (fichero vacío, 0 líneas, sin ninguna ruta que lo renderice): no se toca; candidato a borrar en fase 5.
- `layouts/print.html` con `print.css` compartido: fase 3 (hoy cada impresión conserva su `<style>`).
- Reorganizar `partials/` en `features/<dominio>/`: fase 5.
- Bump de versión: el release 0.5.0 se bumpea una sola vez al cerrar la fase 5.

## 1.5 Dependencies

- Fase 0 (`bc07d88`) ya en la rama. Ninguna dependencia externa nueva (stdlib `html.parser`, FastAPI `TestClient`, ya disponibles).
- `CLAUDE.md`, `.claude/CLAUDE.md`, `.idea/`, `.serena/` y `specs/copiloto-ventas-ubicacion-mensual/` tienen cambios ajenos sin commitear: ningún commit de este PAE los incluye.

## 1.6 Open Questions

Ninguna bloqueante. Las decisiones que se apartan del texto literal del traspaso (arnés del lado servidor
en vez de Playwright; `close_label=None` por defecto) están justificadas en `design.md` D2 y D4.
All requirements are unambiguous. Proceeding to Phase 2.
