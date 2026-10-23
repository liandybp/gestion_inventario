# Diagnóstico de arquitectura del frontend — 2026-10-23

**Versión auditada:** 0.4.15 (`develop`, `0eebbec`, 2026-10-19)
**Alcance:** `app/templates/` (70 plantillas Jinja2), `app/static/css/app.css` (1052 líneas),
`app/static/js/app.js` (767 líneas), y los routers UI en la medida en que afectan al front.
**Stack:** FastAPI + Jinja2 + HTMX, CSS/JS vanilla sin build, Chart.js y JsBarcode servidos en local.

**Veredicto:** el stack es el adecuado para la app; el problema es la **modularización a medio
hacer**. Existen 7 macros en `components/ui.html` pero el markup, el CSS y el JS se copian en cada sitio.

## 0. Hallazgo urgente — XSS almacenado en Inicio (corregido, entra en 0.5.0)

`partials/home_charts.html:30,34,78` volcaba `{{ ..._json | safe }}` dentro de
`<script type="application/json">`, con el JSON generado por `json.dumps` en `routers/ui_home.py`
(`_home_charts_context`), que no escapa `</`. Las etiquetas del pie son `SKU - nombre de producto`:
un producto llamado `</script><img src=x onerror=…>` cerraba el bloque y ejecutaba código al abrir
Inicio (el CSP permite `'unsafe-inline'`). Los nombres pueden llegar desde la importación de facturas PDF.

**Fix:** el router pasa dicts y la plantilla usa `| tojson` (escapa `< > & '`). Era el único `|safe`
del proyecto. Regresión: `tests/unit/test_ui_xss_escapes.py::test_home_charts_json_cannot_break_out_of_script_tag`
(falla con el código anterior, pasa con el fix).

## 1. Lo que no debería estar donde está

| Problema | Medido | Ejemplos |
|---|---|---|
| CSS inline (`style="..."`) | 106 atributos en 21 plantillas | `tab_transfers.html` (29), `tab_history.html` (10), `purchase_panel.html` (10) |
| Colores hex en HTML | 47 | `border:1px solid #e5e7eb` en miniaturas → **rompe el tema oscuro** |
| Bloques `<style>` | 4 | `login.html` y las 3 plantillas de impresión |
| JS en plantillas | 13 `<script>` | `dashboard.html:192-291` (chat del copiloto, tema, ~100 líneas) |
| Handlers inline | 15 `onclick=`/`onchange=` | `onclick="toggleTheme()"`, `onchange="this.form.submit()"` |
| Script Chart.js por widget | `widget_chart.html` | paleta y plugin duplicados en cada render |
| HTML construido en Python | 6 routers | `ui_products.py:572`, `ui_sales_documents.py:500`, `ui_stock.py:356-380` |
| Código muerto / debug en prod | | `debugReturnLots` (`app.js:129`) listener no-op; elementos `#*-debug` en gráficos |

- Las utilidades CSS ya existen (`mt-16`, `mb-12`…) pero se usan `style="margin-top:12px"` repetidos.
- El HTML en Python (respuestas OOB `<input id='sale-unit-price' hx-swap-oob …>`) **duplica markup**
  definido en las plantillas: si cambian clases/atributos en el `.html`, el OOB los pisa sin avisar.
  No hay XSS ahí (todo pasa por `html.escape`), es deuda de arquitectura.
- Mientras haya `onclick=` y `<script>` inline, `script-src` necesita `'unsafe-inline'`, lo que anula
  gran parte del CSP de `main.py:67`.

## 2. Modularización y componentes reutilizables

| Patrón | Hoy | Componente propuesto |
|---|---|---|
| Layout base | **0 `{% extends %}`, 0 `{% block %}`**; 5 plantillas con `<head>` propio | `base.html`, `layouts/app_shell.html`, `layouts/print.html` |
| Modal | esqueleto overlay/container/header/close **copiado en 18 ficheros** | `{% call modal(id, title, wide=False) %}` |
| Fila de formulario | **86 `<div class="row"><label>` a mano**; macro `row` usada 6 veces | `field(name, label, type, value, required, attrs)` |
| Edit forms casi idénticos | 4 `profit_items_*_edit_form.html` solo cambian URL, título y editabilidad | 1 plantilla parametrizada |
| Card | 36 `<div class="card">` manuales vs 13 macro `card` | usar la macro existente |
| Flash | 18 `message_class` manuales vs 11 macro `flash` | usar la macro existente |
| Tablas | 30 `<table>` con wrapper/thead/fila vacía repetidos | `data_table(columns, rows, empty_text)` o `table_wrapper` + `empty_row` |
| Sidebar | 15 botones casi iguales, lógica de roles en plantilla | `NAV_ITEMS` (roles, modos) en Python + `{% for %}` |
| Gráficos | 3 funciones en `app.js` (~250 líneas, polling de `window.Chart`) + `widget_chart.html` con otra paleta | `charts.js` único, paleta desde variables CSS |

`components/sales_doc/` ya sigue el buen patrón; generalizarlo.

### Estructura objetivo (sin build)

```
templates/
├── base.html                  ← <head>, assets, {% block content %}
├── layouts/  app_shell.html · print.html
├── pages/    login.html · dashboard.html
├── components/                ← macros de UI puras, sin lógica de negocio
│   ├── ui.html (card, flash, metric…) · modal.html · forms.html (field, select, submit)
│   └── table.html · nav.html · chart.html
└── features/<dominio>/        ← transfers/, sales/, purchases/, profit_items/…
    ├── tab.html · panel.html · edit_modal.html · _rows.html (fragmentos HTMX)

static/
├── css/  tokens.css (variables + tema) · base.css · layout.css · utilities.css
│         components/*.css (modal, table, form, card, agent) · print.css
└── js/   core.js (config htmx, errores 4xx, modales, actividad)
          components/ (charts.js, barcode.js, product-suggest.js, notes.js)
          features/ (transfers.js, agent-chat.js, theme.js)   ← <script type="module">
```

Principios:
1. **Por dominio, no por tipo de vista** (`partials/` tiene >60 ficheros planos).
2. **Comportamiento por `data-*` + `htmx.onLoad`**: hoy hay 22 listeners globales y `runEnhancers`
   ejecuta 6 inicializadores en cada `afterSwap`. Cada componente se auto-registra (`data-chart`,
   `data-barcode`, `data-product-suggest`); la lógica de traspasos (~140 líneas) va a `features/transfers.js`.
3. **Tokens CSS, cero hex en HTML**: el tema oscuro funciona solo y `toggleTheme` deja de hacer
   `location.reload()` para recolorear los gráficos.
4. **`hx-push-url` en pestañas** en lugar de `localStorage` (`app.js:13-47`): botón atrás y deep-links.
5. **Ningún HTML en Python**: cada respuesta OOB es una plantilla mínima (`features/sales/_price_oob.html`).

## 3. Lado servidor que afecta al front

- Routers UI con queries directas a BD: `ui_transfers.py` (16), `ui_sales.py` (14),
  `ui_profit_items.py` (10), `ui_tabs_common.py` (8). Deberían construir el contexto vía `services/`.
- `user = …; bid = require_active_business_id(db, request)` repetido ~100 veces → dependencia
  `ctx: UIContext = Depends(ui_context)`.
- Routers de >900 líneas (`ui_transfers.py` 1131, `ui_sales_documents.py` 1112, `ui_purchases.py` 924)
  → dividir por subrecurso.

## 4. Plan por fases

| Fase | Contenido | Riesgo |
|---|---|---|
| **0** ✅ | Fix XSS `tojson` + test (commit `252f89e`, release 0.5.0) | Bajo |
| **1** | `base.html` + layouts + macros `modal`/`field`; migrar 18 modales y los 4 `profit_items_*` | Bajo, mecánico |
| **2** | Sacar todo el JS a módulos; quitar `onclick=`/debug; quitar `'unsafe-inline'` de `script-src` | Medio (cubierto por e2e) |
| **3** | `tokens.css` + split de `app.css`; eliminar `style=` y hex; gráficos con paleta CSS | Bajo |
| **4** | Plantillas OOB en vez de HTML en Python; `UIContext`; sidebar data-driven | Medio |
| **5** | Reagrupar `partials/` en `features/<dominio>/`; sacar queries de routers | Medio-alto, por dominio |

**Release:** fases 0-5 = un único release **0.5.0** (decisión del usuario, 2026-10-23): varios commits, ninguno sube versión; `VERSION`/`.bumpversion.cfg`/`CHANGELOG` se bumpean una sola vez al cerrar.

Fases 1-5 tocan >2 archivos → PAE vía `SDD-Agent` con este documento como insumo. Los 44 e2e
dependen de ids como `#tab-content`: el refactor debe preservar ids y `data-*`.

## Contexto y método

- **Serena** (LSP solo Python): lectura de `_home_charts_context` para confirmar el XSS; overview de
  `ui_home.py` y del test de escapes.
- **codebase-memory** (índice de `staging/gestion_inventario`, 2026-10-22, mismo commit): grafo de
  funciones de `app.js` (31 funciones, callers) y cobertura; las plantillas HTML solo tienen
  cobertura parcial (`parse_partial`).
- **Fallback grep/sed** (fuera de cobertura): conteo de antipatrones en HTML/CSS y de queries en routers.
- Verificación del fix: 387 unit + 44 e2e en verde; el test nuevo falla con el código anterior.
