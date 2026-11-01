# Design — Frontend modular, fase 3: CSS sin estilos inline, tokens y paleta, CSP `style-src 'self'`

> SDD Phase 2 · PAE-018 · Fuente: `requirements.md` (REQ-1..10). Base `b0f5dd8`, rama `feature/frontend-modular`.
> **Sin prototipo ni worktree de validación** (ADR-020 y regla del paso 2 del loop, 2026-10-24): el diseño sale del
> diagnóstico y del código real. La red que ve lo que los tests no ven (estilos calculados) es el **primer lote**.

## 2.1 Codebase grounding

| Fichero | Acción | Rol |
|---|---|---|
| `tests/snapshots/style_snapshot.py` | CREATE | Red de estilos calculados (Playwright): baseline desde `b0f5dd8`, `check` al final |
| `tests/snapshots/style_allowlist.txt` | CREATE | Diferencias intencionadas (REQ-6) |
| `tests/snapshots/ui_snapshot.py` | MODIFY | Canónico: `style` → clases equivalentes, ignora `<style>`/`<link>`/meta `htmx-config` |
| `tests/snapshots/baseline/*.txt` | MODIFY | Regenerada desde `b0f5dd8` con el canónico nuevo |
| `tests/e2e/conftest.py` | MODIFY | `js_errors`: falla ante `Content Security Policy` |
| `app/static/css/app.css` | MODIFY | Tokens (badge, gráficos) + bloque final «Fase 3» con las clases que sustituyen a los `style=` + reglas `.htmx-indicator` |
| `app/static/css/login.css`, `print.css`, `label-print.css` | CREATE | Contenido literal de los 4 `<style>` + clases que usan esas páginas |
| 21 plantillas con `style=` (lista en §2.3) | MODIFY | `style="…"` → clases (tabla §2.3) |
| `partials/stock_table.html` | MODIFY | Quitar `class="mb-10"` muerta + `style` |
| `components/ui.html`, `components/modal.html`, `partials/transfer_bulk_modal.html` | MODIFY | Sin parámetros de estilo libre (`container_style` → `container_class`) |
| `login.html`, `transfer_print.html`, `restock_print.html`, `label_single_print.html` | MODIFY | `<style>` → `<link rel="stylesheet">` |
| `layouts/app_shell.html` | MODIFY | `<meta name="htmx-config">` antes de htmx |
| `app/static/js/components/barcode-svg.js` | CREATE | Dibujo del código de barras sin `style` |
| `app/static/js/components/barcode.js`, `pages/label-print.js` | MODIFY | Usan `renderBarcodeSvg` |
| `app/static/js/components/chart-palette.js` | CREATE | `cssVar`, `cssList`, `THEME_EVENT` |
| `components/widget-chart.js`, `components/home-charts.js` | MODIFY | Colores desde CSS; redibujo en `gi:themechange` |
| `features/theme.js` | MODIFY | Sin `location.reload()`; emite `gi:themechange` |
| `app/main.py` | MODIFY | `style-src 'self'` |
| `tests/unit/test_frontend_css.py` | CREATE | Guardas estáticas |
| `tests/e2e/test_css_csp_e2e.py` | CREATE | Códigos de barras, htmx, tokens, guarda |
| `tests/e2e/test_js_behaviors_e2e.py` | MODIFY | `test_theme_toggle_persists_across_reload` → `test_theme_toggle_without_reload` |
| `CHANGELOG.md` | MODIFY | Entrada de la fase 3 en 0.5.0 |
| `app/static/js/JsBarcode.all.min.js`, `htmx.min.js`, `chart.umd.min.js` | REFERENCE | Sin cambios |
| `tests/e2e/test_ui_tour_csp_e2e.py` | REFERENCE | `TABS`, `MODALS`, `login`, `open_tab` (los reutiliza `style_snapshot.py`) |

**2.1b Contexto externo:** ninguno vía MCP. Los datos de librerías salen del código vendorizado (leído en el
diagnóstico) y van literales en los envelopes:
- htmx 1.9.12 lee `<meta name="htmx-config">` al iniciar, **antes** de insertar su hoja de indicador
  (`mergeMetaConfig` → `insertIndicatorStyles`); `attributesToSettle` por defecto `["class","style","width","height"]`.
  La hoja que inyecta: `.htmx-indicator{opacity:0}`, `.htmx-request .htmx-indicator{opacity:1; transition: opacity 200ms ease-in;}`,
  `.htmx-request.htmx-indicator{…igual}`.
- JsBarcode 3.11.6, renderer SVG: `prepareSVG` (ancho = Σ`encoding.width` + márgenes; alto = máx `encoding.height`;
  fondo `rect` con `style="fill:<background>"`), `createGroup` (`transform="translate(x, marginTop)"`), `setGroupOptions`
  (`style="fill:<lineColor>"`), `drawSvgBarcode` (barras por tramos de `1`), `drawSVGText` (`style="font:…"`, `y` y
  `text-anchor` según `textPosition`/`textAlign`), `calculateEncodingAttributes` (ancho = `ceil(max(textWidth, data.length*width))`,
  `barcodePadding` si el texto es más ancho). En modo objeto (`JsBarcode({}, code, opts)`) solo rellena `obj.encodings`.
- Chart.js 4.4.1 dimensiona el `<canvas>` por CSSOM (`el.style.x = …`), que la CSP **sí permite**.

## 2.2 Enfoque

1. **Primero la red** (lote A): `style_snapshot.py` + su baseline desde `b0f5dd8` (worktree temporal con `set -e`, como
   la baseline HTML de la fase 1); el canónico del arnés HTML y su baseline; la guarda `js_errors`. Nada de `app/`.
2. **Después el CSS** (lote B): tokens y clases en `app.css`, las 3 hojas nuevas. Añadir reglas no cambia nada visible
   mientras las plantillas sigan con `style=` (el inline gana). Excepción resuelta: la clase muerta `mb-10` se quita en
   el mismo envelope que define `.mb-10`.
3. **Plantillas** (lotes C-E): cada `style="…"` se sustituye por sus clases; las macros pierden sus parámetros de estilo;
   login e impresiones enlazan su hoja.
4. **JS** (lote F): códigos de barras sin `style`, paleta desde CSS, tema sin recarga.
5. **CSP estricta** (lote G): meta `htmx-config`, `style-src 'self'`, guardas unitarias.
6. **e2e y cierre** (lotes H-I): e2e nuevos, `check` de los dos arneses con la baseline del lote A, CHANGELOG.

Hasta el lote G la CSP sigue permitiendo `'unsafe-inline'`: al final de cada lote la app funciona y la suite pasa.

**Invariantes:** ids y `data-*` intactos (los 72 e2e dependen de ellos); ningún cambio de BD, rutas ni Python de negocio;
`VERSION`/`.bumpversion.cfg` sin tocar; ningún `<script>` inline ni `on*=` (guardas de la fase 2).

## 2.3 Modelo de datos y equivalencias

Sin cambios de BD. El «modelo» de esta fase es la tabla de equivalencias `style` → clases (normalizado: sin espacios
alrededor de `:`/`;` y sin `;` final). La usan las plantillas (lotes C-E) y el canónico del arnés HTML (lote A):

| `style` normalizado | Clases | Regla (bloque final de `app.css` salvo «existe») |
|---|---|---|
| `width:N%` (3,5,6,7,8,9,12,14,15,16,23,62) | `w-Np` | `.w-Np { width: N%; }` |
| `width:Npx` (100,110,120,140,150) / `width:auto` | `w-Npx` / `w-auto` | ídem |
| `margin-top:0` / `6px` / `8px` | `mt-0` / `mt-6` / `mt-8` | nuevas |
| `margin-top:12px` / `14px` / `16px` / `-6px` | `mt-12` / `mt-14` / `mt-16` / `mt-n6` | existen |
| `margin-bottom:6px` / `12px` | `mb-6` / `mb-12` | existen |
| `margin-bottom:14px` | `mb-14` | nueva |
| `margin:0` | `m-0` | existe (+ `.filter-label.m-0`, ver D10) |
| `margin-left:10px` | `ml-10` | nueva |
| `text-decoration:none` | `no-underline` | nueva |
| `margin-top:0;margin-bottom:10px` / `margin-top:-6px;margin-bottom:10px` | `mt-0 mb-10` / `mt-n6 mb-10` | `.mb-10` nueva |
| `margin:8px 0 10px 0` / `margin:6px 0 10px 0` | `my-8-10` / `my-6-10` | nuevas |
| `padding:12px;border-radius:10px` (+`;margin-top:12px`) | `msg-box` (+ `mt-12`) | nueva |
| `padding:10px;border-radius:8px;margin-bottom:10px` | `msg-box-sm mb-10` | nueva |
| `display:flex;gap:8px;align-items:center` y `display:flex;align-items:center;gap:8px` | `flex items-center gap-8` | `.gap-8` nueva |
| `display:flex;gap:12px;align-items:baseline;justify-content:space-between` | `flex gap-12 items-baseline justify-between` | `.items-baseline` nueva |
| `align-items:center` / `gap:12px` | `items-center` / `gap-12` | existen |
| `max-height:48vh;overflow-y:auto` | `scroll-48vh` | nueva |
| `-webkit-overflow-scrolling:touch` | `touch-scroll` | nueva |
| `margin-top:10px;grid-template-columns:auto 1fr` | `mt-10 cols-auto-1fr` | `.cols-auto-1fr` nueva |
| `min-width:85px` / `display:inline-block;min-width:85px` | `min-w-85` / `d-inline-block min-w-85` | nuevas |
| `margin-left:8px;background:var(--brand-red)` | `ml-8 btn-danger` | existen (`.btn-danger` = mismo fondo) |
| miniatura 36 px / 64 px con `border:1px solid #e5e7eb` | `thumb` / `thumb thumb-lg` | nuevas, borde `var(--brand-border)` |
| badge de reposición (`#eef6ff`/`#0b2d42`) | `badge-info` | nueva, tokens `--badge-info-bg/-text` |
| `select` de negocio en `dashboard.html` | `select-business` | nueva |
| `select` de socio en `extraction_edit_form.html` | `select-plain` | nueva |
| `max-width:760px` (`modal(container_style=)`) | `modal-container-760` (`container_class=`) | nueva |

Plantillas (nº de `style=`): `tab_transfers` 29, `tab_history` 10, `purchase_panel` 10, `tab_profit_items` 7,
`sale_panel` 7, `transfer_bulk_modal` 5, `transfer_print` 4, `purchases_history` 4, `login` 4, `dashboard` 4,
`restock_table` 3, `product_edit_form` 3, `stock_table` 1, `sales_history` 2, `products_history` 2,
`product_edit_form_inventory` 2, `action_result` 2, `sales_document_edit_form` 1, `extraction_edit_form` 1,
`customer_detail` 1, `components/ui.html` 1 (`year-filter`) = 103, + 3 en macros = 106.

**Tokens nuevos** (`:root` / `[data-theme="dark"]`): `--badge-info-bg` `#eef6ff` / `rgba(59, 130, 246, 0.15)`,
`--badge-info-text` `#0b2d42` / `#93c5fd`; `--chart-1..8` = `PALETTE_LIGHT` / `PALETTE_DARK` (7) con `--chart-8: initial`
en oscuro; `--chart-accent`, `--chart-accent-fill` (por tema); `--chart-grid`, `--chart-slice-border`,
`--chart-label-stroke`, `--chart-label-fill`, `--home-chart-sales|purchases|profit|line-fill`, `--home-pie-1..11`
(iguales en los dos temas, mismos literales que hoy).

## 2.4 Contratos

Sin endpoints nuevos. Contratos internos:
- `renderBarcodeSvg(svg: SVGSVGElement, code: string, options: object): void` — mismas opciones que `JsBarcode`; lanza
  la misma excepción que JsBarcode si el código no es válido (los llamadores ya la capturan).
- `cssVar(name: string): string`, `cssList(prefix: string): string[]` (de `--<prefix>-1` hasta la primera vacía),
  `THEME_EVENT = 'gi:themechange'`.
- `modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_class=None)`;
  `card(title=None, id=None, extra_class='')`, `subcard(title=None, id=None, extra_class='')`.
- `style_snapshot.py write <dir>` / `check <dir> [--allow <fichero>]` → «OK: …» y exit 0, o «FALLO: …» y exit 1.

## 2.5 Security review

| Check | Respuesta |
|---|---|
| ¿Nuevo límite de auth? | No |
| ¿Datos sensibles nuevos? | No |
| ¿Validación de entrada? | Sin entrada nueva. `renderBarcodeSvg` escribe el código con `createTextNode` (no HTML) |
| ¿Superficie nueva? | Se **reduce**: `style-src 'self'` cierra la inyección de CSS (exfiltración por selectores, *UI redressing*) |
| ¿Secretos? | Ninguno |
| OWASP | A03 (inyección): con CSP estricta un `style=` inyectado ya no se aplica. `'unsafe-hashes'` descartado (ver D4) |

## 2.6 Decisiones (trade-offs)

| # | Decisión | Elegido | Alternativa | Por qué no |
|---|---|---|---|---|
| D1 | Sustituto de `style=` | Clases de utilidad/semánticas en un bloque **al final** de `app.css`, reutilizando las existentes del mismo valor | Una clase por elemento | Duplica; las utilidades ya son el patrón del proyecto (`mt-12`, `gap-12`) |
| D2 | `<style>` de login e impresiones | 3 hojas (`login.css`, `print.css` compartida — los dos bloques son idénticos —, `label-print.css`) | Cargar `app.css` | Cambia la cascada de páginas que hoy no la cargan (decisión de la fase 1) |
| D3 | htmx | Meta `htmx-config` (`includeIndicatorStyles:false`, `attributesToSettle` sin `style`) + reglas del indicador en `app.css` | Nonce para la hoja de htmx | htmx 1.9 no tiene `inlineStyleNonce`; y el *settle* seguiría escribiendo `style` |
| D4 | Códigos de barras | Módulo propio con JsBarcode en modo objeto + atributos de presentación (`fill`, `font-*`) | (a) canvas; (b) parchear JsBarcode; (c) `'unsafe-hashes'` | (a) se imprime borroso; (b) vendor modificado; (c) reabre estilos inline |
| D5 | Paleta | Variables CSS indexadas con los **mismos literales** que hoy | Unificar paletas | Fuera de alcance (cambio visual) |
| D6 | Tema | `theme.js` emite `gi:themechange`; los dos módulos de gráficos redibujan | Mantener `location.reload()` | Perdía el estado de la página (REQ-8) |
| D7 | Red visual | Estilos calculados (`getComputedStyle`, 66 propiedades) por elemento, claves por ruta de etiquetas sin clases | Capturas de pantalla | Ruido de píxeles (Chart.js, antialiasing) y diagnóstico pobre |
| D8 | Arnés HTML | Traducir `style` → clases en el canónico (estricto) | Ignorar `class` | Perdería toda la cobertura de clases |
| D9 | Guarda `js_errors` | Falla ante `Content Security Policy` | `Refused to apply` | No es el texto que usa Chromium para estilos |
| D10 | Trampas de cascada | `.filter-label.m-0 { margin: 0; }`; quitar la `mb-10` muerta de `stock_table.html`; clases de `restock_table` también en `print.css` | Dejar que la red las descubra | Ya se conocen por el diagnóstico: se resuelven en el envelope |
| D11 | Baseline de estilos | En `/tmp/gi-fase3-style-baseline` (≈20 MB, no se versiona); se regenera desde `b0f5dd8` si falta | Versionarla | Tamaño; solo sirve para esta fase |
| D12 | Versión | Sin bump (release 0.5.0 agrupado) | — | Regla del release |

## 2.7 Migración y rollback

Sin BD. Despliegue = código. Rollback = revertir los commits de la fase (un commit por lote); la baseline HTML vuelve
con ellos. Riesgo de caché: las hojas nuevas llevan `?v={{ static_version() }}` (hash de todos los `.css`).

## 2.9 Pre-mortem contra las 10 dimensiones

| Dim | Riesgo | Mitigación (→ envelope) |
|---|---|---|
| 1 SoC | Clases sueltas sin consumidor | Cada clase nueva sale de la tabla §2.3 y la usa al menos una plantilla; `test_frontend_css.py` no lo exige, la revisión sí (→ T8) |
| 2 Acoplamiento | `barcode-svg.js` acoplado a internos de JsBarcode | Solo usa la API pública en modo objeto (`obj.encodings`); la geometría se fija con un e2e que compara contra JsBarcode SVG (→ T30, T37) |
| 3 Capas | JS leyendo colores de CSS | Una sola puerta: `chart-palette.js` (→ T32-T34) |
| 4 Seguridad | Que quede algún `style` (plantillas, Python, librerías, *settle*) | Guardas estáticas + tour con CSP estricta + `js_errors` (→ T4, T35-T37) |
| 5 Robustez | Variable CSS ausente → color vacío | Todas definidas en `:root`; `cssList` corta en la primera vacía; los módulos ya usan `try`/`catch` en el render (→ T9, T32) |
| 6 Consistencia | La baseline de estilos y la rama con datos distintos | Misma siembra (`_seed_stock` + `_seed_tour`), viewport, locale y zona horaria fijos; se genera y compara el mismo día (→ T1-T3) |
| 7 Observabilidad | Violaciones de CSP en producción invisibles | Igual que en la fase 2: fuera de alcance; en desarrollo las cazan los e2e (→ T3) |
| 8 Configuración | Hex fijos nuevos | Colores nuevos solo como tokens; `print.css` (solo tema claro) mantiene sus literales |
| 9 Pruebas | Regresión visual silenciosa (una clase pierde contra otra regla) | Red de estilos calculados en claro y oscuro sobre 74 capturas (→ T1-T3; `check` al cerrar los lotes C, D y E y en T39) |
| 10 Escalabilidad | `static_version()` hashea 3 hojas más (H-406) | Despreciable (3 ficheros pequeños); sin acción |

Riesgos concretos de ejecución:
- **R1** — El *settle* de htmx escribe `style` en los `<canvas>` de Inicio (Chart.js les pone `style` por CSSOM): D3.
- **R2** — `wait_for_function` con expresión suelta usa `eval` → la CSP lo bloquea. Los e2e usan siempre funciones flecha.
- **R3** — La red de estilos tiene dos fuentes de ruido conocidas: ids aleatorios de `notes.js` (`reusable-notes-list-*`,
  se normalizan) y la carrera de Chart.js (se espera a que cada `<canvas>` tenga gráfico).
- **R4** — Que `cc_local` «arregle» a mano una diferencia del `check`: prohibido; si el `check` da FALLO, parar y reportar.
