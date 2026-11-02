# Design — Frontend modular, fase 2: JS a módulos, sin JS inline, CSP sin `'unsafe-inline'`

> SDD Phase 2 · PAE-017 · REQ-1..12 en `requirements.md`

## 0. Contexto y método

- **Serena** (LSP Python): `security_headers_middleware` (`app/main.py`), `_static_asset_version`
  (`app/routers/ui_common.py`), símbolos de `tests/snapshots/ui_snapshot.py` (`_Canon`, `_seed`, `_pages`,
  `_hostile`), modelos `DashboardWidget`/`Business`, `app/agent/widgets.py` (`add_widget`, `widgets_with_payload`).
  Memorias del proyecto (`conventions`, `suggested_commands`).
- **codebase-memory** (reindexado al empezar, rama `feature/frontend-modular`): esquema de `app.js` (31 funciones);
  confirma que las plantillas solo tienen cobertura parcial (18 ficheros `parse_partial`, 4 inservibles, entre ellos
  `tab_transfers.html` y `profit_items_edit_form.html`).
- **Fuera de cobertura, con `grep`/`Read`:** las 70 plantillas Jinja2, `app.js` entero, `htmx.min.js` (semántica de
  `htmx:afterSwap`/OOB), `.gitea/workflows/deploy.yml` y `tests/e2e/conftest.py` (inventario de §0 de requirements).
- **Validación antes de entregar** (loop v3 sin prototipo, como en la fase 1):
  - Resultado completo construido en un worktree desechable (`/tmp/sdd-fase2-wt`, se conserva hasta la revisión):
    **423 unit, 70 e2e (47 existentes + 23 nuevos), arnés OK 40/40**.
  - `tasks.md` generado por script (`gen/`) y simulado sobre una copia limpia de `b7a9881`: `diff -r` **idéntico**.
  - Mutaciones detectadas: 10 por los unit, 6 por el arnés, 8 por los e2e (§8).

## 1. Enfoque

Cuatro bloques, en este orden (cada lote deja la app funcionando):

1. **Arnés antes que nada (lote A).** El canónico pasa a ignorar el cableado JS, que esta fase cambia a propósito.
   La baseline se regenera con el arnés nuevo **sobre el código anterior a la fase** (worktree, `set -euo pipefail`
   y guardas). Desde ahí, cada lote siguiente debe dar `OK: 40`, lo que prueba que el HTML solo cambió en el
   cableado JS.
2. **Módulos nuevos (lotes B-D).** Se crean los 19 ficheros JS. Nadie los carga todavía, así que la app no cambia.
   El código de `app.js` y de los inline se mueve sin cambios de comportamiento, salvo dos cosas: el
   código de depuración y el XSS del tooltip. Va junto con `static_version()` (hash de todo).
3. **Cableado (lotes E-F).** `app_shell` carga `theme-init.js` + `main.js`; se borra `app.js`; cada plantilla
   cambia `on*=`/`<script>` por `data-js-*`/módulo de página. Ningún elemento queda con los dos a la vez.
4. **Cierre (lotes G-I).** CSP `script-src 'self'` + `no-cache`, guardas unitarias, e2e de recorrido y de
   comportamientos, CHANGELOG.

### Estructura resultante (sin build)

```
app/static/js/
├── theme-init.js            clásico, síncrono en <head>: tema antes del primer pintado
├── main.js                  entrada (type="module"): importa e inicializa todo
├── core/      content.js (registro de enhancers) · activity.js · tabs.js · modals.js · swap.js
├── components/ barcode.js · notes.js · product-suggest.js · form-helpers.js · home-charts.js · widget-chart.js
├── features/  transfers.js · agent-chat.js · theme.js
└── pages/     login.js · print.js · label-print.js       (entradas propias, fuera de la app)
```

Invariantes que se conservan: ids y `data-*` existentes; orden de carga (htmx/Chart/JsBarcode síncronos antes del
módulo; los módulos se evalúan antes de `DOMContentLoaded`, igual que el `defer` de `app.js`); orden de enhancers
(barcode → gráficos → notas); las impresiones y el login no cargan htmx/Chart/app (R2 de la fase 1).

## 2. Decisiones

| # | Decisión | Alternativa descartada | Por qué |
|---|---|---|---|
| D1 | Módulos ES nativos, `main.js` llama a `init()` de cada módulo (sin efectos al importar). `pages/*` sí se ejecutan al cargar (son una página cada uno). | Scripts clásicos `defer` con un namespace `window.GI` | Es lo que pedía el diagnóstico (§2), sin build y sin globales. `init()` explícito deja el orden visible en un solo sitio. |
| D2 | `theme-init.js` **clásico y síncrono** | Módulo | Los módulos se difieren: habría destello de tema claro. |
| D3 | Enganches nuevos con prefijo **`data-js-*`** (`select-all`, `autosubmit`, `mirror-to`, `enter-focus`, `theme-toggle`, `agent-toggle`, `agent-size`, `print`, `widget-chart`, `label-sku/-name`). Los `data-*` existentes no se tocan. | Nombres sueltos (`data-print`…) | Convención legible («esto lo usa JS») y una sola regla en el arnés (`data-js-*`), que no tapa ningún atributo existente. |
| D4 | Se conserva la semántica de enhancers: `htmx:afterSwap` + 2 `requestAnimationFrame` (`core/content.js`), también para los widgets. | `htmx.onLoad` (principio 2 del diagnóstico) | Cambiaría el momento en que se dibujan los gráficos (antes del layout). Verificado en `htmx.min.js` 1.9.12: `afterSwap` se emite sobre **cada** elemento de `settleInfo.elts`, también los OOB, así que los widgets del chat llegan igual. La migración a `onLoad` queda para cuando se toque el comportamiento. |
| D5 | El arnés ignora los `<script>` ejecutables (con o sin `src`), los `on*`, los `data-js-*` y los 3 `#*-debug`. Los `<script type="application/json">` sí cuentan. La siembra gana un widget de gráfico. | Regenerar la baseline desde el código migrado | El usuario lo exige desde el commit previo (H-307). Además, solo así el `check` demuestra que no cambió nada más. Qué scripts carga cada página lo fija un unit test (`EXPECTED_SCRIPTS`). |
| D6 | `Cache-Control: no-cache` en `/static/js/` + `static_version()` con el hash de todos los `.css`/`.js` (ruta incluida) | Import maps con `?v=` | Un import map inline necesita nonce o hash en el CSP. `no-cache` revalida con ETag (304) y es lo más simple en LAN. |
| D7 | El widget pasa su especificación en `data-js-widget-chart='{{ … \| tojson }}'` con **comillas simples** | `<script type="application/json">` por widget | En un atributo, el arnés lo ignora (D5). `tojson` escapa `< > & '` pero no `"`: la misma clase de bug que el hotfix 0.4.15. Hay un unit test que lo fija (falla con comillas dobles). |
| D8 | SKU y nombre de la etiqueta en `data-js-label-*` (autoescape) | `tojson` en un `<script>` | No queda JS inline posible. |
| D9 | Tooltip del pastel con `textContent` y `style` por CSSOM | Escapar a mano en el template string | Elimina la clase de bug. CSSOM no depende de `style-src`. |
| D10 | Guarda e2e = `pageerror` + `console.error` (Chromium informa ahí de las violaciones de CSP y de los módulos 404). Las esperas usan `wait_for_function` en forma de función. | Listener `securitypolicyviolation` por init script | La consola ya lo cubre (verificado por mutación). Con `script-src 'self'`, `wait_for_function("expresión")` hace `eval` al sondear y el CSP lo bloquea. |
| D11 | Siembra e2e propia (`_seed_tour` en `conftest.py`, con `InventoryService`): una venta por origen de lote (compra, traspaso, ajuste), producto sin movimientos (`skus`), widget, usuario `mustchange`. El chat del copiloto se intercepta con `page.route` y se responde con la plantilla real. | Reutilizar la siembra del arnés | El arnés fija variables de entorno al importarse y usa fechas fijas. El e2e necesita fechas de hoy (filtros por mes) y un servidor real. |

## 3. Datos y API

- **Modelo de datos:** sin cambios. **Migraciones:** ninguna.
- **API:** sin endpoints nuevos. Cambian dos cabeceras:
  - `Content-Security-Policy`: `script-src 'self'` (antes `'self' 'unsafe-inline'`); el resto igual.
  - `Cache-Control: no-cache` en `GET /static/js/**` (antes sin cabecera; `StaticFiles` ya da ETag/Last-Modified).

## 4. Seguridad

| Check | Respuesta |
|---|---|
| ¿Nueva frontera de auth? | No. |
| ¿Datos sensibles nuevos? | No. |
| Validación de entrada | Sin entradas nuevas. Los `data-js-*` con datos de usuario van autoescapados (D7, D8) y los lee JS con `JSON.parse`/`dataset`, nunca como HTML. |
| Superficie de ataque | Se reduce: sin `'unsafe-inline'`, un HTML inyectado ya no ejecuta handlers ni scripts. Se cierra el XSS del tooltip (REQ-10). |
| Secretos | Ninguno. |
| OWASP | A03 (XSS): mitigación en profundidad por CSP + `textContent`. `htmx` 1.9 solo usa `eval` en `hx-on`, `js:` y filtros `[…]` de `hx-trigger`: grep = 0 usos. Si se añade uno, la consola lo reporta y lo detecta el e2e que pase por ahí. |

## 5. Migración y rollback

Sin datos. Despliegue normal (CD). Rollback = revertir los commits de la fase (los lotes son independientes por
orden: revertir G devuelve `'unsafe-inline'`, revertir E-F devuelve los inline). Riesgo de caché al desplegar:
cubierto por D6 (el navegador revalida cada módulo).

## 6. Pre-mortem (riesgos → mitigación en el envelope)

| # | Riesgo | Mitigación (texto literal en el envelope) |
|---|---|---|
| R1 | Baseline regenerada sobre el código migrado (incidente real de la fase 1: un `;` en vez de `&&`) | T2: `set -euo pipefail`; `git diff --quiet HEAD -- app/`; existe `app.js` y no existe `main.js` (en el repo y en el worktree); worktree desde `HEAD`; `check` al final |
| R2 | El arnés queda ciego por las reglas nuevas | Reglas acotadas (D5). Mutaciones: id, `hx-target`, `data-modal-close`, texto, JSON de gráfico y `\|safe` fallan; el control da OK. Los scripts por página los fija un unit test |
| R3 | Módulos viejos en caché tras desplegar | D6 + test unitario de `no-cache` y del hash con un módulo anidado (mutación `rglob`→`glob` detectada) |
| R4 | Los widgets del chat (OOB) no se dibujan | D4 verificado en el código de htmx + e2e con `page.route`. La mutación «solo `evt.detail.target`» hace fallar el e2e |
| R5 | Listeners duplicados en un estado intermedio (inline + módulo) | Orden de lotes: los módulos no se cargan hasta E. Cada plantilla cambia `on*`→`data-js-*` en un solo paso. Ningún commit deja un elemento con los dos |
| R6 | `wait_for_function` de Playwright con una expresión de texto hace `eval` y el CSP lo bloquea | D10; comentario literal en el e2e. Los 47 e2e existentes no se ven afectados: su condición (`window.htmx`) ya se cumple a la primera, sin sondeo |
| R7 | Una excepción al evaluar un módulo deja sin JS toda la app | Guarda `js_errors` en todos los e2e nuevos; mutación «throw en `features/theme.js`» detectada |
| R8 | `tojson` en un atributo con comillas dobles (bug 0.4.15) | D7 + `test_widget_chart_spec_survives_hostile_title` (mutación a comillas dobles detectada) |
| R9 | Sin el sondeo de `window.Chart`, un gráfico se dibuja antes de que Chart.js cargue | Chart.js es síncrono en `<head>` y los módulos son diferidos: siempre carga antes. Guarda `if (!window.Chart) return` |
| R10 | Los e2e no arrancan sin `.env` (`OPERATOR_PASSWORD`) en el worktree o en `cc_local` | Las verificaciones de los envelopes llevan `SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test` (como el CI) |
| R11 | cc_local «arregla» un script de edición que no cuadra editando a mano | Scripts con `assert count == 1`; la instrucción literal es parar y reportar |
| R12 | Modo desktop (pywebview) sin soporte de módulos | WebView2/WebKitGTK actuales soportan módulos ES. Queda en la lista de prueba manual del paso 8 (no hay e2e de desktop) |

### Pre-mortem contra las 10 dimensiones de `auditoria-ingenieria`

| Dimensión | Pregunta → decisión |
|---|---|
| **1. SoC/SRP** | Cada módulo tiene un consumidor real: `main.js` los importa todos y un unit test falla si alguno queda huérfano (código muerto desde el día cero, el fallo del prototipo de la fase 1 con `forms.html`). `app.js` se borra en el mismo PAE. |
| **2. Cohesión/acoplamiento** | Un solo mecanismo de enhancers (`core/content.js`) en vez de `runEnhancers` con 6 llamadas fijas; los gráficos guardan sus instancias en el módulo (sin `window.__monthly*`). Delegación en `document`, sin acoplarse a ids de partials concretos salvo los que ya existían. |
| **3. Capas/límites** | Dependencias en un solo sentido: `main` → `core`/`components`/`features`; `components` → `core/content`; nada importa `main`. Las páginas sueltas no importan la app. |
| **4. Seguridad** | §4. CSP estricto + XSS del tooltip + `data-js-*` autoescapados con tests de escape. |
| **5. Robustez** | Mismo patrón en todos los handlers: guarda temprana si falta el elemento (`if (!x) return`), `try/catch` donde el original lo tenía (localStorage, JSON, Chart.destroy, htmx.ajax). Sin `console.*`: los errores inesperados afloran como `pageerror` en e2e. |
| **6. Consistencia** | No aplica en servidor (sin cifras ni transacciones). En el cliente: el `?v=` y el `no-cache` cubren el mismo conjunto (`static/js/**`). |
| **7. Observabilidad** | No aplica en servidor. La guarda `js_errors` es la observabilidad del JS en pruebas. |
| **8. Configuración** | Sin variables nuevas. La CSP sigue en un solo sitio (`main.py`). |
| **9. Pruebas** | +9 unit (estáticas, en la suite por defecto), +23 e2e (recorrido + comportamientos), arnés en cada lote; todo validado por mutación (§8). |
| **10. Escalabilidad** | 19 peticiones pequeñas con revalidación 304 en LAN; sin efecto en servidor. Dirección del coste: neutra. |

## 7. Plan de lotes

| Lote | Envelopes | Estado al terminar |
|---|---|---|
| A | T1-T2 | Arnés nuevo + baseline desde `b7a9881`; `OK: 40` |
| B | T3-T7 | Módulos núcleo y componentes de formulario (sin cargar) |
| C | T8-T10 | Gráficos, widget y traspasos (sin cargar) |
| D | T11-T14 | Chat/tema, `main.js`, páginas; `static_version()` |
| E | T15-T19 | `app_shell` carga módulos, `app.js` borrado, dashboard/login/impresiones sin inline |
| F | T20-T24 | Partials sin `on*`; 0 handlers inline en todo `app/templates` |
| G | T25-T26 | CSP `script-src 'self'`, `no-cache`, guardas unitarias |
| H | T27-T28 | Fixtures + e2e de recorrido |
| I | T29-T31 | e2e de comportamientos + CHANGELOG |

## 8. Validación por mutación (ejecutada en una copia del worktree)

| Capa | Mutación | Resultado |
|---|---|---|
| unit | `onclick` en una plantilla · `<script>` inline en login · `'unsafe-inline'` de vuelta · sin `no-cache` · import roto · módulo huérfano · `console.log('debug')` · widget con comillas dobles · hash solo de la raíz · impresión cargando `main.js` | 10/10 fallan |
| arnés | id cambiado · `hx-target` · `data-modal-close` · texto de impresión · id del JSON de gráfico · `\|safe` en clientes | 6/6 fallan; control OK |
| e2e | `<script>` inline en dashboard (CSP) · tooltip con `innerHTML` · «seleccionar todo» roto (6/6 tablas) · `throw` en un módulo · Enter→cantidad quitado · cierre de modal quitado · `onclick` reintroducido · enhancer solo con el target principal (widget OOB) | 8/8 fallan |

Lección registrada durante la validación: la primera versión del diseño suponía que htmx no emite `afterSwap`
en los swaps OOB, y por eso usaba `htmx.onLoad` para los widgets. La mutación demostró que la hipótesis era falsa
(el e2e pasaba igual) y el diseño se corrigió a D4. Se cambió la mutación por la que sí rompe ese camino.
