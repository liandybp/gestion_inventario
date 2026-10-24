# Requirements — Frontend modular, fase 2: JS a módulos, sin JS inline, CSP sin `'unsafe-inline'`

> SDD Phase 1 · PAE-017 · Release 0.5.0 (fases 0-5, un solo release; **ningún commit sube versión**)
> Rama `feature/frontend-modular` (base `b7a9881`). Fuentes: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`
> (§1, §2 y §4 fase 2) y `docs/prototipos/fase1-layouts-macros/traspaso.md` (resultado y lecciones de la fase 1).

## Contexto

Inventario medido sobre `b7a9881` (grep/Read, ver design.md §0):

| Qué | Dónde | Cantidad |
|---|---|---|
| `<script>` inline ejecutables | `layouts/app_shell.html` (tema), `dashboard.html` (chat del copiloto + tema, ~100 líneas), `login.html`, `label_single_print.html`, `partials/widget_chart.html` (llega por htmx) | 5 |
| Handlers inline `on*=` | `dashboard.html` (5), `restock_print`/`transfer_print` (2), 6 casillas «seleccionar todo», `sale_panel` (`onkeydown`), `cart_add` (`onchange`) | 15 |
| JS en un solo fichero | `app/static/js/app.js`, 767 líneas, 31 funciones en un IIFE | 1 |
| Código de depuración | `debugReturnLots` (listener no-op), `setDebug` + 3 `<div id="…-debug">`, sondeo de `window.Chart` con reintentos | — |
| CSP | `main.py`: `script-src 'self' 'unsafe-inline'` | — |

Hallazgo nuevo durante el análisis: **XSS DOM en el tooltip del pastel de Inicio** (`app.js:539`). La etiqueta
`SKU - nombre de producto` se metía en `innerHTML` sin escapar y se ejecutaba al pasar el ratón. Con
`'unsafe-inline'`, un `onerror=` en el nombre de un producto (importable desde facturas PDF) se ejecutaba.

## 1.1 Requisitos funcionales

### REQ-1: Sin JS inline ejecutable
**Priority:** Must Have
WHEN se renderiza cualquier plantilla (página o partial), THE SYSTEM SHALL no contener `<script>` sin `src`
(salvo `type="application/json"`, que son datos) ni atributos `on*=`. Tampoco en HTML construido en Python.

```gherkin
Scenario: ninguna plantilla tiene handlers inline
  GIVEN las plantillas de app/templates/**
  WHEN se buscan atributos on[a-z]+=
  THEN no hay ninguno
Scenario: los scripts de cada plantilla son exactamente los esperados
  GIVEN layouts/app_shell.html, login.html, las 3 impresiones
  THEN cargan solo theme-init.js, htmx, Chart.js, JsBarcode, main.js (módulo) / su módulo de página
  AND el resto de plantillas no carga ningún script
```

### REQ-2: CSP estricto para scripts
**Priority:** Must Have
WHEN el servidor responde, THE SYSTEM SHALL enviar `script-src 'self'` (sin `'unsafe-inline'` ni `'unsafe-eval'`).
`style-src` conserva `'unsafe-inline'` (fase 3).

```gherkin
Scenario: cabecera CSP
  WHEN GET /health
  THEN Content-Security-Policy contiene "script-src 'self';" y no 'unsafe-inline' en script-src
Scenario: el navegador no registra violaciones
  GIVEN el admin recorre todas las pestañas y modales
  THEN la consola no tiene ningún error de CSP
```

### REQ-3: JS organizado en módulos ES sin build
**Priority:** Must Have
THE SYSTEM SHALL servir el JS como módulos ES nativos: `main.js` (entrada, `type="module"`) que importa
`core/`, `components/`, `features/`; `pages/*.js` para login e impresiones; `theme-init.js` clásico y síncrono
en `<head>`. `app.js` desaparece. Ningún módulo queda sin importar (sin código muerto).

```gherkin
Scenario: grafo de módulos
  GIVEN main.js
  WHEN se siguen sus import relativos
  THEN todos los ficheros existen
  AND todo .js no vendor está alcanzado o es una entrada declarada (theme-init, pages/*)
```

### REQ-4: Comportamiento idéntico
**Priority:** Must Have
WHEN el usuario usa cualquier función que antes dependía de JS inline o de `app.js`, THE SYSTEM SHALL comportarse
igual que antes: tema (sin destello, alternar + recarga), panel del copiloto (abrir/cerrar/tamaño/envío
optimista/Enter/errores 4xx/burbuja solo en «Dashboard Dinámico»), autoenvío del selector de negocio, las 6
casillas «seleccionar todo», Enter → cantidad en Ventas, moneda del documento, botones de imprimir, etiqueta
(copias, CODE128, `auto_print=1`), «mostrar contraseña», pestañas (recordar/restaurar), modales (×, overlay,
`data-modal-static`, cierre tras éxito, `X-Modal-Keep`), notas reutilizables, sugerencias de producto, traspasos
(líneas, stock, modal masivo), códigos de barras, los 3 gráficos de Inicio, widgets de gráfico (también los que
llegan por OOB en el chat), menú móvil, swap de errores 4xx, scroll conservado, cabecera `X-User-Activity`.

```gherkin
Scenario: seleccionar todo (6 tablas)
  GIVEN el admin en <pestaña> con filas seleccionables
  WHEN marca la casilla [data-js-select-all=<name>]
  THEN todas las input[name=<name>] de ese form quedan marcadas, y al desmarcar, desmarcadas
Scenario: widget nuevo del chat
  GIVEN el admin envía un mensaje al copiloto
  WHEN la respuesta trae #agent-grid por hx-swap-oob con un widget de gráfico
  THEN el gráfico se dibuja (Chart.getChart(canvas) existe)
```

### REQ-5: Fuera el código de depuración
**Priority:** Must Have
THE SYSTEM SHALL no contener `debugReturnLots`, `setDebug`, los `<div id="monthly-*-debug">`, `console.*` ni el
sondeo de `window.Chart` (Chart.js se carga síncrono antes que los módulos).

### REQ-6: HTML equivalente salvo el cableado JS; ids y `data-*` intactos
**Priority:** Must Have
WHEN se compara el HTML del servidor antes y después de la fase, THE SYSTEM SHALL diferir solo en el cableado JS
(scripts ejecutables, `on*`, `data-js-*`, los 3 `#*-debug`). Ids y `data-*` existentes se conservan (`app.js` y
los 47 e2e dependen de ellos). La baseline del arnés se regenera **desde el commit anterior a la fase** (worktree,
`set -e`, nunca sobre el código migrado) en el mismo commit que el cambio del arnés; el CI la verifica (H-307).

```gherkin
Scenario: arnés de snapshots
  GIVEN la baseline generada con el arnés nuevo sobre b7a9881
  WHEN se ejecuta `ui_snapshot.py check` tras cada lote
  THEN "OK: 40 snapshots equivalentes"
Scenario: el arnés sigue viendo cambios reales
  WHEN se cambia un id, un hx-target, un data-modal-close, un texto o se cuela un |safe
  THEN el check falla
```

### REQ-7: Caché coherente
**Priority:** Must Have
THE SYSTEM SHALL servir `/static/js/**` con `Cache-Control: no-cache`: los módulos que importa `main.js` no
llevan `?v=`. Además, `static_version()` debe calcular el hash de todos los `.css`/`.js` (también los anidados).
Un despliegue nunca mezcla módulos viejos y nuevos.

### REQ-8: e2e de recorrido completo
**Priority:** Must Have
THE SYSTEM SHALL tener e2e (Playwright) que recorran las 17 pestañas del admin, las de operator y owner, los 18
modales (abrir y cerrar con ×; el de cambio de contraseña obligatorio, estático), el login y las 3 impresiones.

### REQ-9: e2e que fallan ante errores de JS o CSP, validados por mutación
**Priority:** Must Have
WHEN durante un e2e hay una excepción no capturada, un `console.error` o una violación de CSP, THE SYSTEM SHALL
hacer fallar el test. La guarda se valida por mutación: reintroducir un `<script>` inline, un `onclick`, romper un
listener o hacer que un módulo lance una excepción hace fallar al menos un e2e.

### REQ-10: Tooltip del pastel sin XSS
**Priority:** Must Have
WHEN el usuario pasa el ratón por el pastel de Inicio, THE SYSTEM SHALL mostrar el nombre del producto como texto
(`textContent`), nunca como HTML.

```gherkin
Scenario: nombre hostil
  GIVEN un producto "Tour <b>uno</b> & 'co'" con ventas este mes
  WHEN se activa su porción del pastel
  THEN el tooltip contiene el texto literal y ningún elemento <b>
```

### REQ-11: Guardas estáticas
**Priority:** Should Have
THE SYSTEM SHALL tener unit tests que fijen REQ-1, 2, 3, 5 y 7 sin navegador (rápidos, en la suite por defecto).

### REQ-12: Sin cambio de versión
**Priority:** Must Have
Ningún commit toca `VERSION` ni `.bumpversion.cfg`. `CHANGELOG.md` acumula la entrada bajo `## 0.5.0 (sin publicar …)`.

## 1.2 MoSCoW

| Prioridad | REQs |
|---|---|
| Must | REQ-1..10, REQ-12 |
| Should | REQ-11 |
| Could | — |
| Won't (esta fase) | ver 1.4 |

## 1.3 NFRs

| Categoría | Respuesta |
|---|---|
| Performance | Sin build ni dependencias nuevas. 19 módulos pequeños por HTTP/1.1 en LAN: coste despreciable; `no-cache` → 304 sin cuerpo. `static_version()` sigue leyendo los assets por render (igual que antes, ~400 KB). |
| Security | Mejora: se cierra el XSS del tooltip y `script-src` deja de permitir inline, así que un `<img onerror>` o un `<script>` colado en una plantilla ya no se ejecuta. Sin datos sensibles nuevos. |
| Scalability | No aplica — cambio de frontend sin efecto en volumen. |
| Observability | No aplica en servidor. En el navegador, los e2e convierten cualquier `console.error` en fallo. |
| Reliability | Un error al evaluar un módulo deja sin JS toda la app (igual que hoy con el IIFE de `app.js`). Mitigación: cada lote verificado, la guarda `js_errors` y la prueba de mutación «throw en un módulo». |
| Compatibilidad | Módulos ES: Chromium/WebKit/Firefox actuales y el webview de pywebview (modo desktop: WebView2 / WebKitGTK). |

## 1.4 Fuera de alcance

- `style-src 'unsafe-inline'`, los 106 `style=`, los colores hex y `tokens.css` → **fase 3**.
- Tema sin `location.reload()` (paleta de gráficos desde CSS) → fase 3.
- `hx-push-url` en pestañas en vez de `localStorage` → posterior (cambia comportamiento).
- Migrar los enhancers a `htmx.onLoad` → no: se conserva la semántica actual (`afterSwap` + 2 frames).
- HTML construido en Python (respuestas OOB) → fase 4. Reagrupar `partials/` → fase 5.
- CSP con nonces/hashes → innecesario: no queda JS inline.
- Deuda detectada y no tocada: `partials/products_history.html` no la incluye nadie (único botón del modal de
  edición de producto), `labels_print.html` vacío, `tests/e2e/test_business_no_pos_e2e.py` sin tests.

## 1.5 Dependencias

- htmx 1.9.12, Chart.js (UMD), JsBarcode, servidos localmente (`app/static/js/*.min.js`): sin cambios.
- Playwright + pytest-playwright (ya en CI). `node` solo para `node --check` en las verificaciones (ya instalado).
- Arnés `tests/snapshots/ui_snapshot.py` (fase 1) y paso `UI snapshots` del CI (H-307).
