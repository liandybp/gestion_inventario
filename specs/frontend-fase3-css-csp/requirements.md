# Requirements — Frontend modular, fase 3: CSS sin estilos inline, tokens y paleta, CSP `style-src 'self'`

> SDD Phase 1 · PAE-018 · Release 0.5.0 (fases 0-5, un solo release; **ningún commit sube versión**)
> Rama `feature/frontend-modular` (base `b0f5dd8`). Fuentes: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`
> (§1, §2 principio 3 y §4 fase 3), `docs/auditorias/2026-10-23-frontend-fase2-js-modulos.md` (H-407) y el diagnóstico
> de la fase 3 hecho el 2026-10-24 sobre `b0f5dd8` (tabla de abajo).
>
> **Alcance aprobado por el usuario (2026-10-24):** CSP `style-src 'self'` + tokens CSS sin hex en el HTML + paleta de
> los gráficos en CSS con cambio de tema sin recarga. **No** se trocea `app.css` (queda para otra fase si se decide).

## Contexto

Inventario medido sobre `b0f5dd8` (grep/Read):

| Qué | Dónde | Cantidad |
|---|---|---|
| Atributos `style="…"` | 22 plantillas (`tab_transfers` 29, `tab_history` 10, `purchase_panel` 10, …) | 106 |
| Parámetros de estilo en macros | `components/ui.html` `card(style=)`/`subcard(style=)` (sin ningún llamador), `components/modal.html` `modal(container_style=)` (1 llamador: `transfer_bulk_modal.html`) | 3 |
| Bloques `<style>` | `login.html`, `transfer_print.html`, `restock_print.html` (idénticos entre sí), `label_single_print.html` | 4 |
| Colores hex en `style=` | 5 miniaturas `border:1px solid #e5e7eb` (rompe el tema oscuro) + badge de reposición `#eef6ff`/`#0b2d42` | 6 |
| `<style>` inyectado por librerías | htmx 1.9.12 (`includeIndicatorStyles`, H-407) | 1 |
| `setAttribute("style")` de htmx | *settle* de htmx 1.9.12: copia del elemento viejo al nuevo con el mismo id los atributos de `attributesToSettle` (`class`, `style`, `width`, `height`); los `<canvas>` de Inicio llevan `style` puesto por Chart.js | 3 por recarga de Inicio |
| `setAttribute("style")` en librerías | **JsBarcode 3.11.6 en modo SVG**: fondo, relleno de barras, fuente del texto y `transform` del `<svg>` | 2 llamadores (`components/barcode.js`, `pages/label-print.js`) |
| HTML con `style=` generado en Python o en JS propio | — | 0 |
| Paletas de gráficos en JS | `components/home-charts.js` (11 rgba + 4 colores de series), `components/widget-chart.js` (`PALETTE_LIGHT` 8 / `PALETTE_DARK` 7 + acentos) | 2 módulos |
| Cambio de tema | `features/theme.js` hace `location.reload()` para recolorear los gráficos | 1 |
| CSP | `main.py`: `style-src 'self' 'unsafe-inline'` | — |

**Hallazgo nuevo (JsBarcode):** con `style-src 'self'` el navegador bloquea los `setAttribute("style", …)` de JsBarcode. El
rectángulo de fondo pierde su `fill:#ffffff` y se pinta con el relleno por defecto de SVG (negro): el código de barras
del historial de compras y el de las etiquetas quedan **negro sobre negro**, y cada render deja errores CSP en consola.

**Hueco de las guardas:** el fixture `js_errors` (`tests/e2e/conftest.py:282`) solo hace fallar el teardown con
`Refused to load the script` / `Refused to execute` / `pageerror`. El aviso de Chromium para un estilo bloqueado
(`Applying inline style violates the following Content Security Policy directive…`) no lo hace fallar. El tour
`test_ui_tour_csp_e2e.py` sí lo detectaría (aserta `js_errors == []`), pero los otros e2e no.

**Hueco del arnés de snapshots:** `tests/snapshots/ui_snapshot.py` compara `style` y `class` tal cual. Esta fase los
cambia en todas las páginas por diseño, así que el arnés HTML no puede demostrar «sin cambio visual»: hace falta una
segunda red que compare **estilos calculados** en el navegador.

**Trampas de cascada conocidas** (un atributo `style` gana a cualquier regla; una clase no):
- `.filter-label` (`app.css`, después de `.m-0`) fija `margin-bottom`: `class="… m-0"` en `sale_panel.html` perdería.
- `partials/stock_table.html` ya usa `class="mb-10"` sin regla CSS (no hace nada): definir `.mb-10` le daría 10 px.
- `restock_print.html` incluye `partials/restock_table.html`: sus clases nuevas tienen que existir también en la hoja
  de impresión, que no carga `app.css`.

## 1.1 Requisitos funcionales

### REQ-1: CSP sin `'unsafe-inline'` en estilos
**Priority:** Must Have
WHEN la app responde a cualquier petición, THE SYSTEM SHALL enviar `style-src 'self'` (sin `'unsafe-inline'`), manteniendo
el resto de directivas exactamente como hoy.

```gherkin
Scenario: cabecera CSP
  WHEN se pide GET /health
  THEN la cabecera Content-Security-Policy contiene "style-src 'self';"
  AND no contiene "'unsafe-inline'"
  AND contiene "script-src 'self';", "default-src 'self';", "object-src 'none';", "frame-ancestors 'none';"
```

### REQ-2: Sin estilos inline en el HTML
**Priority:** Must Have
WHEN se renderiza cualquier plantilla, THE SYSTEM SHALL no contener atributos `style=` ni elementos `<style>`. Las macros
`card`, `subcard` y `modal` no aceptan parámetros de estilo libre. Tampoco el HTML construido en Python.

```gherkin
Scenario: plantillas sin style
  GIVEN los ficheros app/templates/**/*.html
  WHEN se buscan "style=" y "<style"
  THEN no hay ninguna coincidencia
Scenario: macros sin estilo libre
  WHEN se lee components/ui.html y components/modal.html
  THEN no existen los parámetros "style" ni "container_style"
Scenario: Python sin style
  GIVEN app/**/*.py
  THEN ninguna cadena contiene "style="
```

### REQ-3: Sin cambio visual (salvo los cambios intencionados de REQ-6)
**Priority:** Must Have
WHEN se compara la app de esta fase con `b0f5dd8`, THE SYSTEM SHALL producir los mismos estilos calculados en cada
elemento visible de las 17 pestañas del admin, los 18 modales del tour, el login y las 3 páginas de impresión, en tema
claro y en tema oscuro, con viewport fijo; las únicas diferencias admitidas son las de REQ-6.

```gherkin
Scenario: estilos calculados idénticos
  GIVEN una baseline de estilos calculados generada desde b0f5dd8 (worktree, set -e)
  WHEN se ejecuta "style_snapshot.py check <baseline>" sobre la rama
  THEN el resultado es "OK" para todas las capturas
  AND las únicas diferencias listadas como "intencionadas" son las de REQ-6 en tema oscuro
```

### REQ-4: htmx sin `<style>` inyectado
**Priority:** Must Have
WHEN se carga el dashboard, THE SYSTEM SHALL configurar htmx con `includeIndicatorStyles: false` y servir sus reglas de
`.htmx-indicator` desde `app.css`, con el mismo efecto que hoy (oculto en reposo, visible durante la petición); y con
`attributesToSettle: ["class", "width", "height"]` (sin `style`), para que el *settle* no escriba atributos `style`.

```gherkin
Scenario: sin <style> en el head
  GIVEN el admin abre el dashboard
  THEN document no contiene ningún elemento <style>
  AND un .htmx-indicator tiene opacity 0 en reposo
Scenario: el settle no escribe style
  GIVEN el admin en Inicio con los gráficos dibujados
  WHEN vuelve a abrir la pestaña Inicio
  THEN no hay errores CSP en consola
```

### REQ-5: Códigos de barras sin estilos inline y con el mismo dibujo
**Priority:** Must Have
WHEN se dibuja un código de barras (historial de compras y etiqueta individual), THE SYSTEM SHALL generar el SVG sin
ningún atributo `style` (geometría de JsBarcode en modo objeto + atributos de presentación SVG), con la misma posición y
tamaño de cada barra que el SVG de JsBarcode en `b0f5dd8`, fondo blanco, barras negras y, en la etiqueta, el texto en
`12px monospace` centrado.

```gherkin
Scenario: historial de compras
  GIVEN una compra con SKU "SKU-1"
  WHEN el admin abre la pestaña Compras
  THEN el svg.barcode tiene data-rendered="1", ≥ 1 <rect> de barra y ningún atributo style en todo el SVG
  AND no hay errores CSP en consola
Scenario: etiqueta individual
  WHEN se abre /ui/purchase/{id}/label
  THEN cada svg.barcode tiene un <text> con el SKU y ningún atributo style
Scenario: misma geometría que JsBarcode
  GIVEN el mismo código y opciones
  THEN los (x, y, width, height) de las barras coinciden con los del renderer SVG de JsBarcode
```

### REQ-6: Colores por tokens, sin hex en el HTML
**Priority:** Should Have
WHEN un elemento necesita un color, THE SYSTEM SHALL tomarlo de una variable CSS de `app.css` (o del CSS de su página), no
de un hex en la plantilla. Cambios intencionados, **solo en tema oscuro**: el borde de las 5 miniaturas pasa a
`var(--brand-border)` (#334155) y el badge de reposición a tokens propios del tema oscuro. En tema claro los valores
calculados no cambian.

```gherkin
Scenario: sin hex en plantillas
  GIVEN app/templates/**/*.html
  THEN no hay ninguna coincidencia de #[0-9a-fA-F]{3,8} fuera de atributos href/id ni de texto
Scenario: miniatura en oscuro
  GIVEN el tema oscuro y una compra con imagen
  WHEN se abre la pestaña Compras
  THEN el borde de la miniatura es rgb(51, 65, 85)
```

### REQ-7: Paleta de los gráficos en CSS
**Priority:** Should Have
WHEN se dibuja un gráfico (Inicio y widgets del copiloto), THE SYSTEM SHALL leer sus colores de variables CSS de `app.css`
definidas para tema claro y oscuro, con exactamente los mismos valores que las constantes JS de hoy.

```gherkin
Scenario: sin colores en los módulos de gráficos
  GIVEN components/home-charts.js y components/widget-chart.js
  THEN no contienen literales "#rrggbb" ni "rgba(" de paleta
Scenario: mismos colores
  GIVEN el tema claro
  WHEN se dibuja el gráfico mensual de Inicio
  THEN el dataset "Ventas" tiene backgroundColor "rgba(11,45,66,0.85)"
```

### REQ-8: Cambio de tema sin recargar
**Priority:** Should Have
WHEN el usuario pulsa el botón de tema, THE SYSTEM SHALL cambiar `data-theme`, guardarlo en `localStorage` y redibujar los
gráficos visibles con la paleta del tema nuevo, **sin recargar la página** (la pestaña y el estado del formulario se
conservan).

```gherkin
Scenario: tema sin recarga
  GIVEN el admin en la pestaña Copiloto con tema claro y un widget de gráfico (primer color "#0b2d42")
  WHEN pulsa el botón de tema
  THEN html[data-theme] es "dark" y localStorage "gi-theme" es "dark"
  AND la página no se recargó (una marca puesta en window sigue ahí)
  AND el primer color del widget pasa a "#60a5fa"
  AND tras recargar la página el tema sigue siendo "dark"
```

### REQ-9: Guardas permanentes
**Priority:** Must Have
WHEN se ejecuta la suite, THE SYSTEM SHALL tener: unit tests estáticos de REQ-1, REQ-2, REQ-4, REQ-5, REQ-6 y REQ-7;
el fixture `js_errors` haciendo fallar el teardown ante cualquier mensaje con `Content Security Policy`; y el arnés HTML
traduciendo cada `style="…"` de la baseline a las clases que lo sustituyen (comparación estricta, sin ignorar `class`),
ignorando `<style>`, `<link rel="stylesheet">` y la meta `htmx-config`, con baseline regenerada desde `b0f5dd8`.

```gherkin
Scenario: js_errors detecta estilos bloqueados
  GIVEN un e2e que pone style="color: red" en <body> con setAttribute
  THEN js_errors contiene un mensaje con "Content Security Policy"
  AND el color calculado de <body> no es rojo
Scenario: arnés HTML
  WHEN se ejecuta "ui_snapshot.py check tests/snapshots/baseline"
  THEN "OK" en las 40 capturas
```

### REQ-10: CHANGELOG sin bump
**Priority:** Must Have
WHEN se cierra la fase, THE SYSTEM SHALL añadir la entrada de la fase 3 al bloque de 0.5.0 (sin publicar) del `CHANGELOG.md`
sin tocar `VERSION` ni `.bumpversion.cfg`.

## 1.2 MoSCoW

| Priority | REQs |
|---|---|
| Must Have | REQ-1, REQ-2, REQ-3, REQ-4, REQ-5, REQ-9, REQ-10 |
| Should Have | REQ-6, REQ-7, REQ-8 |
| Could Have | — |
| Won't Have (esta fase) | trocear `app.css`; tokenizar los hex que quedan **dentro** de `app.css`; unificar las dos paletas de gráficos |

## 1.3 Requisitos no funcionales

| Categoría | Respuesta |
|---|---|
| **Performance** | Tres hojas nuevas (`login.css`, `print.css`, `label-print.css`), una por página, con cache-busting por contenido (`static_version()` ya hashea todos los `.css`). Cambiar de tema pasa de una recarga completa a redibujar los gráficos visibles. |
| **Security** | Es el objetivo de la fase: cierra la inyección de CSS (`style=`/`<style>` inyectados) como vector de exfiltración y de UI redressing. No hay nuevos límites de autenticación ni datos sensibles. |
| **Scalability** | No aplica: no cambia consultas ni volumen. |
| **Observability** | No aplica en producción. En desarrollo, las violaciones de CSP de estilos hacen fallar los e2e (REQ-9). |
| **Reliability** | Al final de cada lote la app funciona y los tests pasan; hasta el lote del CSP, `'unsafe-inline'` sigue permitido, así que nada se rompe a medias. |

## 1.4 Fuera de alcance

- Trocear `app.css` en `tokens.css`/`base.css`/… (decisión del usuario, 2026-10-24).
- Sustituir los 28 hex que quedan **dentro** de `app.css` por tokens.
- Unificar la paleta de Inicio con la de los widgets (hoy son distintas; esta fase solo las mueve a CSS sin cambiar colores).
- Plantillas OOB en vez de HTML en Python, `UIContext`, sidebar data-driven (fase 4, con H-303 como prerrequisito).
- Reagrupar `partials/` (fase 5).
- `/docs` de FastAPI (Swagger desde CDN, ya bloqueado por `script-src 'self'` desde la fase 2).
- Actualizar htmx o JsBarcode.

## 1.5 Dependencias

- Fases 1-2 del release cerradas en `feature/frontend-modular` (HEAD `b0f5dd8`): layouts, macros, JS en módulos.
- Playwright + Chromium instalados (ya usados por los 72 e2e).
- Sin cambios de BD ni migraciones.

## 1.6 Preguntas abiertas

Ninguna. El alcance lo decidió el usuario y los colores se conservan (solo cambian en oscuro los 6 elementos de REQ-6).
Paso a la Fase 2.
