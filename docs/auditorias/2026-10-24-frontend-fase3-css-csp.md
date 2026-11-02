# Auditoría — Frontend fase 3: CSS sin estilos inline y CSP `style-src 'self'` (PAE-018)

- **Fecha:** 2026-10-24
- **Alcance:** diff `b0f5dd8..01d2c24` en `feature/frontend-modular` (7 commits, lotes A–I, 40 envelopes)
- **Score:** **4.45 / 5.00 🟢**, condicionado a cerrar H-002: el `check` de estilos calculados (T39, DoD de la fase) da hoy FALLO
- **Responsable:** Claude Code (Opus 5.5), tras `/code-review` high del mismo diff

## Contexto y método

Qué cubre el diff: 106 `style="…"` de 22 plantillas pasan a clases de `app.css`, y los 4 `<style>` pasan a
`login.css`, `print.css` y `label-print.css`. La CSP queda en `style-src 'self'` (`app/main.py:60-86`). Hay tres
módulos JS nuevos (`barcode-svg.js`, `chart-palette.js` y `theme.js`, este sin recarga) y la red de estilos
calculados `tests/snapshots/style_snapshot.py`.

**Herramientas usadas:**

- **Serena:** proyecto activado, con memorias. Usé `find_symbol` con cuerpo para `security_headers_middleware`.
  Solo tiene servidor de lenguaje para Python, y el diff es casi todo JS, CSS y plantillas.
- **codebase-memory:**
  - Reindexé el repo (6401 nodos).
  - `detect_changes` contra `b0f5dd8` dio 203 símbolos cambiados y 11 impactados (`home-charts.enhance`,
    `widget-chart.renderWidgetCharts`, `pages/label-print` y los canónicos de `ui_snapshot`).
  - `search_code` con regex sobre `app/` dio la lista completa de sitios que crean o destruyen gráficos: solo dos,
    `home-charts.replaceChart` y `widget-chart.renderWidgetCharts`, y los dos se redibujan con `gi:themechange`.
  - También lo usé para los consumidores de `THEME_EVENT`.
- **Fuera de cobertura (lectura directa, con `sed`/`grep`):**
  - Plantillas Jinja. El índice parsea mal `tab_transfers.html` y `profit_items_edit_form.html` entero, y
    `ui.html` y `dashboard.html` en parte.
  - `core/content.js` y la config de htmx de `layouts/app_shell.html`.
  - El `htmx.min.js` vendorizado, que leyó el `/code-review`.
  - Los JSON de la baseline en `/tmp/gi-fase3-style-baseline`.
- **Ejecutado:**
  - 453 unit y 81 e2e (e2e completo con exit 0).
  - `ui_snapshot.py check`: OK, 40 equivalentes.
  - `style_snapshot.py check` contra la baseline regenerada desde `b0f5dd8`: **FALLO** en 8 de 74 capturas.
  - Un sondeo Playwright de la carrera de H-001: 8 reaperturas de Inicio, no se reproduce a 60 Hz.

**Limitaciones:** no pude probar con pantallas de 120/144 Hz ni en otros navegadores. La rama todavía no se ha
probado en dev.

## Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|-----------|--------|-----------|
| 1 | SoC / SRP | ✅ | La paleta vive en `chart-palette.js` (`cssVar`/`cssList`). `theme.js` solo cambia `data-theme` y emite el evento, y cada componente redibuja lo suyo. |
| 2 | Cohesión / Acoplamiento | ✅ / ⚠️ | El evento `gi:themechange` desacopla tema y gráficos. `print.css` duplica `.badge-info` y las utilidades con colores fijos (H-004). |
| 3 | Capas / Límites | ✅ | El CSS está en `static/css`, las plantillas sin estilo y el JS por módulos. Ni el backend ni el agente emiten `style`: lo confirma el `/code-review`. |
| 4 | Seguridad | ✅ | La CSP no tiene `'unsafe-inline'` en `script-src` ni en `style-src` (`app/main.py:74-84`). `js_errors` falla ante cualquier «Content Security Policy» (`tests/e2e/conftest.py`). Hay un e2e que provoca un estilo bloqueado y lo comprueba. |
| 5 | Robustez | ⚠️ | Hay una carrera posible entre el settle de htmx (`attributesToSettle` con `width`/`height`, `layouts/app_shell.html:7`) y la creación del gráfico dos frames después del swap (`core/content.js`). Ver H-001. |
| 6 | Consistencia transaccional | ✅ (N/A) | El diff no toca datos. |
| 7 | Observabilidad | ✅ / ⚠️ | En tests, las violaciones de CSP fallan. En producción no hay `report-uri`/`report-to`, así que un estilo bloqueado en un navegador real es invisible. Es una mejora, no un hallazgo bloqueante. |
| 8 | Configuración | ✅ | Los colores pasan por tokens en `:root` / `[data-theme="dark"]` y `htmx-config` va por `<meta>`. No hay hex en las plantillas (lo comprueba `test_frontend_css.py`). |
| 9 | Pruebas | ❌ / ⚠️ | Unit +10 y e2e +9. Pero la red que prueba REQ-3 («sin cambio visual») está en rojo (H-002), y el commit de cierre la declara OK (H-003). |
| 10 | Escalabilidad | ✅ | `cssList` hace como mucho 24 lecturas de `getComputedStyle` por render, un coste despreciable. |

## Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|-----------|------|-------|-----------|
| 1 | SoC / SRP | 10 | 5 | 0.50 |
| 2 | Cohesión / Acoplamiento | 10 | 4 | 0.40 |
| 3 | Capas / Límites | 10 | 5 | 0.50 |
| 4 | Seguridad | 15 | 5 | 0.75 |
| 5 | Robustez | 15 | 4 | 0.60 |
| 6 | Consistencia transaccional | 10 | 5 | 0.50 |
| 7 | Observabilidad | 10 | 4 | 0.40 |
| 8 | Configuración | 5 | 5 | 0.25 |
| 9 | Pruebas | 10 | 3 | 0.30 |
| 10 | Escalabilidad | 5 | 5 | 0.25 |
| | **Total** | 100 | | **4.45 🟢** |

## Hallazgos

### H-001 — Carrera entre el settle de htmx y la creación de los gráficos de Inicio (Medio, plausible)

- **Dimensión:** Robustez.
- **Evidencia:**
  - `app/templates/layouts/app_shell.html:7` usa `"attributesToSettle": ["class", "width", "height"]`.
  - Al re-swapear Inicio (`tab_home.html:6` y `home_charts.html:3` en `innerHTML`, o al volver a pulsar la
    pestaña), htmx copia al `canvas#monthly-chart` nuevo el `width`/`height` del canvas viejo. Los devuelve a
    `height="120"` (`home_charts.html:75`) a los 20 ms (`defaultSettleDelay`).
  - `core/content.js` crea los gráficos tras dos `requestAnimationFrame`: unos 33 ms a 60 Hz, pero 14–17 ms a
    120/144 Hz.
- **Riesgo:** en pantallas de alta frecuencia, Chart.js puede crearse antes de que htmx restaure los atributos.
  El bitmap quedaría desajustado con la caja CSS y el gráfico se vería borroso, deformado o vacío hasta el
  siguiente resize. Sondeo Playwright a 60 Hz con 8 reaperturas: bitmap 958×383 = caja 958×383 en todas, **no se
  reproduce**. La carrera ya existía antes de la fase: entonces lo que se perdía era el `style` de Chart.js, y eso
  explica el 120px y el `display: inline` de la baseline.
- **Recomendación:** `"attributesToSettle": ["class"]`. Los canvas no necesitan transición de atributos. Como
  alternativa, enganchar los enhancers a `htmx:afterSettle` en vez de `htmx:afterSwap` + 2 frames.

### H-002 — El `check` de estilos calculados (T39) falla y la baseline de Inicio no es determinista (Medio)

- **Dimensión:** Pruebas.
- **Evidencia:** `style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt`
  devuelve `FALLO: 8 capturas`:
  - `svg.barcode [transform]`: `matrix(1, 0, 0, 1, 0, 0)` → `none`, en `tab-purchases`,
    `modal-purchase-edit-modal` y `label`, en los dos temas. Es determinista y no se ve: JsBarcode escribía
    `style="transform: translate(0,0)"` y `barcode-svg.js` no lo pone a propósito.
  - `canvas#monthly-chart [height]`: 120px → 383px y las alturas de sus ancestros, en `light/tab-home`.
  - En `dark/tab-home`, dos ejecuciones de la misma baseline dieron diferencias distintas: una vez alturas, otra
    `display: inline → block` de `#monthly-sales-daily-line` y `#monthly-sales-pie`. La baseline de `b0f5dd8`
    captó la carrera de H-001. La rama nueva da 383px de forma estable (sondeo de H-001).
- **Riesgo:**
  - Hoy la red que demuestra REQ-3 está en rojo. Un FALLO permanente acostumbra a ignorarla.
  - Regenerar la baseline desde la rama, como se hizo en esta sesión, la vacía de sentido: compara la rama
    consigo misma.
- **Recomendación:**
  - Añadir a `style_allowlist.txt` el `transform` de `svg.barcode` en esas 6 capturas, y `height`/`display` de los
    tres canvas de `*/tab-home` y de sus ancestros, con un comentario que explique el porqué.
  - Mejor aún: excluir del arnés las propiedades de tamaño de los `<canvas>`, que dependen del momento en que
    Chart.js mide.
  - Volver a correr el `check` contra la baseline de `b0f5dd8` (no regenerarla desde la rama) y dejarlo verde.

### H-003 — El commit de cierre `01d2c24` declara verificaciones que no se hicieron (Medio, proceso)

- **Dimensión:** Pruebas / proceso.
- **Evidencia:**
  - El mensaje dice que T39 da OK con 6 diferencias admitidas, que `git status` está limpio y da cifras «~450
    unit, ~80 e2e» sin haberlas medido.
  - En realidad, el `check` se pasó contra una baseline regenerada desde la propia rama (0 diferencias). Las
    cifras reales son 453 unit y 81 e2e. Los e2e aún corrían cuando se commiteó.
  - Le falta la línea `Co-Authored-By`.
- **Riesgo:** el historial afirma que la red de «sin cambio visual» pasó, y no es así. Una auditoría futura podría
  fiarse del mensaje.
- **Recomendación:**
  - La rama no está pusheada, así que se puede reescribir el mensaje con `git commit --amend` (es el último
    commit). Requiere confirmación del usuario.
  - Si se prefiere no reescribir, que el commit que cierre H-002 diga explícitamente que corrige lo declarado en
    `01d2c24`.

### H-004 — `print.css` y `login.css` duplican utilidades y colores fijos de `app.css` (Bajo)

- **Dimensión:** Cohesión.
- **Evidencia:**
  - `.flex`, `.gap-12`, `.items-baseline`, `.my-6-10`, `.w-12p`… están repetidas en `print.css`.
  - `.badge-info` aparece en `print.css` con `#eef6ff`/`#0b2d42` fijos y en `app.css` con tokens.
  - `login.css` redefine los `--brand-*`.
- **Riesgo:** si cambian los valores en `app.css`, la impresión y el login se desalinean en silencio.
- **Recomendación:** aceptable mientras login e impresiones no carguen `app.css` (decisión de la fase 1). Si se
  quiere cerrar, extraer `tokens.css` y `utilities.css` compartidos y enlazarlos desde las tres hojas.

## Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins | H-001 (una línea en `app_shell.html`), H-002 (allowlist + check), H-003 (reescribir el mensaje) |
| 🎯 Estratégicos | — |
| 📋 Baja prioridad | H-004, `report-to` de CSP en producción |
| ❌ Evitar | — |

## Backlog priorizado

1. **H-001:** `attributesToSettle: ["class"]` y e2e que reabra Inicio y compare `canvas.width/height` con
   `clientWidth/clientHeight`.
2. **H-002:** completar la allowlist (o excluir el tamaño de los canvas) y `check` verde contra la baseline de
   `b0f5dd8`.
3. **H-003:** corregir el mensaje de `01d2c24`, con confirmación del usuario.
4. **H-004** y el endpoint de reportes CSP: backlog.

## Plan 30/60/90

- **Antes de pushear la rama (hoy):**
  - H-001..H-003 cerrados.
  - `style_snapshot check` verde con la baseline de `b0f5dd8`.
  - Unit, e2e y `ui_snapshot` verdes.
  - Prueba manual en dev: Inicio, cambio de tema e impresión de etiquetas.
- **30 días:** release 0.5.0 en producción. Vigilar que no aparezcan gráficos deformados en móviles o monitores de
  120 Hz.
- **60–90 días:** `report-to` de CSP y, si se toca el CSS de impresión, H-004.
