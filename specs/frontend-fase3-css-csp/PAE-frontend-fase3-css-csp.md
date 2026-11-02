# PAE-018: frontend-fase3-css-csp

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:
       cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase3-css-csp/tasks.md \
         --test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     Sin --proto (loop por defecto, ADR-020: no hay prototipo).
     Si un humano o cc_local lo ejecuta a mano:
     1. Sesión nueva por lote. Leer SOLO los envelopes del lote (Read offset/limit sobre tasks.md).
     2. El envelope es la única fuente: contenido literal / script literal, sin notas ni docs genéricas.
     3. Si un `assert` de un script falla, o `style_snapshot.py check` da FALLO: PARAR y reportar.
     4. Si el contexto se compacta a mitad de lote: parar.
     5. Un commit por lote; nunca push, sudo, systemctl ni scripts de despliegue; nunca tocar VERSION.
     6. `git add` solo de los ficheros del lote (hay cambios ajenos sin commitear en el repo).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `feature/frontend-modular` (`b0f5dd8`) |
| **Rama de trabajo** | `feature/frontend-modular` (rama del release; sin push) |
| **Fecha** | 2026-10-24 |
| **Versión actual** | 0.4.15 |
| **Versión objetivo** | 0.5.0 al cerrar la fase 5. **Ningún commit de este PAE sube versión** |
| **PAE anterior** | PAE-017 (`frontend-fase2-js-modulos`, cerrado: 4.35/5 🟢) y PAE-015 (copiloto, DoD cerrado) |
| **Loop** | `~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md` v3, paso 2 (PAE). Siguiente: gate del usuario → `cc_local_pae` |

### Documentos fuente de este PAE

- `requirements.md`: REQ-1..10 (Gherkin, MoSCoW, NFRs, fuera de alcance) + contexto medido sobre `b0f5dd8`.
- `design.md`: decisiones D1-D12, tabla de equivalencias `style` → clases, pre-mortem de las 10 dimensiones y R1-R4.
- `tasks.md`: 40 envelopes en 9 lotes, **generado por script** (`gen/`). Sin simulación sobre la app: `gen_tasks.py
  check` solo comprueba en memoria que cada `edit()` encuentra su «antes» en los ficheros reales de `b0f5dd8`.
- Insumos: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§1, §2 principio 3, §4 fase 3),
  `docs/auditorias/2026-10-23-frontend-fase2-js-modulos.md` (H-407) y el diagnóstico de la fase 3 (2026-10-24).
- Sin delegación a otras skills.

### Skills y herramientas asignadas a cc_local

Cada envelope declara `Tools (cc_local)` y `Skills`. Resumen:
- **Write** (contenido literal) para los ficheros nuevos: arnés de estilos, tabla de equivalencias, hojas CSS,
  `barcode-svg.js`, `chart-palette.js`, `theme.js`, tests.
- **Bash** para los scripts literales de edición (Python con `edit()` que exige una sola coincidencia), los scripts
  de baseline (worktree desde `b0f5dd8`), `node --check`, pytest y los dos arneses.
- **Read/Edit** solo para añadir bloques al final (`app.css`, segunda mitad de `style_snapshot.py`).
- **Skills: ninguna.** Todo va literal. **Sin MCP** (regla dura para modelos locales).

## 1. Objetivo

### 1.1 Qué se construye

- CSP `style-src 'self'` (sin `'unsafe-inline'`): ni `style=` ni `<style>` en plantillas, Python, librerías ni htmx.
- Los 106 `style="…"` pasan a clases (bloque final de `app.css`); los 4 `<style>` a `login.css`, `print.css` y
  `label-print.css`; las macros sin parámetros de estilo libre.
- Colores por tokens CSS: sin hex en las plantillas; miniaturas y badge de reposición siguen el tema oscuro.
- Códigos de barras dibujados sin `style` (`components/barcode-svg.js`, misma geometría que JsBarcode).
- Paleta de gráficos en variables CSS y cambio de tema sin recargar la página.
- Red de estilos calculados (`style_snapshot.py`) como primer lote: prueba que nada cambia de aspecto salvo lo previsto.

### 1.2 Por qué

`'unsafe-inline'` en `style-src` es lo último que impide un CSP estricto (diagnóstico §1). Con él, un `style=`
inyectado (p. ej. desde una factura importada) se aplica. Además, los hex en el HTML rompen el tema oscuro y el cambio
de tema recarga la página (pierde lo que el usuario estaba haciendo).

### 1.3 Valor esperado

CSP completo (scripts y estilos solo desde `/static`), tema oscuro coherente, tema sin recarga, y una red visual
reutilizable para las fases 4 y 5.

### 1.4 Qué NO se toca

Trocear `app.css`; los hex internos de `app.css`; unificar paletas; plantillas OOB/`UIContext`/sidebar (fase 4);
reagrupar `partials/` (fase 5); `/docs`; versiones de htmx/JsBarcode/Chart.js; BD, rutas y Python de negocio.

## 2. Decisiones de diseño (detalle en design.md §2.6)

| # | Decisión |
|---|---|
| D1 | `style=` → clases de utilidad/semánticas al **final** de `app.css` (ganan a las reglas generales, como el inline); se reutilizan las existentes del mismo valor |
| D2 | Login e impresiones: 3 hojas propias (no cargan `app.css`, decisión de la fase 1) |
| D3 | htmx: meta `htmx-config` con `includeIndicatorStyles:false` y `attributesToSettle` sin `style` + reglas del indicador en `app.css` |
| D4 | Códigos de barras: JsBarcode en modo objeto + renderer SVG propio con atributos de presentación |
| D5 | Paleta en variables CSS con los mismos literales que hoy (`--chart-8: initial` en oscuro = 7 colores) |
| D6 | `theme.js` emite `gi:themechange`; los dos módulos de gráficos redibujan |
| D7 | Red de estilos calculados por elemento (66 propiedades, claro y oscuro, 74 capturas), fuera de CI |
| D8 | Arnés HTML: traduce `style` antiguo → clases (comparación estricta, sin ignorar `class`) |
| D9 | `js_errors` falla ante cualquier `Content Security Policy` |
| D10 | Trampas de cascada resueltas en el envelope: `.filter-label.m-0`, quitar `mb-10` muerta, clases de `restock_table` en `print.css` |
| D11 | Baseline de estilos en `/tmp/gi-fase3-style-baseline`, no versionada; se regenera desde `b0f5dd8` si falta |
| D12 | Sin bump (release 0.5.0 agrupado) |

## 3. Arquitectura objetivo

### 3.1 Diagrama

```
app_shell.html ── <meta htmx-config> ─▶ htmx (sin hoja inyectada, settle sin style)
      │                               app.css ── :root / [data-theme=dark]: tokens (--brand-*, --badge-info-*, --chart-*, --home-*)
      │                                        └─ bloque «Fase 3»: clases que sustituyen a los style=
      └─ main.js ─▶ components/barcode.js ─▶ components/barcode-svg.js ─▶ JsBarcode (modo objeto)
                 ├▶ components/home-charts.js ─┐
                 ├▶ components/widget-chart.js ┼▶ components/chart-palette.js (cssVar/cssList/THEME_EVENT)
                 └▶ features/theme.js ─────────┘  (emite gi:themechange)
login.html ─▶ login.css      transfer_print/restock_print.html ─▶ print.css      label_single_print.html ─▶ label-print.css
                                                                                  └─ pages/label-print.js ─▶ barcode-svg.js
main.py: CSP default-src 'self'; script-src 'self'; style-src 'self'; …
```

### 3.2 Inventario de archivos

| Acción | Archivos |
|---|---|
| Nuevos (CSS) | `app/static/css/{login,print,label-print}.css` |
| Nuevos (JS) | `app/static/js/components/{barcode-svg,chart-palette}.js` |
| JS modificados | `components/{barcode,widget-chart,home-charts}.js`, `pages/label-print.js`, `features/theme.js` (reescrito) |
| CSS modificado | `app/static/css/app.css` (tokens + bloque final) |
| Plantillas (25) | las 21 con `style=` (design §2.3; incluye `stock_table`, `components/ui.html`, `transfer_bulk_modal`, `login`, `transfer_print`) + `components/modal.html`, `restock_print.html`, `label_single_print.html` (`<link>`) y `layouts/app_shell.html` (meta) |
| Python | `app/main.py` (CSP) |
| Tests | nuevos `tests/snapshots/{style_snapshot,style_classes}.py`, `style_allowlist.txt`, `tests/unit/test_frontend_css.py`, `tests/e2e/test_css_csp_e2e.py`; modificados `ui_snapshot.py` + `baseline/`, `tests/e2e/conftest.py`, `test_js_behaviors_e2e.py` |
| Docs | `CHANGELOG.md` |

### 3.3 Dependencias existentes (firmas reales)

```python
# tests/e2e/conftest.py — las usa style_snapshot.py (sin modificarlas)
def _boot_temp_server(prefix: str, *, extra_env: dict | None = None) -> tuple[str, str, subprocess.Popen, str]: ...
def _seed_stock(db_path: str) -> None: ...
def _seed_tour(db_path: str) -> dict:  # {"purchase": int, "transfer_ref": str}
# tests/e2e/test_ui_tour_csp_e2e.py
TABS: list[str]; MODALS: list[tuple[str, str, str]]
def login(page: Page, base_url: str, user: str = "admin") -> None: ...
def open_tab(page: Page, slug: str) -> None: ...
# tests/snapshots/ui_snapshot.py — class _Canon(HTMLParser): handle_starttag(tag, attrs); _norm(text); _is_js_wiring_attr(name)
```

```javascript
// app/static/js/core/content.js — onContent(fn(root, fromSwap)): enhancers tras carga y cada afterSwap
// window.JsBarcode(target, code, options) — vendor 3.11.6; window.Chart — vendor 4.4.1 (Chart.getChart(canvas))
```

### 3.4 Interfaces nuevas

```javascript
// components/barcode-svg.js
export function renderBarcodeSvg(svg, code, options)   // mismas opciones y excepciones que JsBarcode
// components/chart-palette.js
export function cssVar(name)        // getComputedStyle(:root).getPropertyValue(name).trim()
export function cssList(prefix)     // [--prefix-1, --prefix-2, …] hasta la primera vacía
export const THEME_EVENT = 'gi:themechange'
```

```jinja
{% macro card(title=None, id=None, extra_class='') %}   {% macro subcard(title=None, id=None, extra_class='') %}
{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_class=None) %}
```

```text
tests/snapshots/style_snapshot.py write <dir> | check <dir> [--allow <fichero>]   → "OK: …" (exit 0) | "FALLO: …" (exit 1)
tests/snapshots/style_classes.py: STYLE_TO_CLASSES: dict[str, str]; style_key(value: str) -> str
```

## 4. Fases de implementación (task envelopes)

Los 40 envelopes están en `tasks.md` (línea `Lotes:` incluida). Resumen:

| Lote | Contenido | ~tokens |
|---|---|---|
| A (T1-T7) | Red de estilos + baseline desde `b0f5dd8` y prueba de determinismo; guarda `js_errors`; tabla de equivalencias; canónico del arnés HTML + baseline | 5.4K |
| B (T8-T11) | `app.css` (bloque «Fase 3», fuera la `mb-10` muerta), tokens, `login.css`/`print.css`, `label-print.css` | 2.9K |
| C (T12-T16) | `tab_transfers` (3 partes), `tab_history`+`stock_table`, `purchase_panel` → `check` de estilos | 3.9K |
| D (T17-T20) | `sale_panel`+`action_result`, `tab_profit_items`, historiales de compras/ventas, `restock_table`+`products_history` → `check` | 2.5K |
| E (T21-T29) | formularios de producto, dashboard+extracción, documento+cliente, macros, login, etiqueta, impresiones → `check` | 4.9K |
| F (T30-T34) | `barcode-svg.js` + cableado; `chart-palette.js` + `theme.js`; widget-chart; home-charts | 3.5K |
| G (T35-T36) | meta `htmx-config` + CSP `style-src 'self'`; guardas unitarias | 2.1K |
| H (T37-T38) | e2e de la fase; e2e del tema sin recarga | 2.6K |
| I (T39-T40) | `check` final de estilos; CHANGELOG + suites completas | 0.8K |

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (hecha al generar el PAE)

- `gen_tasks.py check`: las ediciones de los 40 envelopes aplican **en memoria** sobre `b0f5dd8` con una sola
  coincidencia cada una, y el resultado no tiene `style`/`<style>`/hex en plantillas; toda clase usada existe en la hoja
  que carga su página; los módulos de gráficos quedan sin colores fijos. **No** se ha ejecutado la app ni los tests sobre
  el resultado (sin prototipo): eso lo hace la red del lote A durante la implementación.

### 5.1 Verificación por envelope

Cada envelope trae la suya: `node --check` en JS, el arnés HTML y pytest de lo afectado, y el `check` de estilos
calculados al cerrar los lotes B (T8, T9), C, D, E e I. `cc_local_pae` corre además la suite unit al cerrar cada lote.

### 5.2 Protocolo si una verificación falla

1. `assert` de un script de edición: el fichero no está como esperaba el PAE. **Parar y reportar**.
2. `style_snapshot.py check` con FALLO: **parar y reportar la salida** (qué elemento y qué propiedad). Es justo el tipo
   de fallo que esta red existe para cazar; el arreglo lo decide Opus (nuevo envelope), no `cc_local`.
3. Test que falla: reintentar el envelope desde `git checkout -- <Files>` como mucho 2 veces; si sigue, parar
   (`cc_local_pae` deja `lote<X>-PARADA.txt`).
4. Nunca regenerar baselines fuera de T3/T7 (o el bloque de regeneración de T39), ni con `write` sobre el repo.

### 5.3 Tests existentes que deben seguir pasando

443 unit y 72 e2e (uno, `test_theme_toggle_persists_across_reload`, se sustituye en T38 porque esperaba la recarga).
En particular el tour `test_ui_tour_csp_e2e.py` (7), que con la CSP estricta es la prueba más amplia de REQ-1/REQ-2.

### 5.4 Tests e2e (Playwright: el riesgo es de navegador)

| Fichero | Cubre |
|---|---|
| `test_css_csp_e2e.py` (9, nuevo) | sin `<style>` e indicador oculto; reabrir Inicio sin `style` en el *settle*; código de barras del historial y de la etiqueta sin `style` (fondo/barras/texto); geometría idéntica a JsBarcode (12 casos); borde de miniatura por tema (×2); colores del gráfico mensual desde CSS; la guarda detecta un `style` bloqueado |
| `test_js_behaviors_e2e.py::test_theme_toggle_without_reload` | tema sin recarga, widget redibujado con la paleta oscura, persistencia tras recargar |
| `test_ui_tour_csp_e2e.py` (7, existente) | 17 pestañas, 3 roles, modales, login e impresiones con la CSP estricta |

Comando: `SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest tests/e2e -m e2e --no-cov -p no:warnings`.

## 6. Riesgos y mitigaciones (detalle en design §2.9)

| Riesgo | Mitigación |
|---|---|
| Una clase pierde contra otra regla (cambio visual silencioso que ni tests ni revisión ven) | Red de estilos calculados (lote A) con `check` al cerrar cada lote de plantillas |
| Baseline generada sobre código migrado | Scripts con `set -euo pipefail` + `git diff --quiet b0f5dd8 -- app/` + worktree desde `b0f5dd8` |
| Ruido en la red (ids aleatorios de `notes.js`, carrera de Chart.js) | Normalización de `reusable-notes-list-*` y espera a que cada `<canvas>` tenga gráfico; T3 prueba el determinismo |
| htmx escribe `style` (hoja del indicador, *settle*) | Meta `htmx-config` (T35) + e2e de reabrir Inicio |
| JsBarcode negro sobre negro | `barcode-svg.js` + e2e de geometría frente a JsBarcode |
| La guarda no ve violaciones de estilo | `js_errors` busca `Content Security Policy` (T4) + e2e que lo provoca |
| `wait_for_function` con expresión bloqueado por CSP | Siempre funciones flecha |
| `cc_local` «arregla» a mano una diferencia | Prohibido en el PAE: parar y reportar |
| Modo desktop (WebView) | Sin cambios de JS de plataforma; prueba manual en el paso 8 |

## 7. Definición de hecho (DoD)

### 7.1 Checklist (al terminar el lote I)

- [ ] 9 commits (uno por lote) en `feature/frontend-modular`, sin push; ninguno toca `VERSION`/`.bumpversion.cfg`.
- [ ] `grep -rlE '\sstyle\s*=|<style' app/templates` → vacío.
- [ ] Cabecera CSP con `style-src 'self';` y sin `'unsafe-inline'`.
- [ ] Unit: **453 passed** (443 + 10). Arnés HTML: **OK: 40 snapshots equivalentes**. e2e: **81 passed** (72 + 9).
- [ ] `style_snapshot.py check … --allow tests/snapshots/style_allowlist.txt` → **OK (6 diferencias intencionadas)**.
- [ ] Los commits solo tocan ficheros del inventario §3.2.

### 7.2 Script de verificación completa

```bash
set -euo pipefail
cd ~/PyCharmMiscProject/gestion_inventario
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -rfE tests/unit | tail -1
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline | tail -1
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt | tail -1
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
  .venv/bin/python -m pytest tests/e2e -m e2e --no-cov -p no:warnings -rfE | tail -1
! grep -rlE '\sstyle\s*=|<style' app/templates
git diff --quiet b0f5dd8 -- VERSION .bumpversion.cfg
```

### 7.3 Commits esperados (mensajes en inglés)

`feat(frontend-fase3-css-csp): lote A (T1-T7)` … `lote I (T39-T40)`, como en las fases 1 y 2.

### 7.4 Después (pasos 5-8 del loop)

Revisión con Opus (`/code-review` + `auditoria-ingenieria`, Serena + codebase-memory; plantillas, CSS y JS fuera de su
cobertura: grep/Read). Puntos para mirar con lupa: `barcode-svg.js` frente a JsBarcode, el canónico del arnés HTML, que
ningún `style` llegue por otra vía (respuestas OOB, `/docs`), y la prueba manual en dev con tema oscuro, impresión de
etiquetas y el **modo desktop**.
