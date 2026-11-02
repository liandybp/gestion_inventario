# PAE-017: frontend-fase2-js-modulos

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:
       cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase2-js-modulos/tasks.md \
         --test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     Sin --proto (loop por defecto, ADR-020: no hay prototipo).
     Si un humano o cc_local lo ejecuta a mano:
     1. Sesión nueva por lote. Leer SOLO los envelopes del lote (Read offset/limit sobre tasks.md).
     2. El envelope es la única fuente: contenido literal / script literal, sin notas ni docs genéricas.
     3. Si un `assert` de un script falla: PARAR y reportar (no editar a mano para que cuadre).
     4. Si el contexto se compacta a mitad de lote: parar.
     5. Un commit por lote; nunca push, sudo, systemctl ni scripts de despliegue; nunca tocar VERSION.
     6. `git add` solo de los ficheros del lote (hay cambios ajenos sin commitear en el repo).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `feature/frontend-modular` (`b7a9881`) |
| **Rama de trabajo** | `feature/frontend-modular` (rama del release; sin push) |
| **Fecha** | 2026-10-23 |
| **Versión actual** | 0.4.15 |
| **Versión objetivo** | 0.5.0 al cerrar la fase 5. **Ningún commit de este PAE sube versión** |
| **PAE anterior** | PAE-016 (`frontend-fase1-layouts-macros`, cerrado: 4.80/5 🟢) |
| **Loop** | `~/obsidian-lab/04-Harness/loop-desarrollo-agentes.md` v3, paso 2 (PAE). Siguiente: gate del usuario → `cc_local_pae` |

### Documentos fuente de este PAE

- `requirements.md`: REQ-1..12 (Gherkin, MoSCoW, NFRs, fuera de alcance).
- `design.md`: decisiones D1-D11, pre-mortem R1-R12, las 10 dimensiones y la validación por mutación.
- `tasks.md`: 31 envelopes en 9 lotes, **generado por script** (`gen/`) y simulado.
- Insumos: `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§1, §2 y §4) y
  `docs/prototipos/fase1-layouts-macros/traspaso.md`.
- Sin delegación a otras skills (el threat model cabe en design §4).

### Skills y herramientas asignadas a cc_local

Cada envelope declara `Tools (cc_local)` y `Skills`. Resumen:
- **Write** (contenido literal) para los 19 módulos JS, `widget_chart.html` y los tests nuevos.
- **Bash** para ejecutar los scripts literales de edición (Python con `edit()`/`cut()` que exigen una sola
  coincidencia), el script de la baseline, `git rm`, `node --check`, pytest y el arnés.
- **Read/Edit** solo para añadir bloques al final (`tests/e2e/conftest.py`, segunda mitad del e2e de comportamientos).
- **Skills: ninguna.** Todo va literal. **Sin MCP** (regla dura para modelos locales).

## 1. Objetivo

### 1.1 Qué se construye

- Todo el JS en **módulos ES nativos** bajo `app/static/js/`: `main.js` con `core/`, `components/` y `features/`;
  `pages/` para login e impresiones; `theme-init.js` clásico en `<head>`. Fuera `app.js`.
- **0 `<script>` inline ejecutables** (había 5) y **0 handlers `on*=`** (había 15). Los sustituyen enganches `data-js-*`.
- Fuera el código de depuración: `debugReturnLots`, `setDebug`, los 3 `#*-debug` y el sondeo de `window.Chart`.
- **CSP `script-src 'self'`**. `Cache-Control: no-cache` en `/static/js/` y `static_version()` sobre todos los assets.
- Corrección del **XSS DOM del tooltip del pastel** de Inicio.
- Arnés de snapshots que ignora el cableado JS. **Baseline regenerada desde `b7a9881`** en el mismo commit.
- +9 unit y +23 e2e, que fallan ante cualquier error de JS o violación de CSP.

### 1.2 Por qué

Mientras haya JS inline, `script-src` necesita `'unsafe-inline'`, y eso anula gran parte del CSP (diagnóstico §1).
Además, `app.js` es un IIFE de 767 líneas con 22 listeners globales. Durante el análisis apareció un XSS real que
`'unsafe-inline'` dejaba ejecutar.

### 1.3 Valor esperado

- Seguridad: un HTML inyectado deja de poder ejecutar código. Se cierra un XSS.
- Mantenibilidad: un módulo por responsabilidad, sin código muerto (lo prueba un test), base para las fases 3-5.
- Red de seguridad: el arnés + 70 e2e con guarda de consola y CSP.

### 1.4 Qué NO se toca

`style-src`, los `style=` y los colores (fase 3). El `reload()` del tema (fase 3). `hx-push-url` en pestañas. El
HTML construido en Python (fase 4). Reagrupar `partials/` (fase 5). Modelos, migraciones, endpoints, `requirements.txt`,
`docker-compose*`, `VERSION`, `.bumpversion.cfg`. Cambios ajenos sin commitear: `CLAUDE.md`, `.claude/`, `.idea/`,
`.serena/`, `specs/copiloto-ventas-ubicacion-mensual/`.

## 2. Decisiones de diseño (detalle en design.md §2)

| # | Decisión |
|---|---|
| D1 | Módulos ES nativos sin build; `main.js` llama a `init()` de cada módulo; `pages/*` son entradas propias |
| D2 | `theme-init.js` clásico y síncrono (evita el destello de tema) |
| D3 | Enganches nuevos con prefijo `data-js-*`; los `data-*` existentes intactos |
| D4 | Enhancers con la semántica actual (`afterSwap` + 2 frames), también para los widgets: htmx 1.9.12 emite `afterSwap` en los OOB (verificado) |
| D5 | El arnés ignora los scripts ejecutables, `on*`, `data-js-*` y `#*-debug`. Baseline desde el commit previo. Los scripts por página los fija un unit test |
| D6 | `no-cache` en `/static/js/` + hash de todos los `.css`/`.js` |
| D7 | Spec del widget en un atributo `data-js-widget-chart` con comillas **simples** (`tojson` no escapa `"`) |
| D8 | SKU y nombre de la etiqueta en `data-js-label-*` |
| D9 | Tooltip del pastel con `textContent` |
| D10 | Guarda e2e `pageerror` + `console.error`; `wait_for_function` siempre en forma de función |
| D11 | Siembra e2e propia (`_seed_tour`) y chat interceptado con `page.route` + plantilla real |

## 3. Arquitectura objetivo

### 3.1 Diagrama

```
layouts/app_shell.html ── <head> theme-init.js (clásico) · htmx · Chart.js · JsBarcode (clásicos)
                         └ main.js (module) ─┬─ core/content.js  ◄── barcode · home-charts · notes · widget-chart
                                             ├─ core/activity · tabs · modals · swap
                                             ├─ components/product-suggest · form-helpers
                                             └─ features/transfers · agent-chat · theme
login.html ──────────── pages/login.js (module)
restock_print / transfer_print ── pages/print.js (module)
label_single_print ──── JsBarcode (clásico) + pages/label-print.js (module)
main.py middleware ──── CSP script-src 'self' · Cache-Control no-cache en /static/js/
```

### 3.2 Inventario de archivos

| Acción | Archivos |
|---|---|
| Nuevos (19 JS) | `app/static/js/{theme-init,main}.js`, `core/{content,activity,tabs,modals,swap}.js`, `components/{barcode,notes,product-suggest,form-helpers,home-charts,widget-chart}.js`, `features/{transfers,agent-chat,theme}.js`, `pages/{login,print,label-print}.js` |
| Borrado | `app/static/js/app.js` |
| Plantillas (16) | `layouts/app_shell.html`, `dashboard.html`, `login.html`, `label_single_print.html`, `restock_print.html`, `transfer_print.html`, `partials/{widget_chart,home_charts,purchases_history,sales_history,stock_table,tab_history,tab_transfers,tab_profit_items,sale_panel}.html`, `components/sales_doc/cart_add.html` |
| Python | `app/main.py` (CSP + cache), `app/routers/ui_common.py` (`_static_asset_version`) |
| Tests | `tests/snapshots/ui_snapshot.py` + `baseline/` (13 `.txt`), `tests/unit/test_ui_regressions.py`, `tests/unit/test_ui_components.py`, nuevo `tests/unit/test_frontend_js_modules.py`, `tests/e2e/conftest.py` (+bloque), nuevos `tests/e2e/test_ui_tour_csp_e2e.py` y `tests/e2e/test_js_behaviors_e2e.py` |
| Docs | `CHANGELOG.md` |

### 3.3 Dependencias existentes (firmas reales)

```python
# app/main.py — se modifica (T25)
@app.middleware("http")
async def security_headers_middleware(request, call_next): ...   # pone CSP/X-Frame/nosniff/Referrer
# app/routers/ui_common.py — se modifica (T14)
def _static_asset_version() -> str: ...          # global Jinja "static_version", se llama sin argumentos
# app/agent/widgets.py — lo usan el arnés y el e2e
def add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by) -> DashboardWidget
def widgets_with_payload(db, business_id) -> list[dict]          # {"id","type","title","payload"}
# tests/e2e/conftest.py — ya existe
def _boot_temp_server(prefix, *, extra_env=None) -> tuple[str, str, subprocess.Popen, str]
def _seed_stock(db_path: str) -> None
```

### 3.4 Interfaces nuevas

```javascript
// core/content.js
export function onContent(fn /* (root: Document|Element, fromSwap: boolean) => void */) {}
export function init() {}
// todos los demás módulos de main.js
export function init() {}
```

```python
# app/routers/ui_common.py
def _static_asset_version(static_dir: Path | None = None) -> str: ...
# tests/e2e/conftest.py
TOUR_PRODUCT_NAME: str; TOUR_WIDGET_TITLE: str
def _seed_tour(db_path: str) -> dict            # {"purchase": int, "transfer_ref": str}
@pytest.fixture def tour_server() -> tuple[str, dict]
@pytest.fixture def js_errors(page) -> list[str]
```

Atributos `data-js-*`: `select-all="<name>"`, `autosubmit`, `mirror-to="<id>"`, `enter-focus="<name>"`,
`theme-toggle`, `agent-toggle`, `agent-size`, `print`, `widget-chart='<json>'`, `label-sku`, `label-name`.

## 4. Fases de implementación (task envelopes)

**Los envelopes están en `tasks.md`** (generado; no copiar aquí para no desincronizar).

`Lotes: A T1–T2 · B T3–T7 · C T8–T10 · D T11–T14 · E T15–T19 · F T20–T24 · G T25–T26 · H T27–T28 · I T29–T31`

| Lote | Contenido | ~tokens |
|---|---|---|
| A | Arnés (canónico sin cableado JS + widget) y baseline desde el commit previo | 2.8K |
| B | `theme-init`, `core/*`, barcode, notes, product-suggest, form-helpers | 8.0K |
| C | home-charts (sin depuración ni XSS), widget-chart, transfers | 6.7K |
| D | agent-chat, theme, main, pages, `static_version()` | 6.3K |
| E | `app_shell` carga módulos, borrar `app.js`, dashboard/login/etiqueta/impresiones | 4.3K |
| F | widget_chart, home_charts, 6× seleccionar todo, sale_panel, cart_add | 4.4K |
| G | CSP + `no-cache`; unit tests de la fase | 3.9K |
| H | Fixtures e2e (siembra + guarda) + e2e de recorrido | 5.0K |
| I | e2e de comportamientos (2 partes) + CHANGELOG | 5.3K |

Tamaño: los envelopes de más de 800 tokens son **copias literales de un fichero**; hay 7 de ~2-2.8K, con el
precedente de la fase 1. Los demás son scripts que repiten el helper `edit()` de 20 líneas, para ser
autocontenidos. Ninguno toca más de 2 ficheros.

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (hecha al generar el PAE)

- Worktree `/tmp/sdd-fase2-wt` (desde `b7a9881`): **423 unit, 70 e2e, arnés OK 40/40**.
- Simulación (`gen/simulate.sh /tmp/sdd-fase2-wt`): aplicar los 31 envelopes a una copia limpia de `b7a9881` →
  `diff -r` **idéntico**, 423 unit, arnés OK.
- Mutaciones: 10/10 unit, 6/6 arnés, 8/8 e2e (design §8).

### 5.1 Verificación por envelope

Cada envelope trae la suya: `node --check` para cada módulo, `ui_snapshot.py check` tras cada cambio de plantilla
(`OK: 40`), y pytest de los tests afectados. `cc_local_pae` corre además la suite unit al cerrar cada lote.

### 5.2 Protocolo si una verificación falla

1. Si es un `assert` de un script de edición: la plantilla no está como esperaba el PAE. **Parar y reportar**.
2. Si es un test o el arnés: reintentar el envelope desde `git checkout -- <Files>` como mucho 2 veces. Si sigue
   fallando, parar: `cc_local_pae` deja `lote<X>-PARADA.txt`.
3. Nunca regenerar la baseline fuera de T2 ni con `write` sobre el repo.

### 5.3 Tests existentes que deben seguir pasando

414 unit (2 se reescriben en T14/T15 porque dependían de `app.js`) y los 47 e2e. En particular
`test_transfer_bulk_e2e.py` y `test_product_suggest_e2e.py`, que ejercitan JS migrado.

### 5.4 Tests e2e (Playwright: el riesgo es de navegador, no de API)

| Fichero | Cubre |
|---|---|
| `test_ui_tour_csp_e2e.py` (7) | 17 pestañas admin; operator y owner; 16 modales por su botón real; modal de producto (sin botón en la UI, ver Fuera de alcance); modal estático de cambio de contraseña; login + 3 impresiones con la etiqueta dibujada |
| `test_js_behaviors_e2e.py` (16) | Seleccionar todo ×6; tema persistente; autoenvío del negocio; Enter → cantidad; moneda; 3 gráficos + tooltip sin XSS; panel del copiloto + widget sembrado; chat con widget OOB (`page.route`); impresión, copias y `auto_print`; mostrar contraseña; menú móvil |

Todos terminan con `assert js_errors == []`. Comando: `SESSION_SECRET=test OPERATOR_USERNAME=operator
OPERATOR_PASSWORD=test .venv/bin/python -m pytest tests/e2e -m e2e --no-cov -p no:warnings`.

## 6. Riesgos y mitigaciones (detalle en design §6)

| Riesgo | Mitigación |
|---|---|
| Baseline regenerada sobre código migrado (incidente fase 1) | T2 con `set -euo pipefail` + 3 guardas + worktree desde `HEAD` |
| Arnés ciego por las reglas nuevas | Reglas acotadas; 6 mutaciones detectadas; scripts por página en un unit test |
| Módulos viejos en caché | `no-cache` + hash de todos los assets, con tests |
| Widgets OOB sin dibujar | Verificado en htmx 1.9.12 + e2e con chat interceptado (mutación detectada) |
| Listeners duplicados en estados intermedios | Orden de lotes: los módulos se cargan en E y cada plantilla cambia en un solo paso |
| `wait_for_function` con expresión bloqueado por CSP | Forma de función; comentario en el e2e |
| Error al evaluar un módulo = app sin JS | Guarda `js_errors`; mutación `throw` detectada |
| Modo desktop sin módulos ES | WebView2/WebKitGTK los soportan; prueba manual en el paso 8 |

## 7. Definición de hecho (DoD)

### 7.1 Checklist (al terminar el lote I)

- [ ] 9 commits (uno por lote) en `feature/frontend-modular`, sin push; ninguno toca `VERSION`/`.bumpversion.cfg`.
- [ ] `grep -rnE '\son[a-z]+=' app/templates` → vacío; `test ! -e app/static/js/app.js`.
- [ ] `curl -sI localhost:<port>/health | grep -i content-security` → `script-src 'self';`.
- [ ] Unit: **423 passed**. Arnés: **OK: 40 snapshots equivalentes**. e2e: **70 passed**.
- [ ] Código commiteado idéntico al worktree validado (`diff -r -q -x .git -x .venv … /tmp/sdd-fase2-wt .`).
- [ ] Los commits solo tocan ficheros del inventario §3.2 (ningún cambio ajeno).

### 7.2 Script de verificación completa

```bash
set -euo pipefail
cd ~/PyCharmMiscProject/gestion_inventario
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -rfE tests/unit | tail -1
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline | tail -1
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
  .venv/bin/python -m pytest tests/e2e -m e2e --no-cov -p no:warnings -rfE | tail -1
! grep -rnE '\son[a-z]+=' app/templates
git diff --quiet b7a9881 -- VERSION .bumpversion.cfg
```

### 7.3 Commits esperados (mensajes en inglés)

`feat(frontend-fase2-js-modulos): lote A (T1-T2)` … `lote I (T29-T31)`, como en la fase 1.

### 7.4 Después (pasos 5-8 del loop)

Revisión con Opus (`/code-review` + `auditoria-ingenieria`, Serena + codebase-memory; plantillas y JS fuera de su
cobertura: grep/Read). Puntos para mirar con lupa: el canónico del arnés, `core/content.js` frente a la semántica
de `app.js`, el chat del copiloto y la prueba manual en dev, incluido el **modo desktop**.
