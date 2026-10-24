"""Definición de los task envelopes del PAE-017 (fase 2: JS a módulos, CSP sin 'unsafe-inline').

Fuente de verdad del generador (`gen_tasks.py`) y del simulador (`simulate.sh`):
- op "write":  fichero literal, contenido leído del worktree validado (TARGET).
- op "append": bloque literal = TARGET menos BASE (el fichero de la base es prefijo exacto).
- op "part":   fichero literal en dos envelopes: (path, línea_inicio, línea_fin) 1-based inclusive.
- op "script": script bash literal (python3 heredoc con edit()/cut() que exigen 1 coincidencia).
"""

from pathlib import Path

BASE_COMMIT = "b7a9881"

PY_HELPERS = '''import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")
'''


def py(body: str) -> str:
    return "python3 - <<'PY'\n" + PY_HELPERS + "\n" + body.strip("\n") + "\nprint(\"OK\")\nPY\n"


UNIT = "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q"
SNAP = ".venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline"
E2E_ENV = "SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test"
E2E = f"{E2E_ENV} .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -rfE"
NODE = "node --check"

TOOLS_WRITE = "Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills"
TOOLS_SCRIPT = "Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills"
TOOLS_APPEND = "Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna"

# ── Scripts de edición (se ejecutan tal cual; el simulador usa el mismo texto) ────────────

HARNESS_SCRIPT = py(r'''
P = "tests/snapshots/ui_snapshot.py"
edit(P, """class _Canon(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tokens: list[str] = []

    def handle_decl(self, decl: str) -> None:
        self.tokens.append(f"<!{_norm(decl).lower()}>")

    def handle_starttag(self, tag: str, attrs: list) -> None:
        parts = [tag]
        for name, value in sorted(attrs, key=lambda a: a[0]):
            parts.append(name if value is None else f'{name}="{_norm(value)}"')
        self.tokens.append("<" + " ".join(parts) + ">")

    def handle_startendtag(self, tag: str, attrs: list) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        self.tokens.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        text = _norm(data)
        if text:
            self.tokens.append(text)
""", """# Fase 2 (JS a módulos): el canónico ignora el cableado JS, que cambia a propósito, para que
# la baseline generada con el código anterior a la fase siga sirviendo de referencia:
# - <script> ejecutables (inline o con src) y su contenido; los <script type="application/json">
#   (datos de los gráficos) sí cuentan. Qué scripts carga cada página lo fija un unit test.
# - atributos on* (onclick=…) y data-js-* (los enganches que los sustituyen).
# - los 3 <div id="…-debug"> de los gráficos de Inicio, eliminados en la fase 2.
_REMOVED_DEBUG_IDS = {"monthly-chart-debug", "monthly-sales-daily-line-debug", "monthly-sales-pie-debug"}


def _is_js_wiring_attr(name: str) -> bool:
    return name.startswith("on") or name.startswith("data-js-")


class _Canon(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tokens: list[str] = []
        self._skip_tag: str | None = None

    def handle_decl(self, decl: str) -> None:
        self.tokens.append(f"<!{_norm(decl).lower()}>")

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if self._skip_tag:
            return
        attr_map = dict(attrs)
        if tag == "script" and attr_map.get("type") != "application/json":
            self._skip_tag = "script"
            return
        if tag == "div" and attr_map.get("id") in _REMOVED_DEBUG_IDS:
            self._skip_tag = "div"
            return
        parts = [tag]
        for name, value in sorted(attrs, key=lambda a: a[0]):
            if _is_js_wiring_attr(name):
                continue
            parts.append(name if value is None else f'{name}="{_norm(value)}"')
        self.tokens.append("<" + " ".join(parts) + ">")

    def handle_startendtag(self, tag: str, attrs: list) -> None:
        self.handle_starttag(tag, attrs)
        if self._skip_tag == tag:
            self._skip_tag = None

    def handle_endtag(self, tag: str) -> None:
        if self._skip_tag:
            # Los elementos omitidos no tienen hijos del mismo tag (script; div vacío).
            if tag == self._skip_tag:
                self._skip_tag = None
            return
        self.tokens.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self._skip_tag:
            return
        text = _norm(data)
        if text:
            self.tokens.append(text)
""")
edit(P, """        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()""", """        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()
        add_widget(db, bid, type="pie", title=_hostile("Widget"), tool="snapshot", tool_args={},
                   payload={"labels": [_hostile("A"), "B"], "values": [1, 2]}, unit=None, created_by=None)""")
edit(P, "from app.db import get_session  # noqa: E402\n",
     "from app.agent.widgets import add_widget  # noqa: E402\nfrom app.db import get_session  # noqa: E402\n")
''')

BASELINE_SCRIPT = r'''set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
# Guardas: la baseline se genera SIEMPRE con el código anterior a la fase 2 (HEAD, que este
# lote aún no ha cambiado en app/), nunca sobre el código migrado.
git diff --quiet HEAD -- app/
test -e app/static/js/app.js
test ! -e app/static/js/main.js
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" HEAD
test ! -e "$WT/app/static/js/main.js"
cp tests/snapshots/ui_snapshot.py "$WT/tests/snapshots/ui_snapshot.py"
(cd "$WT" && "$REPO/.venv/bin/python" tests/snapshots/ui_snapshot.py write tests/snapshots/baseline)
rm -rf tests/snapshots/baseline
cp -r "$WT/tests/snapshots/baseline" tests/snapshots/baseline
git worktree remove --force "$WT"
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
'''

STATIC_VERSION_SCRIPT = py((Path(__file__).parent / "scripts" / "t14_static_version.py").read_text(encoding="utf-8"))

APP_SHELL_SCRIPT = py(r'''
P = "app/templates/layouts/app_shell.html"
edit(P, "{# Layout de la app (dashboard): tema + htmx, Chart.js, JsBarcode, app.css, app.js. #}",
     "{# Layout de la app (dashboard): tema (theme-init.js) + htmx, Chart.js, JsBarcode, app.css y main.js (módulo). #}")
edit(P, """    <script>
      // Aplicar el tema guardado ANTES del paint (evita flash de color).
      (function () {
        try {
          var t = localStorage.getItem("gi-theme") || "light";
          document.documentElement.setAttribute("data-theme", t);
        } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
      })();
    </script>
""", """    <script src="/static/js/theme-init.js?v={{ static_version() }}"></script>
""")
edit(P, '<script src="/static/js/app.js?v={{ static_version() }}" defer></script>',
     '<script type="module" src="/static/js/main.js?v={{ static_version() }}"></script>')
edit("tests/unit/test_ui_components.py",
     'APP_ASSETS = ("htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "app.js")',
     'APP_ASSETS = ("theme-init.js", "htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "main.js")')
''')

DELETE_APPJS_SCRIPT = r'''set -euo pipefail
git rm -q app/static/js/app.js
# Nadie debe seguir cargando app.js (las plantillas usan main.js / pages/*.js).
! grep -rn 'static/js/app\.js' app/templates app/routers
'''

DASHBOARD_SCRIPT = py(r'''
P = "app/templates/dashboard.html"
edit(P, '<select name="business_id" onchange="this.form.submit()" ', '<select name="business_id" data-js-autosubmit ')
edit(P, 'class="sidebar-theme" onclick="toggleTheme()" ', 'class="sidebar-theme" data-js-theme-toggle ')
edit(P, 'class="agent-fab" onclick="toggleAgentPanel()" ', 'class="agent-fab" data-js-agent-toggle ')
edit(P, 'class="agent-panel-size" onclick="cycleAgentPanelSize()" ', 'class="agent-panel-size" data-js-agent-size ')
edit(P, 'class="agent-panel-close" onclick="toggleAgentPanel()" ', 'class="agent-panel-close" data-js-agent-toggle ')
# El bloque <script> del chat (~100 líneas) pasa a static/js/features/agent-chat.js y features/theme.js.
cut(P, "    <script>\n      function toggleAgentPanel()", "    </script>\n")
''')

LOGIN_LABEL_SCRIPT = py(r'''
L = "app/templates/login.html"
cut(L, "      <script>\n", "      </script>\n")
edit(L, "    </div>\n{% endblock %}",
     '    </div>\n    <script type="module" src="/static/js/pages/login.js?v={{ static_version() }}"></script>\n{% endblock %}')
P = "app/templates/label_single_print.html"
cut(P, "    <script>\n      (function(){", "    </script>\n")
edit(P, '<div class="sheet" id="sheet"></div>',
     '<div class="sheet" id="sheet" data-js-label-sku="{{ label.sku or \'\' }}" data-js-label-name="{{ label.name or \'\' }}"></div>\n'
     '    <script type="module" src="/static/js/pages/label-print.js?v={{ static_version() }}"></script>')
''')

PRINT_PAGES_SCRIPT = py(r'''
for P in ("app/templates/restock_print.html", "app/templates/transfer_print.html"):
    edit(P, '<button type="button" onclick="window.print()">Imprimir</button>', '<button type="button" data-js-print>Imprimir</button>')
    edit(P, "    </style>\n{% endblock %}",
         '    </style>\n    <script type="module" src="/static/js/pages/print.js?v={{ static_version() }}"></script>\n{% endblock %}')
''')

HOME_CHARTS_SCRIPT = py(r'''
P = "app/templates/partials/home_charts.html"
for line in ('      <div id="monthly-sales-daily-line-debug" class="muted mt-8"></div>\n',
             '  <div id="monthly-sales-pie-debug" class="muted mt-8"></div>\n',
             '  <div id="monthly-chart-debug" class="muted mt-8"></div>\n'):
    edit(P, line, "")
''')


def select_all_script(pairs):
    lines = []
    for path, name in pairs:
        lines.append(
            f'edit("app/templates/partials/{path}",\n'
            f'     """onclick="const f=this.closest(\'form\'); if(!f) return; f.querySelectorAll(\'input[name={name}]\').forEach(cb=>cb.checked=this.checked);\\"""",\n'
            f'     \'data-js-select-all="{name}"\')'
        )
    return py("\n".join(lines))


SALE_CART_SCRIPT = py(r'''
edit("app/templates/partials/sale_panel.html",
     """onkeydown="if(event.key==='Enter'){event.preventDefault(); const q=this.form.querySelector('input[name=quantity]'); if(q){q.focus(); q.select && q.select();}}\"""",
     'data-js-enter-focus="quantity"')
edit("app/templates/components/sales_doc/cart_add.html",
     """onchange="document.getElementById('sales-doc-currency-hidden').value=this.value\"""",
     'data-js-mirror-to="sales-doc-currency-hidden"')
''')

CSP_SCRIPT = py((Path(__file__).parent / "scripts" / "t25_csp.py").read_text(encoding="utf-8"))

CHANGELOG_SCRIPT = None  # se rellena en gen_tasks.py con el bloque literal del TARGET

JS = "app/static/js/"

# ── Envelopes ─────────────────────────────────────────────────────────────────────────────
# Campos: id, lot, title, reqs, depends, files, pillars, tools, objective, context, ops, verify
ENVELOPES = [
    dict(id=1, lot="A", title="Arnés de snapshots: ignorar el cableado JS y sembrar un widget de gráfico",
         reqs="REQ-6", depends="none", files=["tests/snapshots/ui_snapshot.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_SCRIPT,
         objective="Que el canónico del arnés ignore `<script>` ejecutables, atributos `on*`/`data-js-*` y los 3 `#*-debug`, y que la siembra cree un widget de gráfico (cubre `widget_chart.html`).",
         context="`tests/snapshots/ui_snapshot.py` (fase 1): clase `_Canon(HTMLParser)` con `handle_decl/starttag/startendtag/endtag/data`; `_seed()` termina con `user = db.scalar(...operator)` + `db.commit()`. `app.agent.widgets.add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by)` hace su propio commit. `_hostile(label)` ya existe.",
         ops=[("script", HARNESS_SCRIPT)],
         verify=[f".venv/bin/python -c \"import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read())\"   # sin salida = OK"]),
    dict(id=2, lot="A", title="Regenerar la baseline desde el commit anterior a la fase (worktree, set -e)",
         reqs="REQ-6", depends="T1", files=["tests/snapshots/baseline/"], pillars="P1, P4, P5, P6, P7",
         tools="Bash (script literal) · **Skills:** ninguna — NO ejecutar `write` sobre el repo, solo este script",
         objective="Baseline nueva = arnés de T1 ejecutado sobre el código ANTERIOR a la fase 2, en el mismo commit que el cambio del arnés (H-307).",
         context="Lección de la fase 1: un `;` en vez de `&&` escribió una vez la baseline sobre el código migrado. Este script usa `set -euo pipefail` y tres guardas (sin cambios en `app/`, existe `app.js`, no existe `main.js`). El arnés fija su propia BD SQLite temporal y credenciales (no usa `.env`).",
         ops=[("bash", BASELINE_SCRIPT)],
         verify=[SNAP + "   # OK: 40 snapshots equivalentes", "git status --short tests/snapshots/baseline   # 13 .txt modificados (scripts y widget), ninguno nuevo ni borrado"]),
    dict(id=3, lot="B", title="`theme-init.js` y `core/content.js`", reqs="REQ-3, REQ-4", depends="T2",
         files=[JS + "theme-init.js", JS + "core/content.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Crear el script clásico de tema (pre-pintado) y el registro de enhancers de contenido.",
         context="Sustituyen al `<script>` inline de `layouts/app_shell.html` (tema) y a `runEnhancers` + `DOMContentLoaded`/`htmx:afterSwap` de `app.js` (misma semántica: doble `requestAnimationFrame` tras el swap). htmx 1.9.12 emite `htmx:afterSwap` sobre cada elemento swapeado, OOB incluidos.",
         ops=[("write", JS + "theme-init.js"), ("write", JS + "core/content.js")],
         verify=[f"{NODE} {JS}theme-init.js && {NODE} {JS}core/content.js && echo OK"]),
    dict(id=4, lot="B", title="`core/activity.js` y `core/tabs.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "core/activity.js", JS + "core/tabs.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulos la actividad de usuario (cabecera `X-User-Activity`), las pestañas, el reintento de carga inicial y el menú móvil.",
         context="Código de `app.js` (`markUserActivity`, `saveActiveTab`, `restoreActiveTab`, `setActiveTab`, `onTabClick`, bloque `DOMContentLoaded`) sin cambios de comportamiento. `restoreActiveTab()` se ejecuta al evaluar el módulo (antes de `DOMContentLoaded`, como el `defer` de `app.js`).",
         ops=[("write", JS + "core/activity.js"), ("write", JS + "core/tabs.js")],
         verify=[f"{NODE} {JS}core/activity.js && {NODE} {JS}core/tabs.js && echo OK"]),
    dict(id=5, lot="B", title="`core/modals.js` y `core/swap.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "core/modals.js", JS + "core/swap.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulos el cierre de modales, `[data-clear-target]`, el swap de errores 4xx y la conservación del scroll.",
         context="Código de `app.js` (`closeModal`, `onModalClose`, `onModalOverlayClick`, `onClearClick`, listeners `htmx:afterRequest`/`beforeRequest`/`beforeSwap`/`afterSwap`) sin cambios de comportamiento. Atributos que se conservan: `data-modal-close`, `data-modal-static`, `data-clear-target`, cabecera `X-Modal-Keep`.",
         ops=[("write", JS + "core/modals.js"), ("write", JS + "core/swap.js")],
         verify=[f"{NODE} {JS}core/modals.js && {NODE} {JS}core/swap.js && echo OK"]),
    dict(id=6, lot="B", title="`components/barcode.js` y `components/notes.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "components/barcode.js", JS + "components/notes.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulos el render de códigos de barras y las notas reutilizables.",
         context="`import { onContent } from '../core/content.js'` (T3). Código de `renderBarcodes`, `saveReusableNote`, `getReusableNotes`, `enhanceNoteInputs` de `app.js`. Se conservan `data-code`, `data-rendered`, `data-note-enhanced`, `data-note-save-listener` y la clave `reusable_notes`.",
         ops=[("write", JS + "components/barcode.js"), ("write", JS + "components/notes.js")],
         verify=[f"{NODE} {JS}components/barcode.js && {NODE} {JS}components/notes.js && echo OK"]),
    dict(id=7, lot="B", title="`components/product-suggest.js` y `components/form-helpers.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "components/product-suggest.js", JS + "components/form-helpers.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulo las sugerencias de producto y crear los enganches `data-js-*` que sustituyen a los 10 handlers de formulario inline.",
         context="`form-helpers.js` sustituye: 6 `onclick` de «seleccionar todo» → `data-js-select-all=\"<name>\"`; `onchange=\"this.form.submit()\"` → `data-js-autosubmit`; `onchange` de moneda en `cart_add.html` → `data-js-mirror-to=\"<id>\"`; `onkeydown` Enter de `sale_panel.html` → `data-js-enter-focus=\"quantity\"`. Listeners delegados en `document` (los partials llegan por htmx).",
         ops=[("write", JS + "components/product-suggest.js"), ("write", JS + "components/form-helpers.js")],
         verify=[f"{NODE} {JS}components/product-suggest.js && {NODE} {JS}components/form-helpers.js && echo OK"]),
    dict(id=8, lot="C", title="`components/home-charts.js` (sin depuración, tooltip sin XSS)", reqs="REQ-3, REQ-4, REQ-5, REQ-10", depends="T3",
         files=[JS + "components/home-charts.js"], pillars="P1, P2, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulo los 3 gráficos de Inicio, sin `setDebug`/`#*-debug`/sondeo de `window.Chart`, y con el tooltip del pastel construido con `textContent`.",
         context="Datos en `<script type=\"application/json\" id=\"monthly-chart-data|monthly-sales-daily-line-data|monthly-sales-pie-data\">` (`partials/home_charts.html`). `window.Chart` es un script clásico del `<head>`: ya existe al evaluar el módulo. Antes el tooltip hacía `innerHTML` con la etiqueta `SKU - nombre` (XSS al pasar el ratón).",
         ops=[("write", JS + "components/home-charts.js")],
         verify=[f"{NODE} {JS}components/home-charts.js && ! grep -ni 'debug\\|innerHTML' {JS}components/home-charts.js && echo OK"]),
    dict(id=9, lot="C", title="`components/widget-chart.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "components/widget-chart.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Sustituir el `<script>` inline de `partials/widget_chart.html` por un enhancer que lee `data-js-widget-chart`.",
         context="El canvas pasará a llevar `data-js-widget-chart='{\"type\",\"title\",\"payload\":{labels,values}}'` (T20). Paleta, opciones y plugin de porcentajes idénticos al inline. Los widgets nuevos llegan por swap OOB (`#agent-grid`); `onContent` los recibe porque htmx emite `afterSwap` sobre cada elemento OOB.",
         ops=[("write", JS + "components/widget-chart.js")],
         verify=[f"{NODE} {JS}components/widget-chart.js && echo OK"]),
    dict(id=10, lot="C", title="`features/transfers.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "features/transfers.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Mover a módulo la lógica de traspasos (líneas, stock, modal masivo, recarga tras `transferBulkSuccess`).",
         context="Código de `app.js` (`onTransferProductChange`, `onTransferAddLine`, `onTransferRemoveLine`, `onTransferToggleEdit`, `_toggleBulkRow`, `onTransferBulkCheckboxChange`, `onTransferBulkSelectAll`, `onTransferBulkSearch`, listener `transferBulkSuccess`) sin cambios. Se conservan `data-transfer-*`, `#transfer-bulk-*`, `.transfer-bulk-*`, `data-search`.",
         ops=[("write", JS + "features/transfers.js")],
         verify=[f"{NODE} {JS}features/transfers.js && echo OK"]),
    dict(id=11, lot="D", title="`features/agent-chat.js` y `features/theme.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "features/agent-chat.js", JS + "features/theme.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Sustituir el `<script>` inline de `dashboard.html` (chat del copiloto y tema) por dos módulos.",
         context="`dashboard.html` usará `data-js-agent-toggle` (burbuja y ✕), `data-js-agent-size` (⤢) y `data-js-theme-toggle` (🌓) en lugar de `onclick=\"toggleAgentPanel()|cycleAgentPanelSize()|toggleTheme()\"`. El chat solo existe para admin/owner fuera de modo desktop: `init()` no hace nada si no hay `#agent-panel`.",
         ops=[("write", JS + "features/agent-chat.js"), ("write", JS + "features/theme.js")],
         verify=[f"{NODE} {JS}features/agent-chat.js && {NODE} {JS}features/theme.js && echo OK"]),
    dict(id=12, lot="D", title="`main.js` (entrada) y `pages/login.js`", reqs="REQ-3, REQ-4", depends="T4, T5, T6, T7, T8, T9, T10, T11",
         files=[JS + "main.js", JS + "pages/login.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Crear el punto de entrada que importa e inicializa todos los módulos y el módulo del login.",
         context="Todos los módulos exportan `init()`. Orden de enhancers = el de `runEnhancers` de `app.js` (barcode → gráficos → notas). `pages/login.js` sustituye el `<script>` inline de `login.html` (casilla «Mostrar contraseña»).",
         ops=[("write", JS + "main.js"), ("write", JS + "pages/login.js")],
         verify=[f"{NODE} {JS}main.js && {NODE} {JS}pages/login.js && echo OK"]),
    dict(id=13, lot="D", title="`pages/print.js` y `pages/label-print.js`", reqs="REQ-3, REQ-4", depends="T3",
         files=[JS + "pages/print.js", JS + "pages/label-print.js"], pillars="P1, P4, P5, P6",
         tools=TOOLS_WRITE, objective="Módulos de las páginas de impresión: botón `[data-js-print]` y la etiqueta individual (copias, CODE128, `auto_print=1`).",
         context="`label_single_print.html` pasará SKU y nombre en `data-js-label-sku`/`data-js-label-name` de `#sheet` (antes: `tojson` dentro del `<script>` inline). `JsBarcode` es un script clásico del `<head>` de esa página.",
         ops=[("write", JS + "pages/print.js"), ("write", JS + "pages/label-print.js")],
         verify=[f"{NODE} {JS}pages/print.js && {NODE} {JS}pages/label-print.js && echo OK"]),
    dict(id=14, lot="D", title="`static_version()` con el hash de todos los `.css`/`.js`", reqs="REQ-7", depends="none",
         files=["app/routers/ui_common.py", "tests/unit/test_ui_regressions.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_SCRIPT, objective="Que el `?v=` cambie si cambia cualquier módulo (también los anidados que `main.js` importa sin `?v=`), y que el test no dependa de `app.js`.",
         context="`_static_asset_version() -> str` (`app/routers/ui_common.py`) hoy calcula el hash de 5 ficheros fijos, incluido `js/app.js`; se registra como global Jinja `static_version` (se llama sin argumentos). El parámetro nuevo `static_dir` solo lo usa el test.",
         ops=[("script", STATIC_VERSION_SCRIPT)],
         verify=[UNIT + " tests/unit/test_ui_regressions.py   # PASS"]),
    dict(id=15, lot="E", title="`layouts/app_shell.html`: `theme-init.js` + `main.js` como módulo", reqs="REQ-1, REQ-3", depends="T12, T14",
         files=["app/templates/layouts/app_shell.html", "tests/unit/test_ui_components.py"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="El layout carga `theme-init.js` (clásico, síncrono) y `main.js` (`type=\"module\"`) en lugar del inline de tema y de `app.js`.",
         context="Los scripts clásicos (htmx, Chart.js, JsBarcode) siguen igual y antes que el módulo. `test_app_shell_loads_all_app_assets` comprueba `APP_ASSETS`.",
         ops=[("script", APP_SHELL_SCRIPT)],
         verify=[UNIT + " tests/unit/test_ui_components.py   # PASS", SNAP + "   # OK: 40"]),
    dict(id=16, lot="E", title="Borrar `app/static/js/app.js`", reqs="REQ-3, REQ-5", depends="T15",
         files=["app/static/js/app.js"], pillars="P1, P4, P6",
         tools="Bash (script literal) · **Skills:** ninguna", objective="Eliminar `app.js` (todo su código vive ya en los módulos; `debugReturnLots` se elimina).",
         context="Tras T15 ninguna plantilla carga `app.js`.", ops=[("bash", DELETE_APPJS_SCRIPT)],
         verify=["test ! -e app/static/js/app.js && echo OK"]),
    dict(id=17, lot="E", title="`dashboard.html`: `data-js-*` y fuera el `<script>` inline", reqs="REQ-1, REQ-4, REQ-6", depends="T11, T15",
         files=["app/templates/dashboard.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="Sustituir los 5 handlers inline por `data-js-*` y borrar el bloque `<script>` del chat (~100 líneas).",
         context="Ids que NO cambian: `agent-fab`, `agent-panel`, `agent-chat-thread`, `agent-chat-form`, `tab-*`, `tab-content`, `modal-container`, `sidebar`, `mobile-menu-*`.",
         ops=[("script", DASHBOARD_SCRIPT)],
         verify=["! grep -nE '<script>|on(click|change)=' app/templates/dashboard.html && echo OK", SNAP + "   # OK: 40"]),
    dict(id=18, lot="E", title="`login.html` y `label_single_print.html` con módulo de página", reqs="REQ-1, REQ-4, REQ-6", depends="T12, T13",
         files=["app/templates/login.html", "app/templates/label_single_print.html"], pillars="P1, P2, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="Quitar los `<script>` inline y cargar `pages/login.js` / `pages/label-print.js`; SKU y nombre de la etiqueta por `data-js-*` (autoescapados).",
         context="`label_single_print.html` sigue cargando `JsBarcode.all.min.js` en su `{% block head %}`. Los ids `login-password`, `login-show-password`, `copies`, `btn-print`, `sheet` no cambian.",
         ops=[("script", LOGIN_LABEL_SCRIPT)],
         verify=["! grep -n '<script>' app/templates/login.html app/templates/label_single_print.html && echo OK", SNAP + "   # OK: 40"]),
    dict(id=19, lot="E", title="Impresión de traspaso y reposición: `data-js-print` + `pages/print.js`", reqs="REQ-1, REQ-4", depends="T13",
         files=["app/templates/restock_print.html", "app/templates/transfer_print.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="Sustituir `onclick=\"window.print()\"` por `data-js-print` y cargar `pages/print.js` (solo eso: las impresiones no cargan htmx/Chart/app).",
         context="Ambas plantillas extienden `layouts/print.html` y declaran su `<style>` en `{% block head %}`.",
         ops=[("script", PRINT_PAGES_SCRIPT)],
         verify=["! grep -n 'onclick' app/templates/restock_print.html app/templates/transfer_print.html && echo OK", SNAP + "   # OK: 40"]),
    dict(id=20, lot="F", title="`widget_chart.html` con `data-js-widget-chart` y `home_charts.html` sin `#*-debug`", reqs="REQ-1, REQ-5, REQ-6", depends="T8, T9",
         files=["app/templates/partials/widget_chart.html", "app/templates/partials/home_charts.html"], pillars="P1, P2, P4, P5, P6",
         tools="Write (widget_chart literal), Bash (script de home_charts + verificación) · **Skills:** ninguna",
         objective="El widget de gráfico pasa su especificación en un atributo (comillas SIMPLES: `tojson` escapa `< > & '` pero no `\"`); fuera los 3 `<div id=\"…-debug\">`.",
         context="`w` = `{id, type, title, payload}` (`app.agent.widgets.widgets_with_payload`). Ids `chart-{{ w.id }}`, `monthly-*` (salvo los `-debug`) no cambian.",
         ops=[("write", "app/templates/partials/widget_chart.html"), ("script", HOME_CHARTS_SCRIPT)],
         verify=["! grep -n '<script>\\|-debug' app/templates/partials/widget_chart.html app/templates/partials/home_charts.html && echo OK", SNAP + "   # OK: 40"]),
    dict(id=21, lot="F", title="«Seleccionar todo» (1/3): `purchases_history` y `sales_history`", reqs="REQ-1, REQ-4", depends="T7",
         files=["app/templates/partials/purchases_history.html", "app/templates/partials/sales_history.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="`onclick` de la casilla maestra → `data-js-select-all=\"<name>\"`.",
         context="El `name` es el de las casillas de fila del mismo form (`movement_ids` / `sale_ids`).",
         ops=[("script", select_all_script([("purchases_history.html", "movement_ids"), ("sales_history.html", "sale_ids")]))],
         verify=["! grep -n 'onclick' app/templates/partials/purchases_history.html app/templates/partials/sales_history.html && echo OK"]),
    dict(id=22, lot="F", title="«Seleccionar todo» (2/3): `stock_table` y `tab_history`", reqs="REQ-1, REQ-4", depends="T7",
         files=["app/templates/partials/stock_table.html", "app/templates/partials/tab_history.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="`onclick` de la casilla maestra → `data-js-select-all=\"<name>\"`.",
         context="`skus` (stock) / `movement_ids` (historial).",
         ops=[("script", select_all_script([("stock_table.html", "skus"), ("tab_history.html", "movement_ids")]))],
         verify=["! grep -n 'onclick' app/templates/partials/stock_table.html app/templates/partials/tab_history.html && echo OK"]),
    dict(id=23, lot="F", title="«Seleccionar todo» (3/3): `tab_transfers` y `tab_profit_items`", reqs="REQ-1, REQ-4", depends="T7",
         files=["app/templates/partials/tab_transfers.html", "app/templates/partials/tab_profit_items.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="`onclick` de la casilla maestra → `data-js-select-all=\"<name>\"`.",
         context="`movement_ids` (traspasos) / `sale_ids` (utilidades por artículo).",
         ops=[("script", select_all_script([("tab_transfers.html", "movement_ids"), ("tab_profit_items.html", "sale_ids")]))],
         verify=["! grep -n 'onclick' app/templates/partials/tab_transfers.html app/templates/partials/tab_profit_items.html && echo OK"]),
    dict(id=24, lot="F", title="`sale_panel` (Enter → cantidad) y `cart_add` (moneda)", reqs="REQ-1, REQ-4", depends="T7",
         files=["app/templates/partials/sale_panel.html", "app/templates/components/sales_doc/cart_add.html"], pillars="P1, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="`onkeydown` → `data-js-enter-focus=\"quantity\"`; `onchange` → `data-js-mirror-to=\"sales-doc-currency-hidden\"`.",
         context="`#sales-doc-currency-hidden` está en `components/sales_doc/issue_form.html` (no cambia).",
         ops=[("script", SALE_CART_SCRIPT)],
         verify=["! grep -rnE '\\son[a-z]+=' app/templates && echo 'OK: 0 handlers inline'", SNAP + "   # OK: 40"]),
    dict(id=25, lot="G", title="CSP `script-src 'self'` y `Cache-Control: no-cache` en `/static/js/`", reqs="REQ-2, REQ-7", depends="T15-T24",
         files=["app/main.py"], pillars="P1, P2, P4, P5, P6",
         tools=TOOLS_SCRIPT, objective="Quitar `'unsafe-inline'` de `script-src` (sin tocar `style-src`, fase 3) y revalidar siempre los módulos.",
         context="`security_headers_middleware(request, call_next)` en `app/main.py` (H-006) pone las cabeceras a TODAS las respuestas, incluidas las de `/static` (`StaticFiles` sirve con ETag/Last-Modified → 304).",
         ops=[("script", CSP_SCRIPT)],
         verify=["grep -n \"script-src 'self'; \" app/main.py && echo OK"]),
    dict(id=26, lot="G", title="Unit tests de la fase 2 (guardas estáticas)", reqs="REQ-1, REQ-2, REQ-3, REQ-5, REQ-7, REQ-11", depends="T25",
         files=["tests/unit/test_frontend_js_modules.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_WRITE, objective="Tests: CSP, `no-cache`, 0 handlers inline (plantillas y HTML en Python), scripts exactos por plantilla, grafo de módulos sin roturas ni huérfanos, sin depuración, escape de `data-js-*`.",
         context="`TestClient(app)` SIN `with` (no arranca el lifespan, que exige credenciales); el middleware corre igual. `templates` = `app.routers.ui_common.templates`.",
         ops=[("write", "tests/unit/test_frontend_js_modules.py")],
         verify=[UNIT + " tests/unit/test_frontend_js_modules.py   # 9 passed", UNIT + " tests/unit   # todo PASS"]),
    dict(id=27, lot="H", title="Fixtures e2e: siembra del recorrido y guarda `js_errors`", reqs="REQ-8, REQ-9", depends="T26",
         files=["tests/e2e/conftest.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_APPEND, objective="Añadir `_seed_tour`, `tour_server` y `js_errors` al final de `tests/e2e/conftest.py`.",
         context="Ya existen en el fichero: `_boot_temp_server(prefix, *, extra_env=None) -> (base_url, db_path, proc, tmp)`, `_seed_stock(db_path)`, imports `create_engine, select, sessionmaker, hash_password, Business, Location, Product, User, datetime, timezone, shutil, subprocess, pytest`. Servicios: `InventoryService(db, business_id).purchase/transfer/sale/adjustment(...)`.",
         ops=[("append", "tests/e2e/conftest.py")],
         verify=[".venv/bin/python -c \"import ast; ast.parse(open('tests/e2e/conftest.py').read())\" && echo OK"]),
    dict(id=28, lot="H", title="e2e: recorrido de pestañas, modales, login e impresiones sin errores de JS/CSP", reqs="REQ-8, REQ-9", depends="T27",
         files=["tests/e2e/test_ui_tour_csp_e2e.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_WRITE, objective="Recorrer las 17 pestañas (admin), operator y owner, los 18 modales (abrir y cerrar con ×), login y las 3 impresiones; fallar ante cualquier `console.error`/excepción/violación de CSP.",
         context="Fixtures de T27: `tour_server` → `(base_url, ids)`, `js_errors` → lista. Con `script-src 'self'`, `page.wait_for_function(\"expresión\")` hace `eval` al sondear y el CSP lo bloquea: usar siempre la forma de función.",
         ops=[("write", "tests/e2e/test_ui_tour_csp_e2e.py")],
         verify=[E2E + " tests/e2e/test_ui_tour_csp_e2e.py   # 7 passed"]),
    dict(id=29, lot="I", title="e2e de comportamientos migrados (1/2)", reqs="REQ-4, REQ-9", depends="T28",
         files=["tests/e2e/test_js_behaviors_e2e.py"], pillars="P1, P4, P5, P6, P7",
         tools=TOOLS_WRITE, objective="Primera mitad: «seleccionar todo» ×6, tema, autoenvío del negocio, Enter → cantidad, moneda del documento.",
         context="Importa `login`, `open_tab` de `tests/e2e/test_ui_tour_csp_e2e.py` (T28) y `TOUR_PRODUCT_NAME`, `TOUR_WIDGET_TITLE` de `tests/e2e/conftest.py` (T27).",
         ops=[("part", "tests/e2e/test_js_behaviors_e2e.py", 1, 103)],
         verify=[".venv/bin/python -c \"import ast; ast.parse(open('tests/e2e/test_js_behaviors_e2e.py').read())\" && echo OK"]),
    dict(id=30, lot="I", title="e2e de comportamientos migrados (2/2)", reqs="REQ-4, REQ-9, REQ-10", depends="T29",
         files=["tests/e2e/test_js_behaviors_e2e.py"], pillars="P1, P2, P4, P5, P6, P7",
         tools=TOOLS_APPEND, objective="Segunda mitad: gráficos de Inicio + tooltip sin XSS, panel del copiloto + widget, chat con widget OOB, impresión y copias, mostrar contraseña, menú móvil.",
         context="El chat del copiloto llamaría al LLM: el test intercepta `POST /ui/agent/chat` con `page.route` y devuelve la plantilla real `partials/agent_chat_turn.html` renderizada con un widget.",
         ops=[("part", "tests/e2e/test_js_behaviors_e2e.py", 104, None)],
         verify=[E2E + " tests/e2e/test_js_behaviors_e2e.py   # 16 passed", E2E + " tests/e2e   # 70 passed"]),
    dict(id=31, lot="I", title="CHANGELOG de la fase 2 (sin bump de versión)", reqs="REQ-12", depends="T30",
         files=["CHANGELOG.md"], pillars="P1, P6",
         tools=TOOLS_SCRIPT, objective="Añadir la entrada de la fase 2 bajo `## 0.5.0 (sin publicar …)`. NO tocar `VERSION` ni `.bumpversion.cfg`.",
         context="La sección `## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)` ya existe; su última viñeta termina en «…en vez de renderizar un modal roto.» y le sigue `## 0.4.15`.",
         ops=[("changelog", "CHANGELOG.md")],
         verify=["git diff --quiet HEAD -- VERSION .bumpversion.cfg && grep -c 'fase 2 — todo el JS' CHANGELOG.md   # 1"]),
]

LOTS = {
    "A": "arnés + baseline (ANTES de tocar plantillas)",
    "B": "módulos núcleo y componentes de formulario",
    "C": "gráficos y traspasos",
    "D": "chat/tema, entrada, páginas y static_version",
    "E": "cableado de layout, dashboard, login e impresiones",
    "F": "cableado de partials",
    "G": "CSP y guardas unitarias",
    "H": "e2e de recorrido",
    "I": "e2e de comportamientos y CHANGELOG",
}
