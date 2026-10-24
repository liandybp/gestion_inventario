"""Definición de los task envelopes del PAE-018 (fase 3: CSS sin estilos inline, tokens y paleta, CSP style-src 'self').

Fuente de verdad de ``gen_tasks.py``. Ops:
- ("write", path): fichero nuevo, contenido literal de ``gen/files/<path>``.
- ("write_text", path, texto): fichero nuevo con el texto dado (hojas generadas desde los <style> reales).
- ("append", path, texto): bloque literal al final de un fichero existente.
- ("script", texto): script bash literal (python3 heredoc con edit()/swap_block() que exigen 1 coincidencia).
Sin prototipo: los «antes» salen de leer los ficheros reales de BASE_COMMIT (``gen_tasks.py check`` lo comprueba en
memoria, sin escribir en el repo ni ejecutar la app).
"""

from __future__ import annotations

import re

import templates_gen as TG

BASE_COMMIT = TG.BASE_COMMIT

# style_snapshot.py se escribe en dos envelopes: líneas 1..SNAP_SPLIT (hasta el final de collect()) y el resto.
_SNAP_LINES = (TG.GEN / "files" / "tests/snapshots/style_snapshot.py").read_text(encoding="utf-8").splitlines()
SNAP_SPLIT = _SNAP_LINES.index("def collect() -> dict[str, dict]:") - 2

PY_HELPERS = '''import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)
'''


def py(body: str) -> str:
    return "python3 - <<'PY'\n" + PY_HELPERS + "\n" + body.strip("\n") + "\nprint(\"OK\")\nPY\n"


def edits_py(path: str, pairs: list[tuple[str, str]]) -> str:
    return "\n".join(f"edit({path!r}, {old!r}, {new!r})" for old, new in pairs)


UNIT = "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q"
E2E = "SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE"
SNAP = ".venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline"
STYLE_BASE = "/tmp/gi-fase3-style-baseline"
STYLE_CHECK = f".venv/bin/python tests/snapshots/style_snapshot.py check {STYLE_BASE}"
STYLE_CHECK_ALLOW = f"{STYLE_CHECK} --allow tests/snapshots/style_allowlist.txt"
NO_STYLE = "grep -rlE '\\sstyle\\s*=|<style' app/templates | wc -l"

TOOLS_WRITE = "Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills"
TOOLS_SCRIPT = "Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills"
TOOLS_APPEND = "Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna"

# ── Lote A: la red, antes de tocar app/ ─────────────────────────────────────────────────────────────────────

STYLE_BASELINE_SCRIPT = f"""set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
# Guardas: la baseline se genera SIEMPRE con el código anterior a la fase 3 ({BASE_COMMIT}), nunca sobre el migrado.
git diff --quiet {BASE_COMMIT} -- app/
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" {BASE_COMMIT}
ln -s "$REPO/.venv" "$WT/.venv"
cp tests/snapshots/style_snapshot.py "$WT/tests/snapshots/style_snapshot.py"
rm -rf {STYLE_BASE}
(cd "$WT" && .venv/bin/python tests/snapshots/style_snapshot.py write {STYLE_BASE})
git worktree remove --force "$WT"
test "$(ls {STYLE_BASE} | wc -l)" -eq 74
# Determinismo: el código de la rama (aún sin cambios en app/) debe dar exactamente la baseline.
.venv/bin/python tests/snapshots/style_snapshot.py check {STYLE_BASE}
"""

HTML_BASELINE_SCRIPT = f"""set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
git diff --quiet {BASE_COMMIT} -- app/
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" {BASE_COMMIT}
ln -s "$REPO/.venv" "$WT/.venv"
cp tests/snapshots/ui_snapshot.py tests/snapshots/style_classes.py "$WT/tests/snapshots/"
(cd "$WT" && .venv/bin/python tests/snapshots/ui_snapshot.py write tests/snapshots/baseline)
rm -rf tests/snapshots/baseline
cp -r "$WT/tests/snapshots/baseline" tests/snapshots/baseline
git worktree remove --force "$WT"
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
"""

GUARD_SCRIPT = py(r'''
edit("tests/e2e/conftest.py", """    real_errors = [e for e in errors if "Refused to load the script" in e or "Refused to execute" in e or "pageerror" in e]""",
"""    # "Content Security Policy": cualquier violación de CSP (scripts y, desde la fase 3, estilos inline:
    # Chromium dice "Applying inline style violates the following Content Security Policy directive…").
    real_errors = [e for e in errors if "Refused to load the script" in e or "Refused to execute" in e
                   or "Content Security Policy" in e or "pageerror" in e]""")
''')

CANON_SCRIPT = py(r'''
P = "tests/snapshots/ui_snapshot.py"
edit(P, """from app.settings import DATABASE_URL  # noqa: E402
""", """from app.settings import DATABASE_URL  # noqa: E402
from tests.snapshots.style_classes import STYLE_TO_CLASSES, style_key  # noqa: E402
""")
edit(P, """def _is_js_wiring_attr(name: str) -> bool:
    return name.startswith("on") or name.startswith("data-js-")
""", """def _is_js_wiring_attr(name: str) -> bool:
    return name.startswith("on") or name.startswith("data-js-")


# Fase 3 (CSS sin estilos inline): cada style="…" de la baseline se traduce a las clases que lo sustituyen
# (tests/snapshots/style_classes.py) y las clases se comparan ordenadas: el check prueba que cada estilo se
# cambió exactamente por sus clases. Se ignoran <style>, <link rel="stylesheet"> (qué hojas carga cada página
# lo fija un unit test) y <meta name="htmx-config">, y la clase muerta mb-10 (sin regla CSS hasta la fase 3)
# del formulario de borrado de stock.
def _canon_classes(tag: str, attr_map: dict) -> str | None:
    classes = (attr_map.get("class") or "").split()
    mapped = STYLE_TO_CLASSES.get(style_key(attr_map["style"])) if attr_map.get("style") else None
    if mapped:
        classes += mapped.split()
    if tag == "form" and attr_map.get("hx-post") == "/ui/stock/delete-selected" and "mb-10" in classes:
        classes.remove("mb-10")
    return " ".join(sorted(set(classes))) if classes else None
""")
edit(P, """        if tag == "script" and attr_map.get("type") != "application/json":
            self._skip_tag = "script"
            return
""", """        if tag == "script" and attr_map.get("type") != "application/json":
            self._skip_tag = "script"
            return
        if tag == "style":
            self._skip_tag = "style"
            return
        if (tag == "link" and attr_map.get("rel") == "stylesheet") or (
                tag == "meta" and attr_map.get("name") == "htmx-config"):
            return
""")
edit(P, """        parts = [tag]
        for name, value in sorted(attrs, key=lambda a: a[0]):""", """        parts = [tag]
        style_mapped = bool(attr_map.get("style")) and style_key(attr_map["style"]) in STYLE_TO_CLASSES
        classes = _canon_classes(tag, attr_map)
        attrs = [(n, v) for n, v in attrs if n != "class" and not (n == "style" and style_mapped)]
        if classes:
            attrs.append(("class", classes))
        for name, value in sorted(attrs, key=lambda a: a[0]):""")
''')

# ── Lote B: CSS (las reglas nuevas no cambian nada mientras las plantillas sigan con style=) ───────────────

TOKENS_SCRIPT = py(r'''
P = "app/static/css/app.css"
edit(P, """  --row-hover: rgba(59, 130, 246, 0.12);
}
""", """  --row-hover: rgba(59, 130, 246, 0.12);
  --badge-info-bg: #eef6ff;
  --badge-info-text: #0b2d42;
  /* Gráficos (components/chart-palette.js lee estas variables). Widgets del copiloto: */
  --chart-1: #0b2d42;
  --chart-2: #cf1f27;
  --chart-3: #2a6f97;
  --chart-4: #4c9a8c;
  --chart-5: #d97706;
  --chart-6: #7c3aed;
  --chart-7: #0891b2;
  --chart-8: #64748b;
  --chart-accent: #0b2d42;
  --chart-accent-fill: rgba(11, 45, 66, 0.08);
  --chart-grid: rgba(15, 23, 42, 0.06);
  --chart-slice-border: #ffffff;
  --chart-label-stroke: rgba(15, 23, 42, 0.6);
  --chart-label-fill: #ffffff;
  /* Gráficos de Inicio (iguales en los dos temas): */
  --home-chart-sales: rgba(11,45,66,0.85);
  --home-chart-purchases: rgba(203, 58, 66, 0.75);
  --home-chart-profit: rgba(16, 185, 129, 0.75);
  --home-chart-line-fill: rgba(11,45,66,0.10);
  --home-pie-1: rgba(11,45,66,0.85);
  --home-pie-2: rgba(203, 58, 66, 0.75);
  --home-pie-3: rgba(16, 185, 129, 0.75);
  --home-pie-4: rgba(59, 130, 246, 0.75);
  --home-pie-5: rgba(245, 158, 11, 0.75);
  --home-pie-6: rgba(99, 102, 241, 0.75);
  --home-pie-7: rgba(236, 72, 153, 0.75);
  --home-pie-8: rgba(34, 197, 94, 0.65);
  --home-pie-9: rgba(148, 163, 184, 0.75);
  --home-pie-10: rgba(15, 118, 110, 0.70);
  --home-pie-11: rgba(107, 114, 128, 0.55);
}
""")
edit(P, """  --row-hover: rgba(148, 163, 184, 0.15);
}
""", """  --row-hover: rgba(148, 163, 184, 0.15);
  --badge-info-bg: rgba(59, 130, 246, 0.15);
  --badge-info-text: #93c5fd;
  /* Paleta oscura de los widgets: 7 colores (--chart-8 sin valor corta la lista). */
  --chart-1: #60a5fa;
  --chart-2: #f87171;
  --chart-3: #34d399;
  --chart-4: #fbbf24;
  --chart-5: #a78bfa;
  --chart-6: #22d3ee;
  --chart-7: #94a3b8;
  --chart-8: initial;
  --chart-accent: #93c5fd;
  --chart-accent-fill: rgba(147, 197, 253, 0.12);
}
""")
''')

APP_CSS_BLOCK = """
/* ═══ Fase 3 (release 0.5.0): sin estilos inline ═══
   CSP style-src 'self': las plantillas no llevan style="…". Estas clases sustituyen a los antiguos atributos con
   los mismos valores; van al final para ganar a las reglas generales, como hacía el inline. */

/* htmx: includeIndicatorStyles=false (meta htmx-config); mismas reglas que inyectaba htmx 1.9. */
.htmx-indicator { opacity: 0; }
.htmx-request .htmx-indicator { opacity: 1; transition: opacity 200ms ease-in; }
.htmx-request.htmx-indicator { opacity: 1; transition: opacity 200ms ease-in; }

.w-3p { width: 3%; }
.w-5p { width: 5%; }
.w-6p { width: 6%; }
.w-7p { width: 7%; }
.w-8p { width: 8%; }
.w-9p { width: 9%; }
.w-12p { width: 12%; }
.w-14p { width: 14%; }
.w-15p { width: 15%; }
.w-16p { width: 16%; }
.w-23p { width: 23%; }
.w-62p { width: 62%; }
.w-100px { width: 100px; }
.w-110px { width: 110px; }
.w-120px { width: 120px; }
.w-140px { width: 140px; }
.w-150px { width: 150px; }
.w-auto { width: auto; }
.min-w-85 { min-width: 85px; }

.mt-0 { margin-top: 0; }
.mt-6 { margin-top: 6px; }
.mt-8 { margin-top: 8px; }
.mb-10 { margin-bottom: 10px; }
.mb-14 { margin-bottom: 14px; }
.ml-10 { margin-left: 10px; }
.my-8-10 { margin: 8px 0 10px 0; }
.my-6-10 { margin: 6px 0 10px 0; }

.d-inline-block { display: inline-block; }
.gap-8 { gap: 8px; }
.items-baseline { align-items: baseline; }
.cols-auto-1fr { grid-template-columns: auto 1fr; }
.no-underline { text-decoration: none; }
.scroll-48vh { max-height: 48vh; overflow-y: auto; }
.touch-scroll { -webkit-overflow-scrolling: touch; }

.msg-box { padding: 12px; border-radius: 10px; }
.msg-box-sm { padding: 10px; border-radius: 8px; }
.modal-container-760 { max-width: 760px; }
/* .filter-label (más arriba) fija margin-bottom: el antiguo inline margin:0 le ganaba. */
.filter-label.m-0 { margin: 0; }

.thumb { width: 36px; height: 36px; object-fit: cover; border-radius: 8px; border: 1px solid var(--brand-border); }
.thumb-lg { width: 64px; height: 64px; }

.badge-info {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 999px;
  background: var(--badge-info-bg);
  color: var(--badge-info-text);
  font-size: 12px;
  font-weight: 600;
}

.select-business { width: 100%; padding: 8px; border-radius: 10px; border: 1px solid var(--brand-border); }
.select-plain {
  width: 100%;
  padding: 8px;
  border: 1px solid var(--brand-border);
  border-radius: 8px;
  background: var(--brand-surface);
  color: var(--brand-text);
}
"""

DEAD_CLASS_SCRIPT = py(r'''
edit("app/templates/partials/stock_table.html", """        hx-include="#inventory_query,#inventory_location_code,#inventory_stock_filter,#inventory_category"
        class="mb-10">""", """        hx-include="#inventory_query,#inventory_location_code,#inventory_stock_filter,#inventory_category">""")
''')

# ── Lotes C-E: plantillas ──────────────────────────────────────────────────────────────────────────────────

MACRO_EDITS = {
    "app/templates/components/ui.html": [
        ("{% macro card(title=None, id=None, style=None, extra_class='') -%}",
         "{% macro card(title=None, id=None, extra_class='') -%}"),
        ("{% macro subcard(title=None, id=None, style=None, extra_class='') -%}",
         "{% macro subcard(title=None, id=None, extra_class='') -%}"),
        ('''  <div class="card{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}{%- if style %} style="{{ style }}"{%- endif -%}>''',
         '''  <div class="card{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}>'''),
        ('''  <div class="card subcard{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}{%- if style %} style="{{ style }}"{%- endif -%}>''',
         '''  <div class="card subcard{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}>'''),
    ],
    "app/templates/components/modal.html": [
        ("close_label=None, container_style=None) -%}", "close_label=None, container_class=None) -%}"),
        ('''  <div class="modal-container{% if wide %} modal-container-wide{% endif %}"{% if container_style %} style="{{ container_style }}"{% endif %}>''',
         '''  <div class="modal-container{% if wide %} modal-container-wide{% endif %}{% if container_class %} {{ container_class }}{% endif %}">'''),
    ],
    "app/templates/partials/transfer_bulk_modal.html": [
        ("container_style='max-width: 760px;')", "container_class='modal-container-760')"),
    ],
}

STYLE_BLOCK_FILES = {
    "app/templates/login.html": "login",
    "app/templates/transfer_print.html": "print",
    "app/templates/restock_print.html": "print",
    "app/templates/label_single_print.html": "label-print",
}


def template_pairs(rel: str) -> list[tuple[str, str]]:
    """Todas las ediciones de una plantilla, en orden: macros/bloque <style> primero, luego cada style=."""
    text = TG.base_text(rel)
    pairs = []
    for old, new in MACRO_EDITS.get(rel, []):
        assert text.count(old) == 1, (rel, old[:50])
        pairs.append((old, new))
        text = text.replace(old, new)
    if rel in STYLE_BLOCK_FILES:
        block, _ = TG.style_block(rel)
        new = TG.link_tag(STYLE_BLOCK_FILES[rel])
        pairs.append((block, new))
        text = text.replace(block, new)
    style_pairs, _ = TG.style_edits(text)
    return pairs + style_pairs


def template_script(files: list[str], part: tuple[int, int] | None = None) -> str:
    body = []
    for rel in files:
        pairs = template_pairs(rel)
        if part:
            pairs = pairs[part[0]:part[1]]
        body.append(edits_py(rel, pairs))
    return py("\n".join(body))


def tpl_env(id_, lot, files, reqs, part=None, title=None):
    names = " y ".join(f"`{f.split('/')[-1]}`" for f in files)
    n = sum(len(template_pairs(f)) for f in files) if not part else part[1] - part[0]
    return {
        "id": id_, "lot": lot, "reqs": reqs, "depends": "{cssblock} (clases en app.css)",
        "title": title or f"Plantillas sin `style=`: {names}", "files": files,
        "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
        "context": ("Cada `style=\"…\"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); "
                    "si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` ({cssblock}). "
                    f"Este envelope hace {n} ediciones; cada `edit()` exige exactamente 1 coincidencia."),
        "objective": f"Quitar los atributos `style` de {names} sin cambiar ids, `data-*` ni el resto del marcado.",
        "ops": [("script", template_script(files, part))],
        "verify": [f"grep -cE '\\sstyle\\s*=' {' '.join(files)}   # "
                   + ("0 en cada fichero" if not part else str(len(template_pairs(files[0])) - part[1])),
                   f"{UNIT} tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS"],
    }


# ── Lote F: JS ─────────────────────────────────────────────────────────────────────────────────────────────

BARCODE_WIRING_SCRIPT = py(r'''
B = "app/static/js/components/barcode.js"
edit(B, """// Códigos de barras: <svg class="barcode" data-code="..."> se renderiza con JsBarcode (global).
import { onContent } from '../core/content.js';
""", """// Códigos de barras: <svg class="barcode" data-code="..."> se dibuja sin estilos inline (barcode-svg.js).
import { onContent } from '../core/content.js';
import { renderBarcodeSvg } from './barcode-svg.js';
""")
edit(B, """      window.JsBarcode(svg, code, { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 });""",
        """      renderBarcodeSvg(svg, code, { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 });""")
L = "app/static/js/pages/label-print.js"
edit(L, """// CODE128 del SKU. SKU y nombre llegan en data-js-label-sku / data-js-label-name de #sheet.
""", """// CODE128 del SKU. SKU y nombre llegan en data-js-label-sku / data-js-label-name de #sheet.
import { renderBarcodeSvg } from '../components/barcode-svg.js';

""")
edit(L, """        window.JsBarcode(svg, sku, {""", """        renderBarcodeSvg(svg, sku, {""")
''')

WIDGET_SCRIPT = py(r'''
W = "app/static/js/components/widget-chart.js"
edit(W, """import { onContent } from '../core/content.js';

const PALETTE_DARK = ['#60a5fa', '#f87171', '#34d399', '#fbbf24', '#a78bfa', '#22d3ee', '#94a3b8'];
const PALETTE_LIGHT = ['#0b2d42', '#cf1f27', '#2a6f97', '#4c9a8c', '#d97706', '#7c3aed', '#0891b2', '#64748b'];
""", """import { onContent } from '../core/content.js';
import { cssList, cssVar, THEME_EVENT } from './chart-palette.js';
""")
edit(W, """      ctx.strokeStyle = 'rgba(15, 23, 42, 0.6)';""", """      ctx.strokeStyle = cssVar('--chart-label-stroke');""")
edit(W, """      ctx.fillStyle = '#ffffff';""", """      ctx.fillStyle = cssVar('--chart-label-fill');""")
edit(W, """  // Paleta armónica con la identidad de la app, adaptada al tema (claro/oscuro).
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const palette = isDark ? PALETTE_DARK : PALETTE_LIGHT;
  const accent = isDark ? '#93c5fd' : '#0b2d42';
  const accentFill = isDark ? 'rgba(147, 197, 253, 0.12)' : 'rgba(11, 45, 66, 0.08)';
""", """  // Paleta del tema activo (variables --chart-* de app.css).
  const palette = cssList('chart');
  const accent = cssVar('--chart-accent');
  const accentFill = cssVar('--chart-accent-fill');
""")
edit(W, """    borderColor = '#ffffff';""", """    borderColor = cssVar('--chart-slice-border');""")
edit(W, """      y: { beginAtZero: true, grid: { color: 'rgba(15, 23, 42, 0.06)' } },""",
        """      y: { beginAtZero: true, grid: { color: cssVar('--chart-grid') } },""")
edit(W, """export function init() {
  onContent(renderWidgetCharts);
}""", """export function init() {
  onContent(renderWidgetCharts);
  // Cambio de tema sin recarga: redibujar con la paleta nueva.
  document.addEventListener(THEME_EVENT, () => renderWidgetCharts(document));
}""")
''')

HOME_SCRIPT = py(r'''
H = "app/static/js/components/home-charts.js"
edit(H, """import { onContent } from '../core/content.js';

const charts = { monthly: null, dailyLine: null, pie: null };

const PIE_COLORS = [
  'rgba(11,45,66,0.85)',
  'rgba(203, 58, 66, 0.75)',
  'rgba(16, 185, 129, 0.75)',
  'rgba(59, 130, 246, 0.75)',
  'rgba(245, 158, 11, 0.75)',
  'rgba(99, 102, 241, 0.75)',
  'rgba(236, 72, 153, 0.75)',
  'rgba(34, 197, 94, 0.65)',
  'rgba(148, 163, 184, 0.75)',
  'rgba(15, 118, 110, 0.70)',
  'rgba(107, 114, 128, 0.55)',
];
""", """import { onContent } from '../core/content.js';
import { cssList, cssVar, THEME_EVENT } from './chart-palette.js';

const charts = { monthly: null, dailyLine: null, pie: null };
""")
edit(H, """        { label: 'Ventas', data: list(payload, 'sales'), backgroundColor: 'rgba(11,45,66,0.85)' },
        { label: 'Compras', data: list(payload, 'purchases'), backgroundColor: 'rgba(203, 58, 66, 0.75)' },
        { label: 'Utilidad', data: list(payload, 'profit'), backgroundColor: 'rgba(16, 185, 129, 0.75)' },""",
"""        { label: 'Ventas', data: list(payload, 'sales'), backgroundColor: cssVar('--home-chart-sales') },
        { label: 'Compras', data: list(payload, 'purchases'), backgroundColor: cssVar('--home-chart-purchases') },
        { label: 'Utilidad', data: list(payload, 'profit'), backgroundColor: cssVar('--home-chart-profit') },""")
edit(H, """          borderColor: 'rgba(11,45,66,0.85)',
          backgroundColor: 'rgba(11,45,66,0.10)',""", """          borderColor: cssVar('--home-chart-sales'),
          backgroundColor: cssVar('--home-chart-line-fill'),""")
edit(H, """    data: { labels, datasets: [{ data: values, backgroundColor: PIE_COLORS }] },""",
        """    data: { labels, datasets: [{ data: values, backgroundColor: cssList('home-pie') }] },""")
edit(H, """export function init() {
  onContent(enhance);
}""", """export function init() {
  onContent(enhance);
  // Cambio de tema sin recarga: redibujar con las variables del tema nuevo.
  document.addEventListener(THEME_EVENT, () => enhance(document, false));
}""")
''')

# ── Lote G: CSP estricta ───────────────────────────────────────────────────────────────────────────────────

CSP_SCRIPT = py(r'''
edit("app/templates/layouts/app_shell.html", """    <script src="/static/js/htmx.min.js?v={{ static_version() }}"></script>
""", """    {# CSP style-src 'self': htmx no inyecta su hoja de .htmx-indicator (las reglas están en app.css) ni copia
       el atributo style en el settle (lo bloquearía la CSP; el servidor ya no envía ninguno). #}
    <meta name="htmx-config" content='{"includeIndicatorStyles": false, "attributesToSettle": ["class", "width", "height"]}' />
    <script src="/static/js/htmx.min.js?v={{ static_version() }}"></script>
""")
edit("app/main.py", """    ``script-src 'self'`` sin ``'unsafe-inline'``: todo el JS vive en ``app/static/js/``
    (fase 2 del release 0.5.0). ``style-src`` conserva ``'unsafe-inline'`` hasta la fase 3.
""", """    ``script-src 'self'`` y ``style-src 'self'``, sin ``'unsafe-inline'``: todo el JS vive en
    ``app/static/js/`` (fase 2 del release 0.5.0) y todo el CSS en ``app/static/css/`` (fase 3).
""")
edit("app/main.py", """        "style-src 'self' 'unsafe-inline'; \"""", """        "style-src 'self'; \"""")
''')

# ── Lote H: e2e ────────────────────────────────────────────────────────────────────────────────────────────

THEME_E2E_SCRIPT = py(r'''
edit("tests/e2e/test_js_behaviors_e2e.py", """def test_theme_toggle_persists_across_reload(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    with page.expect_navigation():
        page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    with page.expect_navigation():
        page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    assert js_errors == []
""", """def test_theme_toggle_without_reload(tour_server, page: Page, js_errors: list[str]) -> None:
    # Fase 3: el tema cambia sin recargar (el estado de la página se conserva), los gráficos se
    # redibujan con la paleta del tema (variables --chart-* de app.css) y la elección persiste.
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "agent")
    first_color = ("window.Chart.getChart(document.querySelector('canvas[data-js-widget-chart]'))"
                   ".data.datasets[0].backgroundColor[0]")
    page.wait_for_function(f"() => !!window.Chart && !!({first_color})")
    assert page.evaluate(first_color) == "#0b2d42"
    page.evaluate("window.__noReload = 1")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    assert page.evaluate("window.__noReload") == 1
    assert page.evaluate("localStorage.getItem('gi-theme')") == "dark"
    page.wait_for_function(f"() => ({first_color}) === '#60a5fa'")
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    assert js_errors == []
""")
''')

# ── Lote I: cierre ─────────────────────────────────────────────────────────────────────────────────────────

FINAL_STYLE_SCRIPT = f"""set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# Si la baseline de {{stylebase}} ya no está (reinicio, /tmp limpiado), regenerarla igual que en T2, desde {BASE_COMMIT}.
if [ ! -d {STYLE_BASE} ] || [ "$(ls {STYLE_BASE} | wc -l)" -ne 74 ]; then
  REPO="$PWD"; WT="$(mktemp -d)/base"
  git worktree add --detach "$WT" {BASE_COMMIT}
  ln -s "$REPO/.venv" "$WT/.venv"
  cp tests/snapshots/style_snapshot.py "$WT/tests/snapshots/style_snapshot.py"
  rm -rf {STYLE_BASE}
  (cd "$WT" && .venv/bin/python tests/snapshots/style_snapshot.py write {STYLE_BASE})
  git worktree remove --force "$WT"
fi
.venv/bin/python tests/snapshots/style_snapshot.py check {STYLE_BASE} --allow tests/snapshots/style_allowlist.txt
"""

CHANGELOG_SCRIPT = py(r'''
edit("CHANGELOG.md", """  de JS o violación de CSP.

## 0.4.15""", """  de JS o violación de CSP.
- refactor(frontend): fase 3 — CSS sin estilos inline. Los 106 `style="…"` de 22 plantillas pasan a clases de
  `app.css` (bloque final «Fase 3»), los 4 `<style>` a `login.css`, `print.css` y `label-print.css`, y las macros
  `card`/`subcard`/`modal` pierden sus parámetros de estilo libre (`modal(container_class=)`). Colores por tokens:
  sin hex en las plantillas; en tema oscuro las miniaturas y el badge de reposición siguen el tema. La CSP pasa a
  `style-src 'self'` (sin `'unsafe-inline'`).
- fix(frontend): con la CSP estricta, JsBarcode en modo SVG dejaba el código de barras negro sobre negro (su fondo
  y sus barras van en `style`): `components/barcode-svg.js` dibuja la misma geometría con atributos de
  presentación. htmx ya no inyecta su hoja de `.htmx-indicator` ni copia `style` en el *settle* (meta `htmx-config`).
- feat(ui): el botón de tema cambia claro/oscuro sin recargar la página; los gráficos toman sus colores de variables
  CSS (`--chart-*`, `--home-*`) y se redibujan (`gi:themechange`).
- test(frontend): red de estilos calculados `tests/snapshots/style_snapshot.py` (74 capturas, claro y oscuro, contra
  el commit anterior a la fase; fuera de CI); el arnés HTML traduce cada `style` antiguo a sus clases; `js_errors`
  falla ante cualquier violación de CSP. +10 unit, +9 e2e.

## 0.4.15""")
''')


def _envs() -> list[dict]:
    E = []
    add = E.append
    add({"key": "snap1", "id": 0, "lot": "A", "reqs": "REQ-3", "depends": "none",
         "title": "Red de estilos calculados (1/2): `tests/snapshots/style_snapshot.py` y diferencias admitidas",
         "files": ["tests/snapshots/style_snapshot.py", "tests/snapshots/style_allowlist.txt"],
         "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
         "context": ("`style_allowlist.txt` = las únicas diferencias admitidas (REQ-6: badge de reposición de Inicio en tema oscuro). "
                     "Reutiliza, sin modificarlos: `tests/e2e/conftest.py::_boot_temp_server(prefix) -> (base_url, db_path, proc, tmp)`, "
                     "`_seed_stock(db_path)`, `_seed_tour(db_path) -> {'purchase': int, 'transfer_ref': str}`, y de "
                     "`tests/e2e/test_ui_tour_csp_e2e.py`: `TABS`, `MODALS`, `login(page, base_url)`, `open_tab(page, slug)`. "
                     "No forma parte de pytest ni de CI."),
         "objective": "Crear la primera mitad del arnés (propiedades y captura de estilos) y su lista de diferencias admitidas.",
         "ops": [("part", "tests/snapshots/style_snapshot.py", 1, SNAP_SPLIT), ("write", "tests/snapshots/style_allowlist.txt")],
         "verify": [f"wc -l < tests/snapshots/style_snapshot.py   # {SNAP_SPLIT}"]})
    add({"key": "snap2", "id": 0, "lot": "A", "reqs": "REQ-3", "depends": "{snap1}",
         "title": "Red de estilos calculados (2/2): `write`/`check` y punto de entrada",
         "files": ["tests/snapshots/style_snapshot.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_APPEND,
         "context": "Continúa el fichero de {snap1}, que termina en la función `_capture(page)`.",
         "objective": "Completar el arnés con `collect`, `write`, `check` (con `--allow`) y `__main__`.",
         "ops": [("part", "tests/snapshots/style_snapshot.py", SNAP_SPLIT + 1, None)],
         "verify": [".venv/bin/python -m py_compile tests/snapshots/style_snapshot.py && echo compile-ok   # compile-ok",
                    ".venv/bin/python tests/snapshots/style_snapshot.py 2>&1 | head -1   # primera línea del docstring (uso)"]})
    add({"key": "stylebase", "id": 0, "lot": "A", "reqs": "REQ-3", "depends": "{snap2}",
         "title": f"Baseline de estilos desde `{BASE_COMMIT}` (worktree, set -e) y prueba de determinismo",
         "files": ["tests/snapshots/style_snapshot.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT + " · tarda ~2 min",
         "context": (f"La baseline (~20 MB, 74 JSON) va a `{STYLE_BASE}` y NO se versiona. Se genera con el código de "
                     f"`{BASE_COMMIT}` en un worktree temporal; después el `check` sobre la rama (que aún no ha tocado `app/`) "
                     "debe dar OK: si no, el arnés no es determinista. No modifica ningún fichero del repo."),
         "objective": "Generar la baseline de estilos de la base y comprobar que el arnés es determinista.",
         "ops": [("script", STYLE_BASELINE_SCRIPT)],
         "verify": [f"ls {STYLE_BASE} | wc -l   # 74",
                    f"{STYLE_CHECK}   # OK: estilos calculados idénticos (0 diferencias intencionadas admitidas)",
                    "git status --short app/ tests/snapshots/baseline   # vacío"]})
    add({"key": "guard", "id": 0, "lot": "A", "reqs": "REQ-9", "depends": "none",
         "title": "Guarda `js_errors`: fallar ante cualquier violación de CSP",
         "files": ["tests/e2e/conftest.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
         "context": ("`js_errors` (fixture autouse, `tests/e2e/conftest.py`) recoge `pageerror` y `console.error`; su teardown "
                     "solo fallaba con textos de scripts. Chromium avisa de un estilo bloqueado con «Applying inline style "
                     "violates the following Content Security Policy directive…»."),
         "objective": "Que el teardown falle también ante cualquier mensaje con `Content Security Policy`.",
         "ops": [("script", GUARD_SCRIPT)],
         "verify": [f"{E2E} tests/e2e/test_js_behaviors_e2e.py   # PASS (aún con 'unsafe-inline' en estilos)"]})
    add({"key": "classes", "id": 0, "lot": "A", "reqs": "REQ-9", "depends": "none",
         "title": "Tabla de equivalencias `style` → clases (`tests/snapshots/style_classes.py`)",
         "files": ["tests/snapshots/style_classes.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
         "context": "Módulo de datos puro (dict + `style_key`). Lo importa el canónico de `ui_snapshot.py` en {canon}.",
         "objective": "Crear la tabla única de equivalencias de la fase 3.",
         "ops": [("write", "tests/snapshots/style_classes.py")],
         "verify": [".venv/bin/python -c \"from tests.snapshots.style_classes import STYLE_TO_CLASSES as M, style_key as k; "
                    "print(len(M), k(' width: 3%; '))\"   # 55 width:3%"]})
    add({"key": "canon", "id": 0, "lot": "A", "reqs": "REQ-9", "depends": "{classes}",
         "title": "Canónico del arnés HTML: `style` → clases; sin `<style>`, `<link>` ni meta `htmx-config`",
         "files": ["tests/snapshots/ui_snapshot.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
         "context": ("`tests/snapshots/ui_snapshot.py`: clase `_Canon(HTMLParser)`, `handle_starttag(tag, attrs)` construye "
                     "`parts` con los atributos ordenados y salta `_is_js_wiring_attr`; `_norm(text)` normaliza valores. "
                     "La baseline actual se generó sin estas reglas: {htmlbase} la regenera."),
         "objective": "Que el arnés compare cada `style` antiguo como sus clases equivalentes, en orden estable.",
         "ops": [("script", CANON_SCRIPT)],
         "verify": [".venv/bin/python -m py_compile tests/snapshots/ui_snapshot.py && echo compile-ok   # compile-ok"]})
    add({"key": "htmlbase", "id": 0, "lot": "A", "reqs": "REQ-9", "depends": "{canon}",
         "title": f"Regenerar la baseline HTML desde `{BASE_COMMIT}` (worktree, set -e)",
         "files": ["tests/snapshots/baseline/"], "pillars": "P1, P4, P5, P6, P7", "tools": TOOLS_SCRIPT,
         "context": ("Igual que en las fases 1 y 2: el arnés nuevo ({classes}+{canon}, copiados al worktree) sobre el código ANTERIOR a la "
                     "fase, en el mismo commit que el cambio del arnés. Ningún `write` sobre el repo fuera de este script."),
         "objective": "Baseline HTML nueva y `check` OK sobre la rama.",
         "ops": [("script", HTML_BASELINE_SCRIPT)],
         "verify": [f"{SNAP} 2>/dev/null | tail -1   # OK: 40 snapshots equivalentes",
                    "git status --short tests/snapshots/baseline | grep -vc '^ M'   # 0 (solo modificados)"]})
    add({"key": "cssblock", "id": 0, "lot": "B", "reqs": "REQ-2, REQ-4, REQ-6", "depends": "none",
         "title": "`app.css`: bloque final «Fase 3» (clases, indicador de htmx) y fuera la `mb-10` muerta",
         "files": ["app/static/css/app.css", "app/templates/partials/stock_table.html"], "pillars": "P1, P4, P5, P6",
         "tools": TOOLS_APPEND + " + Bash (script literal para `stock_table.html`)",
         "context": ("`app.css` termina en `.widget-table tbody tr:hover td { background: var(--row-hover); }` + salto de línea. "
                     "Las reglas no cambian nada visible mientras las plantillas sigan con `style=` (el inline gana). "
                     "`stock_table.html` usa `class=\"mb-10\"` sin regla: al definir `.mb-10` ese formulario ganaría 10 px, "
                     "por eso se quita aquí. `--badge-info-*` los define {tokens} (hasta entonces el badge sigue con su inline)."),
         "objective": "Añadir las clases que sustituyen a los `style=` y las reglas del indicador de htmx.",
         "ops": [("append", "app/static/css/app.css", APP_CSS_BLOCK), ("script", DEAD_CLASS_SCRIPT)],
         "verify": ["grep -c 'Fase 3 (release 0.5.0)' app/static/css/app.css   # 1",
                    "grep -c 'mb-10' app/templates/partials/stock_table.html   # 0",
                    f"{STYLE_CHECK} | tail -1   # OK"]})
    add({"key": "tokens", "id": 0, "lot": "B", "reqs": "REQ-6, REQ-7", "depends": "none",
         "title": "Tokens: badge de reposición y paletas de los gráficos (`:root` y tema oscuro)",
         "files": ["app/static/css/app.css"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
         "context": ("`:root` termina en `--row-hover: rgba(59, 130, 246, 0.12);` y `[data-theme=\"dark\"]` en "
                     "`--row-hover: rgba(148, 163, 184, 0.15);`. Valores = constantes JS actuales de `home-charts.js` y "
                     "`widget-chart.js`. `--chart-8: initial` deja la paleta oscura en 7 colores, como hoy."),
         "objective": "Definir los tokens nuevos sin cambiar ningún color en tema claro.",
         "ops": [("script", TOKENS_SCRIPT)],
         "verify": ["grep -c -- '--chart-1:' app/static/css/app.css   # 2", f"{STYLE_CHECK} | tail -1   # OK"]})
    css = TG.css_files()
    add({"key": "sheets", "id": 0, "lot": "B", "reqs": "REQ-2", "depends": "none",
         "title": "Hojas de login e impresión: `login.css` y `print.css`",
         "files": ["app/static/css/login.css", "app/static/css/print.css"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
         "context": ("Contenido = los `<style>` actuales de `login.html` y de `transfer_print.html`/`restock_print.html` (idénticos), "
                     "más las clases que usan esas páginas tras quitar sus `style=` (`restock_print` incluye `partials/restock_table.html`). "
                     "Aún no las enlaza nadie (lote E)."),
         "objective": "Crear las dos hojas.",
         "ops": [("write_text", "app/static/css/login.css", css["app/static/css/login.css"]),
                 ("write_text", "app/static/css/print.css", css["app/static/css/print.css"])],
         "verify": ["grep -c 'badge-info' app/static/css/print.css   # 1", "grep -c 'w-auto' app/static/css/login.css   # 1"]})
    add({"key": "labelsheet", "id": 0, "lot": "B", "reqs": "REQ-2", "depends": "none",
         "title": "Hoja de la etiqueta: `label-print.css`",
         "files": ["app/static/css/label-print.css"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
         "context": "Contenido = el `<style>` actual de `label_single_print.html`. Aún no la enlaza nadie (lote E).",
         "objective": "Crear la hoja de la etiqueta.",
         "ops": [("write_text", "app/static/css/label-print.css", css["app/static/css/label-print.css"])],
         "verify": ["grep -c '@page { size: 60mm 30mm; margin: 0; }' app/static/css/label-print.css   # 1"]})
    return E


TEMPLATE_GROUPS = [
    # (lote, ficheros, partes) — partes: rangos de ediciones para ficheros grandes.
    ("C", ["app/templates/partials/tab_transfers.html"], (0, 10)),
    ("C", ["app/templates/partials/tab_transfers.html"], (10, 15)),
    ("C", ["app/templates/partials/tab_transfers.html"], (15, 29)),
    ("C", ["app/templates/partials/tab_history.html", "app/templates/partials/stock_table.html"], None),
    ("C", ["app/templates/partials/purchase_panel.html"], None),
    ("D", ["app/templates/partials/sale_panel.html", "app/templates/partials/action_result.html"], None),
    ("D", ["app/templates/partials/tab_profit_items.html"], None),
    ("D", ["app/templates/partials/purchases_history.html", "app/templates/partials/sales_history.html"], None),
    ("D", ["app/templates/partials/restock_table.html", "app/templates/partials/products_history.html"], None),
    ("E", ["app/templates/partials/product_edit_form.html", "app/templates/partials/product_edit_form_inventory.html"], None),
    ("E", ["app/templates/dashboard.html", "app/templates/partials/extraction_edit_form.html"], None),
    ("E", ["app/templates/partials/sales_document_edit_form.html", "app/templates/partials/customer_detail.html"], None),
    ("E", ["app/templates/components/ui.html"], None),
    ("E", ["app/templates/components/modal.html", "app/templates/partials/transfer_bulk_modal.html"], None),
    ("E", ["app/templates/login.html"], None),
    ("E", ["app/templates/label_single_print.html"], None),
    ("E", ["app/templates/transfer_print.html"], None),
    ("E", ["app/templates/restock_print.html"], None),
]


def _resolve(E: list[dict]) -> list[dict]:
    for i, env in enumerate(E, 1):
        env["id"] = i
    names = {env["key"]: f"T{env['id']}" for env in E if "key" in env}

    def sub(text):
        return re.sub(r"\{(" + "|".join(names) + r")\}", lambda m: names[m.group(1)], text)

    for env in E:
        for field in ("depends", "context", "objective", "title"):
            env[field] = sub(env[field])
        env["ops"] = [tuple(sub(x) if isinstance(x, str) else x for x in op) for op in env["ops"]]
        env["verify"] = [sub(v) for v in env["verify"]]
    return E


def build() -> list[dict]:
    E = _envs()
    n = len(E)
    for lot, files, part in TEMPLATE_GROUPS:
        n += 1
        title = None
        if part:
            k = {0: 1, 10: 2, 15: 3}[part[0]]
            title = f"Plantillas sin `style=`: `{files[0].split('/')[-1]}` ({k}/3)"
        env = tpl_env(n, lot, files, "REQ-2, REQ-3, REQ-6", part, title)
        if files[0].endswith(("ui.html", "modal.html")):
            env["context"] += " Además: las macros pierden sus parámetros de estilo libre (`card`/`subcard` sin `style`; `modal(container_class=)`)."
            env["reqs"] = "REQ-2"
        if files[0].endswith(("login.html", "print.html")):
            env["context"] += " Además: el bloque `<style>` se sustituye por el `<link>` de su hoja (creada en el lote B)."
            env["depends"] = "{sheets}, {labelsheet} (hojas nuevas)"
        if part and part[0] > 0:
            env["depends"] = f"T{n - 1}"
        E.append(env)
    # Al final de cada lote de plantillas, la red de estilos: aquí se ve si una clase pierde contra otra regla.
    for i, env in enumerate(E):
        nxt = E[i + 1] if i + 1 < len(E) else None
        if env["lot"] in ("C", "D", "E") and (nxt is None or nxt["lot"] != env["lot"]):
            env["verify"].append(f"{STYLE_CHECK_ALLOW} | tail -1   # OK (cierre del lote {env['lot']}; si FALLO: PARAR y reportar)")
    n += 1
    E.append({"id": n, "lot": "F", "reqs": "REQ-5", "depends": "none",
              "title": "`components/barcode-svg.js`: código de barras sin `style`",
              "files": ["app/static/js/components/barcode-svg.js"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
              "context": ("JsBarcode 3.11.6 es global (`window.JsBarcode`). En modo objeto (`JsBarcode({}, code, opts)`) solo rellena "
                          "`obj.encodings` = `[{data: '1011…', text, options}]`. El módulo replica `prepareSVG`/`createGroup`/"
                          "`drawSvgBarcode`/`drawSVGText`/`calculateEncodingAttributes` con las mismas operaciones y usa "
                          "atributos de presentación (`fill`, `font-family`, `font-size`) en vez de `style`."),
              "objective": "Crear `renderBarcodeSvg(svg, code, options)`.",
              "ops": [("write", "app/static/js/components/barcode-svg.js")],
              "verify": ["node --check app/static/js/components/barcode-svg.js && echo syntax-ok   # syntax-ok"]})
    n += 1
    E.append({"id": n, "lot": "F", "reqs": "REQ-5", "depends": f"T{n - 1}",
              "title": "Historial de compras y etiqueta dibujan con `renderBarcodeSvg`",
              "files": ["app/static/js/components/barcode.js", "app/static/js/pages/label-print.js"],
              "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": "`renderBarcodeSvg` acepta las mismas opciones que `window.JsBarcode`; los dos llamadores ya envuelven la llamada en `try {} catch (e) {}`.",
              "objective": "Sustituir las dos llamadas a JsBarcode en modo SVG.",
              "ops": [("script", BARCODE_WIRING_SCRIPT)],
              "verify": ["node --check app/static/js/components/barcode.js && node --check app/static/js/pages/label-print.js && echo ok   # ok",
                         f"{UNIT} tests/unit/test_frontend_js_modules.py   # PASS (grafo de módulos sin huérfanos)"]})
    n += 1
    E.append({"id": n, "lot": "F", "reqs": "REQ-7, REQ-8", "depends": "{tokens} (tokens)",
              "title": "`components/chart-palette.js` y tema sin recarga (`features/theme.js`)",
              "files": ["app/static/js/components/chart-palette.js", "app/static/js/features/theme.js"],
              "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
              "context": ("`theme.js` (importado por `main.js`, exporta `init()`) hoy hace `location.reload()`. `theme-init.js` aplica "
                          "`localStorage['gi-theme']` antes del primer pintado. Hasta T{0} los gráficos aún no escuchan el evento: "
                          "cambiar de tema recolorea el CSS y los gráficos se actualizan al recargar.").format(n + 2),
              "objective": "Crear el módulo de paleta y emitir `gi:themechange` en vez de recargar.",
              "ops": [("write", "app/static/js/components/chart-palette.js"), ("write", "app/static/js/features/theme.js")],
              "verify": ["node --check app/static/js/components/chart-palette.js && node --check app/static/js/features/theme.js && echo ok   # ok"]})
    n += 1
    E.append({"id": n, "lot": "F", "reqs": "REQ-7, REQ-8", "depends": f"T{n - 1}",
              "title": "`widget-chart.js`: colores desde CSS y redibujo al cambiar de tema",
              "files": ["app/static/js/components/widget-chart.js"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": "`chart-palette.js` exporta `cssVar(name)`, `cssList(prefix)` y `THEME_EVENT`. `renderWidgetCharts(root)` ya destruye la instancia previa de cada canvas.",
              "objective": "Quitar las paletas y colores fijos del módulo.",
              "ops": [("script", WIDGET_SCRIPT)],
              "verify": ["node --check app/static/js/components/widget-chart.js && echo ok   # ok",
                         "grep -cE \"#[0-9a-fA-F]{3,8}|rgba\\(\" app/static/js/components/widget-chart.js   # 0"]})
    n += 1
    E.append({"id": n, "lot": "F", "reqs": "REQ-7, REQ-8", "depends": f"T{n - 2}",
              "title": "`home-charts.js`: colores desde CSS y redibujo al cambiar de tema",
              "files": ["app/static/js/components/home-charts.js"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": "`enhance(root, fromSwap)` redibuja los 3 gráficos de Inicio (`replaceChart` destruye el anterior).",
              "objective": "Quitar los colores fijos del módulo.",
              "ops": [("script", HOME_SCRIPT)],
              "verify": ["node --check app/static/js/components/home-charts.js && echo ok   # ok",
                         "grep -cE \"#[0-9a-fA-F]{3,8}|rgba\\(\" app/static/js/components/home-charts.js   # 0",
                         f"{E2E} tests/e2e/test_ui_tour_csp_e2e.py   # PASS"]})
    n += 1
    E.append({"id": n, "lot": "G", "reqs": "REQ-1, REQ-4", "depends": "lotes B-F",
              "title": "CSP `style-src 'self'` y meta `htmx-config`",
              "files": ["app/templates/layouts/app_shell.html", "app/main.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": ("htmx 1.9.12 lee `<meta name=\"htmx-config\">` al cargar, antes de inyectar su hoja de indicador. "
                          "`attributesToSettle` por defecto incluye `style`: en el *settle* copiaba con `setAttribute` el `style` que "
                          "Chart.js pone por CSSOM en los `<canvas>` de Inicio. Las reglas de `.htmx-indicator` ya están en `app.css` ({cssblock})."),
              "objective": "Quitar `'unsafe-inline'` de `style-src` sin que htmx escriba estilos inline.",
              "ops": [("script", CSP_SCRIPT)],
              "verify": ["grep -c \"style-src 'self'; \" app/main.py   # 1", f"{E2E} tests/e2e/test_ui_tour_csp_e2e.py   # PASS"]})
    n += 1
    E.append({"id": n, "lot": "G", "reqs": "REQ-1, REQ-2, REQ-4..REQ-9", "depends": f"T{n - 1}",
              "title": "Guardas estáticas `tests/unit/test_frontend_css.py`",
              "files": ["tests/unit/test_frontend_css.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
              "context": "Mismo estilo que `tests/unit/test_frontend_js_modules.py` (lee plantillas/JS del disco; CSP vía `TestClient(app).get('/health')` sin lifespan).",
              "objective": "Fijar con tests todo lo que la fase garantiza.",
              "ops": [("write", "tests/unit/test_frontend_css.py")],
              "verify": [f"{UNIT} tests/unit/test_frontend_css.py   # 10 passed", f"{UNIT}   # todo PASS"]})
    n += 1
    E.append({"id": n, "lot": "H", "reqs": "REQ-4, REQ-5, REQ-6, REQ-7, REQ-9", "depends": "lote G",
              "title": "e2e de la fase: `tests/e2e/test_css_csp_e2e.py`",
              "files": ["tests/e2e/test_css_csp_e2e.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_WRITE,
              "context": ("Fixtures existentes: `tour_server -> (base_url, ids)`, `page`, `js_errors` (autouse). Con la CSP estricta, "
                          "`wait_for_function` solo con funciones flecha (una expresión suelta usa `eval`, que la CSP bloquea)."),
              "objective": "Probar en el navegador lo que garantizan REQ-4..REQ-7 y la guarda.",
              "ops": [("write", "tests/e2e/test_css_csp_e2e.py")],
              "verify": [f"{E2E} tests/e2e/test_css_csp_e2e.py   # 9 passed"]})
    n += 1
    E.append({"id": n, "lot": "H", "reqs": "REQ-8", "depends": f"T{n - 1}",
              "title": "e2e del tema: sin recarga, redibujo con la paleta oscura y persistencia",
              "files": ["tests/e2e/test_js_behaviors_e2e.py"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": "`test_js_behaviors_e2e.py` ya importa `login, open_tab`. La siembra del tour crea un widget `pie` (primer color claro `#0b2d42`, oscuro `#60a5fa`).",
              "objective": "Sustituir el e2e que esperaba una navegación al cambiar de tema.",
              "ops": [("script", THEME_E2E_SCRIPT)],
              "verify": [f"{E2E} tests/e2e/test_js_behaviors_e2e.py   # PASS"]})
    n += 1
    E.append({"id": n, "lot": "I", "reqs": "REQ-3, REQ-6", "depends": "lotes A-H",
              "title": "`check` final de estilos calculados (con la CSP estricta y el JS nuevo)",
              "files": ["tests/snapshots/style_allowlist.txt"], "pillars": "P1, P4, P5, P6", "tools": TOOLS_SCRIPT,
              "context": ("Única diferencia admitida en las 74 capturas: el badge de reposición de Inicio en tema oscuro (REQ-6). Si el "
                          "`check` da FALLO: PARAR y reportar la salida completa; no tocar CSS ni plantillas para «hacerlo cuadrar»."),
              "objective": "Demostrar REQ-3: mismos estilos calculados que la base, salvo REQ-6.",
              "ops": [("script", FINAL_STYLE_SCRIPT)],
              "verify": [f"{STYLE_CHECK_ALLOW} | tail -1   # OK: estilos calculados idénticos (6 diferencias intencionadas admitidas)",
                         "git status --short tests/snapshots/style_allowlist.txt   # vacío (no se modifica)"]})
    n += 1
    E.append({"id": n, "lot": "I", "reqs": "REQ-9, REQ-10", "depends": f"T{n - 1}",
              "title": "CHANGELOG de la fase 3 (sin bump) y verificación completa",
              "files": ["CHANGELOG.md"], "pillars": "P1, P4, P5, P6, P7", "tools": TOOLS_SCRIPT,
              "context": "Release 0.5.0 agrupado: `VERSION` y `.bumpversion.cfg` no se tocan.",
              "objective": "Documentar la fase y pasar las tres redes.",
              "ops": [("script", CHANGELOG_SCRIPT)],
              "verify": [f"{SNAP} 2>/dev/null | tail -1   # OK: 40 snapshots equivalentes", f"{UNIT}   # todo PASS",
                         f"{E2E}   # todo PASS", f"{NO_STYLE}   # 0", "git status --short VERSION .bumpversion.cfg   # vacío"]})
    return _resolve(E)


LOTS = {
    "A": "red: estilos calculados, guarda CSP, arnés HTML — nada de app/",
    "B": "CSS: clases, tokens y hojas nuevas (aún sin efecto visible)",
    "C": "plantillas 1/3",
    "D": "plantillas 2/3",
    "E": "plantillas 3/3, macros, login e impresiones",
    "F": "JS: códigos de barras, paleta, tema sin recarga",
    "G": "CSP estricta + guardas unitarias",
    "H": "e2e",
    "I": "cierre: check visual final, CHANGELOG, suites",
}

ENVELOPES = build()
