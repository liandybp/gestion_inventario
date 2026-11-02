"""Fase 3 del release 0.5.0: sin estilos inline, colores por tokens CSS, CSP ``style-src 'self'``.

Guardas estáticas (sin navegador). El comportamiento en el navegador lo cubren los e2e
``tests/e2e/test_ui_tour_csp_e2e.py`` (recorrido con la CSP estricta) y ``tests/e2e/test_css_csp_e2e.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = ROOT / "app" / "templates"
JS_DIR = ROOT / "app" / "static" / "js"
CSS_DIR = ROOT / "app" / "static" / "css"
VENDOR = {"htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js"}

STYLE_ATTR = re.compile(r"\sstyle\s*=", re.IGNORECASE)
STYLE_TAG = re.compile(r"<style\b", re.IGNORECASE)
HEX = re.compile(r"#[0-9a-fA-F]{3,8}\b")
LINK = re.compile(r'<link rel="stylesheet" href="([^"?]+)')

# Hojas que enlaza cada plantilla (ruta sin ?v=). Cualquier otra plantilla: ninguna.
# tokens.css va primero (variables) y utilities.css después de la hoja de la página (gana a sus reglas generales).
EXPECTED_STYLESHEETS = {
    "layouts/app_shell.html": ["/static/css/tokens.css", "/static/css/app.css", "/static/css/utilities.css"],
    "login.html": ["/static/css/tokens.css", "/static/css/login.css", "/static/css/utilities.css"],
    "transfer_print.html": ["/static/css/tokens.css", "/static/css/print.css", "/static/css/utilities.css"],
    "restock_print.html": ["/static/css/tokens.css", "/static/css/print.css", "/static/css/utilities.css"],
    "label_single_print.html": ["/static/css/tokens.css", "/static/css/label-print.css"],
}


def _templates() -> list[Path]:
    return sorted(TEMPLATES_DIR.rglob("*.html"))


def _rel(path: Path) -> str:
    return path.relative_to(TEMPLATES_DIR).as_posix()


def test_csp_style_src_is_self_only() -> None:
    # Sin `with`: no arranca el lifespan (seed de credenciales); el middleware corre igual.
    csp = TestClient(app).get("/health").headers["Content-Security-Policy"]
    directives = dict(d.strip().split(" ", 1) for d in csp.split(";") if d.strip())
    assert directives["style-src"] == "'self'"
    assert directives["script-src"] == "'self'"
    assert "'unsafe-inline'" not in csp
    assert directives["default-src"] == "'self'"
    assert directives["object-src"] == "'none'"
    assert directives["frame-ancestors"] == "'none'"


def test_no_style_attributes_or_blocks_in_templates() -> None:
    found = {}
    for path in _templates():
        text = path.read_text(encoding="utf-8")
        hits = STYLE_ATTR.findall(text) + STYLE_TAG.findall(text)
        if hits:
            found[_rel(path)] = hits
    assert found == {}


def test_no_style_attributes_in_python_html() -> None:
    sources = [p for d in ("routers", "services", "agent") for p in (ROOT / "app" / d).glob("*.py")]
    found = {p.name: STYLE_ATTR.findall(p.read_text(encoding="utf-8")) for p in sources}
    assert {k: v for k, v in found.items() if v} == {}


def test_macros_have_no_free_style_parameters() -> None:
    ui = (TEMPLATES_DIR / "components" / "ui.html").read_text(encoding="utf-8")
    modal = (TEMPLATES_DIR / "components" / "modal.html").read_text(encoding="utf-8")
    assert "{% macro card(title=None, id=None, extra_class='') -%}" in ui
    assert "{% macro subcard(title=None, id=None, extra_class='') -%}" in ui
    assert "container_style" not in modal and "container_class=None" in modal


def test_no_hex_colors_in_templates() -> None:
    found = {_rel(p): HEX.findall(p.read_text(encoding="utf-8")) for p in _templates()}
    assert {k: v for k, v in found.items() if v} == {}


def test_stylesheets_per_template() -> None:
    actual = {}
    for path in _templates():
        links = LINK.findall(path.read_text(encoding="utf-8"))
        if links:
            actual[_rel(path)] = links
    assert actual == EXPECTED_STYLESHEETS
    for links in EXPECTED_STYLESHEETS.values():
        for href in links:
            assert (ROOT / "app" / href.lstrip("/")).is_file(), href


def test_htmx_config_does_not_inject_or_copy_styles() -> None:
    shell = (TEMPLATES_DIR / "layouts" / "app_shell.html").read_text(encoding="utf-8")
    meta = ("""<meta name="htmx-config" content='{"includeIndicatorStyles": false, """
            """"attributesToSettle": ["class"]}' />""")
    assert meta in shell
    # La meta va antes de htmx: htmx lee su configuración al cargar.
    assert shell.index(meta) < shell.index("/static/js/htmx.min.js")
    css = (CSS_DIR / "app.css").read_text(encoding="utf-8")
    assert ".htmx-indicator { opacity: 0; }" in css


def test_chart_modules_take_colors_from_css() -> None:
    color = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\(")
    for name in ("components/home-charts.js", "components/widget-chart.js"):
        assert color.findall((JS_DIR / name).read_text(encoding="utf-8")) == [], name
    css = (CSS_DIR / "tokens.css").read_text(encoding="utf-8")
    assert "--chart-1: #0b2d42;" in css and "--chart-1: #60a5fa;" in css
    assert "--chart-8: initial;" in css  # paleta oscura de 7 colores
    assert "--home-pie-11: rgba(107, 114, 128, 0.55);" in css


def test_theme_toggle_does_not_reload() -> None:
    theme = (JS_DIR / "features" / "theme.js").read_text(encoding="utf-8")
    assert "location.reload" not in theme
    assert "dispatchEvent(new CustomEvent(THEME_EVENT" in theme


def test_barcodes_never_use_jsbarcode_svg_renderer() -> None:
    # JsBarcode en modo SVG escribe style="…" (bloqueado por la CSP): solo se usa en modo objeto.
    calls = {}
    for path in JS_DIR.rglob("*.js"):
        if path.name in VENDOR:
            continue
        hits = re.findall(r"window\.JsBarcode\(([^,]+),", path.read_text(encoding="utf-8"))
        if hits:
            calls[path.relative_to(JS_DIR).as_posix()] = hits
    assert calls == {"components/barcode-svg.js": ["target"]}


def test_tokens_and_shared_utilities_live_in_one_sheet() -> None:
    # Un único sitio para los colores y para las utilidades que usan dashboard, login e impresiones.
    sheets = {p.name: p.read_text(encoding="utf-8") for p in CSS_DIR.glob("*.css")}
    assert [n for n, css in sheets.items() if ":root" in css or '[data-theme="dark"] {' in css] == ["tokens.css"]
    shared = re.findall(r"^(\.[\w-]+) \{", sheets["utilities.css"], re.MULTILINE)
    assert len(shared) == 14
    for name, css in sheets.items():
        if name == "utilities.css":
            continue
        redefined = [c for c in shared if re.search(r"^" + re.escape(c) + r"\s*\{", css, re.MULTILINE)]
        assert redefined == [], (name, redefined)
    for name in ("print.css", "label-print.css"):
        assert HEX.findall(sheets[name]) == [], name
