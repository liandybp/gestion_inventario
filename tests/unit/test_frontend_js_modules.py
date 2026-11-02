"""Fase 2 del release 0.5.0: JS en módulos ES, sin JS inline, CSP sin 'unsafe-inline'.

Guardas estáticas (sin navegador). El comportamiento en el navegador lo cubren los e2e
``tests/e2e/test_ui_tour_csp_e2e.py`` y ``tests/e2e/test_js_behaviors_e2e.py``.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.routers.ui_common import templates

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = ROOT / "app" / "templates"
JS_DIR = ROOT / "app" / "static" / "js"
VENDOR = {"htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js"}
# Entradas que no importa main.js: script clásico del <head> y módulos de página.
STANDALONE = {"theme-init.js", "pages/login.js", "pages/print.js", "pages/label-print.js"}

INLINE_HANDLER = re.compile(r"\son[a-z]+\s*=", re.IGNORECASE)
SCRIPT_TAG = re.compile(r"<script\b([^>]*)>", re.IGNORECASE)
ATTR = re.compile(r'([a-z-]+)="([^"]*)"')
IMPORT = re.compile(r"""^\s*import\b[^'"]*['"](\.{1,2}/[^'"]+)['"]""", re.MULTILINE)

_MAIN = ("/static/js/main.js", "module")
_PRINT = ("/static/js/pages/print.js", "module")
# Scripts ejecutables que carga cada plantilla: (ruta sin ?v=, type). Cualquier otra: ninguno.
EXPECTED_SCRIPTS = {
    "layouts/app_shell.html": [
        ("/static/js/theme-init.js", ""),
        ("/static/js/htmx.min.js", ""),
        ("/static/js/chart.umd.min.js", ""),
        ("/static/js/JsBarcode.all.min.js", ""),
        _MAIN,
    ],
    "login.html": [("/static/js/pages/login.js", "module")],
    "transfer_print.html": [_PRINT],
    "restock_print.html": [_PRINT],
    "label_single_print.html": [
        ("/static/js/JsBarcode.all.min.js", ""),
        ("/static/js/pages/label-print.js", "module"),
    ],
}


def _templates() -> list[Path]:
    return sorted(TEMPLATES_DIR.rglob("*.html"))


def _rel(path: Path) -> str:
    return path.relative_to(TEMPLATES_DIR).as_posix()


def _js_files() -> set[str]:
    return {p.relative_to(JS_DIR).as_posix() for p in JS_DIR.rglob("*.js") if p.name not in VENDOR}


def test_csp_script_src_is_self_only() -> None:
    # Sin `with`: no arranca el lifespan (seed de credenciales); el middleware corre igual.
    csp = TestClient(app).get("/health").headers["Content-Security-Policy"]
    directives = dict(d.strip().split(" ", 1) for d in csp.split(";") if d.strip())
    assert directives["script-src"] == "'self'"


def test_static_js_is_always_revalidated() -> None:
    client = TestClient(app)
    main = client.get("/static/js/main.js")
    nested = client.get("/static/js/core/content.js")
    assert main.status_code == 200 and nested.status_code == 200
    assert main.headers["Cache-Control"] == "no-cache"
    assert nested.headers["Cache-Control"] == "no-cache"


def test_no_inline_event_handlers_in_templates() -> None:
    found = {_rel(p): INLINE_HANDLER.findall(p.read_text(encoding="utf-8")) for p in _templates()}
    assert {k: v for k, v in found.items() if v} == {}


def test_no_inline_event_handlers_in_python_html() -> None:
    sources = list((ROOT / "app" / "routers").glob("*.py")) + list((ROOT / "app" / "services").glob("*.py"))
    pattern = re.compile(r"\son(click|change|input|submit|keydown|keyup|load|error|focus|blur)\s*=")
    found = {p.name: pattern.findall(p.read_text(encoding="utf-8")) for p in sources}
    assert {k: v for k, v in found.items() if v} == {}


def test_script_tags_per_template() -> None:
    actual: dict[str, list[tuple[str, str]]] = {}
    for path in _templates():
        scripts = []
        for raw in SCRIPT_TAG.findall(path.read_text(encoding="utf-8")):
            attrs = dict(ATTR.findall(raw))
            if attrs.get("type") == "application/json":
                continue  # datos, no código
            assert "src" in attrs, f"{_rel(path)}: <script> inline ejecutable"
            scripts.append((attrs["src"].split("?", 1)[0], attrs.get("type", "")))
        if scripts:
            actual[_rel(path)] = scripts
    assert actual == EXPECTED_SCRIPTS


def test_module_graph_resolves_and_has_no_orphans() -> None:
    reachable: set[str] = set()
    pending = ["main.js"]
    while pending:
        rel = pending.pop()
        if rel in reachable:
            continue
        path = JS_DIR / rel
        assert path.is_file(), f"import roto: {rel}"
        reachable.add(rel)
        for spec in IMPORT.findall(path.read_text(encoding="utf-8")):
            pending.append((path.parent / spec).resolve().relative_to(JS_DIR.resolve()).as_posix())
    for rel in STANDALONE:
        assert (JS_DIR / rel).is_file(), rel
    assert _js_files() - reachable - STANDALONE == set(), "módulos que no importa nadie"
    assert "app.js" not in _js_files()


def test_no_debug_code_left() -> None:
    for rel in sorted(_js_files()):
        text = (JS_DIR / rel).read_text(encoding="utf-8")
        assert "debug" not in text.lower(), rel
        # Permitir console.error solo en core/content.js para reportar fallos de enhancers
        if rel == "core/content.js":
            assert 'console.error(`Enhancer error:' in text
        else:
            assert "console." not in text, rel
    for path in _templates():
        assert "-debug" not in path.read_text(encoding="utf-8"), _rel(path)


class _Attrs(HTMLParser):
    """Atributos (ya des-escapados) del primer <tag> [con id=element_id]."""

    def __init__(self, tag: str, element_id: str | None) -> None:
        super().__init__()
        self.tag = tag
        self.element_id = element_id
        self.attrs: dict[str, str | None] = {}

    def handle_starttag(self, tag: str, attrs: list) -> None:
        found = dict(attrs)
        if tag == self.tag and not self.attrs and self.element_id in (None, found.get("id")):
            self.attrs = found


def _attrs_of(html: str, tag: str, element_id: str | None = None) -> dict[str, str | None]:
    parser = _Attrs(tag, element_id)
    parser.feed(html)
    return parser.attrs


def test_widget_chart_spec_survives_hostile_title() -> None:
    title = "</canvas><script>x</script> \"q\" 'a' & <b>"
    w = {"id": 3, "type": "pie", "title": title, "payload": {"labels": ["<b>A</b>", "B'"], "values": [1, 2]}}
    html = templates.env.get_template("features/agent/widget_chart.html").render(w=w)
    assert "<script" not in html and "<b>" not in html
    spec = json.loads(_attrs_of(html, "canvas")["data-js-widget-chart"])
    assert spec == {"type": "pie", "title": title, "payload": w["payload"]}


def test_label_print_data_attributes_escape_values() -> None:
    label = SimpleNamespace(sku='S"1<x>', name="N <b>'n'</b> &")
    html = templates.env.get_template("label_single_print.html").render(
        label=label, copies=1, static_version=lambda: "1"
    )
    sheet = _attrs_of(html, "div", "sheet")
    assert sheet["data-js-label-sku"] == label.sku
    assert sheet["data-js-label-name"] == label.name
