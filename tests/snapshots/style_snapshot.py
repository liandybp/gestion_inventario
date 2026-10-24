"""Arnés de estilos calculados (red de «sin cambio visual» de la fase 3 del release 0.5.0).

Arranca la app real (uvicorn + SQLite temporal con la siembra del tour e2e), recorre en Chromium el login, las 17
pestañas del admin, los modales del tour y las 3 páginas de impresión, en tema claro y oscuro, y guarda (``write``) o
compara (``check``) los estilos calculados de cada elemento.

Uso (desde la raíz del repo):
    .venv/bin/python tests/snapshots/style_snapshot.py write <dir>
    .venv/bin/python tests/snapshots/style_snapshot.py check <dir> [--allow tests/snapshots/style_allowlist.txt]

La clave de cada elemento es su ruta de etiquetas (id y posición entre hermanos del mismo tipo); las clases NO forman
parte de la clave porque la fase 3 las cambia a propósito. No entra en CI: la baseline se genera desde el commit
anterior a la fase (worktree con ``set -e``) y se compara al final. El contenido de ``svg.barcode`` se excluye (lo cubre
su propio e2e). ``--allow`` lista las diferencias intencionadas, una por línea: ``<captura>\\t<clave>\\t<propiedad>``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))
os.environ.setdefault("SESSION_SECRET", "test")
os.environ.setdefault("OPERATOR_USERNAME", "operator")
os.environ.setdefault("OPERATOR_PASSWORD", "test")

from playwright.sync_api import sync_playwright  # noqa: E402

from tests.e2e.conftest import _boot_temp_server, _seed_stock, _seed_tour  # noqa: E402
from tests.e2e.test_ui_tour_csp_e2e import MODALS, TABS, login, open_tab  # noqa: E402

PROPS = [
    "display", "position", "top", "left", "float", "box-sizing", "width", "height", "min-width", "max-width",
    "min-height", "max-height", "margin-top", "margin-right", "margin-bottom", "margin-left",
    "padding-top", "padding-right", "padding-bottom", "padding-left",
    "border-top-width", "border-right-width", "border-bottom-width", "border-left-width",
    "border-top-style", "border-top-color", "border-right-color", "border-bottom-color", "border-left-color",
    "border-top-left-radius", "border-bottom-right-radius",
    "color", "background-color", "background-image", "opacity", "visibility",
    "font-family", "font-size", "font-weight", "font-style", "line-height", "text-align", "text-decoration-line",
    "white-space", "overflow-x", "overflow-y", "object-fit", "vertical-align", "cursor",
    "flex-direction", "flex-wrap", "flex-grow", "flex-shrink", "flex-basis", "align-items", "justify-content",
    "gap", "row-gap", "column-gap", "grid-template-columns", "grid-template-rows",
    "transform", "z-index", "list-style-type",
]

COLLECT_JS = """(props) => {
  const out = {};
  const keyOf = (el) => {
    const parts = [];
    for (let n = el; n && n.nodeType === 1; n = n.parentElement) {
      let part = n.tagName.toLowerCase();
      // notes.js genera ids aleatorios para sus <datalist>: se normalizan.
      if (n.id) part += '#' + n.id.replace(/^(reusable-notes-list-).+$/, '$1*');
      else if (n.parentElement) {
        const same = Array.from(n.parentElement.children).filter((c) => c.tagName === n.tagName);
        if (same.length > 1) part += ':' + (same.indexOf(n) + 1);
      }
      parts.unshift(part);
      if (n.id) break;
    }
    return parts.join('>');
  };
  const all = [document.documentElement, ...document.documentElement.querySelectorAll('body, body *')];
  for (const el of all) {
    const tag = el.tagName.toLowerCase();
    if (tag === 'script' || tag === 'style' || tag === 'link' || tag === 'template') continue;
    if (el.closest('svg.barcode') && !el.matches('svg.barcode')) continue;
    const cs = getComputedStyle(el);
    const rec = {};
    if (cs.display === 'none') { rec.display = 'none'; }
    else { for (const p of props) rec[p] = cs.getPropertyValue(p); }
    let key = keyOf(el), i = 2;
    while (key in out) key = keyOf(el) + '~' + (i++);
    out[key] = rec;
  }
  return out;
}"""

CHARTS_READY_JS = ("() => !window.Chart || [...document.querySelectorAll('canvas')]"
                   ".every((c) => window.Chart.getChart(c))")


def _capture(page) -> dict:
    page.wait_for_load_state("load")
    page.evaluate("document.fonts.ready")
    # Chart.js monta los gráficos de forma asíncrona: esperar a que todo <canvas> tenga el suyo.
    page.wait_for_function(CHARTS_READY_JS, timeout=10000)
    page.wait_for_timeout(300)
    return page.evaluate(COLLECT_JS, PROPS)


def collect() -> dict[str, dict]:
    base_url, db_path, proc, tmp = _boot_temp_server("style-snapshot-")
    shots: dict[str, dict] = {}
    try:
        _seed_stock(db_path)
        ids = _seed_tour(db_path)
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            for theme in ("light", "dark"):
                ctx = browser.new_context(viewport={"width": 1280, "height": 900}, reduced_motion="reduce",
                                          locale="es-ES", timezone_id="Europe/Madrid")
                ctx.add_init_script(f"try {{ localStorage.setItem('gi-theme', '{theme}'); }} catch (e) {{}}")
                page = ctx.new_page()
                page.goto(f"{base_url}/ui/login")
                shots[f"{theme}/login"] = _capture(page)
                login(page, base_url)
                for slug in TABS:
                    open_tab(page, slug)
                    shots[f"{theme}/tab-{slug}"] = _capture(page)
                current = None
                for slug, opener, modal_id in MODALS:
                    if slug != current:
                        open_tab(page, slug)
                        current = slug
                    page.locator(opener).first.click()
                    page.locator(f"#{modal_id}").wait_for(state="visible")
                    shots[f"{theme}/modal-{modal_id}"] = _capture(page)
                    page.locator(f"#{modal_id} .modal-close").first.click()
                    page.locator(f"#{modal_id}").wait_for(state="detached")
                for name, path in (("restock-print", "/ui/restock-print"),
                                   ("transfer-print", f"/ui/transfers/print?ref={ids['transfer_ref']}"),
                                   ("label", f"/ui/purchase/{ids['purchase']}/label")):
                    page.goto(f"{base_url}{path}")
                    shots[f"{theme}/{name}"] = _capture(page)
                ctx.close()
            browser.close()
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)
    return shots


def _file(directory: Path, name: str) -> Path:
    return directory / (name.replace("/", "__") + ".json")


def write(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for name, data in collect().items():
        _file(directory, name).write_text(json.dumps(data, indent=0, sort_keys=True), encoding="utf-8")
    print(f"OK: baseline escrita en {directory}")


def _allowed(allow_path: Path | None) -> set[tuple[str, str, str]]:
    allowed: set[tuple[str, str, str]] = set()
    if allow_path:
        for line in allow_path.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#"):
                cap, key, prop = line.split("\t")
                allowed.add((cap, key, prop))
    return allowed


def check(directory: Path, allow_path: Path | None) -> int:
    allowed = _allowed(allow_path)
    failures, intended = 0, 0
    for name, data in collect().items():
        path = _file(directory, name)
        if not path.exists():
            print(f"FALTA baseline: {name}")
            failures += 1
            continue
        base = json.loads(path.read_text(encoding="utf-8"))
        diffs = []
        for key in sorted(set(base) | set(data)):
            if key not in base or key not in data:
                diffs.append((key, "<elemento>", "falta" if key not in data else "sobra", ""))
                continue
            for prop in sorted(set(base[key]) | set(data[key])):
                if base[key].get(prop) != data[key].get(prop):
                    diffs.append((key, prop, base[key].get(prop), data[key].get(prop)))
        real = [d for d in diffs if (name, d[0], d[1]) not in allowed]
        intended += len(diffs) - len(real)
        if real:
            failures += 1
            print(f"DIFF {name}: {len(real)} diferencias")
            for key, prop, a, b in real[:15]:
                print(f"  {key} [{prop}]: {a!r} -> {b!r}")
    if failures:
        print(f"FALLO: {failures} capturas con diferencias no admitidas")
        return 1
    print(f"OK: estilos calculados idénticos ({intended} diferencias intencionadas admitidas)")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] not in ("write", "check"):
        sys.exit(__doc__)
    target = Path(sys.argv[2]).resolve()
    if sys.argv[1] == "write":
        write(target)
    else:
        allow = Path(sys.argv[4]).resolve() if len(sys.argv) > 4 and sys.argv[3] == "--allow" else None
        sys.exit(check(target, allow))
