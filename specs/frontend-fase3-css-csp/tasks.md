# Task Envelopes — Frontend modular, fase 3: CSS sin estilos inline, tokens y paleta, CSP `style-src 'self'`

> SDD Phase 3 · PAE-018 · Release 0.5.0 (ningún commit sube versión) · Fuente: `requirements.md` (REQ-1..10),
> `design.md` (D1..D12, pre-mortem R1..R4)

**Sin prototipo** (ADR-020 + regla del paso 2 del loop, 2026-10-24). Los envelopes se generan por script
(`specs/frontend-fase3-css-csp/gen/`): los «antes» salen de leer los ficheros reales de `b0f5dd8`, y
`gen_tasks.py check` comprueba **en memoria** que cada `edit()` encuentra exactamente una coincidencia en orden. Nada
de esto se ha ejecutado contra la app: la red que lo comprueba es el **lote A**, y cada lote de plantillas cierra con
el `check` de estilos calculados.

**Ejecución:** `cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase3-css-csp/tasks.md
--test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"`
(sin `--proto`). Rama `feature/frontend-modular`, sin push, sin bump de versión. 40 envelopes.

**Herramienta e2e:** Playwright (`tests/e2e/`, como el resto del proyecto). Con la CSP estricta, `wait_for_function`
solo con funciones flecha.

**Herramientas de cc_local:** cada envelope declara `Tools` y `Skills`. Ningún envelope usa MCP (regla dura) ni skills:
todo va literal (ficheros completos o scripts con `assert` de coincidencia única). Comandos desde la raíz del repo
con `.venv/bin/python`. `node --check` solo valida sintaxis.

**Reglas para cc_local en este PAE:**
- Ficheros nuevos: copiar el contenido literal. Ficheros existentes: ejecutar el script literal; si un `assert` falla,
  PARAR (el fichero no está como se esperaba) — nunca editar a mano para «hacerlo cuadrar».
- Si `style_snapshot.py check` da FALLO: PARAR y reportar la salida completa. No tocar CSS ni plantillas para que cuadre.
- `git add` solo de los `Files` del lote (en el repo hay cambios ajenos sin commitear: `CLAUDE.md`, `.claude/`,
  `.idea/`, `.serena/`, `specs/frontend-fase2-js-modulos/`, `specs/frontend-fase3-css-csp/`).
- Los e2e necesitan `SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test` si no hay `.env`.
- La baseline de estilos vive en `/tmp/gi-fase3-style-baseline` (no se versiona). Si falta, la regenera el script de T2
  (o el del último lote). Un `check` de estilos tarda ~1-2 min.

Lotes: **A** T1–T7 · **B** T8–T11 · **C** T12–T16 · **D** T17–T20 · **E** T21–T29 · **F** T30–T34 · **G** T35–T36 · **H** T37–T38 · **I** T39–T40

- **A** (red: estilos calculados, guarda CSP, arnés HTML — nada de app/): T1, T2, T3, T4, T5, T6, T7
- **B** (CSS: clases, tokens y hojas nuevas (aún sin efecto visible)): T8, T9, T10, T11
- **C** (plantillas 1/3): T12, T13, T14, T15, T16
- **D** (plantillas 2/3): T17, T18, T19, T20
- **E** (plantillas 3/3, macros, login e impresiones): T21, T22, T23, T24, T25, T26, T27, T28, T29
- **F** (JS: códigos de barras, paleta, tema sin recarga): T30, T31, T32, T33, T34
- **G** (CSP estricta + guardas unitarias): T35, T36
- **H** (e2e): T37, T38
- **I** (cierre: check visual final, CHANGELOG, suites): T39, T40

**Orden:** A (la red: nada de `app/`) → B (CSS: reglas nuevas, sin efecto visible) → C, D, E (plantillas; cada lote cierra
con el `check` de estilos) → F (JS) → G (CSP estricta) → H (e2e) → I (cierre). Cada lote = 1 commit; al final de cada
lote la app funciona: hasta G la CSP sigue permitiendo estilos inline.

---

### T1: Red de estilos calculados (1/2): `tests/snapshots/style_snapshot.py` y diferencias admitidas

**Lote:** A · **REQs:** REQ-3
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/style_snapshot.py`, `tests/snapshots/style_allowlist.txt`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

`style_allowlist.txt` = las únicas diferencias admitidas (REQ-6: badge de reposición de Inicio en tema oscuro). Reutiliza, sin modificarlos: `tests/e2e/conftest.py::_boot_temp_server(prefix) -> (base_url, db_path, proc, tmp)`, `_seed_stock(db_path)`, `_seed_tour(db_path) -> {'purchase': int, 'transfer_ref': str}`, y de `tests/e2e/test_ui_tour_csp_e2e.py`: `TABS`, `MODALS`, `login(page, base_url)`, `open_tab(page, slug)`. No forma parte de pytest ni de CI.

#### Objective

Crear la primera mitad del arnés (propiedades y captura de estilos) y su lista de diferencias admitidas.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/snapshots/style_snapshot.py` con EXACTAMENTE este contenido** (fichero nuevo, primera parte; la completa el envelope siguiente):

````python
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
````

**Step 2: escribir `tests/snapshots/style_allowlist.txt` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````text
# Diferencias intencionadas de la fase 3 (REQ-6): en tema oscuro el badge de reposición de Inicio usa los tokens
# --badge-info-bg / --badge-info-text (antes #eef6ff / #0b2d42 fijos; el borde sigue a color).
# Formato: captura<TAB>clave<TAB>propiedad. Las miniaturas no aparecen: la siembra no tiene imágenes.
dark/tab-home	div#restock>div:2>span	background-color
dark/tab-home	div#restock>div:2>span	border-bottom-color
dark/tab-home	div#restock>div:2>span	border-left-color
dark/tab-home	div#restock>div:2>span	border-right-color
dark/tab-home	div#restock>div:2>span	border-top-color
dark/tab-home	div#restock>div:2>span	color
````

#### Verification

```bash
wc -l < tests/snapshots/style_snapshot.py   # 96
```

#### Expected Report

- [ ] `tests/snapshots/style_snapshot.py`, `tests/snapshots/style_allowlist.txt` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T2: Red de estilos calculados (2/2): `write`/`check` y punto de entrada

**Lote:** A · **REQs:** REQ-3
**Depends on:** T1
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/style_snapshot.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna

#### Context

Continúa el fichero de T1, que termina en la función `_capture(page)`.

#### Objective

Completar el arnés con `collect`, `write`, `check` (con `--allow`) y `__main__`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: añadir AL FINAL de `tests/snapshots/style_snapshot.py` EXACTAMENTE este bloque** (sin tocar lo anterior; el fichero ya termina en salto de línea):

````python


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
````

#### Verification

```bash
.venv/bin/python -m py_compile tests/snapshots/style_snapshot.py && echo compile-ok   # compile-ok
.venv/bin/python tests/snapshots/style_snapshot.py 2>&1 | head -1   # primera línea del docstring (uso)
```

#### Expected Report

- [ ] `tests/snapshots/style_snapshot.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T3: Baseline de estilos desde `b0f5dd8` (worktree, set -e) y prueba de determinismo

**Lote:** A · **REQs:** REQ-3
**Depends on:** T2
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/style_snapshot.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills · tarda ~2 min

#### Context

La baseline (~20 MB, 74 JSON) va a `/tmp/gi-fase3-style-baseline` y NO se versiona. Se genera con el código de `b0f5dd8` en un worktree temporal; después el `check` sobre la rama (que aún no ha tocado `app/`) debe dar OK: si no, el arnés no es determinista. No modifica ningún fichero del repo.

#### Objective

Generar la baseline de estilos de la base y comprobar que el arnés es determinista.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
# Guardas: la baseline se genera SIEMPRE con el código anterior a la fase 3 (b0f5dd8), nunca sobre el migrado.
git diff --quiet b0f5dd8 -- app/
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" b0f5dd8
ln -s "$REPO/.venv" "$WT/.venv"
cp tests/snapshots/style_snapshot.py "$WT/tests/snapshots/style_snapshot.py"
rm -rf /tmp/gi-fase3-style-baseline
(cd "$WT" && .venv/bin/python tests/snapshots/style_snapshot.py write /tmp/gi-fase3-style-baseline)
git worktree remove --force "$WT"
test "$(ls /tmp/gi-fase3-style-baseline | wc -l)" -eq 74
# Determinismo: el código de la rama (aún sin cambios en app/) debe dar exactamente la baseline.
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline
````

#### Verification

```bash
ls /tmp/gi-fase3-style-baseline | wc -l   # 74
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline   # OK: estilos calculados idénticos (0 diferencias intencionadas admitidas)
git status --short app/ tests/snapshots/baseline   # vacío
```

#### Expected Report

- [ ] `tests/snapshots/style_snapshot.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T4: Guarda `js_errors`: fallar ante cualquier violación de CSP

**Lote:** A · **REQs:** REQ-9
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/conftest.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`js_errors` (fixture autouse, `tests/e2e/conftest.py`) recoge `pageerror` y `console.error`; su teardown solo fallaba con textos de scripts. Chromium avisa de un estilo bloqueado con «Applying inline style violates the following Content Security Policy directive…».

#### Objective

Que el teardown falle también ante cualquier mensaje con `Content Security Policy`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit("tests/e2e/conftest.py", """    real_errors = [e for e in errors if "Refused to load the script" in e or "Refused to execute" in e or "pageerror" in e]""",
"""    # "Content Security Policy": cualquier violación de CSP (scripts y, desde la fase 3, estilos inline:
    # Chromium dice "Applying inline style violates the following Content Security Policy directive…").
    real_errors = [e for e in errors if "Refused to load the script" in e or "Refused to execute" in e
                   or "Content Security Policy" in e or "pageerror" in e]""")
print("OK")
PY
````

#### Verification

```bash
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE tests/e2e/test_js_behaviors_e2e.py   # PASS (aún con 'unsafe-inline' en estilos)
```

#### Expected Report

- [ ] `tests/e2e/conftest.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T5: Tabla de equivalencias `style` → clases (`tests/snapshots/style_classes.py`)

**Lote:** A · **REQs:** REQ-9
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/style_classes.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

Módulo de datos puro (dict + `style_key`). Lo importa el canónico de `ui_snapshot.py` en T6.

#### Objective

Crear la tabla única de equivalencias de la fase 3.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/snapshots/style_classes.py` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````python
"""Fase 3 del release 0.5.0: equivalencia entre los antiguos atributos ``style="…"`` y las clases que los sustituyen.

La usa el canónico de ``ui_snapshot.py``: al comparar con una baseline generada antes de la fase 3, cada ``style`` se
traduce a sus clases, así el ``check`` prueba que cada estilo se cambió exactamente por ellas. Claves normalizadas con
``style_key`` (sin espacios alrededor de ``:``/``;`` y sin ``;`` final).
"""

from __future__ import annotations

import re

STYLE_TO_CLASSES = {
    "width:3%": "w-3p", "width:5%": "w-5p", "width:6%": "w-6p", "width:7%": "w-7p", "width:8%": "w-8p",
    "width:9%": "w-9p", "width:12%": "w-12p", "width:14%": "w-14p", "width:15%": "w-15p", "width:16%": "w-16p",
    "width:23%": "w-23p", "width:62%": "w-62p",
    "width:100px": "w-100px", "width:110px": "w-110px", "width:120px": "w-120px", "width:140px": "w-140px",
    "width:150px": "w-150px", "width:auto": "w-auto",
    "margin-top:0": "mt-0", "margin-top:6px": "mt-6", "margin-top:8px": "mt-8", "margin-top:12px": "mt-12",
    "margin-top:14px": "mt-14", "margin-top:16px": "mt-16", "margin-top:-6px": "mt-n6",
    "margin-bottom:6px": "mb-6", "margin-bottom:12px": "mb-12", "margin-bottom:14px": "mb-14",
    "margin:0": "m-0", "margin-left:10px": "ml-10", "text-decoration:none": "no-underline",
    "margin-top:0;margin-bottom:10px": "mt-0 mb-10", "margin-top:-6px;margin-bottom:10px": "mt-n6 mb-10",
    "margin:8px 0 10px 0": "my-8-10", "margin:6px 0 10px 0": "my-6-10",
    "padding:12px;border-radius:10px": "msg-box",
    "padding:12px;border-radius:10px;margin-top:12px": "msg-box mt-12",
    "padding:10px;border-radius:8px;margin-bottom:10px": "msg-box-sm mb-10",
    "display:flex;gap:8px;align-items:center": "flex items-center gap-8",
    "display:flex;align-items:center;gap:8px": "flex items-center gap-8",
    "display:flex;gap:12px;align-items:baseline;justify-content:space-between":
        "flex gap-12 items-baseline justify-between",
    "align-items:center": "items-center", "gap:12px": "gap-12",
    "max-height:48vh;overflow-y:auto": "scroll-48vh", "-webkit-overflow-scrolling:touch": "touch-scroll",
    "margin-top:10px;grid-template-columns:auto 1fr": "mt-10 cols-auto-1fr",
    "min-width:85px": "min-w-85", "display:inline-block;min-width:85px": "d-inline-block min-w-85",
    "margin-left:8px;background:var(--brand-red)": "ml-8 btn-danger",
    "width:36px;height:36px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb": "thumb",
    "width:64px;height:64px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb": "thumb thumb-lg",
    "display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;background:#eef6ff;"
    "color:#0b2d42;font-size:12px;font-weight:600": "badge-info",
    "width:100%;padding:8px;border-radius:10px;border:1px solid var(--brand-border)": "select-business",
    "width:100%;padding:8px;border:1px solid var(--brand-border);border-radius:8px;"
    "background:var(--brand-surface);color:var(--brand-text)": "select-plain",
    "max-width:760px": "modal-container-760",
}


def style_key(value: str) -> str:
    value = re.sub(r"\s*([:;])\s*", r"\1", value.strip())
    return re.sub(r"\s+", " ", value).rstrip(";")
````

#### Verification

```bash
.venv/bin/python -c "from tests.snapshots.style_classes import STYLE_TO_CLASSES as M, style_key as k; print(len(M), k(' width: 3%; '))"   # 55 width:3%
```

#### Expected Report

- [ ] `tests/snapshots/style_classes.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T6: Canónico del arnés HTML: `style` → clases; sin `<style>`, `<link>` ni meta `htmx-config`

**Lote:** A · **REQs:** REQ-9
**Depends on:** T5
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`tests/snapshots/ui_snapshot.py`: clase `_Canon(HTMLParser)`, `handle_starttag(tag, attrs)` construye `parts` con los atributos ordenados y salta `_is_js_wiring_attr`; `_norm(text)` normaliza valores. La baseline actual se generó sin estas reglas: T7 la regenera.

#### Objective

Que el arnés compare cada `style` antiguo como sus clases equivalentes, en orden estable.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
.venv/bin/python -m py_compile tests/snapshots/ui_snapshot.py && echo compile-ok   # compile-ok
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T7: Regenerar la baseline HTML desde `b0f5dd8` (worktree, set -e)

**Lote:** A · **REQs:** REQ-9
**Depends on:** T6
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/baseline/`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Igual que en las fases 1 y 2: el arnés nuevo (T5+T6, copiados al worktree) sobre el código ANTERIOR a la fase, en el mismo commit que el cambio del arnés. Ningún `write` sobre el repo fuera de este script.

#### Objective

Baseline HTML nueva y `check` OK sobre la rama.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
git diff --quiet b0f5dd8 -- app/
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" b0f5dd8
ln -s "$REPO/.venv" "$WT/.venv"
cp tests/snapshots/ui_snapshot.py tests/snapshots/style_classes.py "$WT/tests/snapshots/"
(cd "$WT" && .venv/bin/python tests/snapshots/ui_snapshot.py write tests/snapshots/baseline)
rm -rf tests/snapshots/baseline
cp -r "$WT/tests/snapshots/baseline" tests/snapshots/baseline
git worktree remove --force "$WT"
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
````

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1   # OK: 40 snapshots equivalentes
git status --short tests/snapshots/baseline | grep -vc '^ M'   # 0 (solo modificados)
```

#### Expected Report

- [ ] `tests/snapshots/baseline/` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T8: `app.css`: bloque final «Fase 3» (clases, indicador de htmx) y fuera la `mb-10` muerta

**Lote:** B · **REQs:** REQ-2, REQ-4, REQ-6
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/css/app.css`, `app/templates/partials/stock_table.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna + Bash (script literal para `stock_table.html`)

#### Context

`app.css` termina en `.widget-table tbody tr:hover td { background: var(--row-hover); }` + salto de línea. Las reglas no cambian nada visible mientras las plantillas sigan con `style=` (el inline gana). `stock_table.html` usa `class="mb-10"` sin regla: al definir `.mb-10` ese formulario ganaría 10 px, por eso se quita aquí. `--badge-info-*` los define T9 (hasta entonces el badge sigue con su inline).

#### Objective

Añadir las clases que sustituyen a los `style=` y las reglas del indicador de htmx.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: añadir AL FINAL de `app/static/css/app.css` EXACTAMENTE este bloque** (sin tocar lo anterior; el fichero ya termina en salto de línea):

````css

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
````

**Step 2: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit("app/templates/partials/stock_table.html", """        hx-include="#inventory_query,#inventory_location_code,#inventory_stock_filter,#inventory_category"
        class="mb-10">""", """        hx-include="#inventory_query,#inventory_location_code,#inventory_stock_filter,#inventory_category">""")
print("OK")
PY
````

#### Verification

```bash
grep -c 'Fase 3 (release 0.5.0)' app/static/css/app.css   # 1
grep -c 'mb-10' app/templates/partials/stock_table.html   # 0
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline | tail -1   # OK
```

#### Expected Report

- [ ] `app/static/css/app.css`, `app/templates/partials/stock_table.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T9: Tokens: badge de reposición y paletas de los gráficos (`:root` y tema oscuro)

**Lote:** B · **REQs:** REQ-6, REQ-7
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/css/app.css`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`:root` termina en `--row-hover: rgba(59, 130, 246, 0.12);` y `[data-theme="dark"]` en `--row-hover: rgba(148, 163, 184, 0.15);`. Valores = constantes JS actuales de `home-charts.js` y `widget-chart.js`. `--chart-8: initial` deja la paleta oscura en 7 colores, como hoy.

#### Objective

Definir los tokens nuevos sin cambiar ningún color en tema claro.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
grep -c -- '--chart-1:' app/static/css/app.css   # 2
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline | tail -1   # OK
```

#### Expected Report

- [ ] `app/static/css/app.css` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T10: Hojas de login e impresión: `login.css` y `print.css`

**Lote:** B · **REQs:** REQ-2
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/css/login.css`, `app/static/css/print.css`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

Contenido = los `<style>` actuales de `login.html` y de `transfer_print.html`/`restock_print.html` (idénticos), más las clases que usan esas páginas tras quitar sus `style=` (`restock_print` incluye `partials/restock_table.html`). Aún no las enlaza nadie (lote E).

#### Objective

Crear las dos hojas.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/css/login.css` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````css
/* Login (antes <style> en login.html). No carga app.css. */
:root {
  --brand-navy: #0b2d42;
  --brand-border: #e5e7eb;
  --brand-bg: #f8fafc;
  --brand-surface: #ffffff;
  --brand-text: #0f172a;
  --brand-muted: #64748b;
}
*, *::before, *::after { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; background: var(--brand-bg); color: var(--brand-text); }
.card { max-width: 420px; margin: 80px auto; border: 1px solid var(--brand-border); border-radius: 12px; padding: 18px; background: var(--brand-surface); }
.row { display: grid; grid-template-columns: 120px 1fr; gap: 8px; margin-bottom: 10px; align-items: center; }
input { width: 100%; padding: 10px; border: 1px solid var(--brand-border); border-radius: 10px; background: var(--brand-surface); color: var(--brand-text); }
button { padding: 10px 12px; border: 0; border-radius: 10px; background: var(--brand-navy); color: white; cursor: pointer; width: 100%; }
.muted { color: var(--brand-muted); font-size: 13px; }
.error { background: #fef2f2; padding: 10px; border-radius: 10px; margin-bottom: 12px; }

/* Clases de la plantilla (antes style="…"). */
.mt-0 { margin-top: 0; }
.mb-12 { margin-bottom: 12px; }
.flex { display: flex; }
.items-center { align-items: center; }
.gap-8 { gap: 8px; }
.w-auto { width: auto; }
````

**Step 2: escribir `app/static/css/print.css` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````css
/* Impresión de traspasos y de artículos a reponer (antes <style> idéntico en las dos plantillas). */
html, body { margin: 0; padding: 0; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #0f172a; padding: 18px; }

.toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }
.toolbar button { padding: 10px 12px; border: 0; border-radius: 10px; background: #0b2d42; color: white; cursor: pointer; }
.muted { color: #64748b; }

h1 { font-size: 18px; margin: 0 0 8px; }
.meta { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 18px; margin-bottom: 14px; }
.meta div { font-size: 13px; }

table { width: 100%; border-collapse: collapse; table-layout: fixed; }
th, td { border-bottom: 1px solid #e5e7eb; padding: 8px 6px; font-size: 13px; }
th { text-align: left; background: #f8fafc; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }

@media print {
  .toolbar { display: none; }
  body { padding: 0; }
}

/* Anchos de columna de transfer_print.html (antes style="…"). */
.w-12p { width: 12%; }
.w-14p { width: 14%; }
.w-16p { width: 16%; }

/* restock_print.html incluye partials/restock_table.html (antes style="…"). Impresión: solo tema claro. */
.flex { display: flex; }
.gap-12 { gap: 12px; }
.items-baseline { align-items: baseline; }
.justify-between { justify-content: space-between; }
.my-6-10 { margin: 6px 0 10px 0; }
.badge-info {
  display: inline-flex;
  align-items: center;
  padding: 4px 10px;
  border-radius: 999px;
  background: #eef6ff;
  color: #0b2d42;
  font-size: 12px;
  font-weight: 600;
}
````

#### Verification

```bash
grep -c 'badge-info' app/static/css/print.css   # 1
grep -c 'w-auto' app/static/css/login.css   # 1
```

#### Expected Report

- [ ] `app/static/css/login.css`, `app/static/css/print.css` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T11: Hoja de la etiqueta: `label-print.css`

**Lote:** B · **REQs:** REQ-2
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/css/label-print.css`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

Contenido = el `<style>` actual de `label_single_print.html`. Aún no la enlaza nadie (lote E).

#### Objective

Crear la hoja de la etiqueta.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/css/label-print.css` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````css
/* Etiqueta individual (antes <style> en label_single_print.html). */
@page { size: 60mm 30mm; margin: 0; }

html, body { width: 60mm; margin: 0; padding: 0; }
body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #0f172a; }

.toolbar { padding: 10px 0 12px; display: flex; gap: 8px; align-items: center; }
.toolbar input { width: 90px; padding: 8px; border: 1px solid #e5e7eb; border-radius: 10px; }
.toolbar button { padding: 10px 12px; border: 0; border-radius: 10px; background: #0b2d42; color: white; cursor: pointer; }
.toolbar .muted { color: #64748b; font-size: 13px; }

.sheet { display: block; }

.page {
  width: 60mm;
  height: 30mm;
}

.label {
  width: 60mm;
  height: 30mm;
  border: 1px solid transparent;
  border-radius: 2mm;
  padding: 2mm;
  box-sizing: border-box;
  display: grid;
  grid-template-rows: auto 1fr;
  align-items: start;
  overflow: hidden;
}

.name {
  font-weight: 700;
  font-size: 10pt;
  line-height: 1.05;
  margin-bottom: 1mm;
  overflow: hidden;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
}

svg.barcode { width: 100%; height: 100%; }

@media print {
  .toolbar { display: none; }
  .label { border-color: transparent; }
  .page { break-after: page; page-break-after: always; }
  .page:last-child { break-after: auto; page-break-after: auto; }
}
````

#### Verification

```bash
grep -c '@page { size: 60mm 30mm; margin: 0; }' app/static/css/label-print.css   # 1
```

#### Expected Report

- [ ] `app/static/css/label-print.css` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T12: Plantillas sin `style=`: `tab_transfers.html` (1/3)

**Lote:** C · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_transfers.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 10 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `tab_transfers.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/tab_transfers.html', '    <div class="grid-3" style="gap:12px;">', '    <div class="grid-3 gap-12">')
edit('app/templates/partials/tab_transfers.html', '    <div class="row" style="margin-top:10px; grid-template-columns: auto 1fr;">', '    <div class="row mt-10 cols-auto-1fr">')
edit('app/templates/partials/tab_transfers.html', '      <input type="text" name="note" value="{{ note_value or \'\' }}" placeholder="Opcional" style="margin:0;" />', '      <input type="text" name="note" value="{{ note_value or \'\' }}" placeholder="Opcional" class="m-0" />')
edit('app/templates/partials/tab_transfers.html', '            <th style="width:62%;">Producto</th>', '            <th class="w-62p">Producto</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width:23%;">Cantidad</th>', '            <th class="w-23p">Cantidad</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width:15%;"></th>', '            <th class="w-15p"></th>')
edit('app/templates/partials/tab_transfers.html', '      <span id="transfer-loading" class="htmx-indicator" style="margin-left: 10px;">Procesando...</span>', '      <span id="transfer-loading" class="htmx-indicator ml-10">Procesando...</span>')
edit('app/templates/partials/tab_transfers.html', '      <div class="flex gap-12 items-center flex-wrap" style="margin: 8px 0 10px 0;">', '      <div class="flex gap-12 items-center flex-wrap my-8-10">')
edit('app/templates/partials/tab_transfers.html', '    <h3>Últimos traspasos (salida)</h3>\n    <div class="table-wrapper" style="-webkit-overflow-scrolling: touch;">\n      <table class="table-compact transfer-table">', '    <h3>Últimos traspasos (salida)</h3>\n    <div class="table-wrapper touch-scroll">\n      <table class="table-compact transfer-table">')
edit('app/templates/partials/tab_transfers.html', '            <th style="width:3%;">\n              <input type="checkbox" aria-label="Seleccionar todo"', '            <th class="w-3p">\n              <input type="checkbox" aria-label="Seleccionar todo"')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/tab_transfers.html   # 19
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/tab_transfers.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T13: Plantillas sin `style=`: `tab_transfers.html` (2/3)

**Lote:** C · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T12
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_transfers.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `tab_transfers.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/tab_transfers.html', '            </th>\n            {% endif %}\n            <th class="col-date">Fecha</th>\n            <th style="width: 120px;">Tipo</th>\n            <th style="width: 140px;">Usuario</th>\n            <th style="width: 140px;">SKU</th>\n            <th>Producto</th>', '            </th>\n            {% endif %}\n            <th class="col-date">Fecha</th>\n            <th class="w-120px">Tipo</th>\n            <th style="width: 140px;">Usuario</th>\n            <th style="width: 140px;">SKU</th>\n            <th>Producto</th>')
edit('app/templates/partials/tab_transfers.html', '            <th class="w-120px">Tipo</th>\n            <th style="width: 140px;">Usuario</th>\n            <th style="width: 140px;">SKU</th>', '            <th class="w-120px">Tipo</th>\n            <th class="w-140px">Usuario</th>\n            <th style="width: 140px;">SKU</th>')
edit('app/templates/partials/tab_transfers.html', '            <th class="w-140px">Usuario</th>\n            <th style="width: 140px;">SKU</th>\n            <th>Producto</th>', '            <th class="w-140px">Usuario</th>\n            <th class="w-140px">SKU</th>\n            <th>Producto</th>')
edit('app/templates/partials/tab_transfers.html', '            <th class="w-140px">SKU</th>\n            <th>Producto</th>\n            <th style="width: 110px;" class="text-right">Cantidad</th>\n            <th style="width: 110px;">Origen</th>\n            <th style="width: 110px;">Destino</th>', '            <th class="w-140px">SKU</th>\n            <th>Producto</th>\n            <th class="text-right w-110px">Cantidad</th>\n            <th style="width: 110px;">Origen</th>\n            <th style="width: 110px;">Destino</th>')
edit('app/templates/partials/tab_transfers.html', '            <th class="text-right w-110px">Cantidad</th>\n            <th style="width: 110px;">Origen</th>\n            <th style="width: 110px;">Destino</th>', '            <th class="text-right w-110px">Cantidad</th>\n            <th class="w-110px">Origen</th>\n            <th style="width: 110px;">Destino</th>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/tab_transfers.html   # 14
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/tab_transfers.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T14: Plantillas sin `style=`: `tab_transfers.html` (3/3)

**Lote:** C · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T13
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_transfers.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 14 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `tab_transfers.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/tab_transfers.html', '            <th class="w-110px">Origen</th>\n            <th style="width: 110px;">Destino</th>\n            <th style="width: 110px;">Acciones</th>', '            <th class="w-110px">Origen</th>\n            <th class="w-110px">Destino</th>\n            <th style="width: 110px;">Acciones</th>')
edit('app/templates/partials/tab_transfers.html', '            <th class="w-110px">Destino</th>\n            <th style="width: 110px;">Acciones</th>\n          </tr>', '            <th class="w-110px">Destino</th>\n            <th class="w-110px">Acciones</th>\n          </tr>')
edit('app/templates/partials/tab_transfers.html', '                {% if note and \'ref=\' in note %}{% set ref = note.split(\'ref=\', 1)[1].split()[0] %}{% endif %}\n                {% if ref %}\n                  <a href="/ui/transfers/print?ref={{ ref }}" target="_blank" style="text-decoration:none;">\n                    <button type="button">Print</button>\n                  </a>', '                {% if note and \'ref=\' in note %}{% set ref = note.split(\'ref=\', 1)[1].split()[0] %}{% endif %}\n                {% if ref %}\n                  <a href="/ui/transfers/print?ref={{ ref }}" target="_blank" class="no-underline">\n                    <button type="button">Print</button>\n                  </a>')
edit('app/templates/partials/tab_transfers.html', '    <div class="table-wrapper" style="-webkit-overflow-scrolling: touch;">', '    <div class="table-wrapper touch-scroll">')
edit('app/templates/partials/tab_transfers.html', '            <th style="width:3%;"></th>', '            <th class="w-3p"></th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 120px;">Tipo</th>', '            <th class="w-120px">Tipo</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 140px;">Usuario</th>', '            <th class="w-140px">Usuario</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 140px;">SKU</th>', '            <th class="w-140px">SKU</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 110px;" class="text-right">Cantidad</th>', '            <th class="text-right w-110px">Cantidad</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 110px;">Origen</th>', '            <th class="w-110px">Origen</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 110px;">Destino</th>', '            <th class="w-110px">Destino</th>')
edit('app/templates/partials/tab_transfers.html', '            <th style="width: 110px;">Acciones</th>', '            <th class="w-110px">Acciones</th>')
edit('app/templates/partials/tab_transfers.html', '                  <a href="/ui/transfers/print?ref={{ ref }}" target="_blank" style="text-decoration:none;">', '                  <a href="/ui/transfers/print?ref={{ ref }}" target="_blank" class="no-underline">')
edit('app/templates/partials/tab_transfers.html', '  <div id="transfer-edit" style="margin-top:12px;"></div>', '  <div id="transfer-edit" class="mt-12"></div>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/tab_transfers.html   # 0
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/tab_transfers.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T15: Plantillas sin `style=`: `tab_history.html` y `stock_table.html`

**Lote:** C · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_history.html`, `app/templates/partials/stock_table.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 11 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `tab_history.html` y `stock_table.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/tab_history.html', '          <th style="width:3%;">', '          <th class="w-3p">')
edit('app/templates/partials/tab_history.html', '          <th style="width:8%;">Tipo</th>', '          <th class="w-8p">Tipo</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:9%;">Usuario</th>', '          <th class="w-9p">Usuario</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:9%;">SKU</th>', '          <th class="w-9p">SKU</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:23%;">Producto</th>', '          <th class="w-23p">Producto</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:6%;" class="text-center">Unidad</th>', '          <th class="text-center w-6p">Unidad</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:7%;" class="text-right">Cantidad</th>', '          <th class="text-right w-7p">Cantidad</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:8%;" class="text-right">Costo Unit.</th>', '          <th class="text-right w-8p">Costo Unit.</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:8%;" class="text-right">Precio Unit.</th>', '          <th class="text-right w-8p">Precio Unit.</th>')
edit('app/templates/partials/tab_history.html', '          <th style="width:5%;">Nota</th>', '          <th class="w-5p">Nota</th>')
edit('app/templates/partials/stock_table.html', '          <th style="width:3%;">', '          <th class="w-3p">')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/tab_history.html app/templates/partials/stock_table.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/tab_history.html`, `app/templates/partials/stock_table.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T16: Plantillas sin `style=`: `purchase_panel.html`

**Lote:** C · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/purchase_panel.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 10 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `purchase_panel.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/purchase_panel.html', '  <h3 style="margin-top:0;">Importar compra desde factura (PDF)</h3>', '  <h3 class="mt-0">Importar compra desde factura (PDF)</h3>')
edit('app/templates/partials/purchase_panel.html', '  <p class="muted" style="margin-top:-6px;margin-bottom:10px;">', '  <p class="muted mt-n6 mb-10">')
edit('app/templates/partials/purchase_panel.html', '  <form hx-post="/ui/purchase/from-invoice" hx-target="#purchase-panel" hx-swap="outerHTML" hx-encoding="multipart/form-data" enctype="multipart/form-data" style="margin-bottom:14px;">', '  <form hx-post="/ui/purchase/from-invoice" hx-target="#purchase-panel" hx-swap="outerHTML" hx-encoding="multipart/form-data" enctype="multipart/form-data" class="mb-14">')
edit('app/templates/partials/purchase_panel.html', '    <div class="warn" style="padding:12px;border-radius:10px;margin-top:12px;">', '    <div class="warn msg-box mt-12">')
edit('app/templates/partials/purchase_panel.html', '      <div class="muted" style="margin-top:6px;">', '      <div class="muted mt-6">')
edit('app/templates/partials/purchase_panel.html', '      <div style="margin-top:8px;">', '      <div class="mt-8">')
edit('app/templates/partials/purchase_panel.html', '    <p class="muted" style="margin-top:0;margin-bottom:10px;">Registro manual: el costo se guarda tal como lo introduces (sin conversión automática).</p>', '    <p class="muted mt-0 mb-10">Registro manual: el costo se guarda tal como lo introduces (sin conversión automática).</p>')
edit('app/templates/partials/purchase_panel.html', '    <div class="{{ message_class }}" style="padding:12px;border-radius:10px;margin-top:12px;">', '    <div class="{{ message_class }} msg-box mt-12">')
edit('app/templates/partials/purchase_panel.html', '  <h3 style="margin-top:16px;">Historial de compras</h3>', '  <h3 class="mt-16">Historial de compras</h3>')
edit('app/templates/partials/purchase_panel.html', '  <div id="purchase-edit" style="margin-top:12px;"></div>', '  <div id="purchase-edit" class="mt-12"></div>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/purchase_panel.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt | tail -1   # OK (cierre del lote C; si FALLO: PARAR y reportar)
```

#### Expected Report

- [ ] `app/templates/partials/purchase_panel.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T17: Plantillas sin `style=`: `sale_panel.html` y `action_result.html`

**Lote:** D · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/sale_panel.html`, `app/templates/partials/action_result.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 9 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `sale_panel.html` y `action_result.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/sale_panel.html', '    <h3 style="margin-top:0;">Registrar venta</h3>', '    <h3 class="mt-0">Registrar venta</h3>')
edit('app/templates/partials/sale_panel.html', '    <p class="muted" style="margin-top:-6px;">Usa una sola casilla para buscar por nombre/SKU o escanear el código de barras.</p>', '    <p class="muted mt-n6">Usa una sola casilla para buscar por nombre/SKU o escanear el código de barras.</p>')
edit('app/templates/partials/sale_panel.html', '    <div class="{{ message_class }}" style="padding:12px;border-radius:10px;margin-top:12px;">', '    <div class="{{ message_class }} msg-box mt-12">')
edit('app/templates/partials/sale_panel.html', '  <h3 style="margin-top:16px;">Historial de ventas</h3>', '  <h3 class="mt-16">Historial de ventas</h3>')
edit('app/templates/partials/sale_panel.html', '      <div style="display:flex; align-items:center; gap:8px;">', '      <div class="flex items-center gap-8">')
edit('app/templates/partials/sale_panel.html', '        <label for="sales-filter-show-all" class="filter-label" style="margin:0;">Ver todos</label>', '        <label for="sales-filter-show-all" class="filter-label m-0">Ver todos</label>')
edit('app/templates/partials/sale_panel.html', '  <div id="sale-edit" style="margin-top:12px;"></div>', '  <div id="sale-edit" class="mt-12"></div>')
edit('app/templates/partials/action_result.html', '<div class="{{ cls }}" style="padding:12px;border-radius:10px;">', '<div class="{{ cls }} msg-box">')
edit('app/templates/partials/action_result.html', '<div style="margin-top:12px;">', '<div class="mt-12">')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/sale_panel.html app/templates/partials/action_result.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/sale_panel.html`, `app/templates/partials/action_result.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T18: Plantillas sin `style=`: `tab_profit_items.html`

**Lote:** D · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_profit_items.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 7 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `tab_profit_items.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/tab_profit_items.html', '    <div class="flex gap-12 items-center flex-wrap" style="margin: 8px 0 10px 0;">', '    <div class="flex gap-12 items-center flex-wrap my-8-10">')
edit('app/templates/partials/tab_profit_items.html', '    <div class="{{ message_class }}" style="padding:12px;border-radius:10px;margin-top:12px;">', '    <div class="{{ message_class }} msg-box mt-12">')
edit('app/templates/partials/tab_profit_items.html', '            <button type="button" style="min-width: 85px;"', '            <button type="button" class="min-w-85"')
edit('app/templates/partials/tab_profit_items.html', '              <button type="button" class="ml-8" style="min-width: 85px;"\n                      hx-get="/ui/tabs/profit-items/purchase/{{ r.source_movement_id }}/edit?query={{ (filter_query or \'\')|urlencode }}&start_date={{ (filter_start_date or \'\')|urlencode }}&end_date={{ (filter_end_date or \'\')|urlencode }}&location_code={{ (location_code or \'\')|urlencode }}"', '              <button type="button" class="ml-8 min-w-85"\n                      hx-get="/ui/tabs/profit-items/purchase/{{ r.source_movement_id }}/edit?query={{ (filter_query or \'\')|urlencode }}&start_date={{ (filter_start_date or \'\')|urlencode }}&end_date={{ (filter_end_date or \'\')|urlencode }}&location_code={{ (location_code or \'\')|urlencode }}"')
edit('app/templates/partials/tab_profit_items.html', '              <button type="button" class="ml-8" style="min-width: 85px;"\n                      hx-get="/ui/tabs/profit-items/transfer-in/{{ r.source_movement_id }}/edit?query={{ (filter_query or \'\')|urlencode }}&start_date={{ (filter_start_date or \'\')|urlencode }}&end_date={{ (filter_end_date or \'\')|urlencode }}&location_code={{ (location_code or \'\')|urlencode }}"', '              <button type="button" class="ml-8 min-w-85"\n                      hx-get="/ui/tabs/profit-items/transfer-in/{{ r.source_movement_id }}/edit?query={{ (filter_query or \'\')|urlencode }}&start_date={{ (filter_start_date or \'\')|urlencode }}&end_date={{ (filter_end_date or \'\')|urlencode }}&location_code={{ (location_code or \'\')|urlencode }}"')
edit('app/templates/partials/tab_profit_items.html', '              <button type="button" class="ml-8" style="min-width: 85px;"', '              <button type="button" class="ml-8 min-w-85"')
edit('app/templates/partials/tab_profit_items.html', '              <span class="muted ml-8" style="display: inline-block; min-width: 85px;">-</span>', '              <span class="muted ml-8 d-inline-block min-w-85">-</span>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/tab_profit_items.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/tab_profit_items.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T19: Plantillas sin `style=`: `purchases_history.html` y `sales_history.html`

**Lote:** D · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/purchases_history.html`, `app/templates/partials/sales_history.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 6 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `purchases_history.html` y `sales_history.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/purchases_history.html', '    <div class="flex gap-12 items-center flex-wrap" style="margin: 8px 0 10px 0;">', '    <div class="flex gap-12 items-center flex-wrap my-8-10">')
edit('app/templates/partials/purchases_history.html', '              <img src="{{ image_url }}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb;" />', '              <img src="{{ image_url }}" alt="" class="thumb" />')
edit('app/templates/partials/purchases_history.html', '              <div style="display:flex; align-items:center; gap:8px;">', '              <div class="flex items-center gap-8">')
edit('app/templates/partials/purchases_history.html', '                <a href="/ui/purchase/{{ id }}/label?copies={{ (qty|int) if (qty|int) > 0 else 1 }}&auto_print=1" target="_blank" style="text-decoration:none;">', '                <a href="/ui/purchase/{{ id }}/label?copies={{ (qty|int) if (qty|int) > 0 else 1 }}&auto_print=1" target="_blank" class="no-underline">')
edit('app/templates/partials/sales_history.html', '  <div class="flex gap-12 items-center flex-wrap" style="margin: 8px 0 10px 0;">', '  <div class="flex gap-12 items-center flex-wrap my-8-10">')
edit('app/templates/partials/sales_history.html', '            <img src="{{ image_url }}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb;" />', '            <img src="{{ image_url }}" alt="" class="thumb" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/purchases_history.html app/templates/partials/sales_history.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/purchases_history.html`, `app/templates/partials/sales_history.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T20: Plantillas sin `style=`: `restock_table.html` y `products_history.html`

**Lote:** D · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/restock_table.html`, `app/templates/partials/products_history.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `restock_table.html` y `products_history.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/restock_table.html', '<div style="display:flex; gap:12px; align-items:baseline; justify-content:space-between;">', '<div class="flex gap-12 items-baseline justify-between">')
edit('app/templates/partials/restock_table.html', '  <div style="margin: 6px 0 10px 0;">', '  <div class="my-6-10">')
edit('app/templates/partials/restock_table.html', '    <span style="display:inline-flex;align-items:center;padding:4px 10px;border-radius:999px;background:#eef6ff;color:#0b2d42;font-size:12px;font-weight:600;">', '    <span class="badge-info">')
edit('app/templates/partials/products_history.html', '            <img src="{{ p.image_url }}" alt="" style="width:36px;height:36px;object-fit:cover;border-radius:8px;border:1px solid #e5e7eb;" />', '            <img src="{{ p.image_url }}" alt="" class="thumb" />')
edit('app/templates/partials/products_history.html', '          <button type="button" style="margin-left:8px;background: var(--brand-red);"', '          <button type="button" class="ml-8 btn-danger"')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/restock_table.html app/templates/partials/products_history.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt | tail -1   # OK (cierre del lote D; si FALLO: PARAR y reportar)
```

#### Expected Report

- [ ] `app/templates/partials/restock_table.html`, `app/templates/partials/products_history.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T21: Plantillas sin `style=`: `product_edit_form.html` y `product_edit_form_inventory.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/product_edit_form.html`, `app/templates/partials/product_edit_form_inventory.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `product_edit_form.html` y `product_edit_form_inventory.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/product_edit_form.html', '        <div class="error" style="padding:10px;border-radius:8px;margin-bottom:10px;"><strong>Error al actualizar producto</strong><div>{{ form_error }}</div></div>', '        <div class="error msg-box-sm mb-10"><strong>Error al actualizar producto</strong><div>{{ form_error }}</div></div>')
edit('app/templates/partials/product_edit_form.html', '              <div style="margin-bottom:6px;">', '              <div class="mb-6">')
edit('app/templates/partials/product_edit_form.html', '                <img src="{{ product.image_url }}" alt="{{ product.name }}" style="width:64px; height:64px; object-fit:cover; border-radius:8px; border:1px solid #e5e7eb;" />', '                <img src="{{ product.image_url }}" alt="{{ product.name }}" class="thumb thumb-lg" />')
edit('app/templates/partials/product_edit_form_inventory.html', '              <div style="margin-bottom:6px;">', '              <div class="mb-6">')
edit('app/templates/partials/product_edit_form_inventory.html', '                <img src="{{ product.image_url }}" alt="{{ product.name }}" style="width:64px; height:64px; object-fit:cover; border-radius:8px; border:1px solid #e5e7eb;" />', '                <img src="{{ product.image_url }}" alt="{{ product.name }}" class="thumb thumb-lg" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/product_edit_form.html app/templates/partials/product_edit_form_inventory.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/product_edit_form.html`, `app/templates/partials/product_edit_form_inventory.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T22: Plantillas sin `style=`: `dashboard.html` y `extraction_edit_form.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/dashboard.html`, `app/templates/partials/extraction_edit_form.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `dashboard.html` y `extraction_edit_form.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/dashboard.html', '              <div class="muted" style="margin-top:6px;">Negocio: {{ (active_business.name if active_business else \'Recambios\') }}</div>\n              <form method="post" action="/ui/active-business" style="margin-top:8px;">', '              <div class="muted mt-6">Negocio: {{ (active_business.name if active_business else \'Recambios\') }}</div>\n              <form method="post" action="/ui/active-business" style="margin-top:8px;">')
edit('app/templates/dashboard.html', '              <form method="post" action="/ui/active-business" style="margin-top:8px;">', '              <form method="post" action="/ui/active-business" class="mt-8">')
edit('app/templates/dashboard.html', '                <select name="business_id" data-js-autosubmit style="width:100%; padding:8px; border-radius:10px; border:1px solid var(--brand-border);">', '                <select name="business_id" data-js-autosubmit class="select-business">')
edit('app/templates/dashboard.html', '              <div class="muted" style="margin-top:6px;">Negocio: {{ (active_business.name if active_business else \'Recambios\') }}</div>', '              <div class="muted mt-6">Negocio: {{ (active_business.name if active_business else \'Recambios\') }}</div>')
edit('app/templates/partials/extraction_edit_form.html', '          <select name="party" style="width:100%; padding:8px; border:1px solid var(--brand-border); border-radius:8px; background: var(--brand-surface); color: var(--brand-text);">', '          <select name="party" class="select-plain">')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/dashboard.html app/templates/partials/extraction_edit_form.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/dashboard.html`, `app/templates/partials/extraction_edit_form.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T23: Plantillas sin `style=`: `sales_document_edit_form.html` y `customer_detail.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/sales_document_edit_form.html`, `app/templates/partials/customer_detail.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 2 ediciones; cada `edit()` exige exactamente 1 coincidencia.

#### Objective

Quitar los atributos `style` de `sales_document_edit_form.html` y `customer_detail.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/partials/sales_document_edit_form.html', '    <h4 style="margin-top:14px;">Items</h4>', '    <h4 class="mt-14">Items</h4>')
edit('app/templates/partials/customer_detail.html', '  <h4 style="margin-top:14px;">Documentos</h4>', '  <h4 class="mt-14">Documentos</h4>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/partials/sales_document_edit_form.html app/templates/partials/customer_detail.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/partials/sales_document_edit_form.html`, `app/templates/partials/customer_detail.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T24: Plantillas sin `style=`: `ui.html`

**Lote:** E · **REQs:** REQ-2
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/components/ui.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: las macros pierden sus parámetros de estilo libre (`card`/`subcard` sin `style`; `modal(container_class=)`).

#### Objective

Quitar los atributos `style` de `ui.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/components/ui.html', "{% macro card(title=None, id=None, style=None, extra_class='') -%}", "{% macro card(title=None, id=None, extra_class='') -%}")
edit('app/templates/components/ui.html', "{% macro subcard(title=None, id=None, style=None, extra_class='') -%}", "{% macro subcard(title=None, id=None, extra_class='') -%}")
edit('app/templates/components/ui.html', '  <div class="card{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}{%- if style %} style="{{ style }}"{%- endif -%}>', '  <div class="card{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}>')
edit('app/templates/components/ui.html', '  <div class="card subcard{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}{%- if style %} style="{{ style }}"{%- endif -%}>', '  <div class="card subcard{%- if extra_class %} {{ extra_class }}{%- endif -%}"{%- if id %} id="{{ id }}"{%- endif -%}>')
edit('app/templates/components/ui.html', '      <input type="number" name="year" value="{{ current_year }}" min="2020" max="2099" class="year-filter" style="width: 100px;" />', '      <input type="number" name="year" value="{{ current_year }}" min="2020" max="2099" class="year-filter w-100px" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/components/ui.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/components/ui.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T25: Plantillas sin `style=`: `modal.html` y `transfer_bulk_modal.html`

**Lote:** E · **REQs:** REQ-2
**Depends on:** T8 (clases en app.css)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/components/modal.html`, `app/templates/partials/transfer_bulk_modal.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 8 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: las macros pierden sus parámetros de estilo libre (`card`/`subcard` sin `style`; `modal(container_class=)`).

#### Objective

Quitar los atributos `style` de `modal.html` y `transfer_bulk_modal.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/components/modal.html', 'close_label=None, container_style=None) -%}', 'close_label=None, container_class=None) -%}')
edit('app/templates/components/modal.html', '  <div class="modal-container{% if wide %} modal-container-wide{% endif %}"{% if container_style %} style="{{ container_style }}"{% endif %}>', '  <div class="modal-container{% if wide %} modal-container-wide{% endif %}{% if container_class %} {{ container_class }}{% endif %}">')
edit('app/templates/partials/transfer_bulk_modal.html', "container_style='max-width: 760px;')", "container_class='modal-container-760')")
edit('app/templates/partials/transfer_bulk_modal.html', '          <div class="flex gap-10" style="align-items: center;">', '          <div class="flex gap-10 items-center">')
edit('app/templates/partials/transfer_bulk_modal.html', '        <div class="table-wrapper" style="max-height: 48vh; overflow-y: auto;">', '        <div class="table-wrapper scroll-48vh">')
edit('app/templates/partials/transfer_bulk_modal.html', '                <th style="width: 3%;">', '                <th class="w-3p">')
edit('app/templates/partials/transfer_bulk_modal.html', '                <th style="width: 120px;" class="text-right">Stock</th>', '                <th class="text-right w-120px">Stock</th>')
edit('app/templates/partials/transfer_bulk_modal.html', '                <th style="width: 150px;" class="text-right">Cantidad</th>', '                <th class="text-right w-150px">Cantidad</th>')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/components/modal.html app/templates/partials/transfer_bulk_modal.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/components/modal.html`, `app/templates/partials/transfer_bulk_modal.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T26: Plantillas sin `style=`: `login.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T10, T11 (hojas nuevas)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/login.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: el bloque `<style>` se sustituye por el `<link>` de su hoja (creada en el lote B).

#### Objective

Quitar los atributos `style` de `login.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/login.html', '\n    <style>\n      :root {\n        --brand-navy: #0b2d42;\n        --brand-border: #e5e7eb;\n        --brand-bg: #f8fafc;\n        --brand-surface: #ffffff;\n        --brand-text: #0f172a;\n        --brand-muted: #64748b;\n      }\n      *, *::before, *::after { box-sizing: border-box; }\n      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; background: var(--brand-bg); color: var(--brand-text); }\n      .card { max-width: 420px; margin: 80px auto; border: 1px solid var(--brand-border); border-radius: 12px; padding: 18px; background: var(--brand-surface); }\n      .row { display: grid; grid-template-columns: 120px 1fr; gap: 8px; margin-bottom: 10px; align-items: center; }\n      input { width: 100%; padding: 10px; border: 1px solid var(--brand-border); border-radius: 10px; background: var(--brand-surface); color: var(--brand-text); }\n      button { padding: 10px 12px; border: 0; border-radius: 10px; background: var(--brand-navy); color: white; cursor: pointer; width: 100%; }\n      .muted { color: var(--brand-muted); font-size: 13px; }\n      .error { background: #fef2f2; padding: 10px; border-radius: 10px; margin-bottom: 12px; }\n    </style>', '\n    <link rel="stylesheet" href="/static/css/login.css?v={{ static_version() }}" />')
edit('app/templates/login.html', '      <h2 style="margin-top:0;">Acceso</h2>', '      <h2 class="mt-0">Acceso</h2>')
edit('app/templates/login.html', '      <div class="muted" style="margin-bottom:12px;">Inicia sesión para usar el sistema.</div>', '      <div class="muted mb-12">Inicia sesión para usar el sistema.</div>')
edit('app/templates/login.html', '            <label style="display:flex; gap:8px; align-items:center;">', '            <label class="flex items-center gap-8">')
edit('app/templates/login.html', '              <input id="login-show-password" type="checkbox" style="width:auto;" />', '              <input id="login-show-password" type="checkbox" class="w-auto" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/login.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/login.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T27: Plantillas sin `style=`: `label_single_print.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T10, T11 (hojas nuevas)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/label_single_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 1 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: el bloque `<style>` se sustituye por el `<link>` de su hoja (creada en el lote B).

#### Objective

Quitar los atributos `style` de `label_single_print.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/label_single_print.html', '\n    <style>\n      @page { size: 60mm 30mm; margin: 0; }\n\n      html, body { width: 60mm; margin: 0; padding: 0; }\n      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #0f172a; }\n\n      .toolbar { padding: 10px 0 12px; display: flex; gap: 8px; align-items: center; }\n      .toolbar input { width: 90px; padding: 8px; border: 1px solid #e5e7eb; border-radius: 10px; }\n      .toolbar button { padding: 10px 12px; border: 0; border-radius: 10px; background: #0b2d42; color: white; cursor: pointer; }\n      .toolbar .muted { color: #64748b; font-size: 13px; }\n\n      .sheet { display: block; }\n\n      .page {\n        width: 60mm;\n        height: 30mm;\n      }\n\n      .label {\n        width: 60mm;\n        height: 30mm;\n        border: 1px solid transparent;\n        border-radius: 2mm;\n        padding: 2mm;\n        box-sizing: border-box;\n        display: grid;\n        grid-template-rows: auto 1fr;\n        align-items: start;\n        overflow: hidden;\n      }\n\n      .name {\n        font-weight: 700;\n        font-size: 10pt;\n        line-height: 1.05;\n        margin-bottom: 1mm;\n        overflow: hidden;\n        display: -webkit-box;\n        -webkit-line-clamp: 2;\n        -webkit-box-orient: vertical;\n      }\n\n      svg.barcode { width: 100%; height: 100%; }\n\n      @media print {\n        .toolbar { display: none; }\n        .label { border-color: transparent; }\n        .page { break-after: page; page-break-after: always; }\n        .page:last-child { break-after: auto; page-break-after: auto; }\n      }\n    </style>', '\n    <link rel="stylesheet" href="/static/css/label-print.css?v={{ static_version() }}" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/label_single_print.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/label_single_print.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T28: Plantillas sin `style=`: `transfer_print.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T10, T11 (hojas nuevas)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/transfer_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 5 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: el bloque `<style>` se sustituye por el `<link>` de su hoja (creada en el lote B).

#### Objective

Quitar los atributos `style` de `transfer_print.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/transfer_print.html', '\n    <style>\n      html, body { margin: 0; padding: 0; }\n      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #0f172a; padding: 18px; }\n\n      .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }\n      .toolbar button { padding: 10px 12px; border: 0; border-radius: 10px; background: #0b2d42; color: white; cursor: pointer; }\n      .muted { color: #64748b; }\n\n      h1 { font-size: 18px; margin: 0 0 8px; }\n      .meta { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 18px; margin-bottom: 14px; }\n      .meta div { font-size: 13px; }\n\n      table { width: 100%; border-collapse: collapse; table-layout: fixed; }\n      th, td { border-bottom: 1px solid #e5e7eb; padding: 8px 6px; font-size: 13px; }\n      th { text-align: left; background: #f8fafc; }\n      .num { text-align: right; font-variant-numeric: tabular-nums; }\n      code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }\n\n      @media print {\n        .toolbar { display: none; }\n        body { padding: 0; }\n      }\n    </style>', '\n    <link rel="stylesheet" href="/static/css/print.css?v={{ static_version() }}" />')
edit('app/templates/transfer_print.html', '        <col style="width:16%" />', '        <col class="w-16p" />')
edit('app/templates/transfer_print.html', '        <col style="width:12%" />', '        <col class="w-12p" />')
edit('app/templates/transfer_print.html', '        <col style="width:14%" />\n        <col style="width:14%" />', '        <col class="w-14p" />\n        <col style="width:14%" />')
edit('app/templates/transfer_print.html', '        <col style="width:14%" />', '        <col class="w-14p" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/transfer_print.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
```

#### Expected Report

- [ ] `app/templates/transfer_print.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T29: Plantillas sin `style=`: `restock_print.html`

**Lote:** E · **REQs:** REQ-2, REQ-3, REQ-6
**Depends on:** T10, T11 (hojas nuevas)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/restock_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Cada `style="…"` se sustituye por las clases equivalentes (tabla de `tests/snapshots/style_classes.py`); si la etiqueta ya tiene `class`, las clases se añaden al final. Las reglas ya existen en `app.css` (T8). Este envelope hace 1 ediciones; cada `edit()` exige exactamente 1 coincidencia. Además: el bloque `<style>` se sustituye por el `<link>` de su hoja (creada en el lote B).

#### Objective

Quitar los atributos `style` de `restock_print.html` sin cambiar ids, `data-*` ni el resto del marcado.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

edit('app/templates/restock_print.html', '\n    <style>\n      html, body { margin: 0; padding: 0; }\n      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color: #0f172a; padding: 18px; }\n\n      .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }\n      .toolbar button { padding: 10px 12px; border: 0; border-radius: 10px; background: #0b2d42; color: white; cursor: pointer; }\n      .muted { color: #64748b; }\n\n      h1 { font-size: 18px; margin: 0 0 8px; }\n      .meta { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 18px; margin-bottom: 14px; }\n      .meta div { font-size: 13px; }\n\n      table { width: 100%; border-collapse: collapse; table-layout: fixed; }\n      th, td { border-bottom: 1px solid #e5e7eb; padding: 8px 6px; font-size: 13px; }\n      th { text-align: left; background: #f8fafc; }\n      .num { text-align: right; font-variant-numeric: tabular-nums; }\n      code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }\n\n      @media print {\n        .toolbar { display: none; }\n        body { padding: 0; }\n      }\n    </style>', '\n    <link rel="stylesheet" href="/static/css/print.css?v={{ static_version() }}" />')
print("OK")
PY
````

#### Verification

```bash
grep -cE '\sstyle\s*=' app/templates/restock_print.html   # 0 en cada fichero
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py tests/unit/test_frontend_js_modules.py   # PASS
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt | tail -1   # OK (cierre del lote E; si FALLO: PARAR y reportar)
```

#### Expected Report

- [ ] `app/templates/restock_print.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T30: `components/barcode-svg.js`: código de barras sin `style`

**Lote:** F · **REQs:** REQ-5
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/barcode-svg.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

JsBarcode 3.11.6 es global (`window.JsBarcode`). En modo objeto (`JsBarcode({}, code, opts)`) solo rellena `obj.encodings` = `[{data: '1011…', text, options}]`. El módulo replica `prepareSVG`/`createGroup`/`drawSvgBarcode`/`drawSVGText`/`calculateEncodingAttributes` con las mismas operaciones y usa atributos de presentación (`fill`, `font-family`, `font-size`) en vez de `style`.

#### Objective

Crear `renderBarcodeSvg(svg, code, options)`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/barcode-svg.js` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````javascript
// Dibuja un código de barras en un <svg> SIN atributos style (CSP style-src 'self').
// JsBarcode (global) solo calcula las barras en modo objeto; la geometría replica su renderer SVG
// 3.11.6 con las mismas operaciones, y los estilos van como atributos de presentación (fill, font-*).
const SVG_NS = 'http://www.w3.org/2000/svg';

const DEFAULTS = {
  width: 2, height: 100, format: 'auto', displayValue: true, fontOptions: '', font: 'monospace',
  textAlign: 'center', textPosition: 'bottom', textMargin: 2, fontSize: 20,
  background: '#ffffff', lineColor: '#000000', margin: 10,
};

function withMargins(o) {
  o.marginTop = o.marginTop || o.margin;
  o.marginBottom = o.marginBottom || o.margin;
  o.marginRight = o.marginRight || o.margin;
  o.marginLeft = o.marginLeft || o.margin;
  return o;
}

function measureText(text, o) {
  const ctx = document.createElement('canvas').getContext('2d');
  ctx.font = o.fontOptions + ' ' + o.fontSize + 'px ' + o.font;
  const m = ctx.measureText(text);
  return m ? m.width : 0;
}

function layout(enc, o) {
  const textWidth = o.displayValue ? measureText(enc.text, o) : 0;
  const barcodeWidth = enc.data.length * o.width;
  enc.width = Math.ceil(Math.max(textWidth, barcodeWidth));
  enc.height = o.height + (o.displayValue && enc.text.length > 0 ? o.fontSize + o.textMargin : 0)
    + o.marginTop + o.marginBottom;
  enc.barcodePadding = 0;
  if (o.displayValue && barcodeWidth < textWidth) {
    if (o.textAlign === 'center') enc.barcodePadding = Math.floor((textWidth - barcodeWidth) / 2);
    else if (o.textAlign === 'right') enc.barcodePadding = Math.floor(textWidth - barcodeWidth);
  }
}

function el(name, attrs, parent) {
  const node = document.createElementNS(SVG_NS, name);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  parent.appendChild(node);
  return node;
}

function rect(x, y, w, h, parent) {
  return el('rect', { x, y, width: w, height: h }, parent);
}

export function renderBarcodeSvg(svg, code, options) {
  const base = withMargins(Object.assign({}, DEFAULTS, options));
  const target = {};
  window.JsBarcode(target, code, Object.assign({}, options));
  const encodings = target.encodings || [];
  const opts = encodings.map((enc) => withMargins(Object.assign({}, base, enc.options)));
  encodings.forEach((enc, i) => layout(enc, opts[i]));

  while (svg.firstChild) svg.removeChild(svg.firstChild);
  const total = encodings.reduce((acc, enc) => acc + enc.width, 0);
  const height = encodings.reduce((acc, enc) => Math.max(acc, enc.height), 0);
  const width = total + base.marginLeft + base.marginRight;
  svg.setAttribute('width', width + 'px');
  svg.setAttribute('height', height + 'px');
  svg.setAttribute('x', '0px');
  svg.setAttribute('y', '0px');
  svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
  svg.setAttribute('version', '1.1');
  if (base.background) rect(0, 0, width, height, svg).setAttribute('fill', base.background);

  let x = base.marginLeft;
  encodings.forEach((enc, i) => {
    const o = opts[i];
    const g = el('g', { transform: 'translate(' + x + ', ' + o.marginTop + ')', fill: o.lineColor }, svg);
    const yFrom = o.textPosition === 'top' ? o.fontSize + o.textMargin : 0;
    let run = 0;
    let pos = 0;
    for (let b = 0; b < enc.data.length; b++) {
      pos = b * o.width + enc.barcodePadding;
      if (enc.data[b] === '1') run++;
      else if (run > 0) { rect(pos - o.width * run, yFrom, o.width * run, o.height, g); run = 0; }
    }
    if (run > 0) rect(pos - o.width * (run - 1), yFrom, o.width * run, o.height, g);
    if (o.displayValue) {
      const y = o.textPosition === 'top' ? o.fontSize - o.textMargin : o.height + o.textMargin + o.fontSize;
      let tx;
      let anchor;
      if (o.textAlign === 'left' || enc.barcodePadding > 0) { tx = 0; anchor = 'start'; }
      else if (o.textAlign === 'right') { tx = enc.width - 1; anchor = 'end'; }
      else { tx = enc.width / 2; anchor = 'middle'; }
      const attrs = { 'text-anchor': anchor, x: tx, y, 'font-family': o.font, 'font-size': o.fontSize + 'px' };
      if (/bold/.test(o.fontOptions)) attrs['font-weight'] = 'bold';
      if (/italic/.test(o.fontOptions)) attrs['font-style'] = 'italic';
      el('text', attrs, g).appendChild(document.createTextNode(enc.text));
    }
    x += enc.width;
  });
}
````

#### Verification

```bash
node --check app/static/js/components/barcode-svg.js && echo syntax-ok   # syntax-ok
```

#### Expected Report

- [ ] `app/static/js/components/barcode-svg.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T31: Historial de compras y etiqueta dibujan con `renderBarcodeSvg`

**Lote:** F · **REQs:** REQ-5
**Depends on:** T30
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/barcode.js`, `app/static/js/pages/label-print.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`renderBarcodeSvg` acepta las mismas opciones que `window.JsBarcode`; los dos llamadores ya envuelven la llamada en `try {} catch (e) {}`.

#### Objective

Sustituir las dos llamadas a JsBarcode en modo SVG.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
node --check app/static/js/components/barcode.js && node --check app/static/js/pages/label-print.js && echo ok   # ok
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_frontend_js_modules.py   # PASS (grafo de módulos sin huérfanos)
```

#### Expected Report

- [ ] `app/static/js/components/barcode.js`, `app/static/js/pages/label-print.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T32: `components/chart-palette.js` y tema sin recarga (`features/theme.js`)

**Lote:** F · **REQs:** REQ-7, REQ-8
**Depends on:** T9 (tokens)
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/chart-palette.js`, `app/static/js/features/theme.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

`theme.js` (importado por `main.js`, exporta `init()`) hoy hace `location.reload()`. `theme-init.js` aplica `localStorage['gi-theme']` antes del primer pintado. Hasta T34 los gráficos aún no escuchan el evento: cambiar de tema recolorea el CSS y los gráficos se actualizan al recargar.

#### Objective

Crear el módulo de paleta y emitir `gi:themechange` en vez de recargar.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/chart-palette.js` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````javascript
// Colores de los gráficos desde las variables CSS de app.css (:root y [data-theme="dark"]).
// Se leen en cada render: tras un cambio de tema (evento gi:themechange) ya traen la paleta nueva.
function rootStyle() {
  return getComputedStyle(document.documentElement);
}

export function cssVar(name) {
  return rootStyle().getPropertyValue(name).trim();
}

// --<prefix>-1, --<prefix>-2, … hasta la primera vacía.
export function cssList(prefix) {
  const style = rootStyle();
  const out = [];
  for (let i = 1; i <= 24; i++) {
    const value = style.getPropertyValue(`--${prefix}-${i}`).trim();
    if (!value) break;
    out.push(value);
  }
  return out;
}

export const THEME_EVENT = 'gi:themechange';
````

**Step 2: escribir `app/static/js/features/theme.js` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````javascript
// Botón de tema claro/oscuro ([data-js-theme-toggle]). Sin recarga: el CSS cambia solo con
// data-theme y los gráficos se redibujan al recibir gi:themechange (components/chart-palette.js).
import { THEME_EVENT } from '../components/chart-palette.js';

function onClick(evt) {
  if (!evt.target.closest('[data-js-theme-toggle]')) return;
  const root = document.documentElement;
  const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  try { localStorage.setItem('gi-theme', next); } catch (e) {}
  document.dispatchEvent(new CustomEvent(THEME_EVENT, { detail: { theme: next } }));
}

export function init() {
  document.addEventListener('click', onClick);
}
````

#### Verification

```bash
node --check app/static/js/components/chart-palette.js && node --check app/static/js/features/theme.js && echo ok   # ok
```

#### Expected Report

- [ ] `app/static/js/components/chart-palette.js`, `app/static/js/features/theme.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T33: `widget-chart.js`: colores desde CSS y redibujo al cambiar de tema

**Lote:** F · **REQs:** REQ-7, REQ-8
**Depends on:** T32
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/widget-chart.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`chart-palette.js` exporta `cssVar(name)`, `cssList(prefix)` y `THEME_EVENT`. `renderWidgetCharts(root)` ya destruye la instancia previa de cada canvas.

#### Objective

Quitar las paletas y colores fijos del módulo.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
node --check app/static/js/components/widget-chart.js && echo ok   # ok
grep -cE "#[0-9a-fA-F]{3,8}|rgba\(" app/static/js/components/widget-chart.js   # 0
```

#### Expected Report

- [ ] `app/static/js/components/widget-chart.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T34: `home-charts.js`: colores desde CSS y redibujo al cambiar de tema

**Lote:** F · **REQs:** REQ-7, REQ-8
**Depends on:** T32
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/home-charts.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`enhance(root, fromSwap)` redibuja los 3 gráficos de Inicio (`replaceChart` destruye el anterior).

#### Objective

Quitar los colores fijos del módulo.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
node --check app/static/js/components/home-charts.js && echo ok   # ok
grep -cE "#[0-9a-fA-F]{3,8}|rgba\(" app/static/js/components/home-charts.js   # 0
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE tests/e2e/test_ui_tour_csp_e2e.py   # PASS
```

#### Expected Report

- [ ] `app/static/js/components/home-charts.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T35: CSP `style-src 'self'` y meta `htmx-config`

**Lote:** G · **REQs:** REQ-1, REQ-4
**Depends on:** lotes B-F
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/layouts/app_shell.html`, `app/main.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

htmx 1.9.12 lee `<meta name="htmx-config">` al cargar, antes de inyectar su hoja de indicador. `attributesToSettle` por defecto incluye `style`: en el *settle* copiaba con `setAttribute` el `style` que Chart.js pone por CSSOM en los `<canvas>` de Inicio. Las reglas de `.htmx-indicator` ya están en `app.css` (T8).

#### Objective

Quitar `'unsafe-inline'` de `style-src` sin que htmx escriba estilos inline.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
grep -c "style-src 'self'; " app/main.py   # 1
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE tests/e2e/test_ui_tour_csp_e2e.py   # PASS
```

#### Expected Report

- [ ] `app/templates/layouts/app_shell.html`, `app/main.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T36: Guardas estáticas `tests/unit/test_frontend_css.py`

**Lote:** G · **REQs:** REQ-1, REQ-2, REQ-4..REQ-9
**Depends on:** T35
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/unit/test_frontend_css.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

Mismo estilo que `tests/unit/test_frontend_js_modules.py` (lee plantillas/JS del disco; CSP vía `TestClient(app).get('/health')` sin lifespan).

#### Objective

Fijar con tests todo lo que la fase garantiza.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/unit/test_frontend_css.py` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````python
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
EXPECTED_STYLESHEETS = {
    "layouts/app_shell.html": ["/static/css/app.css"],
    "login.html": ["/static/css/login.css"],
    "transfer_print.html": ["/static/css/print.css"],
    "restock_print.html": ["/static/css/print.css"],
    "label_single_print.html": ["/static/css/label-print.css"],
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
            """"attributesToSettle": ["class", "width", "height"]}' />""")
    assert meta in shell
    # La meta va antes de htmx: htmx lee su configuración al cargar.
    assert shell.index(meta) < shell.index("/static/js/htmx.min.js")
    css = (CSS_DIR / "app.css").read_text(encoding="utf-8")
    assert ".htmx-indicator { opacity: 0; }" in css


def test_chart_modules_take_colors_from_css() -> None:
    color = re.compile(r"#[0-9a-fA-F]{3,8}\b|rgba?\(")
    for name in ("components/home-charts.js", "components/widget-chart.js"):
        assert color.findall((JS_DIR / name).read_text(encoding="utf-8")) == [], name
    css = (CSS_DIR / "app.css").read_text(encoding="utf-8")
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
````

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_frontend_css.py   # 10 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q   # todo PASS
```

#### Expected Report

- [ ] `tests/unit/test_frontend_css.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T37: e2e de la fase: `tests/e2e/test_css_csp_e2e.py`

**Lote:** H · **REQs:** REQ-4, REQ-5, REQ-6, REQ-7, REQ-9
**Depends on:** lote G
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_css_csp_e2e.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal, no cargar skills

#### Context

Fixtures existentes: `tour_server -> (base_url, ids)`, `page`, `js_errors` (autouse). Con la CSP estricta, `wait_for_function` solo con funciones flecha (una expresión suelta usa `eval`, que la CSP bloquea).

#### Objective

Probar en el navegador lo que garantizan REQ-4..REQ-7 y la guarda.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/e2e/test_css_csp_e2e.py` con EXACTAMENTE este contenido** (fichero nuevo; crear el directorio si no existe):

````python
"""Fase 3 del release 0.5.0: CSS sin estilos inline con CSP ``style-src 'self'``.

Códigos de barras sin ``style`` (y con la geometría de JsBarcode), htmx sin hoja inyectada ni ``style`` en el settle,
colores por tokens y la guarda ``js_errors`` ante un estilo inline bloqueado. El cambio de tema sin recarga está en
``test_js_behaviors_e2e.py::test_theme_toggle_without_reload``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from tests.e2e.test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

HAS_STYLE = "(svg) => svg.hasAttribute('style') || svg.querySelector('[style]') !== null"

# Dibuja el mismo código con JsBarcode (SVG) y con renderBarcodeSvg y compara la geometría. JsBarcode intenta
# escribir style="…" (la CSP lo bloquea y avisa en consola): el test vacía js_errors al final.
GEOMETRY_JS = """async () => {
  const { renderBarcodeSvg } = await import('/static/js/components/barcode-svg.js');
  const NS = 'http://www.w3.org/2000/svg';
  const dump = (svg) => JSON.stringify([['width', 'height', 'viewBox'].map((a) => svg.getAttribute(a)),
    [...svg.querySelectorAll('rect, g, text')].map((n) => [n.tagName,
      ...['x', 'y', 'width', 'height', 'transform', 'text-anchor'].map((a) => n.getAttribute(a)),
      n.tagName === 'text' ? n.textContent : ''])]);
  const opts = [
    { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 },
    { format: 'CODE128', displayValue: true, fontSize: 12, textMargin: 0, height: 40, margin: 0, width: 1.2 },
    { format: 'CODE128', displayValue: true, fontSize: 40, textMargin: 0, height: 40, margin: 0, width: 1 },
  ];
  const bad = [];
  for (const code of ['TOUR-1', 'SKU000502', 'A', 'abcdefghijklmnopqrstuvwxyz0123456789']) {
    for (const o of opts) {
      const a = document.createElementNS(NS, 'svg');
      const b = document.createElementNS(NS, 'svg');
      window.JsBarcode(a, code, Object.assign({}, o));
      renderBarcodeSvg(b, code, Object.assign({}, o));
      if (dump(a) !== dump(b)) bad.push([code, JSON.stringify(o), dump(a), dump(b)]);
    }
  }
  return bad;
}"""


def test_dashboard_has_no_style_elements_and_indicator_is_hidden(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    assert page.evaluate("document.querySelectorAll('style').length") == 0
    open_tab(page, "transfers")
    expect(page.locator("#transfer-loading")).to_have_css("opacity", "0")
    assert js_errors == []


def test_reopening_home_does_not_settle_style_attributes(tour_server, page: Page, js_errors: list[str]) -> None:
    # Chart.js pone style (por CSSOM) en los <canvas> de Inicio; el settle de htmx lo copiaba con setAttribute.
    base_url, _ = tour_server
    login(page, base_url)
    page.wait_for_function("() => window.Chart && window.Chart.getChart(document.getElementById('monthly-chart'))")
    open_tab(page, "home")
    open_tab(page, "home")
    assert js_errors == []


def test_purchase_history_barcode_renders_without_inline_styles(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "purchases")
    svg = page.locator('svg.barcode[data-code="TOUR-1"]').first
    expect(svg).to_have_attribute("data-rendered", "1")
    assert svg.locator("g rect").count() > 0
    assert svg.evaluate(HAS_STYLE) is False
    # Fondo blanco y barras negras como atributos de presentación (antes style="fill:…").
    expect(svg.locator(":scope > rect")).to_have_attribute("fill", "#ffffff")
    expect(svg.locator("g")).to_have_attribute("fill", "#000000")
    assert js_errors == []


def test_label_barcode_has_text_and_no_inline_styles(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    login(page, base_url)
    page.goto(f"{base_url}/ui/purchase/{ids['purchase']}/label")
    svg = page.locator("#sheet svg.barcode").first
    expect(svg.locator("text")).to_have_text("TOUR-1")
    expect(svg.locator("text")).to_have_attribute("font-size", "12px")
    expect(svg.locator("text")).to_have_attribute("font-family", "monospace")
    assert svg.evaluate(HAS_STYLE) is False
    assert js_errors == []


def test_barcode_geometry_matches_jsbarcode(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    assert page.evaluate(GEOMETRY_JS) == []
    js_errors.clear()  # avisos CSP provocados a propósito por el renderer SVG de JsBarcode


@pytest.mark.parametrize("theme, border", [("light", "rgb(229, 231, 235)"), ("dark", "rgb(51, 65, 85)")])
def test_thumbnail_border_follows_theme(tour_server, page: Page, js_errors: list[str], theme, border) -> None:
    base_url, _ = tour_server
    page.add_init_script(f"localStorage.setItem('gi-theme', '{theme}')")
    login(page, base_url)
    got = page.evaluate("""() => { const img = document.createElement('img'); img.className = 'thumb';
        document.body.appendChild(img); const c = getComputedStyle(img).borderTopColor; img.remove(); return c; }""")
    assert got == border
    assert js_errors == []


def test_home_monthly_chart_colors_come_from_css(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    page.wait_for_function("() => window.Chart && window.Chart.getChart(document.getElementById('monthly-chart'))")
    colors = page.evaluate(
        "window.Chart.getChart(document.getElementById('monthly-chart')).data.datasets.map((d) => d.backgroundColor)")
    assert colors == ["rgba(11,45,66,0.85)", "rgba(203, 58, 66, 0.75)", "rgba(16, 185, 129, 0.75)"]
    assert js_errors == []


def test_js_errors_guard_reports_blocked_inline_style(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    page.evaluate("document.body.setAttribute('style', 'color: red')")
    page.wait_for_timeout(200)
    assert any("Content Security Policy" in e for e in js_errors), js_errors
    assert page.evaluate("getComputedStyle(document.body).color") != "rgb(255, 0, 0)"
    js_errors.clear()  # el teardown del fixture fallaría con el error provocado a propósito
````

#### Verification

```bash
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE tests/e2e/test_css_csp_e2e.py   # 9 passed
```

#### Expected Report

- [ ] `tests/e2e/test_css_csp_e2e.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T38: e2e del tema: sin recarga, redibujo con la paleta oscura y persistencia

**Lote:** H · **REQs:** REQ-8
**Depends on:** T37
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_js_behaviors_e2e.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`test_js_behaviors_e2e.py` ya importa `login, open_tab`. La siembra del tour crea un widget `pie` (primer color claro `#0b2d42`, oscuro `#60a5fa`).

#### Objective

Sustituir el e2e que esperaba una navegación al cambiar de tema.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE tests/e2e/test_js_behaviors_e2e.py   # PASS
```

#### Expected Report

- [ ] `tests/e2e/test_js_behaviors_e2e.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T39: `check` final de estilos calculados (con la CSP estricta y el JS nuevo)

**Lote:** I · **REQs:** REQ-3, REQ-6
**Depends on:** lotes A-H
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/style_allowlist.txt`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Única diferencia admitida en las 74 capturas: el badge de reposición de Inicio en tema oscuro (REQ-6). Si el `check` da FALLO: PARAR y reportar la salida completa; no tocar CSS ni plantillas para «hacerlo cuadrar».

#### Objective

Demostrar REQ-3: mismos estilos calculados que la base, salvo REQ-6.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
# Si la baseline de T3 ya no está (reinicio, /tmp limpiado), regenerarla igual que en T2, desde b0f5dd8.
if [ ! -d /tmp/gi-fase3-style-baseline ] || [ "$(ls /tmp/gi-fase3-style-baseline | wc -l)" -ne 74 ]; then
  REPO="$PWD"; WT="$(mktemp -d)/base"
  git worktree add --detach "$WT" b0f5dd8
  ln -s "$REPO/.venv" "$WT/.venv"
  cp tests/snapshots/style_snapshot.py "$WT/tests/snapshots/style_snapshot.py"
  rm -rf /tmp/gi-fase3-style-baseline
  (cd "$WT" && .venv/bin/python tests/snapshots/style_snapshot.py write /tmp/gi-fase3-style-baseline)
  git worktree remove --force "$WT"
fi
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt
````

#### Verification

```bash
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gi-fase3-style-baseline --allow tests/snapshots/style_allowlist.txt | tail -1   # OK: estilos calculados idénticos (6 diferencias intencionadas admitidas)
git status --short tests/snapshots/style_allowlist.txt   # vacío (no se modifica)
```

#### Expected Report

- [ ] `tests/snapshots/style_allowlist.txt` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T40: CHANGELOG de la fase 3 (sin bump) y verificación completa

**Lote:** I · **REQs:** REQ-9, REQ-10
**Depends on:** T39
**Skill:** ninguna
**Agent:** coder
**Files:** `CHANGELOG.md`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Release 0.5.0 agrupado: `VERSION` y `.bumpversion.cfg` no se tocan.

#### Objective

Documentar la fase y pasar las tres redes.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:60]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def swap_block(path, old, new):
    """Como edit(), para bloques de varias líneas (<style> → <link>)."""
    edit(path, old, new)

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
print("OK")
PY
````

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1   # OK: 40 snapshots equivalentes
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q   # todo PASS
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -q -rfE   # todo PASS
grep -rlE '\sstyle\s*=|<style' app/templates | wc -l   # 0
git status --short VERSION .bumpversion.cfg   # vacío
```

#### Expected Report

- [ ] `CHANGELOG.md` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado
