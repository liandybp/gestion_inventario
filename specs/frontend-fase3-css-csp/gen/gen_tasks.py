"""Genera tasks.md del PAE-018 desde envelopes.py y comprueba sus ediciones EN MEMORIA.

Uso (desde specs/frontend-fase3-css-csp/gen/):
    python3 gen_tasks.py render > ../tasks.md
    python3 gen_tasks.py check     # aplica los envelopes a una copia en memoria de BASE_COMMIT y valida

``check`` no escribe en el repo ni arranca la app: solo comprueba que cada «antes» de cada edit() existe
exactamente una vez en el fichero real (en el orden de los envelopes) y unas invariantes de texto del resultado.
Los scripts bash (worktrees, baselines) no se simulan.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

GEN = Path(__file__).parent
sys.path.insert(0, str(GEN))
import envelopes as E  # noqa: E402
import templates_gen as TG  # noqa: E402

FENCE = {".js": "javascript", ".py": "python", ".html": "html+jinja", ".css": "css", ".md": "markdown", ".txt": "text"}


def literal(rel: str) -> str:
    return (GEN / "files" / rel).read_text(encoding="utf-8")


def part_text(rel: str, start: int, end) -> str:
    lines = literal(rel).splitlines(keepends=True)
    return "".join(lines[start - 1:end])


def tokens(text: str) -> int:
    return round(len(text) / 3.4)


# ── render ──────────────────────────────────────────────────────────────────────────────────────────────────

def render() -> str:
    lots: dict[str, list[int]] = {}
    for env in E.ENVELOPES:
        lots.setdefault(env["lot"], []).append(env["id"])
    lots_line = " · ".join(f"**{k}** T{v[0]}–T{v[-1]}" for k, v in lots.items())
    out = [HEADER.format(lots_line=lots_line, n=len(E.ENVELOPES), lots_detail="\n".join(
        f"- **{k}** ({E.LOTS[k]}): " + ", ".join(f"T{i}" for i in v) for k, v in lots.items()))]
    for env in E.ENVELOPES:
        files = ", ".join(f"`{f}`" for f in env["files"])
        out.append(f"""
---

### T{env['id']}: {env['title']}

**Lote:** {env['lot']} · **REQs:** {env['reqs']}
**Depends on:** {env['depends']}
**Skill:** ninguna
**Agent:** coder
**Files:** {files}
**Pillars:** {env['pillars']}
**Tools (cc_local):** {env['tools']}

#### Context

{env['context']}

#### Objective

{env['objective']}

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.
""")
        for step, op in enumerate(env["ops"], 1):
            kind = op[0]
            if kind == "part":
                rel, start, end = op[1], op[2], op[3]
                lang = FENCE.get(Path(rel).suffix, "")
                if start == 1:
                    out.append(f"\n**Step {step}: escribir `{rel}` con EXACTAMENTE este contenido** (fichero nuevo, primera "
                               f"parte; la completa el envelope siguiente):\n\n````{lang}\n{part_text(rel, start, end)}````\n")
                else:
                    out.append(f"\n**Step {step}: añadir AL FINAL de `{rel}` EXACTAMENTE este bloque** (sin tocar lo anterior; "
                               f"el fichero ya termina en salto de línea):\n\n````{lang}\n{part_text(rel, start, end)}````\n")
            elif kind in ("write", "write_text"):
                text = literal(op[1]) if kind == "write" else op[2]
                lang = FENCE.get(Path(op[1]).suffix, "")
                out.append(f"\n**Step {step}: escribir `{op[1]}` con EXACTAMENTE este contenido** (fichero nuevo; crear el "
                           f"directorio si no existe):\n\n````{lang}\n{text}````\n")
            elif kind == "append":
                lang = FENCE.get(Path(op[1]).suffix, "")
                out.append(f"\n**Step {step}: añadir AL FINAL de `{op[1]}` EXACTAMENTE este bloque** (sin tocar lo anterior; "
                           f"el fichero ya termina en salto de línea):\n\n````{lang}\n{op[2]}````\n")
            else:
                out.append(f"\n**Step {step}: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si "
                           f"algo no cuadra: en ese caso PARAR y reportar, no editar a mano):\n\n````bash\n{op[1]}````\n")
        verify = "\n".join(env["verify"])
        out.append(f"""
#### Verification

```bash
{verify}
```

#### Expected Report

- [ ] {files} — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`; ignorar los cambios ajenos listados arriba)
- [ ] Verificación: todos los comandos con el resultado indicado
""")
    return "".join(out)


# ── check (en memoria) ──────────────────────────────────────────────────────────────────────────────────────

class MemFS:
    def __init__(self) -> None:
        self.files: dict[str, str] = {}

    def read(self, rel: str) -> str:
        if rel not in self.files:
            self.files[rel] = TG.base_text(rel)
        return self.files[rel]

    def edit(self, path: str, old: str, new: str) -> None:
        s = self.read(path)
        assert s.count(old) == 1, f"{path}: {s.count(old)} coincidencias de {old[:70]!r}"
        self.files[path] = s.replace(old, new)


def run_python_heredoc(script: str, fs: MemFS) -> bool:
    m = re.match(r"python3 - <<'PY'\n(.*)\nPY\n$", script, re.S)
    if not m:
        return False
    body = m.group(1).replace(E.PY_HELPERS, "")
    exec(body, {"edit": fs.edit, "swap_block": fs.edit, "print": lambda *a, **k: None})
    return True


def check() -> int:
    fs = MemFS()
    sizes = []
    for env in E.ENVELOPES:
        for op in env["ops"]:
            if op[0] == "part":
                fs.files[op[1]] = (fs.files.get(op[1], "") if op[2] > 1 else "") + part_text(op[1], op[2], op[3])
            elif op[0] == "write":
                fs.files[op[1]] = literal(op[1])
            elif op[0] == "write_text":
                fs.files[op[1]] = op[2]
            elif op[0] == "append":
                fs.files[op[1]] = fs.read(op[1]) + op[2]
            elif not run_python_heredoc(op[1], fs):
                pass  # bash (worktrees/baselines): no se simula
        text = "".join(o[1] if o[0] == "script" else part_text(o[1], o[2], o[3]) if o[0] == "part"
                       else (o[2] if len(o) > 2 else literal(o[1])) for o in env["ops"])
        sizes.append((env["id"], env["lot"], tokens(text), len(env["files"])))
        assert len(env["files"]) <= 2, f"T{env['id']}: más de 2 ficheros"

    templates = sorted(TG.REPO.joinpath("app/templates").rglob("*.html"))
    rels = [str(p.relative_to(TG.REPO)) for p in templates]
    bad = {r: n for r in rels if (n := len(re.findall(r"\sstyle\s*=|<style\b", fs.read(r), re.I)))}
    assert not bad, f"quedan style en: {bad}"
    hexes = {r: h for r in rels if (h := re.findall(r"#[0-9a-fA-F]{3,8}\b", fs.read(r)))}
    assert not hexes, f"quedan hex en: {hexes}"

    # Toda clase añadida por la fase existe en la hoja que carga esa página.
    added = {c for v in TG.STYLE_TO_CLASSES.values() for c in v.split()}
    css = {"app": fs.read("app/static/css/app.css"), "login": fs.read("app/static/css/login.css"),
           "print": fs.read("app/static/css/print.css"), "label": fs.read("app/static/css/label-print.css")}
    page_css = {"app/templates/login.html": "login", "app/templates/transfer_print.html": "print",
                "app/templates/restock_print.html": "print", "app/templates/partials/restock_table.html": "app+print",
                "app/templates/label_single_print.html": "label"}
    for r in rels:
        used = {c for attr in re.findall(r'class="([^"]*)"', fs.read(r)) for c in attr.split()} & added
        for sheet in page_css.get(r, "app").split("+"):
            missing = [c for c in used if not re.search(rf"\.{re.escape(c)}\b", css[sheet])]
            assert not missing, f"{r}: clases sin regla en {sheet}: {missing}"

    assert fs.files["tests/snapshots/style_snapshot.py"] == literal("tests/snapshots/style_snapshot.py"), "partes mal cortadas"
    for rel in ("app/static/js/components/home-charts.js", "app/static/js/components/widget-chart.js"):
        assert not re.findall(r"#[0-9a-fA-F]{3,8}\b|rgba?\(", fs.read(rel)), rel
    main = fs.read("app/main.py")
    assert "\"style-src 'self'; \"" in main and "'self' 'unsafe-inline'" not in main
    assert "location.reload" not in fs.read("app/static/js/features/theme.js")

    for id_, lot, tok, nfiles in sizes:
        flag = "  <-- >800" if tok > 800 else ""
        print(f"T{id_:<3} {lot}  ~{tok:5d} tokens  {nfiles} fich.{flag}")
    lot_tok: dict[str, int] = {}
    for _, lot, tok, _ in sizes:
        lot_tok[lot] = lot_tok.get(lot, 0) + tok
    print("lotes:", ", ".join(f"{k}~{v}" for k, v in lot_tok.items()))
    print(f"OK: {len(E.ENVELOPES)} envelopes; todas las ediciones aplican sobre {E.BASE_COMMIT} (en memoria)")
    return 0


HEADER = """# Task Envelopes — Frontend modular, fase 3: CSS sin estilos inline, tokens y paleta, CSP `style-src 'self'`

> SDD Phase 3 · PAE-018 · Release 0.5.0 (ningún commit sube versión) · Fuente: `requirements.md` (REQ-1..10),
> `design.md` (D1..D12, pre-mortem R1..R4)

**Sin prototipo** (ADR-020 + regla del paso 2 del loop, 2026-10-24). Los envelopes se generan por script
(`specs/frontend-fase3-css-csp/gen/`): los «antes» salen de leer los ficheros reales de `b0f5dd8`, y
`gen_tasks.py check` comprueba **en memoria** que cada `edit()` encuentra exactamente una coincidencia en orden. Nada
de esto se ha ejecutado contra la app: la red que lo comprueba es el **lote A**, y cada lote de plantillas cierra con
el `check` de estilos calculados.

**Ejecución:** `cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase3-css-csp/tasks.md
--test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"`
(sin `--proto`). Rama `feature/frontend-modular`, sin push, sin bump de versión. {n} envelopes.

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

Lotes: {lots_line}

{lots_detail}

**Orden:** A (la red: nada de `app/`) → B (CSS: reglas nuevas, sin efecto visible) → C, D, E (plantillas; cada lote cierra
con el `check` de estilos) → F (JS) → G (CSP estricta) → H (e2e) → I (cierre). Cada lote = 1 commit; al final de cada
lote la app funciona: hasta G la CSP sigue permitiendo estilos inline.
"""


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "render":
        sys.stdout.write(render())
    elif cmd == "check":
        sys.exit(check())
    else:
        sys.exit(__doc__)
