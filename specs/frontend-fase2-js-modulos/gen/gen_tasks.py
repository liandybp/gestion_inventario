"""Genera tasks.md del PAE-017 desde envelopes.py y el worktree validado, y simula su aplicación.

Uso:
    python3 gen_tasks.py render <target_dir> <base_repo> > ../tasks.md
    python3 gen_tasks.py apply  <target_dir> <base_repo> <sim_dir>   # aplica los envelopes en sim_dir

<target_dir>: worktree con el resultado validado. <base_repo>: repo para `git show BASE:path`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import envelopes as E  # noqa: E402

FENCE = {".js": "javascript", ".py": "python", ".html": "html+jinja", ".md": "markdown"}


def base_text(base_repo: str, rel: str) -> str:
    return subprocess.run(["git", "-C", base_repo, "show", f"{E.BASE_COMMIT}:{rel}"],
                          check=True, capture_output=True, text=True).stdout


def changelog_script(target: str, base_repo: str) -> str:
    anchor = "  desconocido fallan con un error explícito (global `template_error`), en vez de renderizar un modal roto.\n"
    new = Path(target, "CHANGELOG.md").read_text(encoding="utf-8")
    old = base_text(base_repo, "CHANGELOG.md")
    a = new.index(anchor) + len(anchor)
    b = new.index("\n## 0.4.15", a)
    block = new[a:b]
    assert old.replace(anchor, anchor + block, 1) == new, "CHANGELOG: el bloque no es una inserción simple"
    return E.py(f'edit("CHANGELOG.md", {anchor!r}, {anchor!r} + """{block}""")')


def append_block(target: str, base_repo: str, rel: str) -> str:
    new = Path(target, rel).read_text(encoding="utf-8")
    old = base_text(base_repo, rel)
    assert new.startswith(old), f"{rel}: el destino no empieza por el fichero base"
    return new[len(old):]


def part_text(target: str, rel: str, start: int, end) -> str:
    lines = Path(target, rel).read_text(encoding="utf-8").splitlines(keepends=True)
    return "".join(lines[start - 1:end])


def resolve_ops(env: dict, target: str, base_repo: str) -> list[tuple]:
    out = []
    for op in env["ops"]:
        kind = op[0]
        if kind == "write":
            out.append(("write", op[1], Path(target, op[1]).read_text(encoding="utf-8")))
        elif kind == "append":
            out.append(("append", op[1], append_block(target, base_repo, op[1])))
        elif kind == "part":
            _, rel, start, end = op
            mode = "write" if start == 1 else "append"
            out.append((mode, rel, part_text(target, rel, start, end)))
        elif kind == "changelog":
            out.append(("script", changelog_script(target, base_repo)))
        else:  # script / bash
            out.append(("script", op[1]))
    return out


def render(target: str, base_repo: str) -> str:
    lots = {}
    for env in E.ENVELOPES:
        lots.setdefault(env["lot"], []).append(env["id"])
    lots_line = " · ".join(f"**{k}** T{v[0]}–T{v[-1]}" for k, v in lots.items())
    out = [HEADER.format(lots_line=lots_line, lots_detail="\n".join(
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
        step = 1
        for op in resolve_ops(env, target, base_repo):
            if op[0] == "write":
                lang = FENCE.get(Path(op[1]).suffix, "")
                out.append(f"\n**Step {step}: escribir `{op[1]}` con EXACTAMENTE este contenido** (crear o sobrescribir; "
                           f"crear el directorio si no existe):\n\n````{lang}\n{op[2]}````\n")
            elif op[0] == "append":
                lang = FENCE.get(Path(op[1]).suffix, "")
                out.append(f"\n**Step {step}: añadir AL FINAL de `{op[1]}` EXACTAMENTE este bloque** (sin tocar lo anterior; "
                           f"el fichero ya termina en salto de línea):\n\n````{lang}\n{op[2]}````\n")
            else:
                out.append(f"\n**Step {step}: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan "
                           f"si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):\n\n````bash\n{op[1]}````\n")
            step += 1
        verify = "\n".join(env["verify"])
        out.append(f"""
#### Verification

```bash
{verify}
```

#### Expected Report

- [ ] {files} — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado
""")
    return "".join(out)


def apply(target: str, base_repo: str, sim: str) -> None:
    for env in E.ENVELOPES:
        for op in resolve_ops(env, target, base_repo):
            if op[0] in ("write", "append"):
                path = Path(sim, op[1])
                path.parent.mkdir(parents=True, exist_ok=True)
                if op[0] == "write":
                    path.write_text(op[2], encoding="utf-8")
                else:
                    with path.open("a", encoding="utf-8") as fh:
                        fh.write(op[2])
            else:
                subprocess.run(["bash", "-c", op[1]], cwd=sim, check=True)
        print(f"T{env['id']} aplicado", file=sys.stderr)


HEADER = """# Task Envelopes — Frontend modular, fase 2: JS a módulos, sin JS inline, CSP sin 'unsafe-inline'

> SDD Phase 3 · PAE-017 · Release 0.5.0 (ningún commit sube versión) · Fuente: `requirements.md` (REQ-1..12),
> `design.md` (D1..D10, pre-mortem R1..R10)

**Generado por script** (`specs/frontend-fase2-js-modulos/gen/`) desde un worktree donde el resultado completo se
construyó y verificó (423 unit + 70 e2e, arnés 40/40, mutaciones detectadas). Simulación: aplicar estos envelopes a
una copia limpia de `b7a9881` reproduce ese worktree byte a byte (`gen/simulate.sh`).

**Ejecución:** `cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase2-js-modulos/tasks.md
--test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"`
(sin `--proto`: no hay prototipo). Rama `feature/frontend-modular`, sin push, sin bump de versión.

**Herramienta e2e:** Playwright (`tests/e2e/`, como el resto del proyecto; no usa `behave`). El riesgo de esta fase
está en el navegador (CSP, módulos, listeners): el arnés de snapshots no ejecuta JS (decisión D2 de la fase 1).

**Herramientas de cc_local:** cada envelope declara `Tools` y `Skills`. Ningún envelope usa MCP (regla dura) ni
skills: todo va literal (ficheros completos o scripts con `assert` de coincidencia única). Los comandos usan
`.venv/bin/python` desde la raíz del repo. `node --check` solo valida sintaxis (Node ya está instalado).

**Reglas para cc_local en este PAE:**
- Ficheros JS/tests: copiar el contenido literal. Plantillas y Python existentes: ejecutar el script literal; si un
  `assert` falla, PARAR (el fichero no está como se esperaba) — nunca editar a mano para «hacerlo cuadrar».
- `git add` solo de los `Files` del lote (en el repo hay cambios ajenos sin commitear: `CLAUDE.md`, `.claude/`,
  `.idea/`, `.serena/`, `specs/copiloto-ventas-ubicacion-mensual/`).
- Los e2e necesitan `SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test` si no hay `.env`.

**Trazabilidad:** REQ-1→T15,T17–T24,T26 · REQ-2→T25,T26,T28 · REQ-3→T3–T13,T15,T16,T26 · REQ-4→T3–T13,T17–T24,T29,T30 ·
REQ-5→T8,T16,T20,T26 · REQ-6→T1,T2,T17,T18,T20 · REQ-7→T14,T25,T26 · REQ-8→T27,T28 · REQ-9→T27–T30 · REQ-10→T8,T30 ·
REQ-11→T26 · REQ-12→T31

Lotes: {lots_line}

{lots_detail}

**Orden:** A (arnés + baseline, ANTES de tocar nada en `app/`) → B, C, D (módulos nuevos: aún nadie los carga) →
E, F (cableado: plantillas pasan a `data-js-*` y módulos) → G (CSP estricto + guardas) → H, I (e2e) . Cada lote =
1 commit, y al final de cada lote la app funciona: hasta G el CSP sigue permitiendo inline, y ningún elemento
tiene a la vez `on*=` y `data-js-*` (no hay listeners duplicados).
"""


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "render":
        sys.stdout.write(render(sys.argv[2], sys.argv[3]))
    elif cmd == "apply":
        apply(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        sys.exit(__doc__)
