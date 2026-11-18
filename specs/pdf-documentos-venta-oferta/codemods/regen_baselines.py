"""Regenera las baselines del arnés de snapshots HTML (lección de 0.5.1: el CI cayó por no regenerarlas).

Lo ejecuta el intérprete `pae-edit` (op `codemod`, runner `python`): `python3 regen_baselines.py <raíz> <ficheros...>`.
Corre `tests/snapshots/ui_snapshot.py write tests/snapshots/baseline` con el Python del proyecto (el del
intérprete no tiene FastAPI). Es idempotente: una segunda pasada produce los mismos ficheros.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def _project_python(root: Path) -> str:
    candidates = [
        root / ".venv" / "bin" / "python",
        Path.home() / "PyCharmMiscProject" / "gestion_inventario" / ".venv" / "bin" / "python",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    return sys.executable


def main(argv: list[str]) -> int:
    root = Path(argv[1]).resolve()
    result = subprocess.run(
        [_project_python(root), "tests/snapshots/ui_snapshot.py", "write", "tests/snapshots/baseline"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=550,
    )
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv))
