"""REQ-5/REQ-4: los routers UI no usan el ORM directamente ni resuelven el negocio por su cuenta.

Regex por línea (mismo patrón que test_routers_no_html.py): las llamadas ORM del proyecto tienen
siempre la forma `db.x(`. Fuera de `ui_context.py` y `ui_auth.py`, los routers no deben llamar a
`get_active_business_config/code/id`, `require_active_business_id` ni `get_current_user_from_session`.
Se ignoran las líneas de comentario.
"""
from __future__ import annotations

import re
from pathlib import Path

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"

ORM_CALLS = re.compile(r"\bselect\(|\bdb\.(get|scalar|scalars|execute|query|add|delete)\(")
SQLA_IMPORT = re.compile(r"from sqlalchemy import .*\b(select|func)\b")
SECURITY_CALLS = re.compile(
    r"\b(get_active_business_config|get_active_business_code|get_active_business_id|"
    r"require_active_business_id|get_current_user_from_session)\("
)

ALLOWED_SECURITY_FILES = {"ui_context.py", "ui_auth.py"}


def _code_lines(source: str) -> list[tuple[int, str]]:
    # Numera antes de filtrar comentarios: el informe cita la línea real del fichero.
    return [(i, ln) for i, ln in enumerate(source.splitlines(), start=1) if not ln.strip().startswith("#")]


def test_no_orm_in_ui_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS_DIR.glob("ui_*.py")):
        for lineno, line in _code_lines(path.read_text(encoding="utf-8")):
            if ORM_CALLS.search(line):
                offenders.append(f"{path.name}:{lineno}")
            if SQLA_IMPORT.search(line):
                offenders.append(f"{path.name}:{lineno}")
    assert offenders == [], f"ORM directo en routers UI:\n" + "\n".join(offenders)


def test_no_direct_security_resolution_in_ui_routers() -> None:
    offenders: list[str] = []
    for path in sorted(ROUTERS_DIR.glob("ui_*.py")):
        if path.name in ALLOWED_SECURITY_FILES:
            continue
        for lineno, line in _code_lines(path.read_text(encoding="utf-8")):
            if SECURITY_CALLS.search(line):
                offenders.append(f"{path.name}:{lineno}")
    assert offenders == [], f"Resolución directa del negocio en routers UI:\n" + "\n".join(offenders)


def test_guard_detects_orm_and_security() -> None:
    assert ORM_CALLS.search("db.scalar(select(Product))")
    assert SQLA_IMPORT.search("from sqlalchemy import select")
    assert SECURITY_CALLS.search("get_active_business_config(db, request)")
    assert _code_lines("x = 1\n    # db.execute() en un comentario\ny = db.get(A, 1)") == [(1, "x = 1"), (3, "y = db.get(A, 1)")]
