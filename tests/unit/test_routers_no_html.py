from __future__ import annotations

import ast
import re
from pathlib import Path

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"
FEATURES_DIR = Path(__file__).resolve().parents[2] / "app" / "templates" / "features"

HTML_MARKERS = ("hx-swap-oob", "<option", "<input", "<select", "<label")


def _string_literals(source: str) -> list[str]:
    # Solo literales de cadena (incluidas las partes fijas de f-strings): un comentario que nombra
    # "hx-swap-oob" no es HTML construido en Python. Los docstrings sí son literales; se excluyen.
    tree = ast.parse(source)
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings
    ]


def test_no_html_markers_in_routers() -> None:
    offenders = []
    for path in sorted(ROUTERS_DIR.glob("*.py")):
        literals = _string_literals(path.read_text(encoding="utf-8"))
        for marker in HTML_MARKERS:
            if any(marker in lit for lit in literals):
                offenders.append(f"{path.name}: {marker}")
    assert offenders == []


def test_guard_detects_html_in_string_literal() -> None:
    assert any("<option" in s for s in _string_literals("x = f'<option value=\"{v}\"></option>'"))
    assert not _string_literals("# <option> en un comentario\ny = 1\n")


def test_features_templates_never_use_safe_or_markup() -> None:
    unsafe = re.compile(r"\|\s*safe\b|Markup\s*\(")
    offenders = [
        str(path.relative_to(FEATURES_DIR))
        for path in sorted(FEATURES_DIR.rglob("*.html"))
        if unsafe.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
