"""Guarda de estructura de plantillas (fase 5a, REQ-4).

``test_literal_template_names_resolve`` carga cada nombre literal de plantilla (rutas de los routers
y ``{% include|extends|from|import %}`` de las plantillas) y falla si alguno no resuelven. Los tests
``test_partials_dir_is_gone`` y ``test_no_partials_references`` se añaden en el Lote E.
"""

from __future__ import annotations

import re
from pathlib import Path

from jinja2.exceptions import TemplateNotFound

from app.routers.ui_common import templates

ROOT = Path(__file__).resolve().parents[2]

# Lote 0: `partials` aún admitido (el Lote E lo quita de esta regex).
_TEMPLATE_NAME = re.compile(
    r"""["']((?:features|layouts|components|partials)/[\w/]+\.html|[\w]+\.html)["']"""
)

_INCLUDE_TAG = re.compile(r"""\{%\s*(include|extends|from|import)\s+['"]([\w/]+\.html)['"]""")


def _template_names() -> set[str]:
    names: set[str] = set()
    for path in sorted((ROOT / "app" / "routers").glob("*.py")):
        names.update(_TEMPLATE_NAME.findall(path.read_text(encoding="utf-8")))
    for path in sorted((ROOT / "app" / "templates").rglob("*.html")):
        for m in _INCLUDE_TAG.finditer(path.read_text(encoding="utf-8")):
            names.add(m.group(2))
    return names


def test_literal_template_names_resolve() -> None:
    broken = []
    for name in sorted(_template_names()):
        try:
            templates.env.get_template(name)
        except TemplateNotFound:
            broken.append(name)
    assert broken == [], "plantillas que no resuelven: " + ", ".join(broken)
