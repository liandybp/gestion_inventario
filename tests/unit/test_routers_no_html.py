from __future__ import annotations

import re
from pathlib import Path

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "app" / "routers"
FEATURES_DIR = Path(__file__).resolve().parents[2] / "app" / "templates" / "features"

HTML_MARKERS = ("hx-swap-oob", "<option", "<input", "<select", "<label")


def test_no_html_markers_in_routers() -> None:
    offenders = []
    for path in sorted(ROUTERS_DIR.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        for marker in HTML_MARKERS:
            if marker in text:
                offenders.append(f"{path.name}: {marker}")
    assert offenders == []


def test_features_templates_never_use_safe_or_markup() -> None:
    unsafe = re.compile(r"\|\s*safe\b|Markup\s*\(")
    offenders = [
        str(path.relative_to(FEATURES_DIR))
        for path in sorted(FEATURES_DIR.rglob("*.html"))
        if unsafe.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []
