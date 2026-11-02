"""Calcula, a partir de los ficheros REALES de BASE_COMMIT, los scripts de edición de plantillas y el contenido de las
hojas nuevas de la fase 3. No escribe nada en el repo: solo genera texto para los envelopes."""

from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from pathlib import Path

GEN = Path(__file__).parent
sys.path.insert(0, str(GEN / "files"))
from tests.snapshots.style_classes import STYLE_TO_CLASSES, style_key  # noqa: E402

BASE_COMMIT = "b0f5dd8"
REPO = GEN.parents[2]


def base_text(rel: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), "show", f"{BASE_COMMIT}:{rel}"],
                          check=True, capture_output=True, text=True).stdout


def _tag_bounds(s: str, pos: int) -> tuple[int, int]:
    start = s.rfind("<", 0, pos)
    while not re.match(r"<[a-zA-Z]", s[start:start + 2]):
        start = s.rfind("<", 0, start)
    i, quote, depth = start, None, 0
    while i < len(s):
        two = s[i:i + 2]
        if quote:
            if s[i] == quote:
                quote = None
        elif two in ("{{", "{%"):
            depth += 1
            i += 2
            continue
        elif two in ("}}", "%}") and depth:
            depth -= 1
            i += 2
            continue
        elif depth == 0 and s[i] in "\"'":
            quote = s[i]
        elif depth == 0 and s[i] == ">":
            return start, i
        i += 1
    raise ValueError("tag sin cerrar")


def _min_window(text: str, new_text: str) -> tuple[int, int, int]:
    """Ventana mínima de líneas completas que contiene el cambio y es única en text.
    Devuelve (a, b_old, b_new): text[a:b_old] → new_text[a:b_new]."""
    i = 0
    while text[i] == new_text[i]:
        i += 1
    j = 0
    while text[len(text) - 1 - j] == new_text[len(new_text) - 1 - j]:
        j += 1
    a = text.rfind("\n", 0, i) + 1
    tail = len(text) - j
    b = text.find("\n", tail)
    b = len(text) if b == -1 else b
    grow_forward = True
    while text.count(text[a:b]) != 1:
        if grow_forward and b < len(text):
            nb = text.find("\n", b + 1)
            b = len(text) if nb == -1 else nb
        elif a > 0:
            a = text.rfind("\n", 0, a - 1) + 1
        grow_forward = not grow_forward
    return a, b, b + (len(new_text) - len(text))


def style_edits(text: str) -> tuple[list[tuple[str, str]], str]:
    """Pares (antes, después) que convierten cada style="…" (sin Jinja) en clases, en orden de aplicación."""
    edits = []
    while True:
        m = re.search(r' style="([^"{]*)"', text)
        if not m:
            return edits, text
        classes = STYLE_TO_CLASSES[style_key(m.group(1))].split()
        start, end = _tag_bounds(text, m.start())
        tag = text[start:end]
        rel = m.start() - start
        tag_wo = tag[:rel] + tag[rel + len(m.group(0)):]
        cm = re.search(r' class="([^"]*)"', tag_wo)
        if cm:
            existing = cm.group(1).split()
            add = [c for c in classes if c not in existing]
            tag_new = tag_wo[:cm.start()] + f' class="{cm.group(1)}{(" " + " ".join(add)) if add else ""}"' + tag_wo[cm.end():]
        else:
            tag_new = tag[:rel] + f' class="{" ".join(classes)}"' + tag[rel + len(m.group(0)):]
        new_text = text[:start] + tag_new + text[end:]
        a, b_old, b_new = _min_window(text, new_text)
        edits.append((text[a:b_old], new_text[a:b_new]))
        text = new_text


STYLE_BLOCK = re.compile(r"\n    <style>\n(.*?)\n    </style>", re.S)


def style_block(rel: str) -> tuple[str, str]:
    """(bloque exacto a sustituir, CSS desindentado) del único <style> de la plantilla."""
    text = base_text(rel)
    m = STYLE_BLOCK.search(text)
    assert m and text.count("<style>") == 1, rel
    return m.group(0), textwrap.dedent(m.group(1)).strip("\n") + "\n"


def link_tag(name: str) -> str:
    return f'\n    <link rel="stylesheet" href="/static/css/{name}.css?v={{{{ static_version() }}}}" />'


def css_files() -> dict[str, str]:
    _, login = style_block("app/templates/login.html")
    _, tr = style_block("app/templates/transfer_print.html")
    _, rs = style_block("app/templates/restock_print.html")
    _, label = style_block("app/templates/label_single_print.html")
    assert tr == rs, "los <style> de transfer_print y restock_print ya no son idénticos"
    return {
        "app/static/css/login.css": "/* Login (antes <style> en login.html). No carga app.css. */\n" + login + """
/* Clases de la plantilla (antes style="…"). */
.mt-0 { margin-top: 0; }
.mb-12 { margin-bottom: 12px; }
.flex { display: flex; }
.items-center { align-items: center; }
.gap-8 { gap: 8px; }
.w-auto { width: auto; }
""",
        "app/static/css/print.css": ("/* Impresión de traspasos y de artículos a reponer (antes <style> idéntico en las dos "
                                     "plantillas). */\n") + tr + """
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
""",
        "app/static/css/label-print.css": "/* Etiqueta individual (antes <style> en label_single_print.html). */\n" + label,
    }
