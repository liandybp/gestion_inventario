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
