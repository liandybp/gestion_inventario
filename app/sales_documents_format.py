"""Funciones puras de formato y de fecha de vencimiento de los documentos de venta.

Las usan el PDF (`app/sales_document_pdf.py`) y el router de documentos (`app/routers/ui_sales_documents.py`).
No importan fpdf, FastAPI ni SQLAlchemy, para poder probarlas sin PDF ni base de datos.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Literal, Optional

INVALID_VALID_UNTIL = "Fecha de vencimiento inválida"

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def currency_column_label(prefix: str, currency_code: Optional[str]) -> str:
    """Etiqueta de una columna de importe: `GEN` se muestra como `CUP`; sin moneda, solo el prefijo."""
    code = (currency_code or "").strip().upper()
    if code == "GEN":
        code = "CUP"
    return f"{prefix} {code}" if code else prefix


def format_amount(value: Optional[float]) -> str:
    """Importe con separador de miles y dos decimales: 303875 -> `303,875.00`."""
    return f"{float(value or 0):,.2f}"


def format_quantity(value: Optional[float]) -> str:
    """Cantidad con hasta 4 decimales sin ceros finales y con separador de miles."""
    text = f"{float(value or 0):,.4f}".rstrip("0").rstrip(".")
    return text or "0"


def format_doc_date(value: Optional[date | datetime]) -> str:
    """Fecha `dd/mm/aaaa`; vacío si no hay fecha."""
    if value is None:
        return ""
    return value.strftime("%d/%m/%Y")


def resolve_valid_until(
    raw: Optional[str],
    *,
    doc_type: str,
    issue_date: datetime,
    validity_days: int,
    on_empty: Literal["default", "keep"] = "default",
    current: Optional[date] = None,
) -> Optional[date]:
    """Fecha de vencimiento de un documento.

    - `doc_type` distinto de `P`: devuelve `None` y no valida `raw`.
    - `raw` vacío con `on_empty="default"`: fecha de emisión + `validity_days`.
    - `raw` vacío con `on_empty="keep"`: devuelve `current` (puede ser `None`).
    - Otro valor: debe ser `aaaa-mm-dd` y no anterior a la fecha de emisión; si no, lanza `ValueError`.
    """
    if (doc_type or "").strip().upper() != "P":
        return None
    text = (raw or "").strip()
    issued_on = issue_date.date()
    if not text:
        if on_empty == "keep":
            return current
        return issued_on + timedelta(days=int(validity_days))
    if not _ISO_DATE.match(text):
        raise ValueError(INVALID_VALID_UNTIL)
    try:
        chosen = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(INVALID_VALID_UNTIL) from exc
    if chosen < issued_on:
        raise ValueError(INVALID_VALID_UNTIL)
    return chosen
