from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import IO, Optional

import pdfplumber


@dataclass(frozen=True)
class InvoiceLine:
    sku: str
    name: str
    quantity: float
    net_unit_price: float


@dataclass(frozen=True)
class ParsedInvoice:
    invoice_number: Optional[str]
    invoice_date: Optional[datetime]
    lines: list[InvoiceLine]


def _parse_spanish_number(s: str) -> float:
    v = (s or "").strip()
    if not v:
        raise ValueError("empty")

    v = v.replace("€", "").replace("\u00a0", " ").strip()
    v = v.replace(" ", "")

    if "," in v:
        v = v.replace(".", "")
        v = v.replace(",", ".")

    return float(v)


def _parse_invoice_number(text: str) -> Optional[str]:
    m = re.search(r"N[úu]mero de factura:\s*([0-9]+)", text)
    if not m:
        return None
    return m.group(1).strip()


def _parse_invoice_date(text: str) -> Optional[datetime]:
    m = re.search(r"Fecha de factura:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1), "%d.%m.%Y")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


def parse_autodoc_pdf(file_obj: IO[bytes]) -> ParsedInvoice:
    try:
        file_obj.seek(0)
    except Exception:
        pass

    raw_lines: list[str] = []
    with pdfplumber.open(file_obj) as pdf:
        for page in pdf.pages:
            txt = page.extract_text() or ""
            for ln in txt.splitlines():
                ln = (ln or "").strip()
                if ln:
                    raw_lines.append(ln)

    full_text = "\n".join(raw_lines)
    invoice_number = _parse_invoice_number(full_text)
    invoice_date = _parse_invoice_date(full_text)

    lines: list[InvoiceLine] = []

    for ln in raw_lines:
        if not re.match(r"^\d+\.\s+", ln):
            continue

        parts = ln.split()
        if len(parts) < 4:
            continue

        pos = parts[0]
        if not pos.endswith("."):
            continue

        sku = parts[1].strip()
        if not sku or sku == "-":
            continue

        euro_matches = list(re.finditer(r"(\d[\d\.]*,\d+)\s*€", ln))
        if not euro_matches:
            continue

        first_euro = euro_matches[0]
        price_str = first_euro.group(1)

        before_price = ln[: first_euro.start()].strip()
        qty_match = re.search(r"(\d+(?:[\.,]\d+)?)\s*$", before_price)
        if not qty_match:
            continue

        try:
            quantity = _parse_spanish_number(qty_match.group(1))
            net_unit_price = _parse_spanish_number(price_str)
        except ValueError:
            continue

        if quantity <= 0 or net_unit_price <= 0:
            continue

        name_span = ln

        code_match = re.search(r"\s(\d{8})\s", ln)
        if code_match:
            name_span = ln[: code_match.start()].strip()

        name_span = re.sub(r"^\d+\.\s+", "", name_span).strip()
        if name_span.startswith(sku):
            name_span = name_span[len(sku) :].strip()

        if not name_span:
            continue

        if name_span.lstrip("-").strip().lower().startswith("bono"):
            continue

        lines.append(
            InvoiceLine(
                sku=sku,
                name=name_span,
                quantity=float(quantity),
                net_unit_price=float(net_unit_price),
            )
        )

    try:
        file_obj.seek(0)
    except Exception:
        pass

    return ParsedInvoice(invoice_number=invoice_number, invoice_date=invoice_date, lines=lines)
