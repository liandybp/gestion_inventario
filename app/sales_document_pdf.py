from __future__ import annotations

from datetime import datetime
from typing import Iterable


def _safe_pdf_text(value: str) -> str:
    try:
        return (value or "").encode("latin-1", "replace").decode("latin-1")
    except Exception:
        return ""


def build_sales_document_pdf(
    *,
    doc_label: str,
    code: str,
    issue_date: datetime,
    currency_symbol: str,
    issuer_name: str,
    issuer_tax_id: str | None,
    issuer_address: str | None,
    client_name: str,
    client_id: str,
    client_address: str | None,
    items: Iterable[dict],
    subtotal: float,
    total: float,
    notes: str | None,
) -> bytes:
    try:
        from fpdf import FPDF  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "No se pudo generar el PDF porque falta la dependencia 'fpdf2'. "
            "Instala dependencias (pip install -r requirements.txt)."
        ) from e

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 7, _safe_pdf_text(issuer_name), ln=1)

    pdf.set_font("Helvetica", "", 10)
    if issuer_tax_id:
        pdf.cell(0, 5, _safe_pdf_text(f"ID fiscal: {issuer_tax_id}"), ln=1)
    if issuer_address:
        pdf.multi_cell(0, 5, _safe_pdf_text(issuer_address))

    pdf.ln(2)

    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 8, _safe_pdf_text(doc_label.upper()), ln=1, align="R")

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, _safe_pdf_text(f"Código: {code}"), ln=1, align="R")
    pdf.cell(0, 5, _safe_pdf_text(f"Fecha: {issue_date.strftime('%Y-%m-%d')}"), ln=1, align="R")

    pdf.ln(4)

    pdf.set_font("Helvetica", "B", 11)
    pdf.cell(0, 6, _safe_pdf_text("Cliente"), ln=1)

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 5, _safe_pdf_text(f"Nombre: {client_name}"), ln=1)
    pdf.cell(0, 5, _safe_pdf_text(f"Identificación: {client_id}"), ln=1)
    if client_address:
        pdf.multi_cell(0, 5, _safe_pdf_text(f"Dirección: {client_address}"))

    pdf.ln(4)

    # Table header
    col_desc = 95
    col_qty = 20
    col_unit = 30
    col_total = 30

    pdf.set_font("Helvetica", "B", 10)
    pdf.set_fill_color(11, 45, 66)
    pdf.set_text_color(255, 255, 255)
    pdf.cell(col_desc, 7, _safe_pdf_text("Descripción"), border=1, fill=True)
    pdf.cell(col_qty, 7, _safe_pdf_text("Cant."), border=1, fill=True, align="R")
    pdf.cell(col_unit, 7, _safe_pdf_text("Precio"), border=1, fill=True, align="R")
    pdf.cell(col_total, 7, _safe_pdf_text("Total"), border=1, fill=True, align="R")
    pdf.ln(7)

    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "", 10)

    for it in items:
        desc = str(it.get("description") or "")
        qty = float(it.get("quantity") or 0)
        unit_price = float(it.get("unit_price") or 0)
        line_total = float(it.get("line_total") or 0)

        x = pdf.get_x()
        y = pdf.get_y()

        # Description as multi_cell
        pdf.multi_cell(col_desc, 6, _safe_pdf_text(desc), border=1)
        h = pdf.get_y() - y

        pdf.set_xy(x + col_desc, y)
        pdf.cell(col_qty, h, _safe_pdf_text(f"{qty:.2f}"), border=1, align="R")
        pdf.cell(col_unit, h, _safe_pdf_text(f"{currency_symbol}{unit_price:.2f}"), border=1, align="R")
        pdf.cell(col_total, h, _safe_pdf_text(f"{currency_symbol}{line_total:.2f}"), border=1, align="R")
        pdf.ln(h)

    pdf.ln(2)

    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, _safe_pdf_text(f"Subtotal: {currency_symbol}{subtotal:.2f}"), ln=1, align="R")
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, _safe_pdf_text(f"Total: {currency_symbol}{total:.2f}"), ln=1, align="R")

    pdf.set_font("Helvetica", "", 9)
    pdf.cell(0, 5, _safe_pdf_text("(Precios sin IVA)"), ln=1, align="R")

    if notes:
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, _safe_pdf_text("Notas"), ln=1)
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, _safe_pdf_text(notes))

    return bytes(pdf.output(dest="S"))
