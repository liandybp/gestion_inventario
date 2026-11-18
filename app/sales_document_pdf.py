"""PDF de facturas y presupuestos con el diseño de la plantilla "OFERTA" (fpdf2, Helvetica, A4)."""

from __future__ import annotations

import io
from datetime import date, datetime
from typing import Any, Iterable, Optional

from app.sales_documents_format import (
    currency_column_label,
    format_amount,
    format_doc_date,
    format_quantity,
)

MARGIN = 15.0
CONTENT_W = 180.0
RIGHT_X = 107.7
RIGHT_W = 87.3
LABEL_W = 40.0
ROW_H = 4.9
TABLE_HEADER_HEIGHT = 7.5
PAGE_BOTTOM = 282.0
FOOTER_GAP = 8.0
BLUE_LIGHT = (220, 230, 241)
BLUE_DARK = (17, 90, 156)

# Iconos vectoriales (trazo en el azul de la caja TOTAL): teléfono y sobre.
_ICON_PHONE_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path fill="none" stroke="#115A9C" stroke-width="1.6" stroke-linejoin="round" d="M5 3h4l2 5-2.5 1.5a11 11 0 0 0 6 6L16 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 5a2 2 0 0 1 2-2z"/></svg>'
_ICON_MAIL_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><rect x="3" y="5" width="18" height="14" rx="2" fill="none" stroke="#115A9C" stroke-width="1.6"/><path d="M3 7l9 6 9-6" fill="none" stroke="#115A9C" stroke-width="1.6"/></svg>'


def _safe_pdf_text(value: str) -> str:
    try:
        return (value or "").encode("latin-1", "replace").decode("latin-1")
    except Exception:
        return ""


def _fold_field_lines(value: Optional[str]) -> str:
    """Convierte los saltos de línea de un campo en espacios para ajustarlo al ancho de su bloque."""
    return " ".join((value or "").splitlines())

def _fit_text(pdf: Any, text: str, width: float, style: str = "B", start: float = 9.0, minimum: float = 6.0) -> str:
    """Fija la fuente en el mayor tamaño (de `start` a `minimum`, pasos de 0,5 pt) en que `text` cabe en
    `width` mm (c_margin de 1 mm por lado). Si ni a `minimum` cabe, corta el texto y añade "..."."""
    available = width - 2.0
    size = start
    while size >= minimum:
        pdf.set_font("Helvetica", style, size)
        if pdf.get_string_width(text) <= available:
            return text
        size -= 0.5
    pdf.set_font("Helvetica", style, minimum)
    cut = text
    while len(cut) > 1 and pdf.get_string_width(cut + "...") > available:
        cut = cut[:-1]
    return cut + "..."


def _count_lines(pdf: Any, text: str, width: float, line_h: float) -> int:
    """Número de líneas que ocupa `text` en una celda de `width` mm con la fuente actual (sin dibujar)."""
    lines = pdf.multi_cell(width, line_h, text, align="L", dry_run=True, output="LINES")
    return max(1, len(lines))


def _double_rule(pdf: Any, y: float) -> None:
    pdf.set_fill_color(0, 0, 0)
    pdf.rect(MARGIN, y, CONTENT_W, 0.35, style="F")
    pdf.rect(MARGIN, y + 0.7, CONTENT_W, 0.35, style="F")


def _draw_header(
    pdf: Any,
    *,
    issuer_name: str,
    issuer_holder: Optional[str],
    issuer_tax_id: Optional[str],
    issuer_address: Optional[str],
    issuer_phone: Optional[str],
    issuer_email: Optional[str],
) -> float:
    """Banda azul clara: emisor a la izquierda, dirección/teléfono/email a la derecha. Devuelve su borde inferior."""
    top = 12.0
    left_x, left_w = MARGIN + 2.0, 88.0
    right_x, right_w = 107.0, 86.0
    icon_w = 5.0

    left_rows: list[tuple[str, float, float, str]] = [(issuer_name, 12.0, 6.0, "B")]
    if issuer_holder:
        left_rows.append((f"TCP {issuer_holder}", 9.0, 4.5, ""))
    if issuer_tax_id:
        left_rows.append((f"NIT {issuer_tax_id}", 9.0, 4.5, ""))

    right_rows: list[tuple[Optional[bytes], str]] = []
    if issuer_address:
        right_rows.append((None, issuer_address))
    if issuer_phone:
        right_rows.append((_ICON_PHONE_SVG, issuer_phone))
    if issuer_email:
        right_rows.append((_ICON_MAIL_SVG, issuer_email))

    left_h = 0.0
    for text, size, line_h, style in left_rows:
        pdf.set_font("Helvetica", style, size)
        left_h += _count_lines(pdf, _safe_pdf_text(text), left_w, line_h) * line_h
    pdf.set_font("Helvetica", "", 9)
    right_h = 0.0
    for icon, text in right_rows:
        width = right_w - (icon_w if icon else 0.0)
        right_h += _count_lines(pdf, _safe_pdf_text(text), width, 4.5) * 4.5 + 0.5

    band_h = max(26.0, max(left_h, right_h) + 6.0)
    pdf.set_fill_color(*BLUE_LIGHT)
    pdf.rect(MARGIN, top, CONTENT_W, band_h, style="F")

    y = top + 3.0
    for text, size, line_h, style in left_rows:
        pdf.set_font("Helvetica", style, size)
        pdf.set_xy(left_x, y)
        pdf.multi_cell(left_w, line_h, _safe_pdf_text(text), align="L", new_x="LEFT", new_y="NEXT")
        y = pdf.get_y()

    pdf.set_font("Helvetica", "", 9)
    y = top + 3.0
    for icon, text in right_rows:
        width = right_w - (icon_w if icon else 0.0)
        if icon is not None:
            pdf.image(io.BytesIO(icon), x=right_x, y=y + 0.2, w=4.0, h=4.0)
        pdf.set_xy(right_x + (icon_w if icon else 0.0), y)
        pdf.multi_cell(width, 4.5, _safe_pdf_text(text), align="L", new_x="LEFT", new_y="NEXT")
        y = pdf.get_y() + 0.5
    return top + band_h


def _draw_title_bar(pdf: Any, y: float, doc_label: str) -> float:
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(MARGIN, y)
    pdf.cell(CONTENT_W / 2, 6.0, "DESTINATARIO:")
    pdf.set_xy(MARGIN + CONTENT_W / 2, y)
    pdf.multi_cell(
        CONTENT_W / 2, 6.0, _safe_pdf_text(doc_label.upper()),
        align="R", new_x="LEFT", new_y="NEXT",
    )
    title_bottom = pdf.get_y()
    _double_rule(pdf, title_bottom)
    return title_bottom + 2.0


def _draw_client(pdf: Any, y: float, client_name: str, client_id: str, client_address: Optional[str]) -> float:
    width = 88.0
    pdf.set_xy(MARGIN, y)
    pdf.set_font("Helvetica", "B", 9)
    pdf.multi_cell(width, ROW_H, _safe_pdf_text(f"CLIENTE: {client_name}"), align="L", new_x="LEFT", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.multi_cell(width, ROW_H, _safe_pdf_text(f"ID: {client_id}"), align="L", new_x="LEFT", new_y="NEXT")
    if client_address:
        pdf.multi_cell(width, ROW_H, _safe_pdf_text(client_address), align="L", new_x="LEFT", new_y="NEXT")
    return pdf.get_y()


def _draw_label_row(pdf: Any, y: float, label: str, value: str) -> float:
    """Fila azul clara etiqueta/valor del bloque documento. La etiqueta reduce su letra hasta caber."""
    pdf.set_fill_color(*BLUE_LIGHT)
    pdf.rect(RIGHT_X, y, RIGHT_W, ROW_H, style="F")
    text = _fit_text(pdf, _safe_pdf_text(label), LABEL_W)
    pdf.set_xy(RIGHT_X, y)
    pdf.cell(LABEL_W, ROW_H, text)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(RIGHT_X + LABEL_W, y)
    pdf.cell(RIGHT_W - LABEL_W, ROW_H, _safe_pdf_text(value), align="R")
    return y + ROW_H


def _draw_totals(pdf: Any, y: float, subtotal: float, total: float) -> float:
    y = _draw_label_row(pdf, y, "Subtotal:", format_amount(subtotal))
    pdf.set_fill_color(*BLUE_DARK)
    pdf.rect(RIGHT_X, y, RIGHT_W, 9.9, style="F")
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(RIGHT_X, y + 1.5)
    pdf.cell(LABEL_W, 6.9, "TOTAL:")
    pdf.set_xy(RIGHT_X + LABEL_W, y + 1.5)
    pdf.cell(RIGHT_W - LABEL_W, 6.9, format_amount(total), align="R")
    pdf.set_text_color(0, 0, 0)
    return y + 9.9


def _columns(price_label: str, amount_label: str) -> tuple[tuple[str, float, str], ...]:
    return (
        ("CONCEPTO", 78.0, "C"),
        ("UNIDADES", 20.0, "C"),
        ("CANTIDAD", 20.0, "C"),
        (price_label, 30.0, "R"),
        (amount_label, 32.0, "R"),
    )


def _draw_table_header(pdf: Any, y: float, price_label: str, amount_label: str) -> float:
    pdf.set_font("Helvetica", "B", 8)
    x = MARGIN
    for label, width, align in _columns(price_label, amount_label):
        pdf.set_xy(x, y)
        pdf.cell(width, 5.0, _safe_pdf_text(label), align=align)
        x += width
    _double_rule(pdf, y + 5.2)
    return y + TABLE_HEADER_HEIGHT


def _draw_rows(pdf: Any, y: float, items: Iterable[dict], price_label: str, amount_label: str) -> float:
    """Mantiene juntas las filas que caben en una página y repite la cabecera en cada continuación."""
    line_height = 5.0
    page_capacity = int((PAGE_BOTTOM - MARGIN - TABLE_HEADER_HEIGHT) // line_height)
    for item in items:
        description = _safe_pdf_text(str(item.get("description") or ""))
        unit = _safe_pdf_text(str(item.get("unit_of_measure") or ""))
        quantity = format_quantity(item.get("quantity"))
        pdf.set_font("Helvetica", "", 8)
        quantity_size = 8.0
        while pdf.get_string_width(quantity) > 18.0 and quantity_size > 5.0:
            quantity_size -= 0.5
            pdf.set_font("Helvetica", "", quantity_size)

        cells = (
            (description, 78.0, "L", 8.0),
            (unit, 20.0, "C", 8.0),
            (quantity, 20.0, "C", quantity_size),
            (format_amount(item.get("unit_price")), 30.0, "R", 8.0),
            (format_amount(item.get("line_total")), 32.0, "R", 8.0),
        )
        wrapped_cells = []
        for text, width, align, size in cells:
            pdf.set_font("Helvetica", "", size)
            lines = pdf.multi_cell(width, line_height, text, dry_run=True, output="LINES")
            wrapped_cells.append((lines, width, align, size))

        line_count = max(1, max(len(lines) for lines, _, _, _ in wrapped_cells))
        if y + min(line_count, page_capacity) * line_height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)

        for start in range(0, line_count, page_capacity):
            if start:
                pdf.add_page()
                y = _draw_table_header(pdf, MARGIN, price_label, amount_label)
            stop = min(start + page_capacity, line_count)
            x = MARGIN
            for lines, width, align, size in wrapped_cells:
                pdf.set_font("Helvetica", "", size)
                for index, line in enumerate(lines[start:stop]):
                    pdf.set_xy(x, y + index * line_height)
                    pdf.cell(width, line_height, line, align=align)
                x += width
            y += (stop - start) * line_height
    return y


def _draw_notes(pdf: Any, y: float, notes: str) -> float:
    text = _safe_pdf_text(notes)
    pdf.set_font("Helvetica", "", 8)
    height = 5.0 + _count_lines(pdf, text, CONTENT_W, 4.0) * 4.0
    fits_on_fresh_page = height <= PAGE_BOTTOM - MARGIN
    if y + 9.0 > PAGE_BOTTOM or (fits_on_fresh_page and y + height > PAGE_BOTTOM):
        pdf.add_page()
        y = MARGIN
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_xy(MARGIN, y)
    pdf.cell(CONTENT_W, 5.0, "Notas:")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(MARGIN, y + 5.0)
    pdf.set_auto_page_break(True, margin=pdf.h - PAGE_BOTTOM)
    pdf.multi_cell(CONTENT_W, 4.0, text, align="L", new_x="LEFT", new_y="NEXT")
    pdf.set_auto_page_break(False)
    return pdf.get_y()


def _bank_lines(
    bank_name: Optional[str],
    bank_branch: Optional[str],
    bank_account: Optional[str],
    bank_account_currency: Optional[str],
) -> list[str]:
    lines: list[str] = []
    head = ", ".join(part for part in ((bank_name or "").strip(), f"Sucursal {bank_branch.strip()}" if (bank_branch or "").strip() else "") if part)
    if head:
        lines.append(head)
    account = (bank_account or "").strip()
    if account:
        currency = (bank_account_currency or "").strip()
        lines.append(f"No. Cuenta ({currency}): {account}" if currency else f"No. Cuenta: {account}")
    return lines


def _draw_footer(pdf: Any, y: float, bank_lines: list[str]) -> None:
    """Pie a dos columnas: FORMA DE PAGO a la izquierda, Recibido por/Cargo/Firma/Fecha a la derecha."""
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_xy(MARGIN, y)
    pdf.cell(85.0, 5.0, "FORMA DE PAGO")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(MARGIN, y + 5.0)
    for text in ("Efectivo___", "Cheque___", "Transferencia Bancaria___", *bank_lines):
        pdf.multi_cell(85.0, 5.0, text, align="L", new_x="LEFT", new_y="NEXT")

    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(RIGHT_X, y)
    pdf.cell(RIGHT_W, 5.0, "Recibido por:")
    pdf.set_draw_color(0, 0, 0)
    pdf.line(RIGHT_X, y + 9.0, RIGHT_X + 75.0, y + 9.0)
    for index, label in enumerate(("Cargo:", "Firma:", "Fecha:"), start=2):
        pdf.set_xy(RIGHT_X, y + 5.0 * index)
        pdf.cell(RIGHT_W, 5.0, f"{label} " + "_" * 30)


def build_sales_document_pdf(
    *,
    doc_type: str,
    doc_label: str,
    code: str,
    issue_date: datetime,
    valid_until: Optional[date],
    currency_code: Optional[str],
    issuer_name: str,
    issuer_holder: Optional[str],
    issuer_tax_id: Optional[str],
    issuer_address: Optional[str],
    issuer_phone: Optional[str],
    issuer_email: Optional[str],
    bank_name: Optional[str],
    bank_branch: Optional[str],
    bank_account: Optional[str],
    bank_account_currency: Optional[str],
    client_name: str,
    client_id: str,
    client_address: Optional[str],
    items: Iterable[dict],
    subtotal: float,
    total: float,
    notes: Optional[str],
) -> bytes:
    try:
        from fpdf import FPDF  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "No se pudo generar el PDF porque falta la dependencia 'fpdf2'. "
            "Instala dependencias (pip install -r requirements.txt)."
        ) from e

    class SalesDocumentPDF(FPDF):
        def header(self) -> None:
            if self.page_no() == 1:
                return
            self.set_font("Helvetica", "", 8)
            self.set_xy(MARGIN, 5.0)
            self.cell(CONTENT_W, 5.0, _safe_pdf_text(f"Documento: {code}"), align="R")

    pdf = SalesDocumentPDF(format="A4", unit="mm")
    pdf.set_margins(MARGIN, 12.0, MARGIN)
    pdf.set_auto_page_break(False)
    pdf.add_page()
    pdf.set_text_color(0, 0, 0)

    band_bottom = _draw_header(
        pdf,
        issuer_name=_fold_field_lines(issuer_name),
        issuer_holder=_fold_field_lines(issuer_holder),
        issuer_tax_id=_fold_field_lines(issuer_tax_id),
        issuer_address=_fold_field_lines(issuer_address),
        issuer_phone=_fold_field_lines(issuer_phone),
        issuer_email=_fold_field_lines(issuer_email),
    )
    y = _draw_title_bar(pdf, band_bottom + 2.0, doc_label)
    client_end = _draw_client(
        pdf, y, _fold_field_lines(client_name), _fold_field_lines(client_id), _fold_field_lines(client_address),
    )

    doc_end = _draw_label_row(pdf, y, f"No. {doc_label}:", code)
    doc_end = _draw_label_row(pdf, doc_end, f"Fecha de {doc_label.lower()}:", format_doc_date(issue_date))
    if (doc_type or "").strip().upper() == "P":
        doc_end = _draw_label_row(pdf, doc_end, "Fecha de vencimiento:", format_doc_date(valid_until))
    totals_end = _draw_totals(pdf, doc_end + 3.0, subtotal, total)

    price_label = currency_column_label("PRECIO", currency_code)
    amount_label = currency_column_label("IMPORTE", currency_code)
    y = _draw_table_header(pdf, max(client_end, totals_end) + 6.0, price_label, amount_label)
    y = _draw_rows(pdf, y, items, price_label, amount_label)
    _double_rule(pdf, y + 0.5)
    y += 3.0

    if (notes or "").strip():
        y = _draw_notes(pdf, y + 2.0, str(notes))

    bank_lines = [
        _safe_pdf_text(_fold_field_lines(line))
        for line in _bank_lines(bank_name, bank_branch, bank_account, bank_account_currency)
    ]
    pdf.set_font("Helvetica", "", 8)
    bank_height = sum(_count_lines(pdf, line, 85.0, 5.0) * 5.0 for line in bank_lines)
    footer_height = max(25.0, 20.0 + bank_height)
    y += FOOTER_GAP
    if y + footer_height > PAGE_BOTTOM:
        pdf.add_page()
        y = MARGIN + 4.0
    _draw_footer(pdf, y, bank_lines)

    return bytes(pdf.output())
