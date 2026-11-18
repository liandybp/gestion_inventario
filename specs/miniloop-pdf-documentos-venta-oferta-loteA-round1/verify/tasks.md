Lotes: **A** T1-T1

### T1: Conservar los datos largos dentro de las páginas del PDF OFERTA
**Apply:** script
**Files:** `app/sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
FOOTER_H = 43.0
@@ after
FOOTER_GAP = 8.0
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    pdf.set_xy(MARGIN + CONTENT_W / 2, y)
    pdf.cell(CONTENT_W / 2, 6.0, _safe_pdf_text(doc_label.upper()), align="R")
    _double_rule(pdf, y + 6.0)
    return y + 8.0
@@ after
    pdf.set_xy(MARGIN + CONTENT_W / 2, y)
    pdf.multi_cell(
        CONTENT_W / 2, 6.0, _safe_pdf_text(doc_label.upper()),
        align="R", new_x="LEFT", new_y="NEXT",
    )
    title_bottom = pdf.get_y()
    _double_rule(pdf, title_bottom)
    return title_bottom + 2.0
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
        pdf.set_font("Helvetica", "", 8)
        height = _count_lines(pdf, description, 78.0, 5.0) * 5.0
        if y + height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)
            pdf.set_font("Helvetica", "", 8)
        pdf.set_xy(MARGIN, y)
        pdf.multi_cell(78.0, 5.0, description, align="L", new_x="LEFT", new_y="NEXT")
        cells = (
            (unit, 20.0, "C"),
            (format_quantity(item.get("quantity")), 20.0, "C"),
            (format_amount(item.get("unit_price")), 30.0, "R"),
            (format_amount(item.get("line_total")), 32.0, "R"),
        )
        x = MARGIN + 78.0
        for text, width, align in cells:
            pdf.set_xy(x, y)
            pdf.cell(width, 5.0, text, align=align)
            x += width
        y += height
@@ after
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
        line_count = 1
        for text, width, _, size in cells:
            pdf.set_font("Helvetica", "", size)
            line_count = max(line_count, _count_lines(pdf, text, width, 5.0))
        height = line_count * 5.0
        if y + height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)

        x = MARGIN
        for text, width, align, size in cells:
            pdf.set_font("Helvetica", "", size)
            pdf.set_xy(x, y)
            pdf.multi_cell(width, 5.0, text, align=align, new_x="LEFT", new_y="NEXT")
            x += width
        y += height
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    height = 5.0 + _count_lines(pdf, text, CONTENT_W, 4.0) * 4.0
    if y + height > PAGE_BOTTOM:
        pdf.add_page()
        y = MARGIN
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_xy(MARGIN, y)
    pdf.cell(CONTENT_W, 5.0, "Notas:")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(MARGIN, y + 5.0)
    pdf.multi_cell(CONTENT_W, 4.0, text, align="L", new_x="LEFT", new_y="NEXT")
    return y + height
@@ after
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
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    left = ["Efectivo___", "Cheque___", "Transferencia Bancaria___", *bank_lines]
    for index, text in enumerate(left, start=1):
        fitted = _fit_text(pdf, _safe_pdf_text(text), 85.0, style="", start=8.0, minimum=6.0)
        pdf.set_xy(MARGIN, y + 5.0 * index)
        pdf.cell(85.0, 5.0, fitted)

    pdf.set_font("Helvetica", "", 8)
@@ after
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(MARGIN, y + 5.0)
    for text in ("Efectivo___", "Cheque___", "Transferencia Bancaria___", *bank_lines):
        pdf.multi_cell(85.0, 5.0, text, align="L", new_x="LEFT", new_y="NEXT")

    pdf.set_font("Helvetica", "", 8)
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    y += 8.0
    if y + (FOOTER_H - 8.0) > PAGE_BOTTOM:
        pdf.add_page()
        y = MARGIN + 4.0
    _draw_footer(pdf, y, _bank_lines(bank_name, bank_branch, bank_account, bank_account_currency))
@@ after
    bank_lines = [
        _safe_pdf_text(line)
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
````

#### Verification

```bash
.venv/bin/python - <<'PY'
from datetime import datetime, timezone
from io import BytesIO

import pdfplumber

from app.sales_document_pdf import build_sales_document_pdf

account = "1234567890" * 6 + "1234"
notes = [f"Línea {index:03}" for index in range(100)]
pdf_bytes = build_sales_document_pdf(
    doc_type="P", doc_label="W" * 64, code="P123",
    issue_date=datetime.now(timezone.utc), valid_until=None, currency_code="GEN",
    issuer_name="Emisor", issuer_holder=None, issuer_tax_id=None,
    issuer_address=None, issuer_phone=None, issuer_email=None,
    bank_name="B" * 128, bank_branch="S" * 64,
    bank_account=account, bank_account_currency="CUP",
    client_name="Cliente", client_id="ID", client_address=None,
    items=[dict(description="Artículo", unit_of_measure="U" * 32,
                quantity=1234567890.1234, unit_price=1, line_total=1)],
    subtotal=1, total=1, notes="\n".join(notes),
)
with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
    pages = [page.crop((0, 0, page.width, page.height)).extract_text() for page in pdf.pages]
    text = "\n".join(pages)
    assert "DESTINATARIO:" in pages[0]
    first_page = pdf.pages[0]
    title_right = first_page.within_bbox((first_page.width / 2, 0, first_page.width, 170))
    assert (title_right.extract_text() or "").count("W") >= 64
    assert "1,234,567,890.1234" in pages[0]
    units = first_page.within_bbox((93 * 72 / 25.4, 0, 113 * 72 / 25.4, first_page.height))
    assert (units.extract_text() or "").count("U") >= 32
    note_lines = [line for line in text.splitlines() if line.startswith("Línea ")]
    assert note_lines == notes
    footer_page, = [page for page, page_text in zip(pdf.pages, pages) if "FORMA DE PAGO" in page_text]
    payment_column = footer_page.within_bbox((0, 0, 100 * 72 / 25.4, footer_page.height))
    payment_text = payment_column.extract_text().split("FORMA DE PAGO", 1)[1]
    compact = "".join(payment_text.split())
    assert "B" * 128 in compact and "S" * 64 in compact
    assert account in compact
PY
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest tests/e2e/test_sales_doc_pdf_oferta_e2e.py -m e2e -q --override-ini="addopts="
```
