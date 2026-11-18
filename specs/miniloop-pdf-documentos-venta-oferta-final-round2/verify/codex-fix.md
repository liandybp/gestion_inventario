Encontré dos defectos en el PDF de facturas y presupuestos. Reproduje ambos en `9c33250` con `pdfplumber`, en memoria. Las 46 pruebas existentes de PDF y formato pasaron. No modifiqué ningún fichero.

1. **P2 · Una primera fila alta deja la cabecera de la tabla sola en la primera página.** Una descripción de 49 líneas y 195 caracteres cabe entera en una página nueva. El generador dibuja la cabecera antes de medir la fila y vuelve a dibujarla al mover la fila. La corrección decide primero el salto y dibuja la cabecera donde empieza la tabla. [app/sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/sales_document_pdf.py:384)
2. **P2 · Un código de moneda ancho mezcla los encabezados de las columnas.** Con `WWWWWWWW`, un código que admite el campo de ocho caracteres, el PDF junta `CANTIDAD` con `PRECIO` y superpone parte del código con `IMPORTE`. La corrección ajusta el tamaño de la letra al ancho de cada columna. [app/sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/sales_document_pdf.py:199)

**Ficheros revisados**

- Raíz: `.bumpversion.cfg`, `CHANGELOG.md`, `CLAUDE.md`, `VERSION`.
- `app/`: `business_config.py`, `migrations.py`, `models.py`, `repositories/business_repository.py`, `routers/ui_businesses.py`, `routers/ui_common.py`, `routers/ui_sales_documents.py`, `sales_document_pdf.py`, `sales_documents_format.py`.
- `app/templates/`: `components/sales_doc/issue_form.html`, `features/businesses/business_edit_form.html`, `features/businesses/tab_businesses.html`, `features/sales_documents/_valid_until_error.html`, `features/sales_documents/sales_document_edit_form.html`, `features/sales_documents/sales_document_preview.html`.
- `tests/`: `e2e/test_sales_doc_pdf_oferta_e2e.py`, `unit/test_business_bank_fields.py`, `unit/test_sales_doc_valid_until.py`, `unit/test_sales_document_pdf.py`, `unit/test_sales_documents_format.py`.
- `tests/snapshots/baseline/`: `empty_tab_businesses.txt`, `empty_tab_documents.txt`, `modal_business.txt`, `operator_tab_documents.txt`, `owner_tab_documents.txt`, `tab_businesses.txt`, `tab_documents.txt`.
- `specs/pdf-documentos-venta-oferta/`: `design.md`, `design/sondas/S11-estres-pdf.txt`, `tasks.md`. De `verify/` revisé los listados de cambios y los resultados de los lotes A–D y C-miniloop. Los tres PNG quedaron fuera de la revisión visual.

```tasks-md
Lotes: **A** T1-T2

### T1: Dibujar la cabecera de la tabla en la página donde empieza la primera fila
**Apply:** script
**Files:** `app/sales_document_pdf.py`, `tests/unit/test_sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: insert_after
@@ anchor
    page_capacity = int((PAGE_BOTTOM - MARGIN - TABLE_HEADER_HEIGHT) // line_height)
@@ content
    header_pending = True
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
        if y + min(line_count, page_capacity) * line_height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)
@@ after
        header_height = TABLE_HEADER_HEIGHT if header_pending else 0.0
        if y + header_height + min(line_count, page_capacity) * line_height > PAGE_BOTTOM:
            pdf.add_page()
            y = MARGIN
            header_pending = True
        if header_pending:
            y = _draw_table_header(pdf, y, price_label, amount_label)
            header_pending = False
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
            y += (stop - start) * line_height
    return y
@@ after
            y += (stop - start) * line_height
    if header_pending:
        return _draw_table_header(pdf, y, price_label, amount_label)
    return y
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    y = _draw_table_header(pdf, max(client_end, totals_end) + 6.0, price_label, amount_label)
    y = _draw_rows(pdf, y, items, price_label, amount_label)
@@ after
    y = _draw_rows(pdf, max(client_end, totals_end) + 6.0, items, price_label, amount_label)
````

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: append
@@ content


def test_table_header_moves_with_a_tall_first_row() -> None:
    lines = [f"L{i:02d}" for i in range(49)]
    items = [{"description": "\n".join(lines), "quantity": 1, "unit_price": 2, "line_total": 2}]
    pdf_bytes = build_sales_document_pdf(**_kwargs(items=items, subtotal=2, total=2))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        first_page, table_page, *_ = [page.extract_text() for page in pdf.pages]

    assert "DESTINATARIO:" in first_page
    assert "CONCEPTO" not in first_page
    assert "CONCEPTO" in table_page
    assert re.findall(r"\bL\d{2}\b", table_page) == lines
````

#### Verification

```bash
.venv/bin/python -B -m pytest -o addopts= -s -p no:cacheprovider --noconftest -q tests/unit/test_sales_document_pdf.py
```

### T2: Ajustar los encabezados de precio e importe al ancho de su columna
**Apply:** script
**Files:** `app/sales_document_pdf.py`, `tests/unit/test_sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    pdf.set_font("Helvetica", "B", 8)
    x = MARGIN
    for label, width, align in _columns(price_label, amount_label):
        pdf.set_xy(x, y)
        pdf.cell(width, 5.0, _safe_pdf_text(label), align=align)
@@ after
    x = MARGIN
    for label, width, align in _columns(price_label, amount_label):
        text = _fit_text(pdf, _safe_pdf_text(label), width, start=8.0, minimum=5.0)
        pdf.set_xy(x, y)
        pdf.cell(width, 5.0, text, align=align)
````

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: append
@@ content


def test_wide_currency_code_stays_inside_each_table_column() -> None:
    currency = "WWWWWWWW"
    pdf_bytes = build_sales_document_pdf(**_kwargs(currency_code=currency))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        points_per_mm = page.width / 210
        quantity = page.within_bbox((113 * points_per_mm, 0, 133 * points_per_mm, page.height))
        price = page.within_bbox((133 * points_per_mm, 0, 163 * points_per_mm, page.height))
        amount = page.within_bbox((163 * points_per_mm, 0, 195 * points_per_mm, page.height))

        assert "CANTIDAD" in quantity.extract_text()
        assert f"PRECIO {currency}" in price.extract_text()
        assert f"IMPORTE {currency}" in amount.extract_text()
````

#### Verification

```bash
.venv/bin/python -B -m pytest -o addopts= -s -p no:cacheprovider --noconftest -q tests/unit/test_sales_document_pdf.py
```
```