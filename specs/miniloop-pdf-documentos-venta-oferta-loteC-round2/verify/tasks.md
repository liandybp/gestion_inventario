Lotes: **A** T1-T2

### T1: Comprobar los límites de los bloques del PDF
**Apply:** script
**Files:** `tests/unit/test_sales_document_pdf.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: replace
@@ before
    text, _ = _read(build_sales_document_pdf(**_kwargs(doc_label=long_label, issuer_address=long_address)))

    assert long_label.upper() in " ".join(text.split())
    assert "Avenida de los Libertadores" in text
    assert "..." in text  # la etiqueta del bloque documento se corta con "..." al llegar a 6 pt
@@ after
    pdf_bytes = build_sales_document_pdf(**_kwargs(doc_label=long_label, issuer_address=long_address))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        band, first_rule, _, document_row = page.rects[:4]
        midpoint = (band["x0"] + band["x1"]) / 2
        address = page.within_bbox((midpoint, band["top"], band["x1"], band["bottom"])).extract_text()
        title = page.within_bbox((midpoint, band["bottom"], band["x1"], first_rule["top"])).extract_text()
        row = page.within_bbox(
            (document_row["x0"], document_row["top"], document_row["x1"], document_row["bottom"])
        ).extract_text()

    assert long_address in " ".join(address.split())
    assert " ".join(title.split()) == long_label.upper()
    assert re.fullmatch(r"No\. Presupuesto .+\.\.\. P202611180001", " ".join(row.split()))
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    text, _ = _read(build_sales_document_pdf(**_kwargs(doc_label=long_label, issuer_address=long_address)))

    assert long_label.upper() in " ".join(text.split())
    assert "Avenida de los Libertadores" in text
    assert "..." in text  # la etiqueta del bloque documento se corta con "..." al llegar a 6 pt
@@ after
    pdf_bytes = build_sales_document_pdf(**_kwargs(doc_label=long_label, issuer_address=long_address))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        band, first_rule, _, document_row = page.rects[:4]
        midpoint = (band["x0"] + band["x1"]) / 2
        address = page.within_bbox((midpoint, band["top"], band["x1"], band["bottom"])).extract_text()
        title = page.within_bbox((midpoint, band["bottom"], band["x1"], first_rule["top"])).extract_text()
        row = page.within_bbox(
            (document_row["x0"], document_row["top"], document_row["x1"], document_row["bottom"])
        ).extract_text()

    assert long_address in " ".join(address.split())
    assert " ".join(title.split()) == long_label.upper()
    assert re.fullmatch(r"No\. Presupuesto .+\.\.\. P202611180001", " ".join(row.split()))
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -o addopts='' -s -p no:cacheprovider -q tests/unit/test_sales_document_pdf.py::test_long_document_label_and_long_address_stay_inside_their_boxes
```

### T2: Comprobar la secuencia completa de filas del PDF paginado
**Apply:** script
**Files:** `tests/unit/test_sales_document_pdf.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: replace
@@ before
    assert sum(t.count("Artículo 60 ") for t in page_texts) == 1
@@ after
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    assert sum(t.count("Artículo 60 ") for t in page_texts) == 1
@@ after
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -o addopts='' -s -p no:cacheprovider -q tests/unit/test_sales_document_pdf.py::test_long_table_repeats_its_header_and_keeps_the_footer_whole
```
