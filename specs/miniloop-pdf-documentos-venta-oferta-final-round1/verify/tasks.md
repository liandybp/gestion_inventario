Lotes: **A** T1-T2

### T1: Identificar el documento en cada página de continuación del PDF
**Apply:** script
**Files:** `app/sales_document_pdf.py`, `tests/unit/test_sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    pdf = FPDF(format="A4", unit="mm")
@@ after
    class SalesDocumentPDF(FPDF):
        def header(self) -> None:
            if self.page_no() == 1:
                return
            self.set_font("Helvetica", "", 8)
            self.set_xy(MARGIN, 5.0)
            self.cell(CONTENT_W, 5.0, _safe_pdf_text(f"Documento: {code}"), align="R")

    pdf = SalesDocumentPDF(format="A4", unit="mm")
````

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: append
@@ content


def test_detached_payment_and_signature_page_identifies_the_document() -> None:
    items = [
        {"description": f"Artículo {i}", "quantity": 1, "unit_price": 2, "line_total": 2}
        for i in range(1, 31)
    ]
    pdf_bytes = build_sales_document_pdf(**_kwargs(items=items, subtotal=60, total=60))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        assert len(pdf.pages) == 2
        first_page, signature_page = (page.extract_text() for page in pdf.pages)

    assert "Artículo 30" in first_page
    assert "CONCEPTO" not in signature_page
    assert "Documento: P202611180001" in signature_page
    assert "FORMA DE PAGO" in signature_page
    assert "Firma:" in signature_page
````

#### Verification

```bash
.venv/bin/python -B -m pytest -o addopts='' -s -p no:cacheprovider tests/unit/test_sales_document_pdf.py
```

### T2: Asociar el campo de vencimiento con su etiqueta al emitir
**Apply:** script
**Files:** `app/templates/components/sales_doc/issue_form.html`, `tests/snapshots/baseline/empty_tab_documents.txt`, `tests/snapshots/baseline/operator_tab_documents.txt`, `tests/snapshots/baseline/owner_tab_documents.txt`, `tests/snapshots/baseline/tab_documents.txt`

#### Instructions

````pae-edit
file: app/templates/components/sales_doc/issue_form.html
op: replace
@@ before
    <div class="row"><label>Vencimiento (solo presupuesto)</label><input type="date" name="valid_until" value="{{ draft.valid_until or default_valid_until(sales_doc_config.quote_validity_days) }}" /></div>
@@ after
    <div class="row">
      <label for="sales-doc-issue-valid-until">Vencimiento (solo presupuesto)</label>
      <input id="sales-doc-issue-valid-until" type="date" name="valid_until" value="{{ draft.valid_until or default_valid_until(sales_doc_config.quote_validity_days) }}" />
    </div>
````

````pae-edit
file: tests/snapshots/baseline/empty_tab_documents.txt
op: replace
@@ before
<label>
Vencimiento (solo presupuesto)
</label>
<input name="valid_until" type="date" value="<D>">
@@ after
<label for="sales-doc-issue-valid-until">
Vencimiento (solo presupuesto)
</label>
<input id="sales-doc-issue-valid-until" name="valid_until" type="date" value="<D>">
````

````pae-edit
file: tests/snapshots/baseline/operator_tab_documents.txt
op: replace
@@ before
<label>
Vencimiento (solo presupuesto)
</label>
<input name="valid_until" type="date" value="<D>">
@@ after
<label for="sales-doc-issue-valid-until">
Vencimiento (solo presupuesto)
</label>
<input id="sales-doc-issue-valid-until" name="valid_until" type="date" value="<D>">
````

````pae-edit
file: tests/snapshots/baseline/owner_tab_documents.txt
op: replace
@@ before
<label>
Vencimiento (solo presupuesto)
</label>
<input name="valid_until" type="date" value="<D>">
@@ after
<label for="sales-doc-issue-valid-until">
Vencimiento (solo presupuesto)
</label>
<input id="sales-doc-issue-valid-until" name="valid_until" type="date" value="<D>">
````

````pae-edit
file: tests/snapshots/baseline/tab_documents.txt
op: replace
@@ before
<label>
Vencimiento (solo presupuesto)
</label>
<input name="valid_until" type="date" value="<D>">
@@ after
<label for="sales-doc-issue-valid-until">
Vencimiento (solo presupuesto)
</label>
<input id="sales-doc-issue-valid-until" name="valid_until" type="date" value="<D>">
````

#### Verification

```bash
.venv/bin/python -B tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```
