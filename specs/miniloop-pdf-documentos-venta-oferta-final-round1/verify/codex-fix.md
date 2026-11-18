Encontré dos bugs en el diff solicitado. No modifiqué ningún fichero.

1. **La página de firma puede quedar sin identificar.** Con un presupuesto de 30 líneas, reproduje un PDF que deja toda la tabla en la primera página y pone los datos de pago y la firma en la segunda, sin el número del documento. Si esa hoja se imprime o se firma por separado, no se puede asociar al presupuesto desde la propia hoja. La corrección imprime el código en cada página de continuación, incluidas las que solo contienen notas o el pie. [app/sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/sales_document_pdf.py:392)
2. **El campo de vencimiento al emitir carece de nombre accesible.** Su etiqueta está al lado del control, pero el HTML no los asocia. Un lector de pantalla puede anunciar el campo sin indicar qué fecha se pide. La corrección enlaza el `label` y el control con `for` e `id`, y actualiza los cuatro snapshots afectados. [issue_form.html](/home/liandy/PyCharmMiscProject/gestion_inventario/app/templates/components/sales_doc/issue_form.html:30)

Pasaron 45 pruebas de PDF y formato. Probé también la corrección del PDF en memoria con el pie separado y con notas de varias páginas. La comprobación de snapshots queda pendiente de aplicar los cambios.

**Ficheros revisados**

- `.bumpversion.cfg`, `CHANGELOG.md`, `CLAUDE.md`, `VERSION`.
- `app/business_config.py`, `app/migrations.py`, `app/models.py`, `app/repositories/business_repository.py`, `app/routers/ui_businesses.py`, `app/routers/ui_common.py`, `app/routers/ui_sales_documents.py`, `app/sales_document_pdf.py`, `app/sales_documents_format.py`.
- `app/templates/components/sales_doc/issue_form.html`, `app/templates/features/businesses/business_edit_form.html`, `app/templates/features/businesses/tab_businesses.html`, `app/templates/features/sales_documents/_valid_until_error.html`, `app/templates/features/sales_documents/sales_document_edit_form.html`, `app/templates/features/sales_documents/sales_document_preview.html`.
- `specs/pdf-documentos-venta-oferta/design.md`, `specs/pdf-documentos-venta-oferta/tasks.md`, `specs/pdf-documentos-venta-oferta/design/sondas/S11-estres-pdf.txt`. Consulté los resultados y resúmenes de `specs/pdf-documentos-venta-oferta/verify/`. De los tres PNG solo comprobé su presencia en el diff.
- `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`, `tests/unit/test_business_bank_fields.py`, `tests/unit/test_sales_doc_valid_until.py`, `tests/unit/test_sales_document_pdf.py`, `tests/unit/test_sales_documents_format.py`.
- `tests/snapshots/baseline/empty_tab_businesses.txt`, `tests/snapshots/baseline/empty_tab_documents.txt`, `tests/snapshots/baseline/modal_business.txt`, `tests/snapshots/baseline/operator_tab_documents.txt`, `tests/snapshots/baseline/owner_tab_documents.txt`, `tests/snapshots/baseline/tab_businesses.txt`, `tests/snapshots/baseline/tab_documents.txt`.

`````tasks-md
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
`````