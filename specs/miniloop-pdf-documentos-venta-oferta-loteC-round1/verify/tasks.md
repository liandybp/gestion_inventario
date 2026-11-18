Lotes: **A** T1-T1

### T1: Admitir saltos de línea en la etiqueta extraída del PDF
**Apply:** script
**Files:** `tests/unit/test_sales_document_pdf.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: replace
@@ before
    assert long_label.upper() in text
@@ after
    assert long_label.upper() in " ".join(text.split())
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    assert long_label.upper() in text
@@ after
    assert long_label.upper() in " ".join(text.split())
````

#### Verification

```bash
.venv/bin/python -m pytest -q --override-ini="addopts=" tests/unit/test_business_bank_fields.py tests/unit/test_sales_doc_valid_until.py tests/unit/test_sales_document_pdf.py tests/unit/test_sales_documents_format.py
```
