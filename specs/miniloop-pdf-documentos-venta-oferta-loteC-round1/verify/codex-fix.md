**Un hallazgo en T21.** El PDF contiene la etiqueta completa, pero la reparte en dos líneas. El test busca una cadena con espacios literales y falla cuando `pdfplumber` devuelve `DE\nVERANO`. La evidencia registra **1 fallo, 615 pruebas superadas y 91 snapshots equivalentes**. La referencia a `JsonPlusSerializer` aparece en un aviso de deprecación.

La corrección normaliza los espacios del texto extraído únicamente en esa aserción. El mismo reemplazo en el test y en su envelope mantiene ambos sincronizados. Las 12 pruebas del PDF pasaron al ejecutar el cambio en memoria. La verificación completa del lote queda pendiente de aplicar el bloque. No modifiqué ningún fichero ni encontré otros bugs en el diff solicitado.

**Ficheros revisados en el diff**

- `tests/unit/test_business_bank_fields.py`
- `tests/unit/test_sales_doc_valid_until.py`
- `tests/unit/test_sales_document_pdf.py`
- `tests/unit/test_sales_documents_format.py`
- `specs/pdf-documentos-venta-oferta/verify/loteC.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteC.test.txt`

**Referencias consultadas**

- `specs/pdf-documentos-venta-oferta/tasks.md` (T20–T26)
- `specs/pdf-documentos-venta-oferta/verify/loteC.lint.txt`
- `app/sales_document_pdf.py` (ajuste de la etiqueta al ancho)
- `/home/liandy/.local/share/harness-loop/pae-edit-format.md`

`````tasks-md
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
`````