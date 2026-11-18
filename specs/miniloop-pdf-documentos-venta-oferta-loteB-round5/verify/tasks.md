Lotes: **A** T1-T1

### T1: Enviar al emitir el vencimiento mostrado en la vista previa
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date and valid_until.strip() else "",
@@ after
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date else "",
````

#### Verification

```bash
.venv/bin/python -m pytest -q tests/e2e/test_sales_doc_pdf_oferta_e2e.py
```

En Documentos, añade una línea al carrito y borra el vencimiento de un presupuesto antes de pulsar «Vista previa». Comprueba que el campo oculto `valid_until` de «Emitir ahora» contiene en formato ISO la fecha mostrada y que el PDF emitido conserva esa fecha.
