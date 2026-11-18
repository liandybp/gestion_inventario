**Un hallazgo funcional.** Al previsualizar un presupuesto con el vencimiento vacío, el servidor calcula y muestra la fecha por defecto, pero envía un valor vacío en «Emitir ahora». Si la emisión cruza la medianoche UTC o cambia el plazo de validez del negocio, se puede guardar una fecha distinta de la que el usuario vio. La corrección envía la fecha calculada en la vista previa. [app/routers/ui_sales_documents.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/routers/ui_sales_documents.py:295)

El diff deja intacto el contrato del Lote 0. La corrección se limita al router. La revisión fue estática y no escribí ficheros ni ejecuté la suite.

**Ficheros revisados en el diff**

- `app/routers/ui_sales_documents.py`
- `app/templates/features/sales_documents/_valid_until_error.html`
- `app/templates/features/sales_documents/sales_document_edit_form.html`
- `app/templates/features/sales_documents/sales_document_preview.html`
- `specs/pdf-documentos-venta-oferta/verify/loteB.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteB.test.txt`

**Ficheros consultados para contrastar el comportamiento**

- `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`
- `app/sales_documents_format.py`
- `app/templates/components/forms.html`
- `app/templates/components/sales_doc/issue_form.html`
- `app/static/js/core/swap.js`
- `app/static/js/core/modals.js`
- `specs/pdf-documentos-venta-oferta/requirements.md`
- `specs/pdf-documentos-venta-oferta/tasks.md`

`````tasks-md
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
`````