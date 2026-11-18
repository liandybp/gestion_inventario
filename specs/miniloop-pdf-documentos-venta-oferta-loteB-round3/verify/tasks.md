Lotes: **A** T1-T2

### T1: Conservar el vencimiento automático al emitir desde la vista previa
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date else "",
@@ after
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date and valid_until.strip() else "",
````

#### Verification

```bash
.venv/bin/python -m pytest -q tests/e2e/test_sales_doc_pdf_oferta_e2e.py
```

Comprobar además que, con el campo vacío y plazo cero, una vista previa generada antes de medianoche UTC envía el campo oculto vacío y permite emitir después de medianoche con vencimiento en la nueva fecha UTC. Con una fecha elegida y válida, comprobar que el campo oculto contiene esa misma fecha.

### T2: Asociar la etiqueta de vencimiento con el campo del modal
**Apply:** script
**Files:** `app/templates/features/sales_documents/sales_document_edit_form.html`

#### Instructions

````pae-edit
file: app/templates/features/sales_documents/sales_document_edit_form.html
op: replace
@@ before
    {{ field('Vencimiento (opcional)', 'valid_until', type='date', value=(doc.valid_until.isoformat() if doc.valid_until else '')) }}
@@ after
    {{ field('Vencimiento (opcional)', 'valid_until', type='date', id='sales-doc-edit-valid-until', value=(doc.valid_until.isoformat() if doc.valid_until else '')) }}
````

#### Verification

```bash
.venv/bin/python -m pytest -q tests/e2e/test_sales_doc_pdf_oferta_e2e.py
```

Abrir el modal de edición de un presupuesto y comprobar que el navegador identifica el control de fecha por el nombre accesible «Vencimiento (opcional)».
