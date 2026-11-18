Encontré dos bugs en el diff. No modifiqué ningún fichero ni ejecuté la suite.

1. **El vencimiento automático se convierte en una fecha fija al pasar por la vista previa.** Si se deja vacío el campo, la vista previa calcula una fecha y la envía como si el usuario la hubiera elegido. Con un plazo de cero días, una vista previa abierta antes de medianoche UTC devuelve un 422 al pulsar «Emitir ahora» después de medianoche. La corrección conserva las fechas elegidas y mantiene vacío el valor enviado cuando corresponde calcularlo en la emisión. [ui_sales_documents.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/routers/ui_sales_documents.py:295)
2. **El campo de vencimiento del modal carece de nombre accesible.** La macro genera una etiqueta y un control separados. Sin `id`, la etiqueta visible queda desvinculada del campo. La corrección los asocia mediante el parámetro `id` de la macro. [sales_document_edit_form.html](/home/liandy/PyCharmMiscProject/gestion_inventario/app/templates/features/sales_documents/sales_document_edit_form.html:14)

**Ficheros del diff revisados**

- `app/routers/ui_sales_documents.py`
- `app/templates/features/sales_documents/_valid_until_error.html`
- `app/templates/features/sales_documents/sales_document_edit_form.html`
- `app/templates/features/sales_documents/sales_document_preview.html`
- `specs/pdf-documentos-venta-oferta/verify/loteB.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteB.test.txt`

```tasks-md
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
```