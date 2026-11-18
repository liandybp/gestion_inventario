**P2 · El vencimiento mostrado en la vista previa puede cambiar al emitir.** Si se deja vacío el campo de un presupuesto, la vista previa muestra la fecha calculada pero envía un campo oculto vacío. «Emitir ahora» vuelve a calcular la fecha. Si cambia el día UTC o el plazo configurado entre ambas peticiones, el documento puede guardar una fecha distinta de la que el usuario confirmó. La corrección envía la fecha ISO mostrada y conserva la validación al emitir. [ui_sales_documents.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/routers/ui_sales_documents.py:295)

La revisión fue estática y en solo lectura. La prueba de regresión se incluye en la tarea y queda pendiente de ejecución.

**Ficheros revisados en el diff**

- `app/routers/ui_sales_documents.py`
- `app/templates/features/sales_documents/_valid_until_error.html`
- `app/templates/features/sales_documents/sales_document_edit_form.html`
- `app/templates/features/sales_documents/sales_document_preview.html`
- `specs/pdf-documentos-venta-oferta/verify/loteB.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteB.test.txt`

```tasks-md
Lotes: **A** T1-T1

### T1: Enviar el vencimiento mostrado en la vista previa
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`, `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date and valid_until.strip() else "",
@@ after
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date is not None else "",
````

````pae-edit
file: tests/e2e/test_sales_doc_pdf_oferta_e2e.py
op: insert_before
@@ anchor
def test_invoice_pdf_has_no_due_date(live_server: str, page: Page) -> None:
@@ content
def test_quote_preview_submits_displayed_default_due_date(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-documents")
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('select[name="doc_type"]').select_option("P")
    _fill_client(page)
    _add_item(page, "E2E-SKU1", "10", 1)

    issue.locator('input[name="valid_until"]').fill("")
    issue.get_by_role("button", name="Vista previa").click()

    preview = page.locator("#sales-doc-preview")
    hidden_due_date = preview.locator('input[type="hidden"][name="valid_until"]')
    expect(hidden_due_date).to_have_value(re.compile(r"^\d{4}-\d{2}-\d{2}$"))
    year, month, day = hidden_due_date.input_value().split("-")
    expect(preview).to_contain_text(f"Vencimiento: {day}/{month}/{year}")


````

#### Verification

```bash
.venv/bin/python -m pytest tests/e2e/test_sales_doc_pdf_oferta_e2e.py -k test_quote_preview_submits_displayed_default_due_date
```
```