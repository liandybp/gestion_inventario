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
