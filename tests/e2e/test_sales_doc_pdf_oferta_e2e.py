"""E2E del Lote 0 (REQ-10): negocio con datos bancarios -> presupuesto en moneda GEN -> PDF con el diseño OFERTA.

El PDF se descarga por `/ui/sales-doc/{id}/pdf` y se lee con pdfplumber. El segundo test emite una factura y
comprueba que su PDF no lleva "Fecha de vencimiento".
"""

from __future__ import annotations

import io
import re

import pdfplumber
import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

BANK_NAME = "Banco Metropolitano"
BANK_BRANCH = "248"
BANK_ACCOUNT = "0598770024085419"
PHONE = "54664732 / 50781349"
EMAIL = "autosandero@gmail.com"


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    # Espera a la carga inicial de #tab-content (hx-trigger=load): si se cambia de pestaña antes,
    # esa respuesta llega después y sobrescribe la pestaña nueva (test intermitente).
    page.locator("#tab-content > *").first.wait_for()


def _open_business_edit(page: Page) -> None:
    page.click("#tab-businesses")
    page.locator('tr:has(code:text-is("recambios")) button[title="Editar negocio"]').click()
    expect(page.locator("#edit-issuer-bank-account")).to_be_visible()


def _add_item(page: Page, sku: str, price: str, rows_after: int) -> None:
    form = page.locator("form#sales-doc-cart-add-form")
    price_field = form.locator('input[name="unit_price"]')
    product = form.locator('input[name="product"]')
    product.fill(sku)
    product.press("Tab")
    # El autorrelleno del precio (GET product-defaults, por OOB) deja el precio de venta del producto (2.0), pero el
    # navegador no lanza siempre `change` (sonda S10). Se espera un instante a que llegue y, llegue o no, el precio
    # se escribe después para que el autorrelleno no lo pise.
    try:
        expect(price_field).not_to_have_value("0", timeout=1500)
    except AssertionError:
        pass
    form.locator('input[name="quantity"]').fill("1")
    price_field.fill(price)
    expect(price_field).to_have_value(price)
    form.locator('button[type="submit"]').click()
    expect(page.locator('#sales-doc-panel input[id^="sales-doc-desc-"]')).to_have_count(rows_after)
    expect(page.locator(f"#sales-doc-price-{rows_after - 1}")).to_have_value(f"{float(price):.4f}")


def _fill_client(page: Page) -> None:
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('input[name="client_name"]').fill("Cliente E2E Oferta")
    issue.locator('input[name="client_id"]').fill("E2E-OFERTA-1")


def _issue_and_read_pdf(page: Page, base_url: str) -> tuple[str, "pdfplumber.page.Page", bytes]:
    page.click('form#sales-doc-issue-form button[type="submit"]')
    link = page.locator('#sales-doc-panel a:has-text("Descargar PDF")').first
    expect(link).to_be_visible()
    href = link.get_attribute("href")
    assert href and "/pdf" in href, href
    response = page.request.get(f"{base_url}{href}")
    assert response.ok, response.status
    assert "application/pdf" in response.headers.get("content-type", "")
    body = response.body()
    with pdfplumber.open(io.BytesIO(body)) as pdf:
        first_page = pdf.pages[0]
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        header_curves = len(first_page.within_bbox((0, 0, first_page.width, 125)).curves)
        header_images = len(first_page.within_bbox((0, 0, first_page.width, 125)).images)
    return text, header_curves + header_images, body


def test_quote_pdf_follows_oferta_design(live_server: str, page: Page) -> None:
    _login(page, live_server)

    # 1. El admin guarda teléfono, email, datos bancarios y plazo de validez del negocio.
    _open_business_edit(page)
    page.fill("#edit-issuer-holder", "Randy Manuel Delgado")
    page.fill("#edit-issuer-tax-id", "01030573448")
    page.fill("#edit-issuer-phone", PHONE)
    page.fill("#edit-issuer-email", EMAIL)
    page.fill("#edit-issuer-bank-name", BANK_NAME)
    page.fill("#edit-issuer-bank-branch", BANK_BRANCH)
    page.fill("#edit-issuer-bank-account", BANK_ACCOUNT)
    page.fill("#edit-issuer-bank-account-currency", "CUP")
    page.fill("#edit-sales-quote-validity-days", "15")
    page.get_by_role("button", name="Guardar Cambios").click()
    page.wait_for_load_state("networkidle")

    # Los cuatro datos bancarios sobreviven al guardado y al reabrir el modal.
    _open_business_edit(page)
    expect(page.locator("#edit-issuer-bank-name")).to_have_value(BANK_NAME)
    expect(page.locator("#edit-issuer-bank-branch")).to_have_value(BANK_BRANCH)
    expect(page.locator("#edit-issuer-bank-account")).to_have_value(BANK_ACCOUNT)
    expect(page.locator("#edit-issuer-bank-account-currency")).to_have_value("CUP")
    expect(page.locator("#edit-issuer-holder")).to_have_value("Randy Manuel Delgado")
    page.keyboard.press("Escape")

    # 2. Documentos: moneda GEN, tipo P, cliente y vencimiento; dos líneas en el carrito.
    page.click("#tab-documents")
    expect(page.locator("#sales-doc-panel")).to_be_visible()
    page.select_option("#sales-doc-currency-code", "GEN")
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('select[name="doc_type"]').select_option("P")
    _fill_client(page)
    issue.locator('input[name="valid_until"]').fill("2099-12-31")

    _add_item(page, "E2E-SKU1", "303875", 1)
    _add_item(page, "E2E-SKU2", "187000", 2)

    # La fecha elegida sobrevive a los cambios del carrito.
    expect(page.locator('form#sales-doc-issue-form input[name="valid_until"]')).to_have_value("2099-12-31")
    expect(page.locator('form#sales-doc-issue-form select[name="doc_type"]')).to_have_value("P")

    # 3. Emitir, descargar y leer el PDF.
    text, header_graphics, _body = _issue_and_read_pdf(page, live_server)

    assert "DESTINATARIO:" in text
    assert "No. Presupuesto:" in text
    assert "Fecha de vencimiento:" in text
    assert "31/12/2099" in text
    assert "Subtotal:" in text
    assert "TOTAL:" in text
    assert "490,875.00" in text
    assert "PRECIO CUP" in text
    assert "IMPORTE CUP" in text
    assert "CONCEPTO" in text and "UNIDADES" in text and "CANTIDAD" in text
    assert re.search(r"Producto E2E Uno\s+ud\s+1\s+303,875\.00\s+303,875\.00", text), text
    assert re.search(r"Producto E2E Dos\s+ud\s+1\s+187,000\.00\s+187,000\.00", text), text
    assert "TCP Randy Manuel Delgado" in text
    assert "NIT 01030573448" in text
    assert PHONE in text
    assert EMAIL in text
    assert "FORMA DE PAGO" in text
    assert f"{BANK_NAME}, Sucursal {BANK_BRANCH}" in text
    assert f"No. Cuenta (CUP): {BANK_ACCOUNT}" in text
    assert "Recibido por:" in text and "Cargo:" in text and "Firma:" in text
    # Iconos vectoriales de teléfono y email en la cabecera.
    assert header_graphics > 0
    # El diseño antiguo y el código interno de moneda no salen.
    assert "GEN" not in text
    assert "Tel.:" not in text
    assert "ID fiscal" not in text
    assert "(Precios sin IVA)" not in text


def test_invoice_pdf_has_no_due_date(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-documents")
    expect(page.locator("#sales-doc-panel")).to_be_visible()
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('select[name="doc_type"]').select_option("F")
    _fill_client(page)
    _add_item(page, "E2E-SKU1", "10", 1)

    text, _graphics, _body = _issue_and_read_pdf(page, live_server)

    assert "FACTURA" in text
    assert "No. Factura:" in text
    assert "Fecha de factura:" in text
    assert "FORMA DE PAGO" in text
    assert "Recibido por:" in text
    assert "Fecha de vencimiento" not in text
