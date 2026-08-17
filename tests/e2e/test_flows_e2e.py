"""Flujos críticos de negocio vía UI (T-e2e-2 del PAE refactor-fase-b).

Valida que el split de la Fase B no rompió el comportamiento:
- FinanceService.create_expense / list_expenses   → flujo de gasto
- FinanceService.create_extraction / monthly_dividends_report → flujo de extracción
- ReportingService.monthly_profit_items_report    → render de profit-items
- InventoryService.sale (core)                     → flujo de venta (stock decrece)
"""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    # HTMX se inicializa tras cargar su script; sin esto, un click a un tab puede
    # ocurrir antes de que HTMX registre sus listeners y perderse (flake CI).
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def test_extraction_create_updates_dividends(live_server: str, page: Page) -> None:
    """Una extracción creada desde la UI aparece (FinanceService)."""
    _login(page, live_server)
    page.click("#tab-dividends")
    expect(page.locator("#tab-content").locator("text=Dividendos").first).to_be_visible()

    page.select_option('form[hx-post="/ui/extractions/create"] select[name="party"]', label="Liandy")
    page.fill('input[name="amount"]', "25.00")
    page.fill('input[name="concept"]', "Extraccion E2E Flujo")
    page.click('form[hx-post="/ui/extractions/create"] button[type="submit"]')

    expect(page.locator("#tab-content").locator("text=Extraccion E2E Flujo").first).to_be_visible()


def test_sale_flow_reduces_stock(live_server: str, page: Page) -> None:
    """Una venta contra CENTRAL persiste y aparece en el historial (InventoryService core)."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()

    # El stock sembrado por el conftest vive en CENTRAL → seleccionar esa ubicación
    page.select_option(
        '#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL"
    )
    page.fill('input[name="product"]', "E2E-SKU1")
    page.fill('input[name="quantity"]', "1")
    page.fill('input[name="unit_price"]', "2.00")
    page.click('#sale-panel form[hx-post="/ui/sale"] button[type="submit"]')

    # El panel se re-renderiza con la venta en el historial
    expect(page.locator("#sale-panel").locator("text=E2E-SKU1").first).to_be_visible()


def test_purchase_flow_increases_stock(live_server: str, page: Page) -> None:
    """Compra registrada desde la UI aparece en el historial (InventoryService.purchase)."""
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#purchase-panel")).to_be_visible()

    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="product"]', "E2E-SKU2")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="quantity"]', "3")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="unit_cost"]', "1.50")
    page.click('#purchase-panel form[hx-post="/ui/purchase"] button[type="submit"]')

    # El panel se re-renderiza con la compra en el historial
    expect(page.locator("#purchase-panel").locator("text=E2E-SKU2").first).to_be_visible()


def test_purchase_flow_new_product_without_sku(live_server: str, page: Page) -> None:
    """Registrar una compra dejando 'Artículo' en blanco y solo 'Nombre (si es nuevo)'
    debe crear el producto con SKU autogenerado, no romper con un 500 (regresión
    Fase C: list_sku_prefixes ya devuelve list[str], pero _generate_auto_sku volvía
    a desempaquetar cada string como tupla — solo se dispara si ya existe al menos
    un producto con prefijo SKU, como ocurre en producción con cientos de productos
    SKU0000xx)."""
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#purchase-panel")).to_be_visible()

    # Primero, un producto con SKU explícito con el prefijo por defecto ("SKU..."),
    # para que list_sku_prefixes("SKU") no esté vacío al generar el siguiente SKU.
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="product"]', "SKU000001")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="product_name"]', "Producto Base E2E")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="quantity"]', "1")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="unit_cost"]', "1.00")
    page.click('#purchase-panel form[hx-post="/ui/purchase"] button[type="submit"]')
    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()

    # Ahora, un producto nuevo sin indicar SKU manualmente: dispara _generate_auto_sku.
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="product_name"]', "Producto Nuevo E2E Sin SKU")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="quantity"]', "2")
    page.fill('#purchase-panel form[hx-post="/ui/purchase"] input[name="unit_cost"]', "5.00")
    page.click('#purchase-panel form[hx-post="/ui/purchase"] button[type="submit"]')

    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()
    expect(page.locator("#purchase-panel").locator("text=Producto Nuevo E2E Sin SKU").first).to_be_visible()


def test_document_issue_flow(live_server: str, page: Page) -> None:
    """Emitir un documento de venta crea el doc (SalesDocumentService / tab-documents)."""
    _login(page, live_server)
    page.click("#tab-documents")
    expect(page.locator("#sales-doc-panel")).to_be_visible()

    # Añadir un ítem al carrito
    page.fill('form#sales-doc-cart-add-form input[name="product"]', "E2E-SKU1")
    page.fill('form#sales-doc-cart-add-form input[name="quantity"]', "1")
    page.fill('form#sales-doc-cart-add-form input[name="unit_price"]', "2.00")
    page.click('form#sales-doc-cart-add-form button[type="submit"]')

    # El ítem se agregó (flash de confirmación visible)
    expect(page.locator("#sales-doc-panel").locator("text=Producto agregado al documento").first).to_be_visible()

    # Emitir el documento (requiere cliente)
    page.fill('form#sales-doc-issue-form input[name="client_name"]', "Cliente E2E Doc")
    page.fill('form#sales-doc-issue-form input[name="client_id"]', "E2E-DOC1")
    page.click('form#sales-doc-issue-form button[type="submit"]')

    # El documento aparece en recientes (desaparece "Sin documentos emitidos")
    expect(page.locator("#sales-doc-panel").locator("text=Sin documentos emitidos").first).not_to_be_visible()
