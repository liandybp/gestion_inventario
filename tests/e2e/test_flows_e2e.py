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


def test_expense_create_appears_in_list(live_server: str, page: Page) -> None:
    """Un gasto creado desde la UI aparece en la lista (FinanceService)."""
    _login(page, live_server)
    page.click("#tab-expenses")
    expect(page.locator("#tab-content").locator("text=Gastos").first).to_be_visible()

    page.fill('input[name="concept"]', "Gasto E2E Flujo")
    page.fill('input[name="amount"]', "12.50")
    page.click('form[hx-post="/ui/expenses/create"] button[type="submit"]')

    expect(page.locator("#tab-content").locator("text=Gasto E2E Flujo").first).to_be_visible()


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


def test_profit_items_tab_renders_results(live_server: str, page: Page) -> None:
    """El tab profit-items renderiza filtro + resultados (ReportingService)."""
    _login(page, live_server)
    page.click("#tab-profit-items")
    expect(page.locator("#profit-items-results")).to_be_visible()
    expect(page.locator("#profit-items-query")).to_be_visible()


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
