"""E2E del checkbox "pagado por transferencia" en el panel de venta rápida."""

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
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def test_sale_payment_method_default_unchecked_persists_and_editable(live_server: str, page: Page) -> None:
    """Nace desmarcado, persiste al marcarlo, se ve en el historial y es editable."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()

    create_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    checkbox = create_form.locator('input[name="paid_by_transfer"]')
    expect(checkbox).not_to_be_checked()

    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    create_form.locator('input[name="product"]').fill("E2E-SKU1")
    create_form.locator('input[name="quantity"]').fill("1")
    create_form.locator('input[name="unit_price"]').fill("2.00")
    checkbox.check()
    create_form.locator('button[type="submit"]').click()

    row = page.locator("#sale-panel table tbody tr").first
    expect(row.locator("text=Transferencia")).to_be_visible()

    row.locator('button:has-text("Edit")').click()
    edit_modal = page.locator("#sale-edit-modal")
    edit_checkbox = edit_modal.locator('input[name="paid_by_transfer"]')
    expect(edit_checkbox).to_be_checked()

    edit_checkbox.uncheck()
    edit_modal.locator('button[type="submit"]').click()

    updated_row = page.locator("#sale-panel table tbody tr").first
    expect(updated_row.locator("text=Efectivo/otro")).to_be_visible()


def test_sale_history_filters_by_payment_method(live_server: str, page: Page) -> None:
    """El filtro 'Método de pago' del historial devuelve solo transferencias."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()

    create_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    create_form.locator('input[name="product"]').fill("E2E-SKU1")
    create_form.locator('input[name="quantity"]').fill("1")
    create_form.locator('input[name="unit_price"]').fill("2.00")
    create_form.locator('input[name="paid_by_transfer"]').check()
    create_form.locator('button[type="submit"]').click()

    page.select_option("#sales-filter-payment", "transfer")
    expect(page.locator("#sales-history").locator("text=Transferencia").first).to_be_visible()
