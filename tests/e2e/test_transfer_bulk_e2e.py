from __future__ import annotations

import pytest

# Si Playwright/pytest-playwright no están instalados (p.ej. CI de Gitea), se
# salta el módulo completo en vez de fallar en la importación durante la colección.
pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login_and_open_bulk(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")

    page.click("#tab-transfers")
    expect(page.locator("button[hx-get='/ui/transfers/bulk']")).to_be_visible()
    page.click("button[hx-get='/ui/transfers/bulk']")
    expect(page.locator("#transfer-bulk-modal")).to_be_visible()


def test_bulk_transfer_moves_selected_products(live_server: str, page: Page) -> None:
    base_url = live_server
    _login_and_open_bulk(page, base_url)

    row = page.locator("#transfer-bulk-modal tr.transfer-bulk-row", has_text="Producto E2E Uno")
    expect(row).to_be_visible()
    row.locator(".transfer-bulk-check").check()

    qty = row.locator('input[name="quantity"]')
    expect(qty).to_be_enabled()
    assert float(qty.input_value()) == 10.0  # stock completo precargado al marcar

    page.click("#transfer-bulk-modal button[type='submit']")

    expect(page.locator("#transfer-bulk-modal")).to_have_count(0)
    # El SKU aparece en el historial (tabla de salida y de entrada); validamos el primero.
    expect(page.locator("#tab-content").locator("text=E2E-SKU1").first).to_be_visible()


def test_bulk_select_all_respects_search_filter(live_server: str, page: Page) -> None:
    base_url = live_server
    _login_and_open_bulk(page, base_url)

    page.fill("#transfer-bulk-search", "Uno")
    page.check("#transfer-bulk-select-all")

    row_uno = page.locator("#transfer-bulk-modal tr.transfer-bulk-row", has_text="Producto E2E Uno")
    row_dos = page.locator("#transfer-bulk-modal tr.transfer-bulk-row", has_text="Producto E2E Dos")
    expect(row_uno.locator(".transfer-bulk-check")).to_be_checked()
    expect(row_dos.locator(".transfer-bulk-check")).not_to_be_checked()


def test_bulk_transfer_error_keeps_modal_open(live_server: str, page: Page) -> None:
    base_url = live_server
    _login_and_open_bulk(page, base_url)

    row = page.locator("#transfer-bulk-modal tr.transfer-bulk-row", has_text="Producto E2E Uno")
    row.locator(".transfer-bulk-check").check()
    row.locator('input[name="quantity"]').fill("999")

    page.click("#transfer-bulk-modal button[type='submit']")

    expect(page.locator("#transfer-bulk-modal")).to_be_visible()
    expect(page.locator("#transfer-bulk-modal").locator("text=Error en traspaso")).to_be_visible()


def test_bulk_modal_uses_selected_locations(live_server: str, page: Page) -> None:
    """Regresión: el modal debe usar el origen/destino seleccionados en el form principal."""
    base_url = live_server
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.click("#tab-transfers")
    expect(page.locator("button[hx-get='/ui/transfers/bulk']")).to_be_visible()

    page.select_option('select[name="from_location_code"]', "POS1")
    page.select_option('select[name="to_location_code"]', "POS2")

    page.click("button[hx-get='/ui/transfers/bulk']")
    expect(page.locator("#transfer-bulk-modal")).to_be_visible()

    summary = page.locator("#transfer-bulk-modal p.muted").inner_text()
    assert "Punto de venta Playa" in summary  # POS1
    assert "Taller Boyeros" in summary        # POS2


def test_transfer_locations_have_no_duplicates(live_server: str, page: Page) -> None:
    """Regresión: la central no debe aparecer dos veces en los selectores de traspaso."""
    base_url = live_server
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")

    page.click("#tab-transfers")
    expect(page.locator("#transfer-form")).to_be_visible()

    for sel in ('select[name="from_location_code"]', 'select[name="to_location_code"]'):
        options = page.locator(f"#transfer-form {sel} option").all_text_contents()
        assert len(options) == len(set(options)), f"{sel} tiene ubicaciones duplicadas: {options}"
