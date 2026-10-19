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


def _open_edit_form(page: Page) -> None:
    page.click("#tab-businesses")
    page.locator('tr:has(code:text-is("recambios")) button[title="Editar negocio"]').click()
    expect(page.locator("#edit-dividends-opening-pending")).to_be_visible()


def test_saving_business_keeps_dividends_opening_pending(page: Page, live_server: str) -> None:
    # Regresión 2026-10-19: guardar el formulario borraba el saldo inicial de dividendos.
    _login(page, live_server)
    _open_edit_form(page)
    field = page.locator("#edit-dividends-opening-pending")
    before = field.input_value()
    assert '"Liandy": 2715' in before, before

    page.get_by_role("button", name="Guardar Cambios").click()
    page.wait_for_load_state("networkidle")

    _open_edit_form(page)
    assert page.locator("#edit-dividends-opening-pending").input_value() == before
