from __future__ import annotations

import time

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

# Los 16 tabs del sidebar (dashboard.html). Los de rol admin se prueban con admin.
TABS = [
    "home", "agent", "inventory", "purchases", "transfers", "sales",
    "documents", "customers", "expenses", "dividends", "profit",
    "profit-items", "history", "activity", "users", "businesses",
]


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def _wait_content_html(content, timeout: float = 8.0) -> str:
    """Espera a que el partial HTMX rellene #tab-content (poll de inner_html)."""
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        last = content.inner_html()
        if len(last.strip()) > 50:
            return last
        time.sleep(0.15)
    return last


def test_all_tabs_render_without_error(live_server: str, page: Page) -> None:
    base_url = live_server
    _login(page, base_url)

    for slug in TABS:
        tab = page.locator(f"#tab-{slug}")
        expect(tab).to_be_visible(timeout=5000)
        tab.click()
        # La respuesta HTMX rellena #tab-content; asertar que NO es un error
        content = page.locator("#tab-content")
        html = _wait_content_html(content)
        expect(content).not_to_contain_text("Internal Server Error")
        expect(content).not_to_contain_text("Traceback")
        # Contenido no vacío (el partial se renderizó)
        assert len(html.strip()) > 50, f"tab '{slug}' quedó vacío: {html[:120]!r}"
