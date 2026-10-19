from __future__ import annotations

import pytest

# Si Playwright/pytest-playwright no están instalados (p.ej. CI de Gitea), se
# salta el módulo completo en vez de fallar en la importación durante la colección.
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
    # Espera a la carga inicial de #tab-content (hx-trigger=load): si se cambia de pestaña antes,
    # esa respuesta llega después y sobrescribe la pestaña nueva (test intermitente).
    page.locator("#tab-content > *").first.wait_for()
