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


def test_create_no_pos_business_and_documents_show_central(live_server: str, page: Page) -> None:
    """Regresión: un negocio sin POS (campos POS vacíos) crea solo central y el
    panel de Documentos ofrece la central (el dropdown de ubicación no queda vacío)."""
    base_url = live_server
    _login(page, base_url)

    # ── Crear negocio SIN POS desde la pestaña Negocios ─────────────
    page.click("#tab-businesses")
    expect(page.locator("#new-code")).to_be_visible()
    page.fill("#new-code", "nopos_e2e")
    page.fill("#new-name", "Negocio Sin POS E2E")
    page.fill("#new-locations-pos", "")  # vacío = negocio sin POS
    page.fill("#new-locations-default-pos", "")
    page.click("button:has-text('Crear Negocio')")

    # El negocio aparece en la lista de la pestaña
    expect(page.locator("#tab-content").locator("text=Negocio Sin POS E2E").first).to_be_visible()

    # ── Cambiar al negocio sin POS (recarga para refrescar el selector del sidebar) ──
    page.reload()
    expect(page.locator("select[name='business_id']")).to_be_visible()
    page.select_option('select[name="business_id"]', label="Negocio Sin POS E2E")
    expect(page.locator(".sidebar-header").locator("text=Negocio: Negocio Sin POS E2E")).to_be_visible()

    # ── Documentos: el select de ubicación debe ofrecer la central (no vacío) ──
    page.click("#tab-documents")
    loc_select = page.locator('select[name="location_code"]')
    expect(loc_select).to_be_visible()
    options = loc_select.locator("option").all_text_contents()
    assert options, "El dropdown de ubicación quedó vacío para un negocio sin POS"
    assert any(
        "central" in (o or "").lower() for o in options
    ), f"El dropdown no ofrece la central: {options}"
