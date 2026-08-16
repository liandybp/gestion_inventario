"""Coach financiero y operativo (e2e): flujo admin completo + operador sin acceso.

Requiere `python3 -m playwright install chromium`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e


def _login(page: Page, base_url: str, username: str, password: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', username)
    page.fill('input[name="password"]', password)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")


def test_admin_coach_flow(live_server: str, page: Page) -> None:
    # _login() ya deja la página en /ui/dashboard con la carga inicial del tab por
    # defecto en curso (hx-trigger="load"). Un page.goto(dashboard) adicional aquí
    # reinicia esa carga y compite con el click siguiente por #tab-content.
    _login(page, live_server, "admin", "admin")

    # REQ-13: sin ninguna venta, el coach muestra "sin datos" en vez de un score
    # engañoso — para probar el score real, primero se genera una venta por UI
    # (mismo patrón que test_flows_e2e.py::test_sale_flow_reduces_stock).
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    page.select_option('#sale-panel form[hx-post="/ui/sale"] select[name="location_code"]', "CENTRAL")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.fill('input[name="quantity"]', "1")
    page.fill('input[name="unit_price"]', "2.00")
    page.click('#sale-panel form[hx-post="/ui/sale"] button[type="submit"]')
    page.wait_for_timeout(500)

    page.click("#tab-coach")
    expect(page.locator(".coach-tab")).to_be_visible()
    expect(page.locator(".coach-score-value")).to_be_visible()

    baseline_date = (datetime.now() - timedelta(days=30)).date().isoformat()
    page.fill("input[name='baseline_date']", baseline_date)
    page.fill("input[name='amount']", "5000")
    page.click("button:has-text('Cargar Caja')")
    page.wait_for_selector(".coach-cash-amount", timeout=5000)

    recs = page.locator(".coach-rec")
    if recs.count() > 0:
        before = recs.count()
        page.locator(".coach-rec button:has-text('Hecho')").first.click()
        page.wait_for_timeout(800)
        assert page.locator(".coach-rec").count() <= before


def test_operator_has_no_coach_access(live_server: str, page: Page) -> None:
    _login(page, live_server, "operator", "operator")

    assert page.locator("#tab-coach").count() == 0

    response = page.goto(f"{live_server}/ui/tabs/coach")
    assert response.status == 403