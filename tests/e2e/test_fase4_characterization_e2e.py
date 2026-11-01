from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

SERVER_IDS = {
    "admin": ["home", "agent", "inventory", "purchases", "transfers", "coach", "sales",
              "documents", "customers", "expenses", "profit-items", "profit", "dividends",
              "history", "users", "businesses", "activity"],
    "owner": ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
              "customers", "expenses", "profit-items", "profit", "dividends", "history"],
    "operator": ["sales", "documents", "customers"],
}


@pytest.mark.parametrize("role", ["admin", "owner", "operator"])
def test_sidebar_ids_by_role(live_server: str, page: Page, role: str) -> None:
    base_url = live_server
    login(page, base_url, role)
    ids = [el.get_attribute("id") for el in page.locator(".sidebar-item").all()]
    assert ids == [f"tab-{slug}" for slug in SERVER_IDS[role]]
    selected = page.locator(".sidebar-item[aria-selected='true']").get_attribute("id")
    expected = "tab-sales" if role == "operator" else "tab-home"
    assert selected == expected


def test_purchase_autofill_cost(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "purchases")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.locator('input[name="product"]').dispatch_event("change")
    expect(page.locator("#purchase-unit-cost")).to_have_value("1.0")


def test_sale_autofill_price(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "sales")
    page.fill('input[name="product"]', "E2E-SKU1")
    page.locator('input[name="product"]').dispatch_event("change")
    expect(page.locator("#sale-unit-price")).to_have_value("2.0")


def test_sales_doc_autofill_price(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "documents")
    page.fill('#sales-doc-cart-add-form input[name="product"]', "E2E-SKU1")
    page.locator('#sales-doc-cart-add-form input[name="product"]').dispatch_event("change")
    expect(page.locator("#sales-doc-unit-price")).to_have_value("2.0")


def test_inventory_lots_select_present(live_server: str, page: Page) -> None:
    base_url = live_server
    login(page, base_url, "admin")
    open_tab(page, "inventory")
    page.locator("#tab-content #stock-table").first.wait_for()
    expect(page.locator("#return-supplier-lots")).to_contain_text("Ingrese SKU")


def test_sales_doc_autofill_client(tour_server, page: Page) -> None:
    """REQ-9/H-702: el autorrelleno de cliente en Documentos (fragmento OOB con required, REQ-12)."""
    base_url, _ = tour_server
    login(page, base_url, "admin")
    open_tab(page, "documents")
    client = page.locator('#sales-doc-issue-form input[name="client_name"]')
    client.fill("Cliente Tour")
    client.dispatch_event("change")
    expect(page.locator("#sales-doc-client-id")).to_have_value("C-TOUR")
    expect(page.locator("#sales-doc-client-address")).to_have_value("Calle 1")
    expect(page.locator("#sales-doc-client-id")).to_have_attribute("required", "")
