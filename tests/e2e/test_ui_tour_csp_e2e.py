"""Fase 2 del release 0.5.0: recorrido de TODAS las pestañas, modales y páginas sueltas con el
CSP sin 'unsafe-inline'. Falla ante cualquier excepción de JS, `console.error` o violación de
CSP (fixture `js_errors`), y comprueba que los modales se abren y se cierran con su ×
(cableado de core/modals.js)."""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

# Las 17 pestañas del admin (dashboard.html), en el orden del sidebar.
TABS = [
    "home", "agent", "inventory", "purchases", "transfers", "coach", "sales", "documents",
    "customers", "expenses", "profit-items", "profit", "dividends", "history", "users",
    "businesses", "activity",
]

# (pestaña, selector del botón que abre el modal, id del modal). El modal de cambio de
# contraseña obligatorio tiene su propio test.
MODALS = [
    ("inventory", '[hx-get$="/edit-inventory"]', "product-inventory-edit-modal"),
    ("purchases", '[hx-get^="/ui/movement/purchase/"]', "purchase-edit-modal"),
    ("transfers", '[hx-get^="/ui/movement/transfer/"]', "transfer-edit-modal"),
    ("transfers", '[hx-get="/ui/transfers/bulk"]', "transfer-bulk-modal"),
    ("sales", '[hx-get^="/ui/movement/sale/"]', "sale-edit-modal"),
    ("documents", '[hx-get^="/ui/sales-doc/"][hx-get$="/edit"]', "sales-doc-edit-modal"),
    ("customers", '[hx-get^="/ui/customer/"][hx-get$="/edit"]', "customer-edit-modal"),
    ("expenses", '[hx-get^="/ui/expense/"][hx-get$="/edit"]', "expense-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/sale/"]', "profit-items-sale-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/purchase/"]', "profit-items-purchase-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/transfer-in/"]', "profit-items-transfer-in-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/adjustment/"]', "profit-items-adjustment-edit-modal"),
    ("dividends", '[hx-get^="/ui/extraction/"][hx-get$="/edit"]', "extraction-edit-modal"),
    ("users", '[hx-get^="/ui/user/"][hx-get$="/edit"]', "user-edit-modal"),
    ("users", '[hx-get$="/reset-password-form"]', "user-reset-password-modal"),
    ("businesses", '[hx-get^="/ui/business/"][hx-get$="/edit"]', "business-edit-modal"),
]


def login(page: Page, base_url: str, user: str = "admin") -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', user)
    page.fill('input[name="password"]', user)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    page.locator("#tab-content > *").first.wait_for()


def open_tab(page: Page, slug: str) -> None:
    with page.expect_response(lambda r: r.url.split("?")[0].endswith(f"/ui/tabs/{slug}")) as resp:
        page.click(f"#tab-{slug}")
    assert resp.value.status == 200, f"/ui/tabs/{slug} -> {resp.value.status}"
    # Los enhancers corren dos frames después del swap (core/content.js).
    page.wait_for_timeout(250)


def test_all_tabs_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    for slug in TABS:
        open_tab(page, slug)
        expect(page.locator("#tab-content")).not_to_contain_text("Internal Server Error")
    assert js_errors == []


def test_all_modals_open_and_close_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    current = None
    for slug, opener, modal_id in MODALS:
        if slug != current:
            open_tab(page, slug)
            current = slug
        page.locator(opener).first.click()
        modal = page.locator(f"#{modal_id}")
        expect(modal).to_be_visible()
        modal.locator(".modal-close").first.click()
        expect(modal).to_have_count(0)
    assert js_errors == []


def test_product_edit_modal_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    # Su único botón está en partials/products_history.html, que hoy no incluye ninguna
    # pestaña: se abre con la misma petición que haría ese botón (hx-target="body").
    base_url, _ = tour_server
    login(page, base_url)
    page.evaluate("htmx.ajax('GET', '/ui/product/TOUR-1/edit', {target: 'body', swap: 'beforeend'})")
    modal = page.locator("#product-edit-modal")
    expect(modal).to_be_visible()
    modal.locator(".modal-close").first.click()
    expect(modal).to_have_count(0)
    assert js_errors == []


def test_forced_password_change_modal_is_static(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url, user="mustchange")
    modal = page.locator("#must-change-password-modal")
    expect(modal).to_be_visible()
    modal.click(position={"x": 5, "y": 5})  # clic en el overlay: data-modal-static="1" no cierra
    expect(modal).to_be_visible()
    assert js_errors == []


@pytest.mark.parametrize("user, tabs", [("operator", ["sales"]), ("owner", ["home", "agent", "sales", "history"])])
def test_other_roles_tabs_without_js_errors(tour_server, page: Page, js_errors: list[str], user, tabs) -> None:
    base_url, _ = tour_server
    login(page, base_url, user=user)
    for slug in tabs:
        open_tab(page, slug)
    assert js_errors == []


def test_login_and_print_pages_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    page.goto(f"{base_url}/ui/login")
    page.wait_for_load_state("load")
    login(page, base_url)
    for path in ("/ui/restock-print", f"/ui/transfers/print?ref={ids['transfer_ref']}",
                 f"/ui/purchase/{ids['purchase']}/label"):
        resp = page.goto(f"{base_url}{path}")
        assert resp.status == 200, path
        page.wait_for_load_state("load")
    expect(page.locator("#sheet svg.barcode rect").first).to_be_attached()
    assert js_errors == []
