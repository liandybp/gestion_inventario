"""E2E REQ-1: al teclear en 'Artículo' el datalist se rellena desde /ui/product-suggest."""

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


def test_sale_product_input_keeps_stock_filtered_list(live_server: str, page: Page) -> None:
    """H-201: el datalist de Ventas filtra por stock en la ubicación; teclear no lo sustituye."""
    _login(page, live_server)
    page.click("#tab-sales")
    expect(page.locator("#sale-panel")).to_be_visible()
    suggest_requests: list[str] = []
    page.on("request", lambda r: suggest_requests.append(r.url) if "/ui/product-suggest" in r.url else None)
    list_html = "document.querySelector('#sale-panel datalist#product-options').innerHTML"
    before = page.evaluate(list_html)

    # El stock sembrado está en CENTRAL y la venta arranca en el POS: sin stock no hay sugerencias,
    # y teclear no debe traer productos del catálogo completo.
    page.fill('#sale-panel input[name="product"]', "Producto E2E")
    page.wait_for_timeout(700)  # > debounce de 250 ms

    assert suggest_requests == []
    assert page.evaluate(list_html) == before


def test_purchase_product_input_fetches_suggestions(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    page.evaluate("document.getElementById('product-options').innerHTML = ''")

    page.fill('#purchase-panel input[name="product"]', "Producto E2E Uno")

    expect(page.locator('datalist#product-options option[value="E2E-SKU1 - Producto E2E Uno"]')).to_have_count(1)


def test_choosing_a_suggestion_keeps_the_list(live_server: str, page: Page) -> None:
    """H-203: elegir una opción no vacía el datalist; vaciar el campo restaura la lista inicial."""
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    options = page.locator("datalist#product-options option")
    initial = options.count()
    assert initial >= 2

    field = page.locator('#purchase-panel input[name="product"]')
    field.fill("Producto E2E")
    expect(page.locator('datalist#product-options option[value="E2E-SKU2 - Producto E2E Dos"]')).to_have_count(1)
    field.fill("E2E-SKU1 - Producto E2E Uno")  # lo que inserta el navegador al elegir la opción
    page.wait_for_timeout(700)
    expect(page.locator('datalist#product-options option[value="E2E-SKU1 - Producto E2E Uno"]')).to_have_count(1)

    page.evaluate("document.getElementById('product-options').innerHTML = '<option value=\"X\"></option>'")
    field.fill("")
    expect(options).to_have_count(initial)


@pytest.mark.parametrize("bad_price", ["nan", "abc"])
def test_sale_rejects_typed_invalid_price(live_server: str, page: Page, bad_price: str) -> None:
    """H-204: antes se vendía al precio por defecto sin avisar."""
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/sale",
        headers={"HX-Request": "true"},
        form={"product": "E2E-SKU1", "quantity": "1", "unit_price": bad_price},
    )
    assert resp.status == 422
    assert "El precio unitario debe ser un número" in resp.text()
