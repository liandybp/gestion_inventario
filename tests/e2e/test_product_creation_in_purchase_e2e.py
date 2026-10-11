"""E2E: la creación de producto vive solo en "Registrar compra"."""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

FORM = '#purchase-panel form[hx-post="/ui/purchase"]'


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def _submit_purchase(page: Page, **fields: str) -> None:
    for name, value in fields.items():
        page.fill(f'{FORM} input[name="{name}"]', value)
    page.click(f'{FORM} button[type="submit"]')


def _history_row(page: Page, text: str):
    return page.locator("#product-panel tr", has_text=text)


def test_purchases_tab_has_no_manual_product_form(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    expect(page.locator('form[hx-post="/ui/product"]')).to_have_count(0)
    expect(page.locator('form[hx-post="/ui/product/from-invoice"]')).to_have_count(1)
    expect(page.locator("datalist#product-options")).to_have_count(1)


def test_post_ui_product_route_removed(live_server: str, page: Page) -> None:
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/product",
        headers={"HX-Request": "true"},
        form={"name": "X", "default_purchase_cost": "1", "default_sale_price": "1"},
    )
    assert resp.status in (404, 405)


def test_purchase_new_product_with_sale_price_refreshes_product_panel(live_server: str, page: Page) -> None:
    """REQ-3 + H-008: precio guardado y #product-panel (datalist + historial) refrescado sin cambiar de pestaña."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product_name="Producto Precio E2E", sale_price="7.50", quantity="1", unit_cost="3.00")
    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()

    expect(page.locator('datalist#product-options option[value*="Producto Precio E2E"]')).to_have_count(1)
    expect(page.locator("datalist#product-options")).to_have_count(1)
    expect(_history_row(page, "Producto Precio E2E")).to_contain_text("7.50")


def test_purchase_new_product_without_sale_price_defaults_to_zero(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product_name="Producto Sin Precio E2E", quantity="1", unit_cost="2.00")
    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()
    expect(_history_row(page, "Producto Sin Precio E2E")).to_contain_text("0.00")


def test_purchase_existing_product_ignores_sale_price(live_server: str, page: Page) -> None:
    """REQ-4 + H-005: el precio no toca un producto existente y se avisa."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product="E2E-SKU1", sale_price="9.99", quantity="1", unit_cost="1.00")
    expect(page.locator("#purchase-panel").locator("text=Precio de venta ignorado").first).to_be_visible()

    page.click("#tab-inventory")
    page.click("#tab-purchases")
    row = _history_row(page, "E2E-SKU1")
    expect(row).to_contain_text("2.00")
    expect(row).not_to_contain_text("9.99")


def test_failed_purchase_does_not_leave_orphan_product(live_server: str, page: Page) -> None:
    """H-007: producto nuevo sin costo → error y ningún producto creado."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product_name="Producto Huerfano E2E", quantity="1")
    expect(page.locator("#purchase-panel").locator("text=Error en compra").first).to_be_visible()
    expect(page.locator("#purchase-panel").locator("text=indica el costo unitario").first).to_be_visible()

    page.click("#tab-inventory")
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    expect(_history_row(page, "Producto Huerfano E2E")).to_have_count(0)


@pytest.mark.parametrize("bad_price", ["-1", "nan", "inf"])
def test_purchase_rejects_invalid_sale_price(live_server: str, page: Page, bad_price: str) -> None:
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/purchase",
        headers={"HX-Request": "true"},
        form={"product_name": f"Producto Invalido {bad_price}", "sale_price": bad_price, "quantity": "1", "unit_cost": "1"},
    )
    assert resp.status == 422
    assert "El precio de venta debe ser un número mayor o igual que 0" in resp.text()


def test_purchase_failing_after_flush_rolls_back_new_product(live_server: str, page: Page) -> None:
    """H-007: la compra falla en el commit (lote duplicado) DESPUÉS del flush del producto nuevo."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product="E2E-SKU1", quantity="1", unit_cost="1.00", lot_code="LOT-DUP-E2E")
    expect(page.locator("#purchase-panel").locator("text=Compra registrada").first).to_be_visible()

    _submit_purchase(page, product_name="Producto Lote Dup E2E", quantity="1", unit_cost="1.00", lot_code="LOT-DUP-E2E")
    expect(page.locator("#purchase-panel").locator("text=Error en compra").first).to_be_visible()
    expect(page.locator("#purchase-panel").locator("text=Lote ya existe").first).to_be_visible()

    page.click("#tab-inventory")
    page.click("#tab-purchases")
    expect(page.locator("#product-panel")).to_be_visible()
    expect(_history_row(page, "Producto Lote Dup E2E")).to_have_count(0)


def test_purchase_error_keeps_typed_values(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    _submit_purchase(page, product_name="Producto Conserva E2E", sale_price="4.20", quantity="3", note="nota e2e")
    expect(page.locator("#purchase-panel").locator("text=Error en compra").first).to_be_visible()

    expect(page.locator(f'{FORM} input[name="product_name"]')).to_have_value("Producto Conserva E2E")
    expect(page.locator(f'{FORM} input[name="sale_price"]')).to_have_value("4.20")
    expect(page.locator(f'{FORM} input[name="quantity"]')).to_have_value("3.0")
    expect(page.locator(f'{FORM} input[name="note"]')).to_have_value("nota e2e")


def test_product_edit_modal_keeps_values_and_shows_error(live_server: str, page: Page) -> None:
    """REQ-3: el error de 'Guardar cambios' se ve dentro del modal y no se pierde lo tecleado."""
    _login(page, live_server)
    page.click("#tab-purchases")
    _history_row(page, "E2E-SKU1").locator('button:has-text("Edit")').click()
    modal = page.locator("#product-edit-modal")
    expect(modal).to_be_visible()

    modal.locator('input[name="new_sku"]').fill("E2E-SKU2")
    modal.locator('input[name="name"]').fill("Nombre nuevo E2E")
    modal.locator('button[type="submit"]').click()

    modal = page.locator("#product-edit-modal")
    expect(modal.get_by_text("El SKU 'E2E-SKU2' ya existe")).to_be_visible()
    expect(modal.locator('input[name="name"]')).to_have_value("Nombre nuevo E2E")
    expect(modal.locator('input[name="new_sku"]')).to_have_value("E2E-SKU2")


@pytest.mark.parametrize(
    ("fields", "detail"),
    [
        ({"quantity": "0"}, "La cantidad debe ser mayor que 0"),
        ({"quantity": "1", "movement_date": "no-es-fecha"}, "Fecha inválida"),
    ],
)
def test_purchase_invalid_quantity_or_date_returns_422_panel(
    live_server: str, page: Page, fields: dict[str, str], detail: str
) -> None:
    """H-101: antes, ValidationError/ValueError escapaban como 500 sin panel de error."""
    _login(page, live_server)
    resp = page.request.post(
        f"{live_server}/ui/purchase",
        headers={"HX-Request": "true"},
        form={"product_name": "Producto Cantidad E2E", "unit_cost": "1", **fields},
    )
    assert resp.status == 422
    body = resp.text()
    assert "Error en compra" in body
    assert detail in body


def test_purchase_quantity_input_blocks_zero(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-purchases")
    expect(page.locator(f'{FORM} input[name="quantity"]')).to_have_attribute("min", "0.0001")
