"""Fase 1 del release 0.5.0: modales migrados a la macro components/modal.html.

Regresión del bug del prototipo descartado: el modal "Editar cliente" mostraba el HTML de los
campos como texto escapado en vez de inputs. También cubre el modal unificado de Utilidades.
"""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

RAW_NAME = "Cliente <b>E2E</b>"


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    page.locator("#tab-content > *").first.wait_for()


def _create_customer_and_open_edit(page: Page) -> None:
    page.click("#tab-customers")
    form = page.locator('form[hx-post="/ui/customer/create"]')
    expect(form).to_be_visible()
    form.locator('input[name="client_id"]').fill("E2E-C1")
    form.locator('input[name="name"]').fill(RAW_NAME)
    form.locator('button[type="submit"]').click()
    row = page.locator("#customers-table tr", has_text="E2E-C1")
    expect(row).to_be_visible()
    row.locator("button", has_text="Editar").click()
    expect(page.locator("#customer-edit-modal")).to_be_visible()


def test_customer_edit_modal_renders_inputs_and_saves(live_server: str, page: Page) -> None:
    _login(page, live_server)
    _create_customer_and_open_edit(page)

    modal = page.locator("#customer-edit-modal")
    name_input = modal.locator('input[name="name"]')
    expect(name_input).to_have_value(RAW_NAME)
    expect(modal.locator('input[name="client_id"]')).to_have_value("E2E-C1")
    assert "<input" not in modal.inner_text()
    assert "&lt;" not in modal.inner_text()

    name_input.fill("Cliente E2E Editado")
    modal.locator('button[type="submit"]').click()
    expect(page.locator("#customer-edit-modal")).to_have_count(0)
    expect(page.locator("#customers-table").locator("text=Cliente E2E Editado")).to_be_visible()


def test_customer_edit_modal_closes_with_x(live_server: str, page: Page) -> None:
    _login(page, live_server)
    _create_customer_and_open_edit(page)

    page.locator("#customer-edit-modal .modal-close").click()
    expect(page.locator("#customer-edit-modal")).to_have_count(0)


def test_profit_items_unified_modals_open_for_sale_and_purchase(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-sales")
    sale_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    sale_form.locator('select[name="location_code"]').select_option("CENTRAL")
    sale_form.locator('input[name="product"]').fill("E2E-SKU1")
    sale_form.locator('input[name="quantity"]').fill("1")
    sale_form.locator('input[name="unit_price"]').fill("2.00")
    sale_form.locator('button[type="submit"]').click()
    expect(page.locator("#sale-panel").locator("text=E2E-SKU1").first).to_be_visible()

    page.click("#tab-profit-items")
    row = page.locator("#tab-content tr", has_text="E2E-SKU1").first
    expect(row).to_be_visible()

    row.locator("button", has_text="Edit sale").click()
    sale_modal = page.locator("#profit-items-sale-edit-modal")
    expect(sale_modal).to_be_visible()
    expect(sale_modal.locator('input[name="product"]')).to_have_value("E2E-SKU1 - Producto E2E Uno")
    expect(sale_modal.locator('input[name="unit_price"]')).to_be_editable()
    sale_modal.locator(".modal-close").click()
    expect(sale_modal).to_have_count(0)

    row.locator("button", has_text="Edit buys").click()
    buy_modal = page.locator("#profit-items-purchase-edit-modal")
    expect(buy_modal).to_be_visible()
    expect(buy_modal.locator('input[name="lot_code"]')).to_have_value("E2E-SKU1-E2E")

    # H-305: los dos pasos anteriores abren y comprueban valores, pero ninguno llega a
    # guardar — una regresión en el `hx-post`/`hx-target` del unificado pasaría sin que
    # ningún e2e la note. Cambia el costo unitario, guarda, y confirma el round-trip
    # reabriendo el modal (la tabla muestra `cogs` calculado, no `unit_cost` literal).
    buy_modal.locator('input[name="unit_cost"]').fill("9.99")
    buy_modal.locator('button[type="submit"]').click()
    expect(buy_modal).to_have_count(0)

    row = page.locator("#tab-content tr", has_text="E2E-SKU1").first
    row.locator("button", has_text="Edit buys").click()
    buy_modal = page.locator("#profit-items-purchase-edit-modal")
    expect(buy_modal.locator('input[name="unit_cost"]')).to_have_value("9.99")
