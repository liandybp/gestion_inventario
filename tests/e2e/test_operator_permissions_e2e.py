"""H-710 (decisión 2026-11-01): el operador vende, crea y edita clientes (no los borra) y gestiona
documentos de venta. Flujo completo en navegador con el usuario `operator` sembrado."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from tests.e2e.test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e


def test_operator_manages_customers_but_cannot_delete(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url, "operator")
    open_tab(page, "customers")

    expect(page.locator("#tab-content")).to_contain_text("Cliente Tour")
    expect(page.locator('#tab-content form[hx-post$="/delete"]')).to_have_count(0)

    # Alta
    create = page.locator('#tab-content form[hx-post="/ui/customer/create"]')
    create.locator('input[name="client_id"]').fill("C-OP")
    create.locator('input[name="name"]').fill("Cliente Operador")
    create.locator('button[type="submit"]').click()
    expect(page.locator("#tab-content")).to_contain_text("Cliente Operador")

    # Edición de un cliente existente
    row = page.locator("#tab-content tr", has_text="Cliente Tour")
    edit_url = row.locator('button[hx-get$="/edit"]').get_attribute("hx-get")
    customer_id = re.search(r"/customer/(\d+)/edit", edit_url).group(1)
    row.locator('button[hx-get$="/edit"]').click()
    modal = page.locator("#customer-edit-modal")
    expect(modal).to_be_visible()
    modal.locator('input[name="name"]').fill("Cliente Tour Editado")
    modal.locator('button[type="submit"]').click()
    expect(page.locator("#tab-content")).to_contain_text("Cliente Tour Editado")

    # Borrado por petición directa: 403 y el cliente sigue ahí
    resp = page.request.post(f"{base_url}/ui/customer/{customer_id}/delete", headers={"HX-Request": "true"})
    assert resp.status == 403
    open_tab(page, "customers")
    expect(page.locator("#tab-content")).to_contain_text("Cliente Tour Editado")
    assert js_errors == []


def test_operator_deletes_sales_document(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url, "operator")
    open_tab(page, "documents")

    delete_form = page.locator('#tab-content form[hx-post^="/ui/sales-doc/"][hx-post$="/delete"]').first
    expect(delete_form).to_be_visible()
    delete_url = delete_form.get_attribute("hx-post")

    page.once("dialog", lambda d: d.accept())
    delete_form.locator('button[type="submit"]').click()
    expect(page.locator("#tab-content")).to_contain_text("Documento eliminado")
    expect(page.locator(f'#tab-content form[hx-post="{delete_url}"]')).to_have_count(0)
    assert js_errors == []
