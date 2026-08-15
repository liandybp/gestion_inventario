"""RBAC en la API JSON (Fase C): un operador no gestiona inventario, pero puede vender.

Valida que `require_admin_or_owner_api` protege los endpoints sensibles y que
el cajero (operator) sigue pudiendo hacer ventas.
"""

from __future__ import annotations

import json

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


def test_operator_cannot_adjust_stock_via_api(live_server: str, page: Page) -> None:
    """RBAC: el operador recibe 403 en POST /movements/adjustment."""
    _login(page, live_server, "operator", "operator")
    resp = page.request.post(
        f"{live_server}/movements/adjustment",
        data={"sku": "E2E-SKU1", "quantity": 1, "location_code": "CENTRAL"},
    )
    assert resp.status == 403, f"esperado 403, recibido {resp.status}"


def test_operator_cannot_transfer_via_api(live_server: str, page: Page) -> None:
    """RBAC: el operador recibe 403 en POST /movements/transfer."""
    _login(page, live_server, "operator", "operator")
    resp = page.request.post(
        f"{live_server}/movements/transfer",
        data=json.dumps({
            "from_location_code": "CENTRAL",
            "to_location_code": "POS1",
            "lines": [{"sku": "E2E-SKU1", "quantity": 1}],
        }),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 403, f"esperado 403, recibido {resp.status}"


def test_operator_can_sell(live_server: str, page: Page) -> None:
    """RBAC: el operador (cajero) PUEDE hacer una venta (POST /movements/sale no da 403)."""
    _login(page, live_server, "operator", "operator")
    resp = page.request.post(
        f"{live_server}/movements/sale",
        data=json.dumps({
            "sku": "E2E-SKU1", "quantity": 1, "location_code": "CENTRAL", "unit_price": 2.0,
        }),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status != 403, "el operador debería poder vender (403 = RBAC sobre-restricto)"
