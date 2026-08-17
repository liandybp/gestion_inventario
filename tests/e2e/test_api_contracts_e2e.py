"""Contract tests de la API JSON (Fase D): RBAC + schema por endpoint + error paths.

Valida contra el live_server (SQLite temporal + seed CENTRAL/E2E-SKU1..2) usando
`page.request` (lleva la sesión del usuario logueado):

- admin: acceso completo a purchase/sale/stock/products, campos del schema.
- operator: 403 en endpoints sensibles (adjustment/transfer/products POST), no en
  sale ni en GETs.
- error paths: payload inválido → 4xx (nunca 5xx).
"""

from __future__ import annotations

import json

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page

pytestmark = pytest.mark.e2e

ADMIN = ("admin", "admin")
OPERATOR = ("operator", "operator")


def _login(page: Page, live_server: str, user: tuple[str, str]) -> None:
    u, p = user
    page.goto(f"{live_server}/ui/login")
    page.fill('input[name="username"]', u)
    page.fill('input[name="password"]', p)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{live_server}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")


def _post_json(page: Page, live_server: str, path: str, payload: dict):
    return page.request.post(
        f"{live_server}{path}",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


def test_admin_rbac_and_schemas(live_server: str, page: Page) -> None:
    """Admin accede a todo; los endpoints devuelven los campos esperados (no 5xx)."""
    _login(page, live_server, ADMIN)

    # purchase → MovementResult {movement, stock_after, ...}
    r = _post_json(page, live_server, "/movements/purchase", {
        "sku": "E2E-SKU1", "quantity": 2, "unit_cost": 1.0,
        "location_code": "CENTRAL",
    })
    assert r.status in (200, 201), f"purchase admin: {r.status}"
    body = r.json()
    assert "movement" in body, f"schema purchase: {body}"
    assert "stock_after" in body, f"schema purchase stock_after: {body}"

    # sale → MovementResult
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 1, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert r.status in (200, 201), f"sale admin: {r.status}"
    assert "movement" in r.json(), f"schema sale: {r.json()}"

    # adjustment → MovementResult
    r = _post_json(page, live_server, "/movements/adjustment", {
        "sku": "E2E-SKU1", "quantity_delta": 1, "unit_cost": 1.0, "location_code": "CENTRAL",
    })
    assert r.status in (200, 201), f"adjustment admin: {r.status}"
    assert "movement" in r.json(), f"schema adjustment: {r.json()}"

    # stock list → list[StockRead]
    r = page.request.get(f"{live_server}/stock")
    assert r.status == 200, f"stock admin: {r.status}"
    rows = r.json()
    assert isinstance(rows, list), "stock debe ser lista"
    if rows:
        assert "sku" in rows[0] and "quantity" in rows[0], f"schema stock: {rows[0]}"

    # stock/{sku} → StockRead
    r = page.request.get(f"{live_server}/stock/E2E-SKU1")
    assert r.status == 200, f"stock/{sku} admin: {r.status}"
    one = r.json()
    assert one["sku"] == "E2E-SKU1", f"schema stock/sku: {one}"

    # products list → list[ProductRead]
    r = page.request.get(f"{live_server}/products")
    assert r.status == 200, f"products admin: {r.status}"


def test_operator_rbac_on_sensitive_endpoints(live_server: str, page: Page) -> None:
    """Operator → 403 en ajuste/traspaso/producto; NO 403 en sale/stock/products GET."""
    _login(page, live_server, OPERATOR)

    for path, payload in [
        ("/movements/adjustment", {"sku": "E2E-SKU1", "quantity_delta": 1, "unit_cost": 1.0, "location_code": "CENTRAL"}),
        ("/movements/transfer", {"from_location_code": "CENTRAL", "to_location_code": "POS1",
                                 "lines": [{"sku": "E2E-SKU1", "quantity": 1}]}),
        ("/products", {"sku": "X", "name": "X", "default_sale_price": 1.0}),
    ]:
        r = _post_json(page, live_server, path, payload)
        assert r.status == 403, f"{path}: esperado 403, recibido {r.status}"

    # sale y GET → no 403
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 1, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert r.status != 403, "operator debería vender"
    assert page.request.get(f"{live_server}/stock").status != 403
    assert page.request.get(f"{live_server}/products").status != 403


def test_api_error_paths_return_4xx(live_server: str, page: Page) -> None:
    """Payloads inválidos devuelven 4xx (no 5xx)."""
    _login(page, live_server, ADMIN)

    # SKU inexistente → 4xx
    r = _post_json(page, live_server, "/movements/adjustment", {
        "sku": "NO_EXISTE", "quantity_delta": 1, "unit_cost": 1.0, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"SKU inexistente: {r.status}"

    # cantidad 0 → 4xx (validation quantity > 0)
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 0, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"cantidad 0: {r.status}"

    # stock insuficiente → 4xx
    r = _post_json(page, live_server, "/movements/sale", {
        "sku": "E2E-SKU1", "quantity": 9999, "unit_price": 2.0, "location_code": "CENTRAL",
    })
    assert 400 <= r.status < 500, f"stock insuficiente: {r.status}"