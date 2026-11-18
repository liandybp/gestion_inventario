"""Sonda S10: ¿cuándo dispara el autorrelleno (GET /ui/sales-doc/product-defaults) en Documentos?

Se ejecuta desde la raíz de un checkout con el código de la rama (worktree de `cc_pae_check --keep`):
    .venv/bin/python specs/.../design/sondas/S10-autorrelleno-documentos.py
Registra, por cada intento de añadir un producto, si el cambio del campo `product` produjo la petición.
"""
from __future__ import annotations

import shutil
import sys
import time

sys.path.insert(0, ".")
from playwright.sync_api import sync_playwright  # noqa: E402

from tests.e2e.conftest import _boot_temp_server, _seed_stock  # noqa: E402

base, db_path, proc, tmp = _boot_temp_server("s10-")
try:
    _seed_stock(db_path)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        calls: list[str] = []
        page.on("request", lambda r: calls.append(f"{r.method} {r.url.split('/ui/')[-1][:40]}") if "/ui/sales-doc" in r.url else None)
        page.goto(f"{base}/ui/login")
        page.fill('input[name="username"]', "admin")
        page.fill('input[name="password"]', "admin")
        page.click('button[type="submit"]')
        page.wait_for_url(f"{base}/ui/dashboard**")
        page.locator("#tab-content > *").first.wait_for()
        page.click("#tab-documents")
        page.locator("#sales-doc-panel").wait_for()
        for attempt, (sku, price) in enumerate((("E2E-SKU1", "303875"), ("E2E-SKU2", "187000")), start=1):
            calls.clear()
            form = page.locator("form#sales-doc-cart-add-form")
            product = form.locator('input[name="product"]')
            product.fill(sku)
            product.press("Tab")
            time.sleep(1.0)
            print(f"intento {attempt}: tras Tab -> {calls} precio={form.locator('input[name=unit_price]').input_value()}")
            form.locator('input[name="quantity"]').fill("1")
            form.locator('input[name="unit_price"]').fill(price)
            time.sleep(0.5)
            print(f"intento {attempt}: precio antes de enviar={form.locator('input[name=unit_price]').input_value()}")
            form.locator('button[type="submit"]').click()
            time.sleep(1.0)
            print(f"intento {attempt}: tras enviar -> {calls}")
        browser.close()
finally:
    proc.terminate()
    shutil.rmtree(tmp, ignore_errors=True)
