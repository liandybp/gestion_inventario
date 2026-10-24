"""Fase 3 del release 0.5.0: CSS sin estilos inline con CSP ``style-src 'self'``.

Códigos de barras sin ``style`` (y con la geometría de JsBarcode), htmx sin hoja inyectada ni ``style`` en el settle,
colores por tokens y la guarda ``js_errors`` ante un estilo inline bloqueado. El cambio de tema sin recarga está en
``test_js_behaviors_e2e.py::test_theme_toggle_without_reload``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from tests.e2e.test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

HAS_STYLE = "(svg) => svg.hasAttribute('style') || svg.querySelector('[style]') !== null"

# Dibuja el mismo código con JsBarcode (SVG) y con renderBarcodeSvg y compara la geometría. JsBarcode intenta
# escribir style="…" (la CSP lo bloquea y avisa en consola): el test vacía js_errors al final.
GEOMETRY_JS = """async () => {
  const { renderBarcodeSvg } = await import('/static/js/components/barcode-svg.js');
  const NS = 'http://www.w3.org/2000/svg';
  const dump = (svg) => JSON.stringify([['width', 'height', 'viewBox'].map((a) => svg.getAttribute(a)),
    [...svg.querySelectorAll('rect, g, text')].map((n) => [n.tagName,
      ...['x', 'y', 'width', 'height', 'transform', 'text-anchor'].map((a) => n.getAttribute(a)),
      n.tagName === 'text' ? n.textContent : ''])]);
  const opts = [
    { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 },
    { format: 'CODE128', displayValue: true, fontSize: 12, textMargin: 0, height: 40, margin: 0, width: 1.2 },
    { format: 'CODE128', displayValue: true, fontSize: 40, textMargin: 0, height: 40, margin: 0, width: 1 },
  ];
  const bad = [];
  for (const code of ['TOUR-1', 'SKU000502', 'A', 'abcdefghijklmnopqrstuvwxyz0123456789']) {
    for (const o of opts) {
      const a = document.createElementNS(NS, 'svg');
      const b = document.createElementNS(NS, 'svg');
      window.JsBarcode(a, code, Object.assign({}, o));
      renderBarcodeSvg(b, code, Object.assign({}, o));
      if (dump(a) !== dump(b)) bad.push([code, JSON.stringify(o), dump(a), dump(b)]);
    }
  }
  return bad;
}"""


def test_dashboard_has_no_style_elements_and_indicator_is_hidden(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    assert page.evaluate("document.querySelectorAll('style').length") == 0
    open_tab(page, "transfers")
    expect(page.locator("#transfer-loading")).to_have_css("opacity", "0")
    assert js_errors == []


def test_reopening_home_does_not_settle_style_attributes(tour_server, page: Page, js_errors: list[str]) -> None:
    # Chart.js pone style (por CSSOM) en los <canvas> de Inicio; el settle de htmx lo copiaba con setAttribute.
    base_url, _ = tour_server
    login(page, base_url)
    page.wait_for_function("() => window.Chart && window.Chart.getChart(document.getElementById('monthly-chart'))")
    open_tab(page, "home")
    open_tab(page, "home")
    assert js_errors == []


def test_purchase_history_barcode_renders_without_inline_styles(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "purchases")
    svg = page.locator('svg.barcode[data-code="TOUR-1"]').first
    expect(svg).to_have_attribute("data-rendered", "1")
    assert svg.locator("g rect").count() > 0
    assert svg.evaluate(HAS_STYLE) is False
    # Fondo blanco y barras negras como atributos de presentación (antes style="fill:…").
    expect(svg.locator(":scope > rect")).to_have_attribute("fill", "#ffffff")
    expect(svg.locator("g")).to_have_attribute("fill", "#000000")
    assert js_errors == []


def test_label_barcode_has_text_and_no_inline_styles(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    login(page, base_url)
    page.goto(f"{base_url}/ui/purchase/{ids['purchase']}/label")
    svg = page.locator("#sheet svg.barcode").first
    expect(svg.locator("text")).to_have_text("TOUR-1")
    expect(svg.locator("text")).to_have_attribute("font-size", "12px")
    expect(svg.locator("text")).to_have_attribute("font-family", "monospace")
    assert svg.evaluate(HAS_STYLE) is False
    assert js_errors == []


def test_barcode_geometry_matches_jsbarcode(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    assert page.evaluate(GEOMETRY_JS) == []
    js_errors.clear()  # avisos CSP provocados a propósito por el renderer SVG de JsBarcode


@pytest.mark.parametrize("theme, border", [("light", "rgb(229, 231, 235)"), ("dark", "rgb(51, 65, 85)")])
def test_thumbnail_border_follows_theme(tour_server, page: Page, js_errors: list[str], theme, border) -> None:
    base_url, _ = tour_server
    page.add_init_script(f"localStorage.setItem('gi-theme', '{theme}')")
    login(page, base_url)
    got = page.evaluate("""() => { const img = document.createElement('img'); img.className = 'thumb';
        document.body.appendChild(img); const c = getComputedStyle(img).borderTopColor; img.remove(); return c; }""")
    assert got == border
    assert js_errors == []


def test_home_monthly_chart_colors_come_from_css(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    page.wait_for_function("() => window.Chart && window.Chart.getChart(document.getElementById('monthly-chart'))")
    colors = page.evaluate(
        "window.Chart.getChart(document.getElementById('monthly-chart')).data.datasets.map((d) => d.backgroundColor)")
    assert colors == ["rgba(11,45,66,0.85)", "rgba(203, 58, 66, 0.75)", "rgba(16, 185, 129, 0.75)"]
    assert js_errors == []


def test_js_errors_guard_reports_blocked_inline_style(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    page.evaluate("document.body.setAttribute('style', 'color: red')")
    page.wait_for_timeout(200)
    assert any("Content Security Policy" in e for e in js_errors), js_errors
    assert page.evaluate("getComputedStyle(document.body).color") != "rgb(255, 0, 0)"
    js_errors.clear()  # el teardown del fixture fallaría con el error provocado a propósito
