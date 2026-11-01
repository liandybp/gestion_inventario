"""Fase 2 del release 0.5.0: cada comportamiento que vivía en un onclick=/onchange=/onkeydown= o
en un <script> inline sigue funcionando desde los módulos de app/static/js/. Todos terminan sin
errores de JS ni violaciones de CSP (fixture `js_errors`)."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from app.routers.ui_common import templates
from tests.e2e.conftest import TOUR_PRODUCT_NAME, TOUR_WIDGET_TITLE
from tests.e2e.test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

# Con script-src 'self', wait_for_function("expresión") hace eval() en cada sondeo y el CSP lo
# bloquea: las esperas usan siempre la forma de función ("(arg) => ...", arg=...).
# window.print() abriría el diálogo: se cuenta en vez de imprimir.
PRINT_STUB = "window.__printed = 0; window.print = () => { window.__printed += 1; };"
CHART_EXISTS = "(id) => !!Chart.getChart(document.getElementById(id))"


@pytest.mark.parametrize(
    "tab, name",
    [
        ("inventory", "skus"),
        ("purchases", "movement_ids"),
        ("transfers", "movement_ids"),
        ("sales", "sale_ids"),
        ("profit-items", "sale_ids"),
        ("history", "movement_ids"),
    ],
)
def test_select_all_checkbox(tour_server, page: Page, js_errors: list[str], tab: str, name: str) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, tab)
    master = page.locator(f'#tab-content [data-js-select-all="{name}"]').first
    form = master.locator("xpath=ancestor::form[1]")
    boxes = form.locator(f'input[name="{name}"]')
    assert boxes.count() > 0
    master.check()
    for i in range(boxes.count()):
        expect(boxes.nth(i)).to_be_checked()
    master.uncheck()
    for i in range(boxes.count()):
        expect(boxes.nth(i)).not_to_be_checked()
    assert js_errors == []


def test_theme_toggle_without_reload(tour_server, page: Page, js_errors: list[str]) -> None:
    # Fase 3: el tema cambia sin recargar (el estado de la página se conserva), los gráficos se
    # redibujan con la paleta del tema (variables --chart-* de app.css) y la elección persiste.
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "agent")
    first_color = ("window.Chart.getChart(document.querySelector('canvas[data-js-widget-chart]'))"
                   ".data.datasets[0].backgroundColor[0]")
    page.wait_for_function(f"() => !!window.Chart && !!({first_color})")
    assert page.evaluate(first_color) == "#0b2d42"
    page.evaluate("window.__noReload = 1")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    assert page.evaluate("window.__noReload") == 1
    assert page.evaluate("localStorage.getItem('gi-theme')") == "dark"
    page.wait_for_function(f"() => ({first_color}) === '#60a5fa'")
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    assert js_errors == []


def test_home_charts_follow_dark_theme(tour_server, page: Page, js_errors: list[str]) -> None:
    # En oscuro la serie de ventas, el texto de los ejes y la rejilla cambian (antes navy sobre fondo
    # oscuro y textos #666); en claro siguen los valores de siempre.
    base_url, _ = tour_server
    login(page, base_url)
    chart = "window.Chart.getChart(document.getElementById('monthly-chart'))"
    page.wait_for_function(f"() => window.Chart && !!{chart}")
    state = (f"() => {{ const c = {chart}; return {{sales: c.data.datasets[0].backgroundColor,"
             f" ticks: c.options.scales.x.ticks.color, grid: c.options.scales.y.grid.color}}; }}")
    light = {"sales": "rgba(11,45,66,0.85)", "ticks": "#666", "grid": "rgba(0, 0, 0, 0.1)"}
    assert page.evaluate(state) == light
    page.click("[data-js-theme-toggle]")
    page.wait_for_function(f"() => {chart}.data.datasets[0].backgroundColor === 'rgba(96, 165, 250, 0.85)'")
    assert page.evaluate(state) == {"sales": "rgba(96, 165, 250, 0.85)", "ticks": "#cbd5e1",
                                    "grid": "rgba(148, 163, 184, 0.2)"}
    page.click("[data-js-theme-toggle]")
    page.wait_for_function(f"() => {chart}.data.datasets[0].backgroundColor === 'rgba(11,45,66,0.85)'")
    assert page.evaluate(state) == light
    assert js_errors == []


def test_escape_closes_top_modal_except_static(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "inventory")
    page.click('#tab-content [hx-get$="/edit-inventory"] >> nth=0')
    modal = page.locator("#product-inventory-edit-modal")
    expect(modal).to_be_visible()
    page.keyboard.press("Escape")
    expect(modal).to_have_count(0)
    # Un modal estático (cambio de contraseña obligatorio) no se cierra con Escape.
    page.evaluate("""() => document.body.insertAdjacentHTML('beforeend',
        '<div class="modal-overlay" id="static-modal" data-modal-static="1"><div class="modal-container"></div></div>')""")
    page.keyboard.press("Escape")
    expect(page.locator("#static-modal")).to_have_count(1)
    assert js_errors == []


def test_business_select_submits_on_change(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    with page.expect_request(lambda r: r.url.endswith("/ui/active-business") and r.method == "POST") as req:
        page.dispatch_event('select[name="business_id"]', "change")
    assert re.search(r"business_id=\d+", req.value.post_data or "")
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    assert js_errors == []


def test_sale_product_enter_moves_focus_to_quantity(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "sales")
    product = page.locator('#sale-panel input[name="product"][data-js-enter-focus="quantity"]')
    product.fill("TOUR-1")
    product.press("Enter")
    focused = page.evaluate("document.activeElement.name")
    assert focused == "quantity"
    expect(page.locator("#sale-panel")).not_to_contain_text("Venta registrada")
    assert js_errors == []


def test_sales_doc_currency_is_mirrored_to_issue_form(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "documents")
    page.select_option("#sales-doc-currency-code", "USD")
    expect(page.locator("#sales-doc-currency-hidden")).to_have_value("USD")
    page.select_option("#sales-doc-currency-code", "GEN")
    expect(page.locator("#sales-doc-currency-hidden")).to_have_value("GEN")
    assert js_errors == []


def test_home_charts_render_and_pie_tooltip_escapes_label(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "home")
    for chart_id in ("monthly-chart", "monthly-sales-daily-line", "monthly-sales-pie"):
        page.wait_for_function(CHART_EXISTS, arg=chart_id)
    # El primer artículo del pastel es TOUR-1 (el de más venta), con nombre hostil.
    page.evaluate("""() => {
        const chart = Chart.getChart(document.getElementById('monthly-sales-pie'));
        const arc = chart.getDatasetMeta(0).data[0];
        const pos = arc.tooltipPosition();
        chart.tooltip.setActiveElements([{datasetIndex: 0, index: 0}], pos);
        chart.update();
    }""")
    tooltip = page.locator("#monthly-sales-pie-tooltip")
    expect(tooltip).to_contain_text("Cantidad vendida")
    expect(tooltip).to_contain_text(TOUR_PRODUCT_NAME)
    assert tooltip.locator("b").count() == 0
    assert js_errors == []


def test_agent_panel_and_seeded_widget_chart(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "agent")
    canvas_id = page.locator("#agent-grid canvas[data-js-widget-chart]").first.get_attribute("id")
    page.wait_for_function(CHART_EXISTS, arg=canvas_id)
    label = page.evaluate(f"Chart.getChart(document.getElementById('{canvas_id}')).data.datasets[0].label")
    assert label == TOUR_WIDGET_TITLE

    panel = page.locator("#agent-panel")
    expect(page.locator("#agent-fab")).to_be_visible()
    page.click("#agent-fab")
    expect(panel).not_to_have_class(re.compile(r"\bhidden\b"))
    page.click("#agent-panel [data-js-agent-size]")
    expect(panel).to_have_class(re.compile(r"\blarge\b"))
    page.click("#agent-panel .agent-panel-close")
    expect(panel).to_have_class(re.compile(r"\bhidden\b"))
    open_tab(page, "sales")
    expect(page.locator("#agent-fab")).to_be_hidden()
    assert js_errors == []


def test_agent_chat_turn_renders_oob_widget_chart(tour_server, page: Page, js_errors: list[str]) -> None:
    """Los widgets nuevos llegan en un swap OOB (#agent-grid), no en el target del chat
    (#agent-chat-thread): el enhancer debe recibir también los elementos OOB. La respuesta del
    chat (que llamaría al LLM) se sustituye por la plantilla real con un widget de barras."""
    base_url, _ = tour_server
    widget = {"id": 9999, "type": "bar", "title": "Ventas OOB", "payload": {"labels": ["x", "y"], "values": [3, 4]}}
    body = templates.env.get_template("partials/agent_chat_turn.html").render(
        user_content="hola", reply="Respuesta e2e", widgets=[widget], currency_symbol="€"
    )
    page.route("**/ui/agent/chat", lambda route: route.fulfill(status=200, content_type="text/html", body=body))
    login(page, base_url)
    open_tab(page, "agent")
    page.click("#agent-fab")
    page.fill("#agent-chat-form textarea[name=message]", "hola copiloto")
    page.press("#agent-chat-form textarea[name=message]", "Enter")
    thread = page.locator("#agent-chat-thread")
    expect(thread.locator(".msg-user").last).to_have_text("hola copiloto")
    expect(thread).to_contain_text("Respuesta e2e")
    expect(page.locator("#agent-typing")).to_have_count(0)
    expect(page.locator("#agent-chat-form textarea[name=message]")).to_have_value("")
    page.wait_for_function(CHART_EXISTS, arg="chart-9999")
    assert js_errors == []


def test_print_buttons_and_label_copies(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    page.add_init_script(PRINT_STUB)
    login(page, base_url)
    for path in ("/ui/restock-print", f"/ui/transfers/print?ref={ids['transfer_ref']}"):
        page.goto(f"{base_url}{path}")
        page.click("[data-js-print]")
        assert page.evaluate("window.__printed") == 1, path

    page.goto(f"{base_url}/ui/purchase/{ids['purchase']}/label?auto_print=1")
    page.wait_for_function("(n) => window.__printed === n", arg=1)
    expect(page.locator("#sheet .name").first).to_have_text(TOUR_PRODUCT_NAME)
    page.fill("#copies", "3")
    page.dispatch_event("#copies", "change")
    expect(page.locator("#sheet svg.barcode")).to_have_count(3)
    expect(page.locator("#sheet svg.barcode rect").first).to_be_attached()
    page.click("#btn-print")
    page.wait_for_function("(n) => window.__printed === n", arg=2)
    assert js_errors == []


def test_login_show_password(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    page.goto(f"{base_url}/ui/login")
    pw = page.locator("#login-password")
    expect(pw).to_have_attribute("type", "password")
    page.check("#login-show-password")
    expect(pw).to_have_attribute("type", "text")
    page.uncheck("#login-show-password")
    expect(pw).to_have_attribute("type", "password")
    assert js_errors == []


def test_mobile_menu_toggle(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    page.set_viewport_size({"width": 400, "height": 800})
    login(page, base_url)
    page.click("#mobile-menu-toggle")
    expect(page.locator("#sidebar")).to_have_class(re.compile(r"\bactive\b"))
    page.click("#tab-sales")
    expect(page.locator("#sidebar")).not_to_have_class(re.compile(r"\bactive\b"))
    assert js_errors == []
