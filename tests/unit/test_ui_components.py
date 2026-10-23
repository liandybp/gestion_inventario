"""Fase 1 del release 0.5.0: layouts (base/app_shell/print) y macros modal/field.

Cubre los riesgos que encontró la verificación del prototipo descartado:
- el cuerpo del modal va por caller() y se autoescapa (nada de HTML concatenado + |safe);
- las páginas de impresión y el login no heredan los assets de la app.
"""

from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.routers.ui_common import templates

APP_ASSETS = ("htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "app.js")
COMPONENTS_DIR = Path(__file__).resolve().parents[2] / "app" / "templates" / "components"


def _render(source: str, **ctx) -> str:
    return templates.env.from_string(source).render(**ctx)


def test_modal_body_is_markup_and_values_are_escaped() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('x-modal', title) %}<form><input value=\"{{ name }}\" /></form>{% endcall %}",
        title="<b>T</b>",
        name='"><script>alert(1)</script>',
    )
    assert '<div class="modal-overlay" id="x-modal">' in html
    assert "<form><input" in html  # el cuerpo es HTML vivo, no texto escapado
    assert "&lt;b&gt;T&lt;/b&gt;" in html  # título autoescapado
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert 'data-modal-close="x-modal"' in html


def test_modal_title_block_keeps_markup_and_escapes_variables() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% set t %}Editar <code>{{ code }}</code>{% endset %}"
        "{% call modal('d-modal', t, wide=True) %}x{% endcall %}",
        code="<i>",
    )
    assert "Editar <code>&lt;i&gt;</code>" in html
    assert 'class="modal-container modal-container-wide"' in html


def test_modal_static_has_no_close_button() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('s-modal', 'S', static=True) %}x{% endcall %}"
    )
    assert 'data-modal-static="1"' in html
    assert "modal-close" not in html


def test_modal_h2_heading_and_close_label() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('b-modal', 'B', heading='h2', close_label='Cerrar') %}x{% endcall %}"
    )
    assert "<h2>B</h2>" in html
    assert 'aria-label="Cerrar"' in html


def test_field_escapes_value_and_renders_flags() -> None:
    html = _render(
        "{% from 'components/forms.html' import field %}"
        "{{ field('Nombre', 'name', value=v, required=True, readonly=True, attrs={'list': 'opts'}) }}",
        v='"><script>x</script>',
    )
    assert "<script>" not in html
    assert "&#34;&gt;&lt;script&gt;" in html
    assert 'name="name"' in html and 'list="opts"' in html
    assert " required" in html and " readonly" in html


def test_field_rejects_invalid_attrs_key() -> None:
    # H-304: antes del bucle manual, una clave con espacios se volcaba literal al HTML
    # (`x onfocus=alert(1) autofocus`), abriendo paso a atributos arbitrarios si `attrs`
    # alguna vez viniera de datos. `xmlattr` la rechaza en vez de volcarla.
    with pytest.raises(ValueError, match="Invalid character in attribute name"):
        _render(
            "{% from 'components/forms.html' import field %}"
            "{{ field('L', 'n', attrs={'x onfocus=alert(1) autofocus': 'v'}) }}"
        )


def test_components_never_use_safe_or_markup() -> None:
    # Regla dura de design.md D1/§2.5: ninguna macro de components/ puede reintroducir el
    # bug del prototipo (HTML concatenado + `|safe`/`Markup(...)` reabre el XSS que la
    # fase 0 cerró).
    # Regex, no literal: Jinja acepta `{{ x | safe }}` con espacios (H-310).
    unsafe = re.compile(r"\|\s*safe\b|Markup\s*\(")
    offenders = [
        path.name
        for path in sorted(COMPONENTS_DIR.glob("*.html"))
        if unsafe.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_field_value_none_omits_attribute_and_id_links_label() -> None:
    html = _render(
        "{% from 'components/forms.html' import field %}"
        "{{ field('Clave', 'pw', type='password', id='pw-id') }}"
    )
    assert "value=" not in html
    assert '<label for="pw-id">Clave</label>' in html
    assert 'id="pw-id"' in html


@pytest.mark.parametrize("name", ["base.html", "layouts/print.html"])
def test_base_and_print_layout_load_no_app_assets(name: str) -> None:
    html = templates.env.get_template(name).render()
    for asset in APP_ASSETS:
        assert asset not in html, f"{name} carga {asset}"


def test_app_shell_loads_all_app_assets() -> None:
    html = templates.env.get_template("layouts/app_shell.html").render(static_version=lambda: "1")
    for asset in APP_ASSETS:
        assert asset in html


@pytest.mark.parametrize(
    "name, parent",
    [
        ("dashboard.html", "layouts/app_shell.html"),
        ("login.html", "base.html"),
        ("transfer_print.html", "layouts/print.html"),
        ("restock_print.html", "layouts/print.html"),
        ("label_single_print.html", "layouts/print.html"),
    ],
)
def test_pages_extend_expected_layout(name: str, parent: str) -> None:
    source, _, _ = templates.env.loader.get_source(templates.env, name)
    assert source.startswith('{% extends "' + parent + '" %}')


@pytest.mark.parametrize(
    "kind, modal_id, editable_field",
    [
        ("sale", "profit-items-sale-edit-modal", "unit_price"),
        ("purchase", "profit-items-purchase-edit-modal", "lot_code"),
        ("adjustment", "profit-items-adjustment-edit-modal", "unit_cost"),
        ("transfer_in", "profit-items-transfer-in-edit-modal", "unit_cost"),
    ],
)
def test_profit_items_edit_form_renders_each_kind(kind: str, modal_id: str, editable_field: str) -> None:
    movement = SimpleNamespace(id=7, quantity=2.0, unit_cost=1.5, unit_price=3.0, note="n")
    html = templates.env.get_template("partials/profit_items_edit_form.html").render(
        kind=kind, movement=movement, product_label="SKU - Prod", movement_date_value="",
        lot_code="L1", product_options=[],
    )
    assert f'id="{modal_id}"' in html
    assert f'name="{editable_field}"' in html
    slug = kind.replace("_", "-")
    assert f'hx-post="/ui/tabs/profit-items/{slug}/7/update"' in html


@pytest.mark.parametrize("kind", ["sale", "purchase", "adjustment", "transfer_in"])
def test_old_profit_items_templates_are_gone(kind: str) -> None:
    from jinja2 import TemplateNotFound

    with pytest.raises(TemplateNotFound):
        templates.env.get_template(f"partials/profit_items_{kind}_edit_form.html")
