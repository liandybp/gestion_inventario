"""Tests de regresión de bugs de producción corregidos (auditoría 2026-08-15).

Cubren los fixes sin test de la sesión:
- H-060: cache-busting ``static_version`` (tema claro/oscuro + burbuja rotos en prod
  por ``?v=40`` no bump-eado).
- H-025: ``NameError`` en ``_location_name_for_code`` (500 en ``/ui/restock-table``).
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from app.routers import ui_common


class _FakeRequest:
    """Request mínimo: solo necesita ``.session`` para la resolución de negocio."""

    session: dict = {}


def test_static_asset_version_is_callable_and_stable():
    """H-060: el global Jinja ``static_version`` es invocable y devuelve 8 hex chars."""
    version = ui_common.templates.env.globals["static_version"]
    assert callable(version)
    value = version()
    assert isinstance(value, str)
    assert len(value) == 8
    int(value, 16)  # hex válido
    assert value == version()  # estable entre llamadas


def test_static_asset_version_derives_from_file_content():
    """H-060: el hash sale del contenido real de los assets (no una constante)."""
    static_dir = Path(ui_common.__file__).resolve().parent.parent / "static"
    css = (static_dir / "css/app.css").read_bytes()
    js = (static_dir / "js/app.js").read_bytes()
    htmx = (static_dir / "js/htmx.min.js").read_bytes()
    chart = (static_dir / "js/chart.umd.min.js").read_bytes()
    barcode = (static_dir / "js/JsBarcode.all.min.js").read_bytes()
    expected = hashlib.sha1(css + js + htmx + chart + barcode).hexdigest()[:8]
    assert ui_common._static_asset_version() == expected


def test_location_name_for_code_no_nameerror(db_session):
    """H-025: regresión del NameError db/request en ``_location_name_for_code``.

    Con la firma vieja ``(location_code, business_code)`` el cuerpo referenciaba
    ``db``/``request`` fuera de scope → NameError y 500 en ``/ui/restock-table``.
    """
    from app.routers.ui_tabs_common import _location_name_for_code

    result = _location_name_for_code("CENTRAL", db_session, _FakeRequest())
    assert isinstance(result, str)
    assert result  # no vacío
