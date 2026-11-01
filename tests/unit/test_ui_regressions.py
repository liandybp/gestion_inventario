"""Tests de regresión de bugs de producción corregidos (auditoría 2026-08-15).

Cubren los fixes sin test de la sesión:
- H-060: cache-busting ``static_version`` (tema claro/oscuro + burbuja rotos en prod
  por ``?v=40`` no bump-eado).
- H-025: ``NameError`` en ``_location_name_for_code`` (500 en ``/ui/restock-table``).
"""
from __future__ import annotations

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


def test_static_asset_version_changes_with_any_nested_module(tmp_path):
    """H-060 + fase 2: el hash cambia si cambia CUALQUIER .css/.js, también un módulo anidado
    (los que importa main.js no llevan ``?v=`` propio)."""
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_text("body{}")
    (tmp_path / "js" / "features").mkdir(parents=True)
    (tmp_path / "js" / "main.js").write_text("import './features/x.js';")
    module = tmp_path / "js" / "features" / "x.js"
    module.write_text("export const a = 1;")
    before = ui_common._static_asset_version(tmp_path)
    assert before == ui_common._static_asset_version(tmp_path)
    module.write_text("export const a = 2;")
    assert ui_common._static_asset_version(tmp_path) != before
    module.rename(tmp_path / "js" / "features" / "y.js")
    assert ui_common._static_asset_version(tmp_path) != before


def test_location_name_for_code_no_nameerror(db_session):
    """H-025: regresión del NameError db/request en ``_location_name_for_code``.

    Con la firma vieja ``(location_code, business_code)`` el cuerpo referenciaba
    ``db``/``request`` fuera de scope → NameError y 500 en ``/ui/restock-table``.
    """
    from app.routers.ui_tabs_common import _location_name_for_code

    result = _location_name_for_code("CENTRAL", db_session, _FakeRequest())
    assert isinstance(result, str)
    assert result  # no vacío
