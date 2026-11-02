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


def _touch_later(path, seconds: int = 5) -> None:
    """Simula una edición posterior: dos escrituras seguidas pueden compartir mtime (reloj de grano grueso)."""
    import os
    st = path.stat()
    os.utime(path, ns=(st.st_atime_ns, st.st_mtime_ns + seconds * 1_000_000_000))


def test_static_asset_version_changes_with_any_nested_module(tmp_path, monkeypatch):
    """H-060 + fase 2: el hash cambia si cambia CUALQUIER .css/.js, también un módulo anidado
    (los que importa main.js no llevan ``?v=`` propio)."""
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_text("body{}")
    (tmp_path / "js" / "features").mkdir(parents=True)
    (tmp_path / "js" / "main.js").write_text("import './features/x.js';")
    module = tmp_path / "js" / "features" / "x.js"
    module.write_text("export const a = 1;")
    monkeypatch.setattr(ui_common, "_STATIC_VERSION_TTL_S", 0)  # este test mide el hash, no el TTL
    before = ui_common._static_asset_version(tmp_path)
    assert before == ui_common._static_asset_version(tmp_path)
    module.write_text("export const a = 2;")
    _touch_later(module)
    assert ui_common._static_asset_version(tmp_path) != before
    module.rename(tmp_path / "js" / "features" / "y.js")
    assert ui_common._static_asset_version(tmp_path) != before


def test_location_name_for_code_no_nameerror(db_session):
    """H-025: regresión del NameError db/request en ``_location_name_for_code``.

    Con la firma vieja ``(location_code, business_code)`` el cuerpo referenciaba
    ``db``/``request`` fuera de scope → NameError y 500 en ``/ui/restock-table``.
    """
    from app.routers.ui_tabs_common import _location_name_for_code
    from app.routers.ui_context import UIContext

    result = _location_name_for_code("CENTRAL", UIContext(request=_FakeRequest(), db=db_session))
    assert isinstance(result, str)
    assert result  # no vacío


def test_static_asset_version_ttl_avoids_disk_and_then_sees_changes(tmp_path, monkeypatch):
    """H-R04: dentro del TTL no se vuelve a leer el disco; pasado el TTL, una edición cambia la versión."""
    (tmp_path / "css").mkdir()
    (tmp_path / "js").mkdir()
    css = tmp_path / "css" / "app.css"
    css.write_text("body{}")
    clock = [1000.0]
    monkeypatch.setattr(ui_common.time, "monotonic", lambda: clock[0])
    first = ui_common._static_asset_version(tmp_path)
    css.write_text("body{color:red}")
    _touch_later(css)
    clock[0] += 0.5
    assert ui_common._static_asset_version(tmp_path) == first  # dentro del TTL: versión cacheada
    clock[0] += 1.0
    assert ui_common._static_asset_version(tmp_path) != first  # pasado el TTL: ve la edición

