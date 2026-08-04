from __future__ import annotations

from types import SimpleNamespace

from app.routers import ui_sales


class _DummyRequest:
    pass


def _mock_config() -> SimpleNamespace:
    return SimpleNamespace(
        locations=SimpleNamespace(
            central=SimpleNamespace(code="CENTRAL", name="Almacen central"),
            pos=[SimpleNamespace(code="POS1", name="Tienda 1"), SimpleNamespace(code="POS2", name="Tienda 2")],
            default_pos="POS1",
        )
    )


def test_sale_pos_context_includes_central_and_pos(monkeypatch) -> None:
    monkeypatch.setattr(ui_sales, "get_active_business_code", lambda db, request: "test")
    monkeypatch.setattr(ui_sales, "load_business_config", lambda _code: _mock_config())

    sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "CENTRAL")

    assert [loc["code"] for loc in sale_locations] == ["CENTRAL", "POS1", "POS2"]
    assert default_code == "POS1"
    assert selected_code == "CENTRAL"


def test_sale_pos_context_defaults_to_default_pos(monkeypatch) -> None:
    monkeypatch.setattr(ui_sales, "get_active_business_code", lambda db, request: "test")
    monkeypatch.setattr(ui_sales, "load_business_config", lambda _code: _mock_config())

    _sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "")

    assert default_code == "POS1"
    assert selected_code == "POS1"

