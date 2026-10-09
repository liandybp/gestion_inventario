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
            default_sale_code=lambda: "POS1",
        )
    )


def test_sale_pos_context_includes_central_and_pos(monkeypatch) -> None:
    monkeypatch.setattr(ui_sales, "get_active_business_config", lambda _db, _request: _mock_config())

    sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "CENTRAL")

    assert [loc["code"] for loc in sale_locations] == ["CENTRAL", "POS1", "POS2"]
    assert default_code == "POS1"
    assert selected_code == "CENTRAL"


def test_sale_pos_context_defaults_to_default_pos(monkeypatch) -> None:
    monkeypatch.setattr(ui_sales, "get_active_business_config", lambda _db, _request: _mock_config())

    _sale_locations, default_code, selected_code = ui_sales._sale_pos_context(None, _DummyRequest(), "")

    assert default_code == "POS1"
    assert selected_code == "POS1"


def test_dt_to_local_input_s_preserves_milliseconds() -> None:
    """Regresión: `dt_to_local_input` (ui_common) solo guarda hasta el minuto. Si el
    form de edición de venta round-tripea la fecha con esa precisión, dos movimientos
    del mismo producto en el mismo minuto pueden invertir su orden cronológico al
    reconstruir el FIFO (`InventoryService._rebuild_product_fifo`), rechazando una
    edición válida con "Insufficient stock" — descubierto por el e2e de
    sale-payment-method. `_dt_to_local_input_s` evita la pérdida de precisión."""
    from datetime import datetime, timezone

    dt = datetime(2026, 10, 9, 12, 28, 19, 123000, tzinfo=timezone.utc)

    assert ui_sales._dt_to_local_input_s(dt) == "2026-10-09T12:28:19.123"

