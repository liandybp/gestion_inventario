"""H-709: filtrar "Utilidades por artículo" con fecha de fin daba NameError (faltaba importar timedelta)."""

from __future__ import annotations

from unittest.mock import MagicMock

from app.routers import ui_context as _uc
from app.routers import ui_profit_items


def test_profit_items_tab_with_date_range_renders(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)
    # raising=False: ui_profit_items deja de importar estas dos funciones cuando pasa a UIContext (fase 5b, T21).
    monkeypatch.setattr(ui_profit_items, "require_active_business_id", lambda _db, _req: business_id, raising=False)
    monkeypatch.setattr(ui_profit_items, "get_current_user_from_session", lambda _db, _req: None, raising=False)
    monkeypatch.setattr(_uc, "get_active_business_id", lambda _db, _req, **_kw: business_id)
    monkeypatch.setattr(_uc, "get_current_user_from_session", lambda _db, _req: None)
    monkeypatch.setattr(ui_profit_items, "_home_locations_context", lambda _ctx: ([], ""))
    request = MagicMock()
    request.session = {}

    response = ui_profit_items.tab_profit_items(
        request, db=db_session, start_date="2026-10-01", end_date="2026-10-31"
    )

    assert response.status_code == 200
    assert response.context["filter_end_date"] == "2026-10-31"
