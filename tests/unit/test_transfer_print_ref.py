"""Regresión H-311: imprimir un traspaso por `movement_id` cuando el traspaso tiene nota.

La nota del `transfer_out` es "Transfer A->B ref=TP-…: <nota>". La ref se extraía con un split por
espacios y quedaba "TP-…:" (con ':'), que salía en el título y el "Ref:" de la impresión.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from app.models import InventoryMovement
from app.routers.ui_transfers import transfer_print

REF = "TP-CENTRAL-POS1-260115100000"


def _patch(monkeypatch, business_id: int) -> None:
    from app.routers import ui_context as _uc
    monkeypatch.setattr(_uc.UIContext, "ensure_admin_or_owner", lambda self: None)
    monkeypatch.setattr(_uc, "get_active_business_id", lambda _db, _req, **_kw: business_id)
    cfg = SimpleNamespace(locations=SimpleNamespace(
        central=SimpleNamespace(code="CENTRAL", name="Central"),
        pos=[SimpleNamespace(code="POS1", name="Tienda")],
    ))
    # raising=False: ui_transfers deja de importarla al pasar a ctx.config (fase 5b); UIContext usa get_business_config.
    monkeypatch.setattr("app.routers.ui_transfers.get_active_business_config", lambda _db, _request: cfg, raising=False)
    monkeypatch.setattr("app.routers.ui_context.get_business_config", lambda _db, _bid: cfg)


def test_print_by_movement_id_strips_colon_when_transfer_has_note(
    db_session, business_id, sample_product, monkeypatch
) -> None:
    _patch(monkeypatch, business_id)
    mv = InventoryMovement(
        business_id=business_id, product_id=sample_product.id, type="transfer_out", quantity=-3,
        movement_date=datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
        note=f"Transfer CENTRAL->POS1 ref={REF}: reposición semanal",
    )
    db_session.add(mv)
    db_session.commit()

    response = transfer_print(request=MagicMock(), db=db_session, ref="", movement_id=mv.id)

    assert response.context["ref"] == REF
    body = response.body.decode("utf-8")
    assert f"Ref: {REF}<" in body
    assert f"{REF}:" not in body
