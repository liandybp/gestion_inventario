from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import Location, Product
from app.services.inventory_service import InventoryService


def _create_product(db: Session, business_id: int) -> Product:
    p = Product(
        business_id=business_id,
        sku="SKU-TST",
        name="Producto",
        min_stock=3,
        unit_of_measure="ud",
        default_purchase_cost=1.0,
        default_sale_price=2.0,
        lead_time_days=0,
    )
    db.add(p)
    db.commit()
    db.refresh(p)
    return p


def test_service_requires_business_id(db_session: Session) -> None:
    with pytest.raises(HTTPException) as exc:
        InventoryService(db_session, business_id=None)
    assert exc.value.status_code == 409


def test_transfer_codes_parser_ok(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)

    from_code, to_code, ref = service._transfer_codes_from_out_note("Transfer CENTRAL->POS1 ref=TP-123: nota")

    assert from_code == "CENTRAL"
    assert to_code == "POS1"
    assert ref == "TP-123"


def test_transfer_codes_parser_invalid_note(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)

    with pytest.raises(HTTPException) as exc:
        service._transfer_codes_from_out_note("nota sin formato")

    assert exc.value.status_code == 409


def test_compact_lot_code_truncates_long_values(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)

    code = service._compact_lot_code("X" * 120, max_len=24)

    assert len(code) <= 24
    assert "-" in code


def test_warning_if_restock_needed(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    product = _create_product(db_session, business_id)

    assert service._warning_if_restock_needed(product, 2) == "Needs restock"
    assert service._warning_if_restock_needed(product, 3) is None


def test_location_id_for_existing_code(db_session: Session, business_id: int) -> None:
    service = InventoryService(db_session, business_id=business_id)
    loc = Location(business_id=business_id, code="POS1", name="Punto")
    db_session.add(loc)
    db_session.commit()

    loc_id = service._location_id_for_code("POS1")

    assert loc_id == loc.id

