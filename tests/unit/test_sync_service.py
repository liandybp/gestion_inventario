from __future__ import annotations

import pytest
from fastapi import HTTPException
from sqlalchemy import select

from app.models import (
    AppliedSyncEvent,
    Business,
    Customer,
    DeviceToken,
    InventoryLot,
    InventoryMovement,
    Location,
    MovementAllocation,
    Product,
    SalesDocument,
    SalesDocumentItem,
    SyncConflict,
    User,
)
from app.sync.service import SyncService


def _device(db, business_id, *, location_id=None) -> DeviceToken:
    d = DeviceToken(token=f"t-{business_id}-{location_id}", business_id=business_id,
                     location_id=location_id, label="POS test")
    db.add(d)
    db.commit()
    db.refresh(d)
    return d


def test_version_too_old_raises_409(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    with pytest.raises(HTTPException) as ei:
        svc.push(app_schema_version=1, events=[])
    assert ei.value.status_code == 409


def test_customer_created_when_no_clash(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e1", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-1", "name": "Ana", "phone": "600111222"}}
    res = svc.push(6, [ev])
    assert "e1" in res["applied_event_ids"]
    assert db_session.query(Customer).filter(Customer.client_id == "C-1").count() == 1


def test_customer_dedup_keeps_first_by_contact(db_session, business_id):
    db_session.add(Customer(business_id=business_id, client_id="C-1", name="Ana", phone="600111222"))
    db_session.commit()
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e2", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-2", "name": "Ana B", "phone": "600111222"}}
    svc.push(6, [ev])
    # No se crea un segundo cliente: se conserva el primero por contacto
    assert db_session.query(Customer).filter(Customer.phone == "600111222").count() == 1


def test_idempotent_replay_does_not_duplicate(db_session, business_id):
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "e3", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-3", "name": "Leo", "email": "leo@x.com"}}
    svc.push(6, [ev])
    svc.push(6, [ev])  # reenvío del mismo event_id
    assert db_session.query(Customer).filter(Customer.client_id == "C-3").count() == 1


def test_sale_unknown_sku_registers_product_not_found_conflict(db_session, business_id):
    # H-006: un SKU inexistente (404) ya no se confunde con falta de stock (409).
    # CENTRAL debe existir para que la resolución de ubicación (H-005) no
    # intercepte el evento antes de llegar al chequeo de producto.
    db_session.add(Location(business_id=business_id, code="CENTRAL", name="Central", role="central"))
    db_session.commit()
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "s1", "event_type": "sale", "seq": 1,
          "payload": {"sku": "NOEXIST", "quantity": 1, "location_code": "CENTRAL"}}
    res = svc.push(6, [ev])
    assert "s1" not in res["applied_event_ids"]
    conflicts = db_session.query(SyncConflict).all()
    assert len(conflicts) == 1
    assert conflicts[0].kind == "sale_product_not_found"
    assert conflicts[0].event_id == "s1"


def test_sale_unknown_location_registers_conflict_without_auto_creating(db_session, business_id):
    # H-005 endurecido: un location_code que no resuelve nunca debe llegar a
    # InventoryService.sale() (que auto-crearía la ubicación fantasma) — se
    # rechaza como conflicto explícito, con o sin device scoped.
    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "s4", "event_type": "sale", "seq": 1,
          "payload": {"sku": "SKU000003", "quantity": 1, "location_code": "TYPO-CODE"}}
    res = svc.push(6, [ev])
    assert "s4" not in res["applied_event_ids"]
    conflicts = db_session.query(SyncConflict).filter(SyncConflict.event_id == "s4").all()
    assert len(conflicts) == 1
    assert conflicts[0].kind == "unknown_location"
    assert db_session.query(Location).filter(Location.code == "TYPO-CODE").count() == 0


def test_sale_out_of_stock_registers_sale_stock_conflict(db_session, business_id):
    # H-006: la causa real de falta de stock (409) sí se etiqueta sale_stock.
    biz = db_session.get(Business, business_id)
    central = Location(business_id=business_id, code="CENTRAL", name="Central", role="central")
    db_session.add(central)
    db_session.flush()
    product = Product(business_id=business_id, sku="SKU000001", name="Prod", min_stock=0,
                       default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db_session.add(product)
    db_session.commit()

    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "s2", "event_type": "sale", "seq": 1,
          "payload": {"sku": "SKU000001", "quantity": 5, "location_code": "CENTRAL"}}
    res = svc.push(6, [ev])
    assert "s2" not in res["applied_event_ids"]
    conflicts = db_session.query(SyncConflict).filter(SyncConflict.event_id == "s2").all()
    assert len(conflicts) == 1
    assert conflicts[0].kind == "sale_stock"


def test_apply_sale_propagates_paid_by_transfer_from_payload(db_session, business_id):
    central = Location(business_id=business_id, code="CENTRAL", name="Central", role="central")
    db_session.add(central)
    db_session.flush()
    product = Product(business_id=business_id, sku="SKU000002", name="Prod", min_stock=0,
                       default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db_session.add(product)
    db_session.flush()
    mv = InventoryMovement(business_id=business_id, product_id=product.id,
                            location_id=central.id, type="purchase", quantity=5, unit_cost=1.0)
    db_session.add(mv)
    db_session.flush()
    lot = InventoryLot(business_id=business_id, movement_id=mv.id, product_id=product.id,
                        location_id=central.id, lot_code="LOT-1",
                        received_at=mv.movement_date, unit_cost=1.0, qty_received=5, qty_remaining=5)
    db_session.add(lot)
    db_session.commit()

    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "s5", "event_type": "sale", "seq": 1,
          "payload": {"sku": "SKU000002", "quantity": 2, "location_code": "CENTRAL",
                      "paid_by_transfer": True}}
    res = svc.push(6, [ev])

    assert "s5" in res["applied_event_ids"]
    applied_mv = db_session.query(InventoryMovement).filter(
        InventoryMovement.business_id == business_id, InventoryMovement.type == "sale"
    ).one()
    assert applied_mv.paid_by_transfer is True


def test_sale_location_mismatch_registers_conflict_without_touching_stock(db_session, business_id):
    # H-005: un device scoped a una ubicación no puede vender contra otra.
    central = Location(business_id=business_id, code="CENTRAL", name="Central", role="central")
    pos1 = Location(business_id=business_id, code="POS1", name="POS 1", role="pos")
    db_session.add_all([central, pos1])
    db_session.flush()
    product = Product(business_id=business_id, sku="SKU000002", name="Prod2", min_stock=0,
                       default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db_session.add(product)
    db_session.commit()

    device = _device(db_session, business_id, location_id=pos1.id)
    svc = SyncService(db_session, device)
    ev = {"event_id": "s3", "event_type": "sale", "seq": 1,
          "payload": {"sku": "SKU000002", "quantity": 1, "location_code": "CENTRAL"}}
    res = svc.push(6, [ev])
    assert "s3" not in res["applied_event_ids"]
    conflicts = db_session.query(SyncConflict).filter(SyncConflict.event_id == "s3").all()
    assert len(conflicts) == 1
    assert conflicts[0].kind == "location_mismatch"


def _second_business(db_session) -> int:
    biz = Business(code="test2", name="Test Business 2")
    db_session.add(biz)
    db_session.commit()
    return int(biz.id)


def test_duplicate_event_id_across_businesses_does_not_block(db_session, business_id):
    # H-004: event_id repetido en dos negocios distintos no bloquea al segundo.
    # client_id distinto en cada payload (Customer.client_id es único GLOBAL, invariante
    # aparte de lo que este test verifica: el scoping del ledger de idempotencia).
    other_bid = _second_business(db_session)
    svc_a = SyncService(db_session, _device(db_session, business_id))
    svc_b = SyncService(db_session, _device(db_session, other_bid))
    ev_a = {"event_id": "shared-id", "event_type": "customer", "seq": 1,
            "payload": {"client_id": "C-SHARED-A", "name": "Ana", "phone": "600999888"}}
    ev_b = {"event_id": "shared-id", "event_type": "customer", "seq": 1,
            "payload": {"client_id": "C-SHARED-B", "name": "Bea", "phone": "600999777"}}
    res_a = svc_a.push(6, [ev_a])
    res_b = svc_b.push(6, [ev_b])
    assert "shared-id" in res_a["applied_event_ids"]
    assert "shared-id" in res_b["applied_event_ids"]
    assert db_session.query(AppliedSyncEvent).filter(AppliedSyncEvent.event_id == "shared-id").count() == 2


def test_customer_client_id_collision_across_businesses_gets_suffix_not_apply_error(db_session, business_id):
    # Regresión: Customer.client_id es unique=True GLOBAL. Si otro negocio ya
    # usa ese client_id, debe resolverse con sufijo + conflicto tipado
    # "customer_client_id" — nunca un IntegrityError sin manejar (apply_error).
    other_bid = _second_business(db_session)
    db_session.add(Customer(business_id=other_bid, client_id="C-GLOBAL", name="De otro negocio"))
    db_session.commit()

    svc = SyncService(db_session, _device(db_session, business_id))
    ev = {"event_id": "cx1", "event_type": "customer", "seq": 1,
          "payload": {"client_id": "C-GLOBAL", "name": "Local", "phone": "600555444"}}
    res = svc.push(6, [ev])
    assert "cx1" in res["applied_event_ids"]
    conflicts = db_session.query(SyncConflict).filter(SyncConflict.event_id == "cx1").all()
    assert len(conflicts) == 1
    assert conflicts[0].kind == "customer_client_id"
    # El cliente local se creó con un client_id reasignado, no con el original (ya tomado).
    assert db_session.query(Customer).filter(
        Customer.business_id == business_id, Customer.name == "Local"
    ).first().client_id != "C-GLOBAL"


def test_pull_excludes_password_hash(db_session, business_id):
    # H-002: el hash de contraseña nunca debe viajar en el snapshot.
    db_session.add(User(username="u1", password_hash="secret-hash", role="operator",
                        is_active=True, business_id=business_id))
    db_session.commit()
    svc = SyncService(db_session, _device(db_session, business_id))
    snap = svc.build_pull()["snapshot"]
    assert len(snap["users"]) >= 1
    assert all("password_hash" not in row for row in snap["users"])


def test_pull_does_not_leak_other_business_movement_allocations(db_session, business_id):
    # H-003: movement_allocations/sales_document_items deben acotarse por negocio.
    other_bid = _second_business(db_session)
    other_central = Location(business_id=other_bid, code="CENTRAL", name="Central", role="central")
    db_session.add(other_central)
    db_session.flush()
    other_product = Product(business_id=other_bid, sku="OTHER-SKU", name="Other", min_stock=0,
                             default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0)
    db_session.add(other_product)
    db_session.flush()
    mv = InventoryMovement(business_id=other_bid, product_id=other_product.id,
                            location_id=other_central.id, type="purchase", quantity=5, unit_cost=1.0)
    db_session.add(mv)
    db_session.flush()
    lot = InventoryLot(business_id=other_bid, movement_id=mv.id, product_id=other_product.id,
                        location_id=other_central.id, lot_code="OTHER-LOT",
                        received_at=mv.movement_date, unit_cost=1.0, qty_received=5, qty_remaining=5)
    db_session.add(lot)
    db_session.flush()
    db_session.add(MovementAllocation(movement_id=mv.id, lot_id=lot.id, quantity=5, unit_cost=1.0))
    db_session.commit()

    svc = SyncService(db_session, _device(db_session, business_id))
    snap = svc.build_pull()["snapshot"]
    assert snap["movement_allocations"] == []


def test_pull_does_not_leak_other_business_sales_document_items(db_session, business_id):
    other_bid = _second_business(db_session)
    doc = SalesDocument(business_id=other_bid, doc_type="F", year_month="202608", seq=1,
                        code="OTHER-DOC-1", issuer_name="X", client_name="Y", client_id="Z")
    db_session.add(doc)
    db_session.flush()
    db_session.add(SalesDocumentItem(document_id=doc.id, line_no=1, description="Item",
                                      quantity=1, unit_price=1.0, line_total=1.0))
    db_session.commit()

    svc = SyncService(db_session, _device(db_session, business_id))
    snap = svc.build_pull()["snapshot"]
    assert snap["sales_document_items"] == []
