from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import SyncEvent
from app.sync.outbox import append_event, verify_chain


def test_append_first_event_has_no_prev_and_seq_1(db_session, business_id):
    ev = append_event(
        db_session, business_id=business_id, event_type="sale", payload={"sku": "X"}
    )
    db_session.commit()
    assert ev.seq == 1
    assert ev.prev_hash is None
    assert ev.event_hash


def test_chain_links_events(db_session, business_id):
    e1 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 2})
    db_session.commit()
    assert e2.seq == 2
    assert e2.prev_hash == e1.event_hash


def test_verify_chain_true_when_intact(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    append_event(db_session, business_id=business_id, event_type="customer", payload={"n": 2})
    db_session.commit()
    assert verify_chain(db_session, business_id) is True


def test_verify_chain_false_when_payload_tampered(db_session, business_id):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 2})
    db_session.commit()
    # Manipular el payload directamente (simula edición del .db)
    row = db_session.get(SyncEvent, e2.id)
    row.payload = '{"n": 999}'
    db_session.commit()
    assert verify_chain(db_session, business_id) is False


def test_verify_chain_true_when_empty(db_session, business_id):
    assert verify_chain(db_session, business_id) is True


@pytest.mark.parametrize(
    "field,tamper",
    [
        ("location_id", 888),   # H-002: editar location_id en el .db rompe la cadena
        ("seq", 9999),          # H-002: editar seq rompe la cadena (y el orden)
        ("status", "synced"),   # H-002: marcar como sincronizado a mano se detecta
    ],
)
def test_verify_chain_false_when_identity_field_tampered(db_session, business_id, field, tamper):
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(
        db_session,
        business_id=business_id,
        event_type="sale",
        payload={"n": 2},
        location_id=7,
    )
    db_session.commit()
    row = db_session.get(SyncEvent, e2.id)
    setattr(row, field, tamper)
    db_session.commit()
    assert verify_chain(db_session, business_id) is False


def test_verify_chain_false_when_business_id_tampered(db_session, business_id):
    # H-002: mover un evento a otro negocio deja un hueco de seq en el original,
    # que verify_chain debe detectar (además del mismatch de hash en el destino).
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    e2 = append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 2})
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 3})
    db_session.commit()
    row = db_session.get(SyncEvent, e2.id)
    row.business_id = 99999
    db_session.commit()
    assert verify_chain(db_session, business_id) is False


def test_duplicate_seq_same_business_rejected(db_session, business_id):
    # H-004: el constraint UNIQUE(business_id, seq) es el backstop de la carrera —
    # dos escrituras concurrentes que produzcan el mismo seq fallan explícitamente
    # en vez de corromper la cadena en silencio.
    append_event(db_session, business_id=business_id, event_type="sale", payload={"n": 1})
    db_session.commit()
    dup = SyncEvent(
        event_id="dup-race-1",
        business_id=business_id,
        event_type="sale",
        payload='{"n": 2}',
        seq=1,  # mismo (business_id, seq) que el evento 1
        prev_hash="x",
        event_hash="x",
        status="pending",
    )
    db_session.add(dup)
    with pytest.raises(IntegrityError):
        db_session.flush()
    db_session.rollback()
