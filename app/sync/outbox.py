from __future__ import annotations

import hashlib
import json
import uuid
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.logger import get_logger
from app.models import SyncEvent

_log = get_logger(__name__)


def compute_event_hash(
    prev_hash: Optional[str],
    event_id: str,
    event_type: str,
    payload: str,
    *,
    business_id: int,
    location_id: Optional[int],
    seq: int,
    status: str,
) -> str:
    """sha256 hex de la concatenación canónica '|'-separada.

    Cubre los campos de identidad/orden (business_id, location_id, seq) y el estado
    (status) además del contenido (event_id, event_type, payload): editar cualquiera
    de ellos en el .db local rompe verify_chain. Nota para P3: transicionar status
    (pending -> synced) exige recalcular el hash del evento cambiado y de los que le
    siguen (cada hash incluye el prev_hash del anterior).
    """
    raw = (
        f"{prev_hash or ''}|{event_id}|{event_type}|{payload}"
        f"|{business_id}|{location_id or ''}|{seq}|{status}"
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def append_event(
    db: Session,
    *,
    business_id: int,
    event_type: str,
    payload: dict,
    location_id: Optional[int] = None,
    event_id: Optional[str] = None,
) -> SyncEvent:
    """Encola un evento en pending_sync_events con seq monotónico y hash encadenado.

    No hace commit (lo hace el caller). event_id se genera (uuid4) si no se pasa.
    La unicidad (business_id, seq) está garantizada por constraint de BD
    (ux_pending_sync_events_business_seq): bajo concurrencia, el segundo insert
    falla con IntegrityError en vez de corromper la cadena en silencio.
    """
    try:
        last = db.scalar(
            select(SyncEvent)
            .where(SyncEvent.business_id == int(business_id))
            .order_by(SyncEvent.seq.desc())
            .limit(1)
        )
        prev_hash = last.event_hash if last is not None else None
        next_seq = (last.seq + 1) if last is not None else 1

        eid = event_id or uuid.uuid4().hex
        payload_str = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        ehash = compute_event_hash(
            prev_hash,
            eid,
            event_type,
            payload_str,
            business_id=int(business_id),
            location_id=location_id,
            seq=next_seq,
            status="pending",
        )

        event = SyncEvent(
            event_id=eid,
            business_id=int(business_id),
            location_id=location_id,
            event_type=event_type,
            payload=payload_str,
            seq=next_seq,
            prev_hash=prev_hash,
            event_hash=ehash,
            status="pending",
        )
        db.add(event)
        db.flush()
        return event
    except Exception as e:
        _log.error(
            f"append_event falló business_id={business_id} event_type={event_type}: {e}",
            exc_info=True,
        )
        raise


def verify_chain(db: Session, business_id: int) -> bool:
    """Recalcula la cadena de hash de los eventos del negocio en orden de seq.

    True si íntegra, False si alguna fila fue alterada, movida o eliminada
    (incluye huecos en seq). 0 eventos -> True. Los fallos de integridad y los
    errores de BD quedan registrados en logs.
    """
    try:
        events = list(
            db.scalars(
                select(SyncEvent)
                .where(SyncEvent.business_id == int(business_id))
                .order_by(SyncEvent.seq.asc())
            )
        )
        prev_hash: Optional[str] = None
        expected_seq = 1
        for ev in events:
            if ev.seq != expected_seq:
                _log.warning(
                    f"verify_chain: seq gap/duplicado business_id={business_id} "
                    f"(esperado {expected_seq}, encontrado {ev.seq}, event_id={ev.event_id})"
                )
                return False
            expected = compute_event_hash(
                prev_hash,
                ev.event_id,
                ev.event_type,
                ev.payload,
                business_id=ev.business_id,
                location_id=ev.location_id,
                seq=ev.seq,
                status=ev.status,
            )
            if expected != ev.event_hash or ev.prev_hash != prev_hash:
                _log.warning(
                    f"verify_chain: hash roto business_id={business_id} "
                    f"event_id={ev.event_id} seq={ev.seq}"
                )
                return False
            prev_hash = ev.event_hash
            expected_seq += 1
        return True
    except Exception as e:
        _log.error(f"verify_chain falló business_id={business_id}: {e}", exc_info=True)
        raise
