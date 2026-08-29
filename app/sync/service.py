from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.logger import get_logger
from app.models import (
    AppliedSyncEvent,
    Business,
    CashBaseline,
    Customer,
    DeviceToken,
    InventoryLot,
    InventoryMovement,
    Location,
    MoneyExtraction,
    MovementAllocation,
    OperatingExpense,
    Product,
    SalesDocument,
    SalesDocumentItem,
    SyncConflict,
    User,
)
from app.repositories.customer_repository import CustomerRepository
from app.repositories.location_repository import LocationRepository
from app.schemas import SaleCreate
from app.services.inventory_service import InventoryService
from app.sync.tables import SYNC_TABLE_ORDER

_log = get_logger(__name__)

MIN_APP_SCHEMA_VERSION = 6

# H-002: columnas que nunca deben viajar en el pull, aunque el modelo las tenga.
_SENSITIVE_COLUMNS = {
    User: {"password_hash"},
}


def _dump_columns(model, exclude: Optional[set[str]] = None) -> list[str]:
    """Nombres de columna a incluir en el dump de `model`, aplicando siempre
    _SENSITIVE_COLUMNS salvo que el caller pase un `exclude` explícito."""
    resolved = _SENSITIVE_COLUMNS.get(model, set()) if exclude is None else exclude
    return [c.name for c in model.__table__.columns if c.name not in resolved]


class SyncService:
    def __init__(self, db: Session, device: DeviceToken):
        self._db = db
        self._device = device
        self._business_id = int(device.business_id)

    def _already_applied(self, event_id: str) -> bool:
        # H-004: scoped por negocio — dos negocios distintos con el mismo
        # event_id (colisión de UUID) no se bloquean entre sí.
        return self._db.scalar(
            select(AppliedSyncEvent).where(
                AppliedSyncEvent.event_id == event_id,
                AppliedSyncEvent.business_id == self._business_id,
            )
        ) is not None

    def _mark_applied(self, event_id: str) -> None:
        self._db.add(AppliedSyncEvent(
            event_id=event_id, device_id=int(self._device.id), business_id=self._business_id
        ))

    def _record_conflict(self, event_id: str, kind: str, detail: dict) -> None:
        self._db.add(SyncConflict(
            business_id=self._business_id, device_id=int(self._device.id),
            event_id=event_id, kind=kind, detail=json.dumps(detail, ensure_ascii=False),
        ))
        self._db.commit()

    def push(self, app_schema_version: int, events: list[dict]) -> dict:
        if int(app_schema_version) < MIN_APP_SCHEMA_VERSION:
            raise HTTPException(status_code=409, detail="versión incompatible, actualice la app")
        applied: list[str] = []
        for ev in sorted(events, key=lambda e: int(e.get("seq", 0))):
            eid = str(ev.get("event_id") or "")
            if not eid or self._already_applied(eid):
                continue
            try:
                if ev.get("event_type") == "sale":
                    ok = self._apply_sale(ev)
                elif ev.get("event_type") == "customer":
                    ok = self._apply_customer(ev)
                else:
                    ok = False
                if ok:
                    applied.append(eid)
            except Exception:  # noqa: BLE001
                self._db.rollback()
                _log.error("Fallo aplicando evento %s (negocio %s)", eid, self._business_id, exc_info=True)
                self._record_conflict(eid, "apply_error", {"event_type": ev.get("event_type")})
        return {"applied_event_ids": applied}

    def _resolve_location_id(self, location_code: Optional[str]) -> Optional[int]:
        code = str(location_code or "").strip()
        if not code:
            return None
        try:
            return LocationRepository(self._db, self._business_id).location_id_for_code(
                code, auto_create=False
            )
        except HTTPException:
            return None

    def _apply_sale(self, ev: dict) -> bool:
        eid = str(ev.get("event_id") or "")
        p = ev.get("payload") or {}
        location_code = p.get("location_code")

        # H-005 (endurecido): resolver el location_code ANTES de llegar a sale(),
        # sin auto_create, siempre — no solo cuando el device está scoped. Si se
        # deja pasar un location_code sin resolver, InventoryService.sale() lo
        # auto-crea (auto_create=True interno), reabriendo el bug de ubicaciones
        # huérfanas ya sufrido en producción (incidente v0.4.5).
        resolved_loc_id: Optional[int] = None
        if location_code:
            resolved_loc_id = self._resolve_location_id(location_code)
            if resolved_loc_id is None:
                self._record_conflict(
                    eid, "unknown_location", {"sku": p.get("sku"), "location_code": location_code},
                )
                return False

        # Si el dispositivo está scoped a una ubicación, el evento debe reportar
        # ventas de esa misma ubicación — nunca de otra (ej. CENTRAL).
        if self._device.location_id is not None and resolved_loc_id != int(self._device.location_id):
            self._record_conflict(
                eid, "location_mismatch",
                {"sku": p.get("sku"), "location_code": location_code,
                 "device_location_id": int(self._device.location_id)},
            )
            return False

        # Marca de idempotencia ANTES de sale(): el commit interno de sale() los persiste juntos.
        self._mark_applied(eid)
        try:
            payload = SaleCreate(
                sku=str(p.get("sku")),
                quantity=float(p.get("quantity")),
                unit_price=p.get("unit_price"),
                movement_date=p.get("movement_date"),
                note=p.get("note"),
                location_code=location_code,
            )
            InventoryService(self._db, business_id=self._business_id).sale(payload)
            return True
        except HTTPException as e:
            self._db.rollback()  # descarta el AppliedSyncEvent pendiente
            # H-006: distinguir la causa real (stock vs. producto inexistente vs.
            # payload inválido) en vez de etiquetar todo como "sale_stock".
            if e.status_code == 409:
                kind = "sale_stock"
            elif e.status_code == 404:
                kind = "sale_product_not_found"
            else:
                kind = "sale_invalid_payload"
            self._record_conflict(
                eid, kind,
                {"sku": p.get("sku"), "quantity": p.get("quantity"),
                 "status_code": e.status_code, "detail": str(e.detail)},
            )
            return False

    def _create_and_mark_customer(
        self, eid: str, client_id: str, p: dict, *, conflict: Optional[tuple[str, dict]] = None
    ) -> None:
        """H-011: crea el cliente + marca el evento aplicado + registra conflicto opcional."""
        repo = CustomerRepository(self._db, self._business_id)
        repo.create(
            client_id=client_id, name=str(p.get("name") or ""),
            address=p.get("address"), phone=p.get("phone"), email=p.get("email"),
        )
        self._mark_applied(eid)
        if conflict is not None:
            kind, detail = conflict
            self._record_conflict(eid, kind, detail)
        else:
            self._db.commit()

    def _apply_customer(self, ev: dict) -> bool:
        eid = str(ev.get("event_id") or "")
        p = ev.get("payload") or {}
        repo = CustomerRepository(self._db, self._business_id)
        # Dedup por contacto: si ya existe uno con el mismo teléfono/email, conservar el primero.
        existing = repo.find_by_contact(p.get("phone"), p.get("email"))
        if existing is not None:
            self._mark_applied(eid)
            self._db.commit()
            return True
        client_id = str(p.get("client_id") or "").strip()
        # Customer.client_id es unique=True GLOBAL (no compuesto por business_id,
        # mismo patrón que products.sku — ver ADR-012). El chequeo de colisión
        # debe ser global: filtrar por business_id aquí dejaría pasar un create()
        # que choca con el client_id de OTRO negocio, cayendo sin manejar en el
        # except genérico de push() y bloqueando el evento para siempre.
        clash = self._db.scalar(
            select(Customer).where(Customer.client_id == client_id)
        ) if client_id else None
        if clash is not None:
            # Mismo client_id pero contacto distinto: personas distintas → sufijo + conflicto.
            new_cid = f"{client_id}-{eid[:6]}"
            self._create_and_mark_customer(
                eid, new_cid, p,
                conflict=("customer_client_id", {"client_id": client_id, "reassigned": new_cid}),
            )
            return True
        self._create_and_mark_customer(eid, client_id, p)
        return True

    def _dump(self, model, *filters, exclude: Optional[set[str]] = None) -> list[dict]:
        # exclude=None -> usar el default de _SENSITIVE_COLUMNS; exclude=set()
        # explícito debe respetarse tal cual ("sin exclusiones"), no caer al
        # default por el truthiness de un set vacío.
        cols = _dump_columns(model, exclude)
        rows = list(self._db.scalars(select(model).where(*filters))) if filters else list(self._db.scalars(select(model)))
        return [{c: getattr(r, c) for c in cols} for r in rows]

    def _dump_movement_allocations(self, bid: int) -> list[dict]:
        # H-003: MovementAllocation no tiene business_id propio — join al
        # movimiento padre para acotar por negocio (evita fuga cross-tenant).
        rows = list(
            self._db.scalars(
                select(MovementAllocation)
                .join(InventoryMovement, MovementAllocation.movement_id == InventoryMovement.id)
                .where(InventoryMovement.business_id == bid)
            )
        )
        cols = _dump_columns(MovementAllocation)
        return [{c: getattr(r, c) for c in cols} for r in rows]

    def _dump_sales_document_items(self, bid: int) -> list[dict]:
        # H-003: SalesDocumentItem no tiene business_id propio — join al
        # documento padre para acotar por negocio (evita fuga cross-tenant).
        rows = list(
            self._db.scalars(
                select(SalesDocumentItem)
                .join(SalesDocument, SalesDocumentItem.document_id == SalesDocument.id)
                .where(SalesDocument.business_id == bid)
            )
        )
        cols = _dump_columns(SalesDocumentItem)
        return [{c: getattr(r, c) for c in cols} for r in rows]

    def build_pull(self) -> dict:
        bid = self._business_id
        snap = {
            "businesses": self._dump(Business, Business.id == bid),
            "locations": self._dump(Location, Location.business_id == bid),
            "products": self._dump(Product, Product.business_id == bid),
            "customers": self._dump(Customer, Customer.business_id == bid),
            "inventory_movements": self._dump(InventoryMovement, InventoryMovement.business_id == bid),
            "inventory_lots": self._dump(InventoryLot, InventoryLot.business_id == bid),
            "movement_allocations": self._dump_movement_allocations(bid),
            "sales_documents": self._dump(SalesDocument, SalesDocument.business_id == bid),
            "sales_document_items": self._dump_sales_document_items(bid),
            "operating_expenses": self._dump(OperatingExpense, OperatingExpense.business_id == bid),
            "money_extractions": self._dump(MoneyExtraction, MoneyExtraction.business_id == bid),
            "cash_baselines": self._dump(CashBaseline, CashBaseline.business_id == bid),
            "users": self._dump(User, User.business_id == bid),
        }
        assert set(snap) == set(SYNC_TABLE_ORDER), (
            "SyncService.build_pull() desincronizado de app/sync/tables.py::SYNC_TABLE_ORDER "
            f"— faltan {set(SYNC_TABLE_ORDER) - set(snap)}, sobran {set(snap) - set(SYNC_TABLE_ORDER)}."
        )
        return {"snapshot": snap, "server_time": datetime.now(timezone.utc)}
