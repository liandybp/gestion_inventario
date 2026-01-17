from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import User
from app.security import get_active_business_id
from app.schemas import (
    AdjustmentCreate,
    MovementResult,
    PurchaseCreate,
    SaleCreate,
    SupplierReturnLotCreate,
    StockRead,
    TransferCreate,
    TransferResult,
)
from app.security import require_user_api
from app.services.inventory_service import InventoryService

router = APIRouter(tags=["inventory"])


def inventory_service_dep(request: Request, db: Session = Depends(session_dep)) -> InventoryService:
    bid = get_active_business_id(db, request)
    return InventoryService(db, business_id=bid)


@router.post("/movements/purchase", response_model=MovementResult)
def create_purchase(
    payload: PurchaseCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    result = service.purchase(payload)
    log_event(
        service.db,
        user,
        action="purchase_create",
        entity_type="movement",
        entity_id=str(result.movement.id),
        detail={"sku": payload.sku, "quantity": payload.quantity, "unit_cost": payload.unit_cost},
    )
    return result


@router.post("/movements/return-supplier-lot", response_model=MovementResult)
def create_supplier_return_lot(
    payload: SupplierReturnLotCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    result = service.supplier_return_by_lot(payload)
    log_event(
        service.db,
        user,
        action="supplier_return_create",
        entity_type="movement",
        entity_id=str(result.movement.id),
        detail={
            "lot_id": payload.lot_id,
            "quantity": payload.quantity,
            "location_code": payload.location_code,
        },
    )
    return result


@router.post("/movements/transfer", response_model=TransferResult)
def create_transfer(
    payload: TransferCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> TransferResult:
    result = service.transfer(payload)
    for line in result.lines:
        for mv_id in (line.movements_out or []) + (line.movements_in or []):
            log_event(
                service.db,
                user,
                action="transfer_create",
                entity_type="movement",
                entity_id=str(mv_id),
                detail={
                    "sku": line.sku,
                    "quantity": float(line.quantity),
                    "to_location_code": result.to_location_code,
                },
            )
    return result


@router.post("/movements/sale", response_model=MovementResult)
def create_sale(
    payload: SaleCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    result = service.sale(payload)
    log_event(
        service.db,
        user,
        action="sale_create",
        entity_type="movement",
        entity_id=str(result.movement.id),
        detail={"sku": payload.sku, "quantity": payload.quantity, "unit_price": payload.unit_price},
    )
    return result


@router.post("/movements/adjustment", response_model=MovementResult)
def create_adjustment(
    payload: AdjustmentCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    result = service.adjustment(payload)
    log_event(
        service.db,
        user,
        action="adjustment_create",
        entity_type="movement",
        entity_id=str(result.movement.id),
        detail={"sku": payload.sku, "quantity_delta": payload.quantity_delta, "unit_cost": payload.unit_cost},
    )
    return result


@router.get("/stock/{sku}", response_model=StockRead)
def get_stock(
    sku: str,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> StockRead:
    return service.stock(sku)


@router.get("/stock", response_model=list[StockRead])
def list_stock(
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> list[StockRead]:
    return service.stock_list()
