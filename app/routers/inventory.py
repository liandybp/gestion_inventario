from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import User
from app.schemas import (
    AdjustmentCreate,
    MovementResult,
    PurchaseCreate,
    SaleCreate,
    StockRead,
)
from app.security import require_user_api
from app.services.inventory_service import InventoryService

router = APIRouter(tags=["inventory"])


def inventory_service_dep(db: Session = Depends(session_dep)) -> InventoryService:
    return InventoryService(db)


@router.post("/movements/purchase", response_model=MovementResult)
def create_purchase(
    payload: PurchaseCreate,
    user: User = Depends(require_user_api),
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    result = service.purchase(payload)
    log_event(
        service._db,
        user,
        action="purchase_create",
        entity_type="movement",
        entity_id=str(result.movement.id),
        detail={"sku": payload.sku, "quantity": payload.quantity, "unit_cost": payload.unit_cost},
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
        service._db,
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
        service._db,
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
