from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.schemas import (
    AdjustmentCreate,
    MovementResult,
    PurchaseCreate,
    SaleCreate,
    StockRead,
)
from app.services.inventory_service import InventoryService

router = APIRouter(tags=["inventory"])


def inventory_service_dep(db: Session = Depends(session_dep)) -> InventoryService:
    return InventoryService(db)


@router.post("/movements/purchase", response_model=MovementResult)
def create_purchase(
    payload: PurchaseCreate,
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    return service.purchase(payload)


@router.post("/movements/sale", response_model=MovementResult)
def create_sale(
    payload: SaleCreate,
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    return service.sale(payload)


@router.post("/movements/adjustment", response_model=MovementResult)
def create_adjustment(
    payload: AdjustmentCreate,
    service: InventoryService = Depends(inventory_service_dep),
) -> MovementResult:
    return service.adjustment(payload)


@router.get("/stock/{sku}", response_model=StockRead)
def get_stock(sku: str, service: InventoryService = Depends(inventory_service_dep)) -> StockRead:
    return service.stock(sku)


@router.get("/stock", response_model=list[StockRead])
def list_stock(service: InventoryService = Depends(inventory_service_dep)) -> list[StockRead]:
    return service.stock_list()
