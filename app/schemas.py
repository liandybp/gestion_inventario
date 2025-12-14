from __future__ import annotations

from pydantic import BaseModel


class ProductCreate(BaseModel):
    sku: str
    name: str


class ProductRead(BaseModel):
    id: int
    sku: str
    name: str

    model_config = {"from_attributes": True}


class PurchaseCreate(BaseModel):
    sku: str
    quantity: float
    unit_cost: float
    note: str | None = None


class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: float
    note: str | None = None


class AdjustmentCreate(BaseModel):
    sku: str
    quantity_delta: float
    note: str | None = None


class MovementRead(BaseModel):
    id: int
    product_id: int
    type: str
    quantity: float
    unit_cost: float | None
    unit_price: float | None
    note: str | None

    model_config = {"from_attributes": True}


class MovementResult(BaseModel):
    movement: MovementRead
    stock_after: float
    warning: str | None = None


class StockRead(BaseModel):
    sku: str
    quantity: float
    is_negative: bool
