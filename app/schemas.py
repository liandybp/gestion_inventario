from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ProductCreate(BaseModel):
    sku: str
    name: str
    category: str | None = None
    min_stock: float = 0
    default_sale_price: float | None = None


class ProductRead(BaseModel):
    id: int
    sku: str
    name: str
    category: str | None
    min_stock: float
    default_sale_price: float | None

    model_config = {"from_attributes": True}


class PurchaseCreate(BaseModel):
    sku: str
    quantity: float
    unit_cost: float
    movement_date: datetime | None = None
    lot_code: str | None = None
    note: str | None = None


class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: float
    movement_date: datetime | None = None
    note: str | None = None


class AdjustmentCreate(BaseModel):
    sku: str
    quantity_delta: float
    unit_cost: float | None = None
    movement_date: datetime | None = None
    note: str | None = None


class MovementRead(BaseModel):
    id: int
    product_id: int
    type: str
    quantity: float
    unit_cost: float | None
    unit_price: float | None
    movement_date: datetime
    note: str | None

    model_config = {"from_attributes": True}


class MovementResult(BaseModel):
    movement: MovementRead
    stock_after: float
    warning: str | None = None


class StockRead(BaseModel):
    sku: str
    quantity: float
    min_stock: float
    needs_restock: bool
