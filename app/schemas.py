from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class ProductCreate(BaseModel):
    sku: Optional[str] = None
    name: str
    category: Optional[str] = None
    min_stock: float = 0
    default_sale_price: Optional[float] = None


class ProductRead(BaseModel):
    id: int
    sku: str
    name: str
    category: Optional[str]
    min_stock: float
    default_sale_price: Optional[float]

    model_config = {"from_attributes": True}


class PurchaseCreate(BaseModel):
    sku: str
    quantity: float
    unit_cost: float
    movement_date: Optional[datetime] = None
    lot_code: Optional[str] = None
    note: Optional[str] = None


class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: float
    movement_date: Optional[datetime] = None
    note: Optional[str] = None


class AdjustmentCreate(BaseModel):
    sku: str
    quantity_delta: float
    unit_cost: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None


class MovementRead(BaseModel):
    id: int
    product_id: int
    type: str
    quantity: float
    unit_cost: Optional[float]
    unit_price: Optional[float]
    movement_date: datetime
    note: Optional[str]

    model_config = {"from_attributes": True}


class MovementResult(BaseModel):
    movement: MovementRead
    stock_after: float
    warning: Optional[str] = None


class StockRead(BaseModel):
    sku: str
    quantity: float
    min_stock: float
    needs_restock: bool
