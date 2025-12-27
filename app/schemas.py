from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator


class ProductCreate(BaseModel):
    sku: Optional[str] = None
    name: str
    category: Optional[str] = None
    min_stock: float = 0
    unit_of_measure: Optional[str] = None
    default_purchase_cost: Optional[float] = None
    default_sale_price: Optional[float] = None
    lead_time_days: int = 0
    image_url: Optional[str] = None


class ProductUpdate(BaseModel):
    sku: Optional[str] = None
    name: str
    category: Optional[str] = None
    min_stock: float = 0
    unit_of_measure: Optional[str] = None
    default_purchase_cost: Optional[float] = None
    default_sale_price: Optional[float] = None
    lead_time_days: Optional[int] = None
    image_url: Optional[str] = None


class ProductRead(BaseModel):
    id: int
    sku: str
    name: str
    category: Optional[str]
    min_stock: float
    unit_of_measure: Optional[str]
    default_purchase_cost: Optional[float]
    default_sale_price: Optional[float]
    lead_time_days: int
    image_url: Optional[str]

    model_config = {"from_attributes": True}


class PurchaseCreate(BaseModel):
    sku: str
    quantity: float
    unit_cost: Optional[float] = None
    movement_date: Optional[datetime] = None
    lot_code: Optional[str] = None
    note: Optional[str] = None

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("quantity must be greater than 0")
        return v


class SaleCreate(BaseModel):
    sku: str
    quantity: float
    unit_price: Optional[float] = None
    movement_date: Optional[datetime] = None
    note: Optional[str] = None

    @field_validator("quantity")
    @classmethod
    def quantity_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("quantity must be greater than 0")
        return v


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
    name: Optional[str] = None
    unit_of_measure: Optional[str] = None
    quantity: float
    min_stock: float
    needs_restock: bool
    lead_time_days: int = 0
    avg_daily_sales: float = 0
    reorder_in_days: Optional[int] = None
