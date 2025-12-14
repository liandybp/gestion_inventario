from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import DateTime, ForeignKey, Numeric, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    min_stock: Mapped[float] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=False, default=0, server_default="0"
    )
    default_sale_price: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )


class InventoryMovement(Base):
    __tablename__ = "inventory_movements"

    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    type: Mapped[str] = mapped_column(String(16), index=True)
    quantity: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    unit_cost: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    unit_price: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    movement_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    note: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class InventoryLot(Base):
    __tablename__ = "inventory_lots"

    id: Mapped[int] = mapped_column(primary_key=True)
    movement_id: Mapped[int] = mapped_column(
        ForeignKey("inventory_movements.id"), index=True
    )
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    lot_code: Mapped[str] = mapped_column(String(64), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    unit_cost: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    qty_received: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    qty_remaining: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class MovementAllocation(Base):
    __tablename__ = "movement_allocations"

    id: Mapped[int] = mapped_column(primary_key=True)
    movement_id: Mapped[int] = mapped_column(
        ForeignKey("inventory_movements.id"), index=True
    )
    lot_id: Mapped[int] = mapped_column(ForeignKey("inventory_lots.id"), index=True)
    quantity: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    unit_cost: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
