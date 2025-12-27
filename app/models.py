from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), nullable=False, default="operator", server_default="operator")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    action: Mapped[str] = mapped_column(String(64), index=True)
    entity_type: Mapped[str] = mapped_column(String(64), index=True)
    entity_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    min_stock: Mapped[float] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=False, default=0, server_default="0"
    )
    unit_of_measure: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    default_purchase_cost: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    default_sale_price: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    lead_time_days: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    image_url: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)


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


class MoneyExtraction(Base):
    __tablename__ = "money_extractions"

    id: Mapped[int] = mapped_column(primary_key=True)
    extraction_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    party: Mapped[str] = mapped_column(String(32), index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    concept: Mapped[str] = mapped_column(String(255))
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
    lot_code: Mapped[str] = mapped_column(String(64), index=True, unique=True)
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


class OperatingExpense(Base):
    __tablename__ = "operating_expenses"

    id: Mapped[int] = mapped_column(primary_key=True)
    expense_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    concept: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
