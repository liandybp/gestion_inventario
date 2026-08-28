from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


class Business(Base):
    __tablename__ = "businesses"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )

    # ── Config de negocio (fuente única de verdad; el .conf es legacy) ──
    # issuer
    issuer_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    issuer_tax_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    issuer_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    issuer_city: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    issuer_postal_code: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    issuer_country: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    issuer_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    issuer_phone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # currency
    currency_code: Mapped[str] = mapped_column(String(8), default="EUR", server_default="EUR")
    currency_symbol: Mapped[str] = mapped_column(String(8), default="€", server_default="€")
    # purchase
    purchase_invoice_source_currency: Mapped[str] = mapped_column(String(8), default="EUR", server_default="EUR")
    purchase_invoice_vat_rate: Mapped[float] = mapped_column(Numeric(10, 4, asdecimal=False), default=1.21, server_default="1.21")
    purchase_invoice_fx_rate: Mapped[Optional[float]] = mapped_column(Numeric(10, 4, asdecimal=False), nullable=True)
    # sales_documents
    sales_default_type: Mapped[str] = mapped_column(String(1), default="F", server_default="F")
    sales_enabled_types: Mapped[str] = mapped_column(String(16), default="F,P", server_default="F,P")
    sales_invoice_label: Mapped[str] = mapped_column(String(64), default="Factura", server_default="Factura")
    sales_quote_label: Mapped[str] = mapped_column(String(64), default="Presupuesto", server_default="Presupuesto")
    # dividends
    dividends_business_label: Mapped[str] = mapped_column(String(64), default="Negocio", server_default="Negocio")
    dividends_partners: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dividends_opening_pending: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dividends_opening_pending_as_of: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    # inventory
    inventory_replenishment_lead_time_days: Mapped[float] = mapped_column(Numeric(10, 2, asdecimal=False), default=25.0, server_default="25")


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(16), nullable=False, default="operator", server_default="operator")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    must_change_password: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class UserBusiness(Base):
    """Many-to-many association between users and businesses.

    Owners can be assigned to multiple businesses and switch between them.
    ``User.business_id`` remains the primary/default business for
    backward compatibility (operators and legacy data).
    """

    __tablename__ = "user_businesses"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class Customer(Base):
    __tablename__ = "customers"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    client_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), index=True
    )


class Location(Base):
    __tablename__ = "locations"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    # "central" | "pos" (nullable para filas legacy pre-backfill)
    role: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class SalesDocument(Base):
    __tablename__ = "sales_documents"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    location_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("locations.id"), nullable=True, index=True
    )
    customer_id: Mapped[Optional[int]] = mapped_column(ForeignKey("customers.id"), nullable=True, index=True)
    doc_type: Mapped[str] = mapped_column(String(1), index=True)
    year_month: Mapped[str] = mapped_column(String(6), index=True)
    seq: Mapped[int] = mapped_column(Integer, index=True)
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)

    issue_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)

    issuer_name: Mapped[str] = mapped_column(String(255))
    issuer_tax_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    issuer_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    client_name: Mapped[str] = mapped_column(String(255))
    client_id: Mapped[str] = mapped_column(String(64))
    client_address: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    currency_code: Mapped[str] = mapped_column(String(8), default="EUR", server_default="EUR")
    currency_symbol: Mapped[str] = mapped_column(String(8), default="€", server_default="€")

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subtotal: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False), default=0, server_default="0")
    total: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False), default=0, server_default="0")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), index=True)


class SalesDocumentItem(Base):
    __tablename__ = "sales_document_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    document_id: Mapped[int] = mapped_column(ForeignKey("sales_documents.id"), index=True)
    line_no: Mapped[int] = mapped_column(Integer)

    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    description: Mapped[str] = mapped_column(String(255))
    unit_of_measure: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    quantity: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    unit_price: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    line_total: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))


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
    __table_args__ = (
        # H-053: filtros calientes multi-tenant por (business, sku)
        Index("ix_products_business_sku", "business_id", "sku"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
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
    __table_args__ = (
        # H-053: filtros calientes multi-tenant (profit por mes, historial, por producto)
        Index(
            "ix_inventory_movements_business_type_date",
            "business_id", "type", "movement_date",
        ),
        Index(
            "ix_inventory_movements_business_product",
            "business_id", "product_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    location_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("locations.id"), nullable=True, index=True
    )
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
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
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
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    movement_id: Mapped[int] = mapped_column(
        ForeignKey("inventory_movements.id"), index=True
    )
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), index=True)
    location_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("locations.id"), nullable=True, index=True
    )
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
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    expense_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    concept: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class DashboardWidget(Base):
    __tablename__ = "dashboard_widgets"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    type: Mapped[str] = mapped_column(String(16))
    title: Mapped[str] = mapped_column(String(255))
    params: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    position: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    created_by: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True, index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class ChatMessage(Base):
    """Mensaje de la conversación del copiloto, por negocio (historial)."""

    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("businesses.id"), nullable=True, index=True
    )
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id"), nullable=True, index=True
    )
    role: Mapped[str] = mapped_column(String(16))  # "user" | "assistant"
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class CashBaseline(Base):
    """Saldo inicial de caja; se usa el más reciente por negocio."""

    __tablename__ = "cash_baselines"

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    baseline_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    amount: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False))
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )


class CoachActionLog(Base):
    """Tracking de recomendaciones: abierta/hecha/descartada."""

    __tablename__ = "coach_action_logs"
    __table_args__ = (Index("ix_coach_action_business_rule", "business_id", "rule_key"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), index=True)
    rule_key: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(16), default="open", server_default="open")
    impact_estimate: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    marked_by: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id"), nullable=True)
    marked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    verified_result: Mapped[Optional[float]] = mapped_column(
        Numeric(14, 4, asdecimal=False), nullable=True
    )
    verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)


class SyncEvent(Base):
    """Outbox local de eventos offline pendientes de sincronizar (append-only).

    En el nodo desktop guarda ventas/altas de cliente hechas sin conexión, con un
    hash encadenado que detecta manipulación. En el servidor central la tabla queda
    inerte hasta que P2/P3 la usen.
    """

    __tablename__ = "pending_sync_events"
    __table_args__ = (
        Index("ix_pending_sync_events_business_seq", "business_id", "seq"),
        # H-004: dos escrituras concurrentes al mismo negocio no pueden duplicar seq
        # — el segundo insert falla con IntegrityError en vez de corromper la cadena.
        UniqueConstraint("business_id", "seq", name="ux_pending_sync_events_business_seq"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    business_id: Mapped[int] = mapped_column(Integer, index=True)
    location_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    event_type: Mapped[str] = mapped_column(String(32), index=True)
    payload: Mapped[str] = mapped_column(Text)
    seq: Mapped[int] = mapped_column(Integer, index=True)
    prev_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    event_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(16), default="pending", server_default="pending", index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), index=True
    )
