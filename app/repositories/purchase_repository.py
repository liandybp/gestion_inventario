"""Acceso a datos de compras (extraído de ui_purchases, H-028)."""
from __future__ import annotations
from typing import Optional
from sqlalchemy import select
from sqlalchemy.orm import Session
from app.models import InventoryLot, InventoryMovement, Product


class PurchaseRepository:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def get_movement(self, movement_id: int) -> Optional[InventoryMovement]:
        return self._db.get(InventoryMovement, int(movement_id))

    def get_product(self, product_id: int) -> Optional[Product]:
        return self._db.get(Product, int(product_id))

    def get_product_by_sku(self, sku: str) -> Optional[Product]:
        stmt = select(Product).where(Product.sku == sku, Product.business_id == self._business_id)
        return self._db.scalar(stmt)

    def get_lot(self, lot_id: int) -> Optional[InventoryLot]:
        return self._db.get(InventoryLot, int(lot_id))

    def get_lot_by_movement(self, movement_id: int) -> Optional[InventoryLot]:
        stmt = select(InventoryLot).where(InventoryLot.movement_id == int(movement_id))
        return self._db.scalar(stmt)

    def list_sku_prefixes(self, prefix: str) -> list[str]:
        rows = self._db.execute(select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))).all()
        return [str(sku or "") for (sku,) in rows]