from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Product


class ProductRepository:
    def __init__(self, db: Session):
        self._db = db

    def get_by_sku(self, sku: str) -> Product | None:
        sku = sku.strip()
        return self._db.scalar(select(Product).where(Product.sku == sku))

    def list(self) -> list[Product]:
        return list(self._db.scalars(select(Product).order_by(Product.id)))

    def add(self, product: Product) -> None:
        self._db.add(product)
