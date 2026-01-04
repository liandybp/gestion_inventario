from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Product


class ProductRepository:
    def __init__(self, db: Session):
        self._db = db

    def get_by_sku(self, sku: str) -> Optional[Product]:
        sku = sku.strip()
        return self._db.scalar(select(Product).where(Product.sku == sku))

    def list(self) -> list[Product]:
        return list(self._db.scalars(select(Product).order_by(Product.id)))

    def list_skus_starting_with(self, prefix: str) -> list[str]:
        rows = self._db.execute(
            select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))
        ).all()
        return [sku for (sku,) in rows]

    def search(self, query: str, limit: int = 20) -> list[Product]:
        q = query.strip()
        if not q:
            return self.list()[:limit]
        like = f"%{q}%"
        return list(
            self._db.scalars(
                select(Product)
                .where((Product.sku.ilike(like)) | (Product.name.ilike(like)))
                .order_by(Product.name)
                .limit(limit)
            )
        )

    def add(self, product: Product) -> None:
        self._db.add(product)
