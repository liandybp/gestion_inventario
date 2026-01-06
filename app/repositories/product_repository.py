from __future__ import annotations

import unicodedata
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Product


def _normalize_text(text: str) -> str:
    """Remove accents/diacritics from text for accent-insensitive search."""
    if not text:
        return ""
    nfd = unicodedata.normalize('NFD', text)
    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')


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
        
        normalized_query = _normalize_text(q).lower()
        
        all_products = self.list()
        matches = []
        for product in all_products:
            normalized_sku = _normalize_text(product.sku or "").lower()
            normalized_name = _normalize_text(product.name or "").lower()
            
            if normalized_query in normalized_sku or normalized_query in normalized_name:
                matches.append(product)
                if len(matches) >= limit:
                    break
        
        return matches

    def add(self, product: Product) -> None:
        self._db.add(product)
