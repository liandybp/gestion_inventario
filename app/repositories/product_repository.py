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
    def __init__(self, db: Session, business_id: Optional[int] = None):
        self._db = db
        if business_id is None:
            raise ValueError("business_id is required")
        self._business_id = int(business_id)

    def get_by_sku(self, sku: str) -> Optional[Product]:
        sku = sku.strip()
        stmt = select(Product).where(Product.sku == sku, Product.business_id == self._business_id)
        return self._db.scalar(stmt)

    def list(self) -> list[Product]:
        stmt = select(Product).where(Product.business_id == self._business_id)
        return list(self._db.scalars(stmt.order_by(Product.id)))

    def list_skus_starting_with(self, prefix: str) -> list[str]:
        stmt = select(Product.sku).where(Product.sku.ilike(f"{prefix}%"), Product.business_id == self._business_id)
        rows = self._db.execute(stmt).all()
        return [sku for (sku,) in rows]

    def list_sku_prefixes(self, prefix: str) -> list[str]:
        # products.sku es unique=True GLOBAL (no compuesto por business_id, ver
        # migrations.py ux_products_sku): el candidato autogenerado debe calcularse
        # sobre TODOS los negocios o choca con SKUs de otros negocios (409 recurrente).
        stmt = select(Product.sku).where(Product.sku.ilike(f"{prefix}%"))
        rows = self._db.execute(stmt).all()
        return [str(sku or "") for (sku,) in rows]

    def search(self, query: str, limit: int = 20) -> list[Product]:
        q = query.strip()
        if not q:
            return self.list()[:limit]

        normalized_query = _normalize_text(q).lower()

        # Solo (id, sku, name) para filtrar: se ejecuta en cada pulsación (/ui/product-suggest) y
        # cargar el catálogo entero como objetos ORM es lo caro. La normalización de tildes se
        # hace en Python porque SQLite/PostgreSQL no la ofrecen de forma portable.
        stmt = (
            select(Product.id, Product.sku, Product.name)
            .where(Product.business_id == self._business_id)
            .order_by(Product.id)
        )
        match_ids: list[int] = []
        for product_id, sku, name in self._db.execute(stmt):
            if normalized_query in _normalize_text(sku or "").lower() or normalized_query in _normalize_text(name or "").lower():
                match_ids.append(product_id)
                if len(match_ids) >= limit:
                    break
        if not match_ids:
            return []

        by_id = {p.id: p for p in self._db.scalars(select(Product).where(Product.id.in_(match_ids)))}
        return [by_id[i] for i in match_ids if i in by_id]

    def add(self, product: Product) -> None:
        self._db.add(product)
