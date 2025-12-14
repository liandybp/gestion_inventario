from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Product
from app.repositories.product_repository import ProductRepository
from app.schemas import ProductCreate, ProductUpdate


class ProductService:
    def __init__(self, db: Session):
        self._db = db
        self._products = ProductRepository(db)

    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        existing = self._products.list_skus_starting_with(prefix)
        max_n = 0
        for sku in existing:
            suffix = sku[len(prefix) :]
            if suffix.isdigit():
                max_n = max(max_n, int(suffix))
        return f"{prefix}{str(max_n + 1).zfill(width)}"

    def create(self, payload: ProductCreate) -> Product:
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")

        if payload.min_stock < 0:
            raise HTTPException(status_code=422, detail="min_stock must be >= 0")
        if payload.default_sale_price is not None and payload.default_sale_price < 0:
            raise HTTPException(
                status_code=422, detail="default_sale_price must be >= 0"
            )
        if payload.default_purchase_cost is not None and payload.default_purchase_cost < 0:
            raise HTTPException(
                status_code=422, detail="default_purchase_cost must be >= 0"
            )

        if payload.default_purchase_cost is None:
            raise HTTPException(status_code=422, detail="default_purchase_cost is required")
        if payload.default_sale_price is None:
            raise HTTPException(status_code=422, detail="default_sale_price is required")

        sku = payload.sku.strip() if payload.sku else ""
        if not sku:
            sku = self._generate_sku()

        product = Product(
            sku=sku,
            name=payload.name.strip(),
            category=payload.category.strip() if payload.category else None,
            min_stock=payload.min_stock,
            unit_of_measure=payload.unit_of_measure.strip()
            if payload.unit_of_measure and payload.unit_of_measure.strip()
            else None,
            default_purchase_cost=payload.default_purchase_cost,
            default_sale_price=payload.default_sale_price,
            image_url=payload.image_url.strip()
            if payload.image_url and payload.image_url.strip()
            else None,
        )
        self._products.add(product)
        try:
            self._db.commit()
        except IntegrityError:
            self._db.rollback()
            raise HTTPException(status_code=409, detail="SKU already exists")
        self._db.refresh(product)
        return product

    def get_by_sku(self, sku: str) -> Product:
        product = self._products.get_by_sku(sku)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

    def update(self, original_sku: str, payload: ProductUpdate) -> Product:
        product = self.get_by_sku(original_sku)

        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        if payload.min_stock < 0:
            raise HTTPException(status_code=422, detail="min_stock must be >= 0")
        if payload.default_sale_price is not None and payload.default_sale_price < 0:
            raise HTTPException(
                status_code=422, detail="default_sale_price must be >= 0"
            )
        if payload.default_purchase_cost is not None and payload.default_purchase_cost < 0:
            raise HTTPException(
                status_code=422, detail="default_purchase_cost must be >= 0"
            )

        new_sku = payload.sku.strip() if payload.sku else ""
        if new_sku:
            product.sku = new_sku
        product.name = payload.name.strip()
        product.category = payload.category.strip() if payload.category else None
        product.min_stock = payload.min_stock
        product.unit_of_measure = (
            payload.unit_of_measure.strip()
            if payload.unit_of_measure and payload.unit_of_measure.strip()
            else None
        )
        product.default_purchase_cost = payload.default_purchase_cost
        product.default_sale_price = payload.default_sale_price
        product.image_url = (
            payload.image_url.strip()
            if payload.image_url and payload.image_url.strip()
            else None
        )

        try:
            self._db.commit()
        except IntegrityError:
            self._db.rollback()
            raise HTTPException(status_code=409, detail="SKU already exists")
        self._db.refresh(product)
        return product

    def list(self) -> list[Product]:
        return self._products.list()

    def search(self, query: str, limit: int = 20) -> list[Product]:
        return self._products.search(query=query, limit=limit)

    def recent(self, limit: int = 20) -> list[Product]:
        return list(self._db.scalars(select(Product).order_by(Product.id.desc()).limit(limit)))
