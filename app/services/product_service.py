from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Product
from app.repositories.product_repository import ProductRepository
from app.schemas import ProductCreate


class ProductService:
    def __init__(self, db: Session):
        self._db = db
        self._products = ProductRepository(db)

    def create(self, payload: ProductCreate) -> Product:
        if payload.min_stock < 0:
            raise HTTPException(status_code=422, detail="min_stock must be >= 0")
        if payload.default_sale_price is not None and payload.default_sale_price < 0:
            raise HTTPException(
                status_code=422, detail="default_sale_price must be >= 0"
            )

        product = Product(
            sku=payload.sku.strip(),
            name=payload.name.strip(),
            category=payload.category.strip() if payload.category else None,
            min_stock=payload.min_stock,
            default_sale_price=payload.default_sale_price,
        )
        self._products.add(product)
        try:
            self._db.commit()
        except IntegrityError:
            self._db.rollback()
            raise HTTPException(status_code=409, detail="SKU already exists")
        self._db.refresh(product)
        return product

    def list(self) -> list[Product]:
        return self._products.list()
