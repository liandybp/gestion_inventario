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
        product = Product(sku=payload.sku.strip(), name=payload.name.strip())
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
