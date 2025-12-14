from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.deps import session_dep
from app.schemas import ProductCreate, ProductRead
from app.services.product_service import ProductService

router = APIRouter(tags=["products"])


def product_service_dep(db: Session = Depends(session_dep)) -> ProductService:
    return ProductService(db)


@router.post("/products", response_model=ProductRead)
def create_product(
    payload: ProductCreate,
    service: ProductService = Depends(product_service_dep),
) -> ProductRead:
    return ProductRead.model_validate(service.create(payload))


@router.get("/products", response_model=list[ProductRead])
def list_products(service: ProductService = Depends(product_service_dep)) -> list[ProductRead]:
    return [ProductRead.model_validate(p) for p in service.list()]
