from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.audit import log_event
from app.deps import session_dep
from app.models import User
from app.schemas import ProductCreate, ProductRead
from app.security import require_user_api
from app.services.product_service import ProductService

router = APIRouter(tags=["products"])


def product_service_dep(db: Session = Depends(session_dep)) -> ProductService:
    return ProductService(db)


@router.post("/products", response_model=ProductRead)
def create_product(
    payload: ProductCreate,
    user: User = Depends(require_user_api),
    service: ProductService = Depends(product_service_dep),
) -> ProductRead:
    created = service.create(payload)
    log_event(
        service._db,
        user,
        action="product_create",
        entity_type="product",
        entity_id=created.sku,
        detail={"name": created.name},
    )
    return ProductRead.model_validate(created)


@router.get("/products", response_model=list[ProductRead])
def list_products(
    user: User = Depends(require_user_api),
    service: ProductService = Depends(product_service_dep),
) -> list[ProductRead]:
    return [ProductRead.model_validate(p) for p in service.list()]
