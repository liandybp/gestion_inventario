from __future__ import annotations

from fastapi import APIRouter

from .ui_auth import router as auth_router
from .ui_expenses import router as expenses_router
from .ui_extractions import router as extractions_router
from .ui_products import router as products_router
from .ui_purchases import router as purchases_router
from .ui_sales import router as sales_router
from .ui_sales_documents import router as sales_documents_router
from .ui_customers import router as customers_router
from .ui_tabs import router as tabs_router

router = APIRouter(prefix="/ui", tags=["ui"])

router.include_router(auth_router)
router.include_router(tabs_router)
router.include_router(products_router)
router.include_router(purchases_router)
router.include_router(sales_router)
router.include_router(sales_documents_router)
router.include_router(customers_router)
router.include_router(expenses_router)
router.include_router(extractions_router)
