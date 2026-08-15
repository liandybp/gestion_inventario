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
from .ui_transfers import router as transfers_router
from .ui_home import router as home_router
from .ui_stock import router as stock_router
from .ui_profit_items import router as profit_items_router
from .ui_history import router as history_router
from .ui_activity import router as activity_router
from .ui_dividends import router as dividends_router
from .ui_profit import router as profit_router

router = APIRouter(prefix="/ui", tags=["ui"])

router.include_router(auth_router)
router.include_router(home_router)
router.include_router(products_router)
router.include_router(purchases_router)
router.include_router(sales_router)
router.include_router(sales_documents_router)
router.include_router(customers_router)
router.include_router(transfers_router)
router.include_router(expenses_router)
router.include_router(extractions_router)
router.include_router(stock_router)
router.include_router(profit_items_router)
router.include_router(history_router)
router.include_router(activity_router)
router.include_router(dividends_router)
router.include_router(profit_router)
