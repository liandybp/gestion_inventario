from __future__ import annotations

from fastapi import FastAPI

from app.db import Base, engine
from app.routers.health import router as health_router
from app.routers.inventory import router as inventory_router
from app.routers.products import router as products_router
from app.routers.ui import router as ui_router

app = FastAPI(title="Inventario")

app.include_router(health_router)
app.include_router(products_router)
app.include_router(inventory_router)
app.include_router(ui_router)


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)
