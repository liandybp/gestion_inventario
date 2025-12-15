from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import SQLAlchemyError

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

os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.on_event("startup")
def on_startup() -> None:
    Base.metadata.create_all(bind=engine)

    with engine.connect() as conn:
        try:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_inventory_lots_lot_code ON inventory_lots(lot_code)"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear el índice UNIQUE para lot_code (hay lotes duplicados en la BD)."
            ) from e

        try:
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_products_sku ON products(sku)"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear el índice UNIQUE para SKU (hay SKUs duplicados en la BD)."
            ) from e

        cols = {
            row[1]
            for row in conn.exec_driver_sql("PRAGMA table_info(products)").fetchall()
        }
        if "unit_of_measure" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN unit_of_measure VARCHAR(32)"
            )
        if "default_purchase_cost" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN default_purchase_cost NUMERIC(14, 4)"
            )
        if "image_url" not in cols:
            conn.exec_driver_sql(
                "ALTER TABLE products ADD COLUMN image_url VARCHAR(512)"
            )
