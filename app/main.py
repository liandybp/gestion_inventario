from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, text

if __name__ == "__main__" and __package__ is None:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.db import Base, engine
from app.db import get_session
from app.auth import hash_password
from app.business_config import load_business_config
from app.routers.health import router as health_router
from app.routers.inventory import router as inventory_router
from app.routers.products import router as products_router
from app.routers.ui import router as ui_router
from app.routers import (
    inventory,
    products,
    ui_auth,
    ui_expenses,
    ui_extractions,
    ui_products,
    ui_purchases,
    ui_sales,
    ui_sales_documents,
    ui_tabs,
    ui_transfers,
    ui_users,
)
from app.migrations import run_startup_tasks
from app.models import (
    Business,
    Customer,
    InventoryLot,
    InventoryMovement,
    Location,
    MoneyExtraction,
    OperatingExpense,
    Product,
    SalesDocument,
    User,
)
from app.utils import get_session_secret


_LATEST_SCHEMA_VERSION = 2


def _ensure_schema_version_table(conn) -> None:
    dialect = conn.dialect.name
    if dialect == "sqlite":
        conn.exec_driver_sql(
            "CREATE TABLE IF NOT EXISTS schema_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)"
        )
        conn.exec_driver_sql(
            "INSERT OR IGNORE INTO schema_version (id, version) VALUES (1, 0)"
        )
        return
    conn.exec_driver_sql(
        "CREATE TABLE IF NOT EXISTS schema_version (id INTEGER PRIMARY KEY, version INTEGER NOT NULL)"
    )
    conn.exec_driver_sql(
        "INSERT INTO schema_version (id, version) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"
    )


def _get_schema_version(conn) -> int:
    row = conn.execute(text("SELECT version FROM schema_version WHERE id = 1")).fetchone()
    return int(row[0]) if row else 0


def _set_schema_version(conn, version: int) -> None:
    conn.execute(text("UPDATE schema_version SET version = :v WHERE id = 1"), {"v": int(version)})


def _run_startup_tasks() -> None:
    """Ejecuta tareas de inicialización de la base de datos."""
    Base.metadata.create_all(bind=engine)

    schema_version = 0
    with engine.begin() as conn:
        _ensure_schema_version_table(conn)
        schema_version = _get_schema_version(conn)

    if engine.dialect.name == "sqlite":
        with engine.begin() as conn:
            _ensure_schema_version_table(conn)
            schema_version = _get_schema_version(conn)
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

            try:
                conn.exec_driver_sql(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ux_users_username ON users(username)"
                )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear el índice UNIQUE para users.username (hay usuarios duplicados en la BD)."
                ) from e

            user_cols = {
                row[1] for row in conn.exec_driver_sql("PRAGMA table_info(users)").fetchall()
            }
            if "business_id" not in user_cols:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN business_id INTEGER")
            if "role" not in user_cols:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN role VARCHAR(16)")
                conn.exec_driver_sql(
                    "UPDATE users SET role='admin' WHERE role IS NULL OR role=''"
                )
            if "must_change_password" not in user_cols:
                conn.exec_driver_sql("ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT 0")

            for tbl in [
                "products",
                "customers",
                "locations",
                "sales_documents",
                "inventory_movements",
                "inventory_lots",
                "operating_expenses",
                "money_extractions",
            ]:
                cols = {row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({tbl})").fetchall()}
                if "business_id" not in cols:
                    conn.exec_driver_sql(f"ALTER TABLE {tbl} ADD COLUMN business_id INTEGER")
                try:
                    conn.exec_driver_sql(
                        f"CREATE INDEX IF NOT EXISTS ix_{tbl}_business_id ON {tbl}(business_id)"
                    )
                except SQLAlchemyError:
                    pass

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
            if "lead_time_days" not in cols:
                conn.exec_driver_sql(
                    "ALTER TABLE products ADD COLUMN lead_time_days INTEGER DEFAULT 0"
                )

            sales_doc_cols = {
                row[1]
                for row in conn.exec_driver_sql("PRAGMA table_info(sales_documents)").fetchall()
            }
            if "customer_id" not in sales_doc_cols:
                conn.exec_driver_sql(
                    "ALTER TABLE sales_documents ADD COLUMN customer_id INTEGER"
                )
            conn.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_sales_documents_customer_id ON sales_documents(customer_id)"
            )

            if "location_id" not in sales_doc_cols:
                conn.exec_driver_sql(
                    "ALTER TABLE sales_documents ADD COLUMN location_id INTEGER"
                )
                conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_sales_documents_location_id ON sales_documents(location_id)"
                )

            mv_cols = {
                row[1]
                for row in conn.exec_driver_sql("PRAGMA table_info(inventory_movements)").fetchall()
            }
            if "location_id" not in mv_cols:
                conn.exec_driver_sql(
                    "ALTER TABLE inventory_movements ADD COLUMN location_id INTEGER"
                )

            try:
                conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_inventory_movements_location_id ON inventory_movements(location_id)"
                )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear el índice para inventory_movements.location_id."
                ) from e

            lot_cols = {
                row[1] for row in conn.exec_driver_sql("PRAGMA table_info(inventory_lots)").fetchall()
            }
            if "location_id" not in lot_cols:
                conn.exec_driver_sql(
                    "ALTER TABLE inventory_lots ADD COLUMN location_id INTEGER"
                )
            try:
                conn.exec_driver_sql(
                    "CREATE INDEX IF NOT EXISTS ix_inventory_lots_location_id ON inventory_lots(location_id)"
                )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear el índice para inventory_lots.location_id."
                ) from e

            if schema_version < 1:
                _set_schema_version(conn, 1)
                schema_version = 1

    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            _ensure_schema_version_table(conn)
            schema_version = _get_schema_version(conn)
            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='role'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(16)"
                    )
                    conn.exec_driver_sql(
                        "UPDATE users SET role='admin' WHERE role IS NULL OR role=''"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna role en users."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='business_id'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS business_id INTEGER"
                    )
                    conn.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_users_business_id ON users(business_id)"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna business_id en users."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='must_change_password'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna must_change_password en users."
                ) from e

            for tbl in [
                "products",
                "customers",
                "locations",
                "sales_documents",
                "inventory_movements",
                "inventory_lots",
                "operating_expenses",
                "money_extractions",
            ]:
                try:
                    exists = conn.exec_driver_sql(
                        f"SELECT 1 FROM information_schema.columns WHERE table_name='{tbl}' AND column_name='business_id'"
                    ).fetchone()
                    if exists is None:
                        conn.exec_driver_sql(
                            f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS business_id INTEGER"
                        )
                        conn.exec_driver_sql(
                            f"CREATE INDEX IF NOT EXISTS ix_{tbl}_business_id ON {tbl}(business_id)"
                        )
                except SQLAlchemyError as e:
                    raise RuntimeError(
                        f"No se pudo crear la columna business_id en {tbl}."
                    ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='products' AND column_name='lead_time_days'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE products ADD COLUMN lead_time_days INTEGER NOT NULL DEFAULT 0"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna lead_time_days en products."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='sales_documents' AND column_name='customer_id'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE sales_documents ADD COLUMN customer_id INTEGER"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna customer_id en sales_documents."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='sales_documents' AND column_name='location_id'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE sales_documents ADD COLUMN IF NOT EXISTS location_id INTEGER"
                    )
                    conn.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_sales_documents_location_id ON sales_documents(location_id)"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna location_id en sales_documents."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='inventory_movements' AND column_name='location_id'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE inventory_movements ADD COLUMN IF NOT EXISTS location_id INTEGER"
                    )
                    conn.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_inventory_movements_location_id ON inventory_movements(location_id)"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna location_id en inventory_movements."
                ) from e

            try:
                exists = conn.exec_driver_sql(
                    "SELECT 1 FROM information_schema.columns WHERE table_name='inventory_lots' AND column_name='location_id'"
                ).fetchone()
                if exists is None:
                    conn.exec_driver_sql(
                        "ALTER TABLE inventory_lots ADD COLUMN IF NOT EXISTS location_id INTEGER"
                    )
                    conn.exec_driver_sql(
                        "CREATE INDEX IF NOT EXISTS ix_inventory_lots_location_id ON inventory_lots(location_id)"
                    )
            except SQLAlchemyError as e:
                raise RuntimeError(
                    "No se pudo crear la columna location_id en inventory_lots."
                ) from e

            if schema_version < 1:
                _set_schema_version(conn, 1)
                schema_version = 1

    if schema_version >= 2:
        return

    db = get_session()
    try:
        config = load_business_config("recambios")

        def ensure_business(code: str, name: str) -> Business:
            c = (code or "").strip()
            n = (name or "").strip() or c
            existing_b = db.scalar(select(Business).where(Business.code == c))
            if existing_b is None:
                existing_b = Business(code=c, name=n)
                db.add(existing_b)
                db.flush()
            else:
                existing_b.name = n
            return existing_b

        default_business = ensure_business("recambios", "Recambios")
        ensure_business("ropa", "Ropa")

        users_to_ensure = [
            {
                "username": os.getenv("ADMIN_USERNAME", "admin"),
                "password": os.getenv("ADMIN_PASSWORD", "admin"),
                "role": "admin",
                "is_active": True,
            },
            {
                "username": os.getenv("OPERATOR_USERNAME", "operator"),
                "password": os.getenv("OPERATOR_PASSWORD", "operator"),
                "role": "operator",
                "is_active": True,
            },
        ]

        for spec in users_to_ensure:
            username = (spec.get("username") or "").strip()
            if not username:
                continue
            existing = db.scalar(select(User).where(User.username == username))
            if existing is None:
                db.add(
                    User(
                        username=username,
                        password_hash=hash_password(str(spec.get("password") or "")),
                        role=str(spec.get("role") or "operator"),
                        is_active=bool(spec.get("is_active", True)),
                        business_id=int(default_business.id),
                    )
                )
            else:
                existing.role = str(spec.get("role") or existing.role or "operator")
                existing.is_active = bool(spec.get("is_active", True))
                if existing.business_id is None:
                    existing.business_id = int(default_business.id)
        db.commit()

        # Ensure locations from config
        central = config.locations.central
        pos_list = list(config.locations.pos or [])
        default_pos_code = str(config.locations.default_pos or "POS1").strip() or "POS1"

        def ensure_location(code: str, name: str) -> Location:
            c = (code or "").strip()
            n = (name or "").strip() or c
            existing_loc = db.scalar(select(Location).where(Location.code == c))
            if existing_loc is None:
                existing_loc = Location(code=c, name=n, business_id=int(default_business.id))
                db.add(existing_loc)
                db.flush()
            else:
                existing_loc.name = n
                if existing_loc.business_id is None:
                    existing_loc.business_id = int(default_business.id)
            return existing_loc

        central_loc = ensure_location(central.code, central.name)
        pos_locs: dict[str, Location] = {}
        for p in pos_list:
            pos_locs[p.code] = ensure_location(p.code, p.name)
        if default_pos_code not in pos_locs:
            first = pos_list[0] if pos_list else None
            if first is not None:
                default_pos_code = first.code

        default_pos_loc = pos_locs.get(default_pos_code)
        if default_pos_loc is None:
            default_pos_loc = ensure_location(default_pos_code, default_pos_code)
            pos_locs[default_pos_code] = default_pos_loc

        # Backfill existing rows
        db.query(Product).filter(Product.business_id.is_(None)).update(
            {Product.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(Customer).filter(Customer.business_id.is_(None)).update(
            {Customer.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(Location).filter(Location.business_id.is_(None)).update(
            {Location.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(InventoryMovement).filter(InventoryMovement.business_id.is_(None)).update(
            {InventoryMovement.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(InventoryLot).filter(InventoryLot.business_id.is_(None)).update(
            {InventoryLot.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(SalesDocument).filter(SalesDocument.business_id.is_(None)).update(
            {SalesDocument.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(OperatingExpense).filter(OperatingExpense.business_id.is_(None)).update(
            {OperatingExpense.business_id: int(default_business.id)}, synchronize_session=False
        )
        db.query(MoneyExtraction).filter(MoneyExtraction.business_id.is_(None)).update(
            {MoneyExtraction.business_id: int(default_business.id)}, synchronize_session=False
        )

        db.query(InventoryMovement).filter(InventoryMovement.location_id.is_(None)).update(
            {InventoryMovement.location_id: central_loc.id}, synchronize_session=False
        )
        db.query(InventoryLot).filter(InventoryLot.location_id.is_(None)).update(
            {InventoryLot.location_id: central_loc.id}, synchronize_session=False
        )
        db.query(SalesDocument).filter(SalesDocument.location_id.is_(None)).update(
            {SalesDocument.location_id: default_pos_loc.id}, synchronize_session=False
        )
        db.commit()
    finally:
        db.close()

    with engine.begin() as conn:
        _ensure_schema_version_table(conn)
        _set_schema_version(conn, _LATEST_SCHEMA_VERSION)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifecycle manager para FastAPI."""
    run_startup_tasks()
    yield


app = FastAPI(title="Inventario", lifespan=lifespan)


@app.middleware("http")
async def ui_auth_middleware(request, call_next):
    path = request.url.path or ""
    if path.startswith("/static") or path == "/health":
        return await call_next(request)
    if path.startswith("/ui") and path not in ("/ui/login", "/ui/logout"):
        session = getattr(request, "session", None) or {}
        if not session.get("username"):
            if request.headers.get("HX-Request") == "true":
                resp = RedirectResponse(url="/ui/login", status_code=302)
                resp.headers["HX-Redirect"] = "/ui/login"
                return resp
            return RedirectResponse(url="/ui/login", status_code=302)
    return await call_next(request)


app.add_middleware(
    SessionMiddleware,
    secret_key=get_session_secret(),
    session_cookie="inventario_session",
    max_age=60 * 60 * 24 * 7,
    same_site="lax",
    https_only=os.getenv("SESSION_HTTPS_ONLY", "0") == "1",
)

app.include_router(health_router)
app.include_router(products_router)
app.include_router(inventory_router)
app.include_router(ui_router)
app.include_router(ui_tabs.router, prefix="/ui")
app.include_router(ui_auth.router, prefix="/ui")
app.include_router(ui_products.router, prefix="/ui")
app.include_router(ui_purchases.router, prefix="/ui")
app.include_router(ui_sales.router, prefix="/ui")
app.include_router(ui_transfers.router, prefix="/ui")
app.include_router(ui_expenses.router, prefix="/ui")
app.include_router(ui_extractions.router, prefix="/ui")
app.include_router(ui_sales_documents.router, prefix="/ui")
app.include_router(ui_users.router, prefix="/ui")

os.makedirs("app/static/uploads", exist_ok=True)
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/")
async def root(request: Request):
    """Redirige a dashboard si hay sesión activa, sino a login."""
    session = getattr(request, "session", None) or {}
    if session.get("username"):
        return RedirectResponse(url="/ui/dashboard", status_code=302)
    return RedirectResponse(url="/ui/login", status_code=302)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", "10000")),
        reload=os.getenv("RELOAD", "1") == "1",
    )
