from __future__ import annotations

import math

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, Product
from app.repositories.product_repository import ProductRepository
from app.repositories.purchase_repository import PurchaseRepository
from app.schemas import ProductCreate, ProductUpdate
from app.services.sku_range import SKURangeExhausted, next_sku_in_range
from app.logger import get_logger

_log = get_logger(__name__)


class ProductService:
    def __init__(self, db: Session, business_id: int | None = None):
        self._db = db
        if business_id is None:
            raise HTTPException(status_code=409, detail="business_id is required")
        self._business_id = int(business_id)
        self._products = ProductRepository(db, business_id=self._business_id)

    @property
    def db(self) -> Session:
        return self._db

    def _generate_sku(self, prefix: str = "SKU", width: int = 6) -> str:
        # Numeración global: products.sku es unique=True global (ver PurchaseRepository.list_sku_prefixes).
        existing = PurchaseRepository(self._db, self._business_id).list_sku_prefixes(prefix)
        return next_sku_in_range(existing, self._business_id, prefix=prefix, width=width)

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

        if payload.lead_time_days is not None and int(payload.lead_time_days) < 0:
            raise HTTPException(status_code=422, detail="lead_time_days must be >= 0")

        if payload.default_purchase_cost is None:
            raise HTTPException(status_code=422, detail="default_purchase_cost is required")
        if payload.default_sale_price is None:
            raise HTTPException(status_code=422, detail="default_sale_price is required")

        if int(payload.lead_time_days or 0) < 0:
            raise HTTPException(status_code=422, detail="lead_time_days must be >= 0")

        sku_raw = payload.sku.strip() if payload.sku else ""
        auto_sku = not bool(sku_raw)

        attempts = 5 if auto_sku else 1
        last_error: Exception | None = None
        for _ in range(attempts):
            try:
                sku = sku_raw or self._generate_sku()
            except SKURangeExhausted as e:
                raise HTTPException(status_code=409, detail=str(e)) from e

            product = Product(
                business_id=self._business_id,
                sku=sku,
                name=payload.name.strip(),
                category=payload.category.strip() if payload.category else None,
                min_stock=payload.min_stock,
                unit_of_measure=payload.unit_of_measure.strip()
                if payload.unit_of_measure and payload.unit_of_measure.strip()
                else None,
                default_purchase_cost=payload.default_purchase_cost,
                default_sale_price=payload.default_sale_price,
                lead_time_days=int(payload.lead_time_days or 0),
                image_url=payload.image_url.strip()
                if payload.image_url and payload.image_url.strip()
                else None,
            )
            self._products.add(product)
            try:
                self._db.commit()
                self._db.refresh(product)
                return product
            except IntegrityError as e:
                self._db.rollback()
                last_error = e
                if not auto_sku:
                    break

        raise HTTPException(status_code=409, detail="SKU already exists") from last_error

    def create_for_purchase(
        self,
        *,
        name: str,
        sku: str | None,
        purchase_cost: float | None,
        sale_price: float | None,
        category: str | None = None,
    ) -> Product:
        """Crea el producto nuevo de una compra manual sin confirmar la transacción.

        Hace flush, no commit: el llamador confirma producto y compra en un único commit
        (o hace rollback si la compra falla). Debe llamarse antes de cualquier otra
        escritura pendiente en la sesión, porque una colisión de SKU hace rollback.
        """
        sku_raw = (sku or "").strip()
        clean_name = (name or "").strip() or sku_raw
        if not clean_name:
            raise HTTPException(
                status_code=422, detail="Debes ingresar 'Artículo' (SKU) o 'Nombre (si es nuevo)'"
            )
        if purchase_cost is None:
            raise HTTPException(status_code=422, detail="Para un producto nuevo indica el costo unitario")
        if not math.isfinite(purchase_cost) or purchase_cost < 0:
            raise HTTPException(status_code=422, detail="El costo unitario debe ser un número mayor o igual que 0")
        if sale_price is not None and (not math.isfinite(sale_price) or sale_price < 0):
            raise HTTPException(status_code=422, detail="El precio de venta debe ser un número mayor o igual que 0")

        auto_sku = not sku_raw
        last_error: Exception | None = None
        for _ in range(5 if auto_sku else 1):
            try:
                candidate = sku_raw or self._generate_sku()
            except SKURangeExhausted as e:
                raise HTTPException(status_code=409, detail=str(e)) from e
            product = Product(
                business_id=self._business_id,
                sku=candidate,
                name=clean_name,
                category=(category or "").strip() or None,
                min_stock=0,
                unit_of_measure=None,
                default_purchase_cost=purchase_cost,
                default_sale_price=sale_price if sale_price is not None else 0,
                lead_time_days=0,
                image_url=None,
            )
            self._products.add(product)
            try:
                self._db.flush()
                return product
            except IntegrityError as e:
                self._db.rollback()
                last_error = e
                _log.warning("SKU collision creating product from purchase: %r", candidate)

        _log.error("Could not create product from purchase (sku=%r, auto=%s)", sku_raw, auto_sku)
        detail = "No se pudo generar un SKU automáticamente" if auto_sku else f"El SKU '{sku_raw}' ya existe"
        raise HTTPException(status_code=409, detail=detail) from last_error

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
        if payload.lead_time_days is not None:
            product.lead_time_days = int(payload.lead_time_days)
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
        stmt = select(Product)
        if self._business_id is not None:
            stmt = stmt.where(Product.business_id == self._business_id)
        return list(self._db.scalars(stmt.order_by(Product.id.desc()).limit(limit)))

    def delete(self, sku: str) -> None:
        product = self.get_by_sku(sku)

        has_movements = (
            self._db.scalar(
                select(InventoryMovement.id)
                .where(InventoryMovement.product_id == product.id)
                .limit(1)
            )
            is not None
        )
        has_lots = (
            self._db.scalar(
                select(InventoryLot.id)
                .where(InventoryLot.product_id == product.id)
                .limit(1)
            )
            is not None
        )
        if has_movements or has_lots:
            raise HTTPException(
                status_code=409,
                detail="No se puede eliminar el producto porque tiene movimientos/lotes registrados.",
            )

        self._db.delete(product)
        self._db.commit()
