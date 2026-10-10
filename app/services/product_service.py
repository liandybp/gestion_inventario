from __future__ import annotations

import math
from collections.abc import Callable

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import InventoryLot, InventoryMovement, Product
from app.repositories.product_repository import ProductRepository
from app.schemas import ProductCreate, ProductUpdate
from app.services.sku_range import SKURangeExhausted, next_sku_in_range
from app.logger import get_logger

_log = get_logger(__name__)


def _ensure_finite(**fields: float | None) -> None:
    """422 si algún valor es NaN o infinito: Pydantic y float() los aceptan y `nan < 0` es False."""
    for field, value in fields.items():
        if value is not None and not math.isfinite(float(value)):
            raise HTTPException(status_code=422, detail=f"{field} must be a finite number")


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
        # Numeración global: products.sku es unique=True global (ver ProductRepository.list_sku_prefixes).
        existing = self._products.list_sku_prefixes(prefix)
        return next_sku_in_range(existing, self._business_id, prefix=prefix, width=width)

    def _sku_conflict_detail(self, sku_raw: str) -> str:
        """Mensaje del 409 por SKU: products.sku es único GLOBAL (ver list_sku_prefixes)."""
        if not sku_raw:
            return "No se pudo generar un SKU automáticamente"
        if self._products.get_by_sku(sku_raw) is not None:
            return f"El SKU '{sku_raw}' ya existe"
        return f"El SKU '{sku_raw}' ya está en uso en otro negocio"

    def _insert_with_sku_retry(
        self, build: Callable[[str], Product], sku_raw: str, *, commit: bool
    ) -> Product:
        """Inserta `build(sku)` reintentando el SKU automático (5 intentos; explícito: 1).

        commit=False solo hace flush (el llamador confirma). Una colisión hace rollback de
        TODA la transacción: llamar antes de cualquier otra escritura pendiente.
        """
        attempts = 1 if sku_raw else 5
        last_error: Exception | None = None
        for _ in range(attempts):
            try:
                candidate = sku_raw or self._generate_sku()
            except SKURangeExhausted as e:
                raise HTTPException(status_code=409, detail=str(e)) from e
            product = build(candidate)
            self._products.add(product)
            try:
                if commit:
                    self._db.commit()
                    self._db.refresh(product)
                else:
                    self._db.flush()
                return product
            except IntegrityError as e:
                self._db.rollback()
                last_error = e
                _log.warning("SKU collision creating product: %r", candidate)

        _log.error("Could not create product (sku=%r, auto=%s)", sku_raw, not sku_raw)
        raise HTTPException(status_code=409, detail=self._sku_conflict_detail(sku_raw)) from last_error

    def create(self, payload: ProductCreate) -> Product:
        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        _ensure_finite(
            min_stock=payload.min_stock,
            default_purchase_cost=payload.default_purchase_cost,
            default_sale_price=payload.default_sale_price,
        )

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

        def _build(sku: str) -> Product:
            return Product(
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

        return self._insert_with_sku_retry(_build, sku_raw, commit=True)

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

        def _build(candidate: str) -> Product:
            return Product(
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

        return self._insert_with_sku_retry(_build, sku_raw, commit=False)

    def get_by_sku(self, sku: str) -> Product:
        product = self._products.get_by_sku(sku)
        if product is None:
            raise HTTPException(status_code=404, detail="Product not found")
        return product

    def update(self, original_sku: str, payload: ProductUpdate) -> Product:
        product = self.get_by_sku(original_sku)

        if not payload.name.strip():
            raise HTTPException(status_code=422, detail="name must not be empty")
        _ensure_finite(
            min_stock=payload.min_stock,
            default_purchase_cost=payload.default_purchase_cost,
            default_sale_price=payload.default_sale_price,
        )
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
            # Solo un cambio de SKU puede chocar con la unicidad del SKU; si no cambió, la
            # restricción violada es otra y el mensaje de SKU duplicado sería falso.
            if new_sku and new_sku != original_sku.strip():
                raise HTTPException(status_code=409, detail=self._sku_conflict_detail(new_sku))
            raise HTTPException(status_code=409, detail="No se pudo guardar el producto: conflicto con datos existentes")
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
