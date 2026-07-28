from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models import User
from app.routers import inventory as inventory_router
from app.routers import products as products_router
from app.schemas import (
    AdjustmentCreate,
    MovementRead,
    MovementResult,
    ProductCreate,
    ProductRead,
    PurchaseCreate,
    SaleCreate,
    StockRead,
    SupplierReturnLotCreate,
    TransferCreate,
    TransferLineCreate,
    TransferLineResult,
    TransferResult,
)


class ProductServiceFake:
    def __init__(self, db: Session):
        self.db = db

    def create(self, payload: ProductCreate):
        return type(
            "P",
            (),
            {
                "id": 1,
                "sku": payload.sku or "SKU1",
                "name": payload.name,
                "category": payload.category,
                "min_stock": payload.min_stock,
                "unit_of_measure": payload.unit_of_measure,
                "default_purchase_cost": payload.default_purchase_cost,
                "default_sale_price": payload.default_sale_price,
                "lead_time_days": payload.lead_time_days,
                "image_url": payload.image_url,
            },
        )()

    def list(self):
        return [
            type(
                "P",
                (),
                {
                    "id": 2,
                    "sku": "SKU2",
                    "name": "Producto 2",
                    "category": None,
                    "min_stock": 0,
                    "unit_of_measure": "ud",
                    "default_purchase_cost": 1.0,
                    "default_sale_price": 2.0,
                    "lead_time_days": 0,
                    "image_url": None,
                },
            )()
        ]


class InventoryServiceFake:
    def __init__(self, db: Session):
        self.db = db

    def _mv_result(self, *, mv_type: str, qty: float) -> MovementResult:
        movement = MovementRead(
            id=1,
            product_id=1,
            location_id=None,
            type=mv_type,
            quantity=qty,
            unit_cost=1.0,
            unit_price=2.0,
            movement_date=datetime.now(timezone.utc),
            note=None,
        )
        return MovementResult(movement=movement, stock_after=10.0, warning=None)

    def purchase(self, payload: PurchaseCreate) -> MovementResult:
        return self._mv_result(mv_type="purchase", qty=payload.quantity)

    def supplier_return_by_lot(self, payload: SupplierReturnLotCreate) -> MovementResult:
        return self._mv_result(mv_type="return_supplier", qty=-payload.quantity)

    def transfer(self, payload: TransferCreate) -> TransferResult:
        return TransferResult(
            from_location_code=payload.from_location_code,
            to_location_code=payload.to_location_code,
            transfer_ref="TP-1",
            lines=[
                TransferLineResult(
                    sku=payload.lines[0].sku,
                    quantity=payload.lines[0].quantity,
                    movements_out=[10],
                    movements_in=[11],
                )
            ],
        )

    def sale(self, payload: SaleCreate) -> MovementResult:
        return self._mv_result(mv_type="sale", qty=-payload.quantity)

    def adjustment(self, payload: AdjustmentCreate) -> MovementResult:
        return self._mv_result(mv_type="adjustment", qty=payload.quantity_delta)

    def stock(self, sku: str) -> StockRead:
        return StockRead(sku=sku, name="P", quantity=5.0, min_stock=1.0, needs_restock=False)

    def stock_list(self) -> list[StockRead]:
        return [StockRead(sku="SKU1", name="P", quantity=5.0, min_stock=1.0, needs_restock=False)]


def _mk_user(db: Session, business_id: int) -> User:
    user = User(
        username="api-user",
        password_hash="x",
        role="admin",
        business_id=business_id,
        is_active=True,
        must_change_password=False,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def test_products_router_functions(db_session: Session, business_id: int) -> None:
    user = _mk_user(db_session, business_id)
    service = ProductServiceFake(db_session)

    created = products_router.create_product(
        ProductCreate(name="A", default_purchase_cost=1.0, default_sale_price=2.0),
        user=user,
        service=service,
    )
    listed = products_router.list_products(user=user, service=service)

    assert isinstance(created, ProductRead)
    assert created.name == "A"
    assert len(listed) == 1
    assert listed[0].sku == "SKU2"


def test_inventory_router_functions(db_session: Session, business_id: int) -> None:
    user = _mk_user(db_session, business_id)
    service = InventoryServiceFake(db_session)

    r1 = inventory_router.create_purchase(PurchaseCreate(sku="SKU1", quantity=1), user=user, service=service)
    r2 = inventory_router.create_supplier_return_lot(
        SupplierReturnLotCreate(lot_id=1, quantity=1), user=user, service=service
    )
    r3 = inventory_router.create_transfer(
        TransferCreate(to_location_code="POS1", lines=[TransferLineCreate(sku="SKU1", quantity=1)]),
        user=user,
        service=service,
    )
    r4 = inventory_router.create_sale(SaleCreate(sku="SKU1", quantity=1), user=user, service=service)
    r5 = inventory_router.create_adjustment(
        AdjustmentCreate(sku="SKU1", quantity_delta=1), user=user, service=service
    )
    stock = inventory_router.get_stock("SKU1", user=user, service=service)
    stock_list = inventory_router.list_stock(user=user, service=service)

    assert r1.movement.type == "purchase"
    assert r2.movement.type == "return_supplier"
    assert r3.transfer_ref == "TP-1"
    assert r4.movement.type == "sale"
    assert r5.movement.type == "adjustment"
    assert stock.sku == "SKU1"
    assert len(stock_list) == 1

