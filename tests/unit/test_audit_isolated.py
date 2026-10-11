"""log_event(isolated=True): un fallo de auditoría no revierte la transacción del llamador."""

from __future__ import annotations

from sqlalchemy.orm import Session

from app.audit import log_event
from app.models import AuditLog, Product
from app.services.product_service import ProductService


def _new_product(db: Session, business_id: int, name: str) -> Product:
    return ProductService(db, business_id=business_id).create_for_purchase(
        name=name, sku=None, purchase_cost=1.0, sale_price=None
    )


def test_isolated_audit_failure_keeps_caller_transaction(db_session: Session, business_id: int) -> None:
    _new_product(db_session, business_id, "Con auditoría aislada")

    # entity_type es NOT NULL: el flush del evento falla dentro del SAVEPOINT.
    log_event(db_session, None, action="product_create", entity_type=None, isolated=True)  # type: ignore[arg-type]
    db_session.commit()

    assert db_session.query(Product).filter(Product.name == "Con auditoría aislada").count() == 1
    assert db_session.query(AuditLog).count() == 0


def test_isolated_audit_success_is_committed_with_caller(db_session: Session, business_id: int) -> None:
    p = _new_product(db_session, business_id, "Con auditoría ok")

    log_event(db_session, None, action="product_create", entity_type="product", entity_id=p.sku, isolated=True)
    db_session.commit()

    assert db_session.query(AuditLog).filter(AuditLog.entity_id == p.sku).count() == 1


def test_non_isolated_audit_failure_rolls_back_caller_transaction(db_session: Session, business_id: int) -> None:
    """Comportamiento por defecto (sin cambios): el fallo revierte todo lo pendiente."""
    _new_product(db_session, business_id, "Sin aislar")

    log_event(db_session, None, action="product_create", entity_type=None)  # type: ignore[arg-type]
    db_session.commit()

    assert db_session.query(Product).filter(Product.name == "Sin aislar").count() == 0
