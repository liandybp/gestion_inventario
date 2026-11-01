"""H-710: permisos por rol en Clientes, Documentos y formularios de edición (decisión 2026-11-01).

- Operador: vende, crea y edita clientes (no los borra), y crea/edita/borra documentos de venta.
- Admin y owner: todo lo anterior y además borran clientes.
El usuario se resuelve de verdad desde la sesión (sin parchear la autorización).
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.auth import hash_password
from app.models import Customer, InventoryMovement, OperatingExpense, User
from app.routers import ui_customers, ui_expenses, ui_sales


def _user(db: Session, business_id: int, role: str) -> None:
    db.add(User(username=role, password_hash=hash_password(role), role=role, is_active=True,
                business_id=business_id))
    db.commit()


def _request(username: str) -> MagicMock:
    req = MagicMock()
    req.session = {"username": username, "active_business_id": None}
    return req


def _customer(db: Session, business_id: int) -> Customer:
    c = Customer(business_id=business_id, client_id="C-RBAC", name="Cliente RBAC")
    db.add(c)
    db.commit()
    return c


def test_operator_cannot_delete_customer(db_session, business_id) -> None:
    _user(db_session, business_id, "operator")
    customer = _customer(db_session, business_id)

    with pytest.raises(HTTPException) as exc:
        ui_customers.customer_delete(_request("operator"), customer_id=customer.id, db=db_session)

    assert exc.value.status_code == 403
    assert db_session.get(Customer, customer.id) is not None


@pytest.mark.parametrize("role", ["admin", "owner"])
def test_admin_and_owner_delete_customer(db_session, business_id, role) -> None:
    _user(db_session, business_id, role)
    customer = _customer(db_session, business_id)

    response = ui_customers.customer_delete(_request(role), customer_id=customer.id, db=db_session)

    assert response.status_code == 200
    assert db_session.get(Customer, customer.id) is None


@pytest.mark.parametrize(("role", "sees_delete"), [("operator", False), ("owner", True), ("admin", True)])
def test_customers_tab_open_to_all_roles_delete_button_by_role(db_session, business_id, role, sees_delete) -> None:
    _user(db_session, business_id, role)
    _customer(db_session, business_id)

    response = ui_customers.tab_customers(_request(role), db=db_session, query="")

    body = response.body.decode("utf-8")
    assert response.status_code == 200
    assert "Cliente RBAC" in body
    assert ("/delete" in body) is sees_delete


def test_operator_cannot_open_expense_edit_form(db_session, business_id) -> None:
    _user(db_session, business_id, "operator")
    expense = OperatingExpense(business_id=business_id, expense_date=datetime.now(timezone.utc),
                               amount=10.0, concept="Luz")
    db_session.add(expense)
    db_session.commit()

    with pytest.raises(HTTPException) as exc:
        ui_expenses.expense_edit_form(_request("operator"), expense_id=expense.id, db=db_session)

    assert exc.value.status_code == 403


def test_operator_cannot_open_sale_edit_form(db_session, business_id, sample_product) -> None:
    _user(db_session, business_id, "operator")
    mv = InventoryMovement(business_id=business_id, product_id=sample_product.id, type="sale", quantity=-1,
                           movement_date=datetime.now(timezone.utc))
    db_session.add(mv)
    db_session.commit()

    with pytest.raises(HTTPException) as exc:
        ui_sales.sale_edit_form(_request("operator"), movement_id=mv.id, db=db_session)

    assert exc.value.status_code == 403


def test_operator_deletes_sales_document_with_items(db_session, business_id) -> None:
    """También H-712: el DELETE del documento fallaba por FK (líneas aún sin borrar)."""
    from app.models import Location, SalesDocument, SalesDocumentItem
    from app.routers import ui_sales_documents

    _user(db_session, business_id, "operator")
    customer = _customer(db_session, business_id)
    loc = db_session.query(Location).filter_by(business_id=business_id).first()
    doc = SalesDocument(business_id=business_id, location_id=loc.id if loc else None, customer_id=customer.id,
                        doc_type="F", year_month="202611", seq=1, code="F202611-0001",
                        issue_date=datetime.now(timezone.utc), issuer_name="Emisor", client_name=customer.name,
                        client_id=customer.client_id, subtotal=2.0, total=2.0)
    db_session.add(doc)
    db_session.flush()
    db_session.add(SalesDocumentItem(document_id=doc.id, line_no=1, sku="X", description="x", quantity=1,
                                     unit_price=2.0, line_total=2.0))
    db_session.commit()
    doc_id = doc.id

    response = ui_sales_documents.sales_doc_delete(_request("operator"), doc_id=doc_id, db=db_session)

    assert response.status_code == 200
    db_session.expire_all()
    assert db_session.get(SalesDocument, doc_id) is None
    assert db_session.query(SalesDocumentItem).filter_by(document_id=doc_id).count() == 0
