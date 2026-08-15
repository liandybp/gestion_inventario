"""Acceso a datos de clientes (extraído de ui_customers, H-028)."""
from __future__ import annotations
from typing import Optional
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session
from app.models import Customer, SalesDocument, SalesDocumentItem
from app.utils import query_match


class CustomerRepository:
    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def list(self, query: str = "", limit: int = 200) -> list[Customer]:
        q = (query or "").strip()
        prefetch_limit = max(int(limit or 0) * 10, 500) if q else int(limit or 0)
        stmt = select(Customer).where(Customer.business_id == self._business_id)
        stmt = stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(prefetch_limit)
        rows = list(self._db.scalars(stmt))
        if q:
            rows = [c for c in rows if query_match(q, str(c.name or ""), str(c.client_id or ""))]
        return rows[: int(limit or 0)]

    def list_like(self, query: str = "", limit: int = 200) -> list[Customer]:
        q = (query or "").strip()
        stmt = select(Customer).where(Customer.business_id == self._business_id)
        if q:
            like = f"%{q}%"
            stmt = stmt.where((Customer.name.ilike(like)) | (Customer.client_id.ilike(like)))
        return list(self._db.scalars(stmt.order_by(Customer.name.asc(), Customer.id.asc()).limit(limit)))

    def get(self, customer_id: int) -> Optional[Customer]:
        return self._db.get(Customer, int(customer_id))

    def create(
        self,
        *,
        client_id: str,
        name: str,
        address: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
    ) -> Customer:
        customer = Customer(
            business_id=self._business_id,
            client_id=client_id,
            name=name,
            address=address,
            phone=phone,
            email=email,
        )
        self._db.add(customer)
        return customer

    def update(self, customer: Customer, **fields) -> None:
        for key, value in fields.items():
            setattr(customer, key, value)

    def delete(self, customer: Customer) -> None:
        self._db.delete(customer)

    def recent_documents(self, customer_id: int, limit: int = 200) -> list[SalesDocument]:
        return list(
            self._db.scalars(
                select(SalesDocument)
                .where(SalesDocument.customer_id == int(customer_id))
                .where(SalesDocument.business_id == self._business_id)
                .order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc())
                .limit(int(limit))
            )
        )

    def document_totals(self, customer_id: int, doc_type: str) -> float:
        return float(
            self._db.scalar(
                select(func.coalesce(func.sum(SalesDocument.total), 0)).where(
                    (SalesDocument.customer_id == int(customer_id))
                    & (SalesDocument.doc_type == doc_type)
                    & (SalesDocument.business_id == self._business_id)
                )
            )
            or 0
        )

    def document_count(self, customer_id: int, doc_type: Optional[str] = None) -> int:
        where = [
            SalesDocument.customer_id == int(customer_id),
            SalesDocument.business_id == self._business_id,
        ]
        if doc_type:
            where.append(SalesDocument.doc_type == doc_type)
        return int(self._db.scalar(select(func.count(SalesDocument.id)).where(and_(*where))) or 0)

    def last_invoice_date(self, customer_id: int) -> Optional[object]:
        return self._db.scalar(
            select(func.max(SalesDocument.issue_date)).where(
                (SalesDocument.customer_id == int(customer_id))
                & (SalesDocument.doc_type == "F")
                & (SalesDocument.business_id == self._business_id)
            )
        )

    def top_items(self, customer_id: int, limit: int = 10) -> list[tuple]:
        return list(
            self._db.execute(
                select(
                    SalesDocumentItem.description,
                    func.coalesce(func.sum(SalesDocumentItem.line_total), 0).label("total"),
                )
                .join(SalesDocument, SalesDocumentItem.document_id == SalesDocument.id)
                .where(
                    and_(
                        SalesDocument.customer_id == int(customer_id),
                        SalesDocument.business_id == self._business_id,
                    )
                )
                .group_by(SalesDocumentItem.description)
                .order_by(func.sum(SalesDocumentItem.line_total).desc())
                .limit(int(limit))
            ).all()
        )