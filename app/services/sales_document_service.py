from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import Customer, SalesDocument, SalesDocumentItem


class SalesDocumentService:
    """Consultas read-only sobre clientes y documentos de venta (facturas/presupuestos).

    Los documentos guardan ``client_name``/``client_id`` desnormalizados (sin FK a
    ``customers``), por lo que las ventas por cliente se agrupan por ``client_name``.
    """

    def __init__(self, db: Session, business_id: int):
        self._db = db
        self._business_id = int(business_id)

    def list_recent(
        self,
        limit: int = 10,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[SalesDocument]:
        stmt = select(SalesDocument).where(SalesDocument.business_id == self._business_id)
        if start is not None:
            stmt = stmt.where(SalesDocument.issue_date >= start)
        if end is not None:
            stmt = stmt.where(SalesDocument.issue_date < end)
        stmt = stmt.order_by(SalesDocument.issue_date.desc(), SalesDocument.id.desc()).limit(int(limit))
        return list(self._db.scalars(stmt))

    def get(self, doc_id: int) -> Optional[SalesDocument]:
        return self._db.get(SalesDocument, int(doc_id))

    def get_items(self, doc_id: int) -> list[SalesDocumentItem]:
        stmt = select(SalesDocumentItem).where(SalesDocumentItem.document_id == int(doc_id))
        return list(self._db.scalars(stmt))

    def get_items_ordered(self, doc_id: int) -> list[SalesDocumentItem]:
        stmt = (
            select(SalesDocumentItem)
            .where(SalesDocumentItem.document_id == int(doc_id))
            .order_by(SalesDocumentItem.line_no.asc(), SalesDocumentItem.id.asc())
        )
        return list(self._db.scalars(stmt))

    def customers_list(self, limit: int = 200) -> list[Customer]:
        stmt = (
            select(Customer)
            .where(Customer.business_id == self._business_id)
            .order_by(Customer.name.asc(), Customer.id.asc())
            .limit(int(limit))
        )
        return list(self._db.scalars(stmt))

    def get_customer_by_client_id(self, client_id: str) -> Optional[Customer]:
        stmt = select(Customer).where(
            Customer.client_id == client_id, Customer.business_id == self._business_id
        )
        return self._db.scalar(stmt)

    def get_customer_by_name(self, name: str) -> Optional[Customer]:
        stmt = (
            select(Customer)
            .where(Customer.name.ilike(name), Customer.business_id == self._business_id)
            .order_by(Customer.id.asc())
            .limit(1)
        )
        return self._db.scalar(stmt)

    def get_customer_by_name_like(self, name: str) -> Optional[Customer]:
        like = f"%{name}%"
        stmt = (
            select(Customer)
            .where(Customer.name.ilike(like), Customer.business_id == self._business_id)
            .order_by(Customer.id.asc())
            .limit(1)
        )
        return self._db.scalar(stmt)

    def add_customer(self, customer: Customer) -> None:
        self._db.add(customer)

    def max_seq(self, year_month: str) -> Optional[int]:
        stmt = (
            select(func.max(SalesDocument.seq))
            .where(SalesDocument.year_month == year_month)
            .where(SalesDocument.business_id == self._business_id)
        )
        return self._db.scalar(stmt)

    def sales_by_customer(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        limit: int = 20,
    ) -> list[dict]:
        """Facturación (solo facturas, doc_type='F') agrupada por cliente, top `limit`."""
        filters = [
            SalesDocument.business_id == self._business_id,
            SalesDocument.doc_type == "F",
        ]
        if start is not None:
            filters.append(SalesDocument.issue_date >= start)
        if end is not None:
            filters.append(SalesDocument.issue_date < end)
        stmt = (
            select(
                SalesDocument.client_name,
                func.count(SalesDocument.id).label("doc_count"),
                func.coalesce(func.sum(SalesDocument.total), 0).label("total"),
            )
            .where(*filters)
            .group_by(SalesDocument.client_name)
            .order_by(func.coalesce(func.sum(SalesDocument.total), 0).desc())
            .limit(int(limit))
        )
        rows = self._db.execute(stmt).all()
        return [
            {"client_name": r[0] or "", "doc_count": int(r[1] or 0), "total": float(r[2] or 0)}
            for r in rows
        ]

    def documents_summary(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> list[dict]:
        """Total facturado vs presupuestado, agrupado por tipo de documento."""
        filters = [SalesDocument.business_id == self._business_id]
        if start is not None:
            filters.append(SalesDocument.issue_date >= start)
        if end is not None:
            filters.append(SalesDocument.issue_date < end)
        stmt = (
            select(
                SalesDocument.doc_type,
                func.count(SalesDocument.id).label("count"),
                func.coalesce(func.sum(SalesDocument.total), 0).label("total"),
            )
            .where(*filters)
            .group_by(SalesDocument.doc_type)
            .order_by(SalesDocument.doc_type.asc())
        )
        rows = self._db.execute(stmt).all()
        return [
            {"doc_type": r[0], "count": int(r[1] or 0), "total": float(r[2] or 0)}
            for r in rows
        ]

    def customers_summary(self) -> dict:
        """Cantidad y listado de clientes del negocio."""
        count = self._db.scalar(
            select(func.count(Customer.id)).where(Customer.business_id == self._business_id)
        ) or 0
        customers = list(self._db.scalars(
            select(Customer)
            .where(Customer.business_id == self._business_id)
            .order_by(Customer.name.asc())
        ).all())
        return {
            "count": int(count),
            "customers": [
                {"client_id": c.client_id, "name": c.name, "phone": c.phone or ""}
                for c in customers
            ],
        }
