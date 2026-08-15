from __future__ import annotations

from app.models import Customer, SalesDocument
from app.services.sales_document_service import SalesDocumentService


def _seed_document(db, business_id: int, doc_type: str, client_name: str, client_id: str, seq: int, total: float) -> None:
    db.add(SalesDocument(
        business_id=business_id,
        doc_type=doc_type,
        year_month="202608",
        seq=seq,
        code=f"{doc_type}-{business_id}-{seq}",
        issuer_name="Test Business",
        client_name=client_name,
        client_id=client_id,
        total=total,
    ))
    db.commit()


def test_sales_by_customer_groups_invoices_only(db_session, business_id):
    _seed_document(db_session, business_id, "F", "Cliente A", "C1", 1, 100.0)
    _seed_document(db_session, business_id, "F", "Cliente B", "C2", 2, 50.0)
    _seed_document(db_session, business_id, "P", "Cliente A", "C1", 3, 200.0)

    service = SalesDocumentService(db_session, business_id)
    rows = service.sales_by_customer()

    assert len(rows) == 2  # solo facturas (F), no presupuestos (P)
    assert rows[0]["client_name"] == "Cliente A"
    assert rows[0]["doc_count"] == 1
    assert rows[0]["total"] == 100.0
    assert rows[1]["client_name"] == "Cliente B"
    assert rows[1]["total"] == 50.0


def test_documents_summary_by_type(db_session, business_id):
    _seed_document(db_session, business_id, "F", "Cliente A", "C1", 1, 100.0)
    _seed_document(db_session, business_id, "F", "Cliente B", "C2", 2, 50.0)
    _seed_document(db_session, business_id, "P", "Cliente A", "C1", 3, 200.0)

    service = SalesDocumentService(db_session, business_id)
    rows = service.documents_summary()
    by_type = {r["doc_type"]: r for r in rows}

    assert by_type["F"]["count"] == 2
    assert by_type["F"]["total"] == 150.0
    assert by_type["P"]["count"] == 1
    assert by_type["P"]["total"] == 200.0


def test_customers_summary_counts(db_session, business_id):
    db_session.add(Customer(business_id=business_id, client_id="C1", name="Cliente A"))
    db_session.add(Customer(business_id=business_id, client_id="C2", name="Cliente B"))
    db_session.commit()

    service = SalesDocumentService(db_session, business_id)
    summary = service.customers_summary()

    assert summary["count"] == 2
    assert [c["name"] for c in summary["customers"]] == ["Cliente A", "Cliente B"]
