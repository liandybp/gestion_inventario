"""Sonda S11: genera PDFs de ejemplo con `build_sales_document_pdf` del checkout actual (cwd) para revisarlos a ojo.

Uso: .venv/bin/python S11-render-pdf.py <directorio de salida>
Genera `oferta.pdf` (P completo), `factura-larga.pdf` (F con 60 líneas: salto de página) y `minimo.pdf` (sin datos opcionales).
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, ".")
from app.sales_document_pdf import build_sales_document_pdf  # noqa: E402

out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
common = dict(
    issue_date=datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc),
    issuer_name="DyB Autopartes",
    issuer_holder="Randy Manuel Delgado",
    issuer_tax_id="01030573448",
    issuer_address="Calle 1 entre 2 y 3, Playa, La Habana, Cuba",
    issuer_phone="54664732 / 50781349",
    issuer_email="autosandero@gmail.com",
    bank_name="Banco Metropolitano",
    bank_branch="248",
    bank_account="0598770024085419",
    bank_account_currency="CUP",
    client_name="COMERCIO GEELY",
    client_id="B244742",
    client_address="Ave 5ta esq. 60, Miramar",
)
items = [
    {"description": "JGO DE PASTILLAS DE FRENO DELANTERAS", "unit_of_measure": "JGO", "quantity": 1, "unit_price": 303875, "line_total": 303875},
    {"description": "U FILTRO DE ACEITE", "unit_of_measure": "U", "quantity": 1, "unit_price": 187000, "line_total": 187000},
]
(out / "oferta.pdf").write_bytes(build_sales_document_pdf(
    doc_type="P", doc_label="Presupuesto", code="P202611180001", valid_until=date(2026, 12, 3), currency_code="GEN",
    items=items, subtotal=490875, total=490875, notes="Entrega en 3 días hábiles.", **common))
many = [{"description": f"Artículo de prueba número {i} con descripción algo larga para ocupar espacio", "unit_of_measure": "ud",
         "quantity": 2.5, "unit_price": 1234.5, "line_total": 3086.25} for i in range(1, 61)]
(out / "factura-larga.pdf").write_bytes(build_sales_document_pdf(
    doc_type="F", doc_label="Factura", code="F202611180002", valid_until=None, currency_code="USD",
    items=many, subtotal=185175, total=185175, notes=None, **common))
minimal = dict(common)
minimal.update(issuer_holder=None, issuer_tax_id=None, issuer_address=None, issuer_phone=None, issuer_email=None,
               bank_name=None, bank_branch=None, bank_account=None, bank_account_currency=None, client_address=None)
(out / "minimo.pdf").write_bytes(build_sales_document_pdf(
    doc_type="P", doc_label="Oferta extraordinaria de verano", code="P1", valid_until=None, currency_code=None,
    items=[{"description": "Servicio", "unit_of_measure": None, "quantity": 1, "unit_price": 10, "line_total": 10}],
    subtotal=10, total=10, notes=None, **minimal))
print("ok", sorted(p.name for p in out.iterdir()))
