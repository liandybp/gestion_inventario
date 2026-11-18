"""PDF de documentos de venta con el diseño OFERTA (REQ-5, REQ-6, REQ-7, REQ-8)."""

from __future__ import annotations

import io
import re
from datetime import date, datetime, timezone

import pdfplumber

from app.sales_document_pdf import build_sales_document_pdf

ITEMS = [
    {"description": "Pastillas de freno", "unit_of_measure": "JGO", "quantity": 1, "unit_price": 303875, "line_total": 303875},
    {"description": "Filtro de aceite", "unit_of_measure": "U", "quantity": 1, "unit_price": 187000, "line_total": 187000},
]


def _kwargs(**overrides) -> dict:
    base = dict(
        doc_type="P",
        doc_label="Presupuesto",
        code="P202611180001",
        issue_date=datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc),
        valid_until=date(2026, 12, 3),
        currency_code="GEN",
        issuer_name="DyB Autopartes",
        issuer_holder="Randy Manuel Delgado",
        issuer_tax_id="01030573448",
        issuer_address="Calle 1 entre 2 y 3, Playa",
        issuer_phone="54664732 / 50781349",
        issuer_email="autosandero@gmail.com",
        bank_name="Banco Metropolitano",
        bank_branch="248",
        bank_account="0598770024085419",
        bank_account_currency="CUP",
        client_name="Cliente Prueba",
        client_id="B244742",
        client_address=None,
        items=ITEMS,
        subtotal=490875.0,
        total=490875.0,
        notes=None,
    )
    base.update(overrides)
    return base


def _read(pdf_bytes: bytes) -> tuple[str, list]:
    assert pdf_bytes.startswith(b"%PDF")
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = list(pdf.pages)
        text = "\n".join((p.extract_text() or "") for p in pages)
        curves = [len(p.curves) + len(p.images) for p in pages]
    return text, curves


def test_complete_quote_pdf() -> None:
    text, graphics = _read(build_sales_document_pdf(**_kwargs()))

    for expected in (
        "DyB Autopartes",
        "TCP Randy Manuel Delgado",
        "NIT 01030573448",
        "54664732 / 50781349",
        "autosandero@gmail.com",
        "DESTINATARIO:",
        "PRESUPUESTO",
        "No. Presupuesto:",
        "P202611180001",
        "Fecha de presupuesto:",
        "18/11/2026",
        "Fecha de vencimiento:",
        "03/12/2026",
        "Subtotal:",
        "TOTAL:",
        "490,875.00",
        "CONCEPTO",
        "UNIDADES",
        "CANTIDAD",
        "PRECIO CUP",
        "IMPORTE CUP",
        "FORMA DE PAGO",
        "Efectivo",
        "Cheque",
        "Transferencia Bancaria",
        "Banco Metropolitano, Sucursal 248",
        "No. Cuenta (CUP): 0598770024085419",
        "Recibido por:",
        "Cargo:",
        "Firma:",
        "Fecha:",
    ):
        assert expected in text, expected
    assert re.search(r"Pastillas de freno\s+JGO\s+1\s+303,875\.00\s+303,875\.00", text), text
    assert re.search(r"Filtro de aceite\s+U\s+1\s+187,000\.00\s+187,000\.00", text), text
    assert graphics[0] > 0  # iconos de teléfono y email


def test_old_design_is_gone() -> None:
    text, _ = _read(build_sales_document_pdf(**_kwargs()))

    for old in ("Código:", "ID fiscal", "Tel.:", "(Precios sin IVA)"):
        assert old not in text, old


def test_no_phone_and_no_email_means_no_icons() -> None:
    text, graphics = _read(build_sales_document_pdf(**_kwargs(issuer_phone=None, issuer_email=None)))

    assert graphics == [0]
    assert "54664732" not in text
    assert "autosandero" not in text


def test_without_bank_data_the_payment_block_has_no_bank_lines() -> None:
    text, _ = _read(
        build_sales_document_pdf(
            **_kwargs(bank_name=None, bank_branch=None, bank_account=None, bank_account_currency=None)
        )
    )

    assert "FORMA DE PAGO" in text
    assert "Sucursal" not in text
    assert "No. Cuenta" not in text


def test_bank_line_without_branch_and_account_without_currency() -> None:
    text, _ = _read(build_sales_document_pdf(**_kwargs(bank_branch=None, bank_account_currency=None)))

    assert "Banco Metropolitano" in text
    assert "Sucursal" not in text
    assert "No. Cuenta: 0598770024085419" in text


def test_currency_labels() -> None:
    gen, _ = _read(build_sales_document_pdf(**_kwargs(currency_code="GEN")))
    usd, _ = _read(build_sales_document_pdf(**_kwargs(currency_code="USD")))
    eur, _ = _read(build_sales_document_pdf(**_kwargs(currency_code="eur")))
    none, _ = _read(build_sales_document_pdf(**_kwargs(currency_code="")))

    assert "PRECIO CUP" in gen and "IMPORTE CUP" in gen
    assert "GEN" not in gen
    assert "PRECIO USD" in usd and "IMPORTE USD" in usd
    assert "PRECIO EUR" in eur and "IMPORTE EUR" in eur
    assert "PRECIO IMPORTE" in none
    assert "CUP" not in none.split("FORMA DE PAGO")[0]


def test_invoice_uses_the_same_design_without_due_date() -> None:
    text, _ = _read(
        build_sales_document_pdf(
            **_kwargs(doc_type="F", doc_label="Factura", code="F202611180002", valid_until=None)
        )
    )

    for expected in ("FACTURA", "No. Factura:", "Fecha de factura:", "FORMA DE PAGO", "Recibido por:", "DESTINATARIO:"):
        assert expected in text, expected
    assert "Fecha de vencimiento" not in text


def test_old_quote_without_due_date_nor_units_still_renders() -> None:
    items = [{"description": "Servicio", "unit_of_measure": None, "quantity": 1, "unit_price": 10, "line_total": 10}]
    text, _ = _read(
        build_sales_document_pdf(**_kwargs(valid_until=None, items=items, subtotal=10.0, total=10.0))
    )

    assert "Fecha de vencimiento:" in text
    assert re.search(r"Fecha de vencimiento:\s*\d{2}/\d{2}/\d{4}", text) is None
    assert re.search(r"Servicio\s+1\s+10\.00\s+10\.00", text), text


def test_quantity_keeps_up_to_four_decimals_and_thousands_separator() -> None:
    items = [
        {"description": "Aceite", "unit_of_measure": "L", "quantity": 0.0001, "unit_price": 5, "line_total": 0.0005},
        {"description": "Tornillos", "unit_of_measure": "ud", "quantity": 1234567, "unit_price": 1, "line_total": 1234567},
        {"description": "Cable", "unit_of_measure": "m", "quantity": 2.5, "unit_price": 4, "line_total": 10},
    ]
    text, _ = _read(build_sales_document_pdf(**_kwargs(items=items)))

    assert re.search(r"Aceite\s+L\s+0\.0001\s+5\.00", text), text
    assert re.search(r"Tornillos\s+ud\s+1,234,567\s+1\.00\s+1,234,567\.00", text), text
    assert re.search(r"Cable\s+m\s+2\.5\s+4\.00\s+10\.00", text), text


def test_notes_and_non_latin1_text_do_not_break_the_pdf() -> None:
    text, _ = _read(
        build_sales_document_pdf(**_kwargs(client_name="Ñandú 日本", notes="Entrega en 3 días hábiles."))
    )

    assert "Notas:" in text
    assert "Entrega en 3 días hábiles." in text
    assert "Ñandú" in text


def test_long_document_label_and_long_address_stay_inside_their_boxes() -> None:
    long_label = "Presupuesto extraordinario de verano 2026"
    long_address = "Avenida de los Libertadores esquina a la calle sesenta y cuatro, edificio azul, apartamento 12, Miramar"
    pdf_bytes = build_sales_document_pdf(**_kwargs(doc_label=long_label, issuer_address=long_address))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        band, first_rule, _, document_row = page.rects[:4]
        midpoint = (band["x0"] + band["x1"]) / 2
        address = page.within_bbox((midpoint, band["top"], band["x1"], band["bottom"])).extract_text()
        title = page.within_bbox((midpoint, band["bottom"], band["x1"], first_rule["top"])).extract_text()
        row = page.within_bbox(
            (document_row["x0"], document_row["top"], document_row["x1"], document_row["bottom"])
        ).extract_text()

    assert long_address in " ".join(address.split())
    assert " ".join(title.split()) == long_label.upper()
    assert re.fullmatch(r"No\. Presupuesto .+\.\.\. P202611180001", " ".join(row.split()))


def test_long_table_repeats_its_header_and_keeps_the_footer_whole() -> None:
    items = [
        {"description": f"Artículo {i} con una descripción larga para ocupar espacio en la tabla del documento",
         "unit_of_measure": "ud", "quantity": 2, "unit_price": 10, "line_total": 20}
        for i in range(1, 61)
    ]
    pdf_bytes = build_sales_document_pdf(**_kwargs(items=items, subtotal=1200.0, total=1200.0))
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_texts = [(p.extract_text() or "") for p in pdf.pages]

    assert len(page_texts) >= 2
    assert all("CONCEPTO" in t and "PRECIO CUP" in t for t in page_texts)
    for label in (
        "FORMA DE PAGO", "Efectivo", "Cheque", "Transferencia Bancaria",
        "Banco Metropolitano, Sucursal 248", "No. Cuenta (CUP): 0598770024085419",
        "Recibido por:", "Cargo:", "Firma:", "Fecha: _",
    ):
        assert page_texts[-1].count(label) == 1, label
        assert all(label not in text for text in page_texts[:-1]), label
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))


def test_detached_payment_and_signature_page_identifies_the_document() -> None:
    items = [
        {"description": f"Artículo {i}", "quantity": 1, "unit_price": 2, "line_total": 2}
        for i in range(1, 31)
    ]
    pdf_bytes = build_sales_document_pdf(**_kwargs(items=items, subtotal=60, total=60))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        assert len(pdf.pages) == 2
        first_page, signature_page = (page.extract_text() for page in pdf.pages)

    assert "Artículo 30" in first_page
    assert "CONCEPTO" not in signature_page
    assert "Documento: P202611180001" in signature_page
    assert "FORMA DE PAGO" in signature_page
    assert "Firma:" in signature_page


def test_table_header_moves_with_a_tall_first_row() -> None:
    lines = [f"L{i:02d}" for i in range(49)]
    items = [{"description": "\n".join(lines), "quantity": 1, "unit_price": 2, "line_total": 2}]
    pdf_bytes = build_sales_document_pdf(**_kwargs(items=items, subtotal=2, total=2))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        first_page, table_page, *_ = [page.extract_text() for page in pdf.pages]

    assert "DESTINATARIO:" in first_page
    assert "CONCEPTO" not in first_page
    assert "CONCEPTO" in table_page
    assert re.findall(r"\bL\d{2}\b", table_page) == lines


def test_wide_currency_code_stays_inside_each_table_column() -> None:
    currency = "WWWWWWWW"
    pdf_bytes = build_sales_document_pdf(**_kwargs(currency_code=currency))

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page = pdf.pages[0]
        points_per_mm = page.width / 210
        quantity = page.within_bbox((113 * points_per_mm, 0, 133 * points_per_mm, page.height))
        price = page.within_bbox((133 * points_per_mm, 0, 163 * points_per_mm, page.height))
        amount = page.within_bbox((163 * points_per_mm, 0, 195 * points_per_mm, page.height))

        assert "CANTIDAD" in quantity.extract_text()
        assert f"PRECIO {currency}" in price.extract_text()
        assert f"IMPORTE {currency}" in amount.extract_text()
