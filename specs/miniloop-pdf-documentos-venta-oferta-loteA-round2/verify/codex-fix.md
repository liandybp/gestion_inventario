Encontré tres bugs en los 23 ficheros del diff solicitado. Los reproduje en memoria y no modifiqué ningún fichero.

1. **P1 — El PDF corta líneas de venta demasiado altas.** Una descripción de 132 caracteres con saltos de línea cabe en la columna de la base de datos, pero supera el espacio de una página. El generador abre otra página y dibuja allí la fila entera, dejando su final fuera del área visible. La corrección conserva juntas las filas que caben en una página y pagina las demás, repitiendo la cabecera. [app/sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/sales_document_pdf.py:205)
2. **P1 — Los campos de cabecera, destinatario y pago pueden desbordar su bloque.** Los saltos de línea en campos como la dirección del cliente consumen altura sin límite y desplazan la tabla fuera de la página. Reproduje la pérdida del final de una dirección de 134 caracteres. La corrección convierte los saltos de estos campos en espacios y deja que el ancho de cada columna determine el ajuste. [app/sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/sales_document_pdf.py:68)
3. **P2 — Un plazo vacío invalida toda la configuración `.conf`.** `quote_validity_days =` llega a Pydantic como una cadena vacía y provoca un `ValidationError`. La corrección usa 15 para valores vacíos o no numéricos y acota los enteros a 0–365, igual que el formulario de negocio. [app/business_config.py](/home/liandy/PyCharmMiscProject/gestion_inventario/app/business_config.py:359)

Comprobé en memoria las correcciones de paginación y ajuste de texto. Los comandos de verificación del bloque quedan para después de aplicar las ediciones.

### Ficheros revisados

- `app/business_config.py`
- `app/migrations.py`
- `app/models.py`
- `app/repositories/business_repository.py`
- `app/routers/ui_businesses.py`
- `app/routers/ui_common.py`
- `app/routers/ui_sales_documents.py`
- `app/sales_document_pdf.py`
- `app/sales_documents_format.py`
- `app/templates/components/sales_doc/issue_form.html`
- `app/templates/features/businesses/business_edit_form.html`
- `app/templates/features/businesses/tab_businesses.html`
- `specs/pdf-documentos-venta-oferta/verify/loteA-contrato-rojo.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteA.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteA.test.txt`
- `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`
- `tests/snapshots/baseline/empty_tab_businesses.txt`
- `tests/snapshots/baseline/empty_tab_documents.txt`
- `tests/snapshots/baseline/modal_business.txt`
- `tests/snapshots/baseline/operator_tab_documents.txt`
- `tests/snapshots/baseline/owner_tab_documents.txt`
- `tests/snapshots/baseline/tab_businesses.txt`
- `tests/snapshots/baseline/tab_documents.txt`

`````tasks-md
Lotes: **A** T1-T3

### T1: Paginar las líneas de venta que superan una página
**Apply:** script
**Files:** `app/sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: insert_after
@@ anchor
ROW_H = 4.9
@@ content
TABLE_HEADER_HEIGHT = 7.5
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    return y + 7.5
@@ after
    return y + TABLE_HEADER_HEIGHT
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    """Una fila por línea. Una fila nunca se parte entre dos páginas; la tabla repite su cabecera en la nueva."""
@@ after
    """Mantiene juntas las filas que caben en una página y repite la cabecera en cada continuación."""
    line_height = 5.0
    page_capacity = int((PAGE_BOTTOM - MARGIN - TABLE_HEADER_HEIGHT) // line_height)
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
        line_count = 1
        for text, width, _, size in cells:
            pdf.set_font("Helvetica", "", size)
            line_count = max(line_count, _count_lines(pdf, text, width, 5.0))
        height = line_count * 5.0
        if y + height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)

        x = MARGIN
        for text, width, align, size in cells:
            pdf.set_font("Helvetica", "", size)
            pdf.set_xy(x, y)
            pdf.multi_cell(width, 5.0, text, align=align, new_x="LEFT", new_y="NEXT")
            x += width
        y += height
@@ after
        wrapped_cells = []
        for text, width, align, size in cells:
            pdf.set_font("Helvetica", "", size)
            lines = pdf.multi_cell(width, line_height, text, dry_run=True, output="LINES")
            wrapped_cells.append((lines, width, align, size))

        line_count = max(1, max(len(lines) for lines, _, _, _ in wrapped_cells))
        if y + min(line_count, page_capacity) * line_height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)

        for start in range(0, line_count, page_capacity):
            if start:
                pdf.add_page()
                y = _draw_table_header(pdf, MARGIN, price_label, amount_label)
            stop = min(start + page_capacity, line_count)
            x = MARGIN
            for lines, width, align, size in wrapped_cells:
                pdf.set_font("Helvetica", "", size)
                for index, line in enumerate(lines[start:stop]):
                    pdf.set_xy(x, y + index * line_height)
                    pdf.cell(width, line_height, line, align=align)
                x += width
            y += (stop - start) * line_height
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python - <<'PY'
from datetime import datetime
from io import BytesIO

import pdfplumber
from app.sales_document_pdf import build_sales_document_pdf

description = "\n".join(["x"] * 60 + ["FIN-DE-LINEA"])
body = build_sales_document_pdf(
    doc_type="F", doc_label="Factura", code="F1", issue_date=datetime(2026, 11, 18),
    valid_until=None, currency_code="USD", issuer_name="Negocio", issuer_holder=None,
    issuer_tax_id=None, issuer_address=None, issuer_phone=None, issuer_email=None,
    bank_name=None, bank_branch=None, bank_account=None, bank_account_currency=None,
    client_name="Cliente", client_id="123", client_address=None,
    items=[dict(description=description, quantity=1, unit_price=123.45, line_total=123.45)],
    subtotal=123.45, total=123.45, notes=None,
)
with pdfplumber.open(BytesIO(body)) as pdf:
    visible = [page.crop(page.bbox).extract_text() for page in pdf.pages]
    continuation = next(text for text in visible if "FIN-DE-LINEA" in text)
    assert "CONCEPTO" in continuation
    assert "123.45" not in continuation
    assert all(char["bottom"] <= page.height for page in pdf.pages for char in page.chars)
PY
```

### T2: Ajustar los campos de cabecera, destinatario y pago al ancho disponible
**Apply:** script
**Files:** `app/sales_document_pdf.py`

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: insert_before
@@ anchor
def _fit_text(pdf: Any, text: str, width: float, style: str = "B", start: float = 9.0, minimum: float = 6.0) -> str:
@@ content
def _fold_field_lines(value: Optional[str]) -> str:
    """Convierte los saltos de línea de un campo en espacios para ajustarlo al ancho de su bloque."""
    return " ".join((value or "").splitlines())


````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
    band_bottom = _draw_header(
        pdf,
        issuer_name=issuer_name,
        issuer_holder=issuer_holder,
        issuer_tax_id=issuer_tax_id,
        issuer_address=issuer_address,
        issuer_phone=issuer_phone,
        issuer_email=issuer_email,
    )
    y = _draw_title_bar(pdf, band_bottom + 2.0, doc_label)
    client_end = _draw_client(pdf, y, client_name, client_id, client_address)
@@ after
    band_bottom = _draw_header(
        pdf,
        issuer_name=_fold_field_lines(issuer_name),
        issuer_holder=_fold_field_lines(issuer_holder),
        issuer_tax_id=_fold_field_lines(issuer_tax_id),
        issuer_address=_fold_field_lines(issuer_address),
        issuer_phone=_fold_field_lines(issuer_phone),
        issuer_email=_fold_field_lines(issuer_email),
    )
    y = _draw_title_bar(pdf, band_bottom + 2.0, doc_label)
    client_end = _draw_client(
        pdf, y, _fold_field_lines(client_name), _fold_field_lines(client_id), _fold_field_lines(client_address),
    )
````

````pae-edit
file: app/sales_document_pdf.py
op: replace
@@ before
        _safe_pdf_text(line)
        for line in _bank_lines(bank_name, bank_branch, bank_account, bank_account_currency)
@@ after
        _safe_pdf_text(_fold_field_lines(line))
        for line in _bank_lines(bank_name, bank_branch, bank_account, bank_account_currency)
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python - <<'PY'
from datetime import datetime
from io import BytesIO

import pdfplumber
from app.sales_document_pdf import build_sales_document_pdf

body = build_sales_document_pdf(
    doc_type="F", doc_label="Factura", code="F1", issue_date=datetime(2026, 11, 18),
    valid_until=None, currency_code="USD", issuer_name="Negocio", issuer_holder=None,
    issuer_tax_id=None, issuer_address="x\n" * 60 + "FIN-EMISOR", issuer_phone=None,
    issuer_email=None, bank_name="x\n" * 54 + "FIN-BANCO", bank_branch=None,
    bank_account=None, bank_account_currency=None, client_name="Cliente", client_id="123",
    client_address="x\n" * 60 + "FIN-CLIENTE",
    items=[dict(description="Repuesto", quantity=1, unit_price=10, line_total=10)],
    subtotal=10, total=10, notes=None,
)
with pdfplumber.open(BytesIO(body)) as pdf:
    visible = "\n".join(page.crop(page.bbox).extract_text() for page in pdf.pages)
    for marker in ("FIN-EMISOR", "FIN-CLIENTE", "FIN-BANCO", "Repuesto", "Recibido por:"):
        assert marker in visible, marker
    assert all(char["bottom"] <= page.height for page in pdf.pages for char in page.chars)
PY
```

### T3: Tolerar plazos vacíos o inválidos al leer la configuración INI
**Apply:** script
**Files:** `app/business_config.py`

#### Instructions

````pae-edit
file: app/business_config.py
op: insert_before
@@ anchor
    cfg = BusinessConfig(
@@ content
    try:
        quote_validity_days = int(get("sales_documents", "quote_validity_days", "15"))
    except ValueError:
        quote_validity_days = 15
    quote_validity_days = max(0, min(365, quote_validity_days))

````

````pae-edit
file: app/business_config.py
op: replace
@@ before
            quote_validity_days=get("sales_documents", "quote_validity_days", "15"),
@@ after
            quote_validity_days=quote_validity_days,
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python - <<'PY'
from configparser import ConfigParser
from pathlib import Path
from unittest.mock import patch

from app.business_config import load_business_config

for index, (raw, expected) in enumerate(
    [("", 15), ("texto", 15), ("-1", 0), ("0", 0), ("30", 30), ("366", 365)]
):
    def read_config(parser, *args, **kwargs):
        parser.read_string(f"[sales_documents]\nquote_validity_days = {raw}\n")

    with patch.object(Path, "exists", return_value=True), patch.object(ConfigParser, "read", read_config):
        config = load_business_config(f"review-validity-{index}")
        assert config.sales_documents.quote_validity_days == expected, raw
PY
```
`````