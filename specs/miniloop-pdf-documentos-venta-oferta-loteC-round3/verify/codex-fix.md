Encontré dos defectos en las pruebas añadidas. Las 73 pruebas del diff pasan en el commit revisado. También verifiqué ambas correcciones en memoria, sin modificar ficheros.

1. **La prueba de paginación acepta un PDF sin la columna de firma.** La condición `<= 1` admite que «Recibido por:» falte por completo o aparezca en otra página. Reproduje el falso positivo omitiendo la columna de firma durante el render. La corrección exige que cada campo del pie aparezca una sola vez y en la última página. [test_sales_document_pdf.py](/home/liandy/PyCharmMiscProject/gestion_inventario/tests/unit/test_sales_document_pdf.py:214)
2. **La prueba del vencimiento por defecto puede fallar al cruzar la medianoche UTC.** El test y la plantilla consultan el reloj por separado. Si cambia el día entre ambas lecturas, sus fechas esperadas difieren. La corrección fija el reloj durante el render y comprueba una fecha esperada fija. [test_sales_doc_valid_until.py](/home/liandy/PyCharmMiscProject/gestion_inventario/tests/unit/test_sales_doc_valid_until.py:131)

**Ficheros revisados en el diff**

- `specs/pdf-documentos-venta-oferta/tasks.md`
- `specs/pdf-documentos-venta-oferta/verify/loteC.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteC.test.txt`
- `tests/unit/test_business_bank_fields.py`
- `tests/unit/test_sales_doc_valid_until.py`
- `tests/unit/test_sales_document_pdf.py`
- `tests/unit/test_sales_documents_format.py`

`````tasks-md
Lotes: **A** T1-T2

### T1: Comprobar el pie completo en la última página del PDF
**Apply:** script
**Files:** `tests/unit/test_sales_document_pdf.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: replace
@@ before
    assert sum(t.count("FORMA DE PAGO") for t in page_texts) == 1
    assert "FORMA DE PAGO" in page_texts[-1]
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
    assert all(t.count("Recibido por:") <= 1 for t in page_texts)
@@ after
    for label in (
        "FORMA DE PAGO", "Efectivo", "Cheque", "Transferencia Bancaria",
        "Banco Metropolitano, Sucursal 248", "No. Cuenta (CUP): 0598770024085419",
        "Recibido por:", "Cargo:", "Firma:", "Fecha: _",
    ):
        assert page_texts[-1].count(label) == 1, label
        assert all(label not in text for text in page_texts[:-1]), label
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    assert sum(t.count("FORMA DE PAGO") for t in page_texts) == 1
    assert "FORMA DE PAGO" in page_texts[-1]
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
    assert all(t.count("Recibido por:") <= 1 for t in page_texts)
@@ after
    for label in (
        "FORMA DE PAGO", "Efectivo", "Cheque", "Transferencia Bancaria",
        "Banco Metropolitano, Sucursal 248", "No. Cuenta (CUP): 0598770024085419",
        "Recibido por:", "Cargo:", "Firma:", "Fecha: _",
    ):
        assert page_texts[-1].count(label) == 1, label
        assert all(label not in text for text in page_texts[:-1]), label
    row_numbers = [int(number) for text in page_texts for number in re.findall(r"Artículo (\d+)\b", text)]
    assert row_numbers == list(range(1, 61))
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest --capture=sys -p no:cacheprovider -o addopts='' -q tests/unit/test_sales_document_pdf.py
```

### T2: Fijar el reloj en la prueba del vencimiento por defecto
**Apply:** script
**Files:** `tests/unit/test_sales_doc_valid_until.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_doc_valid_until.py
op: replace
@@ before
from unittest.mock import AsyncMock, MagicMock
@@ after
from unittest.mock import AsyncMock, MagicMock, patch
````

````pae-edit
file: tests/unit/test_sales_doc_valid_until.py
op: replace
@@ before
    default = (_today() + timedelta(days=15)).isoformat()

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
@@ after
    default = "2026-12-03"

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    with patch("app.routers.ui_common.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
@@ after
import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    default = (_today() + timedelta(days=15)).isoformat()

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
@@ after
    default = "2026-12-03"

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    with patch("app.routers.ui_common.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
````

#### Verification

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest --capture=sys -p no:cacheprovider -o addopts='' -q tests/unit/test_sales_doc_valid_until.py
```
`````