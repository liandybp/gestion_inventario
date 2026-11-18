Encontré tres problemas en las pruebas y en la evidencia de verificación. Las 73 pruebas nuevas pasan. El comprobador de snapshots requiere un directorio temporal escribible, del que carece este entorno. La suite unitaria completa quedó sin confirmar. La revisión fue en solo lectura.

1. **La prueba del formulario acepta una fecha que el navegador mostrará vacía.** Comprueba el atributo `value="31/12/2026"` de un `<input type="date">`. El navegador descarta ese formato al calcular el valor del control, así que la prueba pasa aunque el usuario pierda la fecha. Usar una fecha ISO anterior a la emisión, con el reloj fijado, permite comprobar el rechazo y la conservación de una fecha que el control sí puede mostrar. [test_sales_doc_valid_until.py](/home/liandy/PyCharmMiscProject/gestion_inventario/tests/unit/test_sales_doc_valid_until.py:131)
2. **La prueba de edición de factura guarda una factura con importe cero y pasa.** La llamada omite las cantidades y los precios de las dos líneas. El handler interpreta esas omisiones como ceros, recalcula el total y guarda la factura. La prueba solo comprueba el vencimiento. Debe enviar los valores de las líneas como hace el formulario y comprobar que se conservan sus importes y el total. [test_sales_doc_valid_until.py](/home/liandy/PyCharmMiscProject/gestion_inventario/tests/unit/test_sales_doc_valid_until.py:288)
3. **El informe del lote C registra el fallo de una aserción anterior al commit revisado.** La aserción del traceback ya fue sustituida y la prueba actual pasa. El informe debe regenerarse a partir del árbol corregido para que documente el resultado que corresponde a la entrega. [loteC.test.txt](/home/liandy/PyCharmMiscProject/gestion_inventario/specs/pdf-documentos-venta-oferta/verify/loteC.test.txt:12)

**Ficheros revisados**

- `specs/pdf-documentos-venta-oferta/tasks.md`
- `specs/pdf-documentos-venta-oferta/verify/loteC.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteC.test.txt`
- `tests/unit/test_business_bank_fields.py`
- `tests/unit/test_sales_doc_valid_until.py`
- `tests/unit/test_sales_document_pdf.py`
- `tests/unit/test_sales_documents_format.py`

`````tasks-md
Lotes: **A** T1-T3

### T1: Verificar que el formulario conserva una fecha ISO rechazada
**Apply:** script
**Files:** `tests/unit/test_sales_doc_valid_until.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_doc_valid_until.py
op: replace
@@ before
    default = "2026-12-03"

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    with patch("app.routers.ui_common.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
@@ after
    now = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    past_due_date = "2026-11-17"

    with patch("app.routers.ui_common.datetime", wraps=datetime) as default_clock, patch(
        "app.routers.ui_sales_documents.datetime", wraps=datetime
    ) as issue_clock:
        default_clock.now.return_value = now
        issue_clock.now.return_value = now
        rejected = _issue(db_session, _request([dict(CART_LINE)]), valid_until=past_due_date)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))

    assert rejected.status_code == 422
    assert f'name="valid_until" value="{past_due_date}"' in _body(rejected)
    assert 'name="valid_until" value="2026-12-03"' in _body(empty_draft)
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    default = "2026-12-03"

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    with patch("app.routers.ui_common.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)
@@ after
    now = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    past_due_date = "2026-11-17"

    with patch("app.routers.ui_common.datetime", wraps=datetime) as default_clock, patch(
        "app.routers.ui_sales_documents.datetime", wraps=datetime
    ) as issue_clock:
        default_clock.now.return_value = now
        issue_clock.now.return_value = now
        rejected = _issue(db_session, _request([dict(CART_LINE)]), valid_until=past_due_date)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))

    assert rejected.status_code == 422
    assert f'name="valid_until" value="{past_due_date}"' in _body(rejected)
    assert 'name="valid_until" value="2026-12-03"' in _body(empty_draft)
````

#### Verification

```bash
.venv/bin/python -m pytest -o addopts='' -q tests/unit/test_sales_doc_valid_until.py -k test_issue_form_shows_the_default_due_date_and_keeps_the_typed_one
```

### T2: Conservar los importes en la prueba de edición de factura
**Apply:** script
**Files:** `tests/unit/test_sales_doc_valid_until.py`, `specs/pdf-documentos-venta-oferta/tasks.md`

#### Instructions

````pae-edit
file: tests/unit/test_sales_doc_valid_until.py
op: replace
@@ before
    response = _update(db_session, doc.id, client_name="A", client_id="C-A", valid_until="2099-12-31")

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
@@ after
    items_query = (
        select(SalesDocumentItem)
        .where(SalesDocumentItem.document_id == doc.id)
        .order_by(SalesDocumentItem.line_no)
    )
    fields = {"client_name": "A", "client_id": "C-A", "valid_until": "2099-12-31"}
    for item in db_session.scalars(items_query):
        fields[f"desc_{item.id}"] = item.description
        fields[f"qty_{item.id}"] = str(item.quantity)
        fields[f"price_{item.id}"] = str(item.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
    assert float(doc.total) == 10.0
    assert [float(item.line_total) for item in db_session.scalars(items_query)] == [4.0, 6.0]
````

````pae-edit
file: specs/pdf-documentos-venta-oferta/tasks.md
op: replace
@@ before
    response = _update(db_session, doc.id, client_name="A", client_id="C-A", valid_until="2099-12-31")

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
@@ after
    items_query = (
        select(SalesDocumentItem)
        .where(SalesDocumentItem.document_id == doc.id)
        .order_by(SalesDocumentItem.line_no)
    )
    fields = {"client_name": "A", "client_id": "C-A", "valid_until": "2099-12-31"}
    for item in db_session.scalars(items_query):
        fields[f"desc_{item.id}"] = item.description
        fields[f"qty_{item.id}"] = str(item.quantity)
        fields[f"price_{item.id}"] = str(item.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
    assert float(doc.total) == 10.0
    assert [float(item.line_total) for item in db_session.scalars(items_query)] == [4.0, 6.0]
````

#### Verification

```bash
.venv/bin/python -m pytest -o addopts='' -q tests/unit/test_sales_doc_valid_until.py
```

### T3: Regenerar la evidencia del lote C
**Depends on:** T1, T2
**Apply:** cc_local
**Files:** `specs/pdf-documentos-venta-oferta/verify/loteC.test.txt`

#### Instructions

Ejecuta el comprobador de snapshots y la suite unitaria completa sobre el árbol resultante de T1 y T2. Captura la salida real de ambos comandos en un fichero temporal. Sustituye `loteC.test.txt` por esa salida cuando ambos comandos terminen con código 0. El contenido se obtiene de la ejecución.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline &&
.venv/bin/python -m pytest -o addopts='' -q tests/unit &&
rg -q '^OK: [0-9]+ snapshots equivalentes$' specs/pdf-documentos-venta-oferta/verify/loteC.test.txt &&
rg -q '^[0-9]+ passed' specs/pdf-documentos-venta-oferta/verify/loteC.test.txt &&
! rg -q '^FAILED ' specs/pdf-documentos-venta-oferta/verify/loteC.test.txt
```
`````