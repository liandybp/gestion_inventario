**[P2] Una fecha de vencimiento inválida descarta los cambios del formulario de edición.** El handler devuelve el panel completo con un 422. HTMX lo renderiza y el código de modales cierra el formulario porque el destino está fuera de él. El presupuesto guardado queda intacto, pero se pierden las notas y los cambios de líneas introducidos antes de pulsar «Guardar cambios». T22 comprueba la respuesta y la persistencia en base de datos. La pérdida del formulario se produce en el navegador. [Handler](/home/liandy/PyCharmMiscProject/gestion_inventario/app/routers/ui_sales_documents.py:410) · [Formulario](/home/liandy/PyCharmMiscProject/gestion_inventario/app/templates/features/sales_documents/sales_document_edit_form.html:6) · [Cierre del modal](/home/liandy/PyCharmMiscProject/gestion_inventario/app/static/js/core/modals.js:57)

La corrección reutiliza el fragmento de error de la vista previa y dirige el 422 a un aviso dentro del modal. El formulario conserva sus valores y permite corregir la fecha y volver a guardar. La revisión fue estática y en solo lectura. La verificación del bloque queda pendiente de aplicar la corrección.

**Ficheros revisados en el diff**

- `app/routers/ui_sales_documents.py`
- `app/templates/features/sales_documents/_valid_until_error.html`
- `app/templates/features/sales_documents/sales_document_edit_form.html`
- `app/templates/features/sales_documents/sales_document_preview.html`
- `specs/pdf-documentos-venta-oferta/verify/loteB.diff.txt`
- `specs/pdf-documentos-venta-oferta/verify/loteB.test.txt`

**Ficheros consultados como contexto**

- `specs/pdf-documentos-venta-oferta/tasks.md` (T15–T26)
- `app/routers/ui_common.py`
- `app/sales_documents_format.py`
- `app/static/js/core/modals.js`
- `app/static/js/core/swap.js`
- `app/static/js/htmx.min.js`
- `app/templates/components/sales_doc/issue_form.html`
- `app/templates/components/sales_doc/recent_documents.html`
- `app/templates/components/ui.html`
- `app/templates/features/sales_documents/sales_document_panel.html`
- `tests/snapshots/baseline/modal_sales_document.txt`
- `tests/snapshots/ui_snapshot.py`
- `/home/liandy/.local/share/harness-loop/pae-edit-format.md`

`````tasks-md
Lotes: **A** T1-T1

### T1: Mantener abierto el formulario al rechazar el vencimiento
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`, `app/templates/features/sales_documents/sales_document_edit_form.html`

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
        except ValueError:
            _log.info("valid_until rechazado: %r", raw_valid_until)
            return _valid_until_error_response(request, db=db, bid=bid, config=config)
@@ after
        except ValueError:
            _log.info("valid_until rechazado: %r", raw_valid_until)
            return templates.TemplateResponse(
                request=request,
                name="features/sales_documents/_valid_until_error.html",
                context={},
                status_code=422,
                headers={
                    "HX-Retarget": "#sales-doc-edit-valid-until-error",
                    "HX-Reswap": "innerHTML",
                    "X-Modal-Keep": "1",
                },
            )
````

````pae-edit
file: app/templates/features/sales_documents/sales_document_edit_form.html
op: insert_after
@@ anchor
    {{ field('Vencimiento (opcional)', 'valid_until', type='date', value=(doc.valid_until.isoformat() if doc.valid_until else '')) }}
@@ content
    <div id="sales-doc-edit-valid-until-error" role="alert"></div>
````

#### Verification

```bash
.venv/bin/python - <<'PY'
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from app.routers import ui_sales_documents as routes

request = MagicMock()
request.form = AsyncMock(return_value={
    "client_name": "Cliente",
    "client_id": "C1",
    "valid_until": "2026-11-17",
})
doc = SimpleNamespace(
    business_id=1,
    doc_type="P",
    issue_date=datetime(2026, 11, 18, tzinfo=timezone.utc),
    valid_until=None,
)
ctx = SimpleNamespace(
    user=object(),
    business_id=1,
    config=SimpleNamespace(sales_documents=SimpleNamespace(quote_validity_days=15)),
)

with patch.object(routes, "ui_context", return_value=ctx), \
     patch.object(routes, "SalesDocumentService") as service, \
     patch.object(routes, "_upsert_customer") as upsert:
    service.return_value.get.return_value = doc
    response = asyncio.run(routes.sales_doc_update(request, 1, db=MagicMock()))
    upsert.assert_not_called()

assert response.status_code == 422
assert response.headers["HX-Retarget"] == "#sales-doc-edit-valid-until-error"
assert response.headers["HX-Reswap"] == "innerHTML"
assert response.headers["X-Modal-Keep"] == "1"
assert "Fecha de vencimiento inválida" in response.body.decode()
template = "features/sales_documents/sales_document_edit_form.html"
source, _, _ = routes.templates.env.loader.get_source(routes.templates.env, template)
assert 'id="sales-doc-edit-valid-until-error" role="alert"' in source
routes.templates.env.get_template(template)
PY
```

En el navegador, edita un presupuesto, cambia una nota y la cantidad de una línea, y guarda con una fecha anterior a la emisión. Comprueba que aparece el aviso dentro del modal y que ambos cambios siguen en el formulario. Corrige la fecha, guarda y comprueba que se cierra el modal y se guardan los tres cambios.
`````