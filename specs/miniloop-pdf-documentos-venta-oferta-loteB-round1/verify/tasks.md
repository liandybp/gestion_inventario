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
