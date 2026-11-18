Lotes: **A** T1-T1

### T1: Rechazar un archivo en el campo de vencimiento al editar un presupuesto
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
        raw_valid_until = form.get("valid_until")
        try:
            new_valid_until = resolve_valid_until(
                raw_valid_until if isinstance(raw_valid_until, str) else "",
@@ after
        raw_valid_until = form.get("valid_until")
        try:
            if raw_valid_until is not None and not isinstance(raw_valid_until, str):
                raise ValueError("El vencimiento debe ser texto")
            new_valid_until = resolve_valid_until(
                raw_valid_until,
````

#### Verification

```bash
.venv/bin/python -B - <<'PY'
import asyncio
import io
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from starlette.datastructures import FormData, UploadFile
from app.routers import ui_sales_documents as router

doc = SimpleNamespace(
    doc_type="P", business_id=1, issue_date=datetime(2026, 1, 1),
    valid_until=date(2026, 1, 16),
)
config = SimpleNamespace(sales_documents=SimpleNamespace(quote_validity_days=15))
ctx = SimpleNamespace(user=None, business_id=1, config=config)
service = Mock()
service.get.return_value = doc
upload = UploadFile(io.BytesIO(b"2026-01-20"), filename="fecha.txt")
request = Mock()
request.form = AsyncMock(return_value=FormData([
    ("client_name", "Cliente nuevo"), ("client_id", "B"),
    ("valid_until", upload), ("del_1", "on"),
]))
db = Mock()
with (
    patch.object(router, "ui_context", return_value=ctx),
    patch.object(router, "SalesDocumentService", return_value=service),
    patch.object(router, "_upsert_customer") as upsert,
):
    response = asyncio.run(router.sales_doc_update(request, 1, db))
    assert response.status_code == 422
    assert response.headers["HX-Retarget"] == "#sales-doc-edit-valid-until-error"
    assert "Fecha de vencimiento inválida" in response.body.decode()
    upsert.assert_not_called()
    service.get_items_ordered.assert_not_called()
    db.commit.assert_not_called()
    assert doc.valid_until == date(2026, 1, 16)
PY
```
