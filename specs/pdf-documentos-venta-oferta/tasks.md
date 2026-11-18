# Task Envelopes — PDF de documentos de venta con el diseño de la plantilla "OFERTA"

**Versión:** 0.5.1 → 0.5.2 (PATCH). **Rama base:** `develop` @ `ae8fda8`.
**Fuentes:** `requirements.md` (REQ-1..REQ-10) y `design.md` (§2.1..2.9). Las rutas y los textos de código citados
son los reales de `develop` @ `ae8fda8`.

> **Modo de ejecución:** todos los envelopes son `Apply: script`. El diseño ya resolvió cada decisión, así que
> `cc_local` no escribe código en este PAE: el intérprete `pae_edits.py` aplica los bloques literales y
> `cc_pae_check` los ensaya de punta a punta (incluido el e2e del Lote 0) antes del lanzamiento.
> Un commit por lote. Nunca push, sudo, systemctl ni scripts de despliegue.

Contrato Lote 0: `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`

Lotes: **A** T1–T14 · **B** T15–T19 · **C** T20–T23 · **D** T24–T26

---

# LOTE A — Lote 0 (camino feliz: negocio → documento P → PDF)

### T1: Test e2e del Lote 0 (contrato)

**Depends on:** none
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Crea `tests/e2e/test_sales_doc_pdf_oferta_e2e.py` con dos tests Playwright (REQ-10): un presupuesto en moneda `GEN` y una factura. Ningún otro envelope del PAE toca este fichero.

#### Instructions

````pae-edit
file: tests/e2e/test_sales_doc_pdf_oferta_e2e.py
op: create
@@ content
"""E2E del Lote 0 (REQ-10): negocio con datos bancarios -> presupuesto en moneda GEN -> PDF con el diseño OFERTA.

El PDF se descarga por `/ui/sales-doc/{id}/pdf` y se lee con pdfplumber. El segundo test emite una factura y
comprueba que su PDF no lleva "Fecha de vencimiento".
"""

from __future__ import annotations

import io
import re

import pdfplumber
import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

BANK_NAME = "Banco Metropolitano"
BANK_BRANCH = "248"
BANK_ACCOUNT = "0598770024085419"
PHONE = "54664732 / 50781349"
EMAIL = "autosandero@gmail.com"


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    # Espera a la carga inicial de #tab-content (hx-trigger=load): si se cambia de pestaña antes,
    # esa respuesta llega después y sobrescribe la pestaña nueva (test intermitente).
    page.locator("#tab-content > *").first.wait_for()


def _open_business_edit(page: Page) -> None:
    page.click("#tab-businesses")
    page.locator('tr:has(code:text-is("recambios")) button[title="Editar negocio"]').click()
    expect(page.locator("#edit-issuer-bank-account")).to_be_visible()


def _add_item(page: Page, sku: str, price: str, rows_after: int) -> None:
    form = page.locator("form#sales-doc-cart-add-form")
    price_field = form.locator('input[name="unit_price"]')
    product = form.locator('input[name="product"]')
    product.fill(sku)
    product.press("Tab")
    # El autorrelleno del precio (GET product-defaults, por OOB) deja el precio de venta del producto (2.0), pero el
    # navegador no lanza siempre `change` (sonda S10). Se espera un instante a que llegue y, llegue o no, el precio
    # se escribe después para que el autorrelleno no lo pise.
    try:
        expect(price_field).not_to_have_value("0", timeout=1500)
    except AssertionError:
        pass
    form.locator('input[name="quantity"]').fill("1")
    price_field.fill(price)
    expect(price_field).to_have_value(price)
    form.locator('button[type="submit"]').click()
    expect(page.locator('#sales-doc-panel input[id^="sales-doc-desc-"]')).to_have_count(rows_after)
    expect(page.locator(f"#sales-doc-price-{rows_after - 1}")).to_have_value(f"{float(price):.4f}")


def _fill_client(page: Page) -> None:
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('input[name="client_name"]').fill("Cliente E2E Oferta")
    issue.locator('input[name="client_id"]').fill("E2E-OFERTA-1")


def _issue_and_read_pdf(page: Page, base_url: str) -> tuple[str, "pdfplumber.page.Page", bytes]:
    page.click('form#sales-doc-issue-form button[type="submit"]')
    link = page.locator('#sales-doc-panel a:has-text("Descargar PDF")').first
    expect(link).to_be_visible()
    href = link.get_attribute("href")
    assert href and "/pdf" in href, href
    response = page.request.get(f"{base_url}{href}")
    assert response.ok, response.status
    assert "application/pdf" in response.headers.get("content-type", "")
    body = response.body()
    with pdfplumber.open(io.BytesIO(body)) as pdf:
        first_page = pdf.pages[0]
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        header_curves = len(first_page.within_bbox((0, 0, first_page.width, 125)).curves)
        header_images = len(first_page.within_bbox((0, 0, first_page.width, 125)).images)
    return text, header_curves + header_images, body


def test_quote_pdf_follows_oferta_design(live_server: str, page: Page) -> None:
    _login(page, live_server)

    # 1. El admin guarda teléfono, email, datos bancarios y plazo de validez del negocio.
    _open_business_edit(page)
    page.fill("#edit-issuer-holder", "Randy Manuel Delgado")
    page.fill("#edit-issuer-tax-id", "01030573448")
    page.fill("#edit-issuer-phone", PHONE)
    page.fill("#edit-issuer-email", EMAIL)
    page.fill("#edit-issuer-bank-name", BANK_NAME)
    page.fill("#edit-issuer-bank-branch", BANK_BRANCH)
    page.fill("#edit-issuer-bank-account", BANK_ACCOUNT)
    page.fill("#edit-issuer-bank-account-currency", "CUP")
    page.fill("#edit-sales-quote-validity-days", "15")
    page.get_by_role("button", name="Guardar Cambios").click()
    page.wait_for_load_state("networkidle")

    # Los cuatro datos bancarios sobreviven al guardado y al reabrir el modal.
    _open_business_edit(page)
    expect(page.locator("#edit-issuer-bank-name")).to_have_value(BANK_NAME)
    expect(page.locator("#edit-issuer-bank-branch")).to_have_value(BANK_BRANCH)
    expect(page.locator("#edit-issuer-bank-account")).to_have_value(BANK_ACCOUNT)
    expect(page.locator("#edit-issuer-bank-account-currency")).to_have_value("CUP")
    expect(page.locator("#edit-issuer-holder")).to_have_value("Randy Manuel Delgado")
    page.keyboard.press("Escape")

    # 2. Documentos: moneda GEN, tipo P, cliente y vencimiento; dos líneas en el carrito.
    page.click("#tab-documents")
    expect(page.locator("#sales-doc-panel")).to_be_visible()
    page.select_option("#sales-doc-currency-code", "GEN")
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('select[name="doc_type"]').select_option("P")
    _fill_client(page)
    issue.locator('input[name="valid_until"]').fill("2099-12-31")

    _add_item(page, "E2E-SKU1", "303875", 1)
    _add_item(page, "E2E-SKU2", "187000", 2)

    # La fecha elegida sobrevive a los cambios del carrito.
    expect(page.locator('form#sales-doc-issue-form input[name="valid_until"]')).to_have_value("2099-12-31")
    expect(page.locator('form#sales-doc-issue-form select[name="doc_type"]')).to_have_value("P")

    # 3. Emitir, descargar y leer el PDF.
    text, header_graphics, _body = _issue_and_read_pdf(page, live_server)

    assert "DESTINATARIO:" in text
    assert "No. Presupuesto:" in text
    assert "Fecha de vencimiento:" in text
    assert "31/12/2099" in text
    assert "Subtotal:" in text
    assert "TOTAL:" in text
    assert "490,875.00" in text
    assert "PRECIO CUP" in text
    assert "IMPORTE CUP" in text
    assert "CONCEPTO" in text and "UNIDADES" in text and "CANTIDAD" in text
    assert re.search(r"Producto E2E Uno\s+ud\s+1\s+303,875\.00\s+303,875\.00", text), text
    assert re.search(r"Producto E2E Dos\s+ud\s+1\s+187,000\.00\s+187,000\.00", text), text
    assert "TCP Randy Manuel Delgado" in text
    assert "NIT 01030573448" in text
    assert PHONE in text
    assert EMAIL in text
    assert "FORMA DE PAGO" in text
    assert f"{BANK_NAME}, Sucursal {BANK_BRANCH}" in text
    assert f"No. Cuenta (CUP): {BANK_ACCOUNT}" in text
    assert "Recibido por:" in text and "Cargo:" in text and "Firma:" in text
    # Iconos vectoriales de teléfono y email en la cabecera.
    assert header_graphics > 0
    # El diseño antiguo y el código interno de moneda no salen.
    assert "GEN" not in text
    assert "Tel.:" not in text
    assert "ID fiscal" not in text
    assert "(Precios sin IVA)" not in text


def test_invoice_pdf_has_no_due_date(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-documents")
    expect(page.locator("#sales-doc-panel")).to_be_visible()
    issue = page.locator("form#sales-doc-issue-form")
    issue.locator('select[name="doc_type"]').select_option("F")
    _fill_client(page)
    _add_item(page, "E2E-SKU1", "10", 1)

    text, _graphics, _body = _issue_and_read_pdf(page, live_server)

    assert "FACTURA" in text
    assert "No. Factura:" in text
    assert "Fecha de factura:" in text
    assert "FORMA DE PAGO" in text
    assert "Recibido por:" in text
    assert "Fecha de vencimiento" not in text
````

#### Verification

`.venv/bin/python -m pytest tests/e2e/test_sales_doc_pdf_oferta_e2e.py --collect-only -q --override-ini="addopts="` recoge 2 tests sin error de sintaxis. El test está en rojo hasta que cierre el Lote A.

#### Expected Report

- [ ] `tests/e2e/test_sales_doc_pdf_oferta_e2e.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

### T2: Modelo — columnas bancarias, plazo de validez y `valid_until`

**Depends on:** T1
**Skill:** backend-developer
**Apply:** script
**Files:** `app/models.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/models.py`. Añade a `Business` cuatro columnas bancarias y `sales_quote_validity_days`. Añade a `SalesDocument` la columna `valid_until` (REQ-1, REQ-2, REQ-3).

#### Instructions

````pae-edit
file: app/models.py
op: replace
@@ before
from datetime import datetime
@@ after
from datetime import date, datetime
````

````pae-edit
file: app/models.py
op: replace
@@ before
    Boolean,
    DateTime,
@@ after
    Boolean,
    Date,
    DateTime,
````

````pae-edit
file: app/models.py
op: replace
@@ before
    issuer_phone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
@@ after
    issuer_phone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    # Datos bancarios del negocio (bloque "Forma de pago" del PDF de facturas/presupuestos).
    issuer_bank_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    issuer_bank_branch: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    issuer_bank_account: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    issuer_bank_account_currency: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
````

````pae-edit
file: app/models.py
op: replace
@@ before
    sales_quote_label: Mapped[str] = mapped_column(String(64), default="Presupuesto", server_default="Presupuesto")
@@ after
    sales_quote_label: Mapped[str] = mapped_column(String(64), default="Presupuesto", server_default="Presupuesto")
    # Plazo de validez por defecto de un presupuesto, en días (0-365).
    sales_quote_validity_days: Mapped[int] = mapped_column(Integer, default=15, server_default="15")
````

````pae-edit
file: app/models.py
op: replace
@@ before
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subtotal: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False), default=0, server_default="0")
@@ after
    # Fecha de vencimiento: solo presupuestos (P). NULL en facturas y en presupuestos anteriores.
    valid_until: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    subtotal: Mapped[float] = mapped_column(Numeric(14, 4, asdecimal=False), default=0, server_default="0")
````

#### Verification

`.venv/bin/python -c "import app.models as m; print(m.SalesDocument.valid_until, m.Business.sales_quote_validity_days)"` imprime las dos columnas sin error.

#### Expected Report

- [ ] `app/models.py` modificado con los 5 bloques
- [ ] Ningún otro fichero modificado

### T3: Migraciones idempotentes (SQLite y PostgreSQL)

**Depends on:** T2
**Skill:** data-engineer
**Apply:** script
**Files:** `app/migrations.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/migrations.py`. Añade las cinco columnas nuevas de `businesses` a `_BUSINESS_CONFIG_COLS` y la columna `valid_until` de `sales_documents` a las dos funciones de migración. No modifiques ningún bloque existente (invariante 8).

#### Instructions

````pae-edit
file: app/migrations.py
op: replace
@@ before
    ("inventory_replenishment_lead_time_days", "NUMERIC(10,2) DEFAULT 25"),
@@ after
    ("inventory_replenishment_lead_time_days", "NUMERIC(10,2) DEFAULT 25"),
    ("issuer_bank_name", "VARCHAR(128)"),
    ("issuer_bank_branch", "VARCHAR(64)"),
    ("issuer_bank_account", "VARCHAR(64)"),
    ("issuer_bank_account_currency", "VARCHAR(8)"),
    ("sales_quote_validity_days", "INTEGER DEFAULT 15"),
````

````pae-edit
file: app/migrations.py
op: replace
@@ before
            conn.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_sales_documents_location_id ON sales_documents(location_id)"
            )

        mv_cols = {
@@ after
            conn.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_sales_documents_location_id ON sales_documents(location_id)"
            )

        if "valid_until" not in sales_doc_cols:
            conn.exec_driver_sql(
                "ALTER TABLE sales_documents ADD COLUMN valid_until DATE"
            )

        mv_cols = {
````

````pae-edit
file: app/migrations.py
op: replace
@@ before
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna location_id en sales_documents."
            ) from e
@@ after
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna location_id en sales_documents."
            ) from e

        try:
            conn.exec_driver_sql(
                "ALTER TABLE sales_documents ADD COLUMN IF NOT EXISTS valid_until DATE"
            )
        except SQLAlchemyError as e:
            raise RuntimeError(
                "No se pudo crear la columna valid_until en sales_documents."
            ) from e
````

#### Verification

`.venv/bin/python -c "import app.migrations"` termina sin error.

#### Expected Report

- [ ] `app/migrations.py` modificado con los 3 bloques
- [ ] Ningún otro fichero modificado

### T4: `IssuerConfig` y `SalesDocumentsConfig` con los campos nuevos

**Depends on:** T3
**Skill:** backend-developer
**Apply:** script
**Files:** `app/business_config.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/business_config.py`. Añade cuatro campos bancarios a `IssuerConfig` y `quote_validity_days` a `SalesDocumentsConfig`. `load_business_config` lee las claves nuevas del `.conf` con los mismos valores por defecto.

#### Instructions

````pae-edit
file: app/business_config.py
op: replace
@@ before
    email: str = ""
    phone: str = ""
@@ after
    email: str = ""
    phone: str = ""
    # Datos bancarios del negocio (bloque "Forma de pago" del PDF).
    bank_name: str = ""
    bank_branch: str = ""
    bank_account: str = ""
    bank_account_currency: str = ""
````

````pae-edit
file: app/business_config.py
op: replace
@@ before
    quote_label: str = "Presupuesto"
@@ after
    quote_label: str = "Presupuesto"
    # Plazo de validez por defecto de un presupuesto, en días.
    quote_validity_days: int = 15
````

````pae-edit
file: app/business_config.py
op: replace
@@ before
            phone=get("issuer", "phone", ""),
@@ after
            phone=get("issuer", "phone", ""),
            bank_name=get("issuer", "bank_name", ""),
            bank_branch=get("issuer", "bank_branch", ""),
            bank_account=get("issuer", "bank_account", ""),
            bank_account_currency=get("issuer", "bank_account_currency", ""),
````

````pae-edit
file: app/business_config.py
op: replace
@@ before
            quote_label=get("sales_documents", "quote_label", "Presupuesto"),
@@ after
            quote_label=get("sales_documents", "quote_label", "Presupuesto"),
            quote_validity_days=get("sales_documents", "quote_validity_days", "15"),
````

#### Verification

`.venv/bin/python -c "from app.business_config import BusinessConfig; c = BusinessConfig(); print(c.issuer.bank_account == '', c.sales_documents.quote_validity_days)"` imprime `True 15`.

#### Expected Report

- [ ] `app/business_config.py` modificado con los 4 bloques
- [ ] Ningún otro fichero modificado

### T5: `get_business_config` mapea las columnas nuevas

**Depends on:** T4
**Skill:** backend-developer
**Apply:** script
**Files:** `app/repositories/business_repository.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/repositories/business_repository.py`. `get_business_config` copia los cuatro datos bancarios y el plazo de validez (acotado a 0-365) desde `Business` hacia `BusinessConfig`.

#### Instructions

````pae-edit
file: app/repositories/business_repository.py
op: replace
@@ before
            phone=business.issuer_phone or "",
@@ after
            phone=business.issuer_phone or "",
            bank_name=business.issuer_bank_name or "",
            bank_branch=business.issuer_bank_branch or "",
            bank_account=business.issuer_bank_account or "",
            bank_account_currency=business.issuer_bank_account_currency or "",
````

````pae-edit
file: app/repositories/business_repository.py
op: replace
@@ before
            quote_label=(business.sales_quote_label or "Presupuesto").strip() or "Presupuesto",
@@ after
            quote_label=(business.sales_quote_label or "Presupuesto").strip() or "Presupuesto",
            quote_validity_days=max(
                0,
                min(
                    365,
                    int(business.sales_quote_validity_days if business.sales_quote_validity_days is not None else 15),
                ),
            ),
````

#### Verification

`.venv/bin/python -c "import app.repositories.business_repository"` termina sin error.

#### Expected Report

- [ ] `app/repositories/business_repository.py` modificado con los 2 bloques
- [ ] Ningún otro fichero modificado

### T6: Handlers de *Negocios* guardan los datos bancarios y el plazo

**Depends on:** T5
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_businesses.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_businesses.py`. `business_create` y `business_update` aceptan cinco `Form` nuevos. El servidor recorta banco a 128 caracteres, sucursal y cuenta a 64 y moneda a 8 (REQ-9). El plazo se acota a 0-365; un texto no numérico conserva el valor actual al editar y vale 15 al crear (REQ-2).

#### Instructions

````pae-edit
file: app/routers/ui_businesses.py
op: insert_before
@@ anchor
def _parse_spec_of(raw, fb_code, fb_name) -> tuple[str, str]:
@@ content
def _parse_validity_days(value, fallback: int = 15) -> int:
    """Plazo de validez en días: entero acotado a 0-365. Un texto no numérico devuelve `fallback`."""
    try:
        days = int((value or "").strip())
    except ValueError:
        return fallback
    return max(0, min(365, days))


````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
count: 2
@@ before
    issuer_phone: str = Form(""),
@@ after
    issuer_phone: str = Form(""),
    issuer_bank_name: str = Form(""),
    issuer_bank_branch: str = Form(""),
    issuer_bank_account: str = Form(""),
    issuer_bank_account_currency: str = Form(""),
````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
count: 2
@@ before
    sales_quote_label: str = Form("Presupuesto"),
@@ after
    sales_quote_label: str = Form("Presupuesto"),
    sales_quote_validity_days: str = Form(""),
````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
@@ before
    new_business.issuer_phone = _s(issuer_phone)
@@ after
    new_business.issuer_phone = _s(issuer_phone)
    new_business.issuer_bank_name = _s(issuer_bank_name)[:128]
    new_business.issuer_bank_branch = _s(issuer_bank_branch)[:64]
    new_business.issuer_bank_account = _s(issuer_bank_account)[:64]
    new_business.issuer_bank_account_currency = _s(issuer_bank_account_currency)[:8]
````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
@@ before
    new_business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
@@ after
    new_business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
    new_business.sales_quote_validity_days = _parse_validity_days(_s(sales_quote_validity_days), 15)
````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
@@ before
    business.issuer_phone = _s(issuer_phone)
@@ after
    business.issuer_phone = _s(issuer_phone)
    business.issuer_bank_name = _s(issuer_bank_name)[:128]
    business.issuer_bank_branch = _s(issuer_bank_branch)[:64]
    business.issuer_bank_account = _s(issuer_bank_account)[:64]
    business.issuer_bank_account_currency = _s(issuer_bank_account_currency)[:8]
````

````pae-edit
file: app/routers/ui_businesses.py
op: replace
@@ before
    business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
@@ after
    business.sales_quote_label = _s(sales_quote_label, "Presupuesto")
    current_validity_days = business.sales_quote_validity_days if business.sales_quote_validity_days is not None else 15
    business.sales_quote_validity_days = _parse_validity_days(_s(sales_quote_validity_days), int(current_validity_days))
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_businesses import _parse_validity_days as p; print(p('abc'), p('-3'), p('400'), p('30'))"` imprime `15 0 365 30`.

#### Expected Report

- [ ] `app/routers/ui_businesses.py` modificado con los 7 bloques
- [ ] Ningún otro fichero modificado

### T7: Formularios de *Negocios* con los campos nuevos

**Depends on:** T6
**Skill:** frontend-developer
**Apply:** script
**Files:** `app/templates/features/businesses/tab_businesses.html`, `app/templates/features/businesses/business_edit_form.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `tab_businesses.html` (formulario de crear) y `business_edit_form.html` (formulario de editar). Cada formulario gana cuatro campos bancarios con `maxlength` (128, 64, 64, 8) y el campo "Validez de presupuesto (días)". No añadas `style=`, `<script>` ni colores hexadecimales.

#### Instructions

````pae-edit
file: app/templates/features/businesses/tab_businesses.html
op: replace
@@ before
               placeholder="+58 212 555-1234" />
        <small class="muted">Ej: +58 212 555-1234</small>
      </div>
@@ after
               placeholder="+58 212 555-1234" />
        <small class="muted">Ej: +58 212 555-1234</small>
      </div>

      <div class="form-row">
        <div class="form-group">
          <label for="new-issuer-bank-name">Banco</label>
          <input type="text" id="new-issuer-bank-name" name="issuer_bank_name" maxlength="128" autocomplete="off"
                 placeholder="Banco Metropolitano" />
        </div>

        <div class="form-group">
          <label for="new-issuer-bank-branch">Sucursal</label>
          <input type="text" id="new-issuer-bank-branch" name="issuer_bank_branch" maxlength="64" autocomplete="off"
                 placeholder="248" />
        </div>
      </div>

      <div class="form-row">
        <div class="form-group">
          <label for="new-issuer-bank-account">Número de cuenta</label>
          <input type="text" id="new-issuer-bank-account" name="issuer_bank_account" maxlength="64" autocomplete="off" />
        </div>

        <div class="form-group">
          <label for="new-issuer-bank-account-currency">Moneda de la cuenta</label>
          <input type="text" id="new-issuer-bank-account-currency" name="issuer_bank_account_currency" maxlength="8"
                 autocomplete="off" placeholder="CUP" />
        </div>
      </div>
````

````pae-edit
file: app/templates/features/businesses/tab_businesses.html
op: replace
@@ before
          <small class="muted">Ej: Presupuesto, Quote</small>
        </div>
      </div>
@@ after
          <small class="muted">Ej: Presupuesto, Quote</small>
        </div>
      </div>

      <div class="form-row">
        <div class="form-group">
          <label for="new-sales-quote-validity-days">Validez de presupuesto (días)</label>
          <input type="number" id="new-sales-quote-validity-days" name="sales_quote_validity_days" min="0" max="365"
                 step="1" value="15" />
          <small class="muted">Días entre la emisión y el vencimiento (0-365)</small>
        </div>
      </div>
````

````pae-edit
file: app/templates/features/businesses/business_edit_form.html
op: replace
@@ before
                 value="{{ config.issuer.phone }}" />
          <small class="muted">Ej: +58 212 555-1234</small>
        </div>
@@ after
                 value="{{ config.issuer.phone }}" />
          <small class="muted">Ej: +58 212 555-1234</small>
        </div>

        <div class="form-row">
          <div class="form-group">
            <label for="edit-issuer-bank-name">Banco</label>
            <input type="text" id="edit-issuer-bank-name" name="issuer_bank_name" maxlength="128" autocomplete="off"
                   value="{{ config.issuer.bank_name }}" />
          </div>

          <div class="form-group">
            <label for="edit-issuer-bank-branch">Sucursal</label>
            <input type="text" id="edit-issuer-bank-branch" name="issuer_bank_branch" maxlength="64" autocomplete="off"
                   value="{{ config.issuer.bank_branch }}" />
          </div>
        </div>

        <div class="form-row">
          <div class="form-group">
            <label for="edit-issuer-bank-account">Número de cuenta</label>
            <input type="text" id="edit-issuer-bank-account" name="issuer_bank_account" maxlength="64" autocomplete="off"
                   value="{{ config.issuer.bank_account }}" />
          </div>

          <div class="form-group">
            <label for="edit-issuer-bank-account-currency">Moneda de la cuenta</label>
            <input type="text" id="edit-issuer-bank-account-currency" name="issuer_bank_account_currency" maxlength="8"
                   autocomplete="off" value="{{ config.issuer.bank_account_currency }}" />
          </div>
        </div>
````

````pae-edit
file: app/templates/features/businesses/business_edit_form.html
op: replace
@@ before
            <small class="muted">Ej: Presupuesto, Quote</small>
          </div>
        </div>
@@ after
            <small class="muted">Ej: Presupuesto, Quote</small>
          </div>
        </div>

        <div class="form-row">
          <div class="form-group">
            <label for="edit-sales-quote-validity-days">Validez de presupuesto (días)</label>
            <input type="number" id="edit-sales-quote-validity-days" name="sales_quote_validity_days" min="0" max="365"
                   step="1" value="{{ config.sales_documents.quote_validity_days }}" />
            <small class="muted">Días entre la emisión y el vencimiento (0-365)</small>
          </div>
        </div>
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_common import templates; templates.env.get_template('features/businesses/business_edit_form.html'); templates.env.get_template('features/businesses/tab_businesses.html')"` carga las dos plantillas sin error.

#### Expected Report

- [ ] `tab_businesses.html` y `business_edit_form.html` modificados con los 4 bloques
- [ ] Ningún otro fichero modificado

### T8: Funciones puras de formato y de vencimiento

**Depends on:** T7
**Skill:** backend-developer
**Apply:** script
**Files:** `app/sales_documents_format.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Crea `app/sales_documents_format.py` con `currency_column_label`, `format_amount`, `format_quantity`, `format_doc_date`, `resolve_valid_until` y la constante `INVALID_VALID_UNTIL` (design 2.4.2). El módulo no importa fpdf, FastAPI ni SQLAlchemy.

#### Instructions

````pae-edit
file: app/sales_documents_format.py
op: create
@@ content
"""Funciones puras de formato y de fecha de vencimiento de los documentos de venta.

Las usan el PDF (`app/sales_document_pdf.py`) y el router de documentos (`app/routers/ui_sales_documents.py`).
No importan fpdf, FastAPI ni SQLAlchemy, para poder probarlas sin PDF ni base de datos.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Literal, Optional

INVALID_VALID_UNTIL = "Fecha de vencimiento inválida"

_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def currency_column_label(prefix: str, currency_code: Optional[str]) -> str:
    """Etiqueta de una columna de importe: `GEN` se muestra como `CUP`; sin moneda, solo el prefijo."""
    code = (currency_code or "").strip().upper()
    if code == "GEN":
        code = "CUP"
    return f"{prefix} {code}" if code else prefix


def format_amount(value: Optional[float]) -> str:
    """Importe con separador de miles y dos decimales: 303875 -> `303,875.00`."""
    return f"{float(value or 0):,.2f}"


def format_quantity(value: Optional[float]) -> str:
    """Cantidad con hasta 4 decimales sin ceros finales y con separador de miles."""
    text = f"{float(value or 0):,.4f}".rstrip("0").rstrip(".")
    return text or "0"


def format_doc_date(value: Optional[date | datetime]) -> str:
    """Fecha `dd/mm/aaaa`; vacío si no hay fecha."""
    if value is None:
        return ""
    return value.strftime("%d/%m/%Y")


def resolve_valid_until(
    raw: Optional[str],
    *,
    doc_type: str,
    issue_date: datetime,
    validity_days: int,
    on_empty: Literal["default", "keep"] = "default",
    current: Optional[date] = None,
) -> Optional[date]:
    """Fecha de vencimiento de un documento.

    - `doc_type` distinto de `P`: devuelve `None` y no valida `raw`.
    - `raw` vacío con `on_empty="default"`: fecha de emisión + `validity_days`.
    - `raw` vacío con `on_empty="keep"`: devuelve `current` (puede ser `None`).
    - Otro valor: debe ser `aaaa-mm-dd` y no anterior a la fecha de emisión; si no, lanza `ValueError`.
    """
    if (doc_type or "").strip().upper() != "P":
        return None
    text = (raw or "").strip()
    issued_on = issue_date.date()
    if not text:
        if on_empty == "keep":
            return current
        return issued_on + timedelta(days=int(validity_days))
    if not _ISO_DATE.match(text):
        raise ValueError(INVALID_VALID_UNTIL)
    try:
        chosen = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(INVALID_VALID_UNTIL) from exc
    if chosen < issued_on:
        raise ValueError(INVALID_VALID_UNTIL)
    return chosen
````

#### Verification

`.venv/bin/python -c "from app.sales_documents_format import format_quantity as q, format_amount as a; print(q(1.0), q(2.5), q(0.0001), q(1234567.0), a(303875))"` imprime `1 2.5 0.0001 1,234,567 303,875.00`.

#### Expected Report

- [ ] `app/sales_documents_format.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

### T9: PDF con el diseño de la plantilla "OFERTA"

**Depends on:** T8
**Skill:** backend-developer
**Apply:** script
**Files:** `app/sales_document_pdf.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Reescribe `app/sales_document_pdf.py` con el diseño de design 2.4.1 (REQ-5..REQ-8). La firma de `build_sales_document_pdf` es la de la tabla de design 2.4.1: todos los argumentos son keyword-only y desaparece `currency_symbol`. El único llamador (`sales_doc_pdf` en `app/routers/ui_sales_documents.py`) se actualiza en T12, dentro del mismo lote.

#### Instructions

````pae-edit
file: app/sales_document_pdf.py
op: create
overwrite: true
@@ content
"""PDF de facturas y presupuestos con el diseño de la plantilla "OFERTA" (fpdf2, Helvetica, A4)."""

from __future__ import annotations

import io
from datetime import date, datetime
from typing import Any, Iterable, Optional

from app.sales_documents_format import (
    currency_column_label,
    format_amount,
    format_doc_date,
    format_quantity,
)

MARGIN = 15.0
CONTENT_W = 180.0
RIGHT_X = 107.7
RIGHT_W = 87.3
LABEL_W = 40.0
ROW_H = 4.9
PAGE_BOTTOM = 282.0
FOOTER_H = 43.0
BLUE_LIGHT = (220, 230, 241)
BLUE_DARK = (17, 90, 156)

# Iconos vectoriales (trazo en el azul de la caja TOTAL): teléfono y sobre.
_ICON_PHONE_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path fill="none" stroke="#115A9C" stroke-width="1.6" stroke-linejoin="round" d="M5 3h4l2 5-2.5 1.5a11 11 0 0 0 6 6L16 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 5a2 2 0 0 1 2-2z"/></svg>'
_ICON_MAIL_SVG = b'<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><rect x="3" y="5" width="18" height="14" rx="2" fill="none" stroke="#115A9C" stroke-width="1.6"/><path d="M3 7l9 6 9-6" fill="none" stroke="#115A9C" stroke-width="1.6"/></svg>'


def _safe_pdf_text(value: str) -> str:
    try:
        return (value or "").encode("latin-1", "replace").decode("latin-1")
    except Exception:
        return ""


def _fit_text(pdf: Any, text: str, width: float, style: str = "B", start: float = 9.0, minimum: float = 6.0) -> str:
    """Fija la fuente en el mayor tamaño (de `start` a `minimum`, pasos de 0,5 pt) en que `text` cabe en
    `width` mm (c_margin de 1 mm por lado). Si ni a `minimum` cabe, corta el texto y añade "..."."""
    available = width - 2.0
    size = start
    while size >= minimum:
        pdf.set_font("Helvetica", style, size)
        if pdf.get_string_width(text) <= available:
            return text
        size -= 0.5
    pdf.set_font("Helvetica", style, minimum)
    cut = text
    while len(cut) > 1 and pdf.get_string_width(cut + "...") > available:
        cut = cut[:-1]
    return cut + "..."


def _count_lines(pdf: Any, text: str, width: float, line_h: float) -> int:
    """Número de líneas que ocupa `text` en una celda de `width` mm con la fuente actual (sin dibujar)."""
    lines = pdf.multi_cell(width, line_h, text, align="L", dry_run=True, output="LINES")
    return max(1, len(lines))


def _double_rule(pdf: Any, y: float) -> None:
    pdf.set_fill_color(0, 0, 0)
    pdf.rect(MARGIN, y, CONTENT_W, 0.35, style="F")
    pdf.rect(MARGIN, y + 0.7, CONTENT_W, 0.35, style="F")


def _draw_header(
    pdf: Any,
    *,
    issuer_name: str,
    issuer_holder: Optional[str],
    issuer_tax_id: Optional[str],
    issuer_address: Optional[str],
    issuer_phone: Optional[str],
    issuer_email: Optional[str],
) -> float:
    """Banda azul clara: emisor a la izquierda, dirección/teléfono/email a la derecha. Devuelve su borde inferior."""
    top = 12.0
    left_x, left_w = MARGIN + 2.0, 88.0
    right_x, right_w = 107.0, 86.0
    icon_w = 5.0

    left_rows: list[tuple[str, float, float, str]] = [(issuer_name, 12.0, 6.0, "B")]
    if issuer_holder:
        left_rows.append((f"TCP {issuer_holder}", 9.0, 4.5, ""))
    if issuer_tax_id:
        left_rows.append((f"NIT {issuer_tax_id}", 9.0, 4.5, ""))

    right_rows: list[tuple[Optional[bytes], str]] = []
    if issuer_address:
        right_rows.append((None, issuer_address))
    if issuer_phone:
        right_rows.append((_ICON_PHONE_SVG, issuer_phone))
    if issuer_email:
        right_rows.append((_ICON_MAIL_SVG, issuer_email))

    left_h = 0.0
    for text, size, line_h, style in left_rows:
        pdf.set_font("Helvetica", style, size)
        left_h += _count_lines(pdf, _safe_pdf_text(text), left_w, line_h) * line_h
    pdf.set_font("Helvetica", "", 9)
    right_h = 0.0
    for icon, text in right_rows:
        width = right_w - (icon_w if icon else 0.0)
        right_h += _count_lines(pdf, _safe_pdf_text(text), width, 4.5) * 4.5 + 0.5

    band_h = max(26.0, max(left_h, right_h) + 6.0)
    pdf.set_fill_color(*BLUE_LIGHT)
    pdf.rect(MARGIN, top, CONTENT_W, band_h, style="F")

    y = top + 3.0
    for text, size, line_h, style in left_rows:
        pdf.set_font("Helvetica", style, size)
        pdf.set_xy(left_x, y)
        pdf.multi_cell(left_w, line_h, _safe_pdf_text(text), align="L", new_x="LEFT", new_y="NEXT")
        y = pdf.get_y()

    pdf.set_font("Helvetica", "", 9)
    y = top + 3.0
    for icon, text in right_rows:
        width = right_w - (icon_w if icon else 0.0)
        if icon is not None:
            pdf.image(io.BytesIO(icon), x=right_x, y=y + 0.2, w=4.0, h=4.0)
        pdf.set_xy(right_x + (icon_w if icon else 0.0), y)
        pdf.multi_cell(width, 4.5, _safe_pdf_text(text), align="L", new_x="LEFT", new_y="NEXT")
        y = pdf.get_y() + 0.5
    return top + band_h


def _draw_title_bar(pdf: Any, y: float, doc_label: str) -> float:
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(MARGIN, y)
    pdf.cell(CONTENT_W / 2, 6.0, "DESTINATARIO:")
    pdf.set_xy(MARGIN + CONTENT_W / 2, y)
    pdf.cell(CONTENT_W / 2, 6.0, _safe_pdf_text(doc_label.upper()), align="R")
    _double_rule(pdf, y + 6.0)
    return y + 8.0


def _draw_client(pdf: Any, y: float, client_name: str, client_id: str, client_address: Optional[str]) -> float:
    width = 88.0
    pdf.set_xy(MARGIN, y)
    pdf.set_font("Helvetica", "B", 9)
    pdf.multi_cell(width, ROW_H, _safe_pdf_text(f"CLIENTE: {client_name}"), align="L", new_x="LEFT", new_y="NEXT")
    pdf.set_font("Helvetica", "", 9)
    pdf.multi_cell(width, ROW_H, _safe_pdf_text(f"ID: {client_id}"), align="L", new_x="LEFT", new_y="NEXT")
    if client_address:
        pdf.multi_cell(width, ROW_H, _safe_pdf_text(client_address), align="L", new_x="LEFT", new_y="NEXT")
    return pdf.get_y()


def _draw_label_row(pdf: Any, y: float, label: str, value: str) -> float:
    """Fila azul clara etiqueta/valor del bloque documento. La etiqueta reduce su letra hasta caber."""
    pdf.set_fill_color(*BLUE_LIGHT)
    pdf.rect(RIGHT_X, y, RIGHT_W, ROW_H, style="F")
    text = _fit_text(pdf, _safe_pdf_text(label), LABEL_W)
    pdf.set_xy(RIGHT_X, y)
    pdf.cell(LABEL_W, ROW_H, text)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_xy(RIGHT_X + LABEL_W, y)
    pdf.cell(RIGHT_W - LABEL_W, ROW_H, _safe_pdf_text(value), align="R")
    return y + ROW_H


def _draw_totals(pdf: Any, y: float, subtotal: float, total: float) -> float:
    y = _draw_label_row(pdf, y, "Subtotal:", format_amount(subtotal))
    pdf.set_fill_color(*BLUE_DARK)
    pdf.rect(RIGHT_X, y, RIGHT_W, 9.9, style="F")
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_xy(RIGHT_X, y + 1.5)
    pdf.cell(LABEL_W, 6.9, "TOTAL:")
    pdf.set_xy(RIGHT_X + LABEL_W, y + 1.5)
    pdf.cell(RIGHT_W - LABEL_W, 6.9, format_amount(total), align="R")
    pdf.set_text_color(0, 0, 0)
    return y + 9.9


def _columns(price_label: str, amount_label: str) -> tuple[tuple[str, float, str], ...]:
    return (
        ("CONCEPTO", 78.0, "C"),
        ("UNIDADES", 20.0, "C"),
        ("CANTIDAD", 20.0, "C"),
        (price_label, 30.0, "R"),
        (amount_label, 32.0, "R"),
    )


def _draw_table_header(pdf: Any, y: float, price_label: str, amount_label: str) -> float:
    pdf.set_font("Helvetica", "B", 8)
    x = MARGIN
    for label, width, align in _columns(price_label, amount_label):
        pdf.set_xy(x, y)
        pdf.cell(width, 5.0, _safe_pdf_text(label), align=align)
        x += width
    _double_rule(pdf, y + 5.2)
    return y + 7.5


def _draw_rows(pdf: Any, y: float, items: Iterable[dict], price_label: str, amount_label: str) -> float:
    """Una fila por línea. Una fila nunca se parte entre dos páginas; la tabla repite su cabecera en la nueva."""
    for item in items:
        description = _safe_pdf_text(str(item.get("description") or ""))
        unit = _safe_pdf_text(str(item.get("unit_of_measure") or ""))
        pdf.set_font("Helvetica", "", 8)
        height = _count_lines(pdf, description, 78.0, 5.0) * 5.0
        if y + height > PAGE_BOTTOM:
            pdf.add_page()
            y = _draw_table_header(pdf, MARGIN, price_label, amount_label)
            pdf.set_font("Helvetica", "", 8)
        pdf.set_xy(MARGIN, y)
        pdf.multi_cell(78.0, 5.0, description, align="L", new_x="LEFT", new_y="NEXT")
        cells = (
            (unit, 20.0, "C"),
            (format_quantity(item.get("quantity")), 20.0, "C"),
            (format_amount(item.get("unit_price")), 30.0, "R"),
            (format_amount(item.get("line_total")), 32.0, "R"),
        )
        x = MARGIN + 78.0
        for text, width, align in cells:
            pdf.set_xy(x, y)
            pdf.cell(width, 5.0, text, align=align)
            x += width
        y += height
    return y


def _draw_notes(pdf: Any, y: float, notes: str) -> float:
    text = _safe_pdf_text(notes)
    pdf.set_font("Helvetica", "", 8)
    height = 5.0 + _count_lines(pdf, text, CONTENT_W, 4.0) * 4.0
    if y + height > PAGE_BOTTOM:
        pdf.add_page()
        y = MARGIN
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_xy(MARGIN, y)
    pdf.cell(CONTENT_W, 5.0, "Notas:")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(MARGIN, y + 5.0)
    pdf.multi_cell(CONTENT_W, 4.0, text, align="L", new_x="LEFT", new_y="NEXT")
    return y + height


def _bank_lines(
    bank_name: Optional[str],
    bank_branch: Optional[str],
    bank_account: Optional[str],
    bank_account_currency: Optional[str],
) -> list[str]:
    lines: list[str] = []
    head = ", ".join(part for part in ((bank_name or "").strip(), f"Sucursal {bank_branch.strip()}" if (bank_branch or "").strip() else "") if part)
    if head:
        lines.append(head)
    account = (bank_account or "").strip()
    if account:
        currency = (bank_account_currency or "").strip()
        lines.append(f"No. Cuenta ({currency}): {account}" if currency else f"No. Cuenta: {account}")
    return lines


def _draw_footer(pdf: Any, y: float, bank_lines: list[str]) -> None:
    """Pie a dos columnas: FORMA DE PAGO a la izquierda, Recibido por/Cargo/Firma/Fecha a la derecha."""
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_xy(MARGIN, y)
    pdf.cell(85.0, 5.0, "FORMA DE PAGO")
    left = ["Efectivo___", "Cheque___", "Transferencia Bancaria___", *bank_lines]
    for index, text in enumerate(left, start=1):
        fitted = _fit_text(pdf, _safe_pdf_text(text), 85.0, style="", start=8.0, minimum=6.0)
        pdf.set_xy(MARGIN, y + 5.0 * index)
        pdf.cell(85.0, 5.0, fitted)

    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(RIGHT_X, y)
    pdf.cell(RIGHT_W, 5.0, "Recibido por:")
    pdf.set_draw_color(0, 0, 0)
    pdf.line(RIGHT_X, y + 9.0, RIGHT_X + 75.0, y + 9.0)
    for index, label in enumerate(("Cargo:", "Firma:", "Fecha:"), start=2):
        pdf.set_xy(RIGHT_X, y + 5.0 * index)
        pdf.cell(RIGHT_W, 5.0, f"{label} " + "_" * 30)


def build_sales_document_pdf(
    *,
    doc_type: str,
    doc_label: str,
    code: str,
    issue_date: datetime,
    valid_until: Optional[date],
    currency_code: Optional[str],
    issuer_name: str,
    issuer_holder: Optional[str],
    issuer_tax_id: Optional[str],
    issuer_address: Optional[str],
    issuer_phone: Optional[str],
    issuer_email: Optional[str],
    bank_name: Optional[str],
    bank_branch: Optional[str],
    bank_account: Optional[str],
    bank_account_currency: Optional[str],
    client_name: str,
    client_id: str,
    client_address: Optional[str],
    items: Iterable[dict],
    subtotal: float,
    total: float,
    notes: Optional[str],
) -> bytes:
    try:
        from fpdf import FPDF  # type: ignore
    except ModuleNotFoundError as e:
        raise RuntimeError(
            "No se pudo generar el PDF porque falta la dependencia 'fpdf2'. "
            "Instala dependencias (pip install -r requirements.txt)."
        ) from e

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_margins(MARGIN, 12.0, MARGIN)
    pdf.set_auto_page_break(False)
    pdf.add_page()
    pdf.set_text_color(0, 0, 0)

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

    doc_end = _draw_label_row(pdf, y, f"No. {doc_label}:", code)
    doc_end = _draw_label_row(pdf, doc_end, f"Fecha de {doc_label.lower()}:", format_doc_date(issue_date))
    if (doc_type or "").strip().upper() == "P":
        doc_end = _draw_label_row(pdf, doc_end, "Fecha de vencimiento:", format_doc_date(valid_until))
    totals_end = _draw_totals(pdf, doc_end + 3.0, subtotal, total)

    price_label = currency_column_label("PRECIO", currency_code)
    amount_label = currency_column_label("IMPORTE", currency_code)
    y = _draw_table_header(pdf, max(client_end, totals_end) + 6.0, price_label, amount_label)
    y = _draw_rows(pdf, y, items, price_label, amount_label)
    _double_rule(pdf, y + 0.5)
    y += 3.0

    if (notes or "").strip():
        y = _draw_notes(pdf, y + 2.0, str(notes))

    y += 8.0
    if y + (FOOTER_H - 8.0) > PAGE_BOTTOM:
        pdf.add_page()
        y = MARGIN + 4.0
    _draw_footer(pdf, y, _bank_lines(bank_name, bank_branch, bank_account, bank_account_currency))

    return bytes(pdf.output())
````

#### Verification

`.venv/bin/python -c "import app.sales_document_pdf"` termina sin error.

#### Expected Report

- [ ] `app/sales_document_pdf.py` reescrito con el contenido exacto
- [ ] Ningún otro fichero modificado

### T10: Global de Jinja `default_valid_until`

**Depends on:** T9
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_common.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_common.py`. Añade el global de Jinja `default_valid_until(days)`: hoy (UTC) + `days` días en formato `aaaa-mm-dd`. El formulario de emisión lo usa como valor por defecto del campo de vencimiento (REQ-3).

#### Instructions

````pae-edit
file: app/routers/ui_common.py
op: replace
@@ before
from datetime import datetime, timezone
@@ after
from datetime import datetime, timedelta, timezone
````

````pae-edit
file: app/routers/ui_common.py
op: insert_after
@@ anchor
templates.env.globals["static_version"] = _static_asset_version  # callable: se invoca en cada render
@@ content


def default_valid_until(days: object) -> str:
    """Vencimiento por defecto de un presupuesto: hoy (UTC) + `days` días, en `aaaa-mm-dd`."""
    try:
        offset = int(days or 0)
    except (TypeError, ValueError):
        offset = 0
    return (datetime.now(timezone.utc).date() + timedelta(days=offset)).isoformat()


templates.env.globals["default_valid_until"] = default_valid_until
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_common import default_valid_until as d; print(len(d(15)))"` imprime `10`.

#### Expected Report

- [ ] `app/routers/ui_common.py` modificado con los 2 bloques
- [ ] Ningún otro fichero modificado

### T11: Router de documentos — borrador y carrito conservan `valid_until`

**Depends on:** T10
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_sales_documents.py`. El borrador de sesión (`_get_draft`, `_set_draft`) guarda `valid_until` como texto. Los seis handlers que reciben el formulario de emisión (`preview`, `cart/add`, `cart/update`, `cart/remove`, `cart/clear`, `issue`) aceptan `valid_until: str = Form("")`. Los cinco que llaman a `_set_draft` lo pasan (hallazgo 1 de Codex).

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
from app.sales_document_pdf import build_sales_document_pdf
@@ after
from app.sales_document_pdf import build_sales_document_pdf
from app.sales_documents_format import INVALID_VALID_UNTIL, format_doc_date, resolve_valid_until
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
        "notes": str(draft.get("notes") or "").strip() or None,
@@ after
        "notes": str(draft.get("notes") or "").strip() or None,
        "valid_until": str(draft.get("valid_until") or "").strip() or None,
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
    notes: Optional[str] = None,
) -> None:
    try:
        request.session["sales_doc_draft"] = {
@@ after
    notes: Optional[str] = None,
    valid_until: Optional[str] = None,
) -> None:
    try:
        request.session["sales_doc_draft"] = {
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            "notes": (notes or "").strip() or None,
        }
@@ after
            "notes": (notes or "").strip() or None,
            "valid_until": (valid_until or "").strip() or None,
        }
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
count: 6
@@ before
    currency_code: str = Form(""),
    db: Session = Depends(session_dep),
@@ after
    currency_code: str = Form(""),
    valid_until: str = Form(""),
    db: Session = Depends(session_dep),
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
count: 4
@@ before
        client_address=client_address,
        notes=notes,
    )
@@ after
        client_address=client_address,
        notes=notes,
        valid_until=valid_until,
    )
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            client_address=client_address,
            notes=notes,
        )
@@ after
            client_address=client_address,
            notes=notes,
            valid_until=valid_until,
        )
````

#### Verification

`.venv/bin/python -c "import app.routers.ui_sales_documents"` termina sin error.

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` modificado con los 7 bloques
- [ ] Ningún otro fichero modificado

### T12: Router de documentos — emisión con vencimiento y PDF con la firma nueva

**Depends on:** T11
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_sales_documents.py`. `sales_doc_issue` valida el vencimiento con `resolve_valid_until` antes de crear el cliente y de leer el carrito. Una fecha inválida devuelve el panel con el mensaje "Fecha de vencimiento inválida" y estado 422, conserva el carrito y guarda el borrador (REQ-3). `sales_doc_pdf` llama a `build_sales_document_pdf` con la firma nueva (REQ-5..REQ-8).

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: insert_before
@@ anchor
@router.post("/sales-doc/preview", response_class=HTMLResponse)
@@ content
def _valid_until_error_response(request: Request, *, db: Session, bid: int, config) -> HTMLResponse:
    """Panel de documentos con el error de vencimiento (422 HTML: htmx solo hace swap de 4xx `text/html`)."""
    return templates.TemplateResponse(
        request=request,
        name="features/sales_documents/sales_document_panel.html",
        context={
            "sales_doc_config": config.sales_documents.model_dump(),
            "currency": config.currency.model_dump(),
            "issuer": config.issuer.model_dump(),
            "pos_locations": config.locations.all_locations(),
            "default_doc_location_code": config.locations.default_sale_code(),
            "cart": _get_cart(request),
            "recent_documents": _recent_documents(db, limit=10, business_id=bid),
            "customers": _customers_list(db, business_id=bid, limit=200),
            "draft": _get_draft(request),
            "message": INVALID_VALID_UNTIL,
            "message_class": "error",
        },
        status_code=422,
    )


````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
    customer = _upsert_customer(db, client_id=client_id, name=client_name, address=client_address, business_id=bid)

    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    now = datetime.now(timezone.utc)
    year_month = now.strftime("%Y%m")
@@ after
    now = datetime.now(timezone.utc)
    try:
        valid_until_date = resolve_valid_until(
            valid_until,
            doc_type=doc_type_norm,
            issue_date=now,
            validity_days=int(config.sales_documents.quote_validity_days),
        )
    except ValueError:
        _log.info("valid_until rechazado: %r", valid_until)
        _set_draft(
            request,
            doc_type=doc_type_norm,
            location_code=selected_location_code,
            currency_code=selected_currency_code,
            currency_symbol=selected_currency_symbol,
            client_name=client_name,
            client_id=client_id,
            client_address=client_address,
            notes=notes,
            valid_until=valid_until,
        )
        return _valid_until_error_response(request, db=db, bid=bid, config=config)

    customer = _upsert_customer(db, client_id=client_id, name=client_name, address=client_address, business_id=bid)

    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    year_month = now.strftime("%Y%m")
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
        issue_date=now,
        issuer_name=config.issuer.name,
@@ after
        issue_date=now,
        valid_until=valid_until_date,
        issuer_name=config.issuer.name,
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
        pdf_bytes = build_sales_document_pdf(
            doc_label=doc_label,
            code=doc.code,
            issue_date=doc.issue_date,
            currency_symbol=doc.currency_symbol,
            issuer_name=doc.issuer_name,
            issuer_tax_id=doc.issuer_tax_id,
            issuer_holder=(config.issuer.holder or None),
            issuer_address=doc.issuer_address,
            client_name=doc.client_name,
            client_id=doc.client_id,
            client_address=doc.client_address,
            items=[
                {
                    "description": i.description,
                    "quantity": i.quantity,
                    "unit_price": i.unit_price,
                    "line_total": i.line_total,
                }
                for i in items
            ],
            subtotal=float(doc.subtotal or 0),
            total=float(doc.total or 0),
            notes=doc.notes,
        )
@@ after
        pdf_bytes = build_sales_document_pdf(
            doc_type=(doc.doc_type or "").upper(),
            doc_label=doc_label,
            code=doc.code,
            issue_date=doc.issue_date,
            valid_until=doc.valid_until,
            currency_code=doc.currency_code,
            issuer_name=doc.issuer_name,
            issuer_holder=(config.issuer.holder or None),
            issuer_tax_id=doc.issuer_tax_id,
            issuer_address=doc.issuer_address,
            issuer_phone=(config.issuer.phone or None),
            issuer_email=(config.issuer.email or None),
            bank_name=(config.issuer.bank_name or None),
            bank_branch=(config.issuer.bank_branch or None),
            bank_account=(config.issuer.bank_account or None),
            bank_account_currency=(config.issuer.bank_account_currency or None),
            client_name=doc.client_name,
            client_id=doc.client_id,
            client_address=doc.client_address,
            items=[
                {
                    "description": i.description,
                    "unit_of_measure": i.unit_of_measure,
                    "quantity": i.quantity,
                    "unit_price": i.unit_price,
                    "line_total": i.line_total,
                }
                for i in items
            ],
            subtotal=float(doc.subtotal or 0),
            total=float(doc.total or 0),
            notes=doc.notes,
        )
````

#### Verification

`.venv/bin/python -c "import app.routers.ui_sales_documents"` termina sin error.

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` modificado con los 4 bloques
- [ ] Ningún otro fichero modificado

### T13: Formulario de emisión con el campo de vencimiento

**Depends on:** T12
**Skill:** frontend-developer
**Apply:** script
**Files:** `app/templates/components/sales_doc/issue_form.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/templates/components/sales_doc/issue_form.html`. Añade tras "Notas" el campo `valid_until` (`type="date"`) con el valor del borrador o, si no hay, `default_valid_until(sales_doc_config.quote_validity_days)` (REQ-3).

#### Instructions

````pae-edit
file: app/templates/components/sales_doc/issue_form.html
op: replace
@@ before
    <div class="row"><label>Notas (opcional)</label><input name="notes" value="{{ draft.notes or '' }}" /></div>
@@ after
    <div class="row"><label>Notas (opcional)</label><input name="notes" value="{{ draft.notes or '' }}" /></div>
    <div class="row"><label>Vencimiento (solo presupuesto)</label><input type="date" name="valid_until" value="{{ draft.valid_until or default_valid_until(sales_doc_config.quote_validity_days) }}" /></div>
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_common import templates; templates.env.get_template('components/sales_doc/issue_form.html')"` carga la plantilla sin error.

#### Expected Report

- [ ] `app/templates/components/sales_doc/issue_form.html` modificado con 1 bloque
- [ ] Ningún otro fichero modificado

### T14: Regenerar las baselines del arnés de snapshots (formularios de Negocios y Documentos)

**Depends on:** T13
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/snapshots/baseline/*.txt`
**Pillars:** P1, P4, P5, P6, P9

#### Objective

Regenera las baselines con `python3 tests/snapshots/ui_snapshot.py write tests/snapshots/baseline`, que ejecuta el codemod `regen_baselines.py`. Solo cambian ficheros de `tests/snapshots/baseline/`. Lección de 0.5.1: el CI cayó por no regenerar las baselines en el mismo commit que las plantillas.

#### Instructions

````pae-edit
op: codemod
script: regen_baselines.py
runner: python
files: tests/snapshots/baseline/*.txt
````

#### Verification

`.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` termina con `OK` y exit 0.

#### Expected Report

- [ ] Solo ficheros de `tests/snapshots/baseline/` modificados
- [ ] El diff de cada baseline contiene solo los campos bancarios, el plazo de validez y el campo de vencimiento

---

# LOTE B — Vista previa y edición del vencimiento (REQ-4)

### T15: Vista previa valida y muestra el vencimiento

**Depends on:** T14
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_sales_documents.py`. `sales_doc_preview` valida `valid_until` con `resolve_valid_until` después de guardar el borrador. Una fecha inválida devuelve el fragmento `_valid_until_error.html` con estado 422. El contexto de la plantilla gana `valid_until_label` (`dd/mm/aaaa`) y `valid_until_iso`.

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    items, subtotal = _calc_cart_items(cart)
    total = subtotal
@@ after
    try:
        valid_until_date = resolve_valid_until(
            valid_until,
            doc_type=doc_type_norm,
            issue_date=datetime.now(timezone.utc),
            validity_days=int(config.sales_documents.quote_validity_days),
        )
    except ValueError:
        _log.info("valid_until rechazado: %r", valid_until)
        return templates.TemplateResponse(
            request=request,
            name="features/sales_documents/_valid_until_error.html",
            context={},
            status_code=422,
        )

    cart = _get_cart(request)
    if not cart:
        raise HTTPException(status_code=422, detail="El carrito está vacío")

    items, subtotal = _calc_cart_items(cart)
    total = subtotal
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
            "currency_code": selected_currency_code,
@@ after
            "currency_code": selected_currency_code,
            "valid_until_label": format_doc_date(valid_until_date),
            "valid_until_iso": valid_until_date.isoformat() if valid_until_date else "",
````

#### Verification

`.venv/bin/python -c "import app.routers.ui_sales_documents"` termina sin error.

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` modificado con los 2 bloques
- [ ] Ningún otro fichero modificado

### T16: Plantillas de la vista previa

**Depends on:** T15
**Skill:** frontend-developer
**Apply:** script
**Files:** `app/templates/features/sales_documents/_valid_until_error.html`, `app/templates/features/sales_documents/sales_document_preview.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Crea `_valid_until_error.html` (fragmento de error de la vista previa). Edita `sales_document_preview.html`: muestra "Vencimiento: dd/mm/aaaa" solo en un presupuesto y lleva la fecha ISO en un campo oculto del formulario "Emitir ahora".

#### Instructions

````pae-edit
file: app/templates/features/sales_documents/_valid_until_error.html
op: create
@@ content
{% from 'components/ui.html' import flash %}
{{ flash('Fecha de vencimiento inválida', 'error') }}
````

````pae-edit
file: app/templates/features/sales_documents/sales_document_preview.html
op: replace
@@ before
      <div><strong>Tipo:</strong> {{ doc_label }}</div>
@@ after
      <div><strong>Tipo:</strong> {{ doc_label }}</div>
      {% if doc_type == 'P' %}<div><strong>Vencimiento:</strong> {{ valid_until_label }}</div>{% endif %}
````

````pae-edit
file: app/templates/features/sales_documents/sales_document_preview.html
op: replace
@@ before
    <input type="hidden" name="notes" value="{{ notes or '' }}" />
@@ after
    <input type="hidden" name="notes" value="{{ notes or '' }}" />
    <input type="hidden" name="valid_until" value="{{ valid_until_iso or '' }}" />
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_common import templates; templates.env.get_template('features/sales_documents/_valid_until_error.html'); templates.env.get_template('features/sales_documents/sales_document_preview.html')"` carga las dos plantillas sin error.

#### Expected Report

- [ ] `_valid_until_error.html` creado con el contenido exacto
- [ ] `sales_document_preview.html` modificado con los 2 bloques
- [ ] Ningún otro fichero modificado

### T17: Edición de un documento cambia o conserva el vencimiento

**Depends on:** T16
**Skill:** backend-developer
**Apply:** script
**Files:** `app/routers/ui_sales_documents.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `app/routers/ui_sales_documents.py`. `sales_doc_update` valida `valid_until` de un presupuesto (`on_empty="keep"`) antes de tocar el cliente y las líneas. Una fecha inválida devuelve el panel con 422 y no cambia nada. Un campo vacío conserva el valor guardado, incluido `NULL`. Una factura fuerza `valid_until = NULL`.

#### Instructions

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")

    customer = _upsert_customer(db, client_id=client_id, name=client_name, address=client_address, business_id=bid)
    doc.customer_id = customer.id
@@ after
    if not client_id:
        raise HTTPException(status_code=422, detail="client_id is required")

    new_valid_until = None
    if (doc.doc_type or "").strip().upper() == "P":
        raw_valid_until = form.get("valid_until")
        try:
            new_valid_until = resolve_valid_until(
                raw_valid_until if isinstance(raw_valid_until, str) else "",
                doc_type="P",
                issue_date=doc.issue_date,
                validity_days=int(config.sales_documents.quote_validity_days),
                on_empty="keep",
                current=doc.valid_until,
            )
        except ValueError:
            _log.info("valid_until rechazado: %r", raw_valid_until)
            return _valid_until_error_response(request, db=db, bid=bid, config=config)

    customer = _upsert_customer(db, client_id=client_id, name=client_name, address=client_address, business_id=bid)
    doc.customer_id = customer.id
````

````pae-edit
file: app/routers/ui_sales_documents.py
op: replace
@@ before
    doc.notes = notes
    doc.currency_code = (doc.currency_code or config.currency.code)
@@ after
    doc.notes = notes
    doc.valid_until = new_valid_until
    doc.currency_code = (doc.currency_code or config.currency.code)
````

#### Verification

`.venv/bin/python -c "import app.routers.ui_sales_documents"` termina sin error.

#### Expected Report

- [ ] `app/routers/ui_sales_documents.py` modificado con los 2 bloques
- [ ] Ningún otro fichero modificado

### T18: Formulario de edición con el campo de vencimiento (solo presupuesto)

**Depends on:** T17
**Skill:** frontend-developer
**Apply:** script
**Files:** `app/templates/features/sales_documents/sales_document_edit_form.html`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `sales_document_edit_form.html`. El campo `valid_until` (`type="date"`) aparece solo si el documento es un presupuesto (`P`) y muestra el valor guardado o vacío.

#### Instructions

````pae-edit
file: app/templates/features/sales_documents/sales_document_edit_form.html
op: replace
@@ before
    {{ field('Notas (opcional)', 'notes', value=doc.notes or '') }}
@@ after
    {{ field('Notas (opcional)', 'notes', value=doc.notes or '') }}
    {% if (doc.doc_type or '')|upper == 'P' %}
    {{ field('Vencimiento (opcional)', 'valid_until', type='date', value=(doc.valid_until.isoformat() if doc.valid_until else '')) }}
    {% endif %}
````

#### Verification

`.venv/bin/python -c "from app.routers.ui_common import templates; templates.env.get_template('features/sales_documents/sales_document_edit_form.html')"` carga la plantilla sin error.

#### Expected Report

- [ ] `sales_document_edit_form.html` modificado con 1 bloque
- [ ] Ningún otro fichero modificado

### T19: Regenerar las baselines del arnés de snapshots (vista previa y edición)

**Depends on:** T18
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/snapshots/baseline/*.txt`
**Pillars:** P1, P4, P5, P6, P9

#### Objective

Regenera las baselines con el codemod `regen_baselines.py`. Solo cambian ficheros de `tests/snapshots/baseline/`. Si ninguna baseline cambia, el codemod termina igual con exit 0.

#### Instructions

````pae-edit
op: codemod
script: regen_baselines.py
runner: python
files: tests/snapshots/baseline/*.txt
````

#### Verification

`.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` termina con `OK` y exit 0.

#### Expected Report

- [ ] Solo ficheros de `tests/snapshots/baseline/` modificados (puede no haber ninguno)

---

# LOTE C — Tests unitarios (REQ-1..REQ-9)

### T20: Tests de las funciones puras de formato y vencimiento

**Depends on:** T19
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/unit/test_sales_documents_format.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Crea `tests/unit/test_sales_documents_format.py` con las tablas de casos de REQ-3 y REQ-6 y los formatos de REQ-5.

#### Instructions

````pae-edit
file: tests/unit/test_sales_documents_format.py
op: create
@@ content
"""Funciones puras de formato y de fecha de vencimiento (REQ-3, REQ-5, REQ-6)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from app.sales_documents_format import (
    INVALID_VALID_UNTIL,
    currency_column_label,
    format_amount,
    format_doc_date,
    format_quantity,
    resolve_valid_until,
)

ISSUE = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "code,label",
    [
        ("GEN", "IMPORTE CUP"),
        (" gen ", "IMPORTE CUP"),
        ("USD", "IMPORTE USD"),
        ("eur", "IMPORTE EUR"),
        ("", "IMPORTE"),
        (None, "IMPORTE"),
    ],
)
def test_currency_column_label_import(code, label) -> None:
    assert currency_column_label("IMPORTE", code) == label


def test_currency_column_label_price() -> None:
    assert currency_column_label("PRECIO", "usd") == "PRECIO USD"
    assert currency_column_label("PRECIO", "GEN") == "PRECIO CUP"
    assert currency_column_label("PRECIO", None) == "PRECIO"


@pytest.mark.parametrize(
    "value,expected",
    [(303875, "303,875.00"), (490875.0, "490,875.00"), (1234.5, "1,234.50"), (0, "0.00"), (None, "0.00")],
)
def test_format_amount(value, expected) -> None:
    assert format_amount(value) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (1.0, "1"),
        (2.5, "2.5"),
        (0.0001, "0.0001"),
        (1234567.0, "1,234,567"),
        (10.0, "10"),
        (3.1, "3.1"),
        (0, "0"),
        (None, "0"),
    ],
)
def test_format_quantity(value, expected) -> None:
    assert format_quantity(value) == expected


def test_format_doc_date() -> None:
    assert format_doc_date(date(2026, 12, 3)) == "03/12/2026"
    assert format_doc_date(datetime(2026, 1, 2, 23, 59, tzinfo=timezone.utc)) == "02/01/2026"
    assert format_doc_date(None) == ""


def test_resolve_valid_until_default_is_issue_date_plus_validity_days() -> None:
    assert resolve_valid_until("", doc_type="P", issue_date=ISSUE, validity_days=15) == date(2026, 12, 3)
    assert resolve_valid_until(None, doc_type="P", issue_date=ISSUE, validity_days=0) == date(2026, 11, 18)


def test_resolve_valid_until_accepts_chosen_date() -> None:
    assert resolve_valid_until("2026-12-31", doc_type="P", issue_date=ISSUE, validity_days=15) == date(2026, 12, 31)
    assert resolve_valid_until("2026-11-18", doc_type="p", issue_date=ISSUE, validity_days=15) == date(2026, 11, 18)


@pytest.mark.parametrize("raw", ["2026-12-31", "garbage", "", None])
def test_resolve_valid_until_invoice_is_always_none(raw) -> None:
    assert resolve_valid_until(raw, doc_type="F", issue_date=ISSUE, validity_days=15) is None


@pytest.mark.parametrize("raw", ["31/12/2026", "2026-11-17", "2026-13-40", "20261231", "abc"])
def test_resolve_valid_until_rejects_invalid_values(raw) -> None:
    with pytest.raises(ValueError, match=INVALID_VALID_UNTIL):
        resolve_valid_until(raw, doc_type="P", issue_date=ISSUE, validity_days=15)


def test_resolve_valid_until_keep_mode_returns_current() -> None:
    assert resolve_valid_until(
        "", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=date(2026, 12, 3)
    ) == date(2026, 12, 3)
    assert resolve_valid_until("", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=None) is None
    assert resolve_valid_until(
        "2026-12-20", doc_type="P", issue_date=ISSUE, validity_days=15, on_empty="keep", current=date(2026, 12, 3)
    ) == date(2026, 12, 20)
````

#### Verification

`.venv/bin/python -m pytest tests/unit/test_sales_documents_format.py -q --override-ini="addopts="` termina en PASS.

#### Expected Report

- [ ] `tests/unit/test_sales_documents_format.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

### T21: Tests del PDF (texto leído con pdfplumber)

**Depends on:** T20
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/unit/test_sales_document_pdf.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Crea `tests/unit/test_sales_document_pdf.py`. Cada test genera un PDF con `build_sales_document_pdf` y lee su texto con pdfplumber (REQ-5, REQ-6, REQ-7, REQ-8).

#### Instructions

````pae-edit
file: tests/unit/test_sales_document_pdf.py
op: create
@@ content
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
````

#### Verification

`.venv/bin/python -m pytest tests/unit/test_sales_document_pdf.py -q --override-ini="addopts="` termina en PASS.

#### Expected Report

- [ ] `tests/unit/test_sales_document_pdf.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

### T22: Tests de los handlers de documentos con `valid_until`

**Depends on:** T21
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/unit/test_sales_doc_valid_until.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Crea `tests/unit/test_sales_doc_valid_until.py`. Los tests llaman a los handlers `sales_doc_issue`, `sales_doc_preview`, `sales_doc_update`, `sales_doc_cart_add`, `sales_doc_cart_update`, `sales_doc_cart_remove` y `sales_doc_cart_clear` con un `Request` simulado (REQ-3, REQ-4).

#### Instructions

````pae-edit
file: tests/unit/test_sales_doc_valid_until.py
op: create
@@ content
"""Fecha de vencimiento en los handlers de documentos de venta (REQ-3, REQ-4)."""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import func, select

from app.models import Business, Location, SalesDocument, SalesDocumentItem
from app.routers import ui_context as _uc
from app.routers.ui_sales_documents import (
    _get_draft,
    sales_doc_cart_add,
    sales_doc_cart_clear,
    sales_doc_cart_remove,
    sales_doc_cart_update,
    sales_doc_issue,
    sales_doc_preview,
    sales_doc_update,
)

ERROR = "Fecha de vencimiento inválida"
CART_LINE = {"sku": None, "description": "Pastillas", "unit_of_measure": "JGO", "quantity": 1.0, "unit_price": 10.0}


def _today() -> date:
    return datetime.now(timezone.utc).date()


@pytest.fixture(autouse=True)
def _active_business(db_session, business_id, monkeypatch) -> None:
    monkeypatch.setattr(_uc, "get_active_business_id", lambda _db, _req, **_kw: business_id)
    db_session.add(Location(business_id=business_id, code="TEST_CENTRAL", name="Central", role="central", is_default=True))
    db_session.commit()


def _request(cart: list[dict] | None = None, draft: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.session = {}
    if cart is not None:
        req.session["sales_doc_cart"] = cart
    if draft is not None:
        req.session["sales_doc_draft"] = draft
    return req


def _form(**over) -> dict:
    data = dict(
        doc_type="P",
        location_code="TEST_CENTRAL",
        client_name="Cliente A",
        client_id="C-1",
        client_address="",
        notes="",
        currency_code="USD",
        valid_until="",
    )
    data.update(over)
    return data


def _issue(db, req, **over):
    return sales_doc_issue(request=req, db=db, **_form(**over))


def _documents(db) -> list[SalesDocument]:
    return list(db.scalars(select(SalesDocument).order_by(SalesDocument.id)))


def _body(response) -> str:
    return response.body.decode("utf-8")


def test_issue_quote_with_chosen_due_date(db_session) -> None:
    chosen = _today() + timedelta(days=40)

    response = _issue(db_session, _request([dict(CART_LINE)]), valid_until=chosen.isoformat())

    assert response.status_code == 200
    (doc,) = _documents(db_session)
    assert doc.doc_type == "P"
    assert doc.valid_until == chosen


def test_issue_quote_with_empty_due_date_uses_issue_date_plus_validity(db_session, business_id) -> None:
    db_session.get(Business, business_id).sales_quote_validity_days = 20
    db_session.commit()

    _issue(db_session, _request([dict(CART_LINE)]), valid_until="")

    (doc,) = _documents(db_session)
    assert doc.valid_until == doc.issue_date.date() + timedelta(days=20)


def test_issue_invoice_never_stores_due_date(db_session) -> None:
    _issue(db_session, _request([dict(CART_LINE)]), doc_type="F", valid_until=(_today() + timedelta(days=40)).isoformat())

    (doc,) = _documents(db_session)
    assert doc.doc_type == "F"
    assert doc.valid_until is None


@pytest.mark.parametrize("bad", ["31/12/2026", "no-es-fecha", (_today() - timedelta(days=1)).isoformat()])
def test_issue_quote_with_invalid_due_date_returns_422_html_and_keeps_everything(db_session, bad) -> None:
    req = _request([dict(CART_LINE)])

    response = _issue(db_session, req, valid_until=bad, notes="Nota importante", client_name="Cliente B", client_id="C-2")

    assert response.status_code == 422
    assert response.media_type == "text/html"
    assert ERROR in _body(response)
    assert _documents(db_session) == []
    assert req.session["sales_doc_cart"] == [CART_LINE]
    draft = _get_draft(req)
    assert draft["valid_until"] == bad
    assert draft["client_name"] == "Cliente B"
    assert draft["notes"] == "Nota importante"
    assert db_session.scalar(select(func.count()).select_from(SalesDocument)) == 0


def test_issue_invoice_ignores_a_garbage_due_date(db_session) -> None:
    response = _issue(db_session, _request([dict(CART_LINE)]), doc_type="F", valid_until="31/12/2026")

    assert response.status_code == 200
    assert len(_documents(db_session)) == 1


def test_issue_form_shows_the_default_due_date_and_keeps_the_typed_one(db_session, business_id) -> None:
    db_session.get(Business, business_id).sales_quote_validity_days = 15
    db_session.commit()
    default = "2026-12-03"

    shown_default = _issue(db_session, _request([dict(CART_LINE)]), doc_type="P", valid_until="31/12/2026")
    assert f'name="valid_until" value="{default}"' not in _body(shown_default)
    assert 'name="valid_until" value="31/12/2026"' in _body(shown_default)

    with patch("app.routers.ui_common.datetime", wraps=datetime) as clock:
        clock.now.return_value = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
        empty_draft = sales_doc_cart_clear(request=_request(), db=db_session, **_form(valid_until=""))
    assert f'name="valid_until" value="{default}"' in _body(empty_draft)


def test_cart_handlers_keep_the_due_date_in_the_draft(db_session) -> None:
    chosen = "2099-12-31"

    req = _request()
    sales_doc_cart_add(
        request=req, db=db_session, product="", description="Pastillas", quantity="1", unit_price="10",
        **_form(valid_until=chosen),
    )
    assert _get_draft(req)["valid_until"] == chosen
    assert len(req.session["sales_doc_cart"]) == 1

    req.session["sales_doc_draft"] = {}
    sales_doc_cart_update(
        request=req, db=db_session, index=0, description="Pastillas", quantity="2", unit_price="10",
        **_form(valid_until=chosen),
    )
    assert _get_draft(req)["valid_until"] == chosen

    req.session["sales_doc_draft"] = {}
    sales_doc_cart_remove(request=req, db=db_session, index=0, **_form(valid_until=chosen))
    assert _get_draft(req)["valid_until"] == chosen
    assert req.session["sales_doc_cart"] == []

    req.session["sales_doc_draft"] = {}
    response = sales_doc_cart_clear(request=req, db=db_session, **_form(valid_until=chosen))
    assert _get_draft(req)["valid_until"] == chosen
    assert f'name="valid_until" value="{chosen}"' in _body(response)


def test_preview_quote_shows_due_date_and_carries_it_to_the_issue_form(db_session) -> None:
    response = sales_doc_preview(request=_request([dict(CART_LINE)]), db=db_session, **_form(valid_until="2099-12-31"))

    body = _body(response)
    assert response.status_code == 200
    assert "Vencimiento:" in body and "31/12/2099" in body
    assert 'name="valid_until" value="2099-12-31"' in body


def test_preview_invoice_has_no_due_date(db_session) -> None:
    response = sales_doc_preview(
        request=_request([dict(CART_LINE)]), db=db_session, **_form(doc_type="F", valid_until="2099-12-31")
    )

    assert response.status_code == 200
    assert "Vencimiento" not in _body(response)


def test_preview_with_invalid_due_date_returns_422_html(db_session) -> None:
    req = _request([dict(CART_LINE)])

    response = sales_doc_preview(request=req, db=db_session, **_form(valid_until="31/12/2026"))

    assert response.status_code == 422
    assert ERROR in _body(response)
    assert _get_draft(req)["valid_until"] == "31/12/2026"


def _seed_quote(db, business_id: int, *, valid_until: date | None, doc_type: str = "P") -> SalesDocument:
    doc = SalesDocument(
        business_id=business_id,
        doc_type=doc_type,
        year_month="202611",
        seq=1,
        code=f"{doc_type}2026111800001",
        issue_date=datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc),
        valid_until=valid_until,
        issuer_name="Emisor",
        client_name="A",
        client_id="C-A",
        currency_code="USD",
        currency_symbol="USD",
        subtotal=10.0,
        total=10.0,
    )
    db.add(doc)
    db.flush()
    db.add(SalesDocumentItem(document_id=doc.id, line_no=1, description="Linea 1", quantity=1, unit_price=4, line_total=4))
    db.add(SalesDocumentItem(document_id=doc.id, line_no=2, description="Linea 2", quantity=1, unit_price=6, line_total=6))
    db.commit()
    return doc


def _update(db, doc_id: int, **fields):
    req = _request()
    req.form = AsyncMock(return_value=fields)
    return asyncio.run(sales_doc_update(request=req, doc_id=doc_id, db=db))


def test_update_quote_changes_only_the_due_date(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3))
    items = list(db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)))
    fields = {"client_name": "A", "client_id": "C-A", "valid_until": "2026-12-20"}
    for it in items:
        fields[f"desc_{it.id}"] = it.description
        fields[f"qty_{it.id}"] = str(it.quantity)
        fields[f"price_{it.id}"] = str(it.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until == date(2026, 12, 20)
    assert float(doc.total) == 10.0
    assert db_session.scalar(select(func.count()).select_from(SalesDocumentItem)) == 2


def test_update_old_quote_with_empty_due_date_stays_null(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=None)
    items = list(db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)))
    fields = {"client_name": "A", "client_id": "C-A", "notes": "Solo notas", "valid_until": ""}
    for it in items:
        fields[f"desc_{it.id}"] = it.description
        fields[f"qty_{it.id}"] = str(it.quantity)
        fields[f"price_{it.id}"] = str(it.unit_price)

    response = _update(db_session, doc.id, **fields)

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
    assert doc.notes == "Solo notas"


def test_update_with_invalid_due_date_changes_nothing(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3))
    first = db_session.scalars(select(SalesDocumentItem).where(SalesDocumentItem.document_id == doc.id)).first()

    response = _update(
        db_session, doc.id,
        client_name="B", client_id="C-B", valid_until="31/12/2026", **{f"del_{first.id}": "on"},
    )

    assert response.status_code == 422
    assert response.media_type == "text/html"
    assert ERROR in _body(response)
    db_session.refresh(doc)
    assert doc.client_name == "A"
    assert doc.valid_until == date(2026, 12, 3)
    assert float(doc.total) == 10.0
    assert db_session.scalar(select(func.count()).select_from(SalesDocumentItem)) == 2


def test_update_invoice_forces_null_due_date(db_session, business_id) -> None:
    doc = _seed_quote(db_session, business_id, valid_until=date(2026, 12, 3), doc_type="F")

    response = _update(db_session, doc.id, client_name="A", client_id="C-A", valid_until="2099-12-31")

    assert response.status_code == 200
    db_session.refresh(doc)
    assert doc.valid_until is None
````

#### Verification

`.venv/bin/python -m pytest tests/unit/test_sales_doc_valid_until.py -q --override-ini="addopts="` termina en PASS.

#### Expected Report

- [ ] `tests/unit/test_sales_doc_valid_until.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

### T23: Tests de los datos bancarios y del plazo de validez del negocio

**Depends on:** T22
**Skill:** test-automation-engineer
**Apply:** script
**Files:** `tests/unit/test_business_bank_fields.py`
**Pillars:** P1, P4, P5, P6, P8

#### Objective

Crea `tests/unit/test_business_bank_fields.py`. Los tests crean y editan negocios con los campos nuevos y comprueban el `BusinessConfig` resultante y el formulario de edición (REQ-1, REQ-2, REQ-9).

#### Instructions

````pae-edit
file: tests/unit/test_business_bank_fields.py
op: create
@@ content
"""Datos bancarios y plazo de validez del negocio (REQ-1, REQ-2, REQ-9)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from app.models import Business, User
from app.repositories.business_repository import get_business_config
from app.routers.ui_businesses import business_create, business_edit_form, business_update


@pytest.fixture
def admin_request() -> MagicMock:
    req = MagicMock()
    req.session = {"username": "admin", "active_business_id": 1}
    req.url.path = "/ui/tabs/businesses"
    return req


@pytest.fixture
def admin_user(db_session) -> User:
    user = User(username="admin", password_hash="pbkdf2_sha256$...", role="admin", is_active=True, business_id=1)
    db_session.add(user)
    db_session.commit()
    return user


def _business(db_session, **fields) -> Business:
    biz = Business(code="dyb", name="DyB Autopartes", **fields)
    db_session.add(biz)
    db_session.commit()
    return biz


def test_create_business_saves_bank_data_and_validity(admin_request, db_session, admin_user) -> None:
    response = business_create(
        request=admin_request,
        code="nuevo",
        name="Nuevo",
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        sales_quote_validity_days="30",
        locations_pos="",
        locations_default_pos="",
        db=db_session,
    )

    assert response.status_code == 200
    biz = db_session.scalar(select(Business).where(Business.code == "nuevo"))
    config = get_business_config(db_session, biz.id)
    assert config.issuer.bank_name == "Banco Metropolitano"
    assert config.issuer.bank_branch == "248"
    assert config.issuer.bank_account == "0598770024085419"
    assert config.issuer.bank_account_currency == "CUP"
    assert config.sales_documents.quote_validity_days == 30


def test_create_business_defaults_validity_to_15(admin_request, db_session, admin_user) -> None:
    business_create(
        request=admin_request, code="def", name="Def", sales_quote_validity_days="abc",
        locations_pos="", locations_default_pos="", db=db_session,
    )

    biz = db_session.scalar(select(Business).where(Business.code == "def"))
    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == 15


def test_update_saves_bank_data_and_keeps_the_rest_of_the_issuer(admin_request, db_session, admin_user) -> None:
    biz = _business(
        db_session,
        issuer_holder="Randy Manuel Delgado",
        issuer_tax_id="01030573448",
        issuer_phone="54664732 / 50781349",
        dividends_partners="Liandy,Randy",
    )

    response = business_update(
        request=admin_request,
        business_id=biz.id,
        code="dyb",
        name="DyB Autopartes",
        issuer_holder="Randy Manuel Delgado",
        issuer_tax_id="01030573448",
        issuer_phone="54664732 / 50781349",
        dividends_partners="Liandy,Randy",
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        db=db_session,
    )

    assert response.status_code == 200
    config = get_business_config(db_session, biz.id)
    assert config.issuer.bank_account == "0598770024085419"
    assert config.issuer.holder == "Randy Manuel Delgado"
    assert config.issuer.tax_id == "01030573448"
    assert config.issuer.phone == "54664732 / 50781349"
    assert config.dividends.partners == ["Liandy", "Randy"]


@pytest.mark.parametrize(
    "sent,expected",
    [("30", 30), ("abc", 15), ("-3", 0), ("400", 365), ("0", 0), ("", 15)],
)
def test_update_validity_days_is_bounded_and_non_numeric_keeps_the_current_value(
    admin_request, db_session, admin_user, sent, expected
) -> None:
    biz = _business(db_session, sales_quote_validity_days=15)

    business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        sales_quote_validity_days=sent, db=db_session,
    )

    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == expected


def test_update_with_non_numeric_validity_keeps_a_custom_value(admin_request, db_session, admin_user) -> None:
    biz = _business(db_session, sales_quote_validity_days=45)

    business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        sales_quote_validity_days="abc", db=db_session,
    )

    assert get_business_config(db_session, biz.id).sales_documents.quote_validity_days == 45


def test_update_trims_bank_fields_to_their_column_size(admin_request, db_session, admin_user) -> None:
    biz = _business(db_session)

    response = business_update(
        request=admin_request, business_id=biz.id, code="dyb", name="DyB Autopartes",
        issuer_bank_name="B" * 200, issuer_bank_branch="S" * 65, issuer_bank_account="9" * 70,
        issuer_bank_account_currency="CUPCUPCUP1", db=db_session,
    )

    assert response.status_code == 200
    db_session.refresh(biz)
    assert len(biz.issuer_bank_name) == 128
    assert len(biz.issuer_bank_branch) == 64
    assert len(biz.issuer_bank_account) == 64
    assert biz.issuer_bank_account_currency == "CUPCUPCU"


def test_edit_form_shows_the_saved_values_with_maxlength(admin_request, db_session, admin_user) -> None:
    biz = _business(
        db_session,
        issuer_bank_name="Banco Metropolitano",
        issuer_bank_branch="248",
        issuer_bank_account="0598770024085419",
        issuer_bank_account_currency="CUP",
        sales_quote_validity_days=30,
    )

    body = business_edit_form(request=admin_request, business_id=biz.id, db=db_session).body.decode("utf-8")

    assert 'name="issuer_bank_name" maxlength="128"' in body
    assert 'value="Banco Metropolitano"' in body
    assert 'value="248"' in body
    assert 'value="0598770024085419"' in body
    assert 'name="issuer_bank_account_currency" maxlength="8"' in body
    assert 'name="sales_quote_validity_days" min="0" max="365"' in body
    assert 'value="30"' in body
````

#### Verification

`.venv/bin/python -m pytest tests/unit/test_business_bank_fields.py -q --override-ini="addopts="` termina en PASS.

#### Expected Report

- [ ] `tests/unit/test_business_bank_fields.py` creado con el contenido exacto
- [ ] Ningún otro fichero modificado

---

# LOTE D — Versión 0.5.2 y documentación

### T24: Subir la versión a 0.5.2 (PATCH)

**Depends on:** T23
**Skill:** release-manager
**Apply:** script
**Files:** `VERSION`, `.bumpversion.cfg`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `VERSION` y `.bumpversion.cfg`. La versión pasa de `0.5.1` a `0.5.2`. Es un incremento PATCH, no MINOR.

#### Instructions

````pae-edit
file: VERSION
op: replace
@@ before
0.5.1
@@ after
0.5.2
````

````pae-edit
file: .bumpversion.cfg
op: replace
@@ before
current_version = 0.5.1
@@ after
current_version = 0.5.2
````

#### Verification

`cat VERSION` imprime `0.5.2` y `grep -n "current_version = 0.5.2" .bumpversion.cfg` encuentra una línea.

#### Expected Report

- [ ] `VERSION` y `.bumpversion.cfg` en 0.5.2
- [ ] Ningún otro fichero modificado

### T25: Entrada de 0.5.2 en `CHANGELOG.md`

**Depends on:** T24
**Skill:** technical-writer
**Apply:** script
**Files:** `CHANGELOG.md`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `CHANGELOG.md`. Añade la sección `0.5.2` encima de la sección `0.5.1`.

#### Instructions

````pae-edit
file: CHANGELOG.md
op: insert_before
@@ anchor
## 0.5.1 (2026-11-18 — cabecera TCP/NIT en documentos de venta)
@@ content
## 0.5.2 (2026-11-18 — PDF de facturas y presupuestos con el diseño de la plantilla "OFERTA")

- feat(sales-docs): el PDF de facturas y presupuestos sigue el diseño de la plantilla de referencia: banda de
  cabecera azul clara con emisor (nombre, TCP, NIT) y contacto con iconos vectoriales de teléfono y email, barra
  `DESTINATARIO:` con la etiqueta del documento, bloque cliente/documento (`No.`, fecha, fecha de vencimiento solo
  en presupuestos), fila `Subtotal:` y caja `TOTAL:`, tabla `CONCEPTO | UNIDADES | CANTIDAD | PRECIO | IMPORTE` con
  separador de miles y cabecera repetida en cada página, y pie con `FORMA DE PAGO` (datos bancarios) y
  `Recibido por / Cargo / Firma / Fecha`. Las columnas de importe llevan la moneda elegida al construir el
  documento (`GEN` se muestra como `CUP`). Fechas `dd/mm/aaaa`.
- feat(businesses): *Negocios → Crear/Editar* guarda banco, sucursal, número de cuenta y moneda de la cuenta
  (`issuer_bank_*`, con `maxlength` 128/64/64/8 y recorte en el servidor) y el plazo de validez de presupuestos
  (`sales_quote_validity_days`, 0-365, por defecto 15). Columnas añadidas de forma idempotente al arrancar.
- feat(sales-docs): campo `valid_until` (fecha de vencimiento) en `sales_documents`, solo para presupuestos. El
  formulario de emisión la rellena con hoy + plazo de validez y se puede cambiar; sobrevive a los cambios del
  carrito; se muestra en la vista previa y se edita en el modal de edición. Una fecha inválida devuelve un 422 HTML
  ("Fecha de vencimiento inválida") sin crear ni modificar nada. Los documentos anteriores siguen generando PDF.
- test: unit del formato, del PDF (leído con pdfplumber), de los handlers y de los datos del negocio; e2e del flujo
  completo (negocio → presupuesto en `GEN` → PDF; factura sin vencimiento); baselines de snapshots regeneradas.
````

#### Verification

`grep -n "## 0.5.2" CHANGELOG.md` encuentra una línea y `grep -n "## 0.5.1" CHANGELOG.md` sigue encontrando la suya.

#### Expected Report

- [ ] `CHANGELOG.md` modificado con la sección 0.5.2 encima de la 0.5.1
- [ ] Ningún otro fichero modificado

### T26: Estado actual en `CLAUDE.md` (raíz)

**Depends on:** T25
**Skill:** technical-writer
**Apply:** script
**Files:** `CLAUDE.md`
**Pillars:** P1, P4, P5, P6

#### Objective

Edita solo `CLAUDE.md` (raíz del repo). Añade la entrada de 0.5.2 al principio de la sección "Estado actual". No edites `.claude/CLAUDE.md`: lo aplica Claude Code en la revisión (envelope manual M1 del PAE).

#### Instructions

````pae-edit
file: CLAUDE.md
op: insert_after
@@ anchor
## Estado actual
@@ content

- [x] **0.5.2 — PDF de facturas y presupuestos con el diseño de la plantilla "OFERTA" (2026-11-18, sesión 26, rama
      `feature/pdf-documentos-venta-oferta`)** — PAE `specs/pdf-documentos-venta-oferta/` (26 envelopes, todos
      `Apply: script`; Fases 1-2 en Opus, Fases 3-4 en Sonnet-2). `app/sales_document_pdf.py` reescrito (banda
      azul, iconos SVG de teléfono y email, `DESTINATARIO:`, bloque documento, `Subtotal:`/`TOTAL:`, tabla con
      precio unitario y cabecera repetida, pie `FORMA DE PAGO` + `Recibido por`); funciones puras en
      `app/sales_documents_format.py`. Negocio: `issuer_bank_name/branch/account/account_currency` y
      `sales_quote_validity_days` (0-365, defecto 15), editables en *Negocios*. Documento: `sales_documents.valid_until`
      (solo `P`), por defecto emisión + plazo, editable al emitir y al editar, conservada en el borrador del carrito.
      Fecha inválida → 422 HTML "Fecha de vencimiento inválida" sin tocar nada. Columnas de importe con la moneda del
      documento (`GEN` → `CUP`). Migraciones idempotentes (SQLite y PostgreSQL). Baselines de snapshots regeneradas en
      el mismo lote que las plantillas (lección de 0.5.1).
      - Pendiente tras el despliegue: el usuario rellena en *Negocios → Editar* teléfono, email, banco y plazo de
        DyB Autopartes (los `.conf` son legacy y no se releen); el PDF lee teléfono, email y banco del negocio al
        generarse (como el TCP), no del documento emitido.
      - Fuera de alcance: Marca/Matrícula, plazo de ejecución, lema, número `LOT.`, icono de dirección, opciones de
        moneda nuevas en el selector del carrito.
````

#### Verification

`grep -n "0.5.2 — PDF de facturas" CLAUDE.md` encuentra una línea y `grep -c "^## Estado actual" CLAUDE.md` imprime `1`.

#### Expected Report

- [ ] `CLAUDE.md` modificado con la entrada 0.5.2 bajo "Estado actual"
- [ ] Ningún otro fichero modificado


