# Design — PDF de documentos de venta con el diseño de la plantilla "OFERTA"

Modelo: Opus 5.5 (`cc_claude_code`, Opus-1) · Fecha: 2026-11-18 · Slug: `pdf-documentos-venta-oferta`
Requisitos: `specs/pdf-documentos-venta-oferta/requirements.md` (REQ-1..REQ-10).
Base: `develop` @ `ae8fda8` (0.5.1).

## 2.1 Codebase grounding

Lectura con Serena (`find_symbol` con cuerpo) y `grep` para las plantillas y el YAML de CI (fuera de la cobertura de
Serena). Líneas 1-based.

| Fichero | Rol | Qué toca |
|---|---|---|
| `app/models.py` | MODIFY | `Business` (l. 34-44, bloque issuer): +4 columnas bancarias +`sales_quote_validity_days`. `SalesDocument` (l. 154-169): +`valid_until: Mapped[Optional[date]] = mapped_column(Date, nullable=True)`. Import `date` y `Date` |
| `app/migrations.py` | MODIFY | `_BUSINESS_CONFIG_COLS` (l. 44-68): +5 tuplas, se aplican solas en SQLite (l. 292) y PG (l. 522). `_run_sqlite_schema_migrations` (l. 229-247, bloque `sales_doc_cols`): +`valid_until DATE`. `_run_postgresql_schema_migrations` (l. 450-477, mismo patrón que `location_id`): +`ADD COLUMN IF NOT EXISTS valid_until DATE`. `_backfill_business_config` **no** se toca (backfill único de la v4; hallazgo Codex 11) |
| `app/business_config.py` | MODIFY | `IssuerConfig` (l. 17-27): +`bank_name`, `bank_branch`, `bank_account`, `bank_account_currency` (str, ""). `SalesDocumentsConfig` (l. 35-39): +`quote_validity_days: int = 15`. `load_business_config` (l. 328-347): leer las claves nuevas (con los mismos valores por defecto) para que el modelo pydantic sea coherente; los `.conf` no se tocan |
| `app/repositories/business_repository.py` | MODIFY | `get_business_config` (l. 92-121): mapear las 5 columnas nuevas |
| `app/routers/ui_businesses.py` | MODIFY | Crear (l. 125-254) y actualizar (l. 330-431): 5 `Form` nuevos y sus asignaciones |
| `app/templates/features/businesses/tab_businesses.html` | MODIFY | Form de crear: 4 campos bancarios tras Teléfono (l. ~100) y "Validez de presupuesto (días)" tras la etiqueta de presupuesto (l. ~187) |
| `app/templates/features/businesses/business_edit_form.html` | MODIFY | Igual en el form de editar (l. ~95 y ~183) |
| `app/sales_documents_format.py` | CREATE | Funciones puras: `currency_column_label`, `format_amount`, `format_quantity`, `format_doc_date`, `resolve_valid_until` (ver 2.2) |
| `app/sales_document_pdf.py` | MODIFY (reescritura de `build_sales_document_pdf`) | Diseño nuevo (REQ-5..REQ-8). Firma nueva en 2.4 |
| `app/routers/ui_sales_documents.py` | MODIFY | `_get_draft`/`_set_draft` (l. 115-154): +`valid_until` (texto ISO o ""). `sales_doc_preview` (l. 183-252), `sales_doc_update` (l. 324-450), `sales_doc_cart_add` (l. 512), `sales_doc_cart_update` (l. ~668), `sales_doc_cart_remove` (l. ~749), `sales_doc_cart_clear` (l. ~812): +`valid_until: str = Form("")` y pasarlo a `_set_draft` (hallazgo Codex 1). `sales_doc_issue` (l. 870-996), `sales_doc_pdf` (l. 1000-1055). Helper nuevo `_valid_until_error_panel(...)` (ver 2.4.3) |
| `app/routers/ui_common.py` | MODIFY | Global de Jinja `default_valid_until(days)` junto a `static_version` (l. 68) |
| `app/templates/components/sales_doc/issue_form.html` | MODIFY | Campo `valid_until` (`type="date"`) tras "Notas" |
| `app/templates/features/sales_documents/sales_document_preview.html` | MODIFY | Línea "Vencimiento" (solo P) + `<input type="hidden" name="valid_until">` |
| `app/templates/features/sales_documents/_valid_until_error.html` | CREATE | Fragmento de error de la vista previa: `flash("Fecha de vencimiento inválida", "error")` |
| `app/templates/features/sales_documents/sales_document_edit_form.html` | MODIFY | Campo `valid_until` solo si `doc.doc_type == 'P'` |
| `tests/unit/test_sales_documents_format.py` | CREATE | Unit de las funciones puras |
| `tests/unit/test_sales_document_pdf.py` | CREATE | PDF → texto con `pdfplumber` (REQ-5..REQ-8) |
| `tests/unit/test_sales_doc_valid_until.py` | CREATE | Handlers issue/update/preview con `valid_until` (REQ-3, REQ-4) |
| `tests/unit/test_business_bank_fields.py` | CREATE | Crear/editar negocio con los campos nuevos (REQ-1, REQ-2) |
| `tests/e2e/test_sales_doc_pdf_oferta_e2e.py` | CREATE | Lote 0 / REQ-10 |
| `tests/snapshots/baseline/*.txt` | REGENERATE | 9 baselines afectadas (sonda S5) con `ui_snapshot.py write` |
| `VERSION`, `.bumpversion.cfg`, `CHANGELOG.md`, `CLAUDE.md`, `.claude/CLAUDE.md` | MODIFY | 0.5.1 → 0.5.2 y documentación |
| `app/sync/service.py`, `app/sync/client.py` | REFERENCE | El pull vuelca todas las columnas (`_dump_columns`, l. 46-50) y el cliente ya convierte `Date` desde ISO (`client.py` l. 129-130). Sin cambios |
| `tests/snapshots/ui_snapshot.py` | REFERENCE | Normaliza `\d{4}-\d{2}-\d{2}` a `<D>` (l. 110): la fecha por defecto del form no rompe las baselines |
| `.gitea/workflows/deploy.yml` | REFERENCE | CI instala `requirements.txt` (con `pdfplumber`), corre unit, `ui_snapshot.py check` y e2e |

Nota sobre el invariante 8 de `.claude/CLAUDE.md` ("no modificar archivos existentes en migrations"): las migraciones
son inline en `app/migrations.py` y el patrón aceptado (0.5.1, `issuer_holder`) es añadir tuplas a
`_BUSINESS_CONFIG_COLS` y bloques idempotentes nuevos. No se modifica ningún bloque existente.

### 2.1b Contexto externo

Ninguno. Firmas de fpdf2 2.8.7 comprobadas en las sondas S1 y S6.

## 2.1c Supuestos

| # | Supuesto | Sonda (comando) | Resultado | Evidencia |
|---|---|---|---|---|
| S1 | fpdf2 2.8.7 dibuja `rect(style="F")`, `line`, celdas con relleno y texto latin-1 (`á`), y `pdfplumber` 0.11.10 recupera ese texto en orden de línea | `.venv/bin/python -I /tmp/sdd-probe/s1.py` (PDF con banda, doble línea, caja TOTAL, texto; extracción con `pdfplumber`) | ✅ 7/7 cadenas encontradas, incluida `490,875.00` y `Mátricula` | `specs/pdf-documentos-venta-oferta/design/sondas/S1-fpdf-pdfplumber.txt` |
| S2 | SQLite acepta `ALTER TABLE sales_documents ADD COLUMN valid_until DATE`; SQLAlchemy `Date` lee `None` en filas antiguas y `date` en las nuevas; `date.fromisoformat` rechaza `""` y `dd/mm/aaaa` | `.venv/bin/python -I /tmp/sdd-probe/s2.py` | ✅ `OLD None`, `NEW date(2026,12,3)`; `ValueError` en `""` y `03/12/2026` → el parser trata `""` aparte (vacío = por defecto) | `specs/pdf-documentos-venta-oferta/design/sondas/S2-sqlite-date-column.txt` |
| S3 | La BD de dev es PostgreSQL ≥ 9.6 (`ADD COLUMN IF NOT EXISTS`); el negocio recambios usa USD; hay líneas con `unit_of_measure` vacío | `psql … -c "SET TRANSACTION READ ONLY; SELECT version(); SELECT code, currency_code … FROM businesses; …"` | ✅ PG 16.14; recambios USD, bazar MN, ropa EUR; documentos en GEN/EUR/USD; 3 líneas sin unidad → REQ-6 (GEN→CUP) y REQ-8 (unidad vacía) | `specs/pdf-documentos-venta-oferta/design/sondas/S3-dev-db-pg.txt` |
| S4 | Colores, líneas y geometría de la plantilla de referencia | `.venv/bin/python -I /tmp/sdd-probe/s4.py ~/Descargas/COMERCIO*.pdf` (rects, líneas y fuentes con `pdfplumber`) | ✅ banda `(0.863,0.902,0.945)` = RGB(220,230,241); caja TOTAL `(0.0667,0.353,0.612)` = RGB(17,90,156); dobles líneas negras de 1 pt separadas 1,9 pt; fuentes Calibri/Arial → Helvetica (core de fpdf2) | `specs/pdf-documentos-venta-oferta/design/sondas/S4-reference-colors.txt` |
| S5 | Qué baselines del arnés de snapshots cambian al tocar los forms de negocio y documentos | `grep -lE 'name="client_address"\|name="issuer_phone"\|Vista previa' tests/snapshots/baseline/*` | ✅ 9 baselines → el lote que toca plantillas regenera con `ui_snapshot.py write` en el mismo commit | `specs/pdf-documentos-venta-oferta/design/sondas/S5-snapshots-afectados.txt` |
| S6 | `jsonable_encoder` serializa `date` a ISO (pull de sync); formatos `{:,.2f}` | `.venv/bin/python -c "…jsonable_encoder…; get_string_width…"` | ✅ `'2026-12-03'`; `303,875.00`. ❌ `f"{x:g}"` pasa a notación científica con 7+ cifras; ❌ el ancho medido (31,9 mm) era con la fuente normal, no negrita (Codex 5) → ver S8 | `specs/pdf-documentos-venta-oferta/design/sondas/S6-json-date-y-formatos.txt` |
| S7 | `multi_cell(..., dry_run=True, output="LINES")` mide filas sin dibujar; el salto automático está en `page_break_trigger` | `.venv/bin/python -c "…multi_cell(dry_run=True, output='LINES')…"` | ✅ 4 líneas medidas; `page_break_trigger` = 277,0 mm (A4, margen 20 por defecto; con `set_auto_page_break(margin=15)` = 282) → medir cada fila y el pie antes de dibujarlos (Codex 8) | `specs/pdf-documentos-venta-oferta/design/sondas/S7-fpdf-dry-run.txt` |
| S9 | fpdf2 2.8.7 dibuja iconos SVG (teléfono y sobre, trazos en RGB(17,90,156)) con `pdf.image(BytesIO(svg), w=4, h=4)` sin dependencias nuevas, y el texto de al lado se sigue extrayendo | `.venv/bin/python -I specs/pdf-documentos-venta-oferta/design/sondas/S9-iconos-svg.py` + `pdftoppm` del resultado (revisado a ojo) | ✅ 3 curvas vectoriales; texto `54664732 / 50781349` y `autosandero@gmail.com` extraído; iconos nítidos a 150 dpi | `specs/pdf-documentos-venta-oferta/design/sondas/S9-iconos-svg.txt` |
| S8 | En Helvetica negrita, las etiquetas del bloque documento caben en 40 mm (con `c_margin` 1 mm por lado) reduciendo la letra; cantidad con hasta 4 decimales | `.venv/bin/python -c "…fit(text, 40)…; f'{v:,.4f}'.rstrip('0').rstrip('.')"` | ✅ "Fecha de vencimiento:" 34,23 mm y "Fecha de presupuesto:" 34,76 mm caben a 9 pt en 40 mm; "Fecha de presupuesto extraordinario:" necesita 6 pt → reducir de 0,5 en 0,5 hasta 6 pt y, si aún no cabe, cortar con "..."; ❌ `{:,.2f}` mostraba `0.00` para 0,0001 (Codex 12) → `{:,.4f}` sin ceros finales: `1`, `2.5`, `0.0001`, `1,234,567` | `specs/pdf-documentos-venta-oferta/design/sondas/S8-ajuste-etiquetas-y-cantidad.txt` |

## 2.1d Matriz de estados

El documento tiene ciclo de vida (borrador en sesión → emitido → editado → PDF). Resource: `valid_until`.

| Estado \ Evento | emitir P | emitir F | editar | PDF | fecha no válida |
|---|---|---|---|---|---|
| borrador (carrito + draft en sesión) | `valid_until` = form o emisión+N; carrito y draft se vacían tras el commit | `valid_until` = NULL; carrito y draft se vacían | cambios del carrito guardan `valid_until` en el draft (no se pierde) | no aplica (no hay documento) | 422 HTML (panel con mensaje); no se crea el documento; carrito intacto; draft guardado con la fecha escrita para corregirla |
| P emitido | no aplica | no aplica | `valid_until` = form; si llega vacío se mantiene el valor guardado | fila "Fecha de vencimiento:" con la fecha | 422 HTML en editar; el documento no cambia (validar antes de tocar cliente y líneas) |
| vista previa abierta y se cruza medianoche UTC | la fecha del hidden puede quedar anterior a la nueva emisión → 422 HTML en el panel; el draft guarda esa fecha y el form editable la muestra para corregirla (Codex 9) | no aplica (F ignora la fecha) | no aplica | no aplica | igual que fecha no válida |
| P antiguo (`valid_until` NULL) | no aplica | no aplica | se puede poner fecha; vacío = sigue NULL (modo `keep`, Codex 3) | fila "Fecha de vencimiento:" sin valor | 422 HTML; nada cambia |
| F emitida | no aplica | no aplica | el form no muestra el campo; el handler fuerza NULL | sin fila de vencimiento | se ignora el campo (F no valida fecha) |

Orden en `sales_doc_update`: validar `valid_until` **antes** de `_upsert_customer` y de recorrer líneas. Así un 422
no deja el documento a medias (la sesión no hace commit y el `session_dep` la cierra).

## 2.2 Enfoque

1. **Datos del negocio** (REQ-1, REQ-2): 5 columnas nuevas en `businesses`, siguiendo el camino de `issuer_holder`
   (modelo → `_BUSINESS_CONFIG_COLS` → `IssuerConfig`/`SalesDocumentsConfig` → `get_business_config` → forms y
   handlers de *Negocios*). El plazo de validez se guarda como `INTEGER DEFAULT 15` y se acota a 0-365 en el handler.
2. **Fecha de vencimiento** (REQ-3, REQ-4): columna `sales_documents.valid_until DATE NULL`. La regla vive en una
   función pura `resolve_valid_until(raw, *, doc_type, issue_date, validity_days) -> date | None` en
   `app/sales_documents_format.py`, usada por `sales_doc_issue`, `sales_doc_preview` (solo para validar y mostrar) y
   `sales_doc_update`. Lanza `ValueError("Fecha de vencimiento inválida")`; los handlers la convierten en
   `HTTPException(422, detail="Fecha de vencimiento inválida")` y hacen `_log.info("valid_until rechazado: %r", raw)`.
   Reglas: `doc_type != "P"` → `None`; `raw` vacío y `on_empty == "default"` → `issue_date.date() +
   timedelta(days=validity_days)`; `raw` vacío y `on_empty == "keep"` → `current` (puede ser `None`); si no,
   `date.fromisoformat(raw)`; si el resultado es anterior a `issue_date.date()` → `ValueError`.
   `sales_doc_issue` y `sales_doc_preview` usan `on_empty="default"`; `sales_doc_update` usa `on_empty="keep"` con
   `current=doc.valid_until` e `issue_date=doc.issue_date` (Codex 3: un P antiguo editado con el campo vacío sigue NULL).
   **Errores visibles (Codex 2):** el `HTTPException` devuelve JSON y el manejador HTMX (`app/static/js/core/swap.js`)
   solo inserta 4xx `text/html`. Por eso el error de vencimiento **no** usa `HTTPException`: `issue` y `update`
   devuelven el panel (`sales_document_panel.html`) con `message="Fecha de vencimiento inválida"`,
   `message_class="error"` y `status_code=422`, tras guardar el draft con los valores enviados (issue) o sin tocar el
   documento (update); `preview` devuelve `_valid_until_error.html` con 422. El modal de edición se cierra (el panel
   muestra el error), como cualquier 4xx HTML hoy.
3. **Valor por defecto en el form**: global de Jinja `default_valid_until(days: int) -> str` en `ui_common.py`
   (`(datetime.now(timezone.utc).date() + timedelta(days=int(days or 0))).isoformat()`). El form usa
   `draft.valid_until or default_valid_until(sales_doc_config.quote_validity_days)`. Así no hay que tocar los 11
   sitios que renderizan el panel: todos pasan ya `sales_doc_config`.
4. **PDF** (REQ-5..REQ-8): `build_sales_document_pdf` se reescribe con el diseño de 2.4.1. Los formatos van a
   `app/sales_documents_format.py` para probarlos sin PDF. Teléfono, email y datos bancarios se leen del negocio
   (`ctx.config.issuer`) al generar el PDF, como ya pasa con el TCP; nombre, NIT y dirección siguen saliendo de la
   copia guardada en el documento.
5. **Vista previa y draft**: la vista previa muestra "Vencimiento: dd/mm/aaaa" si `doc_type == "P"` y lleva
   `valid_until` (ISO) en un hidden. `_set_draft(valid_until=...)` guarda el texto tal cual (recortado, o `None` si
   vacío) y `_get_draft` lo devuelve; las cinco llamadas a `_set_draft` (preview y los cuatro handlers del carrito)
   pasan el `valid_until` del form, que llega por `hx-include` del form de emisión (Codex 1).
6. **Longitudes (Codex 7, REQ-9):** el handler recorta con `_s(v)[:n]` (banco 128, sucursal 64, cuenta 64, moneda 8)
   y los inputs llevan `maxlength` con el mismo `n`.

Invariantes que se mantienen:
- Routers UI sin ORM (`tests/unit/test_routers_no_orm.py`): los handlers solo asignan atributos a objetos ya
  cargados; el `commit` lo hace el router como hoy.
- Multi-negocio: `sales_doc_pdf` ya comprueba `doc.business_id == bid`; los datos bancarios salen de `ctx.config`
  del negocio activo, que es el del documento.
- Plantillas sin `style=`, `<script>` ni colores hex (`test_frontend_css.py`, `test_frontend_js_modules.py`).
- Sin dependencias nuevas.

## 2.3 Modelo de datos

### Entity: Business (`businesses`)

| Field | Type | Change | Default | Notes |
|---|---|---|---|---|
| issuer_bank_name | VARCHAR(128) | NEW | NULL | "Banco Metropolitano" |
| issuer_bank_branch | VARCHAR(64) | NEW | NULL | "248" |
| issuer_bank_account | VARCHAR(64) | NEW | NULL | "0598770024085419" (texto: ceros a la izquierda) |
| issuer_bank_account_currency | VARCHAR(8) | NEW | NULL | "CUP" |
| sales_quote_validity_days | INTEGER | NEW | 15 | 0-365 |

**Migración:** cinco tuplas al final de `_BUSINESS_CONFIG_COLS`:
`("issuer_bank_name", "VARCHAR(128)")`, `("issuer_bank_branch", "VARCHAR(64)")`,
`("issuer_bank_account", "VARCHAR(64)")`, `("issuer_bank_account_currency", "VARCHAR(8)")`,
`("sales_quote_validity_days", "INTEGER DEFAULT 15")`.
**Rollback:** el código anterior ignora las columnas; borrarlas es opcional (`ALTER TABLE businesses DROP COLUMN …`).

### Entity: SalesDocument (`sales_documents`)

| Field | Type | Change | Default | Notes |
|---|---|---|---|---|
| valid_until | DATE | NEW | NULL | Solo P. NULL en F y en P antiguos |

**Migración SQLite** (en `_run_sqlite_schema_migrations`, tras el bloque de `location_id`):
`if "valid_until" not in sales_doc_cols: conn.exec_driver_sql("ALTER TABLE sales_documents ADD COLUMN valid_until DATE")`.
**Migración PG** (en `_run_postgresql_schema_migrations`, bloque `try/except SQLAlchemyError` nuevo como el de
`location_id`): `ALTER TABLE sales_documents ADD COLUMN IF NOT EXISTS valid_until DATE`; error → `RuntimeError("No se
pudo crear la columna valid_until en sales_documents.")`.
**Rollback:** columna nullable; el código anterior la ignora.
**Índice:** no (no se filtra por vencimiento).
`_LATEST_SCHEMA_VERSION` no cambia: estas migraciones son idempotentes y corren en cada arranque, igual que
`issuer_holder` en 0.5.1.

## 2.4 Contratos

### 2.4.1 `build_sales_document_pdf` (firma nueva, keyword-only)

```python
def build_sales_document_pdf(
    *,
    doc_type: str,                 # "F" | "P"
    doc_label: str,                # "Factura" / "Presupuesto" / "Oferta"
    code: str,
    issue_date: datetime,
    valid_until: date | None,
    currency_code: str,
    issuer_name: str,
    issuer_holder: str | None,
    issuer_tax_id: str | None,
    issuer_address: str | None,
    issuer_phone: str | None,
    issuer_email: str | None,
    bank_name: str | None,
    bank_branch: str | None,
    bank_account: str | None,
    bank_account_currency: str | None,
    client_name: str,
    client_id: str,
    client_address: str | None,
    items: Iterable[dict],         # claves: description, unit_of_measure, quantity, unit_price, line_total
    subtotal: float,
    total: float,
    notes: str | None,
) -> bytes
```

Se quita `currency_symbol` (la moneda se muestra por `currency_code`, la que se eligió al construir el documento). Único llamador: `sales_doc_pdf`
(`ui_sales_documents.py:1023`, comprobado con `find_referencing_symbols`).

Geometría (A4, mm; márgenes izquierdo/derecho 15, ancho útil `W = 180`; fuente Helvetica):

| Bloque | Posición y estilo |
|---|---|
| Banda cabecera | Alto calculado: se miden ambas columnas con `multi_cell(dry_run=True, output="LINES")` y la banda mide `max(26, líneas × 4,5 + 4)`; `rect(15, 12, 180, alto, "F")`, relleno RGB(220,230,241). Cada dato se escribe con `multi_cell` de su ancho (las líneas largas se parten, Codex 5). Columna izquierda x=17, ancho 88: nombre `B 12`, luego `TCP <holder>`, `NIT <tax_id>` a `""` 9 pt, alto de línea 4,5. Columna derecha x=107, ancho 86, alineada a la izquierda, 9 pt: dirección (texto); teléfono con icono; email con icono (cada línea solo si hay dato). Icono: `pdf.image(io.BytesIO(_ICON_PHONE_SVG \| _ICON_MAIL_SVG), x=107, y=<y de la línea>, w=4, h=4)` y el texto en x=112, ancho 81. Los SVG son constantes `bytes` de módulo en `sales_document_pdf.py`, copiados literalmente de `specs/pdf-documentos-venta-oferta/design/sondas/S9-iconos-svg.py` (`PHONE` y `MAIL`) |
| Barra título | y=40: `DESTINATARIO:` `B 11` a la izquierda; etiqueta en mayúsculas `B 11` alineada a la derecha. Doble línea bajo ella: dos `rect` negros de 180 × 0,35 mm en y=46,0 y y=46,7 |
| Bloque cliente | y=48. Izquierda x=15, ancho 88: `CLIENTE: <nombre>` `B 9`, `ID: <client_id>` 9 pt, dirección (multi_cell) si hay |
| Bloque documento | Derecha x=107,7, ancho 87,3: filas de 4,9 mm con relleno RGB(220,230,241); etiqueta `B 9` en 40 mm (función `_fit_font_size`: baja de 0,5 en 0,5 pt hasta 6 pt; si aún no cabe, corta con "..."), valor 9 pt alineado a la derecha en 47,3 mm. Filas: `No. <doc_label>:` / código; `Fecha de <doc_label.lower()>:` / `dd/mm/aaaa`; solo si `doc_type == "P"`: `Fecha de vencimiento:` / fecha o vacío |
| Fila Subtotal | 3 mm bajo el bloque documento, x=107,7, 87,3 × 4,9 mm, relleno RGB(220,230,241): `Subtotal:` `B 9` a la izquierda, `format_amount(subtotal)` a la derecha |
| Caja TOTAL | Pegada bajo la fila Subtotal, x=107,7, 87,3 × 9,9 mm, relleno RGB(17,90,156), texto blanco `B 11`: `TOTAL:` a la izquierda, `format_amount(total)` a la derecha |
| Tabla | 6 mm bajo la caja (o bajo el bloque cliente si es más largo). Columnas: CONCEPTO 78 (centrado en cabecera, izquierda en filas), UNIDADES 20 (centro), CANTIDAD 20 (centro), PRECIO <moneda> 30 (derecha), IMPORTE <moneda> 32 (derecha). Cabecera `B 8`, doble línea bajo la cabecera; filas 8 pt, sin bordes. Cada fila: medir la descripción con `multi_cell(78, 5, …, dry_run=True, output="LINES")` → `h = len(lines) × 5`; si `get_y() + h > page_break_trigger`, `add_page()` y repetir la cabecera de la tabla; guardar `y`, dibujar la descripción con `multi_cell` y las otras cuatro celdas con `set_xy(x, y)` y alto `h`; después `set_xy(15, y + h)`. Durante la tabla y el pie, `set_auto_page_break(False)` para que fpdf no salte a mitad de fila (Codex 8); se reactiva al final. Doble línea al final |
| Notas | si hay: `Notas:` `B 8` + `multi_cell` 8 pt |
| Pie | 8 mm bajo la tabla. Izquierda x=15: `FORMA DE PAGO` `B 8`, `Efectivo___`, `Cheque___`, `Transferencia Bancaria___`; si `bank_name`: `<bank_name>, Sucursal <bank_branch>` (sin ", Sucursal …" si no hay sucursal); si `bank_account`: `No. Cuenta (<moneda>): <cuenta>` (sin paréntesis si no hay moneda). Derecha x=107,7: `Recibido por:`, línea de 75 mm, `Cargo: ____`, `Firma: ____`, `Fecha: ____`. 8 pt, alto 5 |
| Salto de página | Pie: alto fijo `FOOTER_H = 8 + 7 × 5 = 43` mm; si `get_y() + FOOTER_H > 282`, `add_page()` antes del pie. Notas: medir igual que una fila. Límite inferior 282 mm (margen 15) |

Texto: todo pasa por `_safe_pdf_text` (latin-1, como hoy). Colores restaurados a negro tras la caja TOTAL.

### 2.4.2 `app/sales_documents_format.py` (nuevo, funciones puras)

```python
def currency_column_label(prefix: str, currency_code: str | None) -> str   # ("IMPORTE","GEN") -> "IMPORTE CUP"; ("PRECIO","usd") -> "PRECIO USD"; ("IMPORTE",None) -> "IMPORTE"
def format_amount(value: float | None) -> str                         # 303875 -> "303,875.00"; None -> "0.00"
def format_quantity(value: float | None) -> str                       # f"{v:,.4f}".rstrip("0").rstrip("."): 1.0 -> "1"; 2.5 -> "2.5"; 0.0001 -> "0.0001"; 1234567.0 -> "1,234,567"; None -> "0"
def format_doc_date(value: date | datetime | None) -> str             # -> "03/12/2026"; None -> ""
def resolve_valid_until(raw: str | None, *, doc_type: str, issue_date: datetime, validity_days: int,
                        on_empty: Literal["default", "keep"] = "default",
                        current: date | None = None) -> date | None
```

### 2.4.3 Endpoints HTMX (sin rutas nuevas)

| Ruta | Cambio |
|---|---|
| `POST /ui/sales-doc/preview` | +`valid_until: str = Form("")`. Guarda el draft (con `valid_until`) y valida con `resolve_valid_until` (`issue_date = now UTC`) → 422 con `_valid_until_error.html`. Contexto +`valid_until` (`date` o `None`) y `valid_until_iso` |
| `POST /ui/sales-doc/issue` | +`valid_until: str = Form("")`. Valida **antes** de `_upsert_customer` y de leer el carrito; si falla: `_set_draft(...)` con todos los campos enviados y respuesta del panel con mensaje de error y 422 (sin documento, carrito intacto). `SalesDocument(valid_until=…)` |
| `POST /ui/sales-doc/{id}/update` | Lee `form.get("valid_until")`. Si `doc.doc_type == "P"`: valida (`on_empty="keep"`) **antes** de tocar cliente y líneas; si falla, panel con error y 422 sin cambios. Si no es P: `doc.valid_until = None` |
| `POST /ui/sales-doc/cart/{add,update,remove,clear}` | +`valid_until: str = Form("")`, pasado a `_set_draft` |
| `GET /ui/sales-doc/{id}/pdf` | Pasa la firma nueva (2.4.1) con `ctx.config.issuer.*` para teléfono, email y banco, `subtotal=float(doc.subtotal or 0)` y `unit_price` en cada item |
| `POST /ui/businesses` y `/ui/businesses/{id}/update` | +`issuer_bank_name`, `issuer_bank_branch`, `issuer_bank_account`, `issuer_bank_account_currency`, `sales_quote_validity_days` (str; entero acotado 0-365, no numérico → 15 en crear y valor actual en editar) |

Auth: sin cambios (los mismos `ctx`/roles de hoy: el operador emite y edita documentos; solo admin edita negocios).

## 2.5 Seguridad

| Check | Answer |
|---|---|
| New auth boundary? | No. Rutas existentes con su control de acceso actual |
| New sensitive data? | Cuenta bancaria del negocio: dato que el negocio publica en sus documentos. No es PII de clientes |
| Input validation? | `valid_until` por `date.fromisoformat` + no anterior a la emisión; plazo entero acotado; textos de banco recortados (`_s`) y limitados por el tamaño de columna (VARCHAR) |
| XSS | Jinja autoescape en forms y vista previa; sin `|safe` nuevo |
| IDOR | `sales_doc_pdf`/`update` ya comprueban `business_id`; sin cambio |
| Secrets | Ninguno |
| CSP | Sin JS ni estilos inline nuevos; `<input type="date">` nativo |

## 2.6 Trade-offs

| Decision | Chosen | Alternative | Why not the alternative |
|---|---|---|---|
| Dónde vive el banco | Columnas en `businesses` | Copia en cada documento | Pedido como dato fijo del negocio; misma decisión que el TCP en 0.5.1. Coste: un PDF antiguo regenerado muestra el banco actual |
| Fecha de vencimiento | Columna `valid_until` en el documento | Calcularla al vuelo (emisión + plazo) | El usuario quiere editarla por documento |
| Fecha por defecto en el form | Global de Jinja `default_valid_until` | Pasar `default_valid_until` en los 11 renders del panel | Un solo sitio; `sales_doc_config` ya llega a todos |
| Formatos | Módulo puro `sales_documents_format.py` | Dentro de `sales_document_pdf.py` | Tests sin PDF; el router también usa `resolve_valid_until` sin importar fpdf |
| Iconos de cabecera | SVG embebidos como constantes `bytes` (S9) | PNG en `app/static/` / texto `Tel.:` | Decisión del usuario (iconos); SVG vectorial sin ficheros ni dependencias nuevas |
| Precio unitario y subtotal | Se imprimen (columna PRECIO y fila Subtotal) | Solo cantidad e importe, como la referencia | Decisión del usuario (2ª ronda): el cliente ve el precio por unidad |

## 2.7 Migración y rollback

```
Despliegue:
1. Arranque: ADD COLUMN idempotente (5 en businesses, 1 en sales_documents), SQLite y PG.
2. Documentos existentes: valid_until NULL → PDF sin fecha de vencimiento (REQ-8).
3. Producción: el usuario rellena en Negocios → Editar teléfono, email, banco y plazo (los .conf no se releen).
Rollback:
1. Revertir a 0.5.1: el código anterior ignora las columnas nuevas.
2. No hace falta borrar columnas ni datos.
```

## 2.8 Tests y verificación

- **Lote 0 (e2e, Playwright, `tests/e2e/test_sales_doc_pdf_oferta_e2e.py`):** login admin → *Negocios → Editar* del
  negocio activo (teléfono, email, banco, plazo 15, guardar) → *Documentos*: elegir moneda `GEN`, añadir
  `E2E-SKU1` (cantidad 1, precio 303875) y `E2E-SKU2` (cantidad 1, precio 187000) — unidad `ud` del seed
  (`tests/e2e/conftest.py:63`) —, tipo P, cliente, vencimiento `2099-12-31`, comprobar que el campo sigue en
  `2099-12-31` tras añadir la segunda línea, Emitir → leer el enlace "Descargar PDF" → `page.request.get(href)` →
  texto con `pdfplumber` → `PRECIO CUP`, `IMPORTE CUP`, `ud`, `303,875.00`, `Subtotal:`, `490,875.00`, `31/12/2099`,
  banco y cuenta; `len(page.images) + len(page.curves) > 0` en la banda de cabecera (iconos); negativas: sin "GEN", sin
  "Tel.:" (Codex 4). JGO/U se prueban en el unit del PDF. Segundo test: tipo F →
  PDF con "No. Factura:" y **sin** "Fecha de vencimiento". El test usa el fixture `live_server` y el patrón de login de
  `tests/e2e/test_sale_payment_method_e2e.py`.
- **Unit:** formatos y `resolve_valid_until` (tabla de casos de REQ-3/REQ-6); PDF con `pdfplumber` (P completo, F,
  P antiguo sin fecha ni unidades, negocio sin banco); handlers issue/preview/update (fecha válida, vacía, F, no
  válida → 422 sin documento y con carrito intacto); negocio crear/editar con los campos nuevos y plazo no válido.
- **Snapshots:** `python3 tests/snapshots/ui_snapshot.py write tests/snapshots/baseline` en el lote de plantillas y
  `check` antes de cerrar (lección de 0.5.1: el CI cayó por no regenerarlas).
- **Comandos:** `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit -q
  --override-ini="addopts="`; `.venv/bin/python -m pytest tests/e2e -m e2e -q --override-ini="addopts="`.

## 2.9 Pre-mortem (10 dimensiones)

| Dimensión | Riesgo | Decisión |
|---|---|---|
| 1. SoC/SRP | Lógica de fechas y formatos dentro del router o del PDF | Módulo puro `sales_documents_format.py`; el router solo traduce `ValueError` → 422. Cada función tiene consumidor: PDF (formatos) y router (`resolve_valid_until`) |
| 2. Cohesión/Acoplamiento | Tres handlers repitiendo la regla de vencimiento | Una sola función, tres llamadas. El PDF no conoce la sesión ni la BD |
| 3. Capas | El PDF leyendo la BD | No: recibe todo por argumentos desde `sales_doc_pdf` |
| 4. Seguridad | Ver 2.5 | Sin cambios de permisos |
| 5. Robustez | Fecha mal formada → 500; 422 tras tocar el documento; 422 invisible; fila partida entre páginas | `resolve_valid_until` solo lanza `ValueError`; los tres handlers la llaman **antes** de cualquier escritura y con el mismo `try/except ValueError` + `_log.info` + 422 **HTML**. Filas y pie medidos antes de dibujar. El PDF sigue dentro del `try/except RuntimeError` actual; `None` en unidad/fecha/banco no lanza (REQ-8) |
| 6. Consistencia | "Hoy" del form y fecha de emisión en zonas distintas | Ambos en UTC (`datetime.now(timezone.utc)`); la comparación "no anterior a la emisión" usa `issue_date.date()` UTC. Preview e issue usan la misma función con su propio `now` UTC |
| 7. Observabilidad | Rechazos invisibles | `_log.info("valid_until rechazado: %r", raw)` |
| 8. Configuración | Plazo sin límites | Acotado 0-365; por defecto 15 en modelo, migración, config y form |
| 9. Tests | El CI cae por snapshots (0.5.1) | Regenerar baselines en el mismo lote; Lote 0 e2e con PDF real |
| 10. Escalabilidad | — | No aplica (1 PDF por petición, sin consultas nuevas) |

## 2.9b Gate de diseño

- [x] Cabeceras con `Modelo: Opus 5.5`.
- [x] Escenarios negativos en REQ-1..REQ-7.
- [x] Supuestos S1-S6 con sonda ejecutada y evidencia; el ❌ de S6 tiene decisión.
- [x] Matriz de estados sin celdas vacías.
- [x] Pre-mortem de las 10 dimensiones.
- [x] `cc_design_check` exit 0 y `cc_codex_design_review`: 12 hallazgos en `design/codex-review.md`, todos reales e
      incorporados (tabla de abajo).

### Cambios de la 2ª ronda del usuario (2026-11-18)

| Punto | Cambio en el diseño |
|---|---|
| 1. Precio unitario y subtotal | Columna `PRECIO <moneda>` (30 mm) y fila `Subtotal:` sobre la caja TOTAL; `subtotal` y `unit_price` vuelven a la firma |
| 2. Iconos de teléfono y email | SVG embebidos (sonda S9); sin `Tel.:` |
| 3. Fecha `dd/mm/aaaa` | Sin cambio (`format_doc_date` ya lo hacía) |
| 4. Moneda definida al construir el documento | Sin cambio de código: ya existe el selector "Moneda" del carrito (`components/sales_doc/cart_add.html:20`) que guarda `currency_code` en el documento; el PDF la muestra (`GEN` → `CUP`). Escenario añadido en REQ-6 |

### Hallazgos de Codex incorporados

| # | Hallazgo | Decisión |
|---|---|---|
| 1 | El vencimiento se pierde al tocar el carrito | `valid_until` en `_get_draft`/`_set_draft` y en los 4 handlers del carrito (2.2.5, 2.4.3); escenario negativo en REQ-3 |
| 2 | 422 JSON invisible en HTMX | Respuesta HTML 422 (panel o fragmento) solo para este error (2.2.2); los 422 JSON existentes quedan fuera de alcance |
| 3 | Editar un P antiguo le inventa fecha | `on_empty="keep"` en update; escenario negativo en REQ-4 |
| 4 | Datos del e2e no producen el PDF esperado | E2E con `E2E-SKU1`/`E2E-SKU2`, precios 303875/187000, moneda GEN, unidad `ud` |
| 5 | Etiquetas y direcciones que no caben | `_fit_font_size` (S8) y cabecera con `multi_cell` y alto medido |
| 6 | S5 no prueba qué renders cambian | Se regeneran las baselines y se revisa el diff; modal P y vista previa cubiertos por unit/e2e, no por snapshots (fuera de alcance) |
| 7 | `VARCHAR(n)` distinto en SQLite/PG | Recorte en servidor + `maxlength` (REQ-9 nuevo) |
| 8 | Fila partida entre páginas | Medir filas y pie con `dry_run` (S7), repetir cabecera, sin salto automático en la tabla |
| 9 | Vista previa obsoleta tras medianoche UTC | El 422 HTML guarda el draft con la fecha y el form editable permite corregirla (matriz) |
| 10 | REQ-4 sin negativo de edición | Escenario "Fecha no válida al editar no toca nada" |
| 11 | REQ-9 (`.conf`) no cubre instalación nueva | REQ-9 antiguo fuera de alcance; `_backfill_business_config` no se toca |
| 12 | Cantidad 0,0001 sale como 0.00 | `format_quantity` con 4 decimales sin ceros finales (S8) |

## 2.10 Decisión tras el escalado del Lote A (2026-11-18, Opus-1, ADR-026)

`cc_pae_loop` escaló el lote A con exit 3: las rondas 1 y 2 de Codex corrigieron la misma zona
(`app/sales_document_pdf.py::_draw_rows` y `build_sales_document_pdf`). Las dos rondas se aplicaron con la suite completa
en verde (543 unit + 97 e2e; contrato del Lote 0 2/2). No era un fallo del camino feliz: Codex buscó entradas extremas
(cuenta de 64 dígitos, 100 líneas de notas, etiqueta de 64 caracteres, descripción con saltos más alta que una página,
dirección con saltos, `.conf` con plazo vacío) y cada ronda destapó el siguiente caso.

**Contrato de robustez del PDF (cierra la zona):**
- El PDF **nunca pierde texto ni lo saca de la página**: los textos largos se ajustan en varias líneas dentro de su
  columna; los saltos de línea de los campos de cabecera, destinatario y pago se convierten en espacios.
- Una fila que cabe en una página no se parte; una fila más alta que una página se pagina. La cabecera de la tabla se
  repite en cada página nueva. Notas y pie se miden antes de dibujarse.
- Un plazo de validez vacío o no numérico en el `.conf` vale 15 y se acota a 0-365, como en el formulario.
- Se aceptan las correcciones de las rondas 1 y 2 tal cual están en la rama. Una ronda posterior de Codex que vuelva a
  esta zona sin violar el contrato se descarta (estilo, no bug).

**Verificación (Opus-1, sobre la rama tras la ronda 2):** 9 casos de estrés (normal, cuenta64, notas100, etiqueta64,
desc_alta, lineas60, unidad_larga, direccion_saltos, factura_sin_banco): 0 excepciones, 0 caracteres fuera de la página,
0 textos perdidos. Evidencia: `specs/pdf-documentos-venta-oferta/design/sondas/S11-estres-pdf.txt` y
`design/sondas/render/stress-*.png`.

**Siguiente paso:** el lote A queda aceptado; se reanuda con `cc_pae_loop … --lots B,C,D`.

## 2.11 Decisión tras el escalado del lote B (2026-11-18, Opus-1, ADR-026)

El lote B escaló tras 5 rondas. Las rondas 1-2 cerraron fallos reales: un vencimiento inválido al editar cerraba el
modal y descartaba cambios; un `valid_until` enviado como fichero se tomaba como vacío. La ronda 3 añadió la etiqueta
accesible del campo. A partir de la ronda 3, Codex osciló sobre `sales_doc_preview`: enviar vacío o enviar la fecha
calculada cuando el usuario deja el vencimiento vacío y la emisión cruza la medianoche UTC.

**Decisión (la del diseño original, §2.1d):** la vista previa envía en el campo oculto **la misma fecha que muestra**
(lo que se ve es lo que se emite). Si al emitir esa fecha ya no es válida (p. ej., plazo 0 y cambio de día UTC),
`sales_doc_issue` responde 422 HTML con el panel, el mensaje "Fecha de vencimiento inválida" y el borrador intacto
para corregirla. El estado de la rama tras la ronda 5 ya cumple esto (suite completa en verde: 543 unit + 97 e2e).
Una ronda posterior que vuelva a esta zona sin violar esta regla se descarta.

**Siguiente paso:** el lote B queda aceptado; se reanuda con `cc_pae_loop … --lots C,D`.
