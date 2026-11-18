# Auditoría — PDF de documentos de venta con el diseño "OFERTA" (0.5.2)

| | |
|---|---|
| **Fecha** | 2026-11-19 |
| **Alcance** | Diff `develop...feature/pdf-documentos-venta-oferta` (PAE `specs/pdf-documentos-venta-oferta/`), sin `specs/` ni las baselines de snapshots |
| **Score** | **4.45 / 5.00 🟢** — maduro, seguro desplegar |
| **Responsable** | Claude Opus 5.5 (`cc_claude_code`, Opus-1), revisión 5b del loop v4 |
| **Hallazgos abiertos** | 0 (8 corregidos en la 5b, 3 aceptados con motivo) |

## 1. Contexto y método

La feature reescribe el PDF de factura y presupuesto con el diseño de la plantilla del cliente: banda azul, iconos de
teléfono y email, bloque de documento, Subtotal y TOTAL, tabla con precio unitario y pie con forma de pago y firma.
Añade datos bancarios y plazo de validez al negocio, y la fecha de vencimiento al documento (`valid_until`, solo `P`).
26 envelopes `script` aplicados por `cc_pae_loop`. Tres escalados del miniloop de Codex (lote A, lote B y pasada final)
resueltos por Opus con decisiones registradas en `design.md` §2.10-2.12.

Orden: `/code-review` (7 hallazgos, todos corregidos), después esta auditoría.

**Herramientas:**
- **Serena:**
  - `activate_project`;
  - `find_symbol` con cuerpo de `SalesDocumentPDF`, `sales_doc_issue`, `sales_doc_preview` y `sales_doc_update`;
  - `find_referencing_symbols` de `build_sales_document_pdf` (un único llamador: `sales_doc_pdf`);
  - `replace_content` para la corrección de `header()`.
- **codebase-memory:**
  - `index_repository` sobre la rama;
  - `detect_changes` contra `develop`: 121 símbolos cambiados y 112 impactados, todos tests o llamadores ya revisados;
  - `search_graph` sobre `app/sales_documents_format.py`: todas las funciones tienen grado de entrada > 0, sin código
    muerto.
- **Fuera de cobertura, con `grep`/`sed`:**
  - plantillas Jinja;
  - SQL de `app/migrations.py`;
  - `.gitea/workflows/deploy.yml`;
  - código instalado de fpdf2 2.8.7.

  Los snapshots se comprueban con el arnés `tests/snapshots/ui_snapshot.py`.

**Verificación ejecutada:**
- 91 snapshots equivalentes, 635 unit y 97 e2e.
- Prueba de estrés del PDF con 9 casos extremos (S11).
- Prueba de mutación del test de regresión del solapamiento: falla sin la corrección.

## 2. Checklist por dimensión

| # | Dimensión | Estado | Evidencia |
|---|---|---|---|
| 1 | SoC / SRP | ✅ | Formato y fechas en `app/sales_documents_format.py` (funciones puras, sin fpdf ni ORM); maquetación en `app/sales_document_pdf.py` (una función por bloque: `_draw_header`, `_draw_rows`, `_draw_footer`…); el router solo traduce `ValueError` → 422 |
| 2 | Cohesión / Acoplamiento | ⚠️ | Regla del plazo y fecha por defecto unificadas en la 5b (`parse_validity_days`, `default_valid_until`). `app/routers/ui_sales_documents.py` sigue siendo el fichero más grande del router UI (~1150 líneas, previo a la feature) |
| 3 | Capas / Límites | ✅ | El PDF recibe todo por argumentos y no lee la BD (`sales_doc_pdf` → `build_sales_document_pdf`); el repositorio mapea las columnas a `IssuerConfig`/`SalesDocumentsConfig`; `tests/unit/test_routers_no_orm.py` en verde |
| 4 | Seguridad | ✅ | Sin rutas ni permisos nuevos; autoescape de Jinja sin `|safe` nuevo; longitudes acotadas en servidor (`_fit_column`) y con `maxlength`; sin `style=`/`<script>` nuevos (CSP) |
| 5 | Robustez | ✅ | La fecha se valida antes de cualquier escritura en emitir, previsualizar y editar; 422 HTML visible con HTMX; el contrato del PDF (§2.10) no pierde texto ni lo saca de la página (S11 + test de regresión) |
| 6 | Consistencia transaccional | ✅ | Un solo `commit` por handler; un 422 de vencimiento no toca el documento ni el carrito; migraciones idempotentes (`ADD COLUMN` si falta, SQLite y PG) |
| 7 | Observabilidad | ⚠️ | `_log.info("valid_until rechazado: %r")` en los tres handlers; los fallos de PDF siguen en `_log.exception`. Sin métrica de generación de PDF (no hace falta a este volumen) |
| 8 | Configuración | ⚠️ | Plazo y banco por negocio en BD, editables; el `.conf` legacy ahora se copia entero en el backfill. La equivalencia `GEN` → `CUP` está en código (decisión del usuario) |
| 9 | Pruebas | ✅ | 92 tests unitarios nuevos (formato, PDF con pdfplumber, handlers, negocio, regresión de la 5b); e2e del flujo completo (Lote 0); baselines regeneradas en el mismo cambio |
| 10 | Escalabilidad | ✅ | Sin consultas nuevas por petición; generación del PDF lineal en el número de líneas |

## 3. Scorecard ponderado

| # | Dimensión | Peso | Score | Ponderado |
|---|---|---|---|---|
| 1 | SoC / SRP | 10 % | 5 | 0.50 |
| 2 | Cohesión / Acoplamiento | 10 % | 4 | 0.40 |
| 3 | Capas / Límites | 10 % | 5 | 0.50 |
| 4 | Seguridad | 15 % | 4 | 0.60 |
| 5 | Robustez | 15 % | 4 | 0.60 |
| 6 | Consistencia transaccional | 10 % | 5 | 0.50 |
| 7 | Observabilidad | 10 % | 4 | 0.40 |
| 8 | Configuración | 5 % | 4 | 0.20 |
| 9 | Pruebas | 10 % | 5 | 0.50 |
| 10 | Escalabilidad | 5 % | 5 | 0.25 |
| | **Total** | 100 % | | **4.45 🟢** |

Seguridad y Robustez quedan en 4 por los 422 JSON previos (cliente o carrito vacíos), invisibles con HTMX, que siguen
fuera de alcance (H-009), y por las notas sin tope de longitud (H-010).

## 4. Hallazgos

### Corregidos en la 5b (código + test de regresión en `tests/unit/test_sales_doc_review_fixes.py`)

| ID | Sev. | Dim. | Hallazgo | Corrección |
|---|---|---|---|---|
| H-001 | Alto | 5 | En las páginas de continuación, las notas largas se dibujaban encima de "Documento: <código>": el salto automático de fpdf dejaba el cursor en y=5 mm (`app/sales_document_pdf.py`, `SalesDocumentPDF.header`) | `header()` termina con `set_xy(MARGIN, MARGIN)`; test con 40 líneas + 80 de notas, verificado por mutación |
| H-002 | Medio | 5 | El borrador congelaba la fecha por defecto (hoy + plazo) en la primera acción del carrito: cambiar el plazo o pasar el día daba una fecha vieja o un 422 no buscado | `_draft_valid_until`: el borrador solo guarda una fecha elegida por el usuario en un `P` |
| H-003 | Medio | 5 | El borrador guardaba vencimiento también en facturas; al pasar de F a P se heredaba | Misma función: en `F` no se guarda |
| H-004 | Medio | 8 | `load_business_config` leía banco y plazo del `.conf`, pero `_backfill_business_config` no los copiaba a la BD | El backfill copia los 5 campos |
| H-005 | Bajo | 2 | Regla del plazo (0-365, defecto 15) repetida en 3 sitios con diferencias (texto vs `None`) | `parse_validity_days` en `app/business_config.py`, usada por `.conf`, repositorio y *Negocios* |
| H-006 | Bajo | 2 | Fecha por defecto calculada en dos sitios (global de Jinja y `resolve_valid_until`), el primero sin acotar | `default_valid_until` en `app/sales_documents_format.py`; el global de Jinja delega |
| H-007 | Bajo | 2 | Texto del error fijo en `_valid_until_error.html` y bloque 422 duplicado en vista previa y edición | `_valid_until_error_fragment` con `INVALID_VALID_UNTIL` en el contexto |
| H-008 | Bajo | 2 | Límites de longitud del banco (128/64/64/8) copiados a mano en crear y editar negocio | `_fit_column` lee el tamaño de la columna de `Business` |

### Aceptados (con motivo; no generan backlog)

| ID | Sev. | Dim. | Hallazgo | Motivo |
|---|---|---|---|---|
| H-009 | Bajo | 5 | Los 422 previos "cliente obligatorio" y "carrito vacío" salen en JSON y HTMX no los muestra | Previos a la feature y fuera de alcance por diseño (`requirements.md` §1.4); el botón de emitir solo se usa con cliente y carrito |
| H-010 | Bajo | 4 | El campo de notas no tiene tope de longitud (`Text`) | El PDF lo pagina sin perder texto (§2.10, H-001); solo lo usan usuarios autenticados del negocio |
| H-011 | Bajo | 8 | `GEN` → `CUP` está en código (`currency_column_label`) | Decisión explícita del usuario (2026-11-18); una sola función y testeada |

## 5. Matriz impacto vs esfuerzo

| Cuadrante | Hallazgos |
|---|---|
| 🔥 Quick wins (hechos) | H-001, H-002, H-003, H-004 |
| 📋 Baja prioridad (hechos) | H-005, H-006, H-007, H-008 |
| ❌ Evitar / aceptados | H-009, H-010, H-011 |

## 6. Backlog priorizado

Vacío. Todos los hallazgos están corregidos o aceptados con motivo.

## 7. Plan 30/60/90

- **Ahora:** merge a `develop`, despliegue a staging y prueba de punta a punta. El usuario rellena teléfono, email,
  banco y plazo de DyB Autopartes en *Negocios → Editar*, emite un presupuesto y una factura y revisa los PDF. Después,
  `main` y producción.
- **30 días:** si el usuario lo pide, revisar con él la plantilla en papel (márgenes de impresión reales).
- **60/90 días:** sin acciones derivadas de esta feature.
