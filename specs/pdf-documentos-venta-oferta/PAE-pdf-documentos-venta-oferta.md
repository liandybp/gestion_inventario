# PAE: PDF de documentos de venta con el diseño de la plantilla "OFERTA"

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:  cc_pae_loop <repo> specs/pdf-documentos-venta-oferta/tasks.md   (lote a lote + miniloop de Codex)
     Todos los envelopes son `Apply: script`: los aplica el intérprete `pae_edits.py` (sin modelo) y `cc_pae_check`
     ya los ensayó de punta a punta (ver "Ensayo" abajo). Un commit por lote; nunca push, sudo, systemctl ni
     scripts de despliegue.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestión de inventario |
| **Rama base** | `develop` (`ae8fda8`, 0.5.1) |
| **Rama de trabajo** | `feature/pdf-documentos-venta-oferta` (la crea la sesión del paso 4a, tras la aprobación) |
| **Fecha** | 2026-11-18 |
| **Versión actual** | `0.5.1` (`VERSION`, `.bumpversion.cfg`) |
| **Versión objetivo** | `0.5.2` (PATCH; el lote D la sube) |
| **PAE anterior** | ninguno (0.5.1 se hizo directo en `develop`) |
| **Fase** | planificación (Opus-1 Fases 1-2, Sonnet-2 Fases 3-4) → ejecución (`cc_pae_loop`, script) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/pdf-documentos-venta-oferta/requirements.md` | REQ-1..REQ-10 con Gherkin (positivos y negativos), MoSCoW, NFRs, fuera de alcance |
| Phase 2 — Design | `specs/pdf-documentos-venta-oferta/design.md` | Código leído, supuestos S1-S9 con sondas, matriz de estados, contratos, pre-mortem, 12 hallazgos de Codex incorporados |
| Sondas de diseño | `specs/pdf-documentos-venta-oferta/design/sondas/` | Evidencia S1-S9 (Fase 2) y S10-S11 (Fase 3: autorrelleno de Documentos y PDFs de ejemplo en `render/`) |
| Phase 3 — Task Envelopes | `specs/pdf-documentos-venta-oferta/tasks.md` | 26 envelopes `script` en 4 lotes (A-D) |
| Codemod | `specs/pdf-documentos-venta-oferta/codemods/regen_baselines.py` | Regenera las baselines de snapshots (envelopes T14 y T19) |

### Skills asignadas

- [x] `backend-developer` — modelo, migraciones, config, repositorio, routers, PDF
- [x] `frontend-developer` — plantillas de Negocios y de Documentos
- [x] `test-automation-engineer` — e2e del Lote 0, unit, baselines
- [x] `technical-writer` / `release-manager` — CHANGELOG, CLAUDE.md, versión

---

## 1. Objetivo

### 1.1 Qué se construye

El PDF de factura (`F`) y de presupuesto (`P`) deja el diseño simple actual y reproduce la plantilla de referencia
"OFERTA" (`~/Descargas/COMERCIO GEELY CK B244742.pdf`): banda de cabecera azul clara `RGB(220,230,241)` con el emisor
(nombre, `TCP`, `NIT`) y el contacto con **iconos vectoriales** de teléfono y email; barra `DESTINATARIO:` con la
etiqueta del documento sobre doble línea; bloque cliente y bloque documento (`No.`, fecha `dd/mm/aaaa`, y solo en
presupuestos `Fecha de vencimiento:`); fila `Subtotal:` y caja `TOTAL:` en azul `RGB(17,90,156)`; tabla
`CONCEPTO | UNIDADES | CANTIDAD | PRECIO <moneda> | IMPORTE <moneda>` con separador de miles; y pie con `FORMA DE PAGO`
(datos bancarios) y `Recibido por / Cargo / Firma / Fecha`.

Para alimentarlo, el negocio gana cuatro datos bancarios y un plazo de validez (días), y el documento gana
`valid_until` (fecha de vencimiento, solo `P`). El formulario de emisión rellena la fecha con hoy + plazo y se puede
cambiar; la vista previa y el modal de edición la muestran y la editan.

### 1.2 Por qué

El cliente recibe hoy un PDF que no se parece a su plantilla de oferta y sin teléfono, email, datos de pago, vencimiento
ni firma de recibido. El usuario decidió (2026-11-18, dos rondas) el alcance: mismo diseño para factura y oferta,
precio unitario y subtotal dentro, iconos, fecha `dd/mm/aaaa` y moneda del documento.

### 1.3 Valor esperado

- Un presupuesto emitido en moneda `GEN` produce un PDF con `PRECIO CUP`/`IMPORTE CUP`, `490,875.00`, banco y vencimiento.
- Los documentos ya emitidos siguen generando PDF con el diseño nuevo (sin vencimiento ni unidades).
- Una fecha de vencimiento inválida nunca crea ni modifica un documento y el error se ve en pantalla.

### 1.4 Qué NO se toca

- Marca/Matrícula, plazo de ejecución, lema y número `LOT.` de la referencia; icono de dirección; opciones nuevas del
  selector de moneda del carrito (siguen USD, EUR, GEN).
- Copia de teléfono/email/banco dentro del documento emitido: el PDF los lee del negocio al generarse (como el TCP).
- Los `.conf` legacy (`_backfill_business_config` no se toca); en producción el usuario rellena los datos de DyB en
  *Negocios → Editar* tras el despliegue.
- Los 422 JSON existentes (cliente o carrito vacíos): solo el error nuevo de vencimiento sale como HTML.
- Permisos: nadie gana ni pierde acceso. `app/auth.py`, `app/db.py`, `app/security.py`, `app/deps.py`, `docker-compose*.yml`,
  `.env` y `requirements.txt` quedan intactos (sin dependencias nuevas).

---

## 2. Decisiones de diseño

> Ningún envelope toma decisiones: todo está cerrado aquí y en `design.md`.

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|--------------|---------------|---------------|
| D1 | Datos bancarios y plazo en columnas de `businesses` | Copia en cada documento | Dato fijo del negocio; misma decisión que el TCP en 0.5.1 | Un PDF antiguo regenerado muestra el banco actual |
| D2 | `valid_until` columna `DATE` en `sales_documents` | Calcular emisión + plazo al vuelo | El usuario quiere editarla por documento | Migración idempotente en SQLite y PostgreSQL; `NULL` en `F` y en `P` antiguos |
| D3 | Regla de vencimiento en una función pura `resolve_valid_until` (`on_empty="default"` al emitir, `"keep"` al editar) | Lógica repetida en los tres handlers | Una sola regla, probada sin PDF ni BD | Editar un `P` antiguo sin fecha no le inventa una (hallazgo 3 de Codex) |
| D4 | Error de vencimiento como 422 **HTML** (panel o fragmento) | `HTTPException` JSON | htmx solo hace swap de 4xx `text/html` (hallazgo 2) | Solo este error; los 422 JSON existentes no cambian |
| D5 | `valid_until` viaja en el borrador de sesión y en los cuatro handlers del carrito | Solo en `issue` | Sin esto la fecha escrita se pierde al tocar el carrito (hallazgo 1) | `_get_draft`/`_set_draft` ganan el campo |
| D6 | Valor por defecto del formulario con el global de Jinja `default_valid_until(days)` | Pasarlo en los 11 renders del panel | Un solo sitio; `sales_doc_config` ya llega a todos | Fecha UTC, como el resto del sistema |
| D7 | Formatos y fechas en `app/sales_documents_format.py` | Dentro del PDF | Tests sin PDF; el router usa `resolve_valid_until` sin importar fpdf | Módulo puro, sin fpdf/FastAPI/SQLAlchemy |
| D8 | Iconos SVG embebidos como constantes `bytes` | PNG en `static/` o texto `Tel.:` | Decisión del usuario; vectorial, sin ficheros ni dependencias | Sonda S9 y e2e (`curves`) |
| D9 | Filas y pie medidos con `dry_run` antes de dibujar; sin salto automático de fpdf | Salto automático | Una fila nunca se parte (hallazgo 8) | Cabecera de tabla repetida; PDF de 60 líneas revisado a ojo (sonda S11) |
| D10 | Longitudes recortadas en servidor + `maxlength` (128/64/64/8) | Solo VARCHAR | SQLite no impone `VARCHAR(n)` y PostgreSQL sí (hallazgo 7) | REQ-9 |
| D11 | **Todos los envelopes son `script`** | `cc_local` para los routers y el PDF | El diseño ya resolvió cada decisión; un envelope `script` se ensaya de verdad con `cc_pae_check` (e2e incluido). La auditoría de 5b encontró lotes dañados de `cc_local` en cambios de routers | Sin ambigüedad ni improvisación; el coste lo asume la Fase 3 (código literal en `tasks.md`) |
| D12 | Baselines regeneradas con un codemod en el mismo lote que las plantillas | Regenerar a mano | Lección de 0.5.1: el CI cayó por no regenerarlas | T14 (tras las plantillas del lote A) y T19 (tras las del lote B) |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
 *Negocios → Editar*  ──✦ui_businesses.py──►  businesses (★5 columnas)  ──✦business_repository──►  BusinessConfig
                                                                                                        │
 Documentos: formulario ──✦ui_sales_documents.py──► ★resolve_valid_until (sales_documents_format.py)    │
   (✦issue_form.html: valid_until)        │                                                              │
   preview/issue/update/cart              └──►  sales_documents (★valid_until)                           │
                                                      │                                                  │
 GET /ui/sales-doc/{id}/pdf ──✦sales_doc_pdf───────────┴────► ✦build_sales_document_pdf(…) ◄─────────────┘
                                                              (★iconos SVG, ★formatos, ★pie, ★tabla)
 ★ nuevo   ✦ modificado
```

### 3.2 Inventario de archivos

> **Scope duro:** solo estos archivos. `git diff --name-only develop` debe mostrar exactamente estos (más lo que el
> propio `cc_pae_loop` escribe en `specs/pdf-documentos-venta-oferta/`: `loop/`, `verify/`).

| Archivo | Operación | Lote / envelope | Responsabilidad |
|---------|-----------|-----------------|-----------------|
| `tests/e2e/test_sales_doc_pdf_oferta_e2e.py` | CREATE | A / T1 (contrato) | e2e: negocio → presupuesto en `GEN` → PDF; factura sin vencimiento |
| `app/models.py` | MODIFY | A / T2 | `Business` +5 columnas; `SalesDocument.valid_until` |
| `app/migrations.py` | MODIFY | A / T3 | 5 tuplas en `_BUSINESS_CONFIG_COLS`; `valid_until` en SQLite y PostgreSQL |
| `app/business_config.py` | MODIFY | A / T4 | `IssuerConfig` +4 campos; `SalesDocumentsConfig.quote_validity_days` |
| `app/repositories/business_repository.py` | MODIFY | A / T5 | `get_business_config` mapea las columnas nuevas |
| `app/routers/ui_businesses.py` | MODIFY | A / T6 | 5 `Form`, recorte, `_parse_validity_days` |
| `app/templates/features/businesses/tab_businesses.html` | MODIFY | A / T7 | Campos nuevos en crear |
| `app/templates/features/businesses/business_edit_form.html` | MODIFY | A / T7 | Campos nuevos en editar |
| `app/sales_documents_format.py` | CREATE | A / T8 | Funciones puras de formato y vencimiento |
| `app/sales_document_pdf.py` | MODIFY (reescritura) | A / T9 | Diseño OFERTA |
| `app/routers/ui_common.py` | MODIFY | A / T10 | Global `default_valid_until` |
| `app/routers/ui_sales_documents.py` | MODIFY | A / T11, T12; B / T15, T17 | Borrador, carrito, issue, pdf, preview, update |
| `app/templates/components/sales_doc/issue_form.html` | MODIFY | A / T13 | Campo `valid_until` |
| `tests/snapshots/baseline/*.txt` | REGENERATE | A / T14; B / T19 | Baselines afectadas (formularios de Negocios y Documentos) |
| `app/templates/features/sales_documents/_valid_until_error.html` | CREATE | B / T16 | Fragmento de error de la vista previa |
| `app/templates/features/sales_documents/sales_document_preview.html` | MODIFY | B / T16 | Línea "Vencimiento" + hidden |
| `app/templates/features/sales_documents/sales_document_edit_form.html` | MODIFY | B / T18 | Campo `valid_until` (solo `P`) |
| `tests/unit/test_sales_documents_format.py` | CREATE | C / T20 | Formatos y `resolve_valid_until` |
| `tests/unit/test_sales_document_pdf.py` | CREATE | C / T21 | PDF leído con pdfplumber |
| `tests/unit/test_sales_doc_valid_until.py` | CREATE | C / T22 | Handlers issue/preview/update/carrito |
| `tests/unit/test_business_bank_fields.py` | CREATE | C / T23 | Negocio: crear/editar los campos nuevos |
| `VERSION`, `.bumpversion.cfg` | MODIFY | D / T24 | 0.5.1 → 0.5.2 |
| `CHANGELOG.md` | MODIFY | D / T25 | Entrada 0.5.2 |
| `CLAUDE.md` | MODIFY | D / T26 | Estado actual |
| `.claude/CLAUDE.md` | MODIFY | **manual M1** (Claude Code, en la revisión) | `cc_local` no edita `.claude/`; ver M1 |

### 3.3 Dependencias existentes (firmas reales del repo)

```
# app/routers/ui_context.py — YA EXISTE
def ui_context(request: Request, db: Session) -> UIContext          # ctx.business_id, ctx.config (BusinessConfig)
# app/routers/ui_sales_documents.py — YA EXISTE (se extiende, no se reescribe)
def _get_cart(request) -> list[dict];  def _get_draft(request) -> dict;  def _set_draft(request, *, doc_type=None, ...) -> None
def _recent_documents(db, limit=10, business_id=0);  def _customers_list(db, *, business_id, limit=200)
def _resolve_currency(config, currency_code) -> tuple[str, str]
# app/routers/ui_businesses.py — YA EXISTE
def _str_form(value, fallback: str = "") -> str                      # acepta FormInfo (llamadas directas de tests)
# app/repositories/business_repository.py — YA EXISTE
def get_business_config(db, business_id) -> BusinessConfig
# app/models.py — YA EXISTE
class SalesDocument(Base): issue_date: Mapped[datetime]; currency_code; notes; subtotal; total ...
class SalesDocumentItem(Base): description; unit_of_measure: Optional[str]; quantity; unit_price; line_total
```

### 3.4 Interfaces nuevas a implementar

```
# app/sales_documents_format.py
INVALID_VALID_UNTIL = "Fecha de vencimiento inválida"
def currency_column_label(prefix: str, currency_code: str | None) -> str
def format_amount(value: float | None) -> str            # 303875 -> "303,875.00"
def format_quantity(value: float | None) -> str          # 0.0001 -> "0.0001"; 1234567.0 -> "1,234,567"
def format_doc_date(value: date | datetime | None) -> str    # "03/12/2026"
def resolve_valid_until(raw, *, doc_type, issue_date, validity_days, on_empty="default", current=None) -> date | None  # ValueError

# app/sales_document_pdf.py — firma keyword-only (única llamada: sales_doc_pdf)
def build_sales_document_pdf(*, doc_type, doc_label, code, issue_date, valid_until, currency_code, issuer_name,
    issuer_holder, issuer_tax_id, issuer_address, issuer_phone, issuer_email, bank_name, bank_branch, bank_account,
    bank_account_currency, client_name, client_id, client_address, items, subtotal, total, notes) -> bytes

# app/routers/ui_common.py
def default_valid_until(days) -> str                      # global de Jinja: hoy (UTC) + days, "aaaa-mm-dd"
```

---

## 4. Fases de implementación (Task Envelopes)

> Los 26 envelopes viven en `specs/pdf-documentos-venta-oferta/tasks.md` (no se copian aquí). Todos son
> `Apply: script`: bloques `pae-edit` literales aplicados por el intérprete, sin modelo. El primer envelope del
> Lote A (T1) es el contrato del Lote 0 y ningún otro lo toca (lint `E10`).

Lotes: **A** T1–T14 · **B** T15–T19 · **C** T20–T23 · **D** T24–T26

| Lote | Envelopes | Contenido | REQ |
|------|-----------|-----------|-----|
| **A — Lote 0** | T1–T14 (14, todos script) | e2e (contrato) → modelo → migraciones → config → repositorio → *Negocios* (handlers + formularios) → formato → PDF → global Jinja → router (borrador/carrito, issue, pdf) → formulario de emisión → baselines. Al cerrar, el flujo completo funciona y el e2e pasa | REQ-1, 2, 3, 5, 6, 7, 8, 9, 10 |
| **B** | T15–T19 (5, script) | Vista previa (validación, línea "Vencimiento", hidden), edición (`keep`), formulario de edición, baselines | REQ-4 |
| **C** | T20–T23 (4, script) | Unit: formatos, PDF (pdfplumber), handlers, negocio | REQ-1..REQ-9 |
| **D** | T24–T26 (3, script) | `VERSION`/`.bumpversion.cfg` 0.5.2, `CHANGELOG.md`, `CLAUDE.md` | doc |

Mapeo REQ → envelope: REQ-1/REQ-9 → T2, T3, T4, T5, T6, T7, T23; REQ-2 → T2–T7, T23; REQ-3 → T2, T3, T8, T10–T13,
T20, T22; REQ-4 → T15–T19, T22; REQ-5/REQ-7/REQ-8 → T9, T12, T21; REQ-6 → T8, T9, T20, T21; REQ-10 → T1.

### Envelope manual M1 (no está en `tasks.md`): `.claude/CLAUDE.md` — lo aplica Claude Code en la revisión

`cc_local` no tiene permiso de edición sobre `.claude/`, así que este cambio no entra en ningún lote. Lo aplica
Claude Code al revisar (rama `feature/pdf-documentos-venta-oferta`), con este contenido:

```
En `.claude/CLAUDE.md`, sección "Estado actual del proyecto" (bloque de código), insertar ANTES de la línea que
empieza por `Fase 5b:`:

PDF OFERTA:   0.5.2 (rama feature/pdf-documentos-venta-oferta, sin desplegar). `app/sales_document_pdf.py` reescrito
              con el diseño de la plantilla OFERTA (banda azul, iconos SVG de teléfono/email, tabla con precio
              unitario, pie FORMA DE PAGO). Funciones puras en `app/sales_documents_format.py`. Negocio:
              `issuer_bank_*` y `sales_quote_validity_days`; documento: `sales_documents.valid_until` (solo P),
              422 HTML "Fecha de vencimiento inválida". Los forms de Negocios y Documentos cambian: regenerar
              baselines con `ui_snapshot.py write` en el mismo commit. El PDF lee teléfono, email y banco del negocio
              al generarse (no del documento).

Y sustituir la línea `Versión:      0.4.14 (ver VERSION) — 0.4.11 a 0.4.14 desplegadas en dev y producción (main,`
por `Versión:      0.5.2 (ver VERSION; 0.5.1 es la última en producción) — histórico: 0.4.11 a 0.4.14 desplegadas en dev y producción (main,`.
```

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación

Los comandos son los del `## Ejecución` (rutas reales del proyecto). Antes de lanzar, en `develop` @ `ae8fda8` deben
pasar: unit, snapshots y e2e (parte limpia, sin `tasks.md` aplicado).

### 5.1 Verificación por envelope

Cada envelope `script` declara en prosa su verificación (importar el módulo, cargar la plantilla o recoger los tests).
La verificación real es la de cada lote: `--test-cmd` (snapshots + unit + e2e) y, en el Lote A, `--lot0-test-cmd`.

### 5.2 e2e (comportamiento real) — Playwright, decidido por REQ

Todos los REQ de UI/flujo se prueban con Playwright (no `behave`): dependen de HTMX, del modal de edición, de la
descarga del PDF y del JS del carrito. `tests/e2e/test_sales_doc_pdf_oferta_e2e.py` (T1, contrato del Lote 0) hace login,
edita el negocio activo (teléfono, email, TCP, NIT, banco, plazo), reabre el modal y comprueba que los datos persisten,
elige moneda `GEN` y tipo `P`, escribe vencimiento `2099-12-31`, añade `E2E-SKU1` (1 × 303875) y `E2E-SKU2` (1 × 187000),
comprueba que el vencimiento sobrevive a los cambios del carrito, emite, descarga el PDF por `/ui/sales-doc/{id}/pdf` y
lo lee con `pdfplumber`. Aserciones positivas: `DESTINATARIO:`, `No. Presupuesto:`, `31/12/2099`, `Subtotal:`, `TOTAL:`,
`490,875.00`, `PRECIO CUP`, `IMPORTE CUP`, filas con `ud`, `TCP`, `NIT`, teléfono, email, banco, cuenta, `Recibido por:`,
y gráficos vectoriales en la cabecera. Aserciones negativas: sin `GEN`, `Tel.:`, `ID fiscal` ni `(Precios sin IVA)`.
El segundo test emite una `F`: `FACTURA`, `No. Factura:`, y sin `Fecha de vencimiento`.

### 5.3 Tests existentes que deben seguir pasando

- [ ] Suite unitaria completa (`tests/unit`), con `test_routers_no_orm.py`, `test_ui_query_count.py`,
      `test_templates_layout.py`, `test_frontend_css.py` y `test_frontend_js_modules.py` (las plantillas nuevas no llevan
      `style=`, `<script>`, `on*=` ni colores hex).
- [ ] Arnés de snapshots (`ui_snapshot.py check`): 91 baselines; cambian solo las de los formularios de Negocios y Documentos.
- [ ] e2e existentes (`tests/e2e`), en especial `test_document_issue_flow` y el recorrido de pestañas.

### 5.4 Cobertura de lo nuevo

| Pieza | Test |
|-------|------|
| `sales_documents_format.py` | `tests/unit/test_sales_documents_format.py` (tablas de REQ-3 y REQ-6) |
| `build_sales_document_pdf` | `tests/unit/test_sales_document_pdf.py` (P completo, F, P antiguo, sin banco, sin iconos, monedas, cantidades, etiquetas largas, 60 líneas) |
| handlers de documentos | `tests/unit/test_sales_doc_valid_until.py` (válida, vacía, F, inválida → 422 sin documento y con carrito intacto, borrador, vista previa, edición) |
| negocio | `tests/unit/test_business_bank_fields.py` (crear/editar, plazo acotado, recorte, formulario) |
| flujo completo | e2e del Lote 0 |

### 5.5 Ensayo previo (`cc_pae_check`, Fase 3-4, 2026-11-18)

`cc_pae_check` aplicó los 26 envelopes en un worktree y ejecutó los tests de cada lote. Resultado y evidencia:
ver "Resultado del ensayo" al final de este PAE. Las sondas de la Fase 3 destaparon dos cosas ya resueltas en `tasks.md`:
(S10) el autorrelleno de precio de Documentos no se dispara siempre con `Tab` y pisaba el precio del e2e;
(S11) los PDFs de ejemplo se revisaron a ojo (1 página, mínimo con etiqueta larga, 3 páginas con 60 líneas).

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Baselines de snapshots desactualizadas (CI cae, como en 0.5.1) | Media | Medio | `ui_snapshot.py check` con `DIFF` | T14 y T19 regeneran con codemod; `--test-cmd` de cada lote corre `check` | Regenerar y commitear |
| R2 | Migración en PostgreSQL de producción | Baja | Alto | Arranque con `RuntimeError` de columna | `ADD COLUMN IF NOT EXISTS`, idempotente; volcado previo (`~/backups/`) como en 0.5.0 | Volver a 0.5.1: el código anterior ignora las columnas |
| R3 | e2e intermitente por el autorrelleno de Documentos | Media | Medio | Precio `2.00` en vez de `303875` en el PDF | El helper `_add_item` espera el autorrelleno y verifica el precio de cada fila del carrito antes de seguir; 8/8 ejecuciones verdes en el ensayo | Reintentar; si falla otra vez, parar |
| R4 | Fecha de vencimiento perdida o inválida en silencio | Baja | Medio | Documento con fecha por defecto en vez de la escrita; 500 | `valid_until` en el borrador y en los cuatro handlers del carrito; 422 HTML con mensaje; unit de cada camino | — |
| R5 | Texto largo (etiqueta, dirección, descripción) se sale de su caja o parte una fila | Media | Bajo | Texto cortado o fila a caballo entre páginas | `_fit_text`, medida con `dry_run`, cabecera repetida; unit con etiqueta/dirección largas y 60 líneas | — |
| R6 | El PDF antiguo regenerado muestra el banco actual | Baja | Bajo | Cambia el banco de un PDF viejo | Decisión D1 documentada; igual que el TCP | — |
| R7 | Producción sin datos bancarios tras el despliegue | Alta | Bajo | PDF sin teléfono, email ni banco | Es esperado: el usuario los rellena en *Negocios → Editar* (el PDF no inventa líneas vacías; REQ-5) | — |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit

- [ ] Unit: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit -q --override-ini="addopts="` — verde.
- [ ] Snapshots: `.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` — verde.
- [ ] e2e completo (comando del `## Ejecución`) — verde, incluido `test_sales_doc_pdf_oferta_e2e.py`.
- [ ] Gherkin de `requirements.md`: REQ-1..REQ-10 cubiertos por los tests de 5.4 (positivos y negativos).
- [ ] Invariantes: sin cambios en `app/auth.py`, `app/db.py`, `app/security.py`, `app/deps.py`, docker-compose ni `requirements.txt`; routers UI sin ORM.
- [ ] Scope: `git diff --name-only develop` coincide con 3.2 (más `specs/pdf-documentos-venta-oferta/loop|verify`).
- [ ] Versión 0.5.2 (PATCH, no MINOR) en `VERSION` y `.bumpversion.cfg`; `CHANGELOG.md` y `CLAUDE.md` actualizados; M1 (`.claude/CLAUDE.md`) aplicado por Claude Code.
- [ ] Revisión de Claude Code (`/code-review` con Serena + codebase-memory) y probar en staging (`dev.gestion.legatumbp.com`) un presupuesto real en `GEN`.

### 7.2 Commits esperados (uno por lote)

1. `feat(sales-docs): oferta-style PDF, bank data and quote due date (happy path + e2e)` — lote A
2. `feat(sales-docs): due date in preview and edit` — lote B
3. `test(sales-docs): unit tests for oferta PDF, due date and business bank data` — lote C
4. `chore(release): 0.5.2 — oferta PDF (changelog, docs)` — lote D

### 7.3 Pilares

P1 (legibilidad), P4 (tests), P5 (errores) y P6 (consistencia) en todos los envelopes. P8 (tests de comportamiento) en
los de test. P9 (snapshots) en T14/T19.

---

## Ejecución

> Comando que ejecuta el paso 4 del loop (lo lanza una sesión **nueva** de `cc_claude_2` tras la aprobación, en
> segundo plano con `setsid nohup`). Tomado del `CLAUDE.md` del proyecto, de `.gitea/workflows/deploy.yml` y de
> `tests/e2e/conftest.py`. La suite unitaria completa corre ~15 s; el e2e completo, varios minutos.

```bash
cc_pae_loop /home/liandy/PyCharmMiscProject/gestion_inventario specs/pdf-documentos-venta-oferta/tasks.md \
  --test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test sh -c '.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && .venv/bin/python -m pytest tests/unit -q --override-ini=\"addopts=\" && .venv/bin/python -m pytest tests/e2e -m e2e -q --override-ini=\"addopts=\"'" \
  --lot0-test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest tests/e2e/test_sales_doc_pdf_oferta_e2e.py -m e2e -q --override-ini=\"addopts=\""
```

Notas para la sesión del paso 4a:
- Rama: `feature/pdf-documentos-venta-oferta` desde `develop` @ `ae8fda8` (árbol limpio; `specs/pdf-documentos-venta-oferta/` debe estar commiteado antes de lanzar).
- **No hace falta `--free-port`:** el e2e usa un puerto libre dinámico (`tests/e2e/conftest.py::_free_port`, `_boot_temp_server`).
- Variables del e2e: `conftest.py` fija `DATABASE_URL`, `SESSION_SECRET` y `ADMIN_*` del servidor que arranca; las del comando son las del CI.
- **Ensayo previo (`cc_pae_check`)**: el worktree del ensayo no tiene `.venv` (nunca se enlaza), así que los comandos con `.venv/bin/python` fallan con exit 127 y el contrato "rojo" no se reconoce. Para repetir el ensayo, pasar `--test-cmd`/`--lot0-test-cmd` con la ruta absoluta del intérprete (`/home/liandy/PyCharmMiscProject/gestion_inventario/.venv/bin/python`). `cc_pae_loop` corre en el repo, donde `.venv` sí existe.
- El Lote 0 (A) es el gate duro: si `test_sales_doc_pdf_oferta_e2e.py` no cierra en verde tras T14, parar y devolver a `cc_claude_code`.
- Todos los envelopes son `script`: `cc_local` no escribe código; el miniloop de Codex revisa el diff de cada lote.
- Al terminar, traspasar a `cc_claude_code` para la revisión y para aplicar el envelope manual M1.

## Resultado del ensayo (`cc_pae_check`, 2026-11-18)

- Lint: `I1: 26 envelopes script, 0 cc_local, 60 ediciones`, sin errores (`E1`-`E12`, incluido `E7`: cada `before` coincide con el fichero real de `develop` @ `ae8fda8`).
- Ensayo en worktree, con `--test-cmd` = snapshots + unit completa + e2e completo y `--lot0-test-cmd` = e2e del contrato:
  **`ensayo: lotes A, B, C, D en verde; cobertura completa`**. El contrato (solo T1) dio rojo (exit 1) antes de aplicar T2-T14, y el Lote A lo cerró en verde.
- Estabilidad del e2e del Lote 0: 8 ejecuciones seguidas en verde tras corregir el helper `_add_item` (sonda S10).
- Evidencia: `specs/pdf-documentos-venta-oferta/verify/ensayo/` (`resumen.txt`, `lote{A..D}.test.txt`, `loteA-contrato-rojo.txt`) y PDFs de ejemplo en `design/sondas/render/`.

---

> **PAE complete at `specs/pdf-documentos-venta-oferta/PAE-pdf-documentos-venta-oferta.md`.**
> **26 task envelopes ready (26 `script`, 0 `cc_local`).**
> **Awaiting approval before any implementation begins.**
