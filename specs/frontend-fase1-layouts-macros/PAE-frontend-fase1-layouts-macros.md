# PAE-016: frontend-fase1-layouts-macros

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:
       cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase1-layouts-macros/tasks.md \
         --test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     Sin --proto (loop por defecto, ADR-020: no hay prototipo).
     Si un humano o cc_local lo ejecuta a mano:
     1. Sesión nueva por lote. Leer SOLO los envelopes del lote (Read offset/limit sobre tasks.md).
     2. El envelope es la única fuente: código exacto, sin consultar notas ni docs genéricas.
     3. Verificación de cada envelope; si falla 3 veces, parar y reportar.
     4. Si el contexto se compacta a mitad de lote: parar.
     5. Un commit por lote; nunca push, sudo, systemctl ni scripts de despliegue.
     6. `git add` solo de los ficheros del lote (hay cambios ajenos sin commitear en el repo).
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | `feature/frontend-modular` (`276b411`) |
| **Rama de trabajo** | `feature/frontend-modular` (rama del release; sin push) |
| **Fecha** | 2026-10-23 |
| **Versión actual** | 0.4.15 |
| **Versión objetivo** | 0.5.0 al cerrar la fase 5 — **ningún commit de este PAE sube versión** |
| **PAE anterior** | PAE-015 (`copiloto-ventas-ubicacion-mensual`, pendiente, independiente) |
| **Fase** | PAE: Claude Code (Opus, SDD-Agent) → ejecución: `cc_local` (Qwen3.6 local) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Insumo | `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` (§2, §4) | Alcance de la fase 1 |
| Insumo | `docs/prototipos/fase1-layouts-macros/traspaso.md` ("Estado", "Siguiente etapa") | Riesgos ya materializados en el prototipo descartado |
| Phase 1 — Requirements | `specs/frontend-fase1-layouts-macros/requirements.md` | REQ-1..9 con Gherkin, MoSCoW, NFRs, fuera de alcance |
| Phase 2 — Design | `specs/frontend-fase1-layouts-macros/design.md` | D1..D9, seguridad, pre-mortem R1..R5 + 10 dimensiones |
| Phase 3 — Task Envelopes | `specs/frontend-fase1-layouts-macros/tasks.md` | 21 envelopes en 7 lotes, generados por script y simulados |

### Skills y herramientas asignadas a cc_local

Cada envelope declara `Tools (cc_local)` y `Skills`. Resumen:
- **Tools:** `Read` + `Write` (ficheros nuevos o reescritos enteros) o `Read` + `Edit` (sustituciones literales), y
  `Bash` para las verificaciones, `git rm` (T18) y `git add`/`commit` de cada lote.
- **Skills:** ninguna en los 21 envelopes: el código va literal y una skill solo añadiría contexto al modelo local.
- **MCP:** ninguno (regla dura para modelos locales).

---

## 1. Objetivo

### 1.1 Qué se construye

La base de componentes del frontend sin cambiar el HTML que ve el usuario: `base.html` (sin assets),
`layouts/app_shell.html` (assets de la app) y `layouts/print.html` (sin assets); las 5 plantillas con
`<head>` propio pasan a `{% extends %}`; macros `modal()` (cuerpo con `caller()`) y `field()`; los 18 modales
usan la macro; los 4 `profit_items_*_edit_form.html` se unifican en uno y se borran. Antes de tocar nada, un
arnés lado servidor captura el HTML de 40 páginas como baseline y cada envelope verifica equivalencia contra ella.

### 1.2 Por qué

Diagnóstico del 2026-10-23: 0 `extends`, esqueleto de modal copiado en 18 ficheros, 4 formularios casi
idénticos. Las fases 2-5 del release (JS a módulos, tokens CSS, OOB en plantillas, reorganización por dominio)
necesitan este punto de extensión y una red de seguridad para el refactor. Un primer intento (prototipo de
cc_local) se descartó: rompía el modal migrado, filtraba scripts a las impresiones, cubría ~1/6 del alcance y
su arnés comparaba 404 con 404.

### 1.3 Valor esperado

- 18/18 modales y 5/5 páginas sobre componentes; 0 esqueletos de modal escritos a mano en `partials/`.
- 4 ficheros menos (unificado de Utilidades) y 65 filas de formulario vía `field()`.
- Arnés determinista reutilizable en las fases 2-5 (40 páginas en ~3 s).
- HTML renderizado idéntico (40/40) y suites en verde: 409 unit, 47 e2e.

### 1.4 Qué NO se toca

- Rutas, handlers y servicios (salvo nombre de plantilla + `"kind"` en 4 handlers de `ui_profit_items.py`).
- `app/static/` (CSS/JS), `<style>`/`<script>` inline, `onclick=`, CSP: fases 2-3.
- Filas de formulario fuera de los modales y filas no simples (selects, grupos de checkboxes, imagen).
- `labels_print.html` (vacío, sin ruta). `VERSION`, `.bumpversion.cfg`.
- Cambios ajenos sin commitear: `CLAUDE.md`, `.claude/CLAUDE.md`, `.idea/`, `.serena/`, `specs/copiloto-ventas-ubicacion-mensual/`.

---

## 2. Decisiones de diseño

> cc_local no toma ninguna decisión de esta sección. Detalle en `design.md` §2.6.

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Cuerpo del modal con `caller()` | dict / string `body` | El string concatenado se autoescapa (bug del prototipo); `|safe` reabre XSS | Títulos con marcado vía `{% set %}…{% endset %}` |
| D2 | Arnés **lado servidor** (TestClient + HTML canónico) | Playwright + normalizar hora + `wait_for` (lo que proponía el traspaso) | Sin JS no hay carrera de Chart.js; mide exactamente lo que cambia (plantillas); determinista y rápido | El comportamiento en navegador lo cubren los e2e (44 + 3 nuevos) |
| D3 | Comparación por tokens canónicos | texto exacto | Indentación/orden de atributos cambian sin cambiar el DOM | Comentarios HTML ignorados |
| D4 | `close_label=None` por defecto | `'Cerrar'` (firma del traspaso) | Con `'Cerrar'`, 17 modales cambian su HTML (aria-label) | a11y diferida a fase 3 |
| D5 | `field()` solo en filas simples de los 18 modales | todas las filas / ninguna | Ninguna = código muerto (prototipo); todas = fase desbordada | ~50 filas fuera de modales, fases 3/5 |
| D6 | Unificado de Utilidades con `kind` del router | 4 plantillas con macros | Las 4 solo difieren en URL, título y editabilidad | `kind` literal, nunca del usuario |
| D7 | `layouts/print.html` mínimo | mover estilos comunes | Los 3 `<style>` difieren; unificarlos cambia el HTML | `print.css` en fase 3 |
| D8 | Baseline commiteada | generarla al vuelo | Misma referencia para envelopes y revisión | Fases 2-5 la regeneran cuando cambien HTML a propósito |
| D9 | Arnés fuera de pytest | test de pytest | Levanta la app con lifespan y siembra; no acoplar a los unit | Se ejecuta en cada envelope de plantillas |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
base.html ★ (doctype, meta, {% block title/head/body %}, SIN assets)
├── layouts/app_shell.html ★ (tema + htmx, Chart.js, JsBarcode, app.css, app.js)
│   └── dashboard.html ✦
├── layouts/print.html ★ (sin assets de la app)
│   ├── transfer_print.html ✦   ├── restock_print.html ✦   └── label_single_print.html ✦ (solo JsBarcode)
└── login.html ✦

components/modal.html ★  modal(id, title, wide, heading, static, close_label, container_style) + caller()
components/forms.html ★  field(label, name, type, value, id, required, readonly, disabled, checked, attrs)
   ▲ usados por
partials/*_edit_form.html ✦ (14) · partials/profit_items_edit_form.html ★ ← ui_profit_items.py ✦ (kind)
partials/profit_items_{sale,purchase,adjustment,transfer_in}_edit_form.html ✗ (borrados)

tests/snapshots/ui_snapshot.py ★ → tests/snapshots/baseline/*.txt ★ (40)
· app.js (closeModal, data-modal-close, data-modal-static) — sin cambios, contrato preservado
```

### 3.2 Inventario de archivos

> **Scope duro.** `git diff --name-only 276b411` al terminar debe mostrar exactamente estos ficheros.

| Archivo | Operación | Envelope |
|---------|-----------|----------|
| `tests/snapshots/ui_snapshot.py` | CREATE | T1, T2 |
| `tests/snapshots/baseline/*.txt` (40) | CREATE (generados) | T3 |
| `app/templates/components/modal.html`, `components/forms.html` | CREATE | T4 |
| `app/templates/base.html`, `layouts/print.html` | CREATE | T5 |
| `app/templates/layouts/app_shell.html` / `dashboard.html` | CREATE / MODIFY | T6 |
| `app/templates/login.html` | MODIFY | T7 |
| `app/templates/transfer_print.html`, `restock_print.html` | MODIFY | T8 |
| `app/templates/label_single_print.html` | MODIFY | T9 |
| `app/templates/partials/{customer,expense}_edit_form.html` | MODIFY | T10 |
| `…/{extraction_edit_form,must_change_password_form}.html` | MODIFY | T11 |
| `…/{user_reset_password_form,sale_edit_form}.html` | MODIFY | T12 |
| `…/{purchase_edit_form,transfer_edit_form}.html` | MODIFY | T13 |
| `…/{product_edit_form,product_edit_form_inventory}.html` | MODIFY | T14 |
| `…/{user_edit_form,sales_document_edit_form}.html` | MODIFY | T15 |
| `…/{business_edit_form,transfer_bulk_modal}.html` | MODIFY (solo esqueleto) | T16 |
| `…/profit_items_edit_form.html` / `app/routers/ui_profit_items.py` | CREATE / MODIFY | T17 |
| `…/profit_items_{sale,purchase,adjustment,transfer_in}_edit_form.html` | DELETE | T18 |
| `tests/unit/test_ui_components.py` | CREATE | T19 |
| `tests/e2e/test_modal_components_e2e.py` | CREATE | T20 |
| `CHANGELOG.md` | MODIFY | T21 |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/routers/ui_common.py — YA EXISTE, NO MODIFICAR
templates = Jinja2Templates(directory="app/templates")          # autoescape activo
templates.env.globals["static_version"] = _static_asset_version  # callable

# app/services/inventory_service.py — usado por el arnés
class InventoryService:
    def __init__(self, db: Session, business_id: int | None = None): ...
    def purchase(self, payload: PurchaseCreate) -> MovementResult: ...
    def transfer(self, payload: TransferCreate) -> TransferResult: ...
    def sale(self, payload: SaleCreate) -> MovementResult: ...
    def adjustment(self, payload: AdjustmentCreate) -> MovementResult: ...

# app/settings.py — load_dotenv() (override=False); DATABASE_URL = env("DATABASE_URL", ...)
```

```javascript
// app/static/js/app.js:288-307 — contrato de los modales (NO cambia)
closeModal(id) → document.getElementById(id).remove()
click en [data-modal-close] → closeModal(attr); click en .modal-overlay salvo data-modal-static="1"
```

### 3.4 Interfaces nuevas

```jinja
{# components/modal.html #}
{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}
  {# overlay > container[-wide] > content > header(h3.modal-title | h2, botón cierre salvo static) + caller() #}
{% endmacro %}

{# components/forms.html #}
{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False,
               disabled=False, checked=False, attrs={}) %}
  {# <div class="row"><label[ for=id]>label</label><input …/></div>; value=None omite el atributo #}
{% endmacro %}

{# partials/profit_items_edit_form.html — contexto: kind ∈ {sale, purchase, adjustment, transfer_in} + el de antes #}
```

```bash
# tests/snapshots/ui_snapshot.py
.venv/bin/python tests/snapshots/ui_snapshot.py write <dir>   # captura 40 páginas
.venv/bin/python tests/snapshots/ui_snapshot.py check <dir>   # exit 0 = "OK: 40 snapshots equivalentes"
```

---

## 4. Fases de implementación (Task Envelopes)

Los 21 envelopes completos están en **`tasks.md`** (fuente única para cc_local; no se duplican aquí).

`Lotes: A T1–T3 · B T4–T9 · C T10–T13 · D T14–T16 · E T17–T18 · F T19–T20 · G T21`

| Lote | Envelopes | Qué hace | Tokens aprox. |
|---|---|---|---|
| **A** | T1 arnés (1/2) · T2 arnés (2/2) · T3 baseline | Red de seguridad **antes** de tocar plantillas | ~4.3K |
| **B** | T4 macros · T5 base+print · T6 app_shell+dashboard · T7 login · T8 2 impresiones · T9 etiqueta | Componentes y 5 `extends` | ~4.3K |
| **C** | T10–T13 | 8 modales simples | ~4.3K |
| **D** | T14–T16 | 4 modales + esqueleto de los 2 largos | ~4.3K |
| **E** | T17 unificado + router · T18 `git rm` de los 4 | Utilidades por artículo | ~2.3K |
| **F** | T19 unit (22) · T20 e2e (3) | Tests | ~3.1K |
| **G** | T21 | CHANGELOG | ~0.5K |

Excepciones conscientes a las reglas de envelope: T1 (~2.2K tokens) es copia literal de medio fichero (el
arnés se parte en dos para no pasar de ese tamaño); T18 borra 4 ficheros con un solo `git rm` mecánico.

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (ya hecha al generar el PAE)

- Worktree desechable desde `276b411` con el resultado completo: arnés 40/40, 409 unit, 47 e2e.
- Mutaciones: script en `base.html` → 5 snapshots fallan; cuerpo del modal escapado → 18 fallan y el e2e del
  cliente falla; `readonly` perdido en `field` → 2 fallan.
- Guarda del arnés probada con el `.env` real presente (apunta a la BD de dev): usa el SQLite temporal.
- `tasks.md` generado por script desde ese worktree; aplicado a una copia limpia de `276b411` → `diff -r`
  vacío (salvo el CHANGELOG, que solo escribe T21). Con la baseline generada por el camino T1–T3, todas las
  verificaciones de los envelopes dan su "Expected".
- `cc_local_pae --dry-run`: 7 lotes y ficheros detectados correctamente.

Antes de lanzar: `git status --porcelain app/ tests/` vacío y rama `feature/frontend-modular` en `276b411`
(o posterior sin cambios en `app/templates`).

### 5.1 Verificación por envelope

Cada envelope de plantillas termina con
`.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1` →
`OK: 40 snapshots equivalentes`, más comprobaciones mecánicas propias (conteos `grep`, `head -1`, pytest).
`cc_local_pae` corre además la suite unit tras cada lote.

### 5.2 Protocolo si una verificación falla

1. Si el arnés muestra `DIFF: <clave>`: el diff impreso indica qué token cambió → comparar el fichero con el
   envelope (copia literal). No "arreglar" cambiando la baseline.
2. Tres intentos fallidos → PARAR y reportar (cc_local_pae deja `lote<X>-PARADA.txt`).
3. Nunca regenerar `tests/snapshots/baseline/` después de T3.

### 5.3 Tests existentes que deben seguir pasando

387 unit y 44 e2e (en especial `test_tabs_smoke_e2e`, `test_transfer_bulk_e2e`, `test_sale_payment_method_e2e`,
`test_business_edit_keeps_dividends_e2e`, que abren modales migrados).

### 5.4 Tests e2e

Playwright (el proyecto no usa `behave`), `tests/e2e/test_modal_components_e2e.py`, marker `e2e`:
- `test_customer_edit_modal_renders_inputs_and_saves` — login → Clientes → crear `Cliente <b>E2E</b>` → Editar →
  inputs reales con el valor literal → guardar → modal cerrado y nombre actualizado (REQ-5).
- `test_customer_edit_modal_closes_with_x` — cierre con × (contrato `data-modal-close`).
- `test_profit_items_unified_modals_open_for_sale_and_purchase` — venta → Utilidades → "Edit sale" y
  "Edit buys" abren el modal unificado con sus campos (REQ-8).

Ejecución: `.venv/bin/python -m pytest tests/e2e -m e2e --no-cov`.

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|---|---|---|---|
| R1 | Cuerpo del modal escapado / `|safe` (bug del prototipo) | Baja | Alto (UI rota o XSS) | `caller()` literal; unit + e2e con HTML en el nombre; mutación verificada |
| R2 | Assets de la app filtrados a impresión/login | Baja | Medio | `base.html` sin assets; test de layouts; snapshots de las 3 impresiones con rutas reales |
| R3 | Alcance incompleto | Baja | Alto | Un envelope por par de ficheros; DoD con conteos mecánicos |
| R4 | Arnés que no prueba nada (404 vs 404, falsos positivos) | Baja | Alto | Aborta ante status ≠ 200 o respuesta vacía; normaliza fecha/hora/mes; validado `write`+`check` |
| R5 | El arnés siembra en la BD real (`.env` apunta a dev) | Baja | Alto | Fija `DATABASE_URL` antes de importar la app + guarda dura que aborta si no es el SQLite temporal |
| R6 | cc_local commitea cambios ajenos (`git add -A`) | Media | Medio | Cada envelope: `git add` solo de `Files`; `cc_local_pae` compara el estado sucio antes/después |
| R7 | Baseline capturada tras tocar plantillas | Baja | Alto | T3 comprueba `git status --porcelain app/` vacío antes de generarla |
| R8 | Fecha de captura distinta → falsos positivos de mes/año | Baja | Bajo | Normalización de fecha, hora, año-mes y nombres de mes; datos sembrados con fecha fija |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist (ejecutar en orden al terminar el lote G)

- [ ] `.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` → `OK: 40 snapshots equivalentes`
- [ ] `grep -rl 'class="modal-overlay"' app/templates/partials/` → vacío (REQ-7)
- [ ] `grep -c '{% extends' app/templates/*.html | grep -c ':1'` → 5 (REQ-4)
- [ ] `grep -rl "import modal" app/templates/partials/ | wc -l` → 15 (14 modales + el unificado = los 18 modales; REQ-7)
- [ ] `grep -rl "import field" app/templates/partials/ | wc -l` → 13 y `grep -rho "field(" app/templates/partials/ | wc -l` → 65 (REQ-6)
- [ ] `ls app/templates/partials/ | grep -c 'profit_items_.*edit_form'` → 1 (REQ-8)
- [ ] Unit: `SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest` → 409 passed
- [ ] e2e: `.venv/bin/python -m pytest tests/e2e -m e2e --no-cov` → 47 passed
- [ ] `git diff --name-only 276b411 -- VERSION .bumpversion.cfg` → vacío
- [ ] `git diff --name-only 276b411` ⊆ inventario §3.2 (sin `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`)
- [ ] 7 commits, uno por lote, sin push

### 7.2 Script de verificación completa

```bash
cd ~/PyCharmMiscProject/gestion_inventario
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
grep -rl 'class="modal-overlay"' app/templates/partials/ | wc -l          # 0
grep -c '{% extends' app/templates/*.html | grep -c ':1'                 # 5
grep -rl "import modal" app/templates/partials/ | wc -l                  # 15
grep -rl "import field" app/templates/partials/ | wc -l                  # 13
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings | tail -1
.venv/bin/python -m pytest tests/e2e -m e2e --no-cov -p no:warnings | tail -1
git diff --name-only 276b411 -- VERSION .bumpversion.cfg | wc -l       # 0
```

### 7.3 Commits esperados

`cc_local_pae` usa `feat(frontend-fase1-layouts-macros): lote <X> (T<a>-T<b>)` para los 7 lotes (A–G).
Ninguno toca `VERSION`/`.bumpversion.cfg`. Después: paso 5 del loop (`/code-review` + `auditoria-ingenieria`,
Opus, una pasada) y regenerar el traspaso de la fase.
