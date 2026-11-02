# PAE-frontend-fase5a-features-templates

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:  cc_local_pae . specs/frontend-fase5a-features-templates/tasks.md
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     El envelope es la única fuente. Un commit por lote. Nunca push/sudo/systemctl.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | develop (`bf38a2b`) |
| **Rama de trabajo** | feature/frontend-fase5a |
| **Fecha** | 2026-11-02 |
| **Versión actual** | 0.4.15 |
| **Versión objetivo** | 0.4.15 (sin bump; release agrupado 0.5.0) |
| **PAE anterior** | frontend-fase4-oob-uicontext (fase 4 del release 0.5.0) |
| **Fase** | planificación (Claude Code + DeepSeek) → ejecución (cc_local, Qwen3.6-35B-A3B local) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/frontend-fase5a-features-templates/requirements.md` | REQ-1..7 con Gherkin, MoSCoW, NFRs |
| Phase 2 — Design | `specs/frontend-fase5a-features-templates/design.md` | Tabla §2.3, guarda §2.4, plan de lotes §2.10, pre-mortem §2.9 |
| Phase 3 — Task Envelopes | `specs/frontend-fase5a-features-templates/tasks.md` | 30 envelopes autocontenidos para cc_local |

### Skills asignadas a cc_local

- [ ] `backend-developer` — `tools/move_templates.py` (script de migración)
- [ ] `test-automation-engineer` — `test_templates_layout.py`, REQ-5, comentario e2e
- [ ] `technical-writer` — CHANGELOG, CLAUDE.md, diagnóstico

---

## 1. Objetivo

### 1.1 Qué se construye

Cerrar la fase 5a del release 0.5.0 del frontend modular, con **equivalencia de HTML** (el HTML que ve el
usuario no cambia, verificado por el arnés contra la baseline de `bf38a2b` **sin regenerarla**):

1. **Mover las 54 plantillas vivas** de `app/templates/partials/` a `app/templates/features/<dominio>/` (mismo
   nombre, solo cambia la carpeta) según la tabla de `design.md` §2.3, con `git mv` para conservar el historial
   y sustitución exacta de las rutas de referencia (REQ-1, REQ-2).
2. **Borrar las 2 plantillas muertas** (`action_result.html`, `products_history.html`) y su comentario en el e2e
   (REQ-3).
3. **Guarda `tests/unit/test_templates_layout.py`**: falla si reaparece `partials/`, si queda una referencia
   `partials/` residual, o si un nombre literal de plantilla no resuelve (REQ-4).
4. **Retargetar** `test_old_profit_items_templates_are_gone` a la ruta nueva (REQ-5).
5. **Docs** — árbol de `templates/`, fila "5" del diagnóstico y CHANGELOG 0.5.0 (REQ-6).

### 1.2 Por qué

El diagnóstico `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` §2 (principio 1, "por dominio,
no por tipo de vista") y §4 fila "5" señalan `partials/` como un cajón plano de >60 ficheros. La fase 4 ya
colocó los fragmentos nuevos en `features/<dominio>/`; la 5a lleva allí las 54 plantillas restantes sin tocar
su contenido más allá de las rutas de `{% include %}`.

### 1.3 Valor esperado

- `app/templates/` organizado por dominio: `layouts/`, `components/`, `features/<dominio>/`.
- Una guarda que hace imposible reintroducir `partials/` o dejar una ruta de plantilla rota (cubre también los
  modales de error y las impresiones que el arnés no captura).
- Cero cambio de HTML: 91 snapshots y 74 capturas de estilos idénticas a `bf38a2b`.

### 1.4 Qué NO se toca

- Consultas, `UIContext` o `app/security.py` (es la fase 5b).
- Renombrar ficheros (REQ-7, Won't Have) o cambiar su contenido más allá de las rutas de `{% include %}`.
- `layouts/`, `components/`, `base.html`, `dashboard.html`, `login.html` ni las 4 plantillas de impresión de la
  raíz (`label_single_print.html`, `labels_print.html`, `restock_print.html`, `transfer_print.html`).
- Las 7 plantillas de la fase 4 ya en `features/` (no cambian de ruta ni de contenido).
- Rutas de backend sin enlace desde la UI (`/ui/product/{sku}/edit|update|delete`, `/ui/product/from-invoice`).
- La baseline del arnés no se regenera en ningún lote.

---

## 2. Decisiones de diseño

> cc_local no toma ninguna decisión de esta sección. Todo está cerrado aquí.

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | Mismo nombre, otra carpeta | Quitar prefijo de dominio (`features/sales/tab.html`) | Decisión del usuario: diff revisable, sustitución mecánica sin decisiones (REQ-7 excluido) | Sustitución de `partials/<nombre>.html` → `features/<dominio>/<nombre>.html` |
| D2 | Script `tools/move_templates.py` con la tabla §2.3 como dict | Edición a mano por fichero | ~190 líneas casi idénticas: la edición a mano es donde cc_local se equivoca de carácter | Un comando por dominio; el script aborta si una plantilla no está en la tabla o su aguja no aparece |
| D3 | Plantillas compartidas por 2 routers → dominio de la pestaña dueña | Directorio `shared/` | Solo 3 casos (`product_panel`, `tab_dividends`, `tab_inventory`) con pestaña dueña clara | `purchases`→`products/product_panel`, `print`→`stock/restock_table`, `dashboard`→`sync/sync_status` |
| D4 | Plantillas muertas se borran (no se mueven) | Moverlas también | Mover código muerto lo hace parecer vivo en la estructura nueva | REQ-3: `action_result.html`, `products_history.html` |
| D5 | La baseline del arnés no se regenera | Regenerarla tras el movimiento | Principio rector: el HTML no cambia, el arnés lo demuestra | `ui_snapshot.py check` y `style_snapshot.py check` (0 diferencias) como gates |
| D6 | Guarda creada en Lote 0 solo con `test_literal_template_names_resolve` | Crear los 3 tests desde el inicio | Los otros 2 fallarían mientras `partials/` exista; `--fail-on-skip` rechaza tests saltados en el Lote 0 | `test_partials_dir_is_gone`/`test_no_partials_references` se añaden en el Lote E |

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
app/templates/  (cargador único: app/routers/ui_common.py:22 `Jinja2Templates(directory="app/templates")`)
├── layouts/            (sin cambios)
├── components/         (sin cambios)
├── dashboard.html, login.html, base.html, *print.html   (sin cambios, salvo dashboard/restock_print que
│                                                          actualizan su include a features/…)
└── features/<dominio>/
     ├── purchases/sales/sales_documents/stock/transfers/   (7 fragmentos de la fase 4, sin tocar)
     └── ★ 21 dominios (16 nuevos + 5 existentes)           (54 plantillas movidas desde partials/)

★ tests/unit/test_templates_layout.py  ── guarda REQ-4 ──►  falla si partials/ reaparece o una ruta no resuelve
★ tools/move_templates.py  (Lote 0; borrado en Lote E)  ── git mv + sustitución por dominio
```

★ = nuevo · ✦ = modificado · · = sin cambios

### 3.2 Inventario de archivos

> **Scope duro:** solo estos archivos. Verificación: `git diff --name-only bf38a2b`.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/templates/partials/*` (54) → `app/templates/features/<dominio>/` (54) | RENAME (`git mv`) | plantillas vivas movidas (tabla §2.3) |
| `app/templates/partials/action_result.html`, `products_history.html` | DELETE | plantillas muertas (REQ-3) |
| `app/routers/ui_*.py` (21) | MODIFY | cadenas `partials/<nombre>.html` → `features/<dominio>/<nombre>.html` (154 líneas) |
| `app/templates/dashboard.html`, `restock_print.html` | MODIFY | `{% include %}` a `features/sync/sync_status.html` / `features/stock/restock_table.html` |
| `app/static/js/components/{home-charts,widget-chart}.js` | MODIFY | comentarios que nombran `partials/` (3) |
| `tests/unit/test_ui_product_suggest.py`, `test_ui_components.py`, `test_ui_agent.py`, `test_ui_xss_escapes.py`, `test_frontend_js_modules.py` | MODIFY | rutas `partials/` en literales (9 líneas) |
| `tests/e2e/test_js_behaviors_e2e.py` | MODIFY | 1 ruta `partials/` |
| `tests/e2e/test_ui_tour_csp_e2e.py` | MODIFY | comentario que nombra `products_history.html` (REQ-3) |
| `tests/unit/test_templates_layout.py` | CREATE (Lote 0) + MODIFY (Lote E) | guarda REQ-4 |
| `tools/move_templates.py` | CREATE (Lote 0) + DELETE (Lote E) | script de migración one-off |
| `CLAUDE.md`, `.claude/CLAUDE.md` | MODIFY | árbol de `templates/` (REQ-6) |
| `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` | MODIFY | §4 fila "5" → `**5a ✅**` (REQ-6) |
| `CHANGELOG.md` | MODIFY | bullet fase 5a en 0.5.0 (REQ-6) |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/routers/ui_common.py — ESTO YA EXISTE, NO MODIFICAR
templates = Jinja2Templates(directory="app/templates")
templates.env.get_template(name)          # lanza jinja2.exceptions.TemplateNotFound si no resuelve
templates.env.globals["static_version"] = _static_asset_version
```

### 3.4 Interfaces nuevas a implementar

```python
# tools/move_templates.py (borrado en el Lote E)
MAPPING: dict[str, list[str]] = {...}   # tabla de design.md §2.3
DELETE = ["action_result.html", "products_history.html"]
# Uso: python3 tools/move_templates.py --domain <dominio|delete>

# tests/unit/test_templates_layout.py
def test_literal_template_names_resolve() -> None: ...   # Lote 0
def test_partials_dir_is_gone() -> None: ...            # Lote E
def test_no_partials_references() -> None: ...          # Lote E
```

---

## 4. Fases de implementación (Task Envelopes)

> Los 30 envelopes están en `specs/frontend-fase5a-features-templates/tasks.md`. `cc_local_pae` lee ese fichero
> por rangos de línea (un lote por sesión). Resumen de lotes:

`Lotes: **A** T1–T3 · **B** T4–T11 · **C** T12–T17 · **D** T18–T25 · **E** T26–T30`

| Lote | Envelopes | Contenido | Cierre |
|------|-----------|-----------|--------|
| **A (Lote 0)** | T1–T3 | guarda `test_literal_template_names_resolve` (regex con `partials`), `tools/move_templates.py`, baseline de `style_snapshot.py` desde `bf38a2b` | test nuevo verde; `ui_snapshot.py check` OK; `git diff --stat bf38a2b -- app/` vacío |
| **B** | T4–T11 | dominios sin dependencias cruzadas: activity, auth, businesses, coach, history, profit, sync, users | tras cada dominio: `ui_snapshot.py check` + `pytest test_templates_layout.py` |
| **C** | T12–T17 | agent (9 con includes internos), home, customers, expenses, extractions, dividends | igual que B; además `test_ui_agent.py`, `test_ui_xss_escapes.py`, `test_frontend_js_modules.py` |
| **D** | T18–T25 | products, borrar las 2 muertas, purchases, sales, sales_documents, stock, transfers, profit_items (+ REQ-5) | igual que B; además `test_ui_product_suggest.py`, `test_ui_components.py` |
| **E** | T26–T30 | activar `test_partials_dir_is_gone` y `test_no_partials_references`; quitar `partials` de la regex; borrar `tools/move_templates.py`; comentario e2e; docs REQ-6 | `pytest tests/unit`, `ui_snapshot.py check`, `style_snapshot.py check` (0 diferencias), `pytest tests/e2e -m e2e` |

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q --override-ini="addopts="
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Si algo falla, NO empezar. Reportar y esperar.
```

### 5.1 Verificación por lote

- **Lote A (0)**: `pytest tests/unit/test_templates_layout.py` (1 passed) + `ui_snapshot.py check` + `git diff --stat bf38a2b -- app/` vacío.
- **Lotes B–D**: `ui_snapshot.py check` (canónico idéntico) tras cada dominio; `pytest test_templates_layout.py` verde.
- **Lote E**: `pytest tests/unit`, `ui_snapshot.py check`, `style_snapshot.py check` (0 diferencias), `pytest tests/e2e -m e2e`.

### 5.2 Protocolo cuando una verificación falla

1. Leer el error completo. 2. `git diff` para ver qué cambió. 3. Corregir el código (no el test). 4. Re-ejecutar.
5. Si tras 3 intentos sigue fallando: reportar error completo, diff y lo intentado. No avanzar.

### 5.3 Tests existentes que deben seguir pasando

- [ ] 496 tests de `tests/unit/` (los que referencian `partials/` quedan sustituidos por el script; REQ-5 retargeta uno)
- [ ] 94 e2e de `tests/e2e/` (fixture `js_errors` autouse)
- [ ] `ui_snapshot.py check` — 91 capturas idénticas a la baseline de `bf38a2b` (no regenerada)
- [ ] `style_snapshot.py check` (fuera de CI) — 0 diferencias contra la baseline de `bf38a2b`

### 5.4 Tests e2e

No hay REQ nuevo de UI o flujo de usuario: la fase 5a es un refactor de estructura con HTML idéntico. El
comportamiento se asierra con el arnés (`ui_snapshot.py`) y los estilos calculados (`style_snapshot.py`), y los
94 e2e existentes siguen pasando sin cambios. El único cambio en `tests/e2e/` es el comentario de REQ-3 (T28).

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | Ruta mal escrita en un handler que el arnés no captura (modal de error, impresión) | Media | Alto | 500 en producción | `test_literal_template_names_resolve` carga cada nombre literal (REQ-4c) | revert |
| R2 | Plantilla de un dominio repartida en dos carpetas | Baja | Medio | `partials/` residual | La tabla §2.3 es la única fuente; el script aborta si una plantilla no está en la tabla | revert |
| R3 | `partials/` reintroducido en una fase posterior | Baja | Alto | estructura degradada | `test_partials_dir_is_gone` + `test_no_partials_references` (Lote E) | revert |
| R4 | Un test que pasa sin comprobar nada tras el movimiento | Baja | Medio | `test_old_profit_items_templates_are_gone` tautológico | REQ-5: retargetar a `features/profit_items/` | revert |
| R5 | El script toca un fichero fuera de lo previsto | Baja | Medio | diff fuera de scope | `cc_verify --expect-files` por lote + `git diff --name-only bf38a2b` | revert |
| R6 | La baseline de estilos no se escribe (Playwright/chromium ausente) | Baja | Medio | `style_snapshot.py write` falla | Lote 0 la escribe; si falla, parar (Lote 0 es gate duro) | regenerar baseline |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (en orden)

- [ ] **Pre-check:** `pytest tests/unit -q --override-ini="addopts="` — 100% antes de empezar
- [ ] **Lote 0:** `git diff --stat bf38a2b -- app/` vacío; baseline de estilos escrita
- [ ] **Unit:** `pytest tests/unit -q --override-ini="addopts="` — 100% tras los cambios
- [ ] **Snapshots:** `python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` — OK (91 idénticas)
- [ ] **Estilos (fuera de CI):** `.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gestion_inventario-style-baseline-bf38a2b` — 0 diferencias
- [ ] **e2e:** `python3 -m pytest tests/e2e -m e2e -q --override-ini="addopts="` — verdes
- [ ] **Guarda:** `pytest tests/unit/test_templates_layout.py` — 3 passed
- [ ] **REQ-3:** `grep -rn "action_result\|products_history" app/ tests/` — sin coincidencias
- [ ] **Invariantes:** `app/security.py`, `app/deps.py`, `app/routers/ui_auth.py`, `app/routers/ui_common.py` sin cambios
- [ ] **Scope:** `git diff --name-only bf38a2b` muestra exactamente los archivos de §3.2
- [ ] **Limpieza:** `tools/move_templates.py` borrado; sin `_tmp`/`_test`/`_draft`/`_vN`

### 7.2 Script de verificación completa

```bash
set -e
cd ~/PyCharmMiscProject/gestion_inventario
echo "=== unit ===" && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q --override-ini="addopts="
echo "=== snapshots ===" && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
echo "=== estilos ===" && .venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gestion_inventario-style-baseline-bf38a2b
echo "=== e2e ===" && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e -q --override-ini="addopts="
echo "=== scope ===" && git diff --name-only bf38a2b
```

### 7.3 Commits esperados

```
test(ui): guarda test_templates_layout + script de migración (lote A)
refactor(frontend): fase 5a — partials/ a features/<dominio>/ (lotes B–D)
test(ui): activar guarda completa, borrar script, comentario e2e y docs (lote E)
```

---

## Ejecución

Comando de arranque (lo lanza `cc_deepseek` tras la aprobación, en segundo plano con registro):

```bash
cc_local_pae . specs/frontend-fase5a-features-templates/tasks.md \
  --test-cmd 'python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_templates_layout.py -q --override-ini="addopts="' \
  --lot0-test-cmd 'SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit/test_templates_layout.py -q --override-ini="addopts=" && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline'
```

- `--test-cmd` (por lote, tomado de `.gitea/workflows/deploy.yml` y `CLAUDE.md`): `ui_snapshot.py check` (HTML
  idéntico) + `pytest tests/unit/test_templates_layout.py` (guarda). Sin `--free-port`: los e2e usan puerto
  dinámico (`_free_port()` en `tests/e2e/conftest.py`).
- `--lot0-test-cmd` (test de comportamiento del Lote 0): la guarda armada + HTML idéntico; `--fail-on-skip`
  (añadido por `cc_local_pae`) exige que el test corra de verdad.

Verificación completa de cierre (la corre la revisión 5b, no `cc_local_pae` — es lo que corre el CI `test` de
`.gitea/workflows/deploy.yml`, más el arnés de estilos fuera de CI):

```bash
# unit (deploy.yml:31)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
  python3 -m pytest tests/unit -v --override-ini="addopts="
# ui_snapshot (deploy.yml:38)
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# e2e (deploy.yml:48)
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test \
  python3 -m pytest tests/e2e -m e2e -q --override-ini="addopts="
# estilos (fuera de CI)
.venv/bin/python tests/snapshots/style_snapshot.py check /tmp/gestion_inventario-style-baseline-bf38a2b
```

---

> Generado por: SDD Lead (Phase 4 — PAE Assembly). Fuente: `requirements.md`, `design.md`, `tasks.md`.
