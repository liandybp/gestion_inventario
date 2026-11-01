# PAE-frontend-fase4-oob-uicontext

<!-- ═══════════════════════════════════════════════════════════════
     EJECUCIÓN POR cc_local — NO LEER ESTE PAE ENTERO
     ═══════════════════════════════════════════════════════════════
     Se ejecuta con:  cc_local_pae gestion_inventario specs/frontend-fase4-oob-uicontext/tasks.md
     que lanza UN LOTE POR SESIÓN LIMPIA y pasa a cc_local solo los rangos de líneas de sus envelopes.
     El envelope es la única fuente. Un commit por lote. Nunca push/sudo/systemctl.
     ═══════════════════════════════════════════════════════════════ -->

## Metadatos

| Campo | Valor |
|-------|-------|
| **Proyecto** | gestion_inventario |
| **Rama base** | develop (`b741a46`) |
| **Rama de trabajo** | feature/frontend-fase4 |
| **Fecha** | 2026-11-01 |
| **Versión actual** | 0.4.15 |
| **Versión objetivo** | 0.4.15 (sin bump; release agrupado 0.5.0) |
| **PAE anterior** | ninguno (fase 4 del release 0.5.0) |
| **Fase** | planificación (Claude Code + DeepSeek) → ejecución (cc_local, Qwen3.6-35B-A3B local) |

### Documentos fuente de este PAE

| Fase SDD | Output | Propósito |
|----------|--------|-----------|
| Phase 1 — Requirements | `specs/frontend-fase4-oob-uicontext/requirements.md` | REQ-1..12 con Gherkin, MoSCoW, NFRs |
| Phase 2 — Design | `specs/frontend-fase4-oob-uicontext/design.md` | Arquitectura, tabla de sustituciones, pre-mortem |
| Phase 3 — Task Envelopes | `specs/frontend-fase4-oob-uicontext/tasks.md` | 66 envelopes autocontenidos para cc_local |

### Skills asignadas a cc_local

- [ ] `backend-developer` — `ui_context.py`, `ui_nav.py`, migración de routers
- [ ] `frontend-developer` — macros, plantillas de fragmento, `dashboard.html`
- [ ] `test-automation-engineer` — tests unitarios, e2e, arnés
- [ ] `technical-writer` — CHANGELOG, CLAUDE.md, diagnóstico

---

## 1. Objetivo

### 1.1 Qué se construye

Cerrar la fase 4 del release 0.5.0 del frontend modular, con **equivalencia de HTML** (el HTML que ve el
usuario no cambia, verificado por el arnés contra la baseline de `b741a46`):

1. **Arnés H-303** (`tests/snapshots/ui_snapshot.py`): capturar además de lo actual (40 claves admin) los
   fragmentos OOB, los POST con error, los POST válidos, los estados vacíos y los dashboards/pestañas de
   `operator` y `owner` (REQ-1..5). Baseline generada desde el código sin migrar.
2. **`UIContext`** (`app/routers/ui_context.py`): dependencia perezosa que resuelve usuario/negocio/rol una vez
   por petición, inyectada con `Depends(ui_context)` en los 20 routers UI (REQ-7).
3. **Fragmentos a plantillas** (`templates/features/<dominio>/`): los 7 fragmentos OOB salen de `HTMLResponse` +
   `html.escape` a `templates.TemplateResponse` con autoescape de Jinja2, con macros compartidas por panel y
   fragmento (REQ-6).
4. **Sidebar data-driven** (`app/routers/ui_nav.py` + bucle en `dashboard.html`): `NAV_ITEMS` filtrado por rol y
   modo (REQ-8).
5. **Guarda** (`test_routers_no_html.py`) y **docs** (REQ-10, 11).

### 1.2 Por qué

El diagnóstico `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` señala tres deudas: HTML
construido en Python (6 routers, duplica markup y pisa clases/atributos), `user = …; bid =
require_active_business_id(db, request)` repetido ~100 veces, y sidebar con lógica de roles dentro de la plantilla.
La fase 4 las elimina sin cambiar el HTML renderizado.

### 1.3 Valor esperado

- Cero HTML en los routers (`test_routers_no_html.py` en verde).
- Un único punto de resolución de usuario/negocio (`UIContext`) con una consulta de usuario menos por petición.
- Sidebar testeable sin renderizar (tabla de verdad en `test_ui_nav.py`).
- Red de seguridad ampliada (arnés H-303) que detecta cualquier cambio de HTML/estado/orden de guardias antes del merge.

### 1.4 Qué NO se toca

- `app/security.py`, `app/deps.py`, `app/routers/ui_auth.py` (invariantes de control de acceso y de sesión).
- Routers API JSON (`products.py`, `inventory.py`, `sync.py`).
- `GET /ui/transfers/stock/{sku}` y `HTMLResponse("")` (no son marcado).
- Funciones auxiliares sin `@router` que reciben `(db, request)` (fase 5).
- Divergencias heredadas de REQ-12 (el fragmento de cliente conserva `required`, el panel no).
- Fase 5 (reagrupar `partials/` en `features/<dominio>/` y sacar queries de routers).

---

## 2. Decisiones de diseño

> cc_local no toma ninguna decisión de esta sección. Todo está cerrado aquí.

| # | Decisión | Alternativas | Justificación | Implicaciones |
|---|----------|-------------|---------------|---------------|
| D1 | `UIContext` perezoso, por propiedad | Ansioso (resolver todo en la dependencia) | Cambia el orden de errores (409 antes que 403) en handlers que comprueban el rol primero | Cada handler lee `ctx.user`/`ctx.business_id`/`ctx.ensure_*()` en el mismo punto que hoy |
| D2 | `UIContext` en `app/routers/ui_context.py` | `app/deps.py` o `app/security.py` | Son invariantes de acceso que no se tocan; el contexto es de la capa UI | Depende de `app.security`, no al revés |
| D3 | Migrar los 20 routers UI, no solo los 5 de fragmentos | Dejar dos estilos en paralelo | El diagnóstico asigna `UIContext` a la fase 4; dos estilos es deuda | 32 envelopes de migración mecánica |
| D4 | Funciones auxiliares `(db, request)` sin tocar | Pasarles `ctx` | Cambia firmas usadas por varios routers; se resuelve en fase 5 | Los imports que la auxiliar siga usando se conservan |
| D5 | Macros en `components/forms.html` compartidas por panel y fragmento | Plantilla de fragmento independiente | Una sola definición evita que el OOB pise el panel | `qty_label`/`number_input`/`text_input` |
| D6 | Fragmentos en `templates/features/<dominio>/` | `templates/partials/` | Nacen en la estructura objetivo | La fase 5 no los mueve otra vez |
| D7 | `NAV_ITEMS` en Python + bucle | Macro Jinja2 con la lista dentro | La lista es testeable sin renderizar | `test_ui_nav.py` fija la tabla de verdad |
| D8 | Divergencias heredadas conservadas | Unificarlas | Cambia comportamiento en una release de equivalencia (REQ-12) | `required` solo en el fragmento de cliente |
| D9 | **Tests existentes adaptados a `UIContext`** (inyectar `ctx` real / retargetar monkeypatch) | Mantener los imports muertos para que los `monkeypatch` sigan funcionando | La migración es la fuente de verdad; los imports muertos son deuda y contradicen D4 | 9 envelopes de adaptación (T40–T48) en el Lote C |

> **D9 es la corrección al diseño original**: 10 ficheros de `tests/unit/` llaman a handlers directamente
> (`test_ui_product_update.py`, `test_ui_product_from_invoice.py`, `test_ui_product_suggest.py`,
> `test_ui_xss_escapes.py`, `test_transfer_print_ref.py`, `test_ui_agent.py`, `test_agent_chat.py`,
> `test_coach_rbac.py`, `test_offline_hooks.py`, `test_ui_businesses.py`). La migración rompía esos tests por dos
> vías (monkeypatch sobre símbolos que desaparecen, y llamadas sin `ctx`). Se decidió adaptarlos en el Lote C.

---

## 3. Arquitectura objetivo

### 3.1 Diagrama de componentes

```
[TestClient / navegador]
        │ /ui/...
        ▼
★ app/routers/ui_context.py ──(ctx: UIContext = Depends(ui_context))──► ✦ 20 routers UI
        │  ctx.user / ctx.business_id / ctx.ensure_*()                       │
        ▼                                                                     ▼
· app/security.py (get_current_user_from_session, require_active_business_id)  · app/deps.py (session_dep)
        │
        ▼
✦ app/routers/ui_nav.py (NAV_ITEMS, visible_nav_items, default_tab) ──► ✦ dashboard.html ({% for %})
✦ app/templates/components/forms.html (qty_label/number_input/text_input)
        ├──► ✦ panel: purchase_panel.html, sale_panel.html, cart_add.html, issue_form.html
        └──► ★ features/<dominio>/_*.html (7 fragmentos OOB) ◄── ✦ endpoints (TemplateResponse)
✦ tests/snapshots/ui_snapshot.py (arnés H-303) ──► tests/snapshots/baseline/ (baseline desde b741a46)
```

★ = nuevo · ✦ = modificado · · = sin cambios (invariantes)

### 3.2 Inventario de archivos

> **Scope duro:** solo estos archivos. Verificación: `git diff --name-only b741a46`.

| Archivo | Operación | Responsabilidad |
|---------|-----------|-----------------|
| `app/routers/ui_context.py` | CREATE | `UIContext` perezoso + dependencia `ui_context` |
| `app/routers/ui_nav.py` | CREATE | `NAV_ITEMS`, `visible_nav_items`, `default_tab` |
| `app/routers/ui_activity.py` … `ui_users.py` (20) | MODIFY | migración a `ctx` (tabla de sustituciones design.md §2.3) |
| `app/routers/ui_common.py` | MODIFY | borrar `ensure_admin`/`ensure_admin_or_owner` si 0 referencias |
| `app/templates/components/forms.html` | MODIFY | macros `qty_label`/`number_input`/`text_input` |
| `app/templates/partials/purchase_panel.html` | MODIFY | label/input → macros |
| `app/templates/partials/sale_panel.html` | MODIFY | label/input → macros + datalist `{% include %}` |
| `app/templates/components/sales_doc/cart_add.html` | MODIFY | input → macro |
| `app/templates/components/sales_doc/issue_form.html` | MODIFY | inputs → macros |
| `app/templates/dashboard.html` | MODIFY | sidebar `{% for %}` + `#tab-content` `default_tab` |
| `app/templates/features/purchases/_defaults_oob.html` | CREATE | fragmento OOB de compra |
| `app/templates/features/sales/_defaults_oob.html` | CREATE | fragmento OOB de venta |
| `app/templates/features/sales/_product_options.html` | CREATE | bucle de `<option>` compartido |
| `app/templates/features/sales_documents/_product_defaults_oob.html` | CREATE | fragmento OOB precio documento |
| `app/templates/features/sales_documents/_customer_defaults_oob.html` | CREATE | fragmento OOB cliente (con `required`) |
| `app/templates/features/transfers/_product_options.html` | CREATE | bucle de `<option>` de traspasos |
| `app/templates/features/stock/_supplier_return_lots.html` | CREATE | `<select>` de lotes (4 variantes) |
| `tests/snapshots/ui_snapshot.py` | MODIFY | arnés H-303 (`_request`, siembra, `capture()`) |
| `tests/snapshots/baseline/*` | CREATE | baseline generada desde `b741a46` |
| `tests/e2e/test_fase4_characterization_e2e.py` | CREATE | e2e de caracterización (REQ-9) |
| `tests/unit/test_ui_context.py` | CREATE | pereza + una llamada por dato + `ctx.db` |
| `tests/unit/test_ui_nav.py` | CREATE | tabla de verdad REQ-8 + `default_tab` |
| `tests/unit/test_routers_no_html.py` | CREATE | guarda REQ-10 |
| `tests/unit/test_ui_product_update.py` y otros 9 | MODIFY | adaptación a `UIContext` (T40–T48) |
| `CHANGELOG.md` | MODIFY | sección 0.5.0 |
| `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md` | MODIFY | §4 fila 4 ✅ |
| `CLAUDE.md`, `.claude/CLAUDE.md` | MODIFY | estado de la fase 4 |

### 3.3 Dependencias existentes (firmas reales del repo)

```python
# app/security.py — ESTO YA EXISTE, NO MODIFICAR
def get_current_user_from_session(db: Session, request: Request) -> Optional[User]: ...
def get_active_business_id(db: Session, request: Request) -> Optional[int]: ...
def require_active_business_id(db: Session, request: Request) -> int:  # 409 "Active business_id is required"
# app/deps.py — ESTO YA EXISTE, NO MODIFICAR
def session_dep() -> Generator[Session, None, None]: ...
# app/auth.py — ESTO YA EXISTE, NO MODIFICAR
def hash_password(plain: str) -> str: ...
# app/settings.py — ESTO YA EXISTE
def app_mode_label() -> str: ...  # "desktop" | "server"
```

### 3.4 Interfaces nuevas a implementar

```python
# app/routers/ui_context.py
@dataclass
class UIContext:
    request: Request
    db: Session
    @property
    def user(self) -> Optional[User]: ...
    @property
    def business_id(self) -> int: ...       # 409 si no hay negocio
    def ensure_admin(self) -> None: ...      # 403 "Admin required"
    def ensure_admin_or_owner(self) -> None: ...

def ui_context(request: Request, db: Session = Depends(session_dep)) -> UIContext: ...

# app/routers/ui_nav.py
@dataclass(frozen=True)
class NavItem:
    slug: str
    label: str
    roles: Optional[frozenset[str]]
    server_only: bool = False

NAV_ITEMS: tuple[NavItem, ...] = (...)
def visible_nav_items(role: str, app_mode: str) -> list[NavItem]: ...
def default_tab(role: str) -> str: ...
```

---

## 4. Fases de implementación (Task Envelopes)

> Los 66 envelopes están en `specs/frontend-fase4-oob-uicontext/tasks.md`. `cc_local_pae` lee ese fichero por
> rangos de línea (un lote por sesión). Resumen de lotes:

`Lotes: **A** T1–T5 · **B** T6–T7 · **C** T8–T48 · **D** T49–T59 · **E** T60–T62 · **F** T63–T66`

| Lote | Envelopes | Contenido | Cierre |
|------|-----------|-----------|--------|
| **A (Lote 0)** | T1–T5 | arnés H-303 (`_request`, siembra, `capture()`), baseline desde `b741a46`, e2e de caracterización | `check` OK; mutación falla como se espera; e2e verdes; `git diff --stat b741a46 -- app/` vacío |
| **B** | T6–T7 | `ui_context.py` + `test_ui_context.py` | unit verdes |
| **C** | T8–T48 | migración a `UIContext` (T8–T39, un envelope por router, partido en 2 si >10 sustituciones) + adaptación de tests (T40–T48) | tras cada migración `check` idéntico; al final `pytest tests/unit` verde |
| **D** | T49–T59 | macros en `forms.html` + panels; 7 plantillas de fragmento + endpoints | `check` idéntico |
| **E** | T60–T62 | `ui_nav.py` + `test_ui_nav.py` + bucle en `dashboard.html` | `check` idéntico (3 roles); `test_ui_nav` verde |
| **F** | T63–T66 | `test_routers_no_html.py`; borrar `ensure_*` si 0 refs; CHANGELOG, CLAUDE.md, diagnóstico | suite completa verde |

> Nota sobre el Lote C: el diseño original pedía "unit + check tras cada envelope". Como la migración rompe
> 10 ficheros de tests existentes (D9), la verificación por envelope es el `check` del arnés (canónico idéntico);
> el `pytest tests/unit` completo se ejecuta al final del Lote C, después de T40–T48.

---

## 5. Plan de tests y verificación

### 5.0 Verificación pre-implementación (OBLIGATORIO)

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
# Si algo falla, NO empezar. Reportar y esperar.
```

### 5.1 Verificación por lote

- **Lote A**: `ui_snapshot.py write` + `check` + mutaciones (T5). e2e: `pytest tests/e2e/test_fase4_characterization_e2e.py -m e2e`.
- **Lote B**: `pytest tests/unit/test_ui_context.py`.
- **Lote C**: `ui_snapshot.py check` tras cada migración; `pytest tests/unit/test_ui_*.py` tras cada adaptación; `pytest tests/unit -q` al final.
- **Lote D**: `ui_snapshot.py check` (canónico idéntico) tras cada fragmento.
- **Lote E**: `ui_snapshot.py check` + `pytest tests/unit/test_ui_nav.py`.
- **Lote F**: `pytest tests/unit -q` + `ui_snapshot.py check` + `pytest tests/e2e -m e2e` + `style_snapshot.py check`.

### 5.2 Protocolo cuando una verificación falla

1. Leer el error completo. 2. `git diff` para ver qué se cambió. 3. Corregir el código (no el test). 4. Re-ejecutar.
5. Si tras 3 intentos sigue fallando: reportar error completo, diff y lo intentado. No avanzar.

### 5.3 Tests existentes que deben seguir pasando

- [ ] 465 tests de `tests/unit/` (los 10 adaptados en T40–T48 conservan su aserción original)
- [ ] 84 e2e de `tests/e2e/` (fixture `js_errors` autouse)
- [ ] `ui_snapshot.py check` — 40+ claves admin idénticas a la baseline
- [ ] `style_snapshot.py check` (fuera de CI)

### 5.4 Tests e2e (REQ-9)

Decisión por REQ en Fase 3: REQ-9 usa **Playwright** (interacción real de navegador: sidebar por rol, autorrelleno
OOB vía `hx-get`, select de lotes). Los escenarios de REQ-1..5, 7 y 8 se cubren con el arnés (TestClient) y tests
unitarios, no con `behave`, porque asierran HTML/servicio sin renderizar.

`tests/e2e/test_fase4_characterization_e2e.py`:
- `test_sidebar_ids_by_role` (×3 roles) — ids y orden de REQ-8 en server.
- `test_purchase_autofill_cost`, `test_sale_autofill_price`, `test_sales_doc_autofill_price_and_client`.
- `test_inventory_lots_select_present`.

Los e2e se escriben en el Lote 0 y pasan con el código sin migrar (caracterización).

---

## 6. Riesgos y mitigaciones

| # | Riesgo | Prob. | Impacto | Síntoma | Mitigación | Rollback |
|---|--------|-------|---------|---------|------------|----------|
| R1 | `UIContext` cambia el orden de errores | Media | Alto | `check` DIFF en dashboards/pestañas | Contexto perezoso + sustituciones en el mismo punto; `test_ui_context.py` | `git revert` del lote |
| R2 | Tests directos rotos por la migración | Alta (confirmada) | Alto | `pytest tests/unit` rojo | D9: adaptación explícita T40–T48 | revert |
| R3 | Import sin uso dejado/quitado de más | Media | Medio | `NameError`/`ImportError` en runtime | grep de referencias por envelope; `ui_snapshot.py check` | revert |
| R4 | Canónico de un fragmento no idéntico (escape vs Jinja2) | Media | Alto | `check` DIFF en `frag_*` | Macros con autoescape equivalentes; arnés captura el caso exacto | revert |
| R5 | `_tab_error`/`_render_error` anidadas migradas por error | Media | Alto | 403/409 fuera de orden | Envelopes marcan explícitamente qué auxiliar NO tocar | revert |
| R6 | Baseline dependiente de fecha/entorno | Baja | Alto | `check` DIFF global | `_FrozenDatetime` + entornos hermeticos ya fijados | regenerar baseline |

---

## 7. Definición de hecho (DoD)

### 7.1 Checklist pre-commit (en orden)

- [ ] **Pre-check:** `pytest tests/unit -q` — 100% antes de empezar (output guardado)
- [ ] **Lote 0:** `git diff --stat b741a46 -- app/` vacío; baseline commiteada; mutación documentada
- [ ] **Unit:** `pytest tests/unit -q` — 100% tras los cambios
- [ ] **Snapshots:** `.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline` — OK
- [ ] **e2e:** `pytest tests/e2e -m e2e --no-cov` — verdes
- [ ] **Estilos (fuera de CI):** `.venv/bin/python tests/snapshots/style_snapshot.py check`
- [ ] **Guarda:** `pytest tests/unit/test_routers_no_html.py` — sin HTML en routers ni `|safe`/`Markup(` en `features/`
- [ ] **Invariantes:** `app/security.py`, `app/deps.py`, `app/routers/ui_auth.py` sin cambios
- [ ] **Scope:** `git diff --name-only b741a46` muestra exactamente los archivos de §3.2
- [ ] **Limpieza:** sin `_tmp`/`_test`/`_draft`/`_vN`

### 7.2 Script de verificación completa

```bash
set -e
cd ~/PyCharmMiscProject/gestion_inventario
echo "=== unit ===" && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit -q
echo "=== snapshots ===" && .venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
echo "=== e2e ===" && SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/e2e -m e2e --no-cov
echo "=== scope ===" && git diff --name-only b741a46
```

### 7.3 Commits esperados

```
test(snapshots): arnés H-303 — fragmentos, POST, roles y estados vacíos + baseline
refactor(ui): UIContext perezoso en los routers UI (migración + tests adaptados)
refactor(ui): fragmentos OOB a plantillas features/ + macros compartidas
refactor(ui): sidebar data-driven con NAV_ITEMS
test(ui): guarda test_routers_no_html + limpieza ensure_* + docs fase 4
```

---

## Ejecución

```bash
cc_local_pae . specs/frontend-fase4-oob-uicontext/tasks.md --test-cmd 'SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/unit -q -o addopts="" -p no:warnings && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline' --lot0-test-cmd 'SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: ADMIN_USERNAME=admin ADMIN_PASSWORD=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test python3 -m pytest tests/e2e/test_fase4_characterization_e2e.py -m e2e -q -o addopts="" -p no:warnings && python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline'
```

---

> Generado por: SDD Lead (Phase 4 — PAE Assembly). Fuente: `requirements.md`, `design.md`, `tasks.md`.
