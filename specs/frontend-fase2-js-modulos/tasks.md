# Task Envelopes — Frontend modular, fase 2: JS a módulos, sin JS inline, CSP sin 'unsafe-inline'

> SDD Phase 3 · PAE-017 · Release 0.5.0 (ningún commit sube versión) · Fuente: `requirements.md` (REQ-1..12),
> `design.md` (D1..D10, pre-mortem R1..R10)

**Generado por script** (`specs/frontend-fase2-js-modulos/gen/`) desde un worktree donde el resultado completo se
construyó y verificó (423 unit + 70 e2e, arnés 40/40, mutaciones detectadas). Simulación: aplicar estos envelopes a
una copia limpia de `b7a9881` reproduce ese worktree byte a byte (`gen/simulate.sh`).

**Ejecución:** `cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase2-js-modulos/tasks.md
--test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"`
(sin `--proto`: no hay prototipo). Rama `feature/frontend-modular`, sin push, sin bump de versión.

**Herramienta e2e:** Playwright (`tests/e2e/`, como el resto del proyecto; no usa `behave`). El riesgo de esta fase
está en el navegador (CSP, módulos, listeners): el arnés de snapshots no ejecuta JS (decisión D2 de la fase 1).

**Herramientas de cc_local:** cada envelope declara `Tools` y `Skills`. Ningún envelope usa MCP (regla dura) ni
skills: todo va literal (ficheros completos o scripts con `assert` de coincidencia única). Los comandos usan
`.venv/bin/python` desde la raíz del repo. `node --check` solo valida sintaxis (Node ya está instalado).

**Reglas para cc_local en este PAE:**
- Ficheros JS/tests: copiar el contenido literal. Plantillas y Python existentes: ejecutar el script literal; si un
  `assert` falla, PARAR (el fichero no está como se esperaba) — nunca editar a mano para «hacerlo cuadrar».
- `git add` solo de los `Files` del lote (en el repo hay cambios ajenos sin commitear: `CLAUDE.md`, `.claude/`,
  `.idea/`, `.serena/`, `specs/copiloto-ventas-ubicacion-mensual/`).
- Los e2e necesitan `SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test` si no hay `.env`.

**Trazabilidad:** REQ-1→T15,T17–T24,T26 · REQ-2→T25,T26,T28 · REQ-3→T3–T13,T15,T16,T26 · REQ-4→T3–T13,T17–T24,T29,T30 ·
REQ-5→T8,T16,T20,T26 · REQ-6→T1,T2,T17,T18,T20 · REQ-7→T14,T25,T26 · REQ-8→T27,T28 · REQ-9→T27–T30 · REQ-10→T8,T30 ·
REQ-11→T26 · REQ-12→T31

Lotes: **A** T1–T2 · **B** T3–T7 · **C** T8–T10 · **D** T11–T14 · **E** T15–T19 · **F** T20–T24 · **G** T25–T26 · **H** T27–T28 · **I** T29–T31

- **A** (arnés + baseline (ANTES de tocar plantillas)): T1, T2
- **B** (módulos núcleo y componentes de formulario): T3, T4, T5, T6, T7
- **C** (gráficos y traspasos): T8, T9, T10
- **D** (chat/tema, entrada, páginas y static_version): T11, T12, T13, T14
- **E** (cableado de layout, dashboard, login e impresiones): T15, T16, T17, T18, T19
- **F** (cableado de partials): T20, T21, T22, T23, T24
- **G** (CSP y guardas unitarias): T25, T26
- **H** (e2e de recorrido): T27, T28
- **I** (e2e de comportamientos y CHANGELOG): T29, T30, T31

**Orden:** A (arnés + baseline, ANTES de tocar nada en `app/`) → B, C, D (módulos nuevos: aún nadie los carga) →
E, F (cableado: plantillas pasan a `data-js-*` y módulos) → G (CSP estricto + guardas) → H, I (e2e) . Cada lote =
1 commit, y al final de cada lote la app funciona: hasta G el CSP sigue permitiendo inline, y ningún elemento
tiene a la vez `on*=` y `data-js-*` (no hay listeners duplicados).

---

### T1: Arnés de snapshots: ignorar el cableado JS y sembrar un widget de gráfico

**Lote:** A · **REQs:** REQ-6
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`tests/snapshots/ui_snapshot.py` (fase 1): clase `_Canon(HTMLParser)` con `handle_decl/starttag/startendtag/endtag/data`; `_seed()` termina con `user = db.scalar(...operator)` + `db.commit()`. `app.agent.widgets.add_widget(db, business_id, *, type, title, tool, tool_args, payload, unit, created_by)` hace su propio commit. `_hostile(label)` ya existe.

#### Objective

Que el canónico del arnés ignore `<script>` ejecutables, atributos `on*`/`data-js-*` y los 3 `#*-debug`, y que la siembra cree un widget de gráfico (cubre `widget_chart.html`).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

P = "tests/snapshots/ui_snapshot.py"
edit(P, """class _Canon(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tokens: list[str] = []

    def handle_decl(self, decl: str) -> None:
        self.tokens.append(f"<!{_norm(decl).lower()}>")

    def handle_starttag(self, tag: str, attrs: list) -> None:
        parts = [tag]
        for name, value in sorted(attrs, key=lambda a: a[0]):
            parts.append(name if value is None else f'{name}="{_norm(value)}"')
        self.tokens.append("<" + " ".join(parts) + ">")

    def handle_startendtag(self, tag: str, attrs: list) -> None:
        self.handle_starttag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        self.tokens.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        text = _norm(data)
        if text:
            self.tokens.append(text)
""", """# Fase 2 (JS a módulos): el canónico ignora el cableado JS, que cambia a propósito, para que
# la baseline generada con el código anterior a la fase siga sirviendo de referencia:
# - <script> ejecutables (inline o con src) y su contenido; los <script type="application/json">
#   (datos de los gráficos) sí cuentan. Qué scripts carga cada página lo fija un unit test.
# - atributos on* (onclick=…) y data-js-* (los enganches que los sustituyen).
# - los 3 <div id="…-debug"> de los gráficos de Inicio, eliminados en la fase 2.
_REMOVED_DEBUG_IDS = {"monthly-chart-debug", "monthly-sales-daily-line-debug", "monthly-sales-pie-debug"}


def _is_js_wiring_attr(name: str) -> bool:
    return name.startswith("on") or name.startswith("data-js-")


class _Canon(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.tokens: list[str] = []
        self._skip_tag: str | None = None

    def handle_decl(self, decl: str) -> None:
        self.tokens.append(f"<!{_norm(decl).lower()}>")

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if self._skip_tag:
            return
        attr_map = dict(attrs)
        if tag == "script" and attr_map.get("type") != "application/json":
            self._skip_tag = "script"
            return
        if tag == "div" and attr_map.get("id") in _REMOVED_DEBUG_IDS:
            self._skip_tag = "div"
            return
        parts = [tag]
        for name, value in sorted(attrs, key=lambda a: a[0]):
            if _is_js_wiring_attr(name):
                continue
            parts.append(name if value is None else f'{name}="{_norm(value)}"')
        self.tokens.append("<" + " ".join(parts) + ">")

    def handle_startendtag(self, tag: str, attrs: list) -> None:
        self.handle_starttag(tag, attrs)
        if self._skip_tag == tag:
            self._skip_tag = None

    def handle_endtag(self, tag: str) -> None:
        if self._skip_tag:
            # Los elementos omitidos no tienen hijos del mismo tag (script; div vacío).
            if tag == self._skip_tag:
                self._skip_tag = None
            return
        self.tokens.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self._skip_tag:
            return
        text = _norm(data)
        if text:
            self.tokens.append(text)
""")
edit(P, """        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()""", """        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()
        add_widget(db, bid, type="pie", title=_hostile("Widget"), tool="snapshot", tool_args={},
                   payload={"labels": [_hostile("A"), "B"], "values": [1, 2]}, unit=None, created_by=None)""")
edit(P, "from app.db import get_session  # noqa: E402\n",
     "from app.agent.widgets import add_widget  # noqa: E402\nfrom app.db import get_session  # noqa: E402\n")
print("OK")
PY
````

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read())"   # sin salida = OK
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T2: Regenerar la baseline desde el commit anterior a la fase (worktree, set -e)

**Lote:** A · **REQs:** REQ-6
**Depends on:** T1
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/baseline/`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Bash (script literal) · **Skills:** ninguna — NO ejecutar `write` sobre el repo, solo este script

#### Context

Lección de la fase 1: un `;` en vez de `&&` escribió una vez la baseline sobre el código migrado. Este script usa `set -euo pipefail` y tres guardas (sin cambios en `app/`, existe `app.js`, no existe `main.js`). El arnés fija su propia BD SQLite temporal y credenciales (no usa `.env`).

#### Objective

Baseline nueva = arnés de T1 ejecutado sobre el código ANTERIOR a la fase 2, en el mismo commit que el cambio del arnés (H-307).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
set -euo pipefail
REPO="$(git rev-parse --show-toplevel)"
cd "$REPO"
# Guardas: la baseline se genera SIEMPRE con el código anterior a la fase 2 (HEAD, que este
# lote aún no ha cambiado en app/), nunca sobre el código migrado.
git diff --quiet HEAD -- app/
test -e app/static/js/app.js
test ! -e app/static/js/main.js
WT="$(mktemp -d)/base"
git worktree add --detach "$WT" HEAD
test ! -e "$WT/app/static/js/main.js"
cp tests/snapshots/ui_snapshot.py "$WT/tests/snapshots/ui_snapshot.py"
(cd "$WT" && "$REPO/.venv/bin/python" tests/snapshots/ui_snapshot.py write tests/snapshots/baseline)
rm -rf tests/snapshots/baseline
cp -r "$WT/tests/snapshots/baseline" tests/snapshots/baseline
git worktree remove --force "$WT"
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
````

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40 snapshots equivalentes
git status --short tests/snapshots/baseline   # 13 .txt modificados (scripts y widget), ninguno nuevo ni borrado
```

#### Expected Report

- [ ] `tests/snapshots/baseline/` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T3: `theme-init.js` y `core/content.js`

**Lote:** B · **REQs:** REQ-3, REQ-4
**Depends on:** T2
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/theme-init.js`, `app/static/js/core/content.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Sustituyen al `<script>` inline de `layouts/app_shell.html` (tema) y a `runEnhancers` + `DOMContentLoaded`/`htmx:afterSwap` de `app.js` (misma semántica: doble `requestAnimationFrame` tras el swap). htmx 1.9.12 emite `htmx:afterSwap` sobre cada elemento swapeado, OOB incluidos.

#### Objective

Crear el script clásico de tema (pre-pintado) y el registro de enhancers de contenido.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/theme-init.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Aplica el tema guardado ANTES del primer pintado (evita el destello de color).
// Script clásico y síncrono en <head>: no puede ser un módulo (los módulos se difieren).
(function () {
  try {
    var t = localStorage.getItem("gi-theme") || "light";
    document.documentElement.setAttribute("data-theme", t);
  } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
})();
````

**Step 2: escribir `app/static/js/core/content.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Registro de "enhancers": funciones que se aplican al contenido nuevo.
// Se ejecutan sobre `document` en DOMContentLoaded y sobre cada elemento que swapea htmx
// (evt.target de htmx:afterSwap: el target principal y también cada elemento OOB), tras dos
// frames, para que el layout del contenido nuevo ya esté calculado.
const enhancers = [];

export function onContent(fn) {
  enhancers.push(fn);
}

function run(root, fromSwap) {
  enhancers.forEach((fn) => fn(root, fromSwap));
}

export function init() {
  document.addEventListener('DOMContentLoaded', () => run(document, false));
  document.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.target;
    requestAnimationFrame(() => {
      requestAnimationFrame(() => run(target, true));
    });
  });
}
````

#### Verification

```bash
node --check app/static/js/theme-init.js && node --check app/static/js/core/content.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/theme-init.js`, `app/static/js/core/content.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T4: `core/activity.js` y `core/tabs.js`

**Lote:** B · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/core/activity.js`, `app/static/js/core/tabs.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Código de `app.js` (`markUserActivity`, `saveActiveTab`, `restoreActiveTab`, `setActiveTab`, `onTabClick`, bloque `DOMContentLoaded`) sin cambios de comportamiento. `restoreActiveTab()` se ejecuta al evaluar el módulo (antes de `DOMContentLoaded`, como el `defer` de `app.js`).

#### Objective

Mover a módulos la actividad de usuario (cabecera `X-User-Activity`), las pestañas, el reintento de carga inicial y el menú móvil.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/core/activity.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Actividad del usuario para el timeout de inactividad del servidor: los polls de htmx en
// segundo plano no deben mantener viva la sesión, así que cada petición lleva la hora de la
// última interacción real en la cabecera X-User-Activity.
let lastUserActivity = Date.now();
let lastUserActivityWrite = 0;

function markUserActivity() {
  const now = Date.now();
  if ((now - lastUserActivityWrite) < 1000) return;
  lastUserActivityWrite = now;
  lastUserActivity = now;
}

export function init() {
  ['click', 'keydown', 'scroll', 'touchstart', 'mousemove'].forEach((type) => {
    document.addEventListener(type, markUserActivity, { passive: true, capture: true });
  });
  document.addEventListener('htmx:configRequest', (evt) => {
    try {
      if (evt.detail && evt.detail.headers) {
        evt.detail.headers['X-User-Activity'] = String(lastUserActivity || '');
      }
    } catch (e) {}
  });
}
````

**Step 2: escribir `app/static/js/core/tabs.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Pestañas del sidebar: recordar la activa (localStorage), restaurarla al cargar,
// reintentar la carga inicial de #tab-content y el menú móvil.
const ACTIVE_TAB_KEY = 'active_tab_v1';

function setActiveTab(tabEl) {
  document.querySelectorAll('.sidebar-item').forEach((t) => t.setAttribute('aria-selected', 'false'));
  if (tabEl) tabEl.setAttribute('aria-selected', 'true');
}

function saveActiveTab(tabEl) {
  if (!tabEl) return;
  const tabId = tabEl.id || '';
  const url = tabEl.getAttribute('hx-get') || '';
  if (!url) return;
  try {
    localStorage.setItem(ACTIVE_TAB_KEY, JSON.stringify({ id: tabId, url }));
  } catch (e) {}
}

function restoreActiveTab() {
  let saved = null;
  try {
    saved = JSON.parse(localStorage.getItem(ACTIVE_TAB_KEY) || 'null');
  } catch (e) {
    saved = null;
  }
  if (!saved || !saved.url) return;

  const tabContent = document.getElementById('tab-content');
  if (!tabContent) return;

  const savedBtn = saved.id ? document.getElementById(saved.id) : null;
  let btn = savedBtn && savedBtn.classList.contains('sidebar-item') ? savedBtn : null;
  if (!btn && saved.url) {
    btn = document.querySelector(`.sidebar-item[hx-get="${saved.url}"]`);
  }
  if (!btn) return;

  const url = btn.getAttribute('hx-get') || '';
  if (!url) return;

  tabContent.setAttribute('hx-get', url);
  setActiveTab(btn);
}

function onTabClick(evt) {
  const el = evt.target.closest('.sidebar-item');
  if (!el) return;
  setActiveTab(el);
  saveActiveTab(el);
}

function retryInitialTabLoad() {
  try {
    const tabContent = document.getElementById('tab-content');
    if (tabContent && (!String(tabContent.innerHTML || '').trim())) {
      const url = tabContent.getAttribute('hx-get') || '';
      if (url && window.htmx && typeof window.htmx.trigger === 'function') {
        setTimeout(() => {
          if (!String(tabContent.innerHTML || '').trim()) {
            try { window.htmx.trigger(tabContent, 'load'); } catch (e) {}
          }
        }, 200);
      }
    }
  } catch (e) {}
}

function initMobileMenu() {
  const menuToggle = document.getElementById('mobile-menu-toggle');
  const menuOverlay = document.getElementById('mobile-menu-overlay');
  const sidebar = document.getElementById('sidebar');
  if (!menuToggle || !menuOverlay || !sidebar) return;

  const toggleMenu = () => {
    sidebar.classList.toggle('active');
    menuOverlay.classList.toggle('active');
  };
  menuToggle.addEventListener('click', toggleMenu);
  menuOverlay.addEventListener('click', toggleMenu);
  // Cerrar el menú al elegir una pestaña.
  sidebar.addEventListener('click', (e) => {
    if (e.target.closest('.sidebar-item')) {
      sidebar.classList.remove('active');
      menuOverlay.classList.remove('active');
    }
  });
}

export function init() {
  // Antes de DOMContentLoaded: htmx lee el hx-get de #tab-content al procesar el body.
  restoreActiveTab();
  document.addEventListener('click', onTabClick);
  document.addEventListener('DOMContentLoaded', () => {
    retryInitialTabLoad();
    initMobileMenu();
  });
}
````

#### Verification

```bash
node --check app/static/js/core/activity.js && node --check app/static/js/core/tabs.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/core/activity.js`, `app/static/js/core/tabs.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T5: `core/modals.js` y `core/swap.js`

**Lote:** B · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/core/modals.js`, `app/static/js/core/swap.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Código de `app.js` (`closeModal`, `onModalClose`, `onModalOverlayClick`, `onClearClick`, listeners `htmx:afterRequest`/`beforeRequest`/`beforeSwap`/`afterSwap`) sin cambios de comportamiento. Atributos que se conservan: `data-modal-close`, `data-modal-static`, `data-clear-target`, cabecera `X-Modal-Keep`.

#### Objective

Mover a módulos el cierre de modales, `[data-clear-target]`, el swap de errores 4xx y la conservación del scroll.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/core/modals.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Modales: cierre por botón [data-modal-close], por clic en el overlay (salvo
// data-modal-static="1") y automático tras una petición correcta desde dentro del modal.
// También [data-clear-target]: vacía el elemento indicado.
function closeModal(modalId) {
  const modal = document.getElementById(modalId);
  if (modal) {
    modal.remove();
  }
}

function onModalClose(evt) {
  const closeBtn = evt.target.closest('[data-modal-close]');
  if (!closeBtn) return;
  evt.preventDefault();
  closeModal(closeBtn.getAttribute('data-modal-close'));
}

function onModalOverlayClick(evt) {
  if (evt.target.classList.contains('modal-overlay')) {
    if (evt.target.getAttribute('data-modal-static') === '1') return;
    const modalId = evt.target.closest('[id]')?.id;
    if (modalId) closeModal(modalId);
  }
}

function onClearClick(evt) {
  const btn = evt.target.closest('[data-clear-target]');
  if (!btn) return;
  evt.preventDefault();
  const selector = btn.getAttribute('data-clear-target');
  if (!selector) return;
  const el = document.querySelector(selector);
  if (el) el.innerHTML = '';
}

function onAfterRequest(evt) {
  const elt = evt.detail && evt.detail.elt;
  const xhr = evt.detail && evt.detail.xhr;
  if (!elt || !xhr) return;

  const modal = elt.closest('.modal-overlay');
  if (!modal) return;

  const keepOpen = String(xhr.getResponseHeader('X-Modal-Keep') || '') === '1';
  if (keepOpen) return;

  // Error 4xx en HTML ya swapeado (ver core/swap.js) en un panel FUERA del modal
  // (p. ej. product_edit_form → #product-panel): cerrar el modal para que se vea.
  const target = evt.detail && evt.detail.target;
  const isHtml = String(xhr.getResponseHeader('Content-Type') || '').includes('text/html');
  const errorShownBehind = xhr.status >= 400 && xhr.status < 500 && isHtml && target && !modal.contains(target);

  if ((xhr.status >= 200 && xhr.status < 300) || errorShownBehind) {
    try {
      modal.remove();
    } catch (e) {}
  }
}

export function init() {
  document.addEventListener('click', onClearClick);
  document.addEventListener('click', onModalClose);
  document.addEventListener('click', onModalOverlayClick);
  document.addEventListener('htmx:afterRequest', onAfterRequest);
}
````

**Step 2: escribir `app/static/js/core/swap.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Comportamiento de los swaps de htmx: mostrar los errores 4xx en HTML y conservar el
// scroll al recargar los paneles de compra y venta.
let pendingScrollRestore = null;

function shouldPreserveScrollForElt(elt) {
  if (!elt || typeof elt.closest !== 'function') return false;
  return Boolean(elt.closest('#purchase-panel') || elt.closest('#sale-panel'));
}

export function init() {
  document.addEventListener('htmx:beforeRequest', (evt) => {
    const elt = evt.detail && evt.detail.elt;
    if (!shouldPreserveScrollForElt(elt)) return;
    pendingScrollRestore = window.scrollY;
  });

  // Los routers UI devuelven su panel con el mensaje de error y el status de la
  // HTTPException (400/404/409/422...). htmx no swapea respuestas >= 400 por
  // defecto, así que sin esto el error no se veía. Solo HTML: las 422 de
  // validación de FastAPI son JSON y el 403 de CSRF es texto plano. El chat del
  // copiloto ya inserta sus errores a mano.
  document.addEventListener('htmx:beforeSwap', (evt) => {
    const xhr = evt.detail && evt.detail.xhr;
    if (!xhr || xhr.status < 400 || xhr.status >= 500) return;
    const path = String((evt.detail.requestConfig && evt.detail.requestConfig.path) || '');
    if (path.startsWith('/ui/agent/chat')) return;
    const ct = String(xhr.getResponseHeader('Content-Type') || '');
    if (!ct.includes('text/html')) return;
    evt.detail.shouldSwap = true;
    evt.detail.isError = false;
  });

  document.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.target;
    if (pendingScrollRestore !== null && target && (target.id === 'purchase-panel' || target.id === 'sale-panel')) {
      const y = pendingScrollRestore;
      pendingScrollRestore = null;
      requestAnimationFrame(() => {
        try { window.scrollTo({ top: y, left: 0, behavior: 'auto' }); } catch (e) { window.scrollTo(0, y); }
      });
    }
  });
}
````

#### Verification

```bash
node --check app/static/js/core/modals.js && node --check app/static/js/core/swap.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/core/modals.js`, `app/static/js/core/swap.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T6: `components/barcode.js` y `components/notes.js`

**Lote:** B · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/barcode.js`, `app/static/js/components/notes.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`import { onContent } from '../core/content.js'` (T3). Código de `renderBarcodes`, `saveReusableNote`, `getReusableNotes`, `enhanceNoteInputs` de `app.js`. Se conservan `data-code`, `data-rendered`, `data-note-enhanced`, `data-note-save-listener` y la clave `reusable_notes`.

#### Objective

Mover a módulos el render de códigos de barras y las notas reutilizables.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/barcode.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Códigos de barras: <svg class="barcode" data-code="..."> se renderiza con JsBarcode (global).
import { onContent } from '../core/content.js';

function renderBarcodes(root) {
  if (!window.JsBarcode) return;
  const container = root || document;
  const svgs = container.querySelectorAll('svg.barcode[data-code]:not([data-rendered="1"])');
  svgs.forEach((svg) => {
    const code = svg.getAttribute('data-code') || '';
    if (!code) return;
    try {
      window.JsBarcode(svg, code, { format: 'CODE128', displayValue: false, height: 28, margin: 0, width: 1 });
      svg.setAttribute('data-rendered', '1');
    } catch (e) {}
  });
}

export function init() {
  onContent(renderBarcodes);
}
````

**Step 2: escribir `app/static/js/components/notes.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Notas reutilizables: los <input name="note"> ofrecen (datalist) las últimas 20 notas
// guardadas en localStorage; al enviar el formulario se guarda la nota usada.
import { onContent } from '../core/content.js';

const NOTES_KEY = 'reusable_notes';

function saveReusableNote(note) {
  if (!note || !note.trim()) return;
  const trimmed = note.trim();
  try {
    let notes = JSON.parse(localStorage.getItem(NOTES_KEY) || '[]');
    if (!notes.includes(trimmed)) {
      notes.unshift(trimmed);
      notes = notes.slice(0, 20);
      localStorage.setItem(NOTES_KEY, JSON.stringify(notes));
    }
  } catch (e) {}
}

function getReusableNotes() {
  try {
    return JSON.parse(localStorage.getItem(NOTES_KEY) || '[]');
  } catch (e) {
    return [];
  }
}

function enhanceNoteInputs(root) {
  const noteInputs = (root || document).querySelectorAll('input[name="note"]');
  noteInputs.forEach((input) => {
    if (input.hasAttribute('data-note-enhanced')) return;
    input.setAttribute('data-note-enhanced', 'true');

    let datalist = input.list;
    if (!datalist) {
      const listId = 'reusable-notes-list-' + Math.random().toString(36).substr(2, 9);
      datalist = document.createElement('datalist');
      datalist.id = listId;
      input.setAttribute('list', listId);
      input.parentElement.appendChild(datalist);
    }

    const notes = getReusableNotes();
    datalist.innerHTML = '';
    notes.forEach((note) => {
      const option = document.createElement('option');
      option.value = note;
      datalist.appendChild(option);
    });

    const form = input.closest('form');
    if (form && !form.hasAttribute('data-note-save-listener')) {
      form.setAttribute('data-note-save-listener', 'true');
      form.addEventListener('submit', () => {
        const noteVal = input.value;
        if (noteVal) saveReusableNote(noteVal);
      });
    }
  });
}

export function init() {
  onContent(enhanceNoteInputs);
}
````

#### Verification

```bash
node --check app/static/js/components/barcode.js && node --check app/static/js/components/notes.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/components/barcode.js`, `app/static/js/components/notes.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T7: `components/product-suggest.js` y `components/form-helpers.js`

**Lote:** B · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/product-suggest.js`, `app/static/js/components/form-helpers.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`form-helpers.js` sustituye: 6 `onclick` de «seleccionar todo» → `data-js-select-all="<name>"`; `onchange="this.form.submit()"` → `data-js-autosubmit`; `onchange` de moneda en `cart_add.html` → `data-js-mirror-to="<id>"`; `onkeydown` Enter de `sale_panel.html` → `data-js-enter-focus="quantity"`. Listeners delegados en `document` (los partials llegan por htmx).

#### Objective

Mover a módulo las sugerencias de producto y crear los enganches `data-js-*` que sustituyen a los 10 handlers de formulario inline.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/product-suggest.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Sugerencias dinámicas de productos: los datalist iniciales solo traen los 200 primeros
// productos; al teclear en un input con data-product-suggest se piden coincidencias al
// servidor. No usa hx-get porque varios de estos inputs ya lo usan para cargar los valores
// por defecto del artículo. Ventas y Traspasos NO llevan data-product-suggest: sus datalist
// filtran por stock en la ubicación.
const suggestTimers = new WeakMap();
const suggestInitial = new WeakMap(); // datalist -> contenido inicial (lo comparten varias líneas)

function onInput(evt) {
  const input = evt.target;
  if (!(input instanceof HTMLInputElement) || !input.dataset.productSuggest || !input.list) return;
  const list = input.list;
  if (!suggestInitial.has(list)) suggestInitial.set(list, list.innerHTML);
  clearTimeout(suggestTimers.get(input));
  suggestTimers.set(input, setTimeout(async () => {
    const q = input.value.trim();
    if (q.length < 2) {
      // Campo vaciado: vuelven las sugerencias iniciales.
      if (list.innerHTML !== suggestInitial.get(list)) list.innerHTML = suggestInitial.get(list);
      return;
    }
    // El valor ya es una opción (el usuario eligió una sugerencia o escaneó un código): no consultar.
    if (Array.from(list.options).some((o) => o.value === input.value)) return;
    const url = '/ui/product-suggest?q=' + encodeURIComponent(q) + '&fmt=' + encodeURIComponent(input.dataset.productSuggest);
    try {
      const resp = await fetch(url, { credentials: 'same-origin' });
      // Sesión caducada (redirige al login), error, o el usuario siguió tecleando: descartar.
      if (!resp.ok || resp.redirected || input.value.trim() !== q) return;
      const html = await resp.text();
      // Sin coincidencias: se deja la lista como está (el navegador ya filtra lo que no coincide).
      if (html.trim()) list.innerHTML = html;
    } catch (e) {}
  }, 250));
}

export function init() {
  document.addEventListener('input', onInput);
}
````

**Step 2: escribir `app/static/js/components/form-helpers.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Comportamientos de formulario declarados con data-js-* (sustituyen a los onclick=/onchange=
// /onkeydown= inline, que el CSP sin 'unsafe-inline' bloquea):
//   data-js-select-all="<name>"   casilla que marca/desmarca los input[name=<name>] de su form
//   data-js-autosubmit            al cambiar, envía su formulario (form.submit(), sin htmx)
//   data-js-mirror-to="<id>"      al cambiar, copia su valor al elemento con ese id
//   data-js-enter-focus="<name>"  Enter no envía: pasa el foco al input[name=<name>] del form
function onClick(evt) {
  const master = evt.target.closest('[data-js-select-all]');
  if (!master) return;
  const form = master.closest('form');
  if (!form) return;
  const name = master.getAttribute('data-js-select-all');
  form.querySelectorAll(`input[name="${name}"]`).forEach((cb) => { cb.checked = master.checked; });
}

function onChange(evt) {
  const el = evt.target;
  if (!(el instanceof Element)) return;
  if (el.hasAttribute('data-js-autosubmit') && el.form) {
    el.form.submit();
    return;
  }
  const mirrorId = el.getAttribute('data-js-mirror-to');
  if (mirrorId) {
    const target = document.getElementById(mirrorId);
    if (target) target.value = el.value;
  }
}

function onKeydown(evt) {
  const el = evt.target;
  if (evt.key !== 'Enter' || !(el instanceof Element)) return;
  const name = el.getAttribute('data-js-enter-focus');
  if (!name || !el.form) return;
  evt.preventDefault();
  const next = el.form.querySelector(`input[name="${name}"]`);
  if (next) {
    next.focus();
    if (next.select) next.select();
  }
}

export function init() {
  document.addEventListener('click', onClick);
  document.addEventListener('change', onChange);
  document.addEventListener('keydown', onKeydown);
}
````

#### Verification

```bash
node --check app/static/js/components/product-suggest.js && node --check app/static/js/components/form-helpers.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/components/product-suggest.js`, `app/static/js/components/form-helpers.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T8: `components/home-charts.js` (sin depuración, tooltip sin XSS)

**Lote:** C · **REQs:** REQ-3, REQ-4, REQ-5, REQ-10
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/home-charts.js`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Datos en `<script type="application/json" id="monthly-chart-data|monthly-sales-daily-line-data|monthly-sales-pie-data">` (`partials/home_charts.html`). `window.Chart` es un script clásico del `<head>`: ya existe al evaluar el módulo. Antes el tooltip hacía `innerHTML` con la etiqueta `SKU - nombre` (XSS al pasar el ratón).

#### Objective

Mover a módulo los 3 gráficos de Inicio, sin `setDebug`/`#*-debug`/sondeo de `window.Chart`, y con el tooltip del pastel construido con `textContent`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/home-charts.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Gráficos de Inicio (partials/home_charts.html): barras mensuales, línea de ventas diarias y
// pastel de ventas por artículo. Los datos llegan en <script type="application/json">.
// Chart.js es un script clásico del <head>: ya está cargado cuando se ejecutan los módulos.
import { onContent } from '../core/content.js';

const charts = { monthly: null, dailyLine: null, pie: null };

const PIE_COLORS = [
  'rgba(11,45,66,0.85)',
  'rgba(203, 58, 66, 0.75)',
  'rgba(16, 185, 129, 0.75)',
  'rgba(59, 130, 246, 0.75)',
  'rgba(245, 158, 11, 0.75)',
  'rgba(99, 102, 241, 0.75)',
  'rgba(236, 72, 153, 0.75)',
  'rgba(34, 197, 94, 0.65)',
  'rgba(148, 163, 184, 0.75)',
  'rgba(15, 118, 110, 0.70)',
  'rgba(107, 114, 128, 0.55)',
];

function find(container, selector) {
  return container.querySelector(selector) || document.querySelector(selector);
}

function readPayload(container, selector) {
  const dataEl = find(container, selector);
  try {
    return JSON.parse((dataEl && dataEl.textContent) ? dataEl.textContent : '{}');
  } catch (e) {
    return null;
  }
}

function list(payload, key) {
  return Array.isArray(payload[key]) ? payload[key] : [];
}

function replaceChart(key, canvas, config) {
  if (charts[key]) {
    try { charts[key].destroy(); } catch (e) {}
  }
  charts[key] = new window.Chart(canvas.getContext('2d'), config);
}

function renderMonthlyChart(container) {
  const el = container.querySelector('#monthly-chart');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-chart-data');
  if (!payload) return;
  replaceChart('monthly', el, {
    type: 'bar',
    data: {
      labels: list(payload, 'labels'),
      datasets: [
        { label: 'Ventas', data: list(payload, 'sales'), backgroundColor: 'rgba(11,45,66,0.85)' },
        { label: 'Compras', data: list(payload, 'purchases'), backgroundColor: 'rgba(203, 58, 66, 0.75)' },
        { label: 'Utilidad', data: list(payload, 'profit'), backgroundColor: 'rgba(16, 185, 129, 0.75)' },
      ],
    },
    options: {
      responsive: true,
      plugins: { legend: { position: 'bottom' } },
      scales: { x: { stacked: false }, y: { beginAtZero: true } },
    },
  });
}

function renderDailyLineChart(container) {
  const el = container.querySelector('#monthly-sales-daily-line');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-sales-daily-line-data');
  if (!payload) return;
  replaceChart('dailyLine', el, {
    type: 'line',
    data: {
      labels: list(payload, 'labels'),
      datasets: [
        {
          label: 'Ventas por día (€)',
          data: list(payload, 'sales'),
          borderColor: 'rgba(11,45,66,0.85)',
          backgroundColor: 'rgba(11,45,66,0.10)',
          fill: true,
          tension: 0.25,
          pointRadius: 2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 } },
        y: { beginAtZero: true },
      },
    },
  });
}

// Tooltip del pastel construido con textContent: la etiqueta es "SKU - nombre de producto"
// (dato de usuario). Antes se metía en innerHTML sin escapar (XSS al pasar el ratón).
function fillPieTooltip(box, label, qty, value, total, pct) {
  const lines = [
    [label, true],
    [`Cantidad vendida: ${qty.toFixed(2)}`, false],
    [`Venta del artículo: ${value.toFixed(2)} €`, false],
    [`Venta total mes: ${total.toFixed(2)} €`, false],
    [`Porcentaje: ${pct.toFixed(1)}%`, false],
  ];
  box.replaceChildren(...lines.map(([text, title], i) => {
    const div = document.createElement('div');
    div.textContent = text;
    if (title) {
      div.style.fontWeight = '700';
      div.style.marginBottom = '6px';
    } else {
      div.className = 'muted';
      if (i < lines.length - 1) div.style.marginBottom = '4px';
    }
    return div;
  }));
}

function renderPieChart(container) {
  const el = container.querySelector('#monthly-sales-pie');
  if (!el || !window.Chart) return;
  const payload = readPayload(container, '#monthly-sales-pie-data');
  if (!payload) return;

  const wrap = find(container, '#monthly-sales-pie-wrap');
  const tooltipBox = find(container, '#monthly-sales-pie-tooltip');
  const lineSvg = find(container, '#monthly-sales-pie-line');
  const lineEl = find(container, '#monthly-sales-pie-line-el');

  const labels = list(payload, 'labels');
  const values = list(payload, 'values');
  const qtys = list(payload, 'qtys');
  const total = values.reduce((a, b) => a + (Number(b) || 0), 0);

  const external = (context) => {
    const chart = context.chart;
    const tt = context.tooltip;
    if (!wrap || !tooltipBox || !lineSvg || !lineEl) return;
    if (!tt || tt.opacity === 0) {
      lineSvg.style.display = 'none';
      return;
    }
    const idx = (tt.dataPoints && tt.dataPoints.length) ? tt.dataPoints[0].dataIndex : 0;
    const v = Number(values[idx] || 0);
    fillPieTooltip(tooltipBox, String(labels[idx] || ''), Number(qtys[idx] || 0), v, total,
      total ? (v / total * 100) : 0);

    const wrapRect = wrap.getBoundingClientRect();
    const canvasRect = el.getBoundingClientRect();
    const boxRect = tooltipBox.getBoundingClientRect();
    lineEl.setAttribute('x1', String((canvasRect.left - wrapRect.left) + (tt.caretX || 0)));
    lineEl.setAttribute('y1', String((canvasRect.top - wrapRect.top) + (tt.caretY || 0)));
    lineEl.setAttribute('x2', String(boxRect.left - wrapRect.left));
    lineEl.setAttribute('y2', String((boxRect.top - wrapRect.top) + (boxRect.height / 2)));
    lineSvg.style.display = 'block';
    chart.draw();
  };

  replaceChart('pie', el, {
    type: 'pie',
    data: { labels, datasets: [{ data: values, backgroundColor: PIE_COLORS }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false, external } },
    },
  });
}

function enhance(root, fromSwap) {
  const container = root || document;
  renderMonthlyChart(container);
  renderPieChart(container);
  renderDailyLineChart(container);
  if (!fromSwap) return;
  [charts.dailyLine, charts.pie, charts.monthly].forEach((chart) => {
    try { if (chart && chart.resize) chart.resize(); } catch (e) {}
  });
}

export function init() {
  onContent(enhance);
}
````

#### Verification

```bash
node --check app/static/js/components/home-charts.js && ! grep -ni 'debug\|innerHTML' app/static/js/components/home-charts.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/components/home-charts.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T9: `components/widget-chart.js`

**Lote:** C · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/components/widget-chart.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

El canvas pasará a llevar `data-js-widget-chart='{"type","title","payload":{labels,values}}'` (T20). Paleta, opciones y plugin de porcentajes idénticos al inline. Los widgets nuevos llegan por swap OOB (`#agent-grid`); `onContent` los recibe porque htmx emite `afterSwap` sobre cada elemento OOB.

#### Objective

Sustituir el `<script>` inline de `partials/widget_chart.html` por un enhancer que lee `data-js-widget-chart`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/components/widget-chart.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Widgets de gráfico del copiloto (partials/widget_chart.html):
// <canvas data-js-widget-chart='{"type":..,"title":..,"payload":{labels,values}}'>.
// Los widgets nuevos llegan en un swap OOB (#agent-grid en partials/agent_chat_turn.html):
// htmx 1.9 emite htmx:afterSwap también sobre cada elemento OOB, así que core/content.js
// los recibe como `root`.
import { onContent } from '../core/content.js';

const PALETTE_DARK = ['#60a5fa', '#f87171', '#34d399', '#fbbf24', '#a78bfa', '#22d3ee', '#94a3b8'];
const PALETTE_LIGHT = ['#0b2d42', '#cf1f27', '#2a6f97', '#4c9a8c', '#d97706', '#7c3aed', '#0891b2', '#64748b'];

// Dibuja el porcentaje sobre cada porción de pie/doughnut.
const percentPlugin = {
  id: 'percentLabels',
  afterDatasetsDraw(chart) {
    const t = chart.config.type;
    if (t !== 'pie' && t !== 'doughnut') return;
    const ctx = chart.ctx;
    const meta = chart.getDatasetMeta(0);
    const data = chart.data.datasets[0].data || [];
    const total = data.reduce((a, b) => a + (Number(b) || 0), 0);
    if (total <= 0) return;
    ctx.save();
    ctx.font = 'bold 12px sans-serif';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    meta.data.forEach((arc, i) => {
      const pct = Math.round((Number(data[i]) || 0) / total * 100);
      const mid = (arc.startAngle + arc.endAngle) / 2;
      const radius = t === 'pie' ? arc.outerRadius * 0.6 : (arc.outerRadius + arc.innerRadius) / 2;
      const x = Math.cos(mid) * radius + arc.x;
      const y = Math.sin(mid) * radius + arc.y;
      const txt = pct + '%';
      ctx.strokeStyle = 'rgba(15, 23, 42, 0.6)';
      ctx.lineWidth = 2;
      ctx.strokeText(txt, x, y);
      ctx.fillStyle = '#ffffff';
      ctx.fillText(txt, x, y);
    });
    ctx.restore();
  },
};

function chartConfig(spec) {
  const type = spec.type;
  const payload = spec.payload || {};
  const labels = payload.labels || [];
  const values = payload.values || [];

  // Paleta armónica con la identidad de la app, adaptada al tema (claro/oscuro).
  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const palette = isDark ? PALETTE_DARK : PALETTE_LIGHT;
  const accent = isDark ? '#93c5fd' : '#0b2d42';
  const accentFill = isDark ? 'rgba(147, 197, 253, 0.12)' : 'rgba(11, 45, 66, 0.08)';

  let backgroundColor;
  let borderColor;
  if (type === 'pie' || type === 'doughnut') {
    backgroundColor = labels.map((_, i) => palette[i % palette.length]);
    borderColor = '#ffffff';
  } else if (type === 'bar') {
    backgroundColor = labels.map((_, i) => palette[i % palette.length]);
    borderColor = backgroundColor;
  } else { // line / radar
    backgroundColor = accentFill;
    borderColor = accent;
  }

  const opts = { responsive: true, maintainAspectRatio: false };
  if (type === 'bar' || type === 'line') {
    opts.scales = {
      y: { beginAtZero: true, grid: { color: 'rgba(15, 23, 42, 0.06)' } },
      x: { grid: { display: false } },
    };
    opts.plugins = { legend: { display: false } };
  } else if (type === 'pie' || type === 'doughnut') {
    opts.plugins = { legend: { position: 'right', labels: { boxWidth: 12, font: { size: 11 } } } };
  }

  return {
    type,
    data: {
      labels,
      datasets: [{
        label: spec.title,
        data: values,
        backgroundColor,
        borderColor,
        borderWidth: 1,
        tension: 0.3,
      }],
    },
    options: opts,
    plugins: [percentPlugin],
  };
}

function renderWidgetCharts(root) {
  if (!window.Chart || !root || typeof root.querySelectorAll !== 'function') return;
  const canvases = Array.from(root.querySelectorAll('canvas[data-js-widget-chart]'));
  if (root.matches && root.matches('canvas[data-js-widget-chart]')) canvases.push(root);
  canvases.forEach((el) => {
    let spec;
    try {
      spec = JSON.parse(el.getAttribute('data-js-widget-chart') || '{}');
    } catch (e) {
      return;
    }
    // Destruir la instancia previa del canvas al re-renderizar.
    const prev = window.Chart.getChart(el);
    if (prev) prev.destroy();
    new window.Chart(el, chartConfig(spec));
  });
}

export function init() {
  onContent(renderWidgetCharts);
}
````

#### Verification

```bash
node --check app/static/js/components/widget-chart.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/components/widget-chart.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T10: `features/transfers.js`

**Lote:** C · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/features/transfers.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Código de `app.js` (`onTransferProductChange`, `onTransferAddLine`, `onTransferRemoveLine`, `onTransferToggleEdit`, `_toggleBulkRow`, `onTransferBulkCheckboxChange`, `onTransferBulkSelectAll`, `onTransferBulkSearch`, listener `transferBulkSuccess`) sin cambios. Se conservan `data-transfer-*`, `#transfer-bulk-*`, `.transfer-bulk-*`, `data-search`.

#### Objective

Mover a módulo la lógica de traspasos (líneas, stock, modal masivo, recarga tras `transferBulkSuccess`).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/features/transfers.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Traspasos: líneas dinámicas del formulario, stock disponible al elegir producto y el modal
// de traspaso masivo (filtro, selección, recarga de la pestaña tras el éxito).
function onTransferProductChange(evt) {
  const input = evt.target;
  if (!input || input.name !== 'product') return;

  const row = input.closest('tr');
  if (!row) return;

  const qtyInput = row.querySelector('input[name="quantity"]');
  if (!qtyInput) return;

  let sku = (input.value || '').trim();
  if (sku.includes(' - ')) {
    sku = sku.split(' - ')[0].trim();
  }
  if (!sku) return;

  const form = input.closest('form');
  let fromCode = '';
  if (form) {
    const fromSel = form.querySelector('select[name="from_location_code"]');
    if (fromSel) {
      fromCode = (fromSel.value || '').trim();
    }
  }

  let url = `/ui/transfers/stock/${encodeURIComponent(sku)}`;
  if (fromCode) {
    url = url + `?location_code=${encodeURIComponent(fromCode)}`;
  }

  fetch(url)
    .then((res) => res.text())
    .then((stock) => {
      qtyInput.value = stock || '0';
    })
    .catch(() => {
      qtyInput.value = '0';
    });
}

function onTransferAddLine(evt) {
  const btn = evt.target.closest('[data-transfer-add-line]');
  if (!btn) return;
  evt.preventDefault();

  const container = btn.closest('form') || document;
  const tbody = container.querySelector('#transfer-lines');
  const tmpl = container.querySelector('#transfer-line-template');
  if (!tbody || !tmpl) return;
  const row = tmpl.content.firstElementChild;
  if (!row) return;
  const clone = row.cloneNode(true);
  tbody.appendChild(clone);
  const firstInput = clone.querySelector('input[name="product"]');
  if (firstInput) {
    try { firstInput.focus(); } catch (e) {}
  }
}

function onTransferRemoveLine(evt) {
  const btn = evt.target.closest('[data-transfer-remove-line]');
  if (!btn) return;
  evt.preventDefault();

  const row = btn.closest('tr');
  if (!row) return;
  const tbody = row.parentElement;
  if (!tbody) return;

  const rows = tbody.querySelectorAll('tr');
  if (rows.length <= 1) {
    const prod = row.querySelector('input[name="product"]');
    const qty = row.querySelector('input[name="quantity"]');
    if (prod) prod.value = '';
    if (qty) qty.value = '';
    return;
  }
  row.remove();
}

function onTransferToggleEdit(evt) {
  const btn = evt.target.closest('[data-transfer-toggle-edit]');
  if (!btn) return;
  evt.preventDefault();

  const row = btn.closest('tr');
  if (!row) return;

  const fields = row.querySelectorAll('input[name="product"], input[name="quantity"]');
  const anyEnabled = Array.from(fields).some((el) => !el.disabled);
  if (anyEnabled) {
    fields.forEach((el) => { el.disabled = true; });
    btn.textContent = 'Editar';
  } else {
    fields.forEach((el) => { el.disabled = false; });
    btn.textContent = 'Bloquear';
    const first = row.querySelector('input[name="product"], input[name="quantity"]');
    if (first) {
      try { first.focus(); } catch (e) {}
    }
  }
}

function toggleBulkRow(cb, checked) {
  const row = cb.closest('tr');
  if (!row) return;
  const qty = row.querySelector('input[name="quantity"]');
  const prod = row.querySelector('input[name="product"]');
  if (qty) qty.disabled = !checked;
  if (prod) prod.disabled = !checked;
}

function onTransferBulkCheckboxChange(evt) {
  const cb = evt.target.closest('.transfer-bulk-check');
  if (!cb) return;
  toggleBulkRow(cb, cb.checked);
}

function onTransferBulkSelectAll(evt) {
  const master = evt.target.closest('#transfer-bulk-select-all');
  if (!master) return;
  const modal = master.closest('.modal-overlay');
  if (!modal) return;
  modal.querySelectorAll('.transfer-bulk-check').forEach((cb) => {
    const row = cb.closest('tr');
    if (row && row.style.display === 'none') return;
    cb.checked = master.checked;
    toggleBulkRow(cb, master.checked);
  });
}

function onTransferBulkSearch(evt) {
  const input = evt.target.closest('#transfer-bulk-search');
  if (!input) return;
  const q = (input.value || '').toLowerCase();
  const modal = input.closest('.modal-overlay');
  if (!modal) return;
  modal.querySelectorAll('.transfer-bulk-row').forEach((row) => {
    const hay = (row.getAttribute('data-search') || '').indexOf(q) !== -1;
    row.style.display = hay ? '' : 'none';
  });
}

export function init() {
  document.addEventListener('click', onTransferAddLine);
  document.addEventListener('click', onTransferRemoveLine);
  document.addEventListener('click', onTransferToggleEdit);
  document.addEventListener('change', onTransferProductChange);
  document.addEventListener('change', onTransferBulkCheckboxChange);
  document.addEventListener('change', onTransferBulkSelectAll);
  document.addEventListener('input', onTransferBulkSearch);
  document.addEventListener('transferBulkSuccess', () => {
    try {
      window.htmx.ajax('GET', '/ui/tabs/transfers', { target: '#tab-content', swap: 'innerHTML' });
    } catch (e) {}
  });
}
````

#### Verification

```bash
node --check app/static/js/features/transfers.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/features/transfers.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T11: `features/agent-chat.js` y `features/theme.js`

**Lote:** D · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/features/agent-chat.js`, `app/static/js/features/theme.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`dashboard.html` usará `data-js-agent-toggle` (burbuja y ✕), `data-js-agent-size` (⤢) y `data-js-theme-toggle` (🌓) en lugar de `onclick="toggleAgentPanel()|cycleAgentPanelSize()|toggleTheme()"`. El chat solo existe para admin/owner fuera de modo desktop: `init()` no hace nada si no hay `#agent-panel`.

#### Objective

Sustituir el `<script>` inline de `dashboard.html` (chat del copiloto y tema) por dos módulos.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/features/agent-chat.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Panel de chat del copiloto (dashboard.html, solo admin/owner y modo no desktop):
// abrir/cerrar ([data-js-agent-toggle]), tamaño ([data-js-agent-size]), envío optimista
// y visibilidad de la burbuja solo en la pestaña "Dashboard Dinámico".
const SIZES = ['small', 'medium', 'large'];

function togglePanel() {
  const panel = document.getElementById('agent-panel');
  if (panel) panel.classList.toggle('hidden');
}

function cyclePanelSize() {
  const panel = document.getElementById('agent-panel');
  if (!panel) return;
  let idx = SIZES.indexOf(panel.dataset.size || 'medium');
  idx = (idx + 1) % SIZES.length;
  panel.dataset.size = SIZES[idx];
  panel.classList.remove('small', 'medium', 'large');
  panel.classList.add(SIZES[idx]);
}

// Chat optimista: el mensaje del usuario aparece al instante + indicador "escribiendo…".
function appendBubble(role, text) {
  const thread = document.getElementById('agent-chat-thread');
  if (!thread) return;
  const d = document.createElement('div');
  d.className = 'msg msg-' + role;
  d.textContent = text;
  thread.appendChild(d);
  thread.scrollTop = thread.scrollHeight;
}

function appendTyping() {
  const thread = document.getElementById('agent-chat-thread');
  if (!thread || document.getElementById('agent-typing')) return;
  const d = document.createElement('div');
  d.id = 'agent-typing';
  d.className = 'msg msg-assistant agent-typing';
  d.innerHTML = '<span class="dot"></span><span class="dot"></span><span class="dot"></span>';
  thread.appendChild(d);
  thread.scrollTop = thread.scrollHeight;
}

function initForm() {
  // Enviar el chat: el form usa hx-post (htmx garantiza el envío). Este listener hace el
  // append optimista + indicador, y limpia el input en un microtask para no interferir con
  // la lectura del form por htmx.
  const form = document.getElementById('agent-chat-form');
  if (!form) return;
  const input = form.querySelector('textarea[name=message]');
  form.addEventListener('submit', () => {
    const msg = (input && input.value || '').trim();
    if (!msg) return;
    appendBubble('user', msg); // el mensaje va al historial YA
    appendTyping(); // indicador "escribiendo…"
    setTimeout(() => { if (input) input.value = ''; }, 0);
  }, true); // capture: corre antes de que htmx lea el formulario
  // Enter envía; Shift+Enter inserta salto de línea (textarea multilínea).
  if (input) {
    input.addEventListener('keydown', (evt) => {
      if (evt.key === 'Enter' && !evt.shiftKey) {
        evt.preventDefault();
        form.requestSubmit();
      }
    });
  }
}

export function init() {
  if (!document.getElementById('agent-panel')) return;
  initForm();

  document.addEventListener('click', (evt) => {
    if (evt.target.closest('[data-js-agent-toggle]')) togglePanel();
    else if (evt.target.closest('[data-js-agent-size]')) cyclePanelSize();
  });

  // Al terminar la respuesta: quitar el indicador; si fue un error (4xx), htmx no swapea
  // >=400, así que insertamos el reply de error a mano.
  document.body.addEventListener('htmx:afterRequest', (evt) => {
    const cfg = evt.detail.requestConfig || {};
    if (cfg.path !== '/ui/agent/chat') return;
    const typing = document.getElementById('agent-typing');
    if (typing) typing.remove();
    const xhr = evt.detail.xhr;
    if (xhr && xhr.status >= 400 && xhr.responseText) {
      const thread = document.getElementById('agent-chat-thread');
      if (thread) thread.insertAdjacentHTML('beforeend', xhr.responseText);
    }
  });

  // Mostrar la burbuja SOLO en el Dashboard Dinámico. Solo se actualiza al cambiar de
  // pestaña (swap en #tab-content); la respuesta del chat (swap en #agent-chat-thread)
  // NO debe ocultar la burbuja.
  document.body.addEventListener('htmx:afterSwap', (evt) => {
    const target = evt.detail.target;
    if (target && target.id === 'tab-content') {
      const path = (evt.detail.requestConfig && evt.detail.requestConfig.path) || '';
      const onDashboard = (path === '/ui/tabs/agent');
      const fab = document.getElementById('agent-fab');
      if (fab) fab.style.display = onDashboard ? 'flex' : 'none';
      if (!onDashboard) {
        const panel = document.getElementById('agent-panel');
        if (panel) panel.classList.add('hidden');
      }
    }
    if (target && target.classList && target.classList.contains('agent-thread')) {
      target.scrollTop = target.scrollHeight;
    }
  });
}
````

**Step 2: escribir `app/static/js/features/theme.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Botón de tema claro/oscuro ([data-js-theme-toggle]). Recarga la página para que los
// gráficos se redibujen con la paleta del tema (la paleta CSS llega en la fase 3).
function onClick(evt) {
  if (!evt.target.closest('[data-js-theme-toggle]')) return;
  const root = document.documentElement;
  const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  try { localStorage.setItem('gi-theme', next); } catch (e) {}
  location.reload();
}

export function init() {
  document.addEventListener('click', onClick);
}
````

#### Verification

```bash
node --check app/static/js/features/agent-chat.js && node --check app/static/js/features/theme.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/features/agent-chat.js`, `app/static/js/features/theme.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T12: `main.js` (entrada) y `pages/login.js`

**Lote:** D · **REQs:** REQ-3, REQ-4
**Depends on:** T4, T5, T6, T7, T8, T9, T10, T11
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/main.js`, `app/static/js/pages/login.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Todos los módulos exportan `init()`. Orden de enhancers = el de `runEnhancers` de `app.js` (barcode → gráficos → notas). `pages/login.js` sustituye el `<script>` inline de `login.html` (casilla «Mostrar contraseña»).

#### Objective

Crear el punto de entrada que importa e inicializa todos los módulos y el módulo del login.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/main.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Punto de entrada de la app (layouts/app_shell.html, <script type="module">).
// Cada módulo registra sus listeners en init(); el orden de los enhancers de contenido
// (barcode, home-charts, notes) es el mismo que tenía el antiguo app.js.
import { init as initContent } from './core/content.js';
import { init as initActivity } from './core/activity.js';
import { init as initTabs } from './core/tabs.js';
import { init as initModals } from './core/modals.js';
import { init as initSwap } from './core/swap.js';
import { init as initBarcode } from './components/barcode.js';
import { init as initHomeCharts } from './components/home-charts.js';
import { init as initNotes } from './components/notes.js';
import { init as initWidgetChart } from './components/widget-chart.js';
import { init as initProductSuggest } from './components/product-suggest.js';
import { init as initFormHelpers } from './components/form-helpers.js';
import { init as initTransfers } from './features/transfers.js';
import { init as initTheme } from './features/theme.js';
import { init as initAgentChat } from './features/agent-chat.js';

initContent();
initActivity();
initTabs();
initModals();
initSwap();
initBarcode();
initHomeCharts();
initNotes();
initWidgetChart();
initProductSuggest();
initFormHelpers();
initTransfers();
initTheme();
initAgentChat();
````

**Step 2: escribir `app/static/js/pages/login.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Login: casilla "Mostrar contraseña".
const cb = document.getElementById('login-show-password');
const pw = document.getElementById('login-password');
if (cb && pw) {
  cb.addEventListener('change', () => {
    pw.type = cb.checked ? 'text' : 'password';
  });
}
````

#### Verification

```bash
node --check app/static/js/main.js && node --check app/static/js/pages/login.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/main.js`, `app/static/js/pages/login.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T13: `pages/print.js` y `pages/label-print.js`

**Lote:** D · **REQs:** REQ-3, REQ-4
**Depends on:** T3
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/pages/print.js`, `app/static/js/pages/label-print.js`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`label_single_print.html` pasará SKU y nombre en `data-js-label-sku`/`data-js-label-name` de `#sheet` (antes: `tojson` dentro del `<script>` inline). `JsBarcode` es un script clásico del `<head>` de esa página.

#### Objective

Módulos de las páginas de impresión: botón `[data-js-print]` y la etiqueta individual (copias, CODE128, `auto_print=1`).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/static/js/pages/print.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Páginas de impresión (traspaso, reposición): botón [data-js-print].
document.addEventListener('click', (evt) => {
  if (evt.target.closest('[data-js-print]')) window.print();
});
````

**Step 2: escribir `app/static/js/pages/label-print.js` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````javascript
// Etiqueta individual (label_single_print.html): N copias de nombre + código de barras
// CODE128 del SKU. SKU y nombre llegan en data-js-label-sku / data-js-label-name de #sheet.
const sheet = document.getElementById('sheet');
const copiesInput = document.getElementById('copies');
const sku = (sheet && sheet.dataset.jsLabelSku) || '';
const name = (sheet && sheet.dataset.jsLabelName) || '';

function makeLabelEl() {
  const page = document.createElement('div');
  page.className = 'page';
  const wrap = document.createElement('div');
  wrap.className = 'label';
  const nameEl = document.createElement('div');
  nameEl.className = 'name';
  nameEl.textContent = name;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('class', 'barcode');
  svg.setAttribute('data-code', sku);
  wrap.appendChild(nameEl);
  wrap.appendChild(svg);
  page.appendChild(wrap);
  return page;
}

function render() {
  const n = Math.max(1, parseInt(copiesInput.value || '1', 10) || 1);
  sheet.innerHTML = '';
  for (let i = 0; i < n; i++) {
    sheet.appendChild(makeLabelEl());
  }
  if (window.JsBarcode) {
    sheet.querySelectorAll('svg.barcode[data-code]').forEach((svg) => {
      try {
        window.JsBarcode(svg, sku, {
          format: 'CODE128',
          displayValue: true,
          fontSize: 12,
          textMargin: 0,
          height: 40,
          margin: 0,
          width: 1.2,
        });
      } catch (e) {}
    });
  }
}

if (sheet && copiesInput) {
  copiesInput.addEventListener('change', render);
  render();

  const params = new URLSearchParams(window.location.search || '');
  if ((params.get('auto_print') || '') === '1') {
    setTimeout(() => {
      try { window.print(); } catch (e) {}
    }, 200);
  }

  document.getElementById('btn-print').addEventListener('click', () => {
    render();
    setTimeout(() => {
      try { window.print(); } catch (e) {}
    }, 100);
  });
}
````

#### Verification

```bash
node --check app/static/js/pages/print.js && node --check app/static/js/pages/label-print.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/pages/print.js`, `app/static/js/pages/label-print.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T14: `static_version()` con el hash de todos los `.css`/`.js`

**Lote:** D · **REQs:** REQ-7
**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `app/routers/ui_common.py`, `tests/unit/test_ui_regressions.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`_static_asset_version() -> str` (`app/routers/ui_common.py`) hoy calcula el hash de 5 ficheros fijos, incluido `js/app.js`; se registra como global Jinja `static_version` (se llama sin argumentos). El parámetro nuevo `static_dir` solo lo usa el test.

#### Objective

Que el `?v=` cambie si cambia cualquier módulo (también los anidados que `main.js` importa sin `?v=`), y que el test no dependa de `app.js`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

P = "app/routers/ui_common.py"
edit(P, "def _static_asset_version() -> str:", "def _static_asset_version(static_dir: Path | None = None) -> str:")
edit(P, "solo vigila ``*.py``, así que editar ``app.css``/``app.js`` no reinicia el",
     "solo vigila ``*.py``, así que editar un ``.css``/``.js`` no reinicia el")
edit(P, """    digest = hashlib.sha1()
    static_dir = Path(__file__).resolve().parent.parent / "static"
    for rel in ("css/app.css", "js/app.js", "js/htmx.min.js", "js/chart.umd.min.js", "js/JsBarcode.all.min.js"):
        try:
            digest.update((static_dir / rel).read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:8]""", """    digest = hashlib.sha1()
    root = static_dir or Path(__file__).resolve().parent.parent / "static"
    # Todos los .css/.js (también los módulos de js/core, js/components...): el hash cambia
    # si cambia cualquiera, aunque main.js no cambie. La ruta entra en el hash (renombrar
    # un módulo también invalida la caché).
    files = sorted(p for sub in ("css", "js") for p in (root / sub).rglob("*") if p.suffix in (".css", ".js"))
    for path in files:
        try:
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(path.read_bytes())
        except OSError:
            continue
    return digest.hexdigest()[:8]""")
T = "tests/unit/test_ui_regressions.py"
edit(T, "import hashlib\nfrom pathlib import Path\n\n", "")
edit(T, '''def test_static_asset_version_derives_from_file_content():
    """H-060: el hash sale del contenido real de los assets (no una constante)."""
    static_dir = Path(ui_common.__file__).resolve().parent.parent / "static"
    css = (static_dir / "css/app.css").read_bytes()
    js = (static_dir / "js/app.js").read_bytes()
    htmx = (static_dir / "js/htmx.min.js").read_bytes()
    chart = (static_dir / "js/chart.umd.min.js").read_bytes()
    barcode = (static_dir / "js/JsBarcode.all.min.js").read_bytes()
    expected = hashlib.sha1(css + js + htmx + chart + barcode).hexdigest()[:8]
    assert ui_common._static_asset_version() == expected
''', '''def test_static_asset_version_changes_with_any_nested_module(tmp_path):
    """H-060 + fase 2: el hash cambia si cambia CUALQUIER .css/.js, también un módulo anidado
    (los que importa main.js no llevan ``?v=`` propio)."""
    (tmp_path / "css").mkdir()
    (tmp_path / "css" / "app.css").write_text("body{}")
    (tmp_path / "js" / "features").mkdir(parents=True)
    (tmp_path / "js" / "main.js").write_text("import './features/x.js';")
    module = tmp_path / "js" / "features" / "x.js"
    module.write_text("export const a = 1;")
    before = ui_common._static_asset_version(tmp_path)
    assert before == ui_common._static_asset_version(tmp_path)
    module.write_text("export const a = 2;")
    assert ui_common._static_asset_version(tmp_path) != before
    module.rename(tmp_path / "js" / "features" / "y.js")
    assert ui_common._static_asset_version(tmp_path) != before
''')
print("OK")
PY
````

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_regressions.py   # PASS
```

#### Expected Report

- [ ] `app/routers/ui_common.py`, `tests/unit/test_ui_regressions.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T15: `layouts/app_shell.html`: `theme-init.js` + `main.js` como módulo

**Lote:** E · **REQs:** REQ-1, REQ-3
**Depends on:** T12, T14
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/layouts/app_shell.html`, `tests/unit/test_ui_components.py`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Los scripts clásicos (htmx, Chart.js, JsBarcode) siguen igual y antes que el módulo. `test_app_shell_loads_all_app_assets` comprueba `APP_ASSETS`.

#### Objective

El layout carga `theme-init.js` (clásico, síncrono) y `main.js` (`type="module"`) en lugar del inline de tema y de `app.js`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

P = "app/templates/layouts/app_shell.html"
edit(P, "{# Layout de la app (dashboard): tema + htmx, Chart.js, JsBarcode, app.css, app.js. #}",
     "{# Layout de la app (dashboard): tema (theme-init.js) + htmx, Chart.js, JsBarcode, app.css y main.js (módulo). #}")
edit(P, """    <script>
      // Aplicar el tema guardado ANTES del paint (evita flash de color).
      (function () {
        try {
          var t = localStorage.getItem("gi-theme") || "light";
          document.documentElement.setAttribute("data-theme", t);
        } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
      })();
    </script>
""", """    <script src="/static/js/theme-init.js?v={{ static_version() }}"></script>
""")
edit(P, '<script src="/static/js/app.js?v={{ static_version() }}" defer></script>',
     '<script type="module" src="/static/js/main.js?v={{ static_version() }}"></script>')
edit("tests/unit/test_ui_components.py",
     'APP_ASSETS = ("htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "app.js")',
     'APP_ASSETS = ("theme-init.js", "htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "main.js")')
print("OK")
PY
````

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_ui_components.py   # PASS
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/layouts/app_shell.html`, `tests/unit/test_ui_components.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T16: Borrar `app/static/js/app.js`

**Lote:** E · **REQs:** REQ-3, REQ-5
**Depends on:** T15
**Skill:** ninguna
**Agent:** coder
**Files:** `app/static/js/app.js`
**Pillars:** P1, P4, P6
**Tools (cc_local):** Bash (script literal) · **Skills:** ninguna

#### Context

Tras T15 ninguna plantilla carga `app.js`.

#### Objective

Eliminar `app.js` (todo su código vive ya en los módulos; `debugReturnLots` se elimina).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
set -euo pipefail
git rm -q app/static/js/app.js
# Nadie debe seguir cargando app.js (las plantillas usan main.js / pages/*.js).
! grep -rn 'static/js/app\.js' app/templates app/routers
````

#### Verification

```bash
test ! -e app/static/js/app.js && echo OK
```

#### Expected Report

- [ ] `app/static/js/app.js` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T17: `dashboard.html`: `data-js-*` y fuera el `<script>` inline

**Lote:** E · **REQs:** REQ-1, REQ-4, REQ-6
**Depends on:** T11, T15
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/dashboard.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Ids que NO cambian: `agent-fab`, `agent-panel`, `agent-chat-thread`, `agent-chat-form`, `tab-*`, `tab-content`, `modal-container`, `sidebar`, `mobile-menu-*`.

#### Objective

Sustituir los 5 handlers inline por `data-js-*` y borrar el bloque `<script>` del chat (~100 líneas).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

P = "app/templates/dashboard.html"
edit(P, '<select name="business_id" onchange="this.form.submit()" ', '<select name="business_id" data-js-autosubmit ')
edit(P, 'class="sidebar-theme" onclick="toggleTheme()" ', 'class="sidebar-theme" data-js-theme-toggle ')
edit(P, 'class="agent-fab" onclick="toggleAgentPanel()" ', 'class="agent-fab" data-js-agent-toggle ')
edit(P, 'class="agent-panel-size" onclick="cycleAgentPanelSize()" ', 'class="agent-panel-size" data-js-agent-size ')
edit(P, 'class="agent-panel-close" onclick="toggleAgentPanel()" ', 'class="agent-panel-close" data-js-agent-toggle ')
# El bloque <script> del chat (~100 líneas) pasa a static/js/features/agent-chat.js y features/theme.js.
cut(P, "    <script>\n      function toggleAgentPanel()", "    </script>\n")
print("OK")
PY
````

#### Verification

```bash
! grep -nE '<script>|on(click|change)=' app/templates/dashboard.html && echo OK
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/dashboard.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T18: `login.html` y `label_single_print.html` con módulo de página

**Lote:** E · **REQs:** REQ-1, REQ-4, REQ-6
**Depends on:** T12, T13
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/login.html`, `app/templates/label_single_print.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`label_single_print.html` sigue cargando `JsBarcode.all.min.js` en su `{% block head %}`. Los ids `login-password`, `login-show-password`, `copies`, `btn-print`, `sheet` no cambian.

#### Objective

Quitar los `<script>` inline y cargar `pages/login.js` / `pages/label-print.js`; SKU y nombre de la etiqueta por `data-js-*` (autoescapados).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

L = "app/templates/login.html"
cut(L, "      <script>\n", "      </script>\n")
edit(L, "    </div>\n{% endblock %}",
     '    </div>\n    <script type="module" src="/static/js/pages/login.js?v={{ static_version() }}"></script>\n{% endblock %}')
P = "app/templates/label_single_print.html"
cut(P, "    <script>\n      (function(){", "    </script>\n")
edit(P, '<div class="sheet" id="sheet"></div>',
     '<div class="sheet" id="sheet" data-js-label-sku="{{ label.sku or \'\' }}" data-js-label-name="{{ label.name or \'\' }}"></div>\n'
     '    <script type="module" src="/static/js/pages/label-print.js?v={{ static_version() }}"></script>')
print("OK")
PY
````

#### Verification

```bash
! grep -n '<script>' app/templates/login.html app/templates/label_single_print.html && echo OK
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/login.html`, `app/templates/label_single_print.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T19: Impresión de traspaso y reposición: `data-js-print` + `pages/print.js`

**Lote:** E · **REQs:** REQ-1, REQ-4
**Depends on:** T13
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/restock_print.html`, `app/templates/transfer_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

Ambas plantillas extienden `layouts/print.html` y declaran su `<style>` en `{% block head %}`.

#### Objective

Sustituir `onclick="window.print()"` por `data-js-print` y cargar `pages/print.js` (solo eso: las impresiones no cargan htmx/Chart/app).

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

for P in ("app/templates/restock_print.html", "app/templates/transfer_print.html"):
    edit(P, '<button type="button" onclick="window.print()">Imprimir</button>', '<button type="button" data-js-print>Imprimir</button>')
    edit(P, "    </style>\n{% endblock %}",
         '    </style>\n    <script type="module" src="/static/js/pages/print.js?v={{ static_version() }}"></script>\n{% endblock %}')
print("OK")
PY
````

#### Verification

```bash
! grep -n 'onclick' app/templates/restock_print.html app/templates/transfer_print.html && echo OK
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/restock_print.html`, `app/templates/transfer_print.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T20: `widget_chart.html` con `data-js-widget-chart` y `home_charts.html` sin `#*-debug`

**Lote:** F · **REQs:** REQ-1, REQ-5, REQ-6
**Depends on:** T8, T9
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/widget_chart.html`, `app/templates/partials/home_charts.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Write (widget_chart literal), Bash (script de home_charts + verificación) · **Skills:** ninguna

#### Context

`w` = `{id, type, title, payload}` (`app.agent.widgets.widgets_with_payload`). Ids `chart-{{ w.id }}`, `monthly-*` (salvo los `-debug`) no cambian.

#### Objective

El widget de gráfico pasa su especificación en un atributo (comillas SIMPLES: `tojson` escapa `< > & '` pero no `"`); fuera los 3 `<div id="…-debug">`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `app/templates/partials/widget_chart.html` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````html+jinja
{# Gráfico de un widget del copiloto: lo dibuja static/js/components/widget-chart.js.
   Comillas simples en el atributo: tojson escapa < > & ' pero no ". #}
<div class="widget-chart-wrap">
  <canvas id="chart-{{ w.id }}" role="img" aria-label="{{ w.title }}"
          data-js-widget-chart='{{ {"type": w.type, "title": w.title, "payload": (w.payload or {})} | tojson }}'></canvas>
</div>
````

**Step 2: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

P = "app/templates/partials/home_charts.html"
for line in ('      <div id="monthly-sales-daily-line-debug" class="muted mt-8"></div>\n',
             '  <div id="monthly-sales-pie-debug" class="muted mt-8"></div>\n',
             '  <div id="monthly-chart-debug" class="muted mt-8"></div>\n'):
    edit(P, line, "")
print("OK")
PY
````

#### Verification

```bash
! grep -n '<script>\|-debug' app/templates/partials/widget_chart.html app/templates/partials/home_charts.html && echo OK
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/partials/widget_chart.html`, `app/templates/partials/home_charts.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T21: «Seleccionar todo» (1/3): `purchases_history` y `sales_history`

**Lote:** F · **REQs:** REQ-1, REQ-4
**Depends on:** T7
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/purchases_history.html`, `app/templates/partials/sales_history.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

El `name` es el de las casillas de fila del mismo form (`movement_ids` / `sale_ids`).

#### Objective

`onclick` de la casilla maestra → `data-js-select-all="<name>"`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("app/templates/partials/purchases_history.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=movement_ids]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="movement_ids"')
edit("app/templates/partials/sales_history.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=sale_ids]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="sale_ids"')
print("OK")
PY
````

#### Verification

```bash
! grep -n 'onclick' app/templates/partials/purchases_history.html app/templates/partials/sales_history.html && echo OK
```

#### Expected Report

- [ ] `app/templates/partials/purchases_history.html`, `app/templates/partials/sales_history.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T22: «Seleccionar todo» (2/3): `stock_table` y `tab_history`

**Lote:** F · **REQs:** REQ-1, REQ-4
**Depends on:** T7
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/stock_table.html`, `app/templates/partials/tab_history.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`skus` (stock) / `movement_ids` (historial).

#### Objective

`onclick` de la casilla maestra → `data-js-select-all="<name>"`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("app/templates/partials/stock_table.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=skus]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="skus"')
edit("app/templates/partials/tab_history.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=movement_ids]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="movement_ids"')
print("OK")
PY
````

#### Verification

```bash
! grep -n 'onclick' app/templates/partials/stock_table.html app/templates/partials/tab_history.html && echo OK
```

#### Expected Report

- [ ] `app/templates/partials/stock_table.html`, `app/templates/partials/tab_history.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T23: «Seleccionar todo» (3/3): `tab_transfers` y `tab_profit_items`

**Lote:** F · **REQs:** REQ-1, REQ-4
**Depends on:** T7
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/tab_transfers.html`, `app/templates/partials/tab_profit_items.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`movement_ids` (traspasos) / `sale_ids` (utilidades por artículo).

#### Objective

`onclick` de la casilla maestra → `data-js-select-all="<name>"`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("app/templates/partials/tab_transfers.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=movement_ids]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="movement_ids"')
edit("app/templates/partials/tab_profit_items.html",
     """onclick="const f=this.closest('form'); if(!f) return; f.querySelectorAll('input[name=sale_ids]').forEach(cb=>cb.checked=this.checked);\"""",
     'data-js-select-all="sale_ids"')
print("OK")
PY
````

#### Verification

```bash
! grep -n 'onclick' app/templates/partials/tab_transfers.html app/templates/partials/tab_profit_items.html && echo OK
```

#### Expected Report

- [ ] `app/templates/partials/tab_transfers.html`, `app/templates/partials/tab_profit_items.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T24: `sale_panel` (Enter → cantidad) y `cart_add` (moneda)

**Lote:** F · **REQs:** REQ-1, REQ-4
**Depends on:** T7
**Skill:** ninguna
**Agent:** coder
**Files:** `app/templates/partials/sale_panel.html`, `app/templates/components/sales_doc/cart_add.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`#sales-doc-currency-hidden` está en `components/sales_doc/issue_form.html` (no cambia).

#### Objective

`onkeydown` → `data-js-enter-focus="quantity"`; `onchange` → `data-js-mirror-to="sales-doc-currency-hidden"`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("app/templates/partials/sale_panel.html",
     """onkeydown="if(event.key==='Enter'){event.preventDefault(); const q=this.form.querySelector('input[name=quantity]'); if(q){q.focus(); q.select && q.select();}}\"""",
     'data-js-enter-focus="quantity"')
edit("app/templates/components/sales_doc/cart_add.html",
     """onchange="document.getElementById('sales-doc-currency-hidden').value=this.value\"""",
     'data-js-mirror-to="sales-doc-currency-hidden"')
print("OK")
PY
````

#### Verification

```bash
! grep -rnE '\son[a-z]+=' app/templates && echo 'OK: 0 handlers inline'
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline   # OK: 40
```

#### Expected Report

- [ ] `app/templates/partials/sale_panel.html`, `app/templates/components/sales_doc/cart_add.html` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T25: CSP `script-src 'self'` y `Cache-Control: no-cache` en `/static/js/`

**Lote:** G · **REQs:** REQ-2, REQ-7
**Depends on:** T15-T24
**Skill:** ninguna
**Agent:** coder
**Files:** `app/main.py`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

`security_headers_middleware(request, call_next)` en `app/main.py` (H-006) pone las cabeceras a TODAS las respuestas, incluidas las de `/static` (`StaticFiles` sirve con ETag/Last-Modified → 304).

#### Objective

Quitar `'unsafe-inline'` de `script-src` (sin tocar `style-src`, fase 3) y revalidar siempre los módulos.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("app/main.py", '''    """H-006: security headers básicos (CSP, frame, sniffing, referrer)."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "''', '''    """H-006: security headers básicos (CSP, frame, sniffing, referrer).

    ``script-src 'self'`` sin ``'unsafe-inline'``: todo el JS vive en ``app/static/js/``
    (fase 2 del release 0.5.0). ``style-src`` conserva ``'unsafe-inline'`` hasta la fase 3.
    """
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if request.url.path.startswith("/static/js/"):
        # Los módulos que importa main.js no llevan ?v=: revalidar siempre (304 si no cambió)
        # para que un despliegue nunca mezcle módulos viejos y nuevos.
        response.headers["Cache-Control"] = "no-cache"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "''')
print("OK")
PY
````

#### Verification

```bash
grep -n "script-src 'self'; " app/main.py && echo OK
```

#### Expected Report

- [ ] `app/main.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T26: Unit tests de la fase 2 (guardas estáticas)

**Lote:** G · **REQs:** REQ-1, REQ-2, REQ-3, REQ-5, REQ-7, REQ-11
**Depends on:** T25
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/unit/test_frontend_js_modules.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`TestClient(app)` SIN `with` (no arranca el lifespan, que exige credenciales); el middleware corre igual. `templates` = `app.routers.ui_common.templates`.

#### Objective

Tests: CSP, `no-cache`, 0 handlers inline (plantillas y HTML en Python), scripts exactos por plantilla, grafo de módulos sin roturas ni huérfanos, sin depuración, escape de `data-js-*`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/unit/test_frontend_js_modules.py` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````python
"""Fase 2 del release 0.5.0: JS en módulos ES, sin JS inline, CSP sin 'unsafe-inline'.

Guardas estáticas (sin navegador). El comportamiento en el navegador lo cubren los e2e
``tests/e2e/test_ui_tour_csp_e2e.py`` y ``tests/e2e/test_js_behaviors_e2e.py``.
"""

from __future__ import annotations

import json
import re
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.routers.ui_common import templates

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES_DIR = ROOT / "app" / "templates"
JS_DIR = ROOT / "app" / "static" / "js"
VENDOR = {"htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js"}
# Entradas que no importa main.js: script clásico del <head> y módulos de página.
STANDALONE = {"theme-init.js", "pages/login.js", "pages/print.js", "pages/label-print.js"}

INLINE_HANDLER = re.compile(r"\son[a-z]+\s*=", re.IGNORECASE)
SCRIPT_TAG = re.compile(r"<script\b([^>]*)>", re.IGNORECASE)
ATTR = re.compile(r'([a-z-]+)="([^"]*)"')
IMPORT = re.compile(r"""^\s*import\b[^'"]*['"](\.{1,2}/[^'"]+)['"]""", re.MULTILINE)

_MAIN = ("/static/js/main.js", "module")
_PRINT = ("/static/js/pages/print.js", "module")
# Scripts ejecutables que carga cada plantilla: (ruta sin ?v=, type). Cualquier otra: ninguno.
EXPECTED_SCRIPTS = {
    "layouts/app_shell.html": [
        ("/static/js/theme-init.js", ""),
        ("/static/js/htmx.min.js", ""),
        ("/static/js/chart.umd.min.js", ""),
        ("/static/js/JsBarcode.all.min.js", ""),
        _MAIN,
    ],
    "login.html": [("/static/js/pages/login.js", "module")],
    "transfer_print.html": [_PRINT],
    "restock_print.html": [_PRINT],
    "label_single_print.html": [
        ("/static/js/JsBarcode.all.min.js", ""),
        ("/static/js/pages/label-print.js", "module"),
    ],
}


def _templates() -> list[Path]:
    return sorted(TEMPLATES_DIR.rglob("*.html"))


def _rel(path: Path) -> str:
    return path.relative_to(TEMPLATES_DIR).as_posix()


def _js_files() -> set[str]:
    return {p.relative_to(JS_DIR).as_posix() for p in JS_DIR.rglob("*.js") if p.name not in VENDOR}


def test_csp_script_src_is_self_only() -> None:
    # Sin `with`: no arranca el lifespan (seed de credenciales); el middleware corre igual.
    csp = TestClient(app).get("/health").headers["Content-Security-Policy"]
    directives = dict(d.strip().split(" ", 1) for d in csp.split(";") if d.strip())
    assert directives["script-src"] == "'self'"


def test_static_js_is_always_revalidated() -> None:
    client = TestClient(app)
    main = client.get("/static/js/main.js")
    nested = client.get("/static/js/core/content.js")
    assert main.status_code == 200 and nested.status_code == 200
    assert main.headers["Cache-Control"] == "no-cache"
    assert nested.headers["Cache-Control"] == "no-cache"


def test_no_inline_event_handlers_in_templates() -> None:
    found = {_rel(p): INLINE_HANDLER.findall(p.read_text(encoding="utf-8")) for p in _templates()}
    assert {k: v for k, v in found.items() if v} == {}


def test_no_inline_event_handlers_in_python_html() -> None:
    sources = list((ROOT / "app" / "routers").glob("*.py")) + list((ROOT / "app" / "services").glob("*.py"))
    pattern = re.compile(r"\son(click|change|input|submit|keydown|keyup|load|error|focus|blur)\s*=")
    found = {p.name: pattern.findall(p.read_text(encoding="utf-8")) for p in sources}
    assert {k: v for k, v in found.items() if v} == {}


def test_script_tags_per_template() -> None:
    actual: dict[str, list[tuple[str, str]]] = {}
    for path in _templates():
        scripts = []
        for raw in SCRIPT_TAG.findall(path.read_text(encoding="utf-8")):
            attrs = dict(ATTR.findall(raw))
            if attrs.get("type") == "application/json":
                continue  # datos, no código
            assert "src" in attrs, f"{_rel(path)}: <script> inline ejecutable"
            scripts.append((attrs["src"].split("?", 1)[0], attrs.get("type", "")))
        if scripts:
            actual[_rel(path)] = scripts
    assert actual == EXPECTED_SCRIPTS


def test_module_graph_resolves_and_has_no_orphans() -> None:
    reachable: set[str] = set()
    pending = ["main.js"]
    while pending:
        rel = pending.pop()
        if rel in reachable:
            continue
        path = JS_DIR / rel
        assert path.is_file(), f"import roto: {rel}"
        reachable.add(rel)
        for spec in IMPORT.findall(path.read_text(encoding="utf-8")):
            pending.append((path.parent / spec).resolve().relative_to(JS_DIR.resolve()).as_posix())
    for rel in STANDALONE:
        assert (JS_DIR / rel).is_file(), rel
    assert _js_files() - reachable - STANDALONE == set(), "módulos que no importa nadie"
    assert "app.js" not in _js_files()


def test_no_debug_code_left() -> None:
    for rel in sorted(_js_files()):
        text = (JS_DIR / rel).read_text(encoding="utf-8")
        assert "debug" not in text.lower(), rel
        assert "console." not in text, rel
    for path in _templates():
        assert "-debug" not in path.read_text(encoding="utf-8"), _rel(path)


class _Attrs(HTMLParser):
    """Atributos (ya des-escapados) del primer <tag> [con id=element_id]."""

    def __init__(self, tag: str, element_id: str | None) -> None:
        super().__init__()
        self.tag = tag
        self.element_id = element_id
        self.attrs: dict[str, str | None] = {}

    def handle_starttag(self, tag: str, attrs: list) -> None:
        found = dict(attrs)
        if tag == self.tag and not self.attrs and self.element_id in (None, found.get("id")):
            self.attrs = found


def _attrs_of(html: str, tag: str, element_id: str | None = None) -> dict[str, str | None]:
    parser = _Attrs(tag, element_id)
    parser.feed(html)
    return parser.attrs


def test_widget_chart_spec_survives_hostile_title() -> None:
    title = "</canvas><script>x</script> \"q\" 'a' & <b>"
    w = {"id": 3, "type": "pie", "title": title, "payload": {"labels": ["<b>A</b>", "B'"], "values": [1, 2]}}
    html = templates.env.get_template("partials/widget_chart.html").render(w=w)
    assert "<script" not in html and "<b>" not in html
    spec = json.loads(_attrs_of(html, "canvas")["data-js-widget-chart"])
    assert spec == {"type": "pie", "title": title, "payload": w["payload"]}


def test_label_print_data_attributes_escape_values() -> None:
    label = SimpleNamespace(sku='S"1<x>', name="N <b>'n'</b> &")
    html = templates.env.get_template("label_single_print.html").render(
        label=label, copies=1, static_version=lambda: "1"
    )
    sheet = _attrs_of(html, "div", "sheet")
    assert sheet["data-js-label-sku"] == label.sku
    assert sheet["data-js-label-name"] == label.name
````

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit/test_frontend_js_modules.py   # 9 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings --no-cov -q tests/unit   # todo PASS
```

#### Expected Report

- [ ] `tests/unit/test_frontend_js_modules.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T27: Fixtures e2e: siembra del recorrido y guarda `js_errors`

**Lote:** H · **REQs:** REQ-8, REQ-9
**Depends on:** T26
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/conftest.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna

#### Context

Ya existen en el fichero: `_boot_temp_server(prefix, *, extra_env=None) -> (base_url, db_path, proc, tmp)`, `_seed_stock(db_path)`, imports `create_engine, select, sessionmaker, hash_password, Business, Location, Product, User, datetime, timezone, shutil, subprocess, pytest`. Servicios: `InventoryService(db, business_id).purchase/transfer/sale/adjustment(...)`.

#### Objective

Añadir `_seed_tour`, `tour_server` y `js_errors` al final de `tests/e2e/conftest.py`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: añadir AL FINAL de `tests/e2e/conftest.py` EXACTAMENTE este bloque** (sin tocar lo anterior; el fichero ya termina en salto de línea):

````python


# ── Fase 2 del release 0.5.0 (JS a módulos, CSP sin 'unsafe-inline') ─────────────────────
# Siembra para recorrer TODAS las pestañas y modales: cada modal necesita una fila que lo abra.
TOUR_PRODUCT_NAME = "Tour <b>uno</b> & 'co'"  # hostil: el tooltip del pastel debe mostrarlo como texto
TOUR_WIDGET_TITLE = "Widget <b>tour</b>"


def _seed_tour(db_path: str) -> dict:
    """Movimientos (compra, traspaso, ventas, ajuste), cliente, gasto, extracción, documento,
    widget de gráfico y un usuario con cambio de contraseña obligatorio. Fechas = ahora, para
    que los filtros por mes por defecto los muestren. Devuelve los ids que usan los e2e."""
    from app.agent.widgets import add_widget
    from app.models import (
        Customer, MoneyExtraction, OperatingExpense, SalesDocument, SalesDocumentItem,
    )
    from app.schemas import (
        AdjustmentCreate, PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate,
    )
    from app.services.inventory_service import InventoryService

    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    db = sessionmaker(bind=engine)()
    now = datetime.now(timezone.utc)
    try:
        biz = db.scalar(select(Business).where(Business.code == "recambios"))
        bid = int(biz.id)
        central = db.scalar(select(Location).where(Location.business_id == bid, Location.role == "central"))
        pos = db.scalar(
            select(Location).where(Location.business_id == bid, Location.id != central.id).order_by(Location.id)
        )
        # TOUR-3 sin movimientos: es el único borrable (casillas name="skus" de stock_table).
        for sku, name in (("TOUR-1", TOUR_PRODUCT_NAME), ("TOUR-2", "Tour dos"), ("TOUR-3", "Tour tres")):
            db.add(Product(business_id=bid, sku=sku, name=name, min_stock=50, unit_of_measure="ud",
                           default_purchase_cost=1.0, default_sale_price=2.0, lead_time_days=0))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        purchase = svc.purchase(PurchaseCreate(sku="TOUR-1", quantity=10, unit_cost=1.0, movement_date=now,
                                               lot_code="LOT-TOUR-1", location_code=central.code, note="n"))
        transfer = svc.transfer(TransferCreate(from_location_code=central.code, to_location_code=pos.code,
                                               lines=[TransferLineCreate(sku="TOUR-1", quantity=3)],
                                               movement_date=now, note="n"))
        # Una venta por cada origen de lote: compra, traspaso y ajuste (botones de Utilidades).
        svc.sale(SaleCreate(sku="TOUR-1", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=central.code, note="n"))
        svc.sale(SaleCreate(sku="TOUR-1", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=pos.code, note="n"))
        svc.adjustment(AdjustmentCreate(sku="TOUR-2", quantity_delta=2, unit_cost=1.0, movement_date=now,
                                        location_code=central.code, note="n"))
        svc.sale(SaleCreate(sku="TOUR-2", quantity=1, unit_price=2.0, movement_date=now,
                            location_code=central.code, note="n"))
        customer = Customer(business_id=bid, client_id="C-TOUR", name="Cliente Tour", address="Calle 1",
                            phone="600000000", email="c@tour.test")
        db.add(customer)
        db.add(OperatingExpense(business_id=bid, expense_date=now, amount=12.5, concept="Luz"))
        db.add(MoneyExtraction(business_id=bid, extraction_date=now, party="Negocio", amount=20.0,
                               concept="Retiro"))
        db.flush()
        doc = SalesDocument(
            business_id=bid, location_id=central.id, customer_id=customer.id, doc_type="F",
            year_month=now.strftime("%Y%m"), seq=1, code=f"F{now:%Y%m}-0001", issue_date=now,
            issuer_name="Emisor", client_name=customer.name, client_id=customer.client_id,
            client_address=customer.address, notes="n", subtotal=4.0, total=4.0,
        )
        db.add(doc)
        db.flush()
        db.add(SalesDocumentItem(document_id=doc.id, line_no=1, sku="TOUR-1", description="Tour uno",
                                 unit_of_measure="ud", quantity=2, unit_price=2.0, line_total=4.0))
        db.add(User(username="mustchange", password_hash=hash_password("mustchange"), role="admin",
                    is_active=True, business_id=bid, must_change_password=True))
        db.commit()
        add_widget(db, bid, type="pie", title=TOUR_WIDGET_TITLE, tool="e2e", tool_args={},
                   payload={"labels": ["A", "B"], "values": [1, 2]}, unit=None, created_by=None)
        return {"purchase": purchase.movement.id, "transfer_ref": transfer.transfer_ref or ""}
    finally:
        db.close()
        engine.dispose()


@pytest.fixture(scope="function")
def tour_server() -> tuple[str, dict]:
    """Como `live_server`, más la siembra de `_seed_tour`. Devuelve (base_url, ids)."""
    base_url, db_path, proc, tmp = _boot_temp_server("inventario-e2e-tour-")
    try:
        _seed_stock(db_path)
        ids = _seed_tour(db_path)
        yield base_url, ids
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture(scope="function")
def js_errors(page) -> list[str]:
    """Errores de JS de la página: excepciones no capturadas y mensajes `console.error`
    (Chromium informa ahí las violaciones de CSP y los módulos que no cargan). Los tests
    terminan con `assert js_errors == []`."""
    errors: list[str] = []
    page.on("pageerror", lambda exc: errors.append(f"pageerror: {exc}"))
    page.on("console", lambda msg: errors.append(f"console.error: {msg.text}") if msg.type == "error" else None)
    return errors
````

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/e2e/conftest.py').read())" && echo OK
```

#### Expected Report

- [ ] `tests/e2e/conftest.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T28: e2e: recorrido de pestañas, modales, login e impresiones sin errores de JS/CSP

**Lote:** H · **REQs:** REQ-8, REQ-9
**Depends on:** T27
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_ui_tour_csp_e2e.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Fixtures de T27: `tour_server` → `(base_url, ids)`, `js_errors` → lista. Con `script-src 'self'`, `page.wait_for_function("expresión")` hace `eval` al sondear y el CSP lo bloquea: usar siempre la forma de función.

#### Objective

Recorrer las 17 pestañas (admin), operator y owner, los 18 modales (abrir y cerrar con ×), login y las 3 impresiones; fallar ante cualquier `console.error`/excepción/violación de CSP.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/e2e/test_ui_tour_csp_e2e.py` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````python
"""Fase 2 del release 0.5.0: recorrido de TODAS las pestañas, modales y páginas sueltas con el
CSP sin 'unsafe-inline'. Falla ante cualquier excepción de JS, `console.error` o violación de
CSP (fixture `js_errors`), y comprueba que los modales se abren y se cierran con su ×
(cableado de core/modals.js)."""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

# Las 17 pestañas del admin (dashboard.html), en el orden del sidebar.
TABS = [
    "home", "agent", "inventory", "purchases", "transfers", "coach", "sales", "documents",
    "customers", "expenses", "profit-items", "profit", "dividends", "history", "users",
    "businesses", "activity",
]

# (pestaña, selector del botón que abre el modal, id del modal). El modal de cambio de
# contraseña obligatorio tiene su propio test.
MODALS = [
    ("inventory", '[hx-get$="/edit-inventory"]', "product-inventory-edit-modal"),
    ("purchases", '[hx-get^="/ui/movement/purchase/"]', "purchase-edit-modal"),
    ("transfers", '[hx-get^="/ui/movement/transfer/"]', "transfer-edit-modal"),
    ("transfers", '[hx-get="/ui/transfers/bulk"]', "transfer-bulk-modal"),
    ("sales", '[hx-get^="/ui/movement/sale/"]', "sale-edit-modal"),
    ("documents", '[hx-get^="/ui/sales-doc/"][hx-get$="/edit"]', "sales-doc-edit-modal"),
    ("customers", '[hx-get^="/ui/customer/"][hx-get$="/edit"]', "customer-edit-modal"),
    ("expenses", '[hx-get^="/ui/expense/"][hx-get$="/edit"]', "expense-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/sale/"]', "profit-items-sale-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/purchase/"]', "profit-items-purchase-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/transfer-in/"]', "profit-items-transfer-in-edit-modal"),
    ("profit-items", '[hx-get*="/profit-items/adjustment/"]', "profit-items-adjustment-edit-modal"),
    ("dividends", '[hx-get^="/ui/extraction/"][hx-get$="/edit"]', "extraction-edit-modal"),
    ("users", '[hx-get^="/ui/user/"][hx-get$="/edit"]', "user-edit-modal"),
    ("users", '[hx-get$="/reset-password-form"]', "user-reset-password-modal"),
    ("businesses", '[hx-get^="/ui/business/"][hx-get$="/edit"]', "business-edit-modal"),
]


def login(page: Page, base_url: str, user: str = "admin") -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', user)
    page.fill('input[name="password"]', user)
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    page.locator("#tab-content > *").first.wait_for()


def open_tab(page: Page, slug: str) -> None:
    with page.expect_response(lambda r: r.url.split("?")[0].endswith(f"/ui/tabs/{slug}")) as resp:
        page.click(f"#tab-{slug}")
    assert resp.value.status == 200, f"/ui/tabs/{slug} -> {resp.value.status}"
    # Los enhancers corren dos frames después del swap (core/content.js).
    page.wait_for_timeout(250)


def test_all_tabs_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    for slug in TABS:
        open_tab(page, slug)
        expect(page.locator("#tab-content")).not_to_contain_text("Internal Server Error")
    assert js_errors == []


def test_all_modals_open_and_close_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    current = None
    for slug, opener, modal_id in MODALS:
        if slug != current:
            open_tab(page, slug)
            current = slug
        page.locator(opener).first.click()
        modal = page.locator(f"#{modal_id}")
        expect(modal).to_be_visible()
        modal.locator(".modal-close").first.click()
        expect(modal).to_have_count(0)
    assert js_errors == []


def test_product_edit_modal_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    # Su único botón está en partials/products_history.html, que hoy no incluye ninguna
    # pestaña: se abre con la misma petición que haría ese botón (hx-target="body").
    base_url, _ = tour_server
    login(page, base_url)
    page.evaluate("htmx.ajax('GET', '/ui/product/TOUR-1/edit', {target: 'body', swap: 'beforeend'})")
    modal = page.locator("#product-edit-modal")
    expect(modal).to_be_visible()
    modal.locator(".modal-close").first.click()
    expect(modal).to_have_count(0)
    assert js_errors == []


def test_forced_password_change_modal_is_static(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url, user="mustchange")
    modal = page.locator("#must-change-password-modal")
    expect(modal).to_be_visible()
    modal.click(position={"x": 5, "y": 5})  # clic en el overlay: data-modal-static="1" no cierra
    expect(modal).to_be_visible()
    assert js_errors == []


@pytest.mark.parametrize("user, tabs", [("operator", ["sales"]), ("owner", ["home", "agent", "sales", "history"])])
def test_other_roles_tabs_without_js_errors(tour_server, page: Page, js_errors: list[str], user, tabs) -> None:
    base_url, _ = tour_server
    login(page, base_url, user=user)
    for slug in tabs:
        open_tab(page, slug)
    assert js_errors == []


def test_login_and_print_pages_without_js_errors(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    page.goto(f"{base_url}/ui/login")
    page.wait_for_load_state("load")
    login(page, base_url)
    for path in ("/ui/restock-print", f"/ui/transfers/print?ref={ids['transfer_ref']}",
                 f"/ui/purchase/{ids['purchase']}/label"):
        resp = page.goto(f"{base_url}{path}")
        assert resp.status == 200, path
        page.wait_for_load_state("load")
    expect(page.locator("#sheet svg.barcode rect").first).to_be_attached()
    assert js_errors == []
````

#### Verification

```bash
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -rfE tests/e2e/test_ui_tour_csp_e2e.py   # 7 passed
```

#### Expected Report

- [ ] `tests/e2e/test_ui_tour_csp_e2e.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T29: e2e de comportamientos migrados (1/2)

**Lote:** I · **REQs:** REQ-4, REQ-9
**Depends on:** T28
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_js_behaviors_e2e.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Write (contenido literal), Bash (verificación) · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Importa `login`, `open_tab` de `tests/e2e/test_ui_tour_csp_e2e.py` (T28) y `TOUR_PRODUCT_NAME`, `TOUR_WIDGET_TITLE` de `tests/e2e/conftest.py` (T27).

#### Objective

Primera mitad: «seleccionar todo» ×6, tema, autoenvío del negocio, Enter → cantidad, moneda del documento.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: escribir `tests/e2e/test_js_behaviors_e2e.py` con EXACTAMENTE este contenido** (crear o sobrescribir; crear el directorio si no existe):

````python
"""Fase 2 del release 0.5.0: cada comportamiento que vivía en un onclick=/onchange=/onkeydown= o
en un <script> inline sigue funcionando desde los módulos de app/static/js/. Todos terminan sin
errores de JS ni violaciones de CSP (fixture `js_errors`)."""

from __future__ import annotations

import re

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

from app.routers.ui_common import templates
from tests.e2e.conftest import TOUR_PRODUCT_NAME, TOUR_WIDGET_TITLE
from tests.e2e.test_ui_tour_csp_e2e import login, open_tab

pytestmark = pytest.mark.e2e

# Con script-src 'self', wait_for_function("expresión") hace eval() en cada sondeo y el CSP lo
# bloquea: las esperas usan siempre la forma de función ("(arg) => ...", arg=...).
# window.print() abriría el diálogo: se cuenta en vez de imprimir.
PRINT_STUB = "window.__printed = 0; window.print = () => { window.__printed += 1; };"
CHART_EXISTS = "(id) => !!Chart.getChart(document.getElementById(id))"


@pytest.mark.parametrize(
    "tab, name",
    [
        ("inventory", "skus"),
        ("purchases", "movement_ids"),
        ("transfers", "movement_ids"),
        ("sales", "sale_ids"),
        ("profit-items", "sale_ids"),
        ("history", "movement_ids"),
    ],
)
def test_select_all_checkbox(tour_server, page: Page, js_errors: list[str], tab: str, name: str) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, tab)
    master = page.locator(f'#tab-content [data-js-select-all="{name}"]').first
    form = master.locator("xpath=ancestor::form[1]")
    boxes = form.locator(f'input[name="{name}"]')
    assert boxes.count() > 0
    master.check()
    for i in range(boxes.count()):
        expect(boxes.nth(i)).to_be_checked()
    master.uncheck()
    for i in range(boxes.count()):
        expect(boxes.nth(i)).not_to_be_checked()
    assert js_errors == []


def test_theme_toggle_persists_across_reload(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    with page.expect_navigation():
        page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "dark")
    with page.expect_navigation():
        page.click("[data-js-theme-toggle]")
    expect(page.locator("html")).to_have_attribute("data-theme", "light")
    assert js_errors == []


def test_business_select_submits_on_change(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    with page.expect_request(lambda r: r.url.endswith("/ui/active-business") and r.method == "POST") as req:
        page.dispatch_event('select[name="business_id"]', "change")
    assert re.search(r"business_id=\d+", req.value.post_data or "")
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    assert js_errors == []


def test_sale_product_enter_moves_focus_to_quantity(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "sales")
    product = page.locator('#sale-panel input[name="product"][data-js-enter-focus="quantity"]')
    product.fill("TOUR-1")
    product.press("Enter")
    focused = page.evaluate("document.activeElement.name")
    assert focused == "quantity"
    expect(page.locator("#sale-panel")).not_to_contain_text("Venta registrada")
    assert js_errors == []


def test_sales_doc_currency_is_mirrored_to_issue_form(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "documents")
    page.select_option("#sales-doc-currency-code", "USD")
    expect(page.locator("#sales-doc-currency-hidden")).to_have_value("USD")
    page.select_option("#sales-doc-currency-code", "GEN")
    expect(page.locator("#sales-doc-currency-hidden")).to_have_value("GEN")
    assert js_errors == []


````

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/e2e/test_js_behaviors_e2e.py').read())" && echo OK
```

#### Expected Report

- [ ] `tests/e2e/test_js_behaviors_e2e.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T30: e2e de comportamientos migrados (2/2)

**Lote:** I · **REQs:** REQ-4, REQ-9, REQ-10
**Depends on:** T29
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_js_behaviors_e2e.py`
**Pillars:** P1, P2, P4, P5, P6, P7
**Tools (cc_local):** Read, Edit (añadir el bloque literal al final), Bash (verificación) · **Skills:** ninguna

#### Context

El chat del copiloto llamaría al LLM: el test intercepta `POST /ui/agent/chat` con `page.route` y devuelve la plantilla real `partials/agent_chat_turn.html` renderizada con un widget.

#### Objective

Segunda mitad: gráficos de Inicio + tooltip sin XSS, panel del copiloto + widget, chat con widget OOB, impresión y copias, mostrar contraseña, menú móvil.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: añadir AL FINAL de `tests/e2e/test_js_behaviors_e2e.py` EXACTAMENTE este bloque** (sin tocar lo anterior; el fichero ya termina en salto de línea):

````python
def test_home_charts_render_and_pie_tooltip_escapes_label(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "home")
    for chart_id in ("monthly-chart", "monthly-sales-daily-line", "monthly-sales-pie"):
        page.wait_for_function(CHART_EXISTS, arg=chart_id)
    # El primer artículo del pastel es TOUR-1 (el de más venta), con nombre hostil.
    page.evaluate("""() => {
        const chart = Chart.getChart(document.getElementById('monthly-sales-pie'));
        const arc = chart.getDatasetMeta(0).data[0];
        const pos = arc.tooltipPosition();
        chart.tooltip.setActiveElements([{datasetIndex: 0, index: 0}], pos);
        chart.update();
    }""")
    tooltip = page.locator("#monthly-sales-pie-tooltip")
    expect(tooltip).to_contain_text("Cantidad vendida")
    expect(tooltip).to_contain_text(TOUR_PRODUCT_NAME)
    assert tooltip.locator("b").count() == 0
    assert js_errors == []


def test_agent_panel_and_seeded_widget_chart(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    login(page, base_url)
    open_tab(page, "agent")
    canvas_id = page.locator("#agent-grid canvas[data-js-widget-chart]").first.get_attribute("id")
    page.wait_for_function(CHART_EXISTS, arg=canvas_id)
    label = page.evaluate(f"Chart.getChart(document.getElementById('{canvas_id}')).data.datasets[0].label")
    assert label == TOUR_WIDGET_TITLE

    panel = page.locator("#agent-panel")
    expect(page.locator("#agent-fab")).to_be_visible()
    page.click("#agent-fab")
    expect(panel).not_to_have_class(re.compile(r"\bhidden\b"))
    page.click("#agent-panel [data-js-agent-size]")
    expect(panel).to_have_class(re.compile(r"\blarge\b"))
    page.click("#agent-panel .agent-panel-close")
    expect(panel).to_have_class(re.compile(r"\bhidden\b"))
    open_tab(page, "sales")
    expect(page.locator("#agent-fab")).to_be_hidden()
    assert js_errors == []


def test_agent_chat_turn_renders_oob_widget_chart(tour_server, page: Page, js_errors: list[str]) -> None:
    """Los widgets nuevos llegan en un swap OOB (#agent-grid), no en el target del chat
    (#agent-chat-thread): el enhancer debe recibir también los elementos OOB. La respuesta del
    chat (que llamaría al LLM) se sustituye por la plantilla real con un widget de barras."""
    base_url, _ = tour_server
    widget = {"id": 9999, "type": "bar", "title": "Ventas OOB", "payload": {"labels": ["x", "y"], "values": [3, 4]}}
    body = templates.env.get_template("partials/agent_chat_turn.html").render(
        user_content="hola", reply="Respuesta e2e", widgets=[widget], currency_symbol="€"
    )
    page.route("**/ui/agent/chat", lambda route: route.fulfill(status=200, content_type="text/html", body=body))
    login(page, base_url)
    open_tab(page, "agent")
    page.click("#agent-fab")
    page.fill("#agent-chat-form textarea[name=message]", "hola copiloto")
    page.press("#agent-chat-form textarea[name=message]", "Enter")
    thread = page.locator("#agent-chat-thread")
    expect(thread.locator(".msg-user").last).to_have_text("hola copiloto")
    expect(thread).to_contain_text("Respuesta e2e")
    expect(page.locator("#agent-typing")).to_have_count(0)
    expect(page.locator("#agent-chat-form textarea[name=message]")).to_have_value("")
    page.wait_for_function(CHART_EXISTS, arg="chart-9999")
    assert js_errors == []


def test_print_buttons_and_label_copies(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, ids = tour_server
    page.add_init_script(PRINT_STUB)
    login(page, base_url)
    for path in ("/ui/restock-print", f"/ui/transfers/print?ref={ids['transfer_ref']}"):
        page.goto(f"{base_url}{path}")
        page.click("[data-js-print]")
        assert page.evaluate("window.__printed") == 1, path

    page.goto(f"{base_url}/ui/purchase/{ids['purchase']}/label?auto_print=1")
    page.wait_for_function("(n) => window.__printed === n", arg=1)
    expect(page.locator("#sheet .name").first).to_have_text(TOUR_PRODUCT_NAME)
    page.fill("#copies", "3")
    page.dispatch_event("#copies", "change")
    expect(page.locator("#sheet svg.barcode")).to_have_count(3)
    expect(page.locator("#sheet svg.barcode rect").first).to_be_attached()
    page.click("#btn-print")
    page.wait_for_function("(n) => window.__printed === n", arg=2)
    assert js_errors == []


def test_login_show_password(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    page.goto(f"{base_url}/ui/login")
    pw = page.locator("#login-password")
    expect(pw).to_have_attribute("type", "password")
    page.check("#login-show-password")
    expect(pw).to_have_attribute("type", "text")
    page.uncheck("#login-show-password")
    expect(pw).to_have_attribute("type", "password")
    assert js_errors == []


def test_mobile_menu_toggle(tour_server, page: Page, js_errors: list[str]) -> None:
    base_url, _ = tour_server
    page.set_viewport_size({"width": 400, "height": 800})
    login(page, base_url)
    page.click("#mobile-menu-toggle")
    expect(page.locator("#sidebar")).to_have_class(re.compile(r"\bactive\b"))
    page.click("#tab-sales")
    expect(page.locator("#sidebar")).not_to_have_class(re.compile(r"\bactive\b"))
    assert js_errors == []
````

#### Verification

```bash
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -rfE tests/e2e/test_js_behaviors_e2e.py   # 16 passed
SESSION_SECRET=test OPERATOR_USERNAME=operator OPERATOR_PASSWORD=test .venv/bin/python -m pytest -m e2e --no-cov -p no:warnings -rfE tests/e2e   # 70 passed
```

#### Expected Report

- [ ] `tests/e2e/test_js_behaviors_e2e.py` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado

---

### T31: CHANGELOG de la fase 2 (sin bump de versión)

**Lote:** I · **REQs:** REQ-12
**Depends on:** T30
**Skill:** ninguna
**Agent:** coder
**Files:** `CHANGELOG.md`
**Pillars:** P1, P6
**Tools (cc_local):** Bash (ejecutar el script literal + verificación) · **Skills:** ninguna — no editar a mano, no cargar skills

#### Context

La sección `## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)` ya existe; su última viñeta termina en «…en vez de renderizar un modal roto.» y le sigue `## 0.4.15`.

#### Objective

Añadir la entrada de la fase 2 bajo `## 0.5.0 (sin publicar …)`. NO tocar `VERSION` ni `.bumpversion.cfg`.

#### Instructions

Usa SOLO este envelope: no sigas notas genéricas ni otros documentos. Ejecuta desde la raíz del repo.

**Step 1: ejecutar con Bash EXACTAMENTE este script** (los `assert`/`set -e` abortan si algo no cuadra: en ese caso PARAR y reportar, no editar a mano):

````bash
python3 - <<'PY'
import pathlib


def edit(path, old, new):
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(old) == 1, f"{path}: se esperaba 1 coincidencia y hay {s.count(old)}: {old[:50]!r}"
    p.write_text(s.replace(old, new), encoding="utf-8")


def cut(path, start, end):
    """Borra desde `start` hasta `end` (incluido). `start` debe aparecer 1 sola vez."""
    p = pathlib.Path(path)
    s = p.read_text(encoding="utf-8")
    assert s.count(start) == 1, f"{path}: inicio no único/ausente: {start[:50]!r}"
    a = s.index(start)
    b = s.index(end, a) + len(end)
    p.write_text(s[:a] + s[b:], encoding="utf-8")

edit("CHANGELOG.md", '  desconocido fallan con un error explícito (global `template_error`), en vez de renderizar un modal roto.\n', '  desconocido fallan con un error explícito (global `template_error`), en vez de renderizar un modal roto.\n' + """- refactor(frontend): fase 2 — todo el JS en módulos ES nativos bajo `app/static/js/` (`main.js` +
  `core/`, `components/`, `features/`, `pages/`; `theme-init.js` clásico en `<head>`), sin build. Fuera
  `app.js`, los 5 `<script>` inline ejecutables y los 15 `onclick=`/`onchange=`/`onkeydown=` (sustituidos por
  atributos `data-js-*`), el listener no-op `debugReturnLots`, los 3 `#*-debug` y el sondeo de `window.Chart`.
  El CSP pasa a `script-src 'self'` (sin `'unsafe-inline'`). `/static/js/` se sirve con `Cache-Control:
  no-cache` y `static_version()` calcula el hash de todos los `.css`/`.js`, también los módulos anidados.
- fix(inicio): el tooltip del gráfico de pastel metía la etiqueta (`SKU - nombre de producto`) en
  `innerHTML` sin escapar (XSS al pasar el ratón); ahora se construye con `textContent`.
- test(frontend): el arnés de snapshots ignora el cableado JS (scripts ejecutables, `on*`, `data-js-*`,
  `#*-debug`); baseline regenerada desde el commit anterior a la fase. +9 unit (CSP, sin JS inline, scripts por
  plantilla, grafo de módulos, sin código de depuración, escape de `data-js-*`) y +23 e2e que recorren las 17
  pestañas, los 18 modales, login e impresiones, y cada comportamiento migrado, y fallan ante cualquier error
  de JS o violación de CSP.
""")
print("OK")
PY
````

#### Verification

```bash
git diff --quiet HEAD -- VERSION .bumpversion.cfg && grep -c 'fase 2 — todo el JS' CHANGELOG.md   # 1
```

#### Expected Report

- [ ] `CHANGELOG.md` — creado(s)/modificado(s) según los Steps
- [ ] Ningún otro fichero modificado (`git status --short`)
- [ ] Verificación: todos los comandos con el resultado indicado
