# Task Envelopes — Frontend fase 5a: plantillas de `partials/` a `features/<dominio>/`

Fuente única: `requirements.md` (REQ-1..7) y `design.md` §2.2–2.4, §2.10. Rama base `develop` `bf38a2b`.
Ejecutado por `cc_local_pae` (un lote por sesión limpia). El envelope es la única fuente: no leer el PAE
completo ni otros specs.

Lotes: **A** T1–T3 · **B** T4–T11 · **C** T12–T17 · **D** T18–T25 · **E** T26–T30

<!-- ═══════════════════════════════════════════════════════════════════════════════════
     LOTE A (Lote 0) — camino feliz: guarda + script de migración + baseline de estilos
     ═══════════════════════════════════════════════════════════════════════════════════ -->

### T1: Guarda `test_templates_layout.py` con `test_literal_template_names_resolve`

**Depends on:** none
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_templates_layout.py`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# app/routers/ui_common.py — ESTO YA EXISTE, NO MODIFICAR
templates = Jinja2Templates(directory="app/templates")   # templates.env.get_template(name) resuelve rutas
```

#### Objective

Crear `tests/unit/test_templates_layout.py` con el test `test_literal_template_names_resolve` (REQ-4c).

#### Instructions

**Step 1: Create the file**

Escribe `tests/unit/test_templates_layout.py` con EXACTAMENTE este contenido (nada más, nada menos):

```python
"""Guarda de estructura de plantillas (fase 5a, REQ-4).

``test_literal_template_names_resolve`` carga cada nombre literal de plantilla (rutas de los routers
y ``{% include|extends|from|import %}`` de las plantillas) y falla si alguno no resuelve. Los tests
``test_partials_dir_is_gone`` y ``test_no_partials_references`` se añaden en el Lote E.
"""

from __future__ import annotations

import re
from pathlib import Path

from jinja2.exceptions import TemplateNotFound

from app.routers.ui_common import templates

ROOT = Path(__file__).resolve().parents[2]

# Lote 0: `partials` aún admitido (el Lote E lo quita de esta regex).
_TEMPLATE_NAME = re.compile(
    r"""["']((?:features|layouts|components|partials)/[\w/]+\.html|[\w]+\.html)["']"""
)

_INCLUDE_TAG = re.compile(r"""\{%\s*(include|extends|from|import)\s+['"]([\w/]+\.html)['"]""")


def _template_names() -> set[str]:
    names: set[str] = set()
    for path in sorted((ROOT / "app" / "routers").glob("*.py")):
        names.update(_TEMPLATE_NAME.findall(path.read_text(encoding="utf-8")))
    for path in sorted((ROOT / "app" / "templates").rglob("*.html")):
        for m in _INCLUDE_TAG.finditer(path.read_text(encoding="utf-8")):
            names.add(m.group(2))
    return names


def test_literal_template_names_resolve() -> None:
    broken = []
    for name in sorted(_template_names()):
        try:
            templates.env.get_template(name)
        except TemplateNotFound:
            broken.append(name)
    assert broken == [], "plantillas que no resuelven: " + ", ".join(broken)
```

**Edge cases (ya resueltos):**
- Nombre de plantilla dinámico (`{` en la cadena): la regex `[\w/]+\.html` no lo casa; no hace falta excluirlo a mano.
- Comentarios/docstrings que nombran `partials/`: sin comillas alrededor no los casa la regex (verificado en `bf38a2b`).

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_templates_layout.py -q --override-ini="addopts="
# Expected: 1 passed
```

#### Expected Report

- [ ] `tests/unit/test_templates_layout.py` creado con `test_literal_template_names_resolve`
- [ ] Ningún otro fichero modificado
- [ ] `pytest tests/unit/test_templates_layout.py` — 1 passed

---

### T2: Script `tools/move_templates.py` (tabla de `design.md` §2.3 como dict)

**Depends on:** none
**Skill:** backend-developer
**Agent:** coder
**Files:** `tools/move_templates.py`
**Pillars:** P1, P4, P5, P6

#### Objective

Crear `tools/move_templates.py`: hace `git mv` de cada plantilla a su dominio y sustituye sus referencias.

#### Instructions

**Step 1: Create the file**

Escribe `tools/move_templates.py` con EXACTAMENTE este contenido:

```python
#!/usr/bin/env python3
"""Migración one-off de partials/ a features/<dominio>/ (fase 5a). El Lote E lo borra."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Tabla de design.md §2.3. Clave = dominio, valor = plantillas (desde partials/).
MAPPING: dict[str, list[str]] = {
    "activity": ["tab_activity.html"],
    "agent": ["agent_chat_turn.html", "agent_messages.html", "agent_reply.html", "tab_agent.html",
              "widget_chart.html", "widget_grid.html", "widget_kpi.html", "widget_list.html",
              "widget_table.html"],
    "auth": ["must_change_password_form.html"],
    "businesses": ["business_edit_form.html", "tab_businesses.html"],
    "coach": ["tab_coach.html"],
    "customers": ["customer_detail.html", "customer_edit_form.html", "customers_table.html",
                  "tab_customers.html"],
    "dividends": ["tab_dividends.html"],
    "expenses": ["expense_edit_form.html", "tab_expenses.html"],
    "extractions": ["extraction_edit_form.html"],
    "history": ["tab_history.html"],
    "home": ["home_charts.html", "tab_home.html"],
    "products": ["product_edit_form.html", "product_edit_form_inventory.html", "product_panel.html",
                 "product_suggest_options.html"],
    "profit": ["tab_profit.html"],
    "profit_items": ["profit_items_edit_form.html", "tab_profit_items.html"],
    "purchases": ["purchase_edit_form.html", "purchase_panel.html", "purchases_history.html",
                  "tab_purchases.html"],
    "sales": ["sale_edit_form.html", "sale_panel.html", "sales_history.html", "tab_sales.html"],
    "sales_documents": ["sales_document_edit_form.html", "sales_document_panel.html",
                        "sales_document_preview.html", "tab_documents.html"],
    "stock": ["restock_table.html", "stock_table.html", "tab_inventory.html"],
    "sync": ["sync_status.html"],
    "transfers": ["tab_transfers.html", "transfer_bulk_modal.html", "transfer_edit_form.html"],
    "users": ["tab_users.html", "user_edit_form.html", "user_reset_password_form.html"],
}

DELETE = ["action_result.html", "products_history.html"]


def _files_with_needle(needle: str) -> list[Path]:
    hits: list[Path] = []
    for base in (ROOT / "app", ROOT / "tests"):
        for path in base.rglob("*"):
            if path.is_file() and path.suffix in (".py", ".html", ".js"):
                if needle in path.read_text(encoding="utf-8"):
                    hits.append(path)
    return hits


def _move_domain(domain: str) -> None:
    dest = ROOT / "app" / "templates" / "features" / domain
    dest.mkdir(parents=True, exist_ok=True)
    for name in MAPPING[domain]:
        src = ROOT / "app" / "templates" / "partials" / name
        if not src.exists():
            sys.exit(f"FALLO: no existe {src}")
        old, new = f"partials/{name}", f"features/{domain}/{name}"
        hits = _files_with_needle(old)
        if not hits:
            sys.exit(f"FALLO: '{old}' no aparece en ningún fichero; abortado")
        for path in hits:
            text = path.read_text(encoding="utf-8")
            path.write_text(text.replace(old, new), encoding="utf-8")
        subprocess.run(["git", "mv", str(src), str(dest / name)], check=True, cwd=ROOT)


def _delete_dead() -> None:
    for name in DELETE:
        src = ROOT / "app" / "templates" / "partials" / name
        if src.exists():
            subprocess.run(["git", "rm", str(src)], check=True, cwd=ROOT)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--domain", required=True, choices=sorted(MAPPING) + ["delete"])
    args = ap.parse_args(argv)
    if args.domain == "delete":
        _delete_dead()
    else:
        _move_domain(args.domain)
    print(f"OK: dominio {args.domain}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
```

**Edge cases (ya resueltos):**
- `partials/product_edit_form.html` no toca `partials/product_edit_form_inventory.html`: la aguja incluye `.html`.
- Una plantilla que incluye a otra del mismo dominio: la sustitución recorre todo el árbol antes de cada `git mv`,
  así el `{% include %}` se actualiza aunque el fichero que lo contiene se mueva después.
- Plantilla origen inexistente o aguja sin coincidencias: el script aborta con `sys.exit`.

#### Verification

```bash
python3 tools/move_templates.py --domain activity
git status --short -- app/templates/features/activity app/templates/partials/tab_activity.html
# Expected: tab_activity.html aparece como rename (R) y partials/tab_activity.html ya no existe
git reset --hard  # deshacer la prueba: el Lote B lo hará de verdad
```

#### Expected Report

- [ ] `tools/move_templates.py` creado con `MAPPING`, `DELETE`, `_move_domain`, `_delete_dead`
- [ ] `--domain activity` mueve y sustituye; `--domain delete` borra las 2 muertas
- [ ] Ningún otro fichero modificado

---

### T3: Baseline de `style_snapshot.py` escrita desde `bf38a2b`

**Depends on:** none
**Skill:** none
**Agent:** coder
**Files:** _(ninguno — escribe la baseline fuera de git)_
**Pillars:** P1, P4, P5, P6

#### Context

`tests/snapshots/style_snapshot.py` arranca la app real (uvicorn + SQLite temporal) y recorre en Chromium el
login, las 17 pestañas, los modales y las 3 impresiones, en tema claro y oscuro. `write` guarda los estilos
calculados; `check` los compara. La baseline se escribe AHORA (con `app/` idéntico a `bf38a2b`) para que el
Lote E pueda verificar "0 diferencias".

#### Objective

Escribir la baseline de estilos calculados en `/tmp/gestion_inventario-style-baseline-bf38a2b`.

#### Instructions

Ejecuta SOLO este comando desde la raíz del repo:

```bash
.venv/bin/python tests/snapshots/style_snapshot.py write /tmp/gestion_inventario-style-baseline-bf38a2b
```

No edites ningún fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/style_snapshot.py write /tmp/gestion_inventario-style-baseline-bf38a2b
# Expected: última línea "OK: baseline escrita en /tmp/gestion_inventario-style-baseline-bf38a2b"
```

#### Expected Report

- [ ] `/tmp/gestion_inventario-style-baseline-bf38a2b/` contiene los `.json` de la baseline
- [ ] `git status --short` sin cambios en `app/` ni `tests/`

---

<!-- ═══════════════════════════════════════════════════════════════════════════════════
     LOTES B–D — movimiento por dominio (transcripción: un comando por dominio)
     ═══════════════════════════════════════════════════════════════════════════════════ -->

### T4: Mover dominio `activity` (`tab_activity.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `tab_activity.html` a `features/activity/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain activity`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/activity/tab_activity.html` existe; `partials/tab_activity.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T5: Mover dominio `auth` (`must_change_password_form.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `must_change_password_form.html` a `features/auth/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain auth`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/auth/must_change_password_form.html` existe; `partials/must_change_password_form.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T6: Mover dominio `businesses` (`business_edit_form.html`, `tab_businesses.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 2 plantillas de `businesses` a `features/businesses/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain businesses`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/businesses/` tiene `business_edit_form.html` y `tab_businesses.html`; sus `partials/` ya no existen
- [ ] `ui_snapshot.py check` — OK

---

### T7: Mover dominio `coach` (`tab_coach.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `tab_coach.html` a `features/coach/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain coach`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/coach/tab_coach.html` existe; `partials/tab_coach.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T8: Mover dominio `history` (`tab_history.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `tab_history.html` a `features/history/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain history`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/history/tab_history.html` existe; `partials/tab_history.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T9: Mover dominio `profit` (`tab_profit.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `tab_profit.html` a `features/profit/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain profit`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/profit/tab_profit.html` existe; `partials/tab_profit.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T10: Mover dominio `sync` (`sync_status.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `sync_status.html` a `features/sync/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain sync`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/sync/sync_status.html` existe; `partials/sync_status.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T11: Mover dominio `users` (`tab_users.html`, `user_edit_form.html`, `user_reset_password_form.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 3 plantillas de `users` a `features/users/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain users`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/users/` tiene `tab_users.html`, `user_edit_form.html`, `user_reset_password_form.html`; sus `partials/` ya no existen
- [ ] `ui_snapshot.py check` — OK

---

### T12: Mover dominio `agent` (9 plantillas, includes internos)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 9 plantillas de `agent` a `features/agent/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain agent`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_agent.py tests/unit/test_ui_xss_escapes.py tests/unit/test_frontend_js_modules.py -q --override-ini="addopts="
```

#### Expected Report
- [ ] `app/templates/features/agent/` tiene `agent_chat_turn.html`, `agent_messages.html`, `agent_reply.html`, `tab_agent.html`, `widget_chart.html`, `widget_grid.html`, `widget_kpi.html`, `widget_list.html`, `widget_table.html`
- [ ] Los `{% include %}` internos (`tab_agent`/`agent_chat_turn` → `widget_grid` → 4 `widget_*`) apuntan a `features/agent/`
- [ ] `ui_snapshot.py check` — OK; los 3 tests unitarios — passed

---

### T13: Mover dominio `home` (`home_charts.html`, `tab_home.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 2 plantillas de `home` a `features/home/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain home`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/home/` tiene `home_charts.html` y `tab_home.html`; sus `partials/` ya no existen
- [ ] `tab_home.html` incluye `features/home/home_charts.html`
- [ ] `ui_snapshot.py check` — OK

---

### T14: Mover dominio `customers` (4 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 4 plantillas de `customers` a `features/customers/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain customers`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/customers/` tiene `customer_detail.html`, `customer_edit_form.html`, `customers_table.html`, `tab_customers.html`
- [ ] `tab_customers.html` incluye `features/customers/customers_table.html`
- [ ] `ui_snapshot.py check` — OK

---

### T15: Mover dominio `expenses` (`expense_edit_form.html`, `tab_expenses.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 2 plantillas de `expenses` a `features/expenses/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain expenses`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/expenses/` tiene `expense_edit_form.html` y `tab_expenses.html`
- [ ] `ui_snapshot.py check` — OK

---

### T16: Mover dominio `extractions` (`extraction_edit_form.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `extraction_edit_form.html` a `features/extractions/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain extractions`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/extractions/extraction_edit_form.html` existe; `partials/extraction_edit_form.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T17: Mover dominio `dividends` (`tab_dividends.html`)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover `tab_dividends.html` a `features/dividends/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain dividends`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/dividends/tab_dividends.html` existe; `partials/tab_dividends.html` no
- [ ] `ui_snapshot.py check` — OK

---

### T18: Mover dominio `products` (4 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 4 plantillas de `products` a `features/products/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain products`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/products/` tiene `product_edit_form.html`, `product_edit_form_inventory.html`, `product_panel.html`, `product_suggest_options.html`
- [ ] `ui_snapshot.py check` — OK

---

### T19: Borrar las 2 plantillas muertas (REQ-3)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Borrar `partials/action_result.html` y `partials/products_history.html` (sin consumidor).

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain delete`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
test ! -e app/templates/partials/action_result.html && test ! -e app/templates/partials/products_history.html && echo "borradas"
```

#### Expected Report
- [ ] `app/templates/partials/action_result.html` y `app/templates/partials/products_history.html` borradas
- [ ] `ui_snapshot.py check` — OK

---

### T20: Mover dominio `purchases` (4 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 4 plantillas de `purchases` a `features/purchases/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain purchases`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/purchases/` tiene `purchase_edit_form.html`, `purchase_panel.html`, `purchases_history.html`, `tab_purchases.html`
- [ ] `ui_snapshot.py check` — OK

---

### T21: Mover dominio `sales` (4 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 4 plantillas de `sales` a `features/sales/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain sales`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/sales/` tiene `sale_edit_form.html`, `sale_panel.html`, `sales_history.html`, `tab_sales.html`
- [ ] `ui_snapshot.py check` — OK

---

### T22: Mover dominio `sales_documents` (4 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 4 plantillas de `sales_documents` a `features/sales_documents/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain sales_documents`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/sales_documents/` tiene `sales_document_edit_form.html`, `sales_document_panel.html`, `sales_document_preview.html`, `tab_documents.html`
- [ ] `ui_snapshot.py check` — OK

---

### T23: Mover dominio `stock` (3 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 3 plantillas de `stock` a `features/stock/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain stock`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/stock/` tiene `restock_table.html`, `stock_table.html`, `tab_inventory.html`
- [ ] `ui_snapshot.py check` — OK

---

### T24: Mover dominio `transfers` (3 plantillas)

**Depends on:** T2
**Skill:** none (script)
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Objective
Mover las 3 plantillas de `transfers` a `features/transfers/`.

#### Instructions
Ejecuta desde la raíz: `python3 tools/move_templates.py --domain transfers`

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
```

#### Expected Report
- [ ] `app/templates/features/transfers/` tiene `tab_transfers.html`, `transfer_bulk_modal.html`, `transfer_edit_form.html`
- [ ] `ui_snapshot.py check` — OK

---

### T25: Mover dominio `profit_items` + REQ-5 (test de plantillas antiguas)

**Depends on:** T2
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `app/templates/*`, `app/routers/*`, `app/static/js/*`, `tests/*`
**Pillars:** P1, P4, P5, P6

#### Context

```python
# tests/unit/test_ui_components.py:189-194 — ESTO YA EXISTE, SE MODIFICA en el Step 2
@pytest.mark.parametrize("kind", ["sale", "purchase", "adjustment", "transfer_in"])
def test_old_profit_items_templates_are_gone(kind: str) -> None:
    from jinja2 import TemplateNotFound

    with pytest.raises(TemplateNotFound):
        templates.env.get_template(f"partials/profit_items_{kind}_edit_form.html")
```

#### Objective
Mover las 2 plantillas de `profit_items` y retargetar el test REQ-5 a la ruta nueva.

#### Instructions

**Step 1: Mover el dominio**
```bash
python3 tools/move_templates.py --domain profit_items
```

**Step 2: Editar `tests/unit/test_ui_components.py`** — sustituye SOLO esta línea:

```python
# BEFORE (línea 194):
        templates.env.get_template(f"partials/profit_items_{kind}_edit_form.html")

# AFTER:
        templates.env.get_template(f"features/profit_items/profit_items_{kind}_edit_form.html")
```

No toques ninguna otra línea de ese fichero.

#### Verification
```bash
python3 tests/snapshots/ui_snapshot.py check tests/snapshots/baseline
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_ui_components.py tests/unit/test_ui_product_suggest.py -q --override-ini="addopts="
```

#### Expected Report
- [ ] `app/templates/features/profit_items/` tiene `profit_items_edit_form.html` y `tab_profit_items.html`
- [ ] `test_old_profit_items_templates_are_gone` comprueba `features/profit_items/profit_items_<kind>_edit_form.html`
- [ ] `ui_snapshot.py check` — OK; los tests unitarios — passed

---

<!-- ═══════════════════════════════════════════════════════════════════════════════════
     LOTE E — cierre: activar guarda, quitar partials, borrar script, comentario e2e, docs
     ═══════════════════════════════════════════════════════════════════════════════════ -->

### T26: Activar la guarda completa (REQ-4) y quitar `partials` de la regex

**Depends on:** T1
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/unit/test_templates_layout.py`
**Pillars:** P1, P4, P5, P6

#### Objective
Añadir `test_partials_dir_is_gone` y `test_no_partials_references`, y quitar `partials` de la regex.

#### Instructions

**Step 1:** En `tests/unit/test_templates_layout.py`, sustituye la definición de `_TEMPLATE_NAME`:

```python
# BEFORE:
_TEMPLATE_NAME = re.compile(
    r"""["']((?:features|layouts|components|partials)/[\w/]+\.html|[\w]+\.html)["']"""
)

# AFTER:
_TEMPLATE_NAME = re.compile(
    r"""["']((?:features|layouts|components)/[\w/]+\.html|[\w]+\.html)["']"""
)
```

**Step 2:** Añade al final del fichero estas DOS funciones (después de `test_literal_template_names_resolve`):

```python
def test_partials_dir_is_gone() -> None:
    assert not (ROOT / "app" / "templates" / "partials").exists()


def test_no_partials_references() -> None:
    self_file = Path(__file__)
    offenders = []
    for base, exts in ((ROOT / "app", (".py", ".html", ".js")), (ROOT / "tests", (".py",))):
        for path in sorted(base.rglob("*")):
            if path.suffix not in exts or path == self_file:
                continue
            for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if "partials/" in line:
                    offenders.append(f"{path.relative_to(ROOT)}:{i}")
    assert offenders == [], "referencias a partials/: " + ", ".join(offenders)
```

**Edge cases (ya resueltos):**
- El propio fichero de la guarda se excluye (`path == self_file`) porque su código contiene la cadena de búsqueda.

#### Verification
```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: python3 -m pytest tests/unit/test_templates_layout.py -q --override-ini="addopts="
# Expected: 3 passed
```

#### Expected Report
- [ ] `test_templates_layout.py` tiene 3 tests, todos activos (ningún `skip`)
- [ ] `pytest tests/unit/test_templates_layout.py` — 3 passed

---

### T27: Borrar `tools/move_templates.py`

**Depends on:** T25
**Skill:** none
**Agent:** coder
**Files:** `tools/move_templates.py`
**Pillars:** P1, P4, P5, P6

#### Objective
Borrar el script de un solo uso (no se versiona).

#### Instructions
```bash
git rm tools/move_templates.py
```

#### Verification
```bash
test ! -e tools/move_templates.py && echo "borrado"
```

#### Expected Report
- [ ] `tools/move_templates.py` borrado del working tree y del índice

---

### T28: Quitar el comentario e2e que nombra `products_history.html` (REQ-3)

**Depends on:** none
**Skill:** test-automation-engineer
**Agent:** coder
**Files:** `tests/e2e/test_ui_tour_csp_e2e.py`
**Pillars:** P1, P4, P5, P6

#### Objective
Que el comentario deje de nombrar la plantilla muerta `products_history.html`.

#### Instructions

En `tests/e2e/test_ui_tour_csp_e2e.py`, dentro de `test_product_edit_modal_without_js_errors`, sustituye:

```python
# BEFORE:
    # Su único botón está en partials/products_history.html, que hoy no incluye ninguna
    # pestaña: se abre con la misma petición que haría ese botón (hx-target="body").

# AFTER:
    # El modal de edición no tiene botón visible en ninguna pestaña: se abre con la
    # misma petición que haría ese botón (hx-target="body").
```

No toques ninguna otra línea.

#### Verification
```bash
grep -rn "products_history\|action_result" app/ tests/ || echo "sin rastro"
```

#### Expected Report
- [ ] El comentario ya no contiene `partials/products_history.html`
- [ ] `grep products_history|action_result` en `app/` y `tests/` — sin coincidencias

---

### T29: Docs — árbol de plantillas en `CLAUDE.md` y `.claude/CLAUDE.md` (REQ-6)

**Depends on:** none
**Skill:** technical-writer
**Agent:** coder
**Files:** `CLAUDE.md`, `.claude/CLAUDE.md`
**Pillars:** P1, P4, P5, P6

#### Objective
Actualizar el árbol de `templates/` en los dos CLAUDE.md.

#### Instructions

**Step 1:** En `CLAUDE.md` (raíz), sustituye:

```markdown
# BEFORE:
│   ├── templates/           ← Jinja2 + HTMX partials

# AFTER:
│   ├── templates/           ← Jinja2 + HTMX (layouts/, components/, features/<dominio>/)
```

**Step 2:** En `.claude/CLAUDE.md`, sustituye:

```markdown
# BEFORE:
│   └── templates/               # Jinja2 templates (partials/, components/)

# AFTER:
│   └── templates/               # Jinja2 templates (layouts/, components/, features/<dominio>/)
```

No toques ninguna otra línea de ninguno de los dos ficheros.

#### Verification
```bash
grep -n "templates/" CLAUDE.md .claude/CLAUDE.md | grep -i "partials" || echo "árbol sin partials"
```

#### Expected Report
- [ ] Los dos árboles nombran `layouts/, components/, features/<dominio>/` y ya no `partials/`
- [ ] Ninguna otra línea modificada

---

### T30: Docs — fila "5" del diagnóstico y CHANGELOG 0.5.0 (REQ-6)

**Depends on:** none
**Skill:** technical-writer
**Agent:** coder
**Files:** `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`, `CHANGELOG.md`
**Pillars:** P1, P4, P5, P6

#### Objective
Marcar la fase 5a en el diagnóstico y añadir su entrada en el CHANGELOG.

#### Instructions

**Step 1:** En `docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`, sustituye la fila 5 de la tabla de §4:

```markdown
# BEFORE:
| **5** | Reagrupar `partials/` en `features/<dominio>/`; sacar queries de routers | Medio-alto, por dominio |

# AFTER:
| **5** | Reagrupar `partials/` en `features/<dominio>/`; sacar queries de routers | Medio-alto, por dominio — **5a ✅** |
```

**Step 2:** En `CHANGELOG.md`, inmediatamente DESPUÉS de la línea `## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)`, añade este bullet (antes del bullet de fase 4):

```markdown
- refactor(frontend): fase 5a — las 54 plantillas de `partials/` pasan a `features/<dominio>/` (mismo
  nombre, solo cambia la carpeta), se borran las 2 muertas (`action_result.html`, `products_history.html`)
  y una guarda (`tests/unit/test_templates_layout.py`) impide volver a `partials/` y detecta rutas de
  plantilla rotas. HTML sin cambios (arnés y estilos idénticos a la baseline de bf38a2b).
```

No toques ninguna otra línea.

#### Verification
```bash
grep -n "5a ✅" docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md
grep -n "fase 5a" CHANGELOG.md
```

#### Expected Report
- [ ] Diagnóstico: fila 5 marcada con `**5a ✅**`
- [ ] CHANGELOG: bullet de fase 5a presente bajo el encabezado 0.5.0
- [ ] Ninguna otra línea modificada
