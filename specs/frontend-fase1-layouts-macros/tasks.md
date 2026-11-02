# Task Envelopes — Frontend modular, fase 1: layouts + macros `modal`/`field`

> SDD Phase 3 · PAE-016 · Release 0.5.0 (ningún commit sube versión) · Fuente: `requirements.md` (REQ-1..9),
> `design.md` (D1..D9, pre-mortem R1..R5)

**Generado por script** desde un worktree donde el resultado completo se construyó y verificó (409 unit + 47 e2e,
arnés 40/40, mutaciones detectadas). Simulación: aplicar estos envelopes a una copia limpia de `276b411`
reproduce ese worktree byte a byte.

**Ejecución:** `cc_local_pae ~/PyCharmMiscProject/gestion_inventario specs/frontend-fase1-layouts-macros/tasks.md
--test-cmd "SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest -p no:warnings"`
(sin `--proto`: no hay prototipo). Rama `feature/frontend-modular`, sin push.

**Herramienta e2e:** el proyecto no usa `behave`; su e2e es Playwright (`tests/e2e/`, `live_server`). REQ-5 y
REQ-8 dependen de render + interacción reales (el bug del prototipo solo se veía en el navegador) → Playwright
(T20). REQ-1..4, 6 se verifican completos con el arnés y unit tests.

**Herramientas de cc_local:** cada envelope declara `Tools` y `Skills`. Ningún envelope usa MCP (regla dura) ni
skills: todo el código va literal. Los comandos usan `.venv/bin/python` desde la raíz del repo.

**Trazabilidad:** REQ-1→T1,T2 · REQ-2→T3 · REQ-3→T4,T5,T6,T19 · REQ-4→T6,T7,T8,T9,T19 · REQ-5→T4,T19,T20 ·
REQ-6→T4,T10–T15,T19 · REQ-7→T10–T17 · REQ-8→T17,T18,T19,T20 · REQ-9→T21

Lotes: **A** T1–T3 · **B** T4–T9 · **C** T10–T13 · **D** T14–T16 · **E** T17–T18 · **F** T19–T20 · **G** T21–T21

**Orden:** A (arnés + baseline, ANTES de tocar plantillas) → B (macros y layouts) → C, D (modales) → E (unificado)
→ F (tests) → G (changelog). Cada lote = 1 commit.

---

### T1: Arnés de snapshots (1/2): canonizador y siembra

**Depends on:** none
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P3, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Firmas reales usadas: `InventoryService(db, business_id).purchase(PurchaseCreate)`, `.transfer(TransferCreate)`, `.sale(SaleCreate)`, `.adjustment(AdjustmentCreate)` (→ `MovementResult.movement.id`, `TransferResult.lines[0].movements_out`); `app.db.get_session()`; `app.settings.DATABASE_URL`; modelos `Customer`, `OperatingExpense`, `MoneyExtraction`, `SalesDocument`, `SalesDocumentItem`. `load_dotenv()` no sobrescribe variables ya puestas.

#### Objective

Crear la primera mitad del arnés lado servidor (REQ-1).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: crear `tests/snapshots/ui_snapshot.py` con EXACTAMENTE este contenido** (el T2 añade el resto al final):

```python
"""Arnés de snapshots del HTML de la UI (red de seguridad del release 0.5.0).

Renderiza cada página/parcial relevante con FastAPI TestClient contra un SQLite temporal
sembrado con datos fijos, normaliza el HTML a una secuencia canónica de tokens y la
guarda (``write``) o la compara con la guardada (``check``).

Uso (desde la raíz del repo):
    .venv/bin/python tests/snapshots/ui_snapshot.py write tests/snapshots/baseline
    .venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline

No forma parte de la suite de pytest. Es lado servidor: no ejecuta JS, así que no hay
carreras de Chart.js. Toda página debe devolver 200: si alguna no, el arnés falla
(nunca compara dos errores entre sí).
"""

from __future__ import annotations

import difflib
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TMP = tempfile.mkdtemp(prefix="ui-snapshot-")
os.environ.update({
    "DATABASE_URL": f"sqlite+pysqlite:///{_TMP}/snap.db",
    "SESSION_SECRET": "snapshot-secret",
    "SESSION_HTTPS_ONLY": "0",
    "ADMIN_USERNAME": "admin",
    "ADMIN_PASSWORD": "admin",
    "OPERATOR_USERNAME": "operator",
    "OPERATOR_PASSWORD": "operator",
})
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from app.db import get_session  # noqa: E402
from app.main import app  # noqa: E402
from app.models import (  # noqa: E402
    Business, Customer, InventoryMovement, Location, MoneyExtraction,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User,
)
from app.schemas import (  # noqa: E402
    AdjustmentCreate, PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate,
)
from app.services.inventory_service import InventoryService  # noqa: E402
from app.settings import DATABASE_URL  # noqa: E402

# Guarda dura: nunca sembrar en una BD real (el .env del repo apunta a la de dev).
if not DATABASE_URL.startswith("sqlite") or _TMP not in DATABASE_URL:
    raise SystemExit(f"ui_snapshot: DATABASE_URL inesperada ({DATABASE_URL}); abortado")

_FIXED = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)

_SUBS = [
    (re.compile(r"\?v=[0-9A-Za-z]+"), "?v=<V>"),
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?"), "<DT>"),
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "<D>"),
    (re.compile(r"\d{2}/\d{2}/\d{4}"), "<D>"),
    (re.compile(r"\b\d{4}-\d{2}\b"), "<YM>"),
    (re.compile(r"\b\d{1,2}:\d{2}(:\d{2})?\b"), "<T>"),
    (re.compile(r"\b(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|"
                r"octubre|noviembre|diciembre)\b", re.IGNORECASE), "<MES>"),
]


def _norm(text: str) -> str:
    text = " ".join(text.split())
    for rx, repl in _SUBS:
        text = rx.sub(repl, text)
    return text


class _Canon(HTMLParser):
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


def canonical(html: str) -> str:
    parser = _Canon()
    parser.feed(html)
    parser.close()
    return "\n".join(parser.tokens) + "\n"


def _seed() -> dict:
    db = get_session()
    try:
        biz = db.scalar(select(Business).order_by(Business.id))
        bid = int(biz.id)
        central = db.scalar(select(Location).where(Location.business_id == bid, Location.role == "central"))
        pos = db.scalar(
            select(Location).where(Location.business_id == bid, Location.id != central.id).order_by(Location.id)
        )
        db.add(Product(
            business_id=bid, sku="SNAP-1", name="Producto Snapshot", category="Cat",
            min_stock=5, unit_of_measure="ud", default_purchase_cost=1.0,
            default_sale_price=2.0, lead_time_days=0,
        ))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        purchase = svc.purchase(PurchaseCreate(
            sku="SNAP-1", quantity=10, unit_cost=1.0, movement_date=_FIXED,
            lot_code="LOT-SNAP-1", location_code=central.code,
        ))
        transfer = svc.transfer(TransferCreate(
            from_location_code=central.code, to_location_code=pos.code,
            lines=[TransferLineCreate(sku="SNAP-1", quantity=3)], movement_date=_FIXED,
        ))
        sale = svc.sale(SaleCreate(
            sku="SNAP-1", quantity=1, unit_price=2.0, movement_date=_FIXED, location_code=pos.code,
        ))
        adjustment = svc.adjustment(AdjustmentCreate(
            sku="SNAP-1", quantity_delta=2, unit_cost=1.0, movement_date=_FIXED,
            location_code=central.code,
        ))
        customer = Customer(business_id=bid, client_id="C-SNAP", name="Cliente Snapshot",
                            address="Calle 1", phone="600000000", email="c@snap.test")
        db.add(customer)
        expense = OperatingExpense(business_id=bid, expense_date=_FIXED, amount=12.5, concept="Luz")
        extraction = MoneyExtraction(business_id=bid, extraction_date=_FIXED, party="Negocio",
                                     amount=20.0, concept="Retiro")
        db.add_all([expense, extraction])
        db.flush()
        doc = SalesDocument(
            business_id=bid, location_id=central.id, customer_id=customer.id, doc_type="F",
            year_month="202601", seq=1, code="F202601-0001", issue_date=_FIXED,
            issuer_name="Emisor", client_name=customer.name, client_id=customer.client_id,
            client_address=customer.address, subtotal=4.0, total=4.0,
        )
        db.add(doc)
        db.flush()
        db.add(SalesDocumentItem(document_id=doc.id, line_no=1, sku="SNAP-1",
                                 description="Producto Snapshot", unit_of_measure="ud",
                                 quantity=2, unit_price=2.0, line_total=4.0))
        user = db.scalar(select(User).where(User.username == "operator"))
        db.commit()
        transfer_in = db.scalar(
            select(InventoryMovement.id).where(
                InventoryMovement.business_id == bid, InventoryMovement.type == "transfer_in"
            )
        )
        return {
            "bid": bid,
            "purchase": purchase.movement.id,
            "transfer_out": transfer.lines[0].movements_out[0],
            "transfer_in": int(transfer_in),
            "transfer_ref": transfer.transfer_ref or "",
            "sale": sale.movement.id,
            "adjustment": adjustment.movement.id,
            "customer": customer.id,
            "expense": expense.id,
            "extraction": extraction.id,
            "doc": doc.id,
            "user": user.id,
        }
    finally:
        db.close()


```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python -c "import ast; ast.parse(open('tests/snapshots/ui_snapshot.py').read()); print('ok')"
# Expected: ok
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` creado (mitad 1)
- [ ] Sintaxis válida

---

### T2: Arnés de snapshots (2/2): páginas, captura y CLI

**Depends on:** T1
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/ui_snapshot.py`
**Pillars:** P1, P3, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

T1 ya define `canonical()`, `_seed() -> dict` (ids: bid, purchase, transfer_out, transfer_in, sale, adjustment, customer, expense, extraction, doc, user), `get_session`, `select`, `User`, `TestClient`, `app`. Rutas reales verificadas: impresiones en `/ui/transfers/print?movement_id=`, `/ui/restock-print`, `/ui/purchase/{id}/label`; `/ui/must-change-password-form` devuelve vacío salvo que el usuario tenga `must_change_password`.

#### Objective

Completar el arnés: lista de 40 páginas, captura con aborto ante status≠200, `write`/`check` (REQ-1).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: AÑADIR AL FINAL de `tests/snapshots/ui_snapshot.py` EXACTAMENTE este contenido** (sin tocar lo que escribió T1):

```python
TABS = [
    "home", "agent", "inventory", "purchases", "transfers", "coach", "sales", "documents",
    "customers", "expenses", "profit-items", "profit", "dividends", "history", "users",
    "businesses", "activity",
]


def _pages(ids: dict) -> dict[str, str]:
    pages = {"dashboard": "/ui/dashboard"}
    for slug in TABS:
        pages[f"tab_{slug}"] = f"/ui/tabs/{slug}"
    pages.update({
        "modal_business": f"/ui/business/{ids['bid']}/edit",
        "modal_customer": f"/ui/customer/{ids['customer']}/edit",
        "modal_expense": f"/ui/expense/{ids['expense']}/edit",
        "modal_extraction": f"/ui/extraction/{ids['extraction']}/edit",
        "modal_product": "/ui/product/SNAP-1/edit",
        "modal_product_inventory": "/ui/product/SNAP-1/edit-inventory",
        "modal_profit_items_adjustment": f"/ui/tabs/profit-items/adjustment/{ids['adjustment']}/edit",
        "modal_profit_items_purchase": f"/ui/tabs/profit-items/purchase/{ids['purchase']}/edit",
        "modal_profit_items_sale": f"/ui/tabs/profit-items/sale/{ids['sale']}/edit",
        "modal_profit_items_transfer_in": f"/ui/tabs/profit-items/transfer-in/{ids['transfer_in']}/edit",
        "modal_purchase": f"/ui/movement/purchase/{ids['purchase']}/edit",
        "modal_sale": f"/ui/movement/sale/{ids['sale']}/edit",
        "modal_sales_document": f"/ui/sales-doc/{ids['doc']}/edit",
        "modal_transfer_bulk": "/ui/transfers/bulk",
        "modal_transfer": f"/ui/movement/transfer/{ids['transfer_out']}/edit",
        "modal_user": f"/ui/user/{ids['user']}/edit",
        "modal_user_reset_password": f"/ui/user/{ids['user']}/reset-password-form",
        "print_transfer": f"/ui/transfers/print?movement_id={ids['transfer_out']}",
        "print_restock": "/ui/restock-print",
        "print_label": f"/ui/purchase/{ids['purchase']}/label",
    })
    return pages


def _force_password_change() -> None:
    db = get_session()
    try:
        admin = db.scalar(select(User).where(User.username == "admin"))
        admin.must_change_password = True
        db.commit()
    finally:
        db.close()


def _get(client: TestClient, key: str, url: str, hx: bool) -> str:
    resp = client.get(url, headers={"HX-Request": "true"} if hx else {})
    text = canonical(resp.text)
    if resp.status_code != 200 or text.count("\n") < 3:
        raise SystemExit(f"{key} {url} -> {resp.status_code}, {len(resp.text)} bytes: {resp.text[:200]!r}")
    return text


def capture() -> dict[str, str]:
    logging.disable(logging.CRITICAL)
    out: dict[str, str] = {}
    with TestClient(app) as client:
        out["login"] = _get(client, "login", "/ui/login", hx=False)
        login = client.post("/ui/login", data={"username": "admin", "password": "admin"},
                            follow_redirects=False)
        if login.status_code != 302:
            raise SystemExit(f"login falló: {login.status_code}")
        ids = _seed()
        for key, url in _pages(ids).items():
            out[key] = _get(client, key, url, hx=not key.startswith(("dashboard", "print_")))
        # Último: el modal solo se renderiza si el usuario debe cambiar la contraseña.
        _force_password_change()
        out["modal_must_change_password"] = _get(
            client, "modal_must_change_password", "/ui/must-change-password-form", hx=True
        )
    return out


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[1] not in ("write", "check"):
        print(__doc__)
        return 2
    target = Path(argv[2])
    snaps = capture()
    if argv[1] == "write":
        target.mkdir(parents=True, exist_ok=True)
        for old in target.glob("*.txt"):
            old.unlink()
        for key, text in snaps.items():
            (target / f"{key}.txt").write_text(text, encoding="utf-8")
        print(f"OK: {len(snaps)} snapshots escritos en {target}")
        return 0
    failures = 0
    expected_keys = {p.stem for p in target.glob("*.txt")}
    for key in sorted(expected_keys - set(snaps)):
        print(f"FALTA: {key}")
        failures += 1
    for key, text in snaps.items():
        path = target / f"{key}.txt"
        if not path.exists():
            print(f"NUEVO (sin baseline): {key}")
            failures += 1
            continue
        before = path.read_text(encoding="utf-8")
        if before != text:
            failures += 1
            print(f"DIFF: {key}")
            diff = difflib.unified_diff(before.splitlines(), text.splitlines(), "baseline", "actual", lineterm="", n=2)
            for line in list(diff)[:40]:
                print("   ", line)
    if failures:
        print(f"FALLO: {failures} de {len(snaps)} snapshots difieren")
        return 1
    print(f"OK: {len(snaps)} snapshots equivalentes")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py write /tmp/snap-t2 2>/dev/null | tail -1
# Expected: OK: 40 snapshots escritos en /tmp/snap-t2
.venv/bin/python tests/snapshots/ui_snapshot.py check /tmp/snap-t2 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes   (determinismo: dos ejecuciones idénticas)
```

#### Expected Report

- [ ] `tests/snapshots/ui_snapshot.py` completo
- [ ] `write` → 40 escritos; `check` → 40 equivalentes

---

### T3: Generar y commitear la baseline (antes de tocar plantillas)

**Depends on:** T2
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/snapshots/baseline/`
**Pillars:** P1, P4, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

El arnés de T1-T2 existe. `app/` NO debe tener cambios todavía: la baseline es la foto del HTML previo al refactor.

#### Objective

Generar `tests/snapshots/baseline/` (40 `.txt`) desde el código sin modificar (REQ-2).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2:** comprobar que `git status --porcelain app/` no muestra nada. Si muestra algo, PARAR y reportar.

**Step 3:** ejecutar `.venv/bin/python tests/snapshots/ui_snapshot.py write tests/snapshots/baseline 2>/dev/null | tail -1`

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
ls tests/snapshots/baseline/*.txt | wc -l
# Expected: 40
grep -c 'modal-overlay' tests/snapshots/baseline/modal_customer.txt
# Expected: 1
grep -l 'Not Found' tests/snapshots/baseline/*.txt | wc -l
# Expected: 0
```

#### Expected Report

- [ ] 40 ficheros en `tests/snapshots/baseline/`
- [ ] `app/` sin cambios

---

### T4: Macros `modal()` y `field()`

**Depends on:** T3
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/components/modal.html`, `app/templates/components/forms.html`
**Pillars:** P1, P2, P4, P5, P6, P7
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Esqueleto actual de modal (copiado en 18 ficheros): `<div class="modal-overlay" id=…><div class="modal-container"><div class="modal-content"><div class="modal-header"><h3 class="modal-title">…</h3><button type="button" class="modal-close" data-modal-close="<id>">&times;</button></div>…`. `app.js` cierra con `[data-modal-close]` y respeta `data-modal-static="1"`. Entorno Jinja2 con autoescape.

#### Objective

Crear las macros con cuerpo vía `caller()` y todo valor autoescapado — nunca `|safe` (REQ-5, REQ-6).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/components/modal.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{# Esqueleto de modal (overlay/container/header/cierre). El cuerpo va en {% call %}:
     {% from 'components/modal.html' import modal %}
     {% call modal('customer-edit-modal', 'Editar cliente') %} ...form... {% endcall %}
   `title` se autoescapa: si necesita markup, pasarlo como bloque
   {% set t %}Editar <code>{{ x }}</code>{% endset %} (Jinja2 lo marca seguro y escapa {{ x }}).
   Nunca construir HTML concatenando strings ni marcarlo como seguro aquí. #}
{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) -%}
<div class="modal-overlay" id="{{ id }}"{% if static %} data-modal-static="1"{% endif %}>
  <div class="modal-container{% if wide %} modal-container-wide{% endif %}"{% if container_style %} style="{{ container_style }}"{% endif %}>
    <div class="modal-content">
      <div class="modal-header">
        {% if heading == 'h2' %}<h2>{{ title }}</h2>{% else %}<h3 class="modal-title">{{ title }}</h3>{% endif %}
        {% if not static %}<button type="button" class="modal-close" data-modal-close="{{ id }}"{% if close_label %} aria-label="{{ close_label }}"{% endif %}>&times;</button>{% endif %}
      </div>
{{ caller() }}
    </div>
  </div>
</div>
{%- endmacro %}
```

**Step 3: escribir `app/templates/components/forms.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{# Fila de formulario <div class="row"><label/><input/></div>.
   `value=None` omite el atributo; para value="" pasar ''. `attrs` = atributos extra
   (hx-*, data-*, list, step, min, autocomplete...) como dict; todo se autoescapa. #}
{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) -%}
<div class="row"><label{% if id %} for="{{ id }}"{% endif %}>{{ label }}</label><input{% if id %} id="{{ id }}"{% endif %}{% if name %} name="{{ name }}"{% endif %}{% if type %} type="{{ type }}"{% endif %}{% for k, v in attrs.items() %} {{ k }}="{{ v }}"{% endfor %}{% if value is not none %} value="{{ value }}"{% endif %}{% if checked %} checked{% endif %}{% if required %} required{% endif %}{% if readonly %} readonly{% endif %}{% if disabled %} disabled{% endif %} /></div>
{%- endmacro %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python -c "from app.routers.ui_common import templates as t; [t.env.get_template(n) for n in ('components/modal.html','components/forms.html')]; print('ok')" 2>/dev/null
# Expected: ok
grep -c 'safe' app/templates/components/modal.html app/templates/components/forms.html
# Expected: 0 en ambos
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
```

#### Expected Report

- [ ] 2 ficheros creados
- [ ] Arnés: 40 equivalentes

---

### T5: `base.html` y `layouts/print.html`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/base.html`, `app/templates/layouts/print.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Las 5 plantillas con `<head>` propio comparten las líneas 1-5 (doctype, `<html lang="es">`, `<head>`, meta charset, meta viewport) y la 6 es `<title>`. Solo `dashboard.html` carga htmx/Chart.js/JsBarcode/app.css/app.js.

#### Objective

Crear el esqueleto común SIN assets y el layout de impresión (REQ-3).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/base.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>{% block title %}Inventario{% endblock %}</title>
    {# base.html NO carga assets: cada layout declara los suyos en este bloque. #}
    {% block head %}{% endblock %}
  </head>
  <body>
    {% block body %}{% endblock %}
  </body>
</html>
```

**Step 3: escribir `app/templates/layouts/print.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{# Layout de impresión. NO carga los assets de la app (htmx, Chart.js, JsBarcode, app.css,
   app.js): cada plantilla de impresión declara en {% block head %} solo lo que usa. #}
{% extends "base.html" %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python -c "from app.routers.ui_common import templates as t; h = t.env.get_template('base.html').render() + t.env.get_template('layouts/print.html').render(); print(sum(a in h for a in ('htmx', 'chart.umd', 'JsBarcode', 'app.css', 'app.js')))" 2>/dev/null
# Expected: 0
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
```

#### Expected Report

- [ ] 2 ficheros creados, sin assets
- [ ] Arnés: 40 equivalentes

---

### T6: `layouts/app_shell.html` y `dashboard.html` con `extends`

**Depends on:** T5
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/layouts/app_shell.html`, `app/templates/dashboard.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existe `base.html` (bloques `title` [por defecto `Inventario`], `head`, `body`; sin assets). El contenido del `<head>` de `dashboard.html` (script de tema + htmx, Chart.js, JsBarcode, app.css, app.js) pasa tal cual al layout; el título del dashboard ya es `Inventario`. Baseline del HTML previo en `tests/snapshots/baseline/`.

#### Objective

El dashboard hereda de `layouts/app_shell.html`, que es el único sitio que carga los assets de la app (REQ-3, REQ-4).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/layouts/app_shell.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{# Layout de la app (dashboard): tema + htmx, Chart.js, JsBarcode, app.css, app.js. #}
{% extends "base.html" %}
{% block head %}
    <script>
      // Aplicar el tema guardado ANTES del paint (evita flash de color).
      (function () {
        try {
          var t = localStorage.getItem("gi-theme") || "light";
          document.documentElement.setAttribute("data-theme", t);
        } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
      })();
    </script>
    <script src="/static/js/htmx.min.js?v={{ static_version() }}"></script>
    <script src="/static/js/chart.umd.min.js?v={{ static_version() }}"></script>
    <script src="/static/js/JsBarcode.all.min.js?v={{ static_version() }}"></script>
    <link rel="stylesheet" href="/static/css/app.css?v={{ static_version() }}" />
    <script src="/static/js/app.js?v={{ static_version() }}" defer></script>
{% endblock %}
```

**Step 3: en `app/templates/dashboard.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 3.1 — BEFORE:
```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Inventario</title>
    <script>
      // Aplicar el tema guardado ANTES del paint (evita flash de color).
      (function () {
        try {
          var t = localStorage.getItem("gi-theme") || "light";
          document.documentElement.setAttribute("data-theme", t);
        } catch (e) { document.documentElement.setAttribute("data-theme", "light"); }
      })();
    </script>
    <script src="/static/js/htmx.min.js?v={{ static_version() }}"></script>
    <script src="/static/js/chart.umd.min.js?v={{ static_version() }}"></script>
    <script src="/static/js/JsBarcode.all.min.js?v={{ static_version() }}"></script>
    <link rel="stylesheet" href="/static/css/app.css?v={{ static_version() }}" />
    <script src="/static/js/app.js?v={{ static_version() }}" defer></script>
  </head>
  <body>
```
AFTER:
```jinja
{% extends "layouts/app_shell.html" %}
{% block body %}
```

Step 3.2 — BEFORE:
```jinja
  </body>
</html>
```
AFTER:
```jinja
{% endblock %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
head -1 app/templates/dashboard.html
# Expected: {% extends "layouts/app_shell.html" %}
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
```

#### Expected Report

- [ ] `layouts/app_shell.html` creado; `dashboard.html` con 2 sustituciones
- [ ] Arnés: 40 equivalentes

---

### T7: `login.html` con `extends base.html`

**Depends on:** T5
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/login.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existen `base.html` (bloques `title`, `head`, `body`; sin assets) y `layouts/print.html` (`extends base.html`, sin assets). Las líneas 1-5 de esta plantilla son idénticas a las de `base.html`; la 6 es su `<title>`. Su `<head>` actual NO carga assets de la app y no debe empezar a cargarlos.

#### Objective

Migrar el login a herencia sin cambiar su HTML (REQ-4).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: en `app/templates/login.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 2.1 — BEFORE:
```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Login</title>
```
AFTER:
```jinja
{% extends "base.html" %}
{% block title %}Login{% endblock %}
{% block head %}
```

Step 2.2 — BEFORE:
```jinja
  </head>
  <body>
```
AFTER:
```jinja
{% endblock %}
{% block body %}
```

Step 2.3 — BEFORE:
```jinja
  </body>
</html>
```
AFTER:
```jinja
{% endblock %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
head -1 app/templates/login.html
# Expected: {% extends "base.html" %}
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
```

#### Expected Report

- [ ] 3 sustituciones en `login.html`
- [ ] Arnés: 40 equivalentes (incluye `login`)

---

### T8: `transfer_print.html` y `restock_print.html` con `extends layouts/print.html`

**Depends on:** T5
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/transfer_print.html`, `app/templates/restock_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existen `base.html` (bloques `title`, `head`, `body`; sin assets) y `layouts/print.html` (`extends base.html`, sin assets). Las líneas 1-5 de esta plantilla son idénticas a las de `base.html`; la 6 es su `<title>`. Su `<head>` actual NO carga assets de la app y no debe empezar a cargarlos.

#### Objective

Migrar 2 impresiones a herencia sin cargar assets nuevos (REQ-4).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: en `app/templates/transfer_print.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 2.1 — BEFORE:
```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Traspaso {{ ref }}</title>
```
AFTER:
```jinja
{% extends "layouts/print.html" %}
{% block title %}Traspaso {{ ref }}{% endblock %}
{% block head %}
```

Step 2.2 — BEFORE:
```jinja
  </head>
  <body>
```
AFTER:
```jinja
{% endblock %}
{% block body %}
```

Step 2.3 — BEFORE:
```jinja
  </body>
</html>
```
AFTER:
```jinja
{% endblock %}
```

**Step 3: en `app/templates/restock_print.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 3.1 — BEFORE:
```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Artículos a reponer{% if selected_location_name %} - {{ selected_location_name }}{% endif %}</title>
```
AFTER:
```jinja
{% extends "layouts/print.html" %}
{% block title %}Artículos a reponer{% if selected_location_name %} - {{ selected_location_name }}{% endif %}{% endblock %}
{% block head %}
```

Step 3.2 — BEFORE:
```jinja
  </head>
  <body>
```
AFTER:
```jinja
{% endblock %}
{% block body %}
```

Step 3.3 — BEFORE:
```jinja
  </body>
</html>
```
AFTER:
```jinja
{% endblock %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -cE 'htmx|app\.js|app\.css' tests/snapshots/baseline/print_restock.txt
# Expected: 0
```

#### Expected Report

- [ ] 3 sustituciones en cada fichero
- [ ] Arnés: 40 equivalentes (incluye `print_transfer`, `print_restock`)

---

### T9: `label_single_print.html` con `extends layouts/print.html`

**Depends on:** T5
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/label_single_print.html`
**Pillars:** P1, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existen `base.html` (bloques `title`, `head`, `body`; sin assets) y `layouts/print.html` (`extends base.html`, sin assets). Las líneas 1-5 de esta plantilla son idénticas a las de `base.html`; la 6 es su `<title>`. Su `<head>` actual NO carga assets de la app y no debe empezar a cargarlos. Esta es la única impresión que carga `JsBarcode.all.min.js`: se queda en su bloque `head`.

#### Objective

Migrar la etiqueta a herencia conservando solo JsBarcode (REQ-4).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: en `app/templates/label_single_print.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 2.1 — BEFORE:
```jinja
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Etiqueta</title>
```
AFTER:
```jinja
{% extends "layouts/print.html" %}
{% block title %}Etiqueta{% endblock %}
{% block head %}
```

Step 2.2 — BEFORE:
```jinja
  </head>
  <body>
```
AFTER:
```jinja
{% endblock %}
{% block body %}
```

Step 2.3 — BEFORE:
```jinja
  </body>
</html>
```
AFTER:
```jinja
{% endblock %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c '{% extends' app/templates/*.html | grep -c ':1'
# Expected: 5
```

#### Expected Report

- [ ] 3 sustituciones
- [ ] Arnés: 40 equivalentes
- [ ] 5 plantillas raíz con `extends`

---

### T10: Migrar `customer_edit_form.html` y `expense_edit_form.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/customer_edit_form.html`, `app/templates/partials/expense_edit_form.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `customer_edit_form.html` y `expense_edit_form.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/customer_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('customer-edit-modal', 'Editar cliente') %}
      <form hx-post="/ui/customer/{{ customer.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('ID cliente', 'client_id', value=customer.client_id, required=True) }}
        {{ field('Nombre', 'name', value=customer.name, required=True) }}
        {{ field('Dirección (opcional)', 'address', value=customer.address or '') }}
        {{ field('Teléfono (opcional)', 'phone', value=customer.phone or '') }}
        {{ field('Email (opcional)', 'email', value=customer.email or '') }}
        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="customer-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/expense_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('expense-edit-modal', 'Editar gasto') %}
      <form hx-post="/ui/expense/{{ expense.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('Fecha', 'expense_date', type='datetime-local', value=expense_date_value or '') }}
        {{ field('Cantidad', 'amount', type='number', value=expense.amount or 0, required=True, attrs={'step': '0.0001'}) }}
        {{ field('Concepto', 'concept', value=expense.concept, required=True) }}
        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="expense-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/customer_edit_form.html app/templates/partials/expense_edit_form.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `customer_edit_form.html` y `expense_edit_form.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T11: Migrar `extraction_edit_form.html` y `must_change_password_form.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/extraction_edit_form.html`, `app/templates/partials/must_change_password_form.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `extraction_edit_form.html` y `must_change_password_form.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/extraction_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('extraction-edit-modal', 'Editar retiro') %}
      <form hx-post="/ui/extraction/{{ extraction.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('Fecha', 'extraction_date', type='datetime-local', value=extraction_date_value or '') }}
        <div class="row">
          <label>Parte</label>
          <select name="party" style="width:100%; padding:8px; border:1px solid var(--brand-border); border-radius:8px; background: var(--brand-surface); color: var(--brand-text);">
            <option value="{{ dividends.business_label }}" {% if extraction.party == dividends.business_label %}selected{% endif %}>{{ dividends.business_label }}</option>
            {% for p in dividends.partners %}
              <option value="{{ p }}" {% if extraction.party == p %}selected{% endif %}>{{ p }}</option>
            {% endfor %}
          </select>
        </div>
        {{ field('Cantidad', 'amount', type='number', value=extraction.amount or 0, required=True, attrs={'step': '0.0001'}) }}
        {{ field('Concepto', 'concept', value=extraction.concept, required=True) }}
        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="extraction-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/must_change_password_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('must-change-password-modal', 'Cambiar Contraseña', static=True) %}
      {% if message %}
      <div class="message {{ message_class }}">
        <strong>{{ message }}</strong>
        {% if message_detail %}<div class="muted">{{ message_detail }}</div>{% endif %}
      </div>
      {% endif %}

      <form hx-post="/ui/must-change-password" hx-target="#modal-container" hx-swap="innerHTML">
        {{ field('Nueva Contraseña *', 'new_password', type='password', id='mcp-new-password', required=True, attrs={'autocomplete': 'new-password'}) }}
        {{ field('Confirmar Contraseña *', 'confirm_password', type='password', id='mcp-confirm-password', required=True, attrs={'autocomplete': 'new-password'}) }}

        <button type="submit">Guardar Contraseña</button>
      </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/extraction_edit_form.html app/templates/partials/must_change_password_form.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `extraction_edit_form.html` y `must_change_password_form.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T12: Migrar `user_reset_password_form.html` y `sale_edit_form.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/user_reset_password_form.html`, `app/templates/partials/sale_edit_form.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `user_reset_password_form.html` y `sale_edit_form.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/user_reset_password_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('user-reset-password-modal', 'Resetear Contraseña: ' ~ user.username) %}
      <form hx-post="/ui/user/{{ user.id }}/reset-password" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('Nueva Contraseña *', 'new_password', type='password', id='new-password', required=True, attrs={'autocomplete': 'new-password'}) }}
        <div class="row">
          <label>
            <input type="checkbox" name="must_change" value="true" />
            Forzar cambio de contraseña en el próximo login
          </label>
          <div></div>
        </div>

        <button type="submit">Resetear Contraseña</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="user-reset-password-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/sale_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('sale-edit-modal', 'Editar venta') %}
      <form hx-post="/ui/movement/sale/{{ movement.id }}/update" hx-target="#sale-panel" hx-swap="outerHTML">
        {{ field('Artículo', 'product', value=product_label, required=True, attrs={'list': 'product-options'}) }}
        {{ field('Cantidad', 'quantity', type='number', value=(movement.quantity * -1) if movement.quantity < 0 else movement.quantity, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Precio unitario', 'unit_price', type='number', value=movement.unit_price or 0, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Fecha', 'movement_date', type='datetime-local', value=movement_date_value, attrs={'step': '0.001'}) }}
        {{ field('Nota', 'note', value=movement.note or '') }}
        {{ field('Pagado por transferencia', 'paid_by_transfer', type='checkbox', value='1', checked=movement.paid_by_transfer) }}

        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="sale-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/user_reset_password_form.html app/templates/partials/sale_edit_form.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `user_reset_password_form.html` y `sale_edit_form.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T13: Migrar `purchase_edit_form.html` y `transfer_edit_form.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/purchase_edit_form.html`, `app/templates/partials/transfer_edit_form.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `purchase_edit_form.html` y `transfer_edit_form.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/purchase_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('purchase-edit-modal', 'Editar compra') %}
      <form hx-post="/ui/movement/purchase/{{ movement.id }}/update" hx-target="#purchase-panel" hx-swap="outerHTML">
        {{ field('Artículo', 'product', value=product_label, required=True, attrs={'list': 'product-options', 'data-product-suggest': 'label'}) }}
        {{ field('Cantidad', 'quantity', type='number', value=movement.quantity, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Costo unitario (' ~ (purchase_currency_code or 'EUR') ~ ')', 'unit_cost', type='number', value=movement.unit_cost or 0, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Fecha', 'movement_date', type='datetime-local', value=movement_date_value) }}
        {{ field('Lote', 'lot_code', value=lot_code) }}
        {{ field('Nota', 'note', value=movement.note or '') }}

        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="purchase-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/transfer_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/ui.html' import flash %}
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('transfer-edit-modal', 'Editar traspaso') %}
      {{ flash(message, message_class or '', message_detail) }}

      <form hx-post="/ui/movement/transfer/{{ movement.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('Artículo', 'product', value=product_label, disabled=True) }}
        {{ field('Cantidad', 'quantity', type='number', value=quantity_value, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Fecha', 'movement_date', type='datetime-local', value=movement_date_value) }}
        {{ field('Nota', 'note', value=note_value or '') }}

        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="transfer-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/purchase_edit_form.html app/templates/partials/transfer_edit_form.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `purchase_edit_form.html` y `transfer_edit_form.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T14: Migrar `product_edit_form.html` y `product_edit_form_inventory.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/product_edit_form.html`, `app/templates/partials/product_edit_form_inventory.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `product_edit_form.html` y `product_edit_form_inventory.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/product_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('product-edit-modal', 'Editar producto') %}

      {% if form_error %}
        <div class="error" style="padding:10px;border-radius:8px;margin-bottom:10px;"><strong>Error al actualizar producto</strong><div>{{ form_error }}</div></div>
      {% endif %}

      <form hx-post="/ui/product/{{ product.sku }}/update" hx-target="#product-panel" hx-swap="outerHTML" hx-encoding="multipart/form-data" enctype="multipart/form-data">
        {{ field('SKU', 'new_sku', value=new_sku_value if new_sku_value is defined else product.sku) }}
        {{ field('Nombre', 'name', value=product.name, required=True) }}
        {{ field('Unidad', 'unit_of_measure', value=product.unit_of_measure or '') }}
        <div class="row">
          <label>Imagen</label>
          <div>
            {% if product.image_url %}
              <div style="margin-bottom:6px;">
                <img src="{{ product.image_url }}" alt="{{ product.name }}" style="width:64px; height:64px; object-fit:cover; border-radius:8px; border:1px solid #e5e7eb;" />
              </div>
            {% endif %}
            <input name="image_file" type="file" accept="image/*" />
          </div>
        </div>
        {{ field('Categoría', 'category', value=product.category or '') }}
        {{ field('Stock mínimo', 'min_stock', type='number', value=product.min_stock or 0, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Costo compra por defecto', 'default_purchase_cost', type='number', value=product.default_purchase_cost or '', attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Precio venta por defecto', 'default_sale_price', type='number', value=product.default_sale_price or '', attrs={'step': '0.0001', 'min': '0'}) }}
        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="product-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/product_edit_form_inventory.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('product-inventory-edit-modal', 'Editar producto') %}

      <form hx-post="/ui/product/{{ product.sku }}/update-inventory" hx-target="#tab-content" hx-swap="innerHTML" hx-encoding="multipart/form-data" enctype="multipart/form-data">
        <input type="hidden" name="location_code" value="{{ location_code or '' }}" />
        {{ field('SKU', 'new_sku', value=product.sku) }}
        {{ field('Nombre', 'name', value=product.name, required=True) }}
        {{ field('Unidad', 'unit_of_measure', value=product.unit_of_measure or '') }}
        <div class="row">
          <label>Imagen</label>
          <div>
            {% if product.image_url %}
              <div style="margin-bottom:6px;">
                <img src="{{ product.image_url }}" alt="{{ product.name }}" style="width:64px; height:64px; object-fit:cover; border-radius:8px; border:1px solid #e5e7eb;" />
              </div>
            {% endif %}
            <input name="image_file" type="file" accept="image/*" />
          </div>
        </div>
        {{ field('Categoría', 'category', value=product.category or '') }}
        {{ field('Stock actual', 'desired_stock', type='number', value=current_stock or 0, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Inventario inicial', 'initial_inventory', type='checkbox', value='1') }}
        {{ field('Stock mínimo', 'min_stock', type='number', value=product.min_stock or 0, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Tiempo de aprovisionamiento (días)', 'lead_time_days', type='number', value=product.lead_time_days or 0, attrs={'step': '1', 'min': '0'}) }}
        {{ field('Costo compra por defecto', 'default_purchase_cost', type='number', value=product.default_purchase_cost or '', attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Precio venta por defecto', 'default_sale_price', type='number', value=product.default_sale_price or '', attrs={'step': '0.0001', 'min': '0'}) }}
        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="product-inventory-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/product_edit_form.html app/templates/partials/product_edit_form_inventory.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `product_edit_form.html` y `product_edit_form_inventory.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T15: Migrar `user_edit_form.html` y `sales_document_edit_form.html` a `modal()`/`field()`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/user_edit_form.html`, `app/templates/partials/sales_document_edit_form.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar.

#### Objective

Reescribir `user_edit_form.html` y `sales_document_edit_form.html` con la macro de modal y `field()`, sin cambiar el HTML renderizado (REQ-6, REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/user_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% call modal('user-edit-modal', 'Editar Usuario: ' ~ user.username) %}

      {% if message %}
      <div class="message {{ message_class }}">
        <strong>{{ message }}</strong>
        {% if message_detail %}<div class="muted">{{ message_detail }}</div>{% endif %}
      </div>
      {% endif %}

      <form hx-post="/ui/user/{{ user.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        {{ field('Usuario *', 'username', type='text', id='edit-username', value=user.username, required=True, attrs={'autocomplete': 'off'}) }}

        <div class="row">
          <label for="edit-role">Rol *</label>
          <select id="edit-role" name="role" required>
            <option value="operator" {% if (user.role or '')|lower == 'operator' %}selected{% endif %}>Operador</option>
            <option value="owner" {% if (user.role or '')|lower == 'owner' %}selected{% endif %}>Dueño</option>
            <option value="admin" {% if (user.role or '')|lower == 'admin' %}selected{% endif %}>Administrador</option>
          </select>
        </div>

        <div class="row">
          <label>Negocios</label>
          <div>
            <div class="checkbox-group">
              {% for b in businesses %}
              <label class="checkbox-label">
                <input type="checkbox" name="business_ids" value="{{ b.id }}" {% if b.id in user_business_ids %}checked{% endif %} /> {{ b.name }}
              </label>
              {% endfor %}
            </div>
            <small class="muted">Dueño puede tener varios negocios; Operador usa el primero seleccionado</small>
          </div>
        </div>

        <div class="row">
          <label>
            <input type="checkbox" name="is_active" value="true" {% if user.is_active %}checked{% endif %} />
            Usuario activo
          </label>
          <div></div>
        </div>

        <button type="submit">Guardar Cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="user-edit-modal">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: escribir `app/templates/partials/sales_document_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{% set doc_title %}Editar documento <code>{{ doc.code }}</code>{% endset %}
{% call modal('sales-doc-edit-modal', doc_title, wide=True) %}

      <form hx-post="/ui/sales-doc/{{ doc.id }}/update" hx-target="#sales-doc-panel" hx-swap="outerHTML">
    <div class="grid grid-2">
      {{ field('Cliente (Nombre)', 'client_name', value=doc.client_name, required=True) }}
      {{ field('Cliente (ID)', 'client_id', value=doc.client_id, required=True) }}
    </div>
    {{ field('Dirección (opcional)', 'client_address', value=doc.client_address or '') }}
    {{ field('Notas (opcional)', 'notes', value=doc.notes or '') }}

    <h4 style="margin-top:14px;">Items</h4>
    <div class="table-wrapper">
      <table class="sales-doc-items-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Descripción</th>
            <th class="text-right">Cant.</th>
            <th class="text-right">Precio</th>
            <th class="text-right">Eliminar</th>
          </tr>
        </thead>
        <tbody>
          {% for it in items %}
            <tr>
              <td>{{ it.line_no }}</td>
              <td><input name="desc_{{ it.id }}" class="item-desc-input" value="{{ it.description }}" /></td>
              <td class="text-right"><input name="qty_{{ it.id }}" class="item-num-input" type="number" step="0.0001" min="0" value="{{ it.quantity }}" /></td>
              <td class="text-right"><input name="price_{{ it.id }}" class="item-num-input" type="number" step="0.0001" min="0" value="{{ it.unit_price }}" /></td>
              <td class="text-right"><input name="del_{{ it.id }}" type="checkbox" /></td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <h4 class="mt-14">Añadir item</h4>
    <div class="sales-doc-add-item-grid">
      <div class="add-item-field">
        <label>Descripción</label>
        <input name="new_desc" />
      </div>
      <div class="add-item-field">
        <label>Cantidad</label>
        <input name="new_qty" type="number" step="0.0001" min="0" value="1" />
      </div>
      <div class="add-item-field">
        <label>Precio</label>
        <input name="new_price" type="number" step="0.0001" min="0" value="0" />
      </div>
    </div>

    <button type="submit" class="mt-12">Guardar cambios</button>
    <button type="button" class="btn-secondary ml-8" data-modal-close="sales-doc-edit-modal">Cancelar</button>
  </form>
{% endcall %}
```


**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'modal-overlay' app/templates/partials/user_edit_form.html app/templates/partials/sales_document_edit_form.html
# Expected: 0 en cada fichero
```

#### Expected Report

- [ ] Ficheros reescritos: `user_edit_form.html` y `sales_document_edit_form.html`
- [ ] Ningún otro fichero modificado
- [ ] Arnés: `OK: 40 snapshots equivalentes`

---

### T16: Esqueleto de `business_edit_form.html` y `transfer_bulk_modal.html`

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/business_edit_form.html`, `app/templates/partials/transfer_bulk_modal.html`
**Pillars:** P1, P2, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar. Ficheros largos: solo cambian la cabecera del modal y las 3 líneas de cierre; el resto queda igual.

#### Objective

Migrar los 2 modales largos a `modal()` solo en el esqueleto (REQ-7).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: en `app/templates/partials/business_edit_form.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 2.1 — BEFORE:
```jinja
<div class="modal-overlay" id="business-edit-modal">
  <div class="modal-container modal-container-wide">
    <div class="modal-content">
      <div class="modal-header">
        <h2>Editar Negocio</h2>
        <button type="button" class="modal-close" data-modal-close="business-edit-modal" aria-label="Cerrar">&times;</button>
      </div>
```
AFTER:
```jinja
{% from 'components/modal.html' import modal %}
{% call modal('business-edit-modal', 'Editar Negocio', wide=True, heading='h2', close_label='Cerrar') %}
```

Step 2.2 — BEFORE:
```jinja
      </form>
    </div>
  </div>
</div>
```
AFTER:
```jinja
      </form>
{% endcall %}
```

**Step 3: en `app/templates/partials/transfer_bulk_modal.html`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 3.1 — BEFORE:
```jinja
{% from 'components/ui.html' import flash %}
<div class="modal-overlay" id="transfer-bulk-modal">
  <div class="modal-container" style="max-width: 760px;">
    <div class="modal-content">
      <div class="modal-header">
        <h3 class="modal-title">Traspaso masivo</h3>
        <button type="button" class="modal-close" data-modal-close="transfer-bulk-modal">&times;</button>
      </div>
```
AFTER:
```jinja
{% from 'components/ui.html' import flash %}
{% from 'components/modal.html' import modal %}
{% call modal('transfer-bulk-modal', 'Traspaso masivo', container_style='max-width: 760px;') %}
```

Step 3.2 — BEFORE:
```jinja
      </form>
    </div>
  </div>
</div>
```
AFTER:
```jinja
      </form>
{% endcall %}
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -rl 'class="modal-overlay"' app/templates/partials/ | grep -v profit_items_ | wc -l
# Expected: 0
```

#### Expected Report

- [ ] 2 sustituciones en cada fichero
- [ ] Arnés: 40 equivalentes

---

### T17: Modal unificado de Utilidades por artículo + router

**Depends on:** T4
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/profit_items_edit_form.html`, `app/routers/ui_profit_items.py`
**Pillars:** P1, P2, P3, P4, P5, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

Ya existen (lotes A-B): `components/modal.html` → `{% macro modal(id, title, wide=False, heading='h3', static=False, close_label=None, container_style=None) %}` (cuerpo vía `{% call %}`) y `components/forms.html` → `{% macro field(label, name=None, type=None, value=None, id=None, required=False, readonly=False, disabled=False, checked=False, attrs={}) %}` (fila `<div class="row"><label/><input/></div>`). Baseline del HTML previo en `tests/snapshots/baseline/` (lote A). El HTML renderizado NO debe cambiar. Los 4 handlers GET `/ui/tabs/profit-items/{adjustment|transfer-in|sale|purchase}/{movement_id}/edit` de `app/routers/ui_profit_items.py` renderizan hoy `partials/profit_items_<kind>_edit_form.html` con `movement`, `product_label`, `movement_date_value`, `filter_*` y (sale/purchase) `product_options`, (purchase) `lot_code`. No cambiar nada más de los handlers.

#### Objective

Un único modal parametrizado por `kind` sustituye a los 4 (REQ-8).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `app/templates/partials/profit_items_edit_form.html` con EXACTAMENTE este contenido** (crear o sobrescribir):

```jinja
{% from 'components/modal.html' import modal %}
{% from 'components/forms.html' import field %}
{# Modal de edición de "Utilidades por artículo". `kind` lo pasa el router:
   sale | purchase → artículo, cantidad, precio/costo, fecha y nota editables (purchase añade lote);
   adjustment | transfer_in → solo el costo unitario es editable. #}
{% set slug = kind|replace('_', '-') %}
{% set titles = {'sale': 'Editar venta', 'purchase': 'Editar compra (costo)', 'adjustment': 'Editar ajuste (costo)', 'transfer_in': 'Editar traspaso (costo)'} %}
{% set modal_id = 'profit-items-' ~ slug ~ '-edit-modal' %}
{% set list_id = 'profit-items-product-options' if kind == 'sale' else 'profit-items-purchase-product-options' %}
{% call modal(modal_id, titles[kind]) %}
      <form hx-post="/ui/tabs/profit-items/{{ slug }}/{{ movement.id }}/update" hx-target="#tab-content" hx-swap="innerHTML">
        <input type="hidden" name="query" value="{{ filter_query or '' }}" />
        <input type="hidden" name="start_date" value="{{ filter_start_date or '' }}" />
        <input type="hidden" name="end_date" value="{{ filter_end_date or '' }}" />
        <input type="hidden" name="location_code" value="{{ filter_location_code or '' }}" />

        {% if kind in ('adjustment', 'transfer_in') %}
        {{ field('Artículo', value=product_label, disabled=True) }}
        {{ field('Cantidad', 'quantity', type='number', value=movement.quantity, readonly=True, attrs={'step': '0.0001', 'min': '0'} if kind == 'transfer_in' else {'step': '0.0001'}) }}
        {{ field('Costo unitario', 'unit_cost', type='number', value=movement.unit_cost or 0, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Fecha', 'movement_date', type='datetime-local', value=movement_date_value, readonly=True) }}
        {{ field('Nota', 'note', value=movement.note or '', readonly=True) }}
        {% else %}
        {{ field('Artículo', 'product', value=product_label, required=True, attrs={'list': list_id, 'data-product-suggest': 'label'}) }}
        {% if kind == 'sale' %}
        {{ field('Cantidad', 'quantity', type='number', value=(movement.quantity * -1) if movement.quantity < 0 else movement.quantity, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Precio unitario', 'unit_price', type='number', value=movement.unit_price or 0, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {% else %}
        {{ field('Cantidad', 'quantity', type='number', value=movement.quantity, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {{ field('Costo unitario', 'unit_cost', type='number', value=movement.unit_cost or 0, required=True, attrs={'step': '0.0001', 'min': '0'}) }}
        {% endif %}
        {{ field('Fecha', 'movement_date', type='datetime-local', value=movement_date_value) }}
        {% if kind == 'purchase' %}
        {{ field('Lote', 'lot_code', value=lot_code) }}
        {% endif %}
        {{ field('Nota', 'note', value=movement.note or '') }}

        <datalist id="{{ list_id }}">
          {% for p in product_options %}
            <option value="{{ p.sku }} - {{ p.name }}"></option>
          {% endfor %}
        </datalist>
        {% endif %}

        <button type="submit">Guardar cambios</button>
        <button type="button" class="btn-secondary ml-8" data-modal-close="{{ modal_id }}">Cancelar</button>
      </form>
{% endcall %}
```

**Step 3: en `app/routers/ui_profit_items.py`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 3.1 — BEFORE:
```python
        name="partials/profit_items_adjustment_edit_form.html",
        context={
            "movement": mv,
```
AFTER:
```python
        name="partials/profit_items_edit_form.html",
        context={
            "kind": "adjustment",
            "movement": mv,
```

Step 3.2 — BEFORE:
```python
        name="partials/profit_items_transfer_in_edit_form.html",
        context={
            "movement": mv,
```
AFTER:
```python
        name="partials/profit_items_edit_form.html",
        context={
            "kind": "transfer_in",
            "movement": mv,
```

Step 3.3 — BEFORE:
```python
        name="partials/profit_items_sale_edit_form.html",
        context={
            "movement": mv,
```
AFTER:
```python
        name="partials/profit_items_edit_form.html",
        context={
            "kind": "sale",
            "movement": mv,
```

Step 3.4 — BEFORE:
```python
        name="partials/profit_items_purchase_edit_form.html",
        context={
            "movement": mv,
```
AFTER:
```python
        name="partials/profit_items_edit_form.html",
        context={
            "kind": "purchase",
            "movement": mv,
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
grep -c 'profit_items_edit_form.html' app/routers/ui_profit_items.py
# Expected: 4
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit --no-cov -p no:warnings 2>&1 | tail -1
# Expected: N passed (sin failed)
```

#### Expected Report

- [ ] Plantilla creada; 4 sustituciones en el router
- [ ] Arnés: 40 equivalentes (incluye los 4 `modal_profit_items_*`)

---

### T18: Borrar los 4 `profit_items_*_edit_form.html` huérfanos

**Depends on:** T17
**Skill:** ninguna
**Agent:** refactor
**Files:** `app/templates/partials/profit_items_sale_edit_form.html`, `app/templates/partials/profit_items_purchase_edit_form.html`, `app/templates/partials/profit_items_adjustment_edit_form.html`, `app/templates/partials/profit_items_transfer_in_edit_form.html`
**Pillars:** P1, P4, P6
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Tras T17 ninguna ruta renderiza estos 4 ficheros (`grep -rn "profit_items_sale_edit_form\|profit_items_purchase_edit_form\|profit_items_adjustment_edit_form\|profit_items_transfer_in_edit_form" app tests` → vacío).

#### Objective

Eliminar los 4 ficheros sustituidos por el unificado (REQ-8). Excepción a la regla de 2 ficheros: es un único `git rm`.

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2:** comprobar que el `grep` del Context no devuelve nada. Si devuelve algo, PARAR y reportar.

**Step 3:** `git rm app/templates/partials/profit_items_sale_edit_form.html app/templates/partials/profit_items_purchase_edit_form.html app/templates/partials/profit_items_adjustment_edit_form.html app/templates/partials/profit_items_transfer_in_edit_form.html`

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
ls app/templates/partials/ | grep -c 'profit_items_.*edit_form'
# Expected: 1   (solo profit_items_edit_form.html)
.venv/bin/python tests/snapshots/ui_snapshot.py check tests/snapshots/baseline 2>/dev/null | tail -1
# Expected: OK: 40 snapshots equivalentes
```

#### Expected Report

- [ ] 4 ficheros borrados con `git rm`
- [ ] Arnés: 40 equivalentes

---

### T19: Tests unitarios de macros, layouts y unificado

**Depends on:** T18
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/unit/test_ui_components.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

`from app.routers.ui_common import templates` (entorno Jinja2 de la app, autoescape activo, global `static_version`).

#### Objective

Fijar con tests los riesgos R1-R3 del pre-mortem (REQ-3, 4, 5, 6, 8).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `tests/unit/test_ui_components.py` con EXACTAMENTE este contenido** (crear o sobrescribir):

```python
"""Fase 1 del release 0.5.0: layouts (base/app_shell/print) y macros modal/field.

Cubre los riesgos que encontró la verificación del prototipo descartado:
- el cuerpo del modal va por caller() y se autoescapa (nada de HTML concatenado + |safe);
- las páginas de impresión y el login no heredan los assets de la app.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.routers.ui_common import templates

APP_ASSETS = ("htmx.min.js", "chart.umd.min.js", "JsBarcode.all.min.js", "app.css", "app.js")


def _render(source: str, **ctx) -> str:
    return templates.env.from_string(source).render(**ctx)


def test_modal_body_is_markup_and_values_are_escaped() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('x-modal', title) %}<form><input value=\"{{ name }}\" /></form>{% endcall %}",
        title="<b>T</b>",
        name='"><script>alert(1)</script>',
    )
    assert '<div class="modal-overlay" id="x-modal">' in html
    assert "<form><input" in html  # el cuerpo es HTML vivo, no texto escapado
    assert "&lt;b&gt;T&lt;/b&gt;" in html  # título autoescapado
    assert "<script>" not in html
    assert "&lt;script&gt;" in html
    assert 'data-modal-close="x-modal"' in html


def test_modal_title_block_keeps_markup_and_escapes_variables() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% set t %}Editar <code>{{ code }}</code>{% endset %}"
        "{% call modal('d-modal', t, wide=True) %}x{% endcall %}",
        code="<i>",
    )
    assert "Editar <code>&lt;i&gt;</code>" in html
    assert 'class="modal-container modal-container-wide"' in html


def test_modal_static_has_no_close_button() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('s-modal', 'S', static=True) %}x{% endcall %}"
    )
    assert 'data-modal-static="1"' in html
    assert "modal-close" not in html


def test_modal_h2_heading_and_close_label() -> None:
    html = _render(
        "{% from 'components/modal.html' import modal %}"
        "{% call modal('b-modal', 'B', heading='h2', close_label='Cerrar') %}x{% endcall %}"
    )
    assert "<h2>B</h2>" in html
    assert 'aria-label="Cerrar"' in html


def test_field_escapes_value_and_renders_flags() -> None:
    html = _render(
        "{% from 'components/forms.html' import field %}"
        "{{ field('Nombre', 'name', value=v, required=True, readonly=True, attrs={'list': 'opts'}) }}",
        v='"><script>x</script>',
    )
    assert "<script>" not in html
    assert "&#34;&gt;&lt;script&gt;" in html
    assert 'name="name"' in html and 'list="opts"' in html
    assert " required" in html and " readonly" in html


def test_field_value_none_omits_attribute_and_id_links_label() -> None:
    html = _render(
        "{% from 'components/forms.html' import field %}"
        "{{ field('Clave', 'pw', type='password', id='pw-id') }}"
    )
    assert "value=" not in html
    assert '<label for="pw-id">Clave</label>' in html
    assert 'id="pw-id"' in html


@pytest.mark.parametrize("name", ["base.html", "layouts/print.html"])
def test_base_and_print_layout_load_no_app_assets(name: str) -> None:
    html = templates.env.get_template(name).render()
    for asset in APP_ASSETS:
        assert asset not in html, f"{name} carga {asset}"


def test_app_shell_loads_all_app_assets() -> None:
    html = templates.env.get_template("layouts/app_shell.html").render(static_version=lambda: "1")
    for asset in APP_ASSETS:
        assert asset in html


@pytest.mark.parametrize(
    "name, parent",
    [
        ("dashboard.html", "layouts/app_shell.html"),
        ("login.html", "base.html"),
        ("transfer_print.html", "layouts/print.html"),
        ("restock_print.html", "layouts/print.html"),
        ("label_single_print.html", "layouts/print.html"),
    ],
)
def test_pages_extend_expected_layout(name: str, parent: str) -> None:
    source, _, _ = templates.env.loader.get_source(templates.env, name)
    assert source.startswith('{% extends "' + parent + '" %}')


@pytest.mark.parametrize(
    "kind, modal_id, editable_field",
    [
        ("sale", "profit-items-sale-edit-modal", "unit_price"),
        ("purchase", "profit-items-purchase-edit-modal", "lot_code"),
        ("adjustment", "profit-items-adjustment-edit-modal", "unit_cost"),
        ("transfer_in", "profit-items-transfer-in-edit-modal", "unit_cost"),
    ],
)
def test_profit_items_edit_form_renders_each_kind(kind: str, modal_id: str, editable_field: str) -> None:
    movement = SimpleNamespace(id=7, quantity=2.0, unit_cost=1.5, unit_price=3.0, note="n")
    html = templates.env.get_template("partials/profit_items_edit_form.html").render(
        kind=kind, movement=movement, product_label="SKU - Prod", movement_date_value="",
        lot_code="L1", product_options=[],
    )
    assert f'id="{modal_id}"' in html
    assert f'name="{editable_field}"' in html
    slug = kind.replace("_", "-")
    assert f'hx-post="/ui/tabs/profit-items/{slug}/7/update"' in html


@pytest.mark.parametrize("kind", ["sale", "purchase", "adjustment", "transfer_in"])
def test_old_profit_items_templates_are_gone(kind: str) -> None:
    from jinja2 import TemplateNotFound

    with pytest.raises(TemplateNotFound):
        templates.env.get_template(f"partials/profit_items_{kind}_edit_form.html")
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit/test_ui_components.py --no-cov -p no:warnings 2>&1 | tail -1
# Expected: 22 passed
SESSION_SECRET=test DATABASE_URL=sqlite+pysqlite:///:memory: .venv/bin/python -m pytest tests/unit --no-cov -p no:warnings 2>&1 | tail -1
# Expected: 409 passed
```

#### Expected Report

- [ ] Fichero creado
- [ ] 22 passed; suite unit 409 passed

---

### T20: e2e de los modales migrados

**Depends on:** T18
**Skill:** ninguna
**Agent:** coder
**Files:** `tests/e2e/test_modal_components_e2e.py`
**Pillars:** P1, P4, P5, P6, P7
**Tools (cc_local):** Read, Write, Bash · **Skills:** ninguna — código literal en el envelope, no cargar skills

#### Context

Fixture `live_server` (`tests/e2e/conftest.py`): uvicorn + SQLite temporal, admin/admin, productos `E2E-SKU1` ("Producto E2E Uno", lote `E2E-SKU1-E2E`) y `E2E-SKU2` con stock en `CENTRAL`. Marker `e2e`.

#### Objective

e2e del flujo real: editar cliente (cuerpo del modal = inputs, no texto), cerrar con ×, modales de Utilidades (REQ-5, REQ-8).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: escribir `tests/e2e/test_modal_components_e2e.py` con EXACTAMENTE este contenido** (crear o sobrescribir):

```python
"""Fase 1 del release 0.5.0: modales migrados a la macro components/modal.html.

Regresión del bug del prototipo descartado: el modal "Editar cliente" mostraba el HTML de los
campos como texto escapado en vez de inputs. También cubre el modal unificado de Utilidades.
"""

from __future__ import annotations

import pytest

pytest.importorskip("playwright")
pytest.importorskip("pytest_playwright")

from playwright.sync_api import Page, expect

pytestmark = pytest.mark.e2e

RAW_NAME = "Cliente <b>E2E</b>"


def _login(page: Page, base_url: str) -> None:
    page.goto(f"{base_url}/ui/login")
    page.fill('input[name="username"]', "admin")
    page.fill('input[name="password"]', "admin")
    page.click('button[type="submit"]')
    page.wait_for_url(f"{base_url}/ui/dashboard**")
    page.wait_for_function("typeof window.htmx !== 'undefined' && typeof window.htmx.trigger === 'function'")
    page.locator("#tab-content > *").first.wait_for()


def _create_customer_and_open_edit(page: Page) -> None:
    page.click("#tab-customers")
    form = page.locator('form[hx-post="/ui/customer/create"]')
    expect(form).to_be_visible()
    form.locator('input[name="client_id"]').fill("E2E-C1")
    form.locator('input[name="name"]').fill(RAW_NAME)
    form.locator('button[type="submit"]').click()
    row = page.locator("#customers-table tr", has_text="E2E-C1")
    expect(row).to_be_visible()
    row.locator("button", has_text="Editar").click()
    expect(page.locator("#customer-edit-modal")).to_be_visible()


def test_customer_edit_modal_renders_inputs_and_saves(live_server: str, page: Page) -> None:
    _login(page, live_server)
    _create_customer_and_open_edit(page)

    modal = page.locator("#customer-edit-modal")
    name_input = modal.locator('input[name="name"]')
    expect(name_input).to_have_value(RAW_NAME)
    expect(modal.locator('input[name="client_id"]')).to_have_value("E2E-C1")
    assert "<input" not in modal.inner_text()
    assert "&lt;" not in modal.inner_text()

    name_input.fill("Cliente E2E Editado")
    modal.locator('button[type="submit"]').click()
    expect(page.locator("#customer-edit-modal")).to_have_count(0)
    expect(page.locator("#customers-table").locator("text=Cliente E2E Editado")).to_be_visible()


def test_customer_edit_modal_closes_with_x(live_server: str, page: Page) -> None:
    _login(page, live_server)
    _create_customer_and_open_edit(page)

    page.locator("#customer-edit-modal .modal-close").click()
    expect(page.locator("#customer-edit-modal")).to_have_count(0)


def test_profit_items_unified_modals_open_for_sale_and_purchase(live_server: str, page: Page) -> None:
    _login(page, live_server)
    page.click("#tab-sales")
    sale_form = page.locator('#sale-panel form[hx-post="/ui/sale"]')
    sale_form.locator('select[name="location_code"]').select_option("CENTRAL")
    sale_form.locator('input[name="product"]').fill("E2E-SKU1")
    sale_form.locator('input[name="quantity"]').fill("1")
    sale_form.locator('input[name="unit_price"]').fill("2.00")
    sale_form.locator('button[type="submit"]').click()
    expect(page.locator("#sale-panel").locator("text=E2E-SKU1").first).to_be_visible()

    page.click("#tab-profit-items")
    row = page.locator("#tab-content tr", has_text="E2E-SKU1").first
    expect(row).to_be_visible()

    row.locator("button", has_text="Edit sale").click()
    sale_modal = page.locator("#profit-items-sale-edit-modal")
    expect(sale_modal).to_be_visible()
    expect(sale_modal.locator('input[name="product"]')).to_have_value("E2E-SKU1 - Producto E2E Uno")
    expect(sale_modal.locator('input[name="unit_price"]')).to_be_editable()
    sale_modal.locator(".modal-close").click()
    expect(sale_modal).to_have_count(0)

    row.locator("button", has_text="Edit buys").click()
    buy_modal = page.locator("#profit-items-purchase-edit-modal")
    expect(buy_modal).to_be_visible()
    expect(buy_modal.locator('input[name="lot_code"]')).to_have_value("E2E-SKU1-E2E")
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
.venv/bin/python -m pytest tests/e2e/test_modal_components_e2e.py -m e2e --no-cov -p no:warnings 2>&1 | tail -1
# Expected: 3 passed
```

#### Expected Report

- [ ] Fichero creado
- [ ] 3 passed

---

### T21: CHANGELOG de la fase 1 (sin bump de versión)

**Depends on:** T20
**Skill:** ninguna
**Agent:** coder
**Files:** `CHANGELOG.md`
**Pillars:** P1, P6
**Tools (cc_local):** Read, Edit, Bash · **Skills:** ninguna — sustituciones literales, no cargar skills

#### Context

`CHANGELOG.md` ya tiene la sección `## 0.5.0 (sin publicar — release del frontend modular, fases 0-5)`. El release se bumpea una sola vez al cerrar la fase 5.

#### Objective

Registrar la fase 1 en el changelog (REQ-9).

#### Instructions

**Step 1:** `Read` de cada fichero de `Files` que ya exista (para Edit).

**Step 2: en `CHANGELOG.md`, sustituir (Edit, `old_string` → `new_string`, cada bloque aparece una sola vez):**

Step 2.1 — BEFORE:
```jinja
- docs: diagnóstico de arquitectura del frontend (`docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`).
```
AFTER:
```jinja
- docs: diagnóstico de arquitectura del frontend (`docs/auditorias/2026-10-23-frontend-arquitectura-diagnostico.md`).
- refactor(frontend): fase 1 — `base.html` + `layouts/app_shell.html` y `layouts/print.html` con `{% extends %}`
  (dashboard, login y las 3 impresiones; login e impresiones siguen sin cargar htmx/Chart.js/JsBarcode/app.css/
  app.js). Macros `components/modal.html` (`modal()` con `caller()`) y `components/forms.html` (`field()`); los 18
  modales migrados y los 4 `profit_items_*_edit_form.html` unificados en `profit_items_edit_form.html`. HTML
  renderizado sin cambios, verificado con el arnés `tests/snapshots/ui_snapshot.py` (40 páginas, baseline en
  `tests/snapshots/baseline/`). +22 unit, +3 e2e.
```

**Reglas:**
- `git add` SOLO de los ficheros de `Files` (nunca `git add -A`/`.`: hay cambios ajenos sin commitear en `CLAUDE.md`, `.claude/`, `.idea/`, `.serena/`, `specs/copiloto-…`).
- No tocar `VERSION`, `.bumpversion.cfg` ni ningún otro fichero.

#### Verification

```bash
grep -c 'fase 1' CHANGELOG.md
# Expected: 1
git diff --name-only HEAD -- VERSION .bumpversion.cfg | wc -l
# Expected: 0
```

#### Expected Report

- [ ] Entrada añadida bajo 0.5.0
- [ ] `VERSION`/`.bumpversion.cfg` intactos

---
