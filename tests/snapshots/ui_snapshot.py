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

import atexit
import difflib
import logging
import os
import re
from urllib.parse import quote
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_TMP = tempfile.mkdtemp(prefix="ui-snapshot-")
atexit.register(shutil.rmtree, _TMP, ignore_errors=True)
# Hermético: fija TODAS las variables de app/settings.py que afectan al HTML renderizado,
# para que el arnés no dependa del .env/shell de la máquina donde corre (H-301). Sin esto,
# un `APP_MODE=desktop` heredado del entorno cambia `dashboard.html` sin que el arnés lo note.
for _key in list(os.environ):
    if _key.startswith("BUSINESS_CONFIG_PATH_"):
        del os.environ[_key]
os.environ.update({
    "DATABASE_URL": f"sqlite+pysqlite:///{_TMP}/snap.db",
    "SESSION_SECRET": "snapshot-secret",
    "SESSION_HTTPS_ONLY": "0",
    "ADMIN_USERNAME": "admin",
    "ADMIN_PASSWORD": "admin",
    "OPERATOR_USERNAME": "operator",
    "OPERATOR_PASSWORD": "operator",
    "APP_MODE": "server",
    "BUSINESS_CONFIG_PATH": "",
    "BUSINESS_CONFIG_DIR": "",
    "DEV_ACTIONS_ENABLED": "1",
    "AGENT_LLM_BASE_URL": "http://localhost:8080/v1",
    "AGENT_LLM_API_KEY": "",
})
os.chdir(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT))
# `app/settings.py` llama a `load_dotenv()` al importarse, que añade al entorno lo que falte:
# sin esto, el .env de la máquina volvería a meter variables que no fijamos arriba (p. ej.
# `BUSINESS_CONFIG_PATH_<CODE>`, que el bucle anterior acaba de borrar) (H-309).
try:
    import dotenv  # noqa: E402

    dotenv.load_dotenv = lambda *args, **kwargs: False
except ImportError:
    pass

from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import select  # noqa: E402

from app.agent.widgets import add_widget  # noqa: E402
from app.db import get_session  # noqa: E402
from app.main import app  # noqa: E402
from app.models import (  # noqa: E402
    Business, Customer, InventoryMovement, Location, MoneyExtraction,
    OperatingExpense, Product, SalesDocument, SalesDocumentItem, User, UserBusiness,
)
from app.schemas import (  # noqa: E402
    AdjustmentCreate, PurchaseCreate, SaleCreate, TransferCreate, TransferLineCreate,
)
from app.services.inventory_service import InventoryService  # noqa: E402
from app.auth import hash_password  # noqa: E402
from app.settings import DATABASE_URL  # noqa: E402
from tests.snapshots.style_classes import STYLE_TO_CLASSES, style_key  # noqa: E402

# Guarda dura: nunca sembrar en una BD real (el .env del repo apunta a la de dev).
if not DATABASE_URL.startswith("sqlite") or _TMP not in DATABASE_URL:
    raise SystemExit(f"ui_snapshot: DATABASE_URL inesperada ({DATABASE_URL}); abortado")

_FIXED = datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc)

# Inicio dibuja el mes en curso (un punto por día) y los últimos 12 meses a partir de
# `datetime.now()`: sin reloj fijo, la baseline deja de valer al cambiar de mes. Se congela en
# la fecha en que se generó la baseline actual.
_NOW = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return _NOW if tz is None else _NOW.astimezone(tz)


import app.routers.ui_home as _ui_home  # noqa: E402

_ui_home.datetime = _FrozenDatetime

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


# Fase 2 (JS a módulos): el canónico ignora el cableado JS, que cambia a propósito, para que
# la baseline generada con el código anterior a la fase siga sirviendo de referencia:
# - <script> ejecutables (inline o con src) y su contenido; los <script type="application/json">
#   (datos de los gráficos) sí cuentan. Qué scripts carga cada página lo fija un unit test.
# - atributos on* (onclick=…) y data-js-* (los enganches que los sustituyen).
# - los 3 <div id="…-debug"> de los gráficos de Inicio, eliminados en la fase 2.
_REMOVED_DEBUG_IDS = {"monthly-chart-debug", "monthly-sales-daily-line-debug", "monthly-sales-pie-debug"}


def _is_js_wiring_attr(name: str) -> bool:
    return name.startswith("on") or name.startswith("data-js-")


# Fase 3 (CSS sin estilos inline): cada style="…" de la baseline se traduce a las clases que lo sustituyen
# (tests/snapshots/style_classes.py) y las clases se comparan ordenadas: el check prueba que cada estilo se
# cambió exactamente por sus clases. Se ignoran <style>, <link rel="stylesheet"> (qué hojas carga cada página
# lo fija un unit test) y <meta name="htmx-config">, y la clase muerta mb-10 (sin regla CSS hasta la fase 3)
# del formulario de borrado de stock.
def _canon_classes(tag: str, attr_map: dict) -> str | None:
    classes = (attr_map.get("class") or "").split()
    mapped = STYLE_TO_CLASSES.get(style_key(attr_map["style"])) if attr_map.get("style") else None
    if mapped:
        classes += mapped.split()
    if tag == "form" and attr_map.get("hx-post") == "/ui/stock/delete-selected" and "mb-10" in classes:
        classes.remove("mb-10")
    return " ".join(sorted(set(classes))) if classes else None


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
        if tag == "style":
            self._skip_tag = "style"
            return
        if (tag == "link" and attr_map.get("rel") == "stylesheet") or (
                tag == "meta" and attr_map.get("name") == "htmx-config"):
            return
        if tag == "div" and attr_map.get("id") in _REMOVED_DEBUG_IDS:
            self._skip_tag = "div"
            return
        parts = [tag]
        style_mapped = bool(attr_map.get("style")) and style_key(attr_map["style"]) in STYLE_TO_CLASSES
        classes = _canon_classes(tag, attr_map)
        attrs = [(n, v) for n, v in attrs if n != "class" and not (n == "style" and style_mapped)]
        if classes:
            attrs.append(("class", classes))
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


def canonical(html: str) -> str:
    parser = _Canon()
    parser.feed(html)
    parser.close()
    return "\n".join(parser.tokens) + "\n"


def _hostile(label: str) -> str:
    # H-302: la siembra original no tenía ningún `< > & " '`, así que un `|safe`/`Markup(...)`
    # colado en una macro daba el mismo canónico que el escape correcto — el arnés nunca
    # probaba nada ahí. Un `<b>`/`"`/`&` sin escapar cambia los tokens (se convierte en
    # etiqueta o rompe el atributo), así que el `check` sí lo detecta.
    return f'{label} <b>"H"</b> & \'co\''


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
            business_id=bid, sku="SNAP-1", name=_hostile("Producto Snapshot"), category="Cat",
            min_stock=5, unit_of_measure="ud", default_purchase_cost=1.0,
            default_sale_price=2.0, lead_time_days=0,
        ))
        db.flush()
        svc = InventoryService(db, business_id=bid)
        purchase = svc.purchase(PurchaseCreate(
            sku="SNAP-1", quantity=10, unit_cost=1.0, movement_date=_FIXED,
            lot_code="LOT-SNAP-1", location_code=central.code, note=_hostile("Nota compra"),
        ))
        transfer = svc.transfer(TransferCreate(
            from_location_code=central.code, to_location_code=pos.code,
            lines=[TransferLineCreate(sku="SNAP-1", quantity=3)], movement_date=_FIXED,
            note=_hostile("Nota traspaso"),
        ))
        sale = svc.sale(SaleCreate(
            sku="SNAP-1", quantity=1, unit_price=2.0, movement_date=_FIXED, location_code=pos.code,
            note=_hostile("Nota venta"),
        ))
        adjustment = svc.adjustment(AdjustmentCreate(
            sku="SNAP-1", quantity_delta=2, unit_cost=1.0, movement_date=_FIXED,
            location_code=central.code, note=_hostile("Nota ajuste"),
        ))
        customer = Customer(business_id=bid, client_id="C-SNAP", name=_hostile("Cliente Snapshot"),
                            address=_hostile("Calle 1"), phone="600000000", email="c@snap.test")
        db.add(customer)
        expense = OperatingExpense(business_id=bid, expense_date=_FIXED, amount=12.5,
                                   concept=_hostile("Luz"))
        extraction = MoneyExtraction(business_id=bid, extraction_date=_FIXED, party="Negocio",
                                     amount=20.0, concept=_hostile("Retiro"))
        db.add_all([expense, extraction])
        db.flush()
        doc = SalesDocument(
            business_id=bid, location_id=central.id, customer_id=customer.id, doc_type="F",
            year_month="202601", seq=1, code="F202601-0001", issue_date=_FIXED,
            issuer_name="Emisor", client_name=customer.name, client_id=customer.client_id,
            client_address=customer.address, notes=_hostile("Nota doc"), subtotal=4.0, total=4.0,
        )
        db.add(doc)
        db.flush()
        db.add(SalesDocumentItem(document_id=doc.id, line_no=1, sku="SNAP-1",
                                 description=_hostile("Producto Snapshot"), unit_of_measure="ud",
                                 quantity=2, unit_price=2.0, line_total=4.0))
        user = db.scalar(select(User).where(User.username == "operator"))
        # Usuarios para la captura por rol (REQ-1). Contraseña = nombre de usuario.
        db.add(User(username="snap-operator", password_hash=hash_password("snap-operator"),
                    role="operator", is_active=True, business_id=bid))
        db.add(User(username="snap-owner", password_hash=hash_password("snap-owner"),
                    role="owner", is_active=True, business_id=bid))
        db.flush()
        _snap_owner = db.scalar(select(User).where(User.username == "snap-owner"))
        db.add(UserBusiness(user_id=int(_snap_owner.id), business_id=bid))
        db.commit()
        add_widget(db, bid, type="pie", title=_hostile("Widget"), tool="snapshot", tool_args={},
                   payload={"labels": [_hostile("A"), "B"], "values": [1, 2]}, unit=None, created_by=None)
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
            "customer_name": customer.name,
            "pos_code": pos.code,
            "central_code": central.code,
        }
    finally:
        db.close()


_ROLE_TABS = {
    "operator": ["sales"],
    "owner": ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
              "customers", "expenses", "profit-items", "profit", "dividends", "history"],
}

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
        # Por `ref=`, como enlaza la UI (tab_transfers.html); `movement_id=` no lo usa nadie (H-311).
        "print_transfer": f"/ui/transfers/print?ref={ids['transfer_ref']}",
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


def _request(client: TestClient, key: str, method: str, url: str, data=None, hx: bool = True) -> str:
    resp = client.request(method, url, data=data, headers={"HX-Request": "true"} if hx else {})
    return f"status={resp.status_code}\n" + canonical(resp.text)


def _login(client: TestClient, username: str, password: str) -> None:
    resp = client.post("/ui/login", data={"username": username, "password": password}, follow_redirects=False)
    if resp.status_code != 302:
        raise SystemExit(f"login {username} falló: {resp.status_code}")


def _create_snap_empty(client: TestClient) -> None:
    client.post("/ui/businesses/create", data={"code": "snap-empty", "name": "Snap Empty"},
                headers={"HX-Request": "true"}, follow_redirects=False)


def _empty_business_id() -> int:
    db = get_session()
    try:
        return int(db.scalar(select(Business).where(Business.code == "snap-empty")).id)
    finally:
        db.close()


def _add_empty_product(bid: int) -> None:
    db = get_session()
    try:
        db.add(Product(business_id=bid, sku="SNAP-EMPTY", name="Producto Empty", category="Cat",
                       min_stock=0, unit_of_measure="ud", default_purchase_cost=1.0,
                       default_sale_price=2.0, lead_time_days=0))
        db.commit()
    finally:
        db.close()


def _capture_role(role: str) -> dict[str, str]:
    out: dict[str, str] = {}
    client = TestClient(app)  # nuevo por rol; sin `with`: no relanza el lifespan
    _login(client, f"snap-{role}", f"snap-{role}")
    out[f"{role}_dashboard"] = _get(client, f"{role}_dashboard", "/ui/dashboard", hx=False)
    for slug in _ROLE_TABS[role]:
        out[f"{role}_tab_{slug}"] = _get(client, f"{role}_tab_{slug}", f"/ui/tabs/{slug}", hx=True)
    return out


def capture() -> dict[str, str]:
    logging.disable(logging.CRITICAL)
    out: dict[str, str] = {}
    with TestClient(app) as client:
        out["login"] = _get(client, "login", "/ui/login", hx=False)
        _login(client, "admin", "admin")
        ids = _seed()
        # 1. Lo actual: 40 claves admin, sin cambios de clave.
        for key, url in _pages(ids).items():
            out[key] = _get(client, key, url, hx=not key.startswith(("dashboard", "print_")))
        # 2. Fragmentos OOB (REQ-3) como admin.
        out["frag_defaults_purchase"] = _request(client, "frag_defaults_purchase", "GET",
                                                 "/ui/product-defaults/purchase?product=SNAP-1")
        out["frag_defaults_sale"] = _request(client, "frag_defaults_sale", "GET",
                                             "/ui/product-defaults/sale?product=SNAP-1")
        out["frag_salesdoc_product"] = _request(client, "frag_salesdoc_product", "GET",
                                                "/ui/sales-doc/product-defaults?product=SNAP-1")
        out["frag_salesdoc_customer"] = _request(
            client, "frag_salesdoc_customer", "GET",
            f"/ui/sales-doc/customer-defaults?client_name={quote(ids['customer_name'])}")
        out["frag_sale_options"] = _request(client, "frag_sale_options", "GET",
                                            f"/ui/sale/product-options?location_code={ids['pos_code']}")
        out["frag_transfer_options"] = _request(client, "frag_transfer_options", "GET",
                                                f"/ui/transfers/product-options?from_location_code={ids['central_code']}")
        out["frag_lots_empty"] = _request(client, "frag_lots_empty", "GET", "/ui/return-supplier/lots")
        out["frag_lots_none"] = _request(client, "frag_lots_none", "GET",
                                         "/ui/return-supplier/lots?sku=SNAP-NOLOT")
        out["frag_lots_ok"] = _request(client, "frag_lots_ok", "GET",
                                       "/ui/return-supplier/lots?sku=SNAP-1")
        # 3. POST con error (REQ-2) y POST válidos (REQ-3).
        out["post_purchase_qty0"] = _request(client, "post_purchase_qty0", "POST", "/ui/purchase",
                                             data={"product": "SNAP-1", "quantity": "0", "unit_cost": "1.0"})
        out["post_user_duplicate"] = _request(client, "post_user_duplicate", "POST", "/ui/users/create",
                                              data={"username": "admin", "password": "x", "role": "admin",
                                                    "is_active": "true"})
        out["post_transfer_over_stock"] = _request(client, "post_transfer_over_stock", "POST", "/ui/transfers",
                                                   data={"from_location_code": ids["central_code"],
                                                         "to_location_code": ids["pos_code"],
                                                         "product": "SNAP-1", "quantity": "9999"})
        out["post_purchase_new_product"] = _request(client, "post_purchase_new_product", "POST", "/ui/purchase",
                                                    data={"product": "SNAP-NEW", "product_name": "Producto Nuevo",
                                                          "quantity": "1", "unit_cost": "5.0",
                                                          "movement_date": "2026-10-24T12:00"})
        out["post_sale_ok"] = _request(client, "post_sale_ok", "POST", "/ui/sale",
                                       data={"product": "SNAP-1", "quantity": "1", "unit_price": "2.0",
                                             "location_code": ids["pos_code"]})
        # 4. Negocio vacío snap-empty + cambio de negocio + estados vacíos + producto para REQ-2.
        _create_snap_empty(client)
        empty_bid = _empty_business_id()
        client.post("/ui/active-business", data={"business_id": str(empty_bid)}, follow_redirects=False)
        for slug in TABS:
            out[f"empty_tab_{slug}"] = _get(client, f"empty_tab_{slug}", f"/ui/tabs/{slug}", hx=True)
        _add_empty_product(empty_bid)
        client.post("/ui/active-business", data={"business_id": str(ids['bid'])}, follow_redirects=False)
        # 5. SKU del otro negocio (REQ-2).
        out["post_product_sku_other_business"] = _request(
            client, "post_product_sku_other_business", "POST", "/ui/product/SNAP-1/update",
            data={"new_sku": "SNAP-EMPTY", "name": "SNAP-1", "unit_of_measure": "ud"})
        # 6. Captura por rol (REQ-1): un TestClient nuevo por rol, sin `with`.
        out.update(_capture_role("operator"))
        out.update(_capture_role("owner"))
        # 7. Último, como hoy: cambio de contraseña obligatorio.
        _force_password_change()
        out["modal_must_change_password"] = _get(
            client, "modal_must_change_password", "/ui/must-change-password-form", hx=True)
        out["post_must_change_password_mismatch"] = _request(
            client, "post_must_change_password_mismatch", "POST", "/ui/must-change-password",
            data={"new_password": "x", "confirm_password": "y"})
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

