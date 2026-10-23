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

