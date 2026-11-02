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
