from __future__ import annotations

import pytest

from app.routers.ui_nav import default_tab, visible_nav_items

EXPECTED = {
    ("admin", "server"): ["home", "agent", "inventory", "purchases", "transfers", "coach", "sales",
                          "documents", "customers", "expenses", "profit-items", "profit", "dividends",
                          "history", "users", "businesses", "activity"],
    ("owner", "server"): ["home", "agent", "inventory", "purchases", "transfers", "sales", "documents",
                          "customers", "expenses", "profit-items", "profit", "dividends", "history"],
    ("operator", "server"): ["sales"],
    ("admin", "desktop"): ["home", "inventory", "coach", "sales", "documents", "customers", "expenses",
                           "profit-items", "profit", "dividends", "history", "users", "businesses", "activity"],
    ("owner", "desktop"): ["home", "inventory", "sales", "documents", "customers", "expenses",
                           "profit-items", "profit", "dividends", "history"],
    ("operator", "desktop"): ["sales"],
}


@pytest.mark.parametrize("role,mode", list(EXPECTED))
def test_visible_nav_items(role: str, mode: str) -> None:
    assert [i.slug for i in visible_nav_items(role, mode)] == EXPECTED[(role, mode)]


def test_default_tab() -> None:
    assert default_tab("operator") == "sales"
    assert default_tab("admin") == "home"
    assert default_tab("owner") == "home"
    assert default_tab("") == "home"
