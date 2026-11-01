from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

_ALL_ROLES = frozenset({"admin", "owner", "operator"})


@dataclass(frozen=True)
class NavItem:
    slug: str
    label: str
    roles: Optional[frozenset[str]]  # None = visible para cualquier rol (solo "sales")
    server_only: bool = False


NAV_ITEMS: tuple[NavItem, ...] = (
    NavItem("home", "Inicio", frozenset({"admin", "owner"})),
    NavItem("agent", "Dashboard Dinámico", frozenset({"admin", "owner"}), server_only=True),
    NavItem("inventory", "Inventario", frozenset({"admin", "owner"})),
    NavItem("purchases", "Compras", frozenset({"admin", "owner"}), server_only=True),
    NavItem("transfers", "Traspasos", frozenset({"admin", "owner"}), server_only=True),
    NavItem("coach", "Coach", frozenset({"admin"})),
    NavItem("sales", "Ventas", None),
    NavItem("documents", "Documentos", frozenset({"admin", "owner"})),
    NavItem("customers", "Clientes", frozenset({"admin", "owner"})),
    NavItem("expenses", "Costos operativos", frozenset({"admin", "owner"})),
    NavItem("profit-items", "Utilidades por artículo", frozenset({"admin", "owner"})),
    NavItem("profit", "Utilidades", frozenset({"admin", "owner"})),
    NavItem("dividends", "Dividendos", frozenset({"admin", "owner"})),
    NavItem("history", "Historial", frozenset({"admin", "owner"})),
    NavItem("users", "Usuarios", frozenset({"admin"})),
    NavItem("businesses", "Negocios", frozenset({"admin"})),
    NavItem("activity", "Actividad", frozenset({"admin"})),
)


def visible_nav_items(role: str, app_mode: str) -> list[NavItem]:
    out: list[NavItem] = []
    for item in NAV_ITEMS:
        if item.server_only and app_mode == "desktop":
            continue
        if item.roles is None or role in item.roles:
            out.append(item)
    return out


def default_tab(role: str) -> str:
    return "sales" if role == "operator" else "home"
