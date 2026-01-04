from __future__ import annotations

import configparser
import json
import os
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field


class IssuerConfig(BaseModel):
    name: str = "Mi Negocio"
    tax_id: str = ""
    address: str = ""
    city: str = ""
    postal_code: str = ""
    country: str = ""
    email: str = ""
    phone: str = ""


class CurrencyConfig(BaseModel):
    code: str = "EUR"
    symbol: str = "€"


class SalesDocumentsConfig(BaseModel):
    default_type: Literal["F", "P"] = "F"
    enabled_types: list[Literal["F", "P"]] = Field(default_factory=lambda: ["F", "P"])
    invoice_label: str = "Factura"
    quote_label: str = "Presupuesto"


class DividendsConfig(BaseModel):
    business_label: str = "Negocio"
    partners: list[str] = Field(default_factory=lambda: ["Liandy", "Randy"])
    opening_pending: dict[str, float] = Field(default_factory=dict)


class BusinessConfig(BaseModel):
    issuer: IssuerConfig = Field(default_factory=IssuerConfig)
    currency: CurrencyConfig = Field(default_factory=CurrencyConfig)
    sales_documents: SalesDocumentsConfig = Field(default_factory=SalesDocumentsConfig)
    dividends: DividendsConfig = Field(default_factory=DividendsConfig)


_cached_config: Optional[BusinessConfig] = None


def load_business_config() -> BusinessConfig:
    global _cached_config
    if _cached_config is not None:
        return _cached_config

    config_path = os.getenv("BUSINESS_CONFIG_PATH", "app/business_config.conf")
    path = Path(config_path)
    if not path.exists():
        _cached_config = BusinessConfig()
        return _cached_config

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        _cached_config = BusinessConfig.model_validate(data)
        return _cached_config

    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")

    def get(section: str, key: str, default: str = "") -> str:
        try:
            return (parser.get(section, key, fallback=default) or "").strip()
        except Exception:
            return (default or "").strip()

    def get_list(section: str, key: str) -> list[str]:
        raw = get(section, key, "")
        if not raw:
            return []
        parts = [p.strip() for p in raw.split(",")]
        return [p for p in parts if p]

    def get_opening_pending() -> dict[str, float]:
        raw = get("dividends", "opening_pending", "")
        if not raw:
            return {}
        raw_str = raw.strip()
        if not raw_str:
            return {}
        if raw_str.startswith("{"):
            try:
                data = json.loads(raw_str)
                if isinstance(data, dict):
                    out: dict[str, float] = {}
                    for k, v in data.items():
                        key = (str(k) or "").strip()
                        if not key:
                            continue
                        try:
                            out[key] = float(v)
                        except Exception:
                            continue
                    return out
            except Exception:
                return {}

        # Format: party:amount,party:amount
        out2: dict[str, float] = {}
        for part in [p.strip() for p in raw_str.split(",") if p.strip()]:
            if ":" not in part:
                continue
            k, v = part.split(":", 1)
            key = (k or "").strip()
            if not key:
                continue
            try:
                out2[key] = float((v or "").strip())
            except Exception:
                continue
        return out2

    cfg = BusinessConfig(
        issuer=IssuerConfig(
            name=get("issuer", "name", "Mi Negocio"),
            tax_id=get("issuer", "tax_id", ""),
            address=get("issuer", "address", ""),
            city=get("issuer", "city", ""),
            postal_code=get("issuer", "postal_code", ""),
            country=get("issuer", "country", ""),
            email=get("issuer", "email", ""),
            phone=get("issuer", "phone", ""),
        ),
        currency=CurrencyConfig(
            code=get("currency", "code", "EUR"),
            symbol=get("currency", "symbol", "€"),
        ),
        sales_documents=SalesDocumentsConfig(
            default_type=(get("sales_documents", "default_type", "F").upper() or "F"),
            enabled_types=[t for t in (get_list("sales_documents", "enabled_types") or ["F", "P"]) if t in ("F", "P")],
            invoice_label=get("sales_documents", "invoice_label", "Factura"),
            quote_label=get("sales_documents", "quote_label", "Presupuesto"),
        ),
        dividends=DividendsConfig(
            business_label=get("dividends", "business_label", "Negocio"),
            partners=get_list("dividends", "partners"),
            opening_pending=get_opening_pending(),
        ),
    )

    if not cfg.sales_documents.enabled_types:
        cfg.sales_documents.enabled_types = ["F", "P"]
    if cfg.sales_documents.default_type not in ("F", "P"):
        cfg.sales_documents.default_type = "F"

    _cached_config = cfg
    return _cached_config
