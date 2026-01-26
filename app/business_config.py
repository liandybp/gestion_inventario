from __future__ import annotations

import configparser
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from typing_extensions import Literal

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
    enabled_types: List[Literal["F", "P"]] = Field(default_factory=lambda: ["F", "P"])
    invoice_label: str = "Factura"
    quote_label: str = "Presupuesto"


class DividendsConfig(BaseModel):
    business_label: str = "Negocio"
    partners: List[str] = Field(default_factory=lambda: ["Liandy", "Randy"])
    opening_pending: Dict[str, float] = Field(default_factory=dict)


class LocationSpec(BaseModel):
    code: str
    name: str


class LocationsConfig(BaseModel):
    central: LocationSpec = Field(
        default_factory=lambda: LocationSpec(code="CENTRAL", name="Almacén Central")
    )
    pos: List[LocationSpec] = Field(
        default_factory=lambda: [LocationSpec(code="POS1", name="Punto de venta 1")]
    )
    default_pos: str = "POS1"


class BusinessConfig(BaseModel):
    issuer: IssuerConfig = Field(default_factory=IssuerConfig)
    currency: CurrencyConfig = Field(default_factory=CurrencyConfig)
    sales_documents: SalesDocumentsConfig = Field(default_factory=SalesDocumentsConfig)
    dividends: DividendsConfig = Field(default_factory=DividendsConfig)
    locations: LocationsConfig = Field(default_factory=LocationsConfig)


_cached_configs: Dict[str, Tuple[BusinessConfig, str, float]] = {}


def load_business_config(business_code: Optional[str] = None) -> BusinessConfig:
    global _cached_configs

    key = (business_code or "").strip().lower()

    base_path = os.getenv("BUSINESS_CONFIG_PATH", "app/business_config.conf")
    base = Path(base_path)

    def pick_path() -> Path:
        if not key:
            return base

        env_key = f"BUSINESS_CONFIG_PATH_{key.upper()}"
        override = (os.getenv(env_key) or "").strip()
        if override:
            return Path(override)

        dir_override = (os.getenv("BUSINESS_CONFIG_DIR") or "").strip()
        if dir_override:
            d = Path(dir_override)
            cand = d / f"business_config.{key}.conf"
            if cand.exists():
                return cand
            cand2 = d / f"business_config.{key}.json"
            if cand2.exists():
                return cand2

        cand3 = Path("app") / f"business_config.{key}.conf"
        if cand3.exists():
            return cand3
        cand4 = Path("app") / f"business_config.{key}.json"
        if cand4.exists():
            return cand4

        return base

    path = pick_path()
    path_str = str(path)
    mtime = 0.0
    try:
        mtime = float(path.stat().st_mtime)
    except Exception:
        mtime = 0.0

    cached = _cached_configs.get(key)
    if cached is not None:
        cfg_cached, cached_path, cached_mtime = cached
        if cached_path == path_str and float(cached_mtime or 0.0) == float(mtime or 0.0):
            return cfg_cached
    if not path.exists():
        cfg0 = BusinessConfig()
        _cached_configs[key] = (cfg0, path_str, mtime)
        return cfg0

    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if hasattr(BusinessConfig, "model_validate"):
            cfg_json = BusinessConfig.model_validate(data)
        else:
            cfg_json = BusinessConfig.parse_obj(data)
        _cached_configs[key] = (cfg_json, path_str, mtime)
        return cfg_json

    parser = configparser.ConfigParser()
    parser.read(path, encoding="utf-8")

    def get(section: str, key: str, default: str = "") -> str:
        try:
            return (parser.get(section, key, fallback=default) or "").strip()
        except Exception:
            return (default or "").strip()

    def get_list(section: str, key: str) -> List[str]:
        raw = get(section, key, "")
        if not raw:
            return []
        parts = [p.strip() for p in raw.split(",")]
        return [p for p in parts if p]

    def get_opening_pending() -> Dict[str, float]:
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
                    out: Dict[str, float] = {}
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
                # Tolerate pseudo-JSON with comma decimals like: {"Negocio": 605,04, "A": 1,23}
                out3: Dict[str, float] = {}
                for m in re.finditer(r"\"([^\"]+)\"\s*:\s*([0-9]+(?:[\.,][0-9]+)?)", raw_str):
                    k = (m.group(1) or "").strip()
                    v_raw = (m.group(2) or "").strip()
                    if not k or not v_raw:
                        continue
                    v_norm = v_raw.replace(",", ".")
                    try:
                        out3[k] = float(v_norm)
                    except Exception:
                        continue
                if out3:
                    return out3
                # Fall back to alternate format parsing below.

        # Format: party:amount,party:amount
        out2: Dict[str, float] = {}
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

    def parse_location_spec(raw: str, fallback_code: str, fallback_name: str) -> LocationSpec:
        raw2 = (raw or "").strip()
        if not raw2:
            return LocationSpec(code=fallback_code, name=fallback_name)
        if ":" in raw2:
            code, name = raw2.split(":", 1)
            code = (code or "").strip() or fallback_code
            name = (name or "").strip() or fallback_name
            return LocationSpec(code=code, name=name)
        return LocationSpec(code=raw2, name=fallback_name)

    def parse_location_list(raw_list: List[str]) -> List[LocationSpec]:
        out: List[LocationSpec] = []
        for part in raw_list:
            p = (part or "").strip()
            if not p:
                continue
            if ":" in p:
                code, name = p.split(":", 1)
                code = (code or "").strip()
                name = (name or "").strip() or code
                if code:
                    out.append(LocationSpec(code=code, name=name))
                continue
            out.append(LocationSpec(code=p, name=p))
        return out

    central_spec = parse_location_spec(
        get("locations", "central", "CENTRAL:Almacén Central"),
        fallback_code="CENTRAL",
        fallback_name="Almacén Central",
    )
    pos_list = parse_location_list(get_list("locations", "pos"))
    if not pos_list:
        pos_list = [LocationSpec(code="POS1", name="Punto de venta 1")]
    default_pos = (get("locations", "default_pos", "POS1") or "POS1").strip()
    pos_codes = {p.code for p in pos_list}
    if default_pos not in pos_codes:
        default_pos = pos_list[0].code

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
        locations=LocationsConfig(
            central=central_spec,
            pos=pos_list,
            default_pos=default_pos,
        ),
    )

    if not cfg.sales_documents.enabled_types:
        cfg.sales_documents.enabled_types = ["F", "P"]
    if cfg.sales_documents.default_type not in ("F", "P"):
        cfg.sales_documents.default_type = "F"

    if not cfg.locations.pos:
        cfg.locations.pos = [LocationSpec(code="POS1", name="Punto de venta 1")]
    if cfg.locations.default_pos not in {p.code for p in cfg.locations.pos}:
        cfg.locations.default_pos = cfg.locations.pos[0].code

    _cached_configs[key] = (cfg, path_str, mtime)
    return cfg
