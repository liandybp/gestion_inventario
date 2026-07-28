from __future__ import annotations

import json

from app import business_config
from app.business_config import load_business_config


def _write(path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_load_business_config_defaults_when_file_missing(monkeypatch, tmp_path) -> None:
    missing = tmp_path / "does_not_exist.conf"
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(missing))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.currency.code == "EUR"
    assert cfg.purchase.invoice_vat_rate == 1.21
    assert cfg.purchase.invoice_fx_rate is None


def test_purchase_fx_rate_empty_is_none(monkeypatch, tmp_path) -> None:
    cfg_file = tmp_path / "business_config.conf"
    _write(
        cfg_file,
        """
[currency]
code = USD
symbol = $

[purchase]
invoice_source_currency = EUR
invoice_vat_rate = 1.21
invoice_fx_rate =
""".strip(),
    )
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(cfg_file))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.currency.code == "USD"
    assert cfg.purchase.invoice_source_currency == "EUR"
    assert cfg.purchase.invoice_vat_rate == 1.21
    assert cfg.purchase.invoice_fx_rate is None


def test_purchase_fx_rate_parses_comma_decimal(monkeypatch, tmp_path) -> None:
    cfg_file = tmp_path / "business_config.conf"
    _write(
        cfg_file,
        """
[purchase]
invoice_vat_rate = 1,21
invoice_fx_rate = 1,15
""".strip(),
    )
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(cfg_file))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.purchase.invoice_vat_rate == 1.21
    assert cfg.purchase.invoice_fx_rate == 1.15


def test_business_specific_config_from_dir(monkeypatch, tmp_path) -> None:
    cfg_dir = tmp_path / "configs"
    cfg_dir.mkdir()
    _write(
        cfg_dir / "business_config.ropa.conf",
        """
[currency]
code = EUR
symbol = €

[purchase]
invoice_fx_rate = 1.0
""".strip(),
    )
    monkeypatch.setenv("BUSINESS_CONFIG_DIR", str(cfg_dir))
    monkeypatch.delenv("BUSINESS_CONFIG_PATH_ROPA", raising=False)
    business_config._cached_configs.clear()

    cfg = load_business_config("ropa")

    assert cfg.currency.code == "EUR"
    assert cfg.purchase.invoice_fx_rate == 1.0


def test_json_config_load(monkeypatch, tmp_path) -> None:
    cfg_file = tmp_path / "business_config.json"
    cfg_file.write_text(
        json.dumps(
            {
                "currency": {"code": "USD", "symbol": "$"},
                "purchase": {
                    "invoice_source_currency": "EUR",
                    "invoice_vat_rate": 1.21,
                    "invoice_fx_rate": 1.15,
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(cfg_file))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.currency.code == "USD"
    assert cfg.purchase.invoice_fx_rate == 1.15


def test_opening_pending_supports_colon_format(monkeypatch, tmp_path) -> None:
    cfg_file = tmp_path / "business_config.conf"
    _write(
        cfg_file,
        """
[dividends]
opening_pending = Negocio:10,Mariela:2.5
""".strip(),
    )
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(cfg_file))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.dividends.opening_pending["Negocio"] == 10.0
    assert cfg.dividends.opening_pending["Mariela"] == 2.5


def test_locations_and_sales_document_defaults(monkeypatch, tmp_path) -> None:
    cfg_file = tmp_path / "business_config.conf"
    _write(
        cfg_file,
        """
[locations]
central = CEN:Central
pos = POSX:Tienda X
default_pos = POS_MISSING

[sales_documents]
default_type = F
enabled_types = X
""".strip(),
    )
    monkeypatch.setenv("BUSINESS_CONFIG_PATH", str(cfg_file))
    business_config._cached_configs.clear()

    cfg = load_business_config()

    assert cfg.locations.central.code == "CEN"
    assert cfg.locations.default_pos == "POSX"
    assert cfg.sales_documents.default_type == "F"
    assert cfg.sales_documents.enabled_types == ["F", "P"]



