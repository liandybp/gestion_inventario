from __future__ import annotations

from app.models import Business, Location
from app.repositories.business_repository import get_business_config, get_business_locations


def test_get_business_config_defaults_when_none(db_session):
    cfg = get_business_config(db_session, None)
    assert cfg.currency.code == "EUR"
    assert cfg.issuer.name == "Mi Negocio"


def test_get_business_config_reconstructs(db_session, business_id):
    biz = db_session.get(Business, business_id)
    biz.issuer_name = "Mi Emisor"
    biz.currency_code = "USD"
    biz.currency_symbol = "$"
    biz.dividends_partners = "Liandy, Randy"
    biz.dividends_opening_pending = '{"Negocio": 605.5}'
    biz.sales_enabled_types = "F,P"
    biz.purchase_invoice_vat_rate = 1.10
    db_session.commit()

    cfg = get_business_config(db_session, business_id)
    assert cfg.issuer.name == "Mi Emisor"
    assert cfg.currency.code == "USD"
    assert cfg.currency.symbol == "$"
    assert cfg.dividends.partners == ["Liandy", "Randy"]
    assert cfg.dividends.opening_pending == {"Negocio": 605.5}
    assert cfg.sales_documents.enabled_types == ["F", "P"]
    assert cfg.purchase.invoice_vat_rate == 1.10


def test_get_business_locations_central_pos_default(db_session, business_id):
    db_session.add(Location(business_id=business_id, code="TEST_CENTRAL", name="Central",
                            role="central", position=0))
    db_session.add(Location(business_id=business_id, code="TEST_POS1", name="Tienda 1",
                            role="pos", position=1, is_default=True))
    db_session.add(Location(business_id=business_id, code="TEST_POS2", name="Tienda 2",
                            role="pos", position=2))
    db_session.commit()

    loc = get_business_locations(db_session, business_id)
    assert loc.central.code == "TEST_CENTRAL"
    assert [p.code for p in loc.pos] == ["TEST_POS1", "TEST_POS2"]
    assert loc.default_pos == "TEST_POS1"


def test_get_business_locations_legacy_infers_central(db_session, business_id):
    # rows legacy sin role: la central se infiere por convención de código
    db_session.add(Location(business_id=business_id, code="TEST_CENTRAL", name="Central"))
    db_session.add(Location(business_id=business_id, code="TEST_POS1", name="Tienda 1"))
    db_session.commit()

    loc = get_business_locations(db_session, business_id)
    assert loc.central.code == "TEST_CENTRAL"
    assert [p.code for p in loc.pos] == ["TEST_POS1"]


def test_get_business_locations_no_pos(db_session, business_id):
    """Negocio sin POS (solo central) → pos vacío, default_pos None y ventas usan central."""
    db_session.add(Location(business_id=business_id, code="TEST_CENTRAL", name="Central",
                            role="central", position=0))
    db_session.commit()

    loc = get_business_locations(db_session, business_id)
    assert loc.central.code == "TEST_CENTRAL"
    assert loc.pos == []
    assert loc.default_pos is None
    assert loc.default_sale_code() == "TEST_CENTRAL"
