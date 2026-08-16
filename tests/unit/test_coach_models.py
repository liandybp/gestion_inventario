from datetime import datetime, timezone

import pytest

from app.models import Business, CashBaseline, CoachActionLog


@pytest.fixture
def seeded_business(db_session) -> Business:
    biz = db_session.query(Business).filter(Business.code == "test").first()
    assert biz is not None
    return biz


def test_cashbaseline_creates(db_session, seeded_business):
    row = CashBaseline(
        business_id=seeded_business.id,
        baseline_date=datetime.now(timezone.utc),
        amount=1000.0,
    )
    db_session.add(row)
    db_session.commit()
    assert row.id is not None


def test_coachactionlog_creates(db_session, seeded_business):
    row = CoachActionLog(business_id=seeded_business.id, rule_key="margin_drop", status="open")
    db_session.add(row)
    db_session.commit()
    assert row.status == "open"