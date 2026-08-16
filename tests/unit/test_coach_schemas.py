from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.schemas import CashBaselineCreate, CoachActionUpdate


def test_cashbaseline_rejects_negative():
    with pytest.raises(ValidationError):
        CashBaselineCreate(baseline_date=datetime.now(timezone.utc), amount=-1)


def test_cashbaseline_rejects_future_date():
    from datetime import timedelta

    with pytest.raises(ValidationError):
        CashBaselineCreate(
            baseline_date=datetime.now(timezone.utc) + timedelta(days=1), amount=100
        )


def test_coachaction_accepts_done():
    obj = CoachActionUpdate(rule_key="margin_drop", status="done")
    assert obj.status == "done"


def test_coachaction_rejects_invalid_status():
    with pytest.raises(ValidationError):
        CoachActionUpdate(rule_key="margin_drop", status="invalid")