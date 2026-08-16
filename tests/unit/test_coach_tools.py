import pytest

from app.agent.tools import TOOL_REGISTRY, coach_health_score, coach_recommendations
from app.models import Business
from app.services.inventory_service import InventoryService


@pytest.fixture
def seeded_business(db_session) -> Business:
    biz = db_session.query(Business).filter(Business.code == "test").first()
    assert biz is not None
    return biz


def test_coach_health_score_scoped(db_session, seeded_business):
    service = InventoryService(db_session, seeded_business.id)
    result = coach_health_score(service, {})
    assert "value" in result
    assert "label" in result


def test_coach_recommendations_shape(db_session, seeded_business):
    service = InventoryService(db_session, seeded_business.id)
    result = coach_recommendations(service, {})
    assert result["columns"] == ["Síntoma", "Acción", "Impacto"]
    assert isinstance(result["rows"], list)


def test_tools_registered_in_registry():
    assert "coach_health_score" in TOOL_REGISTRY
    assert "coach_recommendations" in TOOL_REGISTRY