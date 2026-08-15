from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_health_has_expected_checks(monkeypatch):
    monkeypatch.setattr("app.settings.AGENT_LLM_BASE_URL", "")
    client = TestClient(app)
    res = client.get("/health")
    body = res.json()
    assert set(body["checks"].keys()) == {"database", "schema_version", "agent_llm"}
    assert "version" in body
    # H-065: 200 solo si ok; 503 si degrada (BD o schema_version fallan)
    assert (res.status_code == 200) == (body["status"] == "ok")
    assert (res.status_code == 503) == (body["status"] == "degraded")
