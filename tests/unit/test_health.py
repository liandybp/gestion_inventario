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


def test_agent_llm_check_sends_api_key_when_configured(monkeypatch):
    """H-R02: sin la key, una API externa responde 401 y el check marcaba "degraded" en falso."""
    import urllib.request

    from app.routers.health import _agent_llm_ok

    seen = []

    class _Resp:
        def read(self):
            return b"{}"

    def _fake_urlopen(req, timeout):
        seen.append(req)
        return _Resp()

    monkeypatch.setattr("app.settings.AGENT_LLM_BASE_URL", "https://api.example.test/v1")
    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)

    monkeypatch.setattr("app.settings.AGENT_LLM_API_KEY", "sk-test")
    assert _agent_llm_ok() == "ok"
    assert seen[-1].full_url == "https://api.example.test/v1/models"
    assert seen[-1].get_header("Authorization") == "Bearer sk-test"

    monkeypatch.setattr("app.settings.AGENT_LLM_API_KEY", "")
    assert _agent_llm_ok() == "ok"
    assert seen[-1].get_header("Authorization") is None


def test_agent_llm_check_degraded_on_error(monkeypatch):
    import urllib.request

    from app.routers.health import _agent_llm_ok

    def _boom(req, timeout):
        raise OSError("401")

    monkeypatch.setattr("app.settings.AGENT_LLM_BASE_URL", "https://api.example.test/v1")
    monkeypatch.setattr(urllib.request, "urlopen", _boom)
    assert _agent_llm_ok() == "degraded"
