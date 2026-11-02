"""Reportes de violaciones de CSP: ``POST /csp-report`` (app/routers/csp_report.py)."""

from __future__ import annotations

import json
import logging

import pytest
from fastapi.testclient import TestClient

import app.routers.csp_report as csp_report
from app.main import app

LEGACY = {"csp-report": {
    "document-uri": "http://localhost/ui/", "violated-directive": "style-src-attr",
    "effective-directive": "style-src-attr", "blocked-uri": "inline", "disposition": "enforce",
    "source-file": "http://localhost/static/js/x.js", "line-number": 7, "script-sample": "secreto",
}}
REPORTING_API = [{"type": "csp-violation", "url": "http://localhost/ui/", "body": {
    "documentURL": "http://localhost/ui/", "effectiveDirective": "script-src-elem",
    "blockedURL": "https://evil.example/x.js", "disposition": "enforce",
}}, {"type": "deprecation", "body": {"id": "x"}}]


@pytest.fixture(autouse=True)
def _reset_window(monkeypatch):
    monkeypatch.setattr(csp_report, "_window_start", 0.0)
    monkeypatch.setattr(csp_report, "_window_count", 0)


def _violations(caplog) -> list[dict]:
    return [json.loads(r.getMessage().split(" ", 1)[1]) for r in caplog.records
            if r.getMessage().startswith("csp_violation ")]


def test_csp_header_reports_to_endpoint() -> None:
    csp = TestClient(app).get("/health").headers["Content-Security-Policy"]
    assert "report-uri /csp-report" in csp


def test_legacy_report_is_logged_without_session_or_htmx_header(caplog) -> None:
    caplog.set_level(logging.WARNING)
    resp = TestClient(app).post("/csp-report", content=json.dumps(LEGACY),
                                headers={"Content-Type": "application/csp-report"})
    assert resp.status_code == 204
    assert _violations(caplog) == [{
        "document": "http://localhost/ui/", "directive": "style-src-attr", "violated": "style-src-attr",
        "blocked": "inline", "source": "http://localhost/static/js/x.js", "line": "7", "disposition": "enforce",
    }]
    assert "secreto" not in caplog.text  # el script-sample no se registra


def test_reporting_api_only_logs_csp_violations(caplog) -> None:
    caplog.set_level(logging.WARNING)
    resp = TestClient(app).post("/csp-report", content=json.dumps(REPORTING_API),
                                headers={"Content-Type": "application/reports+json"})
    assert resp.status_code == 204
    assert _violations(caplog) == [{"document": "http://localhost/ui/", "directive": "script-src-elem",
                                    "blocked": "https://evil.example/x.js", "disposition": "enforce"}]


def test_long_fields_are_truncated(caplog) -> None:
    caplog.set_level(logging.WARNING)
    body = {"csp-report": {"blocked-uri": "x" * 5000}}
    TestClient(app).post("/csp-report", content=json.dumps(body))
    assert len(_violations(caplog)[0]["blocked"]) == 300


@pytest.mark.parametrize("raw", [b"", b"no-json", b"\xff\xfe", b"[1, 2]", b'{"otra": 1}'])
def test_invalid_payloads_are_ignored(caplog, raw) -> None:
    caplog.set_level(logging.WARNING)
    assert TestClient(app).post("/csp-report", content=raw).status_code == 204
    assert _violations(caplog) == []


def test_oversized_body_is_rejected(caplog) -> None:
    caplog.set_level(logging.WARNING)
    big = json.dumps({"csp-report": {"blocked-uri": "x" * csp_report.MAX_BODY_BYTES}})
    assert TestClient(app).post("/csp-report", content=big).status_code == 413
    assert _violations(caplog) == []


def test_reports_per_minute_are_capped(caplog, monkeypatch) -> None:
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(csp_report, "MAX_REPORTS_PER_MINUTE", 3)
    client = TestClient(app)
    for _ in range(5):
        assert client.post("/csp-report", content=json.dumps(LEGACY)).status_code == 204
    assert len(_violations(caplog)) == 3


def test_oversized_streamed_body_without_content_length_is_rejected(caplog) -> None:
    """H-R03: el tope se aplica leyendo por trozos, también si falta Content-Length."""
    from app.routers.csp_report import MAX_BODY_BYTES

    def _chunks():
        for _ in range(MAX_BODY_BYTES // 1024 + 2):
            yield b"x" * 1024

    resp = TestClient(app).post("/csp-report", content=_chunks())
    assert resp.status_code == 413
    assert "csp_violation" not in caplog.text
