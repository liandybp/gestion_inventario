from __future__ import annotations

import asyncio

import pytest

import app.agent.graph as g
from app.agent import runner
from app.agent.schemas import PlanModel


class _FakePlanner:
    def __init__(self, plan: PlanModel):
        self._plan = plan

    def invoke(self, messages):
        return self._plan


def test_run_agent_uses_thread_local_session_and_returns_reply(monkeypatch):
    """H-003: run_agent invokes the graph on a worker thread with its own DB
    session (C3 fix) and returns the reply/widgets without a network call."""
    monkeypatch.setattr(
        g, "_PLANNER", _FakePlanner(PlanModel(tool=None, widgets=[], reply="Hola, soy el copiloto."))
    )
    result = asyncio.run(runner.run_agent(1, "hola"))
    assert result["reply"] == "Hola, soy el copiloto."
    assert result["widgets"] == []
    assert result["error"] is None


def test_run_agent_propagates_error_when_llm_unavailable(monkeypatch):
    """H-003 / B2: a hard LLM failure surfaces as error (no crash)."""
    class _BoomPlanner:
        def invoke(self, messages):
            raise ConnectionError("no hay LLM")

    monkeypatch.setattr(g, "_PLANNER", _BoomPlanner())
    result = asyncio.run(runner.run_agent(1, "hola"))
    # Reply amigable no vacío (PlanModel exige min_length=1) + error propagado.
    assert "no está disponible" in result["reply"]
    assert result["error"] is not None
    assert "LLM no disponible" in result["error"]


def test_run_agent_forwards_last_tool_to_planner_prompt(monkeypatch):
    """REQ-3: last_tool llega al system prompt que ve el LLM."""
    captured = {}

    class _CaptureSystemPlanner:
        def invoke(self, messages):
            captured["system"] = messages[0].content
            return PlanModel(tool=None, widgets=[], reply="ok")

    monkeypatch.setattr(g, "_PLANNER", _CaptureSystemPlanner())
    last_tool = {"tool": "sales_by_product", "tool_args": {"start": "2026-01-01"}, "type": "bar"}
    result = asyncio.run(runner.run_agent(1, "¿y por ubicación?", last_tool=last_tool))
    assert result["error"] is None
    assert "sales_by_product" in captured["system"]
