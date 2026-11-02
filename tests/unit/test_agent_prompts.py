from __future__ import annotations

from app.agent.prompts import build_system_prompt


def test_build_system_prompt_without_last_tool_has_no_context_block():
    prompt = build_system_prompt()
    assert "Contexto adicional" not in prompt


def test_build_system_prompt_with_last_tool_includes_it():
    last_tool = {"tool": "sales_by_product",
                 "tool_args": {"start": "2026-01-01", "end": "2026-01-31"}, "type": "bar"}
    prompt = build_system_prompt(last_tool=last_tool)
    assert "Contexto adicional" in prompt
    assert "sales_by_product" in prompt
    assert '"start": "2026-01-01"' in prompt


def test_prompt_has_monthly_location_rules():
    prompt = build_system_prompt()
    assert "sales_by_location_monthly con months=N" in prompt
    assert "N meses calendario INCLUYENDO el actual" in prompt


def test_prompt_has_honesty_rule():
    assert "No tengo X; te muestro Y" in build_system_prompt()


def test_prompt_separates_pending_dividends_from_extractions():
    assert "extractions_by_party da solo lo ya retirado, no lo pendiente" in build_system_prompt()


def test_prompt_gives_precomputed_month_ranges_at_month_start(monkeypatch):
    """A principios de mes el modelo resolvía "este mes" como el mes anterior (eval 2026-11-02)."""
    from datetime import datetime, timezone

    from app.agent import prompts

    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 2, 10, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(prompts, "datetime", _Frozen)
    prompt = prompts.build_system_prompt()
    assert "start=2026-11-01 y end=2026-11-02" in prompt
    assert "start=2026-10-01 y end=2026-10-31" in prompt


def test_prompt_month_ranges_cross_year(monkeypatch):
    from datetime import datetime, timezone

    from app.agent import prompts

    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2027, 1, 15, 10, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(prompts, "datetime", _Frozen)
    prompt = prompts.build_system_prompt()
    assert "start=2027-01-01 y end=2027-01-15" in prompt
    assert "start=2026-12-01 y end=2026-12-31" in prompt
