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
