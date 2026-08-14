from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class WidgetSpec(BaseModel):
    # 'list' is deliberately excluded: no tool in TOOL_REGISTRY produces a
    # payload with an 'items' key, so it would always render empty (G6).
    # Tipos de Chart.js soportados: pie/doughnut (proporción), radar (multivariable).
    type: Literal["kpi", "line", "bar", "pie", "doughnut", "radar", "table"]
    title: str = Field(min_length=1, max_length=255)
    tool: str = Field(min_length=1)
    tool_args: dict = Field(default_factory=dict)
    unit: Optional[str] = None


class PlanModel(BaseModel):
    tool: Optional[str] = None
    tool_args: dict = Field(default_factory=dict)
    widgets: list[WidgetSpec] = Field(default_factory=list)
    reply: str = Field(min_length=1)
