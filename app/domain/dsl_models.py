"""Pydantic models for strategy DSL payloads."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


ConditionKind = Literal[
    "crosses_above",
    "within_time",
    "once_per_day",
    "max_spread_cents",
    "min_volume",
]

ActionKind = Literal["enter_long", "move_stop_to_breakeven"]

LevelName = Literal[
    "opening_range_high",
    "opening_range_low",
    "session_low",
    "session_high",
    "last_price",
]


class SessionModel(BaseModel):
    """Trading session and opening-range configuration."""

    timezone: str = "UTC"
    market_open: str = "14:30"
    market_close: str = "21:00"
    opening_range_minutes: int = Field(default=5, ge=1, le=120)


class RiskModel(BaseModel):
    """Risk configuration used by enter actions."""

    risk_dollars: Decimal = Field(gt=0)


class ExitModel(BaseModel):
    """Exit behavior defaults used by the DSL engine."""

    breakeven_trigger_r: Decimal = Field(default=Decimal("1.0"), gt=0)


class ConditionModel(BaseModel):
    """Single condition node in the DSL."""

    kind: ConditionKind
    params: dict[str, Any] = Field(default_factory=dict)


class ActionModel(BaseModel):
    """Single action node in the DSL."""

    kind: ActionKind
    params: dict[str, Any] = Field(default_factory=dict)


class StrategyModel(BaseModel):
    """Top-level strategy model for YAML/JSON definitions."""

    version: str
    strategy_id: str
    symbol: str
    enabled: bool = True
    timeframe: str = "1m"
    session: SessionModel = Field(default_factory=SessionModel)
    constraints: list[ConditionModel] = Field(default_factory=list)
    entry: list[ConditionModel] = Field(default_factory=list)
    risk: RiskModel
    exit: ExitModel = Field(default_factory=ExitModel)
    actions: list[ActionModel] = Field(default_factory=list)

    @field_validator("strategy_id", "symbol")
    @classmethod
    def _not_blank(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("must not be empty")
        return normalized
