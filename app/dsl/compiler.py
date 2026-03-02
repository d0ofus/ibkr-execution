"""Compile validated strategy models into executable plans."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time, timezone, tzinfo
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.domain.dsl_models import StrategyModel
from app.domain.errors import ValidationError
from app.dsl.validators import validate_strategy


@dataclass(frozen=True)
class CompiledCondition:
    """Compiled condition for deterministic runtime evaluation."""

    kind: str
    params: dict[str, object]


@dataclass(frozen=True)
class CompiledAction:
    """Compiled action for deterministic runtime execution."""

    kind: str
    params: dict[str, object]


@dataclass(frozen=True)
class CompiledStrategy:
    """Compiled representation of a strategy."""

    strategy_id: str
    symbol: str
    enabled: bool
    timezone: tzinfo
    market_open: time
    market_close: time
    opening_range_minutes: int
    risk_dollars: Decimal
    breakeven_trigger_r: Decimal
    constraints: tuple[CompiledCondition, ...]
    entry: tuple[CompiledCondition, ...]
    actions: tuple[CompiledAction, ...]


def compile_strategy(strategy: StrategyModel) -> CompiledStrategy:
    """Compile a validated strategy into an executable representation."""
    validate_strategy(strategy)

    if strategy.session.timezone.upper() == "UTC":
        resolved_timezone: tzinfo = timezone.utc
    else:
        try:
            resolved_timezone = ZoneInfo(strategy.session.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValidationError(f"Unknown timezone: {strategy.session.timezone}") from exc

    return CompiledStrategy(
        strategy_id=strategy.strategy_id,
        symbol=strategy.symbol.upper(),
        enabled=strategy.enabled,
        timezone=resolved_timezone,
        market_open=_parse_time_hhmm(strategy.session.market_open),
        market_close=_parse_time_hhmm(strategy.session.market_close),
        opening_range_minutes=strategy.session.opening_range_minutes,
        risk_dollars=strategy.risk.risk_dollars,
        breakeven_trigger_r=strategy.exit.breakeven_trigger_r,
        constraints=tuple(
            CompiledCondition(kind=item.kind, params=dict(item.params))
            for item in strategy.constraints
        ),
        entry=tuple(
            CompiledCondition(kind=item.kind, params=dict(item.params))
            for item in strategy.entry
        ),
        actions=tuple(CompiledAction(kind=item.kind, params=dict(item.params)) for item in strategy.actions),
    )


def _parse_time_hhmm(value: str) -> time:
    hour_text, minute_text = value.split(":", maxsplit=1)
    return time(hour=int(hour_text), minute=int(minute_text))
