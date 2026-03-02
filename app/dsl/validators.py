"""DSL semantic validators."""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from decimal import InvalidOperation

from app.domain.dsl_models import ActionModel, ConditionModel, StrategyModel
from app.domain.errors import ValidationError


_SUPPORTED_LEVELS = {
    "opening_range_high",
    "opening_range_low",
    "session_low",
    "session_high",
    "last_price",
}


def validate_strategy(strategy: StrategyModel) -> None:
    """Validate a parsed strategy model for semantic correctness."""
    if strategy.timeframe != "1m":
        raise ValidationError("DSL timeframe must be exactly '1m'")

    _validate_time_hhmm(strategy.session.market_open, "session.market_open")
    _validate_time_hhmm(strategy.session.market_close, "session.market_close")

    enter_long_actions = [action for action in strategy.actions if action.kind == "enter_long"]
    if len(enter_long_actions) != 1:
        raise ValidationError("DSL requires exactly one enter_long action")

    for condition in strategy.constraints:
        _validate_condition(condition)
    for condition in strategy.entry:
        _validate_condition(condition)

    for action in strategy.actions:
        _validate_action(action)


def _validate_condition(condition: ConditionModel) -> None:
    params = condition.params

    if condition.kind == "crosses_above":
        level = str(params.get("level", "")).strip()
        if level not in _SUPPORTED_LEVELS:
            raise ValidationError("crosses_above requires a valid 'level'")
        return

    if condition.kind == "within_time":
        start = str(params.get("start", "")).strip()
        end = str(params.get("end", "")).strip()
        _validate_time_hhmm(start, "within_time.start")
        _validate_time_hhmm(end, "within_time.end")
        if _parse_time_hhmm(start) >= _parse_time_hhmm(end):
            raise ValidationError("within_time.start must be earlier than within_time.end")
        return

    if condition.kind == "once_per_day":
        return

    if condition.kind == "max_spread_cents":
        try:
            cents = Decimal(str(params.get("cents", "")))
        except InvalidOperation as exc:
            raise ValidationError("max_spread_cents.cents must be numeric") from exc
        if cents < Decimal("0"):
            raise ValidationError("max_spread_cents.cents must be >= 0")
        return

    if condition.kind == "min_volume":
        try:
            volume = int(params.get("value", -1))
        except (TypeError, ValueError) as exc:
            raise ValidationError("min_volume.value must be an integer") from exc
        if volume < 0:
            raise ValidationError("min_volume.value must be >= 0")
        return

    raise ValidationError(f"Unsupported condition kind: {condition.kind}")


def _validate_action(action: ActionModel) -> None:
    params = action.params

    if action.kind == "enter_long":
        initial_stop = str(params.get("initial_stop", "")).strip()
        if initial_stop != "session_low":
            raise ValidationError("enter_long.initial_stop must be 'session_low'")
        return

    if action.kind == "move_stop_to_breakeven":
        trigger_r_raw = params.get("trigger_r", "1")
        try:
            trigger_r = Decimal(str(trigger_r_raw))
        except InvalidOperation as exc:
            raise ValidationError("move_stop_to_breakeven.trigger_r must be numeric") from exc
        if trigger_r <= Decimal("0"):
            raise ValidationError("move_stop_to_breakeven.trigger_r must be > 0")
        return

    raise ValidationError(f"Unsupported action kind: {action.kind}")


def _validate_time_hhmm(value: str, field_name: str) -> None:
    try:
        _ = _parse_time_hhmm(value)
    except ValueError as exc:
        raise ValidationError(f"{field_name} must be in HH:MM format") from exc


def _parse_time_hhmm(value: str) -> time:
    parts = value.split(":")
    if len(parts) != 2:
        raise ValueError("invalid time")
    hour = int(parts[0])
    minute = int(parts[1])
    return time(hour=hour, minute=minute)
