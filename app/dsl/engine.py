"""Deterministic DSL execution engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from app.domain.enums import OrderIntentSource, Side
from app.domain.models import MarketBar, OrderIntent
from app.dsl.compiler import CompiledAction, CompiledCondition, CompiledStrategy


@dataclass
class _EntryState:
    """Track per-session entry and breakeven action state."""

    entry_intent_id: str
    entry_price: Decimal
    stop_price: Decimal
    breakeven_emitted: bool = False


class DslEngine:
    """Evaluate compiled strategy rules against incoming bars."""

    def __init__(self, compiled_strategy: CompiledStrategy) -> None:
        self._compiled_strategy = compiled_strategy
        self._current_session_date: date | None = None
        self._session_high: Decimal | None = None
        self._session_low: Decimal | None = None
        self._opening_range_high: Decimal | None = None
        self._opening_range_low: Decimal | None = None
        self._previous_close: Decimal | None = None
        self._entry_state: _EntryState | None = None

    def on_bar(self, bar: MarketBar, *, spread_cents: Decimal | None = None) -> list[OrderIntent]:
        """Process a bar and return any emitted order intents."""
        if not self._compiled_strategy.enabled:
            return []

        if bar.symbol.upper() != self._compiled_strategy.symbol:
            return []

        local_ts = bar.timestamp.astimezone(self._compiled_strategy.timezone)
        self._reset_if_new_session(local_ts)

        in_market_window = self._in_market_window(local_ts)
        if in_market_window:
            self._session_high = bar.high if self._session_high is None else max(self._session_high, bar.high)
            self._session_low = bar.low if self._session_low is None else min(self._session_low, bar.low)
            self._update_opening_range(local_ts=local_ts, bar=bar)

        emitted: list[OrderIntent] = []

        if in_market_window and self._entry_state is None:
            if self._evaluate_condition_set(self._compiled_strategy.constraints, bar, local_ts, spread_cents) and self._evaluate_condition_set(
                self._compiled_strategy.entry,
                bar,
                local_ts,
                spread_cents,
            ):
                entry_intent = self._emit_enter_long(bar=bar, local_ts=local_ts)
                if entry_intent is not None:
                    emitted.append(entry_intent)

        if in_market_window and self._entry_state is not None:
            breakeven_intent = self._maybe_emit_breakeven(bar=bar, local_ts=local_ts)
            if breakeven_intent is not None:
                emitted.append(breakeven_intent)

        self._previous_close = bar.close
        return emitted

    def _emit_enter_long(self, *, bar: MarketBar, local_ts: datetime) -> OrderIntent | None:
        action = self._find_action("enter_long")
        if action is None:
            return None

        initial_stop_level = str(action.params.get("initial_stop", "session_low"))
        stop_price = self._resolve_level(initial_stop_level, bar)
        if stop_price is None:
            return None

        intent_id = f"{self._compiled_strategy.strategy_id}:{local_ts.date().isoformat()}:enter_long"
        intent = OrderIntent(
            intent_id=intent_id,
            symbol=self._compiled_strategy.symbol,
            side=Side.BUY,
            entry_price=bar.close,
            stop_price=stop_price,
            risk_dollars=self._compiled_strategy.risk_dollars,
            source=OrderIntentSource.STRATEGY,
            strategy_id=self._compiled_strategy.strategy_id,
            intent_type="enter_long",
        )
        self._entry_state = _EntryState(
            entry_intent_id=intent_id,
            entry_price=bar.close,
            stop_price=stop_price,
        )
        return intent

    def _maybe_emit_breakeven(self, *, bar: MarketBar, local_ts: datetime) -> OrderIntent | None:
        action = self._find_action("move_stop_to_breakeven")
        if action is None:
            return None

        assert self._entry_state is not None
        if self._entry_state.breakeven_emitted:
            return None

        trigger_r = Decimal(str(action.params.get("trigger_r", self._compiled_strategy.breakeven_trigger_r)))
        risk_distance = self._entry_state.entry_price - self._entry_state.stop_price
        if risk_distance <= Decimal("0"):
            return None

        trigger_price = self._entry_state.entry_price + (risk_distance * trigger_r)
        if bar.close < trigger_price:
            return None

        intent_id = f"{self._compiled_strategy.strategy_id}:{local_ts.date().isoformat()}:move_stop_to_breakeven"
        intent = OrderIntent(
            intent_id=intent_id,
            symbol=self._compiled_strategy.symbol,
            side=Side.SELL,
            entry_price=self._entry_state.entry_price,
            stop_price=self._entry_state.entry_price,
            risk_dollars=self._compiled_strategy.risk_dollars,
            source=OrderIntentSource.STRATEGY,
            strategy_id=self._compiled_strategy.strategy_id,
            intent_type="move_stop_to_breakeven",
            metadata={"target_intent_id": self._entry_state.entry_intent_id},
        )
        self._entry_state.breakeven_emitted = True
        return intent

    def _evaluate_condition_set(
        self,
        conditions: tuple[CompiledCondition, ...],
        bar: MarketBar,
        local_ts: datetime,
        spread_cents: Decimal | None,
    ) -> bool:
        return all(self._evaluate_condition(item, bar, local_ts, spread_cents) for item in conditions)

    def _evaluate_condition(
        self,
        condition: CompiledCondition,
        bar: MarketBar,
        local_ts: datetime,
        spread_cents: Decimal | None,
    ) -> bool:
        if condition.kind == "once_per_day":
            return self._entry_state is None

        if condition.kind == "within_time":
            start = _parse_time_hhmm(str(condition.params["start"]))
            end = _parse_time_hhmm(str(condition.params["end"]))
            t = local_ts.time()
            return start <= t <= end

        if condition.kind == "max_spread_cents":
            if spread_cents is None:
                return False
            limit = Decimal(str(condition.params["cents"]))
            return spread_cents <= limit

        if condition.kind == "min_volume":
            raw_threshold = condition.params.get("value")
            if raw_threshold is None:
                return False
            threshold = int(str(raw_threshold))
            return bar.volume >= threshold

        if condition.kind == "crosses_above":
            level_name = str(condition.params["level"])
            level_value = self._resolve_level(level_name, bar)
            if level_value is None or self._previous_close is None:
                return False
            return self._previous_close <= level_value and bar.close > level_value

        return False

    def _resolve_level(self, level_name: str, bar: MarketBar) -> Decimal | None:
        if level_name == "opening_range_high":
            return self._opening_range_high
        if level_name == "opening_range_low":
            return self._opening_range_low
        if level_name == "session_low":
            return self._session_low
        if level_name == "session_high":
            return self._session_high
        if level_name == "last_price":
            return bar.close
        return None

    def _find_action(self, kind: str) -> CompiledAction | None:
        return next((item for item in self._compiled_strategy.actions if item.kind == kind), None)

    def _reset_if_new_session(self, local_ts: datetime) -> None:
        session_date = local_ts.date()
        if self._current_session_date == session_date:
            return

        self._current_session_date = session_date
        self._session_high = None
        self._session_low = None
        self._opening_range_high = None
        self._opening_range_low = None
        self._previous_close = None
        self._entry_state = None

    def _in_market_window(self, local_ts: datetime) -> bool:
        now_time = local_ts.time()
        return self._compiled_strategy.market_open <= now_time < self._compiled_strategy.market_close

    def _update_opening_range(self, *, local_ts: datetime, bar: MarketBar) -> None:
        session_start = local_ts.replace(
            hour=self._compiled_strategy.market_open.hour,
            minute=self._compiled_strategy.market_open.minute,
            second=0,
            microsecond=0,
        )
        opening_end = session_start + timedelta(minutes=self._compiled_strategy.opening_range_minutes)
        if not (session_start <= local_ts < opening_end):
            return

        self._opening_range_high = (
            bar.high if self._opening_range_high is None else max(self._opening_range_high, bar.high)
        )
        self._opening_range_low = (
            bar.low if self._opening_range_low is None else min(self._opening_range_low, bar.low)
        )


def _parse_time_hhmm(value: str) -> time:
    hour_text, minute_text = value.split(":", maxsplit=1)
    return datetime.strptime(f"{hour_text}:{minute_text}", "%H:%M").time()
