"""Unit tests for deterministic DSL runtime engine behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from app.domain.models import MarketBar
from app.dsl.compiler import compile_strategy
from app.dsl.engine import DslEngine
from app.dsl.parser import load_strategy_from_text


_STRATEGY_YAML = """
version: "1"
strategy_id: "orb_engine"
symbol: "AAPL"
enabled: true
timeframe: "1m"
session:
  timezone: "UTC"
  market_open: "14:30"
  market_close: "21:00"
  opening_range_minutes: 3
constraints:
  - kind: once_per_day
  - kind: within_time
    params:
      start: "14:30"
      end: "16:00"
  - kind: max_spread_cents
    params:
      cents: 5
  - kind: min_volume
    params:
      value: 100
entry:
  - kind: crosses_above
    params:
      level: opening_range_high
risk:
  risk_dollars: 100
exit:
  breakeven_trigger_r: 1
actions:
  - kind: enter_long
    params:
      initial_stop: session_low
  - kind: move_stop_to_breakeven
    params:
      trigger_r: 1
"""


def _bar(minute: int, *, high: str, low: str, close: str, volume: int = 200) -> MarketBar:
    return MarketBar(
        symbol="AAPL",
        timestamp=datetime(2026, 1, 5, 14, minute, 0, tzinfo=UTC),
        open=Decimal(close),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=volume,
    )


def test_engine_emits_entry_with_session_low_stop_then_breakeven_once() -> None:
    compiled = compile_strategy(load_strategy_from_text(_STRATEGY_YAML, source_format="yaml"))
    engine = DslEngine(compiled)

    emitted = []
    bars = [
        _bar(30, high="100.10", low="99.80", close="99.90"),
        _bar(31, high="100.20", low="99.60", close="99.95"),
        _bar(32, high="100.15", low="99.40", close="100.00"),
        _bar(33, high="100.50", low="99.70", close="100.30"),
        _bar(34, high="101.40", low="100.20", close="101.30"),
        _bar(35, high="101.80", low="101.00", close="101.60"),
    ]

    for bar in bars:
        emitted.extend(engine.on_bar(bar, spread_cents=Decimal("2")))

    assert len(emitted) == 2

    entry = emitted[0]
    assert entry.intent_type == "enter_long"
    assert entry.entry_price == Decimal("100.30")
    assert entry.stop_price == Decimal("99.40")
    assert entry.risk_dollars == Decimal("100")

    breakeven = emitted[1]
    assert breakeven.intent_type == "move_stop_to_breakeven"
    assert breakeven.stop_price == Decimal("100.30")
    assert breakeven.metadata["target_intent_id"] == entry.intent_id


def test_engine_blocks_entry_when_spread_or_volume_constraints_fail() -> None:
    compiled = compile_strategy(load_strategy_from_text(_STRATEGY_YAML, source_format="yaml"))
    engine = DslEngine(compiled)

    bars = [
        _bar(30, high="100.10", low="99.80", close="99.90", volume=200),
        _bar(31, high="100.20", low="99.70", close="99.95", volume=200),
        _bar(32, high="100.15", low="99.60", close="100.00", volume=80),
        _bar(33, high="100.50", low="99.70", close="100.30", volume=80),
    ]

    emitted = []
    for bar in bars:
        emitted.extend(engine.on_bar(bar, spread_cents=Decimal("8")))

    assert emitted == []


def test_engine_once_per_day_prevents_second_entry_same_session() -> None:
    compiled = compile_strategy(load_strategy_from_text(_STRATEGY_YAML, source_format="yaml"))
    engine = DslEngine(compiled)

    bars = [
        _bar(30, high="100.10", low="99.80", close="99.90"),
        _bar(31, high="100.20", low="99.60", close="99.95"),
        _bar(32, high="100.15", low="99.40", close="100.00"),
        _bar(33, high="100.50", low="99.70", close="100.30"),
        _bar(36, high="102.20", low="100.90", close="102.00"),
    ]

    emitted = []
    for bar in bars:
        emitted.extend(engine.on_bar(bar, spread_cents=Decimal("1")))

    entry_count = sum(1 for item in emitted if item.intent_type == "enter_long")
    assert entry_count == 1
