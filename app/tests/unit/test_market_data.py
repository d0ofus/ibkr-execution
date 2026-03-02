"""Unit tests for market data buffering and deterministic 1-minute aggregation."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from app.data.market_data import MarketDataBuffer
from app.domain.models import MarketBar


def _bar(second: int, *, price_base: Decimal, volume: int = 10) -> MarketBar:
    timestamp = datetime(2026, 1, 5, 14, 30, second, tzinfo=UTC)
    return MarketBar(
        symbol="AAPL",
        timestamp=timestamp,
        open=price_base,
        high=price_base + Decimal("0.30"),
        low=price_base - Decimal("0.20"),
        close=price_base + Decimal("0.10"),
        volume=volume,
    )


def test_market_data_buffer_bounded_memory_keeps_recent_bars_only() -> None:
    buffer = MarketDataBuffer(max_bars_per_symbol=3)

    for idx in range(5):
        buffer.add_bar(
            MarketBar(
                symbol="AAPL",
                timestamp=datetime(2026, 1, 5, 14, 30, idx, tzinfo=UTC),
                open=Decimal("100") + Decimal(idx),
                high=Decimal("101") + Decimal(idx),
                low=Decimal("99") + Decimal(idx),
                close=Decimal("100.5") + Decimal(idx),
                volume=100,
            )
        )

    recent = buffer.get_recent_bars("AAPL", limit=10)
    assert len(recent) == 3
    assert [bar.open for bar in recent] == [Decimal("102"), Decimal("103"), Decimal("104")]


def test_aggregate_5s_bars_to_completed_1m_bar() -> None:
    buffer = MarketDataBuffer(max_bars_per_symbol=500)

    completed = None
    completed = buffer.add_bar_and_aggregate_1m(_bar(0, price_base=Decimal("100.00"), volume=10))
    assert completed is None

    completed = buffer.add_bar_and_aggregate_1m(_bar(5, price_base=Decimal("100.10"), volume=20))
    assert completed is None

    completed = buffer.add_bar_and_aggregate_1m(_bar(10, price_base=Decimal("99.90"), volume=30))
    assert completed is None

    next_minute_bar = MarketBar(
        symbol="AAPL",
        timestamp=datetime(2026, 1, 5, 14, 31, 0, tzinfo=UTC),
        open=Decimal("100.50"),
        high=Decimal("100.70"),
        low=Decimal("100.40"),
        close=Decimal("100.60"),
        volume=40,
    )
    completed = buffer.add_bar_and_aggregate_1m(next_minute_bar)

    assert completed is not None
    assert completed.timestamp == datetime(2026, 1, 5, 14, 30, 0, tzinfo=UTC)
    assert completed.open == Decimal("100.00")
    assert completed.high == Decimal("100.40")
    assert completed.low == Decimal("99.70")
    assert completed.close == Decimal("100.00")
    assert completed.volume == 60


def test_flush_aggregate_returns_in_progress_minute_bar() -> None:
    buffer = MarketDataBuffer(max_bars_per_symbol=500)

    _ = buffer.add_bar_and_aggregate_1m(_bar(0, price_base=Decimal("101.00"), volume=11))
    _ = buffer.add_bar_and_aggregate_1m(_bar(5, price_base=Decimal("101.20"), volume=13))

    flushed = buffer.flush_aggregate_1m("AAPL")
    assert flushed is not None
    assert flushed.open == Decimal("101.00")
    assert flushed.close == Decimal("101.30")
    assert flushed.high == Decimal("101.50")
    assert flushed.low == Decimal("100.80")
    assert flushed.volume == 24

    assert buffer.flush_aggregate_1m("AAPL") is None
