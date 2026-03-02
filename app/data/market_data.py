"""Bounded-memory market data buffering and deterministic bar aggregation."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from app.domain.models import MarketBar


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def floor_to_minute(timestamp: datetime) -> datetime:
    """Floor a timestamp to the start of its minute in UTC."""
    value = _ensure_utc(timestamp)
    return value.replace(second=0, microsecond=0)


@dataclass
class _MinuteAggregate:
    """Mutable aggregator state for a single symbol-minute bucket."""

    symbol: str
    minute_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    @classmethod
    def from_bar(cls, bar: MarketBar) -> "_MinuteAggregate":
        minute_start = floor_to_minute(bar.timestamp)
        return cls(
            symbol=bar.symbol,
            minute_start=minute_start,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )

    def update(self, bar: MarketBar) -> None:
        self.high = max(self.high, bar.high)
        self.low = min(self.low, bar.low)
        self.close = bar.close
        self.volume += bar.volume

    def to_market_bar(self) -> MarketBar:
        return MarketBar(
            symbol=self.symbol,
            timestamp=self.minute_start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
        )


class MarketDataBuffer:
    """Store and query recent bars in bounded memory."""

    def __init__(self, max_bars_per_symbol: int = 1000) -> None:
        if max_bars_per_symbol <= 0:
            raise ValueError("max_bars_per_symbol must be positive")

        self._max_bars_per_symbol = max_bars_per_symbol
        self._bars_by_symbol: dict[str, deque[MarketBar]] = defaultdict(
            lambda: deque(maxlen=self._max_bars_per_symbol)
        )
        self._active_minute_aggregates: dict[str, _MinuteAggregate] = {}

    def add_bar(self, bar: MarketBar) -> None:
        """Insert a new bar into the bounded buffer."""
        symbol = bar.symbol.strip().upper()
        if not symbol:
            raise ValueError("bar symbol cannot be empty")

        normalized_bar = MarketBar(
            symbol=symbol,
            timestamp=_ensure_utc(bar.timestamp),
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
        )
        self._bars_by_symbol[symbol].append(normalized_bar)

    def add_bar_and_aggregate_1m(self, bar: MarketBar) -> MarketBar | None:
        """Add lower-timeframe bar and return completed 1m bar when minute rolls."""
        self.add_bar(bar)

        symbol = bar.symbol.strip().upper()
        normalized_bar = self._bars_by_symbol[symbol][-1]
        minute_start = floor_to_minute(normalized_bar.timestamp)

        current = self._active_minute_aggregates.get(symbol)
        if current is None:
            self._active_minute_aggregates[symbol] = _MinuteAggregate.from_bar(normalized_bar)
            return None

        if minute_start == current.minute_start:
            current.update(normalized_bar)
            return None

        completed = current.to_market_bar()
        self._active_minute_aggregates[symbol] = _MinuteAggregate.from_bar(normalized_bar)
        return completed

    def flush_aggregate_1m(self, symbol: str) -> MarketBar | None:
        """Flush in-progress minute aggregate for a symbol, if present."""
        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol cannot be empty")

        aggregate = self._active_minute_aggregates.pop(normalized_symbol, None)
        if aggregate is None:
            return None
        return aggregate.to_market_bar()

    def get_recent_bars(self, symbol: str, limit: int) -> list[MarketBar]:
        """Return most recent bars for a symbol."""
        if limit <= 0:
            return []

        normalized_symbol = symbol.strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol cannot be empty")

        bars = list(self._bars_by_symbol.get(normalized_symbol, []))
        return bars[-limit:]
