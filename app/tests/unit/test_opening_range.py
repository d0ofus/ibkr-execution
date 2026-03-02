"""Unit tests for opening-range and session extrema calculations."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from app.data.opening_range import OpeningRangeCalculator
from app.domain.models import MarketBar


def _bar(minute: int, *, high: str, low: str, close: str) -> MarketBar:
    timestamp = datetime(2026, 1, 5, 14, minute, 0, tzinfo=UTC)
    return MarketBar(
        symbol="AAPL",
        timestamp=timestamp,
        open=Decimal(close),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=100,
    )


def test_opening_range_and_session_low_so_far_are_deterministic() -> None:
    calc = OpeningRangeCalculator(opening_range_minutes=3)

    s1 = calc.on_bar(_bar(30, high="101.0", low="99.8", close="100.5"))
    assert s1.opening_range_high == Decimal("101.0")
    assert s1.opening_range_low == Decimal("99.8")
    assert s1.session_low == Decimal("99.8")

    s2 = calc.on_bar(_bar(31, high="101.2", low="99.5", close="100.7"))
    assert s2.opening_range_high == Decimal("101.2")
    assert s2.opening_range_low == Decimal("99.5")
    assert s2.session_low == Decimal("99.5")

    s3 = calc.on_bar(_bar(32, high="100.9", low="99.6", close="100.3"))
    assert s3.opening_range_high == Decimal("101.2")
    assert s3.opening_range_low == Decimal("99.5")
    assert s3.session_low == Decimal("99.5")

    s4 = calc.on_bar(_bar(33, high="102.0", low="99.4", close="101.8"))
    assert s4.opening_range_high == Decimal("101.2")
    assert s4.opening_range_low == Decimal("99.5")
    assert s4.session_high == Decimal("102.0")
    assert s4.session_low == Decimal("99.4")


def test_opening_range_resets_on_new_session_day() -> None:
    calc = OpeningRangeCalculator(opening_range_minutes=2)

    first_day = MarketBar(
        symbol="AAPL",
        timestamp=datetime(2026, 1, 5, 14, 30, 0, tzinfo=UTC),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
    )
    _ = calc.on_bar(first_day)

    second_day = MarketBar(
        symbol="AAPL",
        timestamp=datetime(2026, 1, 6, 14, 30, 0, tzinfo=UTC),
        open=Decimal("200"),
        high=Decimal("201"),
        low=Decimal("199"),
        close=Decimal("200.5"),
        volume=100,
    )
    snap = calc.on_bar(second_day)

    assert snap.opening_range_high == Decimal("201")
    assert snap.opening_range_low == Decimal("199")
    assert snap.session_high == Decimal("201")
    assert snap.session_low == Decimal("199")
