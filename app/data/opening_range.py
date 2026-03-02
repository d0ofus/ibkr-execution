"""Opening range and session extrema calculations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from app.domain.models import MarketBar


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@dataclass(frozen=True)
class OpeningRangeSnapshot:
    """Current opening-range computation output."""

    opening_range_high: Decimal | None
    opening_range_low: Decimal | None
    session_high: Decimal | None
    session_low: Decimal | None


class OpeningRangeCalculator:
    """Compute ORB metrics from incoming bars."""

    def __init__(
        self,
        opening_range_minutes: int,
        *,
        session_start_utc: time = time(14, 30),
    ) -> None:
        if opening_range_minutes <= 0:
            raise ValueError("opening_range_minutes must be positive")

        self._opening_range_minutes = opening_range_minutes
        self._session_start_utc = session_start_utc

        self._current_session_date: date | None = None
        self._opening_range_end: datetime | None = None
        self._opening_range_high: Decimal | None = None
        self._opening_range_low: Decimal | None = None
        self._session_high: Decimal | None = None
        self._session_low: Decimal | None = None

    def on_bar(self, bar: MarketBar) -> OpeningRangeSnapshot:
        """Update calculator state from the next bar."""
        timestamp = _ensure_utc(bar.timestamp)
        self._ensure_session(timestamp)

        self._session_high = bar.high if self._session_high is None else max(self._session_high, bar.high)
        self._session_low = bar.low if self._session_low is None else min(self._session_low, bar.low)

        if self._opening_range_end is not None and timestamp < self._opening_range_end:
            self._opening_range_high = (
                bar.high
                if self._opening_range_high is None
                else max(self._opening_range_high, bar.high)
            )
            self._opening_range_low = (
                bar.low if self._opening_range_low is None else min(self._opening_range_low, bar.low)
            )

        return OpeningRangeSnapshot(
            opening_range_high=self._opening_range_high,
            opening_range_low=self._opening_range_low,
            session_high=self._session_high,
            session_low=self._session_low,
        )

    def _ensure_session(self, timestamp: datetime) -> None:
        if self._current_session_date == timestamp.date():
            return

        self._current_session_date = timestamp.date()
        session_start = datetime.combine(
            timestamp.date(),
            self._session_start_utc,
            tzinfo=UTC,
        )
        self._opening_range_end = session_start + timedelta(minutes=self._opening_range_minutes)
        self._opening_range_high = None
        self._opening_range_low = None
        self._session_high = None
        self._session_low = None
