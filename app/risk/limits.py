"""Risk limit models and checks."""

from dataclasses import dataclass
from decimal import Decimal

from app.domain.errors import RiskCheckError


@dataclass(frozen=True)
class RiskLimits:
    """Static risk limits for order and trade checks."""

    max_positions: int
    max_notional: Decimal
    max_daily_loss: Decimal
    max_orders_per_minute: int
    max_trades_per_symbol_per_day: int


@dataclass(frozen=True)
class LimitCheckInput:
    """Runtime values used by risk-limit checks."""

    current_open_positions: int
    proposed_position_increase: int
    current_open_notional: Decimal
    proposed_notional: Decimal
    realized_daily_loss: Decimal
    orders_last_minute: int
    proposed_order_count: int
    trades_for_symbol_today: int


class LimitChecker:
    """Evaluate whether intents can proceed under configured limits."""

    def __init__(self, limits: RiskLimits) -> None:
        self._limits = limits

    def check_all(self, metrics: LimitCheckInput) -> None:
        """Run all configured limit checks."""
        if metrics.current_open_positions + metrics.proposed_position_increase > self._limits.max_positions:
            raise RiskCheckError("max_positions limit exceeded")

        projected_notional = metrics.current_open_notional + metrics.proposed_notional
        if projected_notional > self._limits.max_notional:
            raise RiskCheckError("max_notional limit exceeded")

        if metrics.realized_daily_loss >= self._limits.max_daily_loss:
            raise RiskCheckError("max_daily_loss limit exceeded")

        if metrics.orders_last_minute + metrics.proposed_order_count > self._limits.max_orders_per_minute:
            raise RiskCheckError("max_orders_per_minute limit exceeded")

        if metrics.trades_for_symbol_today + 1 > self._limits.max_trades_per_symbol_per_day:
            raise RiskCheckError("max_trades_per_symbol_per_day limit exceeded")
