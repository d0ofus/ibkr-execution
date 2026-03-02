"""Risk-based position sizing interfaces."""

from decimal import Decimal, ROUND_FLOOR

from app.domain.errors import RiskCheckError


def calculate_position_size(risk_dollars: Decimal, entry_price: Decimal, stop_price: Decimal) -> int:
    """Calculate share quantity from risk and stop distance."""
    if risk_dollars <= Decimal("0"):
        raise RiskCheckError("risk_dollars must be positive")

    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= Decimal("0"):
        raise RiskCheckError("entry and stop must have non-zero distance")

    raw_qty = (risk_dollars / stop_distance).to_integral_value(rounding=ROUND_FLOOR)
    quantity = int(raw_qty)
    if quantity < 1:
        raise RiskCheckError("calculated quantity is less than 1 share")

    return quantity
