"""Builders for broker order structures used by execution."""

from decimal import Decimal

from app.domain.enums import AdaptivePriority, BrokerOrderRole, Side, TimeInForce
from app.domain.models import BrokerOrderSpec


def _require_positive_quantity(quantity: int) -> None:
    if quantity <= 0:
        raise ValueError("quantity must be positive")


def _require_positive_price(price: Decimal, field_name: str) -> None:
    if price <= Decimal("0"):
        raise ValueError(f"{field_name} must be positive")


def _opposite_side(side: Side) -> Side:
    return Side.SELL if side == Side.BUY else Side.BUY


def build_adaptive_entry_order(
    side: Side,
    quantity: int,
    limit_price: Decimal,
    priority: AdaptivePriority,
    *,
    order_id: int | None = None,
    parent_order_id: int | None = None,
    transmit: bool = True,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> BrokerOrderSpec:
    """Build a single adaptive algo entry order specification."""
    _require_positive_quantity(quantity)
    _require_positive_price(limit_price, "limit_price")

    return BrokerOrderSpec(
        role=BrokerOrderRole.ENTRY,
        side=side,
        quantity=quantity,
        order_type="LMT",
        time_in_force=time_in_force,
        limit_price=limit_price,
        order_id=order_id,
        parent_order_id=parent_order_id,
        transmit=transmit,
        algo_strategy="Adaptive",
        algo_params={"adaptivePriority": priority.value},
    )


def build_bracket_orders(
    side: Side,
    quantity: int,
    entry_price: Decimal,
    stop_price: Decimal,
    *,
    parent_order_id: int = 1,
    stop_order_id: int = 2,
    adaptive_priority: AdaptivePriority = AdaptivePriority.NORMAL,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> list[BrokerOrderSpec]:
    """Build parent/child bracket order specifications with linkage semantics."""
    _require_positive_quantity(quantity)
    _require_positive_price(entry_price, "entry_price")
    _require_positive_price(stop_price, "stop_price")

    parent_order = build_adaptive_entry_order(
        side=side,
        quantity=quantity,
        limit_price=entry_price,
        priority=adaptive_priority,
        order_id=parent_order_id,
        transmit=False,
        time_in_force=time_in_force,
    )

    stop_order = BrokerOrderSpec(
        role=BrokerOrderRole.STOP,
        side=_opposite_side(side),
        quantity=quantity,
        order_type="STP",
        time_in_force=time_in_force,
        stop_price=stop_price,
        order_id=stop_order_id,
        parent_order_id=parent_order_id,
        transmit=True,
    )

    return [parent_order, stop_order]


def build_breakeven_adjustment_order(
    side: Side,
    quantity: int,
    stop_price: Decimal,
    *,
    existing_stop_order_id: int,
    trigger_price: Decimal | None = None,
    prefer_adjustable: bool = True,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> BrokerOrderSpec:
    """Build breakeven stop adjustment order with adjustable-stop preference."""
    _require_positive_quantity(quantity)
    _require_positive_price(stop_price, "stop_price")

    if existing_stop_order_id <= 0:
        raise ValueError("existing_stop_order_id must be positive")

    if prefer_adjustable and trigger_price is not None:
        _require_positive_price(trigger_price, "trigger_price")
        return BrokerOrderSpec(
            role=BrokerOrderRole.BREAKEVEN_ADJUSTMENT,
            side=side,
            quantity=quantity,
            order_type="STP",
            time_in_force=time_in_force,
            stop_price=stop_price,
            modification_of_order_id=existing_stop_order_id,
            is_adjustable=True,
            trigger_price=trigger_price,
            adjusted_stop_price=stop_price,
            adjust_once=True,
            transmit=True,
        )

    return BrokerOrderSpec(
        role=BrokerOrderRole.BREAKEVEN_ADJUSTMENT,
        side=side,
        quantity=quantity,
        order_type="MODIFY_STP",
        time_in_force=time_in_force,
        stop_price=stop_price,
        modification_of_order_id=existing_stop_order_id,
        is_adjustable=False,
        adjust_once=True,
        transmit=True,
    )


def build_take_profit_order(
    side: Side,
    quantity: int,
    limit_price: Decimal,
    *,
    time_in_force: TimeInForce = TimeInForce.DAY,
) -> BrokerOrderSpec:
    """Build a limit take-profit order specification."""
    _require_positive_quantity(quantity)
    _require_positive_price(limit_price, "limit_price")
    return BrokerOrderSpec(
        role=BrokerOrderRole.TAKE_PROFIT,
        side=side,
        quantity=quantity,
        order_type="LMT",
        time_in_force=time_in_force,
        limit_price=limit_price,
        transmit=True,
    )
