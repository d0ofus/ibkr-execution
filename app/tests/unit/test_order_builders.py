"""Unit tests for order builders (adaptive, bracket, and breakeven adjustment)."""

from decimal import Decimal

import pytest

from app.broker.order_builders import (
    build_adaptive_entry_order,
    build_bracket_orders,
    build_breakeven_adjustment_order,
)
from app.domain.enums import AdaptivePriority, BrokerOrderRole, Side, TimeInForce


def test_build_adaptive_entry_order_sets_adaptive_algo_fields() -> None:
    order = build_adaptive_entry_order(
        side=Side.BUY,
        quantity=25,
        limit_price=Decimal("101.50"),
        priority=AdaptivePriority.PATIENT,
        order_id=9001,
    )

    assert order.role == BrokerOrderRole.ENTRY
    assert order.side == Side.BUY
    assert order.order_type == "LMT"
    assert order.limit_price == Decimal("101.50")
    assert order.algo_strategy == "Adaptive"
    assert order.algo_params == {"adaptivePriority": "Patient"}
    assert order.transmit is True
    assert order.order_id == 9001


def test_build_bracket_orders_have_correct_linkage_and_transmit_flags() -> None:
    parent, stop = build_bracket_orders(
        side=Side.BUY,
        quantity=100,
        entry_price=Decimal("200.25"),
        stop_price=Decimal("198.10"),
        parent_order_id=4100,
        stop_order_id=4101,
        adaptive_priority=AdaptivePriority.NORMAL,
    )

    assert parent.role == BrokerOrderRole.ENTRY
    assert parent.order_id == 4100
    assert parent.parent_order_id is None
    assert parent.transmit is False
    assert parent.algo_strategy == "Adaptive"
    assert parent.algo_params["adaptivePriority"] == "Normal"

    assert stop.role == BrokerOrderRole.STOP
    assert stop.order_id == 4101
    assert stop.parent_order_id == 4100
    assert stop.side == Side.SELL
    assert stop.order_type == "STP"
    assert stop.stop_price == Decimal("198.10")
    assert stop.transmit is True


def test_build_bracket_orders_for_short_entry_inverts_stop_side() -> None:
    parent, stop = build_bracket_orders(
        side=Side.SELL,
        quantity=10,
        entry_price=Decimal("50.00"),
        stop_price=Decimal("51.00"),
    )

    assert parent.side == Side.SELL
    assert stop.side == Side.BUY


def test_build_breakeven_adjustment_prefers_adjustable_stop_when_possible() -> None:
    adjustment = build_breakeven_adjustment_order(
        side=Side.SELL,
        quantity=100,
        stop_price=Decimal("100.00"),
        existing_stop_order_id=5001,
        trigger_price=Decimal("101.00"),
        prefer_adjustable=True,
        time_in_force=TimeInForce.GTC,
    )

    assert adjustment.role == BrokerOrderRole.BREAKEVEN_ADJUSTMENT
    assert adjustment.order_type == "STP"
    assert adjustment.is_adjustable is True
    assert adjustment.trigger_price == Decimal("101.00")
    assert adjustment.adjusted_stop_price == Decimal("100.00")
    assert adjustment.adjust_once is True
    assert adjustment.modification_of_order_id == 5001
    assert adjustment.time_in_force == TimeInForce.GTC


def test_build_breakeven_adjustment_falls_back_to_single_modify_once() -> None:
    adjustment = build_breakeven_adjustment_order(
        side=Side.SELL,
        quantity=75,
        stop_price=Decimal("99.50"),
        existing_stop_order_id=6002,
        trigger_price=None,
        prefer_adjustable=False,
    )

    assert adjustment.order_type == "MODIFY_STP"
    assert adjustment.is_adjustable is False
    assert adjustment.adjust_once is True
    assert adjustment.trigger_price is None
    assert adjustment.modification_of_order_id == 6002
    assert adjustment.transmit is True


def test_build_breakeven_adjustment_rejects_invalid_existing_order_id() -> None:
    with pytest.raises(ValueError):
        _ = build_breakeven_adjustment_order(
            side=Side.SELL,
            quantity=10,
            stop_price=Decimal("1.00"),
            existing_stop_order_id=0,
        )
