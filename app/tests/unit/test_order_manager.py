"""Unit tests for OrderManager risk, transmission, and trade-control behavior."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest

from app.domain.enums import EnvironmentMode, Side, TradeState
from app.domain.errors import PinnedContractRequiredError, RiskCheckError
from app.domain.models import BrokerOrderSpec, ContractRef, OrderIntent, TradeRecord, utc_now
from app.execution.order_manager import OrderManager
from app.risk.limits import RiskLimits


class FakeBrokerClient:
    """Mock broker client that records order transmissions."""

    def __init__(self) -> None:
        self.place_calls: list[tuple[ContractRef, list[BrokerOrderSpec]]] = []
        self.cancel_calls: list[str] = []

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def place_orders(self, contract: ContractRef, orders: list[BrokerOrderSpec]) -> list[str]:
        self.place_calls.append((contract, list(orders)))
        base = len(self.place_calls) * 100
        return [str(base + i) for i in range(len(orders))]

    def cancel_order(self, broker_order_id: str) -> None:
        self.cancel_calls.append(broker_order_id)

    def request_open_orders(self) -> None:
        pass

    def request_positions(self) -> None:
        pass

    def request_executions(self, since=None):
        _ = since

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        _ = (contract, bar_size)


class FakeContractService:
    """Mock contract service with configurable pin-enforcement behavior."""

    def __init__(self, contract: ContractRef | None = None, *, raise_error: bool = False) -> None:
        self._contract = contract or ContractRef(symbol="AAPL", con_id=265598)
        self._raise_error = raise_error

    def require_pinned_contract(self, symbol: str, environment: EnvironmentMode = EnvironmentMode.PAPER) -> ContractRef:
        _ = (symbol, environment)
        if self._raise_error:
            raise PinnedContractRequiredError("missing pin")
        return self._contract


@dataclass
class FakeTradeRepository:
    """In-memory trade repository used by order-manager tests."""

    trades: dict[str, TradeRecord]
    state_updates: list[tuple[str, TradeState]]

    def create_trade_from_intent(
        self,
        intent: OrderIntent,
        *,
        trade_id: str,
        quantity: int,
        state: TradeState,
        environment: EnvironmentMode,
        con_id: int | None = None,
    ) -> TradeRecord:
        record = TradeRecord(
            trade_id=trade_id,
            intent_id=intent.intent_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=quantity,
            state=state,
            environment=environment,
            con_id=con_id,
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        self.trades[trade_id] = record
        return record

    def update_state(self, trade_id: str, state: TradeState) -> TradeRecord | None:
        record = self.trades.get(trade_id)
        if record is None:
            return None
        updated = TradeRecord(
            trade_id=record.trade_id,
            intent_id=record.intent_id,
            symbol=record.symbol,
            side=record.side,
            quantity=record.quantity,
            state=state,
            environment=record.environment,
            con_id=record.con_id,
            created_at=record.created_at,
            updated_at=utc_now(),
        )
        self.trades[trade_id] = updated
        self.state_updates.append((trade_id, state))
        return updated


_DEFAULT_LIMITS = RiskLimits(
    max_positions=10,
    max_notional=Decimal("1000000"),
    max_daily_loss=Decimal("10000"),
    max_orders_per_minute=100,
    max_trades_per_symbol_per_day=10,
)


def _make_intent(intent_id: str, symbol: str = "AAPL") -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        symbol=symbol,
        side=Side.BUY,
        entry_price=Decimal("100.00"),
        stop_price=Decimal("99.00"),
        risk_dollars=Decimal("250.00"),
    )


def _build_manager(
    *,
    limits: RiskLimits = _DEFAULT_LIMITS,
    min_stop_distance: Decimal = Decimal("0.01"),
    contract_service: FakeContractService | None = None,
) -> tuple[OrderManager, FakeBrokerClient, FakeTradeRepository]:
    broker = FakeBrokerClient()
    repo = FakeTradeRepository(trades={}, state_updates=[])
    manager = OrderManager(
        broker_client=broker,
        contract_service=contract_service or FakeContractService(),
        trade_repository=repo,
        risk_limits=limits,
        min_stop_distance=min_stop_distance,
        environment=EnvironmentMode.PAPER,
    )
    return manager, broker, repo


def test_submit_intent_transmits_with_pinned_conid_and_persists_trade() -> None:
    manager, broker, repo = _build_manager()

    trade_id = manager.submit_intent(_make_intent("intent-1"))

    assert trade_id in repo.trades
    assert len(broker.place_calls) == 1
    contract, orders = broker.place_calls[0]
    assert contract.con_id == 265598
    assert len(orders) == 2
    assert orders[0].quantity == 250
    assert orders[1].quantity == 250
    assert repo.trades[trade_id].state == TradeState.BROKER_ACKNOWLEDGED


def test_submit_intent_is_idempotent_by_intent_id() -> None:
    manager, broker, _ = _build_manager()
    intent = _make_intent("intent-same")

    first_trade_id = manager.submit_intent(intent)
    second_trade_id = manager.submit_intent(intent)

    assert first_trade_id == second_trade_id
    assert len(broker.place_calls) == 1


def test_submit_intent_rejects_when_pinned_contract_missing() -> None:
    manager, broker, _ = _build_manager(contract_service=FakeContractService(raise_error=True))

    with pytest.raises(PinnedContractRequiredError):
        _ = manager.submit_intent(_make_intent("intent-no-pin"))

    assert len(broker.place_calls) == 0


def test_submit_intent_rejects_when_stop_distance_below_minimum() -> None:
    manager, _, _ = _build_manager(min_stop_distance=Decimal("0.50"))
    intent = OrderIntent(
        intent_id="intent-tight",
        symbol="AAPL",
        side=Side.BUY,
        entry_price=Decimal("100.00"),
        stop_price=Decimal("99.75"),
        risk_dollars=Decimal("100.00"),
    )

    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(intent)


def test_limits_max_positions_enforced() -> None:
    limits = RiskLimits(
        max_positions=1,
        max_notional=Decimal("1000000"),
        max_daily_loss=Decimal("10000"),
        max_orders_per_minute=100,
        max_trades_per_symbol_per_day=10,
    )
    manager, _, _ = _build_manager(limits=limits)

    _ = manager.submit_intent(_make_intent("intent-p1", symbol="AAPL"))
    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(_make_intent("intent-p2", symbol="MSFT"))


def test_limits_max_notional_enforced() -> None:
    limits = RiskLimits(
        max_positions=10,
        max_notional=Decimal("1000"),
        max_daily_loss=Decimal("10000"),
        max_orders_per_minute=100,
        max_trades_per_symbol_per_day=10,
    )
    manager, _, _ = _build_manager(limits=limits)

    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(_make_intent("intent-notional"))


def test_limits_max_daily_loss_enforced() -> None:
    limits = RiskLimits(
        max_positions=10,
        max_notional=Decimal("1000000"),
        max_daily_loss=Decimal("1000"),
        max_orders_per_minute=100,
        max_trades_per_symbol_per_day=10,
    )
    manager, _, _ = _build_manager(limits=limits)
    manager.set_realized_daily_loss(Decimal("1000"))

    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(_make_intent("intent-loss"))


def test_limits_orders_per_minute_enforced() -> None:
    limits = RiskLimits(
        max_positions=10,
        max_notional=Decimal("1000000"),
        max_daily_loss=Decimal("10000"),
        max_orders_per_minute=2,
        max_trades_per_symbol_per_day=10,
    )
    manager, _, _ = _build_manager(limits=limits)

    _ = manager.submit_intent(_make_intent("intent-o1", symbol="AAPL"))
    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(_make_intent("intent-o2", symbol="MSFT"))


def test_limits_trades_per_symbol_per_day_enforced() -> None:
    limits = RiskLimits(
        max_positions=10,
        max_notional=Decimal("1000000"),
        max_daily_loss=Decimal("10000"),
        max_orders_per_minute=100,
        max_trades_per_symbol_per_day=1,
    )
    manager, _, _ = _build_manager(limits=limits)

    _ = manager.submit_intent(_make_intent("intent-s1", symbol="AAPL"))
    with pytest.raises(RiskCheckError):
        _ = manager.submit_intent(_make_intent("intent-s2", symbol="AAPL"))


def test_partial_fill_updates_stop_order_quantity_to_filled_quantity() -> None:
    manager, broker, repo = _build_manager()
    trade_id = manager.submit_intent(_make_intent("intent-fill"))

    manager.on_fill_update(trade_id, filled_quantity=100)

    assert len(broker.place_calls) == 2
    _, modify_orders = broker.place_calls[1]
    assert len(modify_orders) == 1
    modify = modify_orders[0]
    assert modify.order_type == "MODIFY_STP"
    assert modify.quantity == 100
    assert modify.modification_of_order_id is not None
    assert repo.trades[trade_id].state == TradeState.PARTIALLY_FILLED


def test_breakeven_adjustment_applies_once_when_trigger_hit() -> None:
    manager, broker, _ = _build_manager()
    trade_id = manager.submit_intent(_make_intent("intent-be"))
    manager.on_fill_update(trade_id, filled_quantity=250)

    applied_first = manager.on_market_price(trade_id, last_price=Decimal("101.00"))
    applied_second = manager.on_market_price(trade_id, last_price=Decimal("102.00"))

    assert applied_first is True
    assert applied_second is False

    assert len(broker.place_calls) == 3
    _, adjustment_orders = broker.place_calls[2]
    adjustment = adjustment_orders[0]
    assert adjustment.adjust_once is True
    assert adjustment.stop_price == Decimal("100.00")


def test_cancel_trade_cancels_broker_orders_and_marks_state() -> None:
    manager, broker, repo = _build_manager()
    trade_id = manager.submit_intent(_make_intent("intent-cancel"))

    manager.cancel_trade(trade_id)

    assert len(broker.cancel_calls) == 2
    assert repo.trades[trade_id].state == TradeState.CANCELLED
    assert trade_id not in manager.list_open_trades()


def test_submit_fixed_qty_entry_uses_explicit_quantity() -> None:
    manager, broker, _ = _build_manager()

    trade_id = manager.submit_fixed_qty_entry(
        symbol="AAPL",
        side=Side.BUY,
        entry_price=Decimal("100.00"),
        stop_price=Decimal("99.00"),
        quantity=40,
        strategy_id="workspace_orb",
        intent_id="orb-fixed-1",
    )

    assert trade_id
    assert len(broker.place_calls) == 1
    _, orders = broker.place_calls[0]
    assert orders[0].quantity == 40
    assert orders[1].quantity == 40


def test_take_profit_partial_reduces_stop_quantity_to_remaining() -> None:
    manager, broker, _ = _build_manager()
    trade_id = manager.submit_intent(_make_intent("intent-partial"))
    manager.on_fill_update(trade_id, filled_quantity=250)

    result = manager.take_profit_partial(trade_id, qty=80, limit_price=Decimal("102.00"))

    assert result is True
    assert len(broker.place_calls) == 4

    _, take_profit_orders = broker.place_calls[2]
    assert take_profit_orders[0].role.value == "take_profit"
    assert take_profit_orders[0].quantity == 80

    _, stop_modify_orders = broker.place_calls[3]
    assert stop_modify_orders[0].order_type == "MODIFY_STP"
    assert stop_modify_orders[0].quantity == 170
