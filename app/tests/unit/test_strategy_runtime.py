"""Unit tests for live strategy runtime orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.domain.enums import EnvironmentMode
from app.domain.models import ContractRef, MarketBar, OrderIntent
from app.execution.strategy_runtime import StrategyExecutionRuntime


@dataclass
class FakeOrderManager:
    intents: list[OrderIntent] = field(default_factory=list)
    breakeven_trade_ids: list[str] = field(default_factory=list)
    broker_status_updates: list[tuple[int, int]] = field(default_factory=list)

    def submit_intent(self, intent: OrderIntent) -> str:
        self.intents.append(intent)
        return f"trade-{len(self.intents)}"

    def apply_breakeven_adjustment(self, trade_id: str) -> bool:
        self.breakeven_trade_ids.append(trade_id)
        return True

    def on_broker_order_status(self, *, broker_order_id: int, filled_quantity: int) -> None:
        self.broker_status_updates.append((broker_order_id, filled_quantity))


@dataclass
class FakeBrokerClient:
    subscriptions: list[tuple[ContractRef, str]] = field(default_factory=list)

    def connect(self) -> None:
        return

    def disconnect(self) -> None:
        return

    def is_connected(self) -> bool:
        return True

    def place_orders(self, contract: ContractRef, orders):  # type: ignore[no-untyped-def]
        _ = (contract, orders)
        return []

    def cancel_order(self, broker_order_id: str) -> None:
        _ = broker_order_id

    def request_open_orders(self) -> None:
        return

    def request_positions(self) -> None:
        return

    def request_executions(self, since=None):  # type: ignore[no-untyped-def]
        _ = since

    def subscribe_bars(self, contract: ContractRef, bar_size: str) -> None:
        self.subscriptions.append((contract, bar_size))


@dataclass
class FakeContractService:
    def require_pinned_contract(
        self,
        symbol: str,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
    ) -> ContractRef:
        _ = environment
        return ContractRef(symbol=symbol, con_id=12345, exchange="SMART", primary_exchange="NASDAQ")


def _strategy_payload() -> str:
    return """
version: "1.0"
strategy_id: "orb-test"
symbol: "AAPL"
enabled: true
timeframe: "1m"
session:
  timezone: "UTC"
  market_open: "00:00"
  market_close: "23:59"
  opening_range_minutes: 1
constraints:
  - kind: once_per_day
entry:
  - kind: min_volume
    params:
      value: 1
risk:
  risk_dollars: 100
actions:
  - kind: enter_long
    params:
      initial_stop: session_low
  - kind: move_stop_to_breakeven
    params:
      trigger_r: 0.5
"""


def _bars_for_two_minutes() -> list[MarketBar]:
    start = datetime(2026, 1, 2, 15, 30, tzinfo=UTC)
    bars: list[MarketBar] = []

    for index in range(12):
        ts = start + timedelta(seconds=5 * index)
        bars.append(
            MarketBar(
                symbol="AAPL",
                timestamp=ts,
                open=Decimal("100.00"),
                high=Decimal("100.20"),
                low=Decimal("99.00"),
                close=Decimal("100.00"),
                volume=10,
            )
        )

    minute_two_start = start + timedelta(minutes=1)
    for index in range(12):
        ts = minute_two_start + timedelta(seconds=5 * index)
        bars.append(
            MarketBar(
                symbol="AAPL",
                timestamp=ts,
                open=Decimal("100.50"),
                high=Decimal("101.20"),
                low=Decimal("100.40"),
                close=Decimal("101.00"),
                volume=10,
            )
        )
    bars.append(
        MarketBar(
            symbol="AAPL",
            timestamp=minute_two_start + timedelta(minutes=1),
            open=Decimal("101.00"),
            high=Decimal("101.10"),
            low=Decimal("100.90"),
            close=Decimal("101.00"),
            volume=10,
        )
    )
    return bars


def test_strategy_runtime_emits_intents_and_routes_breakeven() -> None:
    order_manager = FakeOrderManager()
    broker_client = FakeBrokerClient()
    contract_service = FakeContractService()
    runtime = StrategyExecutionRuntime(
        order_manager=order_manager,
        broker_client=broker_client,
        contract_service=contract_service,
    )

    upserted = runtime.upsert_definition(source_payload=_strategy_payload(), source_format="yaml")
    assert upserted.strategy_id == "orb-test"
    started = runtime.start("orb-test")
    assert started.running is True
    assert len(broker_client.subscriptions) == 1

    for bar in _bars_for_two_minutes():
        runtime.on_realtime_bar(bar)

    assert any(item.intent_type == "enter_long" for item in order_manager.intents)
    assert "trade-1" in order_manager.breakeven_trade_ids


def test_strategy_runtime_forwards_order_status_updates() -> None:
    order_manager = FakeOrderManager()
    runtime = StrategyExecutionRuntime(
        order_manager=order_manager,
        broker_client=FakeBrokerClient(),
        contract_service=FakeContractService(),
    )
    runtime.on_order_status_update(order_id=1000, filled_quantity=50)
    assert order_manager.broker_status_updates == [(1000, 50)]
