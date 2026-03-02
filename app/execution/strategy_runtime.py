"""Live strategy runtime orchestration from bars to order intents."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Literal, Protocol

from app.broker.contracts import ContractService
from app.broker.ibkr_client import BrokerClient
from app.data.market_data import MarketDataBuffer
from app.domain.enums import EnvironmentMode
from app.domain.errors import StrategyNotFoundError, ValidationError
from app.domain.models import MarketBar, OrderIntent
from app.dsl.compiler import CompiledStrategy, compile_strategy
from app.dsl.engine import DslEngine
from app.dsl.parser import load_strategy_from_text


class OrderManagerLike(Protocol):
    """Execution methods consumed by strategy runtime."""

    def submit_intent(self, intent: OrderIntent) -> str:
        """Submit a strategy-emitted order intent."""

    def apply_breakeven_adjustment(self, trade_id: str) -> bool:
        """Attempt one-time stop move-to-breakeven for a trade."""

    def on_broker_order_status(self, *, broker_order_id: int, filled_quantity: int) -> None:
        """Apply broker fill/status updates for known entry orders."""


@dataclass(frozen=True)
class StrategyDefinition:
    """Stored source definition and compiled metadata."""

    strategy_id: str
    source_format: Literal["yaml", "json"]
    source_payload: str
    symbol: str
    enabled: bool


@dataclass(frozen=True)
class StrategyRuntimeStatus:
    """Runtime status for UI and API reporting."""

    strategy_id: str
    running: bool
    symbol: str
    enabled: bool
    last_error: str | None = None


@dataclass(frozen=True)
class StrategyDefinitionView:
    """Read model for strategy definition retrieval endpoints."""

    strategy_id: str
    source_format: Literal["yaml", "json"]
    source_payload: str
    symbol: str
    enabled: bool


class StrategyExecutionRuntime:
    """Coordinates strategy definitions, bar processing, and execution handoff."""

    def __init__(
        self,
        *,
        order_manager: OrderManagerLike,
        broker_client: BrokerClient,
        contract_service: ContractService,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
        market_data_buffer: MarketDataBuffer | None = None,
    ) -> None:
        self._order_manager = order_manager
        self._broker_client = broker_client
        self._contract_service = contract_service
        self._environment = environment
        self._market_data_buffer = market_data_buffer or MarketDataBuffer(max_bars_per_symbol=10_000)

        self._definitions: dict[str, StrategyDefinition] = {}
        self._compiled: dict[str, CompiledStrategy] = {}
        self._engines: dict[str, DslEngine] = {}
        self._running: set[str] = set()
        self._subscribed_symbols: set[str] = set()
        self._last_errors: dict[str, str] = {}
        self._entry_intent_to_trade_id: dict[str, str] = {}

    def set_environment(self, environment: EnvironmentMode) -> None:
        """Set trading environment used for pin checks and data subscriptions."""
        self._environment = environment

    def upsert_definition(
        self,
        *,
        source_payload: str,
        source_format: Literal["yaml", "json"],
    ) -> StrategyRuntimeStatus:
        """Create or update a strategy definition from YAML/JSON source."""
        model = load_strategy_from_text(payload=source_payload, source_format=source_format)
        compiled = compile_strategy(model)
        strategy_id = compiled.strategy_id

        definition = StrategyDefinition(
            strategy_id=strategy_id,
            source_format=source_format,
            source_payload=source_payload,
            symbol=compiled.symbol,
            enabled=compiled.enabled,
        )
        self._definitions[strategy_id] = definition
        self._compiled[strategy_id] = compiled
        self._engines[strategy_id] = DslEngine(compiled)
        self._last_errors.pop(strategy_id, None)

        if strategy_id in self._running:
            # Reset the runtime engine for deterministic restarts after edit.
            self._engines[strategy_id] = DslEngine(compiled)

        return self._status_for(strategy_id)

    def get_definition(self, strategy_id: str) -> StrategyDefinitionView:
        """Return full source definition for strategy editor UI."""
        definition = self._definitions.get(strategy_id)
        if definition is None:
            raise StrategyNotFoundError(f"Strategy {strategy_id} not found.")
        return StrategyDefinitionView(
            strategy_id=definition.strategy_id,
            source_format=definition.source_format,
            source_payload=definition.source_payload,
            symbol=definition.symbol,
            enabled=definition.enabled,
        )

    def list(self) -> list[StrategyRuntimeStatus]:
        """List runtime statuses for all known strategies."""
        return [self._status_for(strategy_id) for strategy_id in sorted(self._definitions)]

    def start(self, strategy_id: str) -> StrategyRuntimeStatus:
        """Start a strategy and subscribe market data for its symbol."""
        compiled = self._compiled.get(strategy_id)
        if compiled is None:
            raise StrategyNotFoundError(f"Strategy {strategy_id} not found.")
        if not compiled.enabled:
            raise ValidationError(f"Strategy {strategy_id} is disabled in its definition.")

        contract = self._contract_service.require_pinned_contract(
            symbol=compiled.symbol,
            environment=self._environment,
        )
        if compiled.symbol not in self._subscribed_symbols:
            self._broker_client.subscribe_bars(contract=contract, bar_size="5 secs")
            self._subscribed_symbols.add(compiled.symbol)

        self._engines[strategy_id] = DslEngine(compiled)
        self._running.add(strategy_id)
        self._last_errors.pop(strategy_id, None)
        return self._status_for(strategy_id)

    def stop(self, strategy_id: str) -> StrategyRuntimeStatus:
        """Stop a strategy."""
        if strategy_id not in self._definitions:
            raise StrategyNotFoundError(f"Strategy {strategy_id} not found.")
        self._running.discard(strategy_id)
        return self._status_for(strategy_id)

    def stop_all(self) -> None:
        """Stop all strategies."""
        self._running.clear()

    def on_realtime_bar(self, bar: MarketBar) -> None:
        """Handle incoming 5-second bars, aggregate to 1-minute, and evaluate strategies."""
        completed_bar = self._market_data_buffer.add_bar_and_aggregate_1m(bar)
        if completed_bar is None:
            return
        self._process_1m_bar(completed_bar)

    def on_order_status_update(self, *, order_id: int, filled_quantity: int) -> None:
        """Forward broker fill updates into order manager runtime state."""
        self._order_manager.on_broker_order_status(
            broker_order_id=order_id,
            filled_quantity=filled_quantity,
        )

    def _process_1m_bar(self, bar: MarketBar) -> None:
        symbol = bar.symbol.upper()
        running_ids = [
            strategy_id
            for strategy_id in self._running
            if self._definitions[strategy_id].symbol == symbol
        ]
        for strategy_id in running_ids:
            engine = self._engines[strategy_id]
            try:
                intents = engine.on_bar(bar, spread_cents=None)
                self._handle_intents(intents)
            except Exception as exc:
                self._last_errors[strategy_id] = str(exc)

    def _handle_intents(self, intents: Sequence[OrderIntent]) -> None:
        for intent in intents:
            if intent.intent_type == "enter_long":
                trade_id = self._order_manager.submit_intent(intent)
                self._entry_intent_to_trade_id[intent.intent_id] = trade_id
                continue

            if intent.intent_type == "move_stop_to_breakeven":
                target_intent_id = intent.metadata.get("target_intent_id")
                if not target_intent_id:
                    continue
                target_trade_id = self._entry_intent_to_trade_id.get(target_intent_id)
                if target_trade_id is None:
                    continue
                _ = self._order_manager.apply_breakeven_adjustment(target_trade_id)

    def _status_for(self, strategy_id: str) -> StrategyRuntimeStatus:
        definition = self._definitions[strategy_id]
        return StrategyRuntimeStatus(
            strategy_id=strategy_id,
            running=strategy_id in self._running,
            symbol=definition.symbol,
            enabled=definition.enabled,
            last_error=self._last_errors.get(strategy_id),
        )
