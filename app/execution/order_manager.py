"""Centralized execution boundary for all broker transmissions."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Protocol
from uuid import uuid4

from app.broker.contracts import ContractService
from app.broker.ibkr_client import BrokerClient
from app.broker.order_builders import (
    build_bracket_orders,
    build_breakeven_adjustment_order,
    build_take_profit_order,
)
from app.domain.enums import (
    AdaptivePriority,
    BrokerOrderRole,
    EnvironmentMode,
    OrderIntentSource,
    Side,
    TimeInForce,
    TradeState,
)
from app.domain.errors import RiskCheckError, ValidationError
from app.domain.models import BrokerOrderSpec, ContractRef, OrderIntent, TradeRecord
from app.risk.limits import LimitCheckInput, LimitChecker, RiskLimits
from app.risk.sizing import calculate_position_size
from app.risk.validation import validate_intent


class TradeRepository(Protocol):
    """Persistence contract used by order manager."""

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
        """Persist a trade projection from an intent."""

    def update_state(self, trade_id: str, state: TradeState) -> TradeRecord | None:
        """Update persisted trade state."""


@dataclass
class TradeRuntimeContext:
    """In-memory runtime state required for order management controls."""

    trade_id: str
    intent: OrderIntent
    contract: ContractRef
    quantity: int
    entry_local_order_id: int
    stop_local_order_id: int
    broker_order_ids: list[str]
    state: TradeState
    filled_quantity: int = 0
    protected_quantity: int = 0
    breakeven_applied: bool = False
    partial_take_profit_applied: bool = False
    partial_take_profit_order_ids: list[str] = field(default_factory=list)


class OrderManager:
    """Validate, risk-check, and transmit intents through a broker client."""

    _TERMINAL_STATES = {
        TradeState.CANCELLED,
        TradeState.CLOSED,
        TradeState.REJECTED,
        TradeState.ERROR,
    }

    def __init__(
        self,
        *,
        broker_client: BrokerClient,
        contract_service: ContractService,
        trade_repository: TradeRepository,
        risk_limits: RiskLimits,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
        min_stop_distance: Decimal = Decimal("0.01"),
        adaptive_priority: AdaptivePriority = AdaptivePriority.NORMAL,
        prefer_adjustable_breakeven: bool = True,
    ) -> None:
        self._broker_client = broker_client
        self._contract_service = contract_service
        self._trade_repository = trade_repository
        self._limit_checker = LimitChecker(risk_limits)
        self._environment = environment
        self._min_stop_distance = min_stop_distance
        self._adaptive_priority = adaptive_priority
        self._prefer_adjustable_breakeven = prefer_adjustable_breakeven

        self._intent_to_trade_id: dict[str, str] = {}
        self._entry_order_id_to_trade_id: dict[int, str] = {}
        self._trade_contexts: dict[str, TradeRuntimeContext] = {}
        self._orders_sent_at: deque[datetime] = deque()
        self._trades_per_symbol_day: defaultdict[tuple[str, date], int] = defaultdict(int)
        self._realized_daily_loss: Decimal = Decimal("0")
        self._next_local_order_id: int = 1000

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=UTC)

    def set_realized_daily_loss(self, value: Decimal) -> None:
        """Set externally computed realized daily loss used for risk checks."""
        if value < Decimal("0"):
            raise ValidationError("realized daily loss cannot be negative")
        self._realized_daily_loss = value

    def set_environment(self, environment: EnvironmentMode) -> None:
        """Switch execution environment context (paper/live)."""
        self._environment = environment

    def submit_intent(self, intent: OrderIntent) -> str:
        """Submit an order intent and return a trade identifier."""
        return self._submit_intent_internal(intent=intent, quantity_override=None)

    def submit_fixed_qty_entry(
        self,
        *,
        symbol: str,
        side: Side,
        entry_price: Decimal,
        stop_price: Decimal,
        quantity: int,
        strategy_id: str,
        intent_id: str | None = None,
    ) -> str:
        """Submit an entry using explicit fixed quantity (used by ORB runner)."""
        if quantity <= 0:
            raise ValidationError("quantity must be positive")

        risk_dollars = abs(entry_price - stop_price) * Decimal(quantity)
        if risk_dollars <= Decimal("0"):
            raise ValidationError("risk_dollars must be positive")

        intent = OrderIntent(
            intent_id=intent_id or str(uuid4()),
            symbol=symbol.upper(),
            side=side,
            entry_price=entry_price,
            stop_price=stop_price,
            risk_dollars=risk_dollars,
            source=OrderIntentSource.STRATEGY,
            strategy_id=strategy_id,
            intent_type="enter_long",
        )
        return self._submit_intent_internal(intent=intent, quantity_override=quantity)

    def on_fill_update(self, trade_id: str, filled_quantity: int) -> None:
        """Apply fill update and keep protective stop quantity aligned to filled quantity."""
        context = self._require_context(trade_id)

        if filled_quantity < context.filled_quantity:
            raise ValidationError("filled quantity cannot decrease")
        if filled_quantity > context.quantity:
            raise ValidationError("filled quantity cannot exceed order quantity")

        if filled_quantity == context.filled_quantity:
            return

        context.filled_quantity = filled_quantity
        context.protected_quantity = max(context.protected_quantity, filled_quantity)

        if filled_quantity < context.quantity:
            context.state = TradeState.PARTIALLY_FILLED
            self._trade_repository.update_state(trade_id, TradeState.PARTIALLY_FILLED)
        else:
            context.state = TradeState.FILLED
            self._trade_repository.update_state(trade_id, TradeState.FILLED)

        stop_side = self._protective_stop_side(context.intent.side)
        modify_stop = BrokerOrderSpec(
            role=BrokerOrderRole.STOP,
            side=stop_side,
            quantity=context.protected_quantity,
            order_type="MODIFY_STP",
            time_in_force=TimeInForce.DAY,
            stop_price=context.intent.stop_price,
            modification_of_order_id=context.stop_local_order_id,
            adjust_once=False,
        )
        self._broker_client.place_orders(context.contract, [modify_stop])
        self._record_transmitted_orders(order_count=1)

    def on_broker_order_status(
        self,
        *,
        broker_order_id: int,
        filled_quantity: int,
    ) -> None:
        """Apply a broker fill update keyed by broker order identifier."""
        trade_id = self._entry_order_id_to_trade_id.get(broker_order_id)
        if trade_id is None:
            return
        self.on_fill_update(trade_id, filled_quantity=filled_quantity)

    def on_market_price(self, trade_id: str, last_price: Decimal) -> bool:
        """Apply one-time breakeven adjustment when configured trigger is reached."""
        context = self._require_context(trade_id)
        if context.breakeven_applied:
            return False
        if context.filled_quantity <= 0:
            return False

        trigger_price = self._breakeven_trigger_price(context.intent)
        if not self._breakeven_trigger_hit(context.intent.side, last_price, trigger_price):
            return False

        return self._apply_breakeven_adjustment(context)

    def apply_breakeven_adjustment(self, trade_id: str) -> bool:
        """Apply one-time breakeven adjustment without an internal trigger check."""
        context = self._require_context(trade_id)
        if context.breakeven_applied:
            return False
        if context.filled_quantity <= 0:
            return False
        return self._apply_breakeven_adjustment(context)

    def move_stop_to_breakeven(self, trade_id: str) -> bool:
        """Public alias for one-time stop move-to-breakeven action."""
        return self.apply_breakeven_adjustment(trade_id)

    def take_profit_partial(self, trade_id: str, *, qty: int, limit_price: Decimal) -> bool:
        """Submit a partial take-profit and reduce the protective stop quantity."""
        context = self._require_context(trade_id)
        if qty <= 0:
            raise ValidationError("qty must be positive")

        available = context.protected_quantity if context.protected_quantity > 0 else context.filled_quantity
        if available <= 0:
            raise ValidationError("cannot take profit before any fills")
        if qty >= available:
            raise ValidationError("partial take profit qty must be less than protected quantity")

        take_profit_side = self._protective_stop_side(context.intent.side)
        take_profit_order = build_take_profit_order(
            side=take_profit_side,
            quantity=qty,
            limit_price=limit_price,
        )
        broker_order_ids = self._broker_client.place_orders(context.contract, [take_profit_order])
        context.partial_take_profit_order_ids.extend(broker_order_ids)

        remaining = available - qty
        modify_stop = BrokerOrderSpec(
            role=BrokerOrderRole.STOP,
            side=take_profit_side,
            quantity=remaining,
            order_type="MODIFY_STP",
            time_in_force=TimeInForce.DAY,
            stop_price=(context.intent.entry_price if context.breakeven_applied else context.intent.stop_price),
            modification_of_order_id=context.stop_local_order_id,
            adjust_once=False,
        )
        self._broker_client.place_orders(context.contract, [modify_stop])
        context.protected_quantity = remaining
        context.partial_take_profit_applied = True
        context.state = TradeState.EXIT_PENDING
        self._trade_repository.update_state(trade_id, TradeState.EXIT_PENDING)
        self._record_transmitted_orders(order_count=2)
        return True

    def cancel_trade(self, trade_id: str) -> None:
        """Cancel all open orders for an existing trade."""
        context = self._require_context(trade_id)
        for broker_order_id in context.broker_order_ids:
            self._broker_client.cancel_order(broker_order_id)
        for broker_order_id in context.partial_take_profit_order_ids:
            self._broker_client.cancel_order(broker_order_id)

        context.state = TradeState.CANCELLED
        self._trade_repository.update_state(trade_id, TradeState.CANCELLED)

    def list_open_trades(self) -> list[str]:
        """Return identifiers for currently open trades."""
        return [
            trade_id
            for trade_id, context in self._trade_contexts.items()
            if context.state not in self._TERMINAL_STATES
        ]

    def _submit_intent_internal(self, *, intent: OrderIntent, quantity_override: int | None) -> str:
        validate_intent(intent)

        existing_trade_id = self._intent_to_trade_id.get(intent.intent_id)
        if existing_trade_id is not None:
            return existing_trade_id

        stop_distance = abs(intent.entry_price - intent.stop_price)
        if stop_distance < self._min_stop_distance:
            raise RiskCheckError("stop distance is below MIN_STOP_DISTANCE")

        quantity = (
            quantity_override
            if quantity_override is not None
            else calculate_position_size(
                risk_dollars=intent.risk_dollars,
                entry_price=intent.entry_price,
                stop_price=intent.stop_price,
            )
        )

        contract = self._contract_service.require_pinned_contract(
            symbol=intent.symbol,
            environment=self._environment,
        )

        parent_order_id = self._reserve_local_order_id()
        stop_order_id = self._reserve_local_order_id()
        orders = build_bracket_orders(
            side=intent.side,
            quantity=quantity,
            entry_price=intent.entry_price,
            stop_price=intent.stop_price,
            parent_order_id=parent_order_id,
            stop_order_id=stop_order_id,
            adaptive_priority=self._adaptive_priority,
        )

        self._enforce_limits(intent=intent, quantity=quantity, proposed_order_count=len(orders))

        trade_id = str(uuid4())
        self._trade_repository.create_trade_from_intent(
            intent,
            trade_id=trade_id,
            quantity=quantity,
            state=TradeState.TRANSMIT_PENDING,
            environment=self._environment,
            con_id=contract.con_id,
        )

        try:
            broker_order_ids = self._broker_client.place_orders(contract=contract, orders=orders)
        except Exception:
            self._trade_repository.update_state(trade_id, TradeState.ERROR)
            raise

        self._trade_repository.update_state(trade_id, TradeState.BROKER_ACKNOWLEDGED)

        entry_order_id = int(broker_order_ids[0]) if broker_order_ids else parent_order_id
        stop_order_id_final = int(broker_order_ids[1]) if len(broker_order_ids) > 1 else stop_order_id

        context = TradeRuntimeContext(
            trade_id=trade_id,
            intent=intent,
            contract=contract,
            quantity=quantity,
            entry_local_order_id=entry_order_id,
            stop_local_order_id=stop_order_id_final,
            broker_order_ids=list(broker_order_ids),
            state=TradeState.BROKER_ACKNOWLEDGED,
        )
        self._trade_contexts[trade_id] = context
        self._intent_to_trade_id[intent.intent_id] = trade_id
        self._entry_order_id_to_trade_id[context.entry_local_order_id] = trade_id

        self._record_transmitted_orders(order_count=len(orders))
        self._record_trade_for_symbol(intent.symbol)

        return trade_id

    def _enforce_limits(self, *, intent: OrderIntent, quantity: int, proposed_order_count: int) -> None:
        now = self._now()
        self._prune_old_order_timestamps(now)

        current_open_contexts = [
            context
            for context in self._trade_contexts.values()
            if context.state not in self._TERMINAL_STATES
        ]

        current_open_notional = sum(
            (context.intent.entry_price * Decimal(context.quantity) for context in current_open_contexts),
            start=Decimal("0"),
        )
        proposed_notional = intent.entry_price * Decimal(quantity)

        symbol_day_key = (intent.symbol.upper(), now.date())
        metrics = LimitCheckInput(
            current_open_positions=len(current_open_contexts),
            proposed_position_increase=1,
            current_open_notional=current_open_notional,
            proposed_notional=proposed_notional,
            realized_daily_loss=self._realized_daily_loss,
            orders_last_minute=len(self._orders_sent_at),
            proposed_order_count=proposed_order_count,
            trades_for_symbol_today=self._trades_per_symbol_day[symbol_day_key],
        )
        self._limit_checker.check_all(metrics)

    def _record_trade_for_symbol(self, symbol: str) -> None:
        key = (symbol.upper(), self._now().date())
        self._trades_per_symbol_day[key] += 1

    def _record_transmitted_orders(self, *, order_count: int) -> None:
        now = self._now()
        for _ in range(order_count):
            self._orders_sent_at.append(now)
        self._prune_old_order_timestamps(now)

    def _prune_old_order_timestamps(self, now: datetime) -> None:
        cutoff = now - timedelta(minutes=1)
        while self._orders_sent_at and self._orders_sent_at[0] < cutoff:
            self._orders_sent_at.popleft()

    def _reserve_local_order_id(self) -> int:
        order_id = self._next_local_order_id
        self._next_local_order_id += 1
        return order_id

    def _require_context(self, trade_id: str) -> TradeRuntimeContext:
        context = self._trade_contexts.get(trade_id)
        if context is None:
            raise ValidationError(f"unknown trade_id: {trade_id}")
        return context

    @staticmethod
    def _protective_stop_side(entry_side: Side) -> Side:
        return Side.SELL if entry_side == Side.BUY else Side.BUY

    @staticmethod
    def _breakeven_trigger_price(intent: OrderIntent) -> Decimal:
        risk_distance = abs(intent.entry_price - intent.stop_price)
        if intent.side == Side.BUY:
            return intent.entry_price + risk_distance
        return intent.entry_price - risk_distance

    @staticmethod
    def _breakeven_trigger_hit(entry_side: Side, last_price: Decimal, trigger_price: Decimal) -> bool:
        if entry_side == Side.BUY:
            return last_price >= trigger_price
        return last_price <= trigger_price

    def _apply_breakeven_adjustment(self, context: TradeRuntimeContext) -> bool:
        stop_side = self._protective_stop_side(context.intent.side)
        quantity = context.protected_quantity if context.protected_quantity > 0 else context.filled_quantity
        adjustment = build_breakeven_adjustment_order(
            side=stop_side,
            quantity=quantity,
            stop_price=context.intent.entry_price,
            existing_stop_order_id=context.stop_local_order_id,
            trigger_price=self._breakeven_trigger_price(context.intent),
            prefer_adjustable=self._prefer_adjustable_breakeven,
        )
        self._broker_client.place_orders(context.contract, [adjustment])
        self._record_transmitted_orders(order_count=1)
        context.breakeven_applied = True
        return True
