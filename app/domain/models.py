"""Core domain models for intents, trades, contracts, and persistence projections."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

from app.domain.enums import (
    BrokerOrderRole,
    EnvironmentMode,
    OrderIntentSource,
    Side,
    TimeInForce,
    TradeState,
)


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(tz=UTC)


@dataclass(frozen=True)
class ContractRef:
    """ConId-qualified contract reference used for execution."""

    symbol: str
    con_id: int
    exchange: str = "SMART"
    primary_exchange: str | None = None
    sec_type: str = "STK"
    currency: str = "USD"


@dataclass(frozen=True)
class MarketBar:
    """Market data bar used by DSL and replay engines."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class MarketDataInstrument:
    """Instrument descriptor for market-data subscriptions."""

    symbol: str
    sec_type: str
    exchange: str
    currency: str
    con_id: int | None = None
    primary_exchange: str | None = None


@dataclass(frozen=True)
class OrderIntent:
    """Strategy or manual intent before broker transmission."""

    intent_id: str
    symbol: str
    side: Side
    entry_price: Decimal
    stop_price: Decimal
    risk_dollars: Decimal
    source: OrderIntentSource = OrderIntentSource.API
    strategy_id: str | None = None
    intent_type: str = "enter_long"
    metadata: dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class BrokerOrderSpec:
    """Broker-agnostic order specification for transmission adapters."""

    role: BrokerOrderRole
    side: Side
    quantity: int
    order_type: str
    time_in_force: TimeInForce
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    order_id: int | None = None
    parent_order_id: int | None = None
    transmit: bool = True
    algo_strategy: str | None = None
    algo_params: dict[str, str] = field(default_factory=dict)
    modification_of_order_id: int | None = None
    is_adjustable: bool = False
    trigger_price: Decimal | None = None
    adjusted_stop_price: Decimal | None = None
    adjust_once: bool = False


@dataclass(frozen=True)
class TradeRecord:
    """Persistent trade state projection."""

    trade_id: str
    intent_id: str
    symbol: str
    side: Side
    quantity: int
    state: TradeState
    environment: EnvironmentMode
    con_id: int | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class PinnedContract:
    """Pinned contract identity used for failsafe execution."""

    symbol: str
    environment: EnvironmentMode
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str = "STK"
    currency: str = "USD"
    is_active: bool = True
    pinned_at: datetime = field(default_factory=utc_now)
    revoked_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True)
class AuditLogEvent:
    """Immutable audit event record."""

    actor: str
    action: str
    target: str
    payload: dict[str, object]
    payload_hash: str
    created_at: datetime = field(default_factory=utc_now)
    id: int | None = None


@dataclass(frozen=True)
class MarketQuote:
    """Normalized quote snapshot/update emitted by broker transports."""

    symbol: str
    sec_type: str
    exchange: str
    currency: str
    con_id: int | None = None
    primary_exchange: str | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    last: Decimal | None = None
    close: Decimal | None = None
    day_high: Decimal | None = None
    day_low: Decimal | None = None
    prev_day_low: Decimal | None = None
    volume: int | None = None
    avg_volume: int | None = None
    avg_volume_at_time: int | None = None
    delayed: bool = False
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class WorkspaceSettings:
    """Persisted workspace grid settings scoped to user and environment."""

    user_key: str
    environment: EnvironmentMode
    settings_json: str
    updated_at: datetime = field(default_factory=utc_now)
    id: int | None = None
