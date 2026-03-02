"""Domain enumerations used across the system."""

from enum import StrEnum


class EnvironmentMode(StrEnum):
    """Execution environment mode."""

    PAPER = "paper"
    LIVE = "live"
    REPLAY = "replay"


class Side(StrEnum):
    """Order side for equity orders."""

    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(StrEnum):
    """Supported time-in-force values."""

    DAY = "DAY"
    GTC = "GTC"


class TradeState(StrEnum):
    """Trade lifecycle states."""

    INTENT_RECEIVED = "intent_received"
    INTENT_VALIDATED = "intent_validated"
    RISK_APPROVED = "risk_approved"
    READY_TO_TRANSMIT = "ready_to_transmit"
    TRANSMIT_PENDING = "transmit_pending"
    BROKER_ACKNOWLEDGED = "broker_acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    EXIT_PENDING = "exit_pending"
    CLOSED = "closed"
    CANCEL_PENDING = "cancel_pending"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    ERROR = "error"


class BrokerOrderRole(StrEnum):
    """Role of an order in a multi-order trade."""

    ENTRY = "entry"
    STOP = "stop"
    TAKE_PROFIT = "take_profit"
    BREAKEVEN_ADJUSTMENT = "breakeven_adjustment"


class AdaptivePriority(StrEnum):
    """IB Adaptive algo priority profile."""

    PATIENT = "Patient"
    NORMAL = "Normal"
    URGENT = "Urgent"


class OrderIntentSource(StrEnum):
    """Source channel that emitted an order intent."""

    MANUAL = "manual"
    API = "api"
    STRATEGY = "strategy"
