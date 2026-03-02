"""Intent-level validation entry points."""

from decimal import Decimal

from app.domain.errors import ValidationError
from app.domain.models import OrderIntent


def validate_intent(intent: OrderIntent) -> None:
    """Validate order intent fields before risk and execution checks."""
    if not intent.intent_id.strip():
        raise ValidationError("intent_id is required")

    if not intent.symbol.strip():
        raise ValidationError("symbol is required")

    if intent.entry_price <= Decimal("0"):
        raise ValidationError("entry_price must be positive")

    if intent.stop_price <= Decimal("0"):
        raise ValidationError("stop_price must be positive")

    if intent.risk_dollars <= Decimal("0"):
        raise ValidationError("risk_dollars must be positive")

    if intent.entry_price == intent.stop_price:
        raise ValidationError("entry_price and stop_price cannot be equal")
