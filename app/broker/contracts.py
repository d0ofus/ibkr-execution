"""Contract qualification, ambiguity handling, and pin-enforcement services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from app.domain.enums import EnvironmentMode
from app.domain.errors import (
    ContractAmbiguityError,
    ContractError,
    ContractNotFoundError,
    PinnedContractRequiredError,
)
from app.domain.models import ContractRef, PinnedContract


@dataclass(frozen=True)
class ContractCandidate:
    """Qualified contract candidate returned by broker lookup."""

    symbol: str
    con_id: int
    exchange: str
    primary_exchange: str | None
    sec_type: str
    currency: str


class ContractQualifier(Protocol):
    """Broker-facing protocol for symbol qualification."""

    def qualify(self, symbol: str) -> list[ContractCandidate]:
        """Return all matching contract candidates for the symbol."""


class PinnedContractStore(Protocol):
    """Persistence contract for pinned contract workflows."""

    def pin(self, contract: PinnedContract) -> PinnedContract:
        """Persist and return a pinned contract entry."""

    def get_active(self, symbol: str, environment: EnvironmentMode) -> PinnedContract | None:
        """Return active pinned contract for symbol and environment."""


class ContractService:
    """Contract failsafe service used by order execution workflows."""

    def __init__(
        self,
        qualifier: ContractQualifier,
        pinned_contract_store: PinnedContractStore,
        *,
        allowed_routing_exchanges: Iterable[str] | None = None,
        required_sec_type: str = "STK",
        required_currency: str = "USD",
    ) -> None:
        self._qualifier = qualifier
        self._pinned_contract_store = pinned_contract_store
        allowed = set(allowed_routing_exchanges or {"SMART"})
        self._allowed_routing_exchanges = {item.upper() for item in allowed}
        self._required_sec_type = required_sec_type.upper()
        self._required_currency = required_currency.upper()

    def resolve_candidates(self, symbol: str) -> list[ContractCandidate]:
        """Resolve symbol into filtered contract candidates."""
        normalized_symbol = self._normalize_symbol(symbol)
        raw_candidates = self._qualifier.qualify(normalized_symbol)

        filtered = [
            self._normalize_candidate(candidate, fallback_symbol=normalized_symbol)
            for candidate in raw_candidates
            if self._candidate_is_allowed(candidate)
        ]
        filtered.sort(key=lambda item: item.con_id)

        if not filtered:
            raise ContractNotFoundError(
                f"No valid contracts for {normalized_symbol} under STK/USD and routing policy."
            )

        return filtered

    def resolve_for_execution(self, symbol: str, selected_con_id: int | None = None) -> ContractRef:
        """Resolve and return a single conId-qualified contract for execution."""
        candidates = self.resolve_candidates(symbol)

        if selected_con_id is None:
            if len(candidates) > 1:
                raise ContractAmbiguityError(
                    "Multiple valid contracts found. Explicit conId selection is required."
                )
            selected = candidates[0]
        else:
            selected_match = next((item for item in candidates if item.con_id == selected_con_id), None)
            if selected_match is None:
                raise ContractNotFoundError(
                    f"Selected conId {selected_con_id} is not valid for {self._normalize_symbol(symbol)}."
                )
            selected = selected_match

        return ContractRef(
            symbol=selected.symbol,
            con_id=selected.con_id,
            exchange=selected.exchange,
            primary_exchange=selected.primary_exchange,
            sec_type=selected.sec_type,
            currency=selected.currency,
        )

    def pin_contract(
        self,
        symbol: str,
        environment: EnvironmentMode,
        selected_con_id: int,
    ) -> PinnedContract:
        """Resolve and persist a pinned contract for a symbol and environment."""
        resolved = self.resolve_for_execution(symbol=symbol, selected_con_id=selected_con_id)
        pin = PinnedContract(
            symbol=resolved.symbol,
            environment=environment,
            con_id=resolved.con_id,
            exchange=resolved.exchange,
            primary_exchange=resolved.primary_exchange,
            sec_type=resolved.sec_type,
            currency=resolved.currency,
        )
        return self._pinned_contract_store.pin(pin)

    def require_pinned_contract(
        self,
        symbol: str,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
    ) -> ContractRef:
        """Return active pinned contract or raise if unavailable/invalid."""
        normalized_symbol = self._normalize_symbol(symbol)
        active_pin = self._pinned_contract_store.get_active(
            symbol=normalized_symbol,
            environment=environment,
        )
        if active_pin is None:
            raise PinnedContractRequiredError(
                f"Active pinned contract required for {normalized_symbol} in {environment.value}."
            )

        if active_pin.sec_type.upper() != self._required_sec_type:
            raise ContractError(
                f"Pinned contract secType {active_pin.sec_type} is not allowed."
            )
        if active_pin.currency.upper() != self._required_currency:
            raise ContractError(
                f"Pinned contract currency {active_pin.currency} is not allowed."
            )
        if active_pin.exchange.upper() not in self._allowed_routing_exchanges:
            raise ContractError(
                f"Pinned contract exchange {active_pin.exchange} violates routing policy."
            )

        return ContractRef(
            symbol=active_pin.symbol,
            con_id=active_pin.con_id,
            exchange=active_pin.exchange,
            primary_exchange=active_pin.primary_exchange,
            sec_type=active_pin.sec_type,
            currency=active_pin.currency,
        )

    def _candidate_is_allowed(self, candidate: ContractCandidate) -> bool:
        return (
            candidate.sec_type.upper() == self._required_sec_type
            and candidate.currency.upper() == self._required_currency
            and candidate.exchange.upper() in self._allowed_routing_exchanges
        )

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ContractError("Symbol cannot be empty.")
        return normalized

    @staticmethod
    def _normalize_candidate(candidate: ContractCandidate, fallback_symbol: str) -> ContractCandidate:
        symbol = candidate.symbol.strip().upper() if candidate.symbol.strip() else fallback_symbol
        return ContractCandidate(
            symbol=symbol,
            con_id=candidate.con_id,
            exchange=candidate.exchange.strip().upper(),
            primary_exchange=(
                candidate.primary_exchange.strip().upper()
                if isinstance(candidate.primary_exchange, str) and candidate.primary_exchange.strip()
                else None
            ),
            sec_type=candidate.sec_type.strip().upper(),
            currency=candidate.currency.strip().upper(),
        )
