"""Typed error hierarchy for execution and safety checks."""


class ExecBotError(Exception):
    """Base error type for the application."""


class ValidationError(ExecBotError):
    """Raised for invalid input or state."""


class RiskCheckError(ExecBotError):
    """Raised when risk policy rejects an action."""


class ContractError(ExecBotError):
    """Raised for contract resolution and pinning issues."""


class ContractNotFoundError(ContractError):
    """Raised when no matching contract can be qualified."""


class ContractAmbiguityError(ContractError):
    """Raised when multiple qualifying contracts are found."""


class PinnedContractRequiredError(ContractError):
    """Raised when trading requires an active pinned contract."""


class BrokerConnectivityError(ExecBotError):
    """Raised for broker connection-level issues."""


class PacingLimitError(BrokerConnectivityError):
    """Raised when request pacing limits would be exceeded."""


class PersistenceError(ExecBotError):
    """Raised for persistence layer failures."""


class DuplicateRecordError(PersistenceError):
    """Raised when inserting a duplicate constrained record."""


class RecordNotFoundError(PersistenceError):
    """Raised when a requested record does not exist."""
