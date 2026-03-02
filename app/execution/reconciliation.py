"""Broker/local state reconciliation orchestration."""

from datetime import datetime


class ReconciliationService:
    """Synchronize local state with broker state after reconnects."""

    def reconcile(self, since: datetime | None = None) -> None:
        """Run reconciliation flow for orders, executions, and positions."""
        _ = since
        raise NotImplementedError("Reconciliation logic is implemented in Phase 4/6.")
