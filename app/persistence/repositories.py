"""Concrete SQLAlchemy repositories for trades, pinned contracts, and audit logs."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
import json

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from app.domain.enums import EnvironmentMode, Side, TradeState
from app.domain.errors import DuplicateRecordError, PersistenceError
from app.domain.models import AuditLogEvent, OrderIntent, PinnedContract, TradeRecord, utc_now
from app.persistence.db import AuditLogOrm, PinnedContractOrm, TradeOrm


def _serialize_payload(payload: dict[str, object]) -> tuple[str, str]:
    """Serialize a payload to stable JSON and return its SHA-256 hash."""
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    payload_hash = sha256(payload_json.encode("utf-8")).hexdigest()
    return payload_json, payload_hash


class RepositoryBase:
    """Shared repository helper methods."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session_scope(self) -> Iterator[Session]:
        session: Session = self._session_factory()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            raise PersistenceError(str(exc)) from exc
        finally:
            session.close()


class SqlAlchemyTradeRepository(RepositoryBase):
    """Repository for CRUD operations on trade records."""

    def create(self, trade: TradeRecord) -> TradeRecord:
        """Insert and return a trade record."""
        row = TradeOrm(
            trade_id=trade.trade_id,
            intent_id=trade.intent_id,
            symbol=trade.symbol,
            side=trade.side.value,
            quantity=trade.quantity,
            state=trade.state.value,
            environment=trade.environment.value,
            con_id=trade.con_id,
            created_at=trade.created_at,
            updated_at=trade.updated_at,
        )

        session: Session = self._session_factory()
        try:
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._to_model(row)
        except IntegrityError as exc:
            session.rollback()
            raise DuplicateRecordError(str(exc)) from exc
        except SQLAlchemyError as exc:
            session.rollback()
            raise PersistenceError(str(exc)) from exc
        finally:
            session.close()

    def create_trade_from_intent(
        self,
        intent: OrderIntent,
        *,
        trade_id: str,
        quantity: int = 0,
        state: TradeState = TradeState.INTENT_RECEIVED,
        environment: EnvironmentMode = EnvironmentMode.PAPER,
        con_id: int | None = None,
    ) -> TradeRecord:
        """Create a trade record from an order intent."""
        now = utc_now()
        record = TradeRecord(
            trade_id=trade_id,
            intent_id=intent.intent_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=quantity,
            state=state,
            environment=environment,
            con_id=con_id,
            created_at=now,
            updated_at=now,
        )
        return self.create(record)

    def get(self, trade_id: str) -> TradeRecord | None:
        """Return trade by identifier, if present."""
        with self._session_scope() as session:
            row = session.get(TradeOrm, trade_id)
            if row is None:
                return None
            return self._to_model(row)

    def update_state(self, trade_id: str, state: TradeState) -> TradeRecord | None:
        """Update trade state and return updated model."""
        with self._session_scope() as session:
            row = session.get(TradeOrm, trade_id)
            if row is None:
                return None
            row.state = state.value
            row.updated_at = utc_now()
            session.add(row)
            session.flush()
            return self._to_model(row)

    def delete(self, trade_id: str) -> bool:
        """Delete trade by identifier and report whether it existed."""
        with self._session_scope() as session:
            row = session.get(TradeOrm, trade_id)
            if row is None:
                return False
            session.delete(row)
            return True

    def list_all(self) -> list[TradeRecord]:
        """List all trades sorted by creation time."""
        with self._session_scope() as session:
            rows = session.scalars(select(TradeOrm).order_by(TradeOrm.created_at.asc())).all()
            return [self._to_model(row) for row in rows]

    @staticmethod
    def _to_model(row: TradeOrm) -> TradeRecord:
        return TradeRecord(
            trade_id=row.trade_id,
            intent_id=row.intent_id,
            symbol=row.symbol,
            side=Side(row.side),
            quantity=row.quantity,
            state=TradeState(row.state),
            environment=EnvironmentMode(row.environment),
            con_id=row.con_id,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


class SqlAlchemyPinnedContractRepository(RepositoryBase):
    """Repository for pinned contract CRUD and active-pin enforcement."""

    def pin(self, contract: PinnedContract) -> PinnedContract:
        """Create a new active pin and deactivate existing active pin for symbol/environment."""
        with self._session_scope() as session:
            now = utc_now()
            active_rows = session.scalars(
                select(PinnedContractOrm).where(
                    PinnedContractOrm.symbol == contract.symbol,
                    PinnedContractOrm.environment == contract.environment.value,
                    PinnedContractOrm.is_active.is_(True),
                )
            ).all()

            for active_row in active_rows:
                active_row.is_active = False
                active_row.revoked_at = now
                session.add(active_row)

            row = PinnedContractOrm(
                symbol=contract.symbol,
                environment=contract.environment.value,
                con_id=contract.con_id,
                exchange=contract.exchange,
                primary_exchange=contract.primary_exchange,
                sec_type=contract.sec_type,
                currency=contract.currency,
                is_active=True,
                pinned_at=contract.pinned_at,
                revoked_at=None,
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._to_model(row)

    def get_active(self, symbol: str, environment: EnvironmentMode) -> PinnedContract | None:
        """Return the active pinned contract for symbol/environment."""
        with self._session_scope() as session:
            row = session.scalar(
                select(PinnedContractOrm)
                .where(
                    PinnedContractOrm.symbol == symbol,
                    PinnedContractOrm.environment == environment.value,
                    PinnedContractOrm.is_active.is_(True),
                )
                .order_by(PinnedContractOrm.pinned_at.desc())
            )
            if row is None:
                return None
            return self._to_model(row)

    def list_active(self, environment: EnvironmentMode | None = None) -> list[PinnedContract]:
        """List currently active pins, optionally filtered by environment."""
        with self._session_scope() as session:
            stmt = select(PinnedContractOrm).where(PinnedContractOrm.is_active.is_(True))
            if environment is not None:
                stmt = stmt.where(PinnedContractOrm.environment == environment.value)
            rows = session.scalars(stmt.order_by(PinnedContractOrm.symbol.asc())).all()
            return [self._to_model(row) for row in rows]

    def list_for_symbol(self, symbol: str, environment: EnvironmentMode) -> list[PinnedContract]:
        """List all pin history rows for symbol/environment pair."""
        with self._session_scope() as session:
            rows = session.scalars(
                select(PinnedContractOrm)
                .where(
                    PinnedContractOrm.symbol == symbol,
                    PinnedContractOrm.environment == environment.value,
                )
                .order_by(PinnedContractOrm.pinned_at.asc())
            ).all()
            return [self._to_model(row) for row in rows]

    def revoke_active(self, symbol: str, environment: EnvironmentMode) -> bool:
        """Deactivate any active pin for symbol/environment."""
        with self._session_scope() as session:
            row = session.scalar(
                select(PinnedContractOrm).where(
                    PinnedContractOrm.symbol == symbol,
                    PinnedContractOrm.environment == environment.value,
                    PinnedContractOrm.is_active.is_(True),
                )
            )
            if row is None:
                return False
            row.is_active = False
            row.revoked_at = utc_now()
            session.add(row)
            return True

    @staticmethod
    def _to_model(row: PinnedContractOrm) -> PinnedContract:
        return PinnedContract(
            id=row.id,
            symbol=row.symbol,
            environment=EnvironmentMode(row.environment),
            con_id=row.con_id,
            exchange=row.exchange,
            primary_exchange=row.primary_exchange,
            sec_type=row.sec_type,
            currency=row.currency,
            is_active=row.is_active,
            pinned_at=row.pinned_at,
            revoked_at=row.revoked_at,
        )


class SqlAlchemyAuditLogRepository(RepositoryBase):
    """Repository for immutable audit log entries."""

    def append_event(
        self,
        actor: str,
        action: str,
        target: str,
        payload: dict[str, object],
    ) -> AuditLogEvent:
        """Append and return immutable audit log event."""
        payload_json, payload_hash = _serialize_payload(payload)
        with self._session_scope() as session:
            row = AuditLogOrm(
                actor=actor,
                action=action,
                target=target,
                payload_json=payload_json,
                payload_hash=payload_hash,
                created_at=utc_now(),
            )
            session.add(row)
            session.flush()
            session.refresh(row)
            return self._to_model(row)

    def get(self, event_id: int) -> AuditLogEvent | None:
        """Get audit event by identifier."""
        with self._session_scope() as session:
            row = session.get(AuditLogOrm, event_id)
            if row is None:
                return None
            return self._to_model(row)

    def list_events(self, limit: int = 100) -> list[AuditLogEvent]:
        """List most recent audit events ordered by descending ID."""
        with self._session_scope() as session:
            rows = session.scalars(
                select(AuditLogOrm).order_by(AuditLogOrm.id.desc()).limit(limit)
            ).all()
            return [self._to_model(row) for row in rows]

    @staticmethod
    def _to_model(row: AuditLogOrm) -> AuditLogEvent:
        payload = json.loads(row.payload_json)
        return AuditLogEvent(
            id=row.id,
            actor=row.actor,
            action=row.action,
            target=row.target,
            payload=payload,
            payload_hash=row.payload_hash,
            created_at=row.created_at,
        )
