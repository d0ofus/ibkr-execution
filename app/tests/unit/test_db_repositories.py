"""Unit tests for SQLAlchemy DB setup and repository CRUD behavior."""

from pathlib import Path
from uuid import uuid4

from sqlalchemy import inspect

from app.domain.enums import EnvironmentMode, Side, TradeState
from app.domain.models import PinnedContract, TradeRecord, utc_now
from app.persistence.db import create_all_tables, create_engine_and_session
from app.persistence.repositories import (
    SqlAlchemyAuditLogRepository,
    SqlAlchemyPinnedContractRepository,
    SqlAlchemyTradeRepository,
)


def _make_sqlite_url(db_path: Path) -> str:
    return f"sqlite+pysqlite:///{db_path.as_posix()}"


def _new_db_path(test_name: str) -> Path:
    base_dir = Path(".tmp_tests")
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / f"{test_name}_{uuid4().hex}.db"


def test_schema_contains_required_tables() -> None:
    """DB bootstrap creates required phase-2 tables."""
    engine, _ = create_engine_and_session(_make_sqlite_url(_new_db_path("schema")))
    create_all_tables(engine)

    table_names = set(inspect(engine).get_table_names())
    assert "trades" in table_names
    assert "audit_log" in table_names
    assert "pinned_contracts" in table_names


def test_trade_repository_crud() -> None:
    """Trade repository supports basic create, read, update, and delete."""
    engine, session_factory = create_engine_and_session(_make_sqlite_url(_new_db_path("trades")))
    create_all_tables(engine)

    repo = SqlAlchemyTradeRepository(session_factory)
    now = utc_now()
    trade = TradeRecord(
        trade_id="trade-1",
        intent_id="intent-1",
        symbol="AAPL",
        side=Side.BUY,
        quantity=100,
        state=TradeState.INTENT_RECEIVED,
        environment=EnvironmentMode.PAPER,
        con_id=265598,
        created_at=now,
        updated_at=now,
    )

    created = repo.create(trade)
    assert created.trade_id == "trade-1"
    assert created.state == TradeState.INTENT_RECEIVED

    fetched = repo.get("trade-1")
    assert fetched is not None
    assert fetched.intent_id == "intent-1"

    updated = repo.update_state("trade-1", TradeState.RISK_APPROVED)
    assert updated is not None
    assert updated.state == TradeState.RISK_APPROVED

    deleted = repo.delete("trade-1")
    assert deleted is True
    assert repo.get("trade-1") is None


def test_pinned_contract_repository_pin_and_revoke() -> None:
    """Pinned contract repository keeps one active pin per symbol and environment."""
    engine, session_factory = create_engine_and_session(_make_sqlite_url(_new_db_path("pins")))
    create_all_tables(engine)

    repo = SqlAlchemyPinnedContractRepository(session_factory)
    first_pin = PinnedContract(
        symbol="MSFT",
        environment=EnvironmentMode.PAPER,
        con_id=272093,
        exchange="SMART",
        primary_exchange="NASDAQ",
    )
    second_pin = PinnedContract(
        symbol="MSFT",
        environment=EnvironmentMode.PAPER,
        con_id=272094,
        exchange="SMART",
        primary_exchange="NASDAQ",
    )

    created_first = repo.pin(first_pin)
    assert created_first.id is not None
    assert created_first.is_active is True

    created_second = repo.pin(second_pin)
    assert created_second.id is not None
    assert created_second.con_id == 272094

    active = repo.get_active("MSFT", EnvironmentMode.PAPER)
    assert active is not None
    assert active.con_id == 272094

    history = repo.list_for_symbol("MSFT", EnvironmentMode.PAPER)
    assert len(history) == 2
    assert sum(1 for item in history if item.is_active) == 1

    revoked = repo.revoke_active("MSFT", EnvironmentMode.PAPER)
    assert revoked is True
    assert repo.get_active("MSFT", EnvironmentMode.PAPER) is None


def test_audit_log_repository_append_and_read() -> None:
    """Audit log repository stores immutable events and hash metadata."""
    engine, session_factory = create_engine_and_session(_make_sqlite_url(_new_db_path("audit")))
    create_all_tables(engine)

    repo = SqlAlchemyAuditLogRepository(session_factory)
    event = repo.append_event(
        actor="api",
        action="submit_intent",
        target="orders/intent",
        payload={"intent_id": "intent-9", "quantity": 25},
    )

    assert event.id is not None
    assert len(event.payload_hash) == 64

    fetched = repo.get(event.id)
    assert fetched is not None
    assert fetched.actor == "api"
    assert fetched.payload["intent_id"] == "intent-9"

    items = repo.list_events(limit=10)
    assert len(items) == 1
    assert items[0].id == event.id
