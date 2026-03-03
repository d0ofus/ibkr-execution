"""Database engine, ORM models, and session factory setup."""

from datetime import UTC, datetime

from sqlalchemy import Boolean, DateTime, Engine, Index, Integer, String, Text, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(tz=UTC)


class Base(DeclarativeBase):
    """Declarative base for SQLAlchemy models."""


class TradeOrm(Base):
    """ORM model for trade lifecycle projection."""

    __tablename__ = "trades"

    trade_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    intent_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    state: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    environment: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    con_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        onupdate=utc_now,
    )


class PinnedContractOrm(Base):
    """ORM model for pinned contracts used by contract failsafe policy."""

    __tablename__ = "pinned_contracts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    environment: Mapped[str] = mapped_column(String(16), nullable=False)
    con_id: Mapped[int] = mapped_column(Integer, nullable=False)
    exchange: Mapped[str] = mapped_column(String(16), nullable=False)
    primary_exchange: Mapped[str | None] = mapped_column(String(32), nullable=True)
    sec_type: Mapped[str] = mapped_column(String(16), nullable=False, default="STK")
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="USD")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    pinned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_pinned_contracts_symbol_environment", "symbol", "environment"),
        Index(
            "ux_pinned_contracts_active_symbol_environment",
            "symbol",
            "environment",
            unique=True,
            sqlite_where=text("is_active = 1"),
        ),
    )


class AuditLogOrm(Base):
    """ORM model for immutable audit logs."""

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    action: Mapped[str] = mapped_column(String(128), nullable=False)
    target: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class WorkspaceSettingsOrm(Base):
    """ORM model for persisted workspace state by user/environment."""

    __tablename__ = "workspace_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_key: Mapped[str] = mapped_column(String(128), nullable=False)
    environment: Mapped[str] = mapped_column(String(16), nullable=False)
    settings_json: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    __table_args__ = (
        Index(
            "ux_workspace_settings_user_environment",
            "user_key",
            "environment",
            unique=True,
        ),
    )


def create_engine_and_session(
    database_url: str,
    *,
    echo: bool = False,
) -> tuple[Engine, sessionmaker[Session]]:
    """Create SQLAlchemy engine and session factory."""
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args = {"check_same_thread": False}

    engine = create_engine(database_url, future=True, echo=echo, connect_args=connect_args)
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine, factory


def get_session_factory(database_url: str) -> sessionmaker[Session]:
    """Return a configured SQLAlchemy session factory."""
    _, factory = create_engine_and_session(database_url=database_url)
    return factory


def create_all_tables(engine: Engine) -> None:
    """Create all ORM tables in the configured database."""
    Base.metadata.create_all(bind=engine)


def drop_all_tables(engine: Engine) -> None:
    """Drop all ORM tables in the configured database."""
    Base.metadata.drop_all(bind=engine)
