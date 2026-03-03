"""Typed runtime configuration for the application."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "ibkr-exec-bot"
    app_env: str = "dev"
    log_level: str = "INFO"

    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    ibkr_client_id: int = 1
    ibkr_account: str = ""
    ibkr_paper_host: str = "127.0.0.1"
    ibkr_paper_port: int = 4002
    ibkr_paper_client_id: int = 1
    ibkr_paper_account: str = ""
    ibkr_live_host: str = "127.0.0.1"
    ibkr_live_port: int = 4001
    ibkr_live_client_id: int = 2
    ibkr_live_account: str = ""

    database_url: str = "sqlite:///./ibkr_exec.db"

    live_trading: bool = False
    ack_live_trading: str = ""
    dry_run: bool = True

    min_stop_distance: float = Field(default=0.01, ge=0)
    default_risk_dollars: float = Field(default=100.0, ge=0)
    max_positions: int = Field(default=10, ge=1)
    max_notional: float = Field(default=1_000_000.0, gt=0)
    max_daily_loss: float = Field(default=10_000.0, gt=0)
    max_orders_per_minute: int = Field(default=120, ge=1)
    max_trades_per_symbol_per_day: int = Field(default=10, ge=1)
    ibkr_max_requests_per_second: int = Field(default=45, ge=1)
    ibkr_pacing_window_seconds: float = Field(default=1.0, gt=0)
    watchdog_stale_after_seconds: float = Field(default=30.0, gt=0)
    watchdog_check_interval_seconds: float = Field(default=5.0, gt=0)
    memory_log_interval_seconds: float = Field(default=300.0, gt=0)


def get_settings() -> Settings:
    """Return runtime settings."""
    return Settings()
