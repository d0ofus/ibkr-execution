# ibkr-exec-bot

Safety-first IBKR execution bot for US equities.

## Current Status

Phases 0-11 implemented in this repository with deterministic replay coverage and operational hardening.

## Quick Start

1. Create venv: `python -m venv .venv --without-pip`
2. Install deps into venv:
   `python -m pip --python .\.venv\Scripts\python.exe install -e ".[dev]"`
3. Run tests:
   `.\.venv\Scripts\python.exe -m pytest -q -p no:cacheprovider`

## Runbook

### Runtime Safety Gates

1. Paper mode is default.
2. Live mode requires all of:
   - `LIVE_TRADING=true`
   - `ACK_LIVE_TRADING=I_UNDERSTAND`
   - `POST /arm_live` in the current process session
3. `POST /kill` disarms live and blocks strategy starts/order intent submissions.

### Operational Hardening

1. Broker pacing limiter:
   - Controlled by `IBKR_MAX_REQUESTS_PER_SECOND` and `IBKR_PACING_WINDOW_SECONDS`.
   - Exceeding pacing raises `PacingLimitError`.
2. Market data watchdog:
   - Detect stale feed during US market hours.
   - Use `WATCHDOG_STALE_AFTER_SECONDS` and `WATCHDOG_CHECK_INTERVAL_SECONDS`.
   - Intended reconnect action: call `IbkrClient.handle_market_data_stale(...)`.
3. Periodic memory logging:
   - `PeriodicMemoryLogger` starts with API startup and logs memory snapshots.
   - Interval controlled by `MEMORY_LOG_INTERVAL_SECONDS`.

### Service Deployment (systemd)

Service unit with restart policy:
`deploy/systemd/ibkr-exec-bot.service`

Key settings:
1. `Restart=always`
2. `RestartSec=5`
3. `EnvironmentFile=/opt/ibkr-exec-bot/.env`

Install example:
1. `sudo cp deploy/systemd/ibkr-exec-bot.service /etc/systemd/system/`
2. `sudo systemctl daemon-reload`
3. `sudo systemctl enable ibkr-exec-bot`
4. `sudo systemctl start ibkr-exec-bot`
5. `sudo systemctl status ibkr-exec-bot`
