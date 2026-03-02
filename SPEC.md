# IBKR Execution Bot Specification (Phase 0)

## 1) System Overview

`ibkr-exec-bot` is a production-grade Python 3.11+ execution system for US equities on Interactive Brokers (IBKR) via the TWS/Gateway socket API. It supports manual and strategy-driven order entry under strict safety controls, deterministic replay for trust-building, and an architecture that centralizes broker interaction in a single execution boundary.

Core principle: strategies and DSL logic never transmit directly. They emit `OrderIntent` objects. Only `OrderManager` can validate, risk-check, build broker orders, and transmit.

## 2) Non-Negotiables

1. Asset scope: US equities only (`secType=STK`, `currency=USD`).
2. Contract failsafe: all orders use conId-qualified contracts; no raw-symbol placement.
3. Ambiguity handling: if contract resolution returns multiple valid candidates, trading is blocked until explicit conId selection.
4. Pinned contracts: trading requires a pinned contract record; resolution without pin is insufficient for transmission.
5. Execution boundary: only `OrderManager` may call broker order APIs.
6. Safety defaults: paper trading by default.
7. Live gating requires all of:
   - `LIVE_TRADING=true`
   - `ACK_LIVE_TRADING="I_UNDERSTAND"`
   - explicit runtime arming via `POST /arm_live`
8. Replay mode is deterministic, offline-only, and must never connect to IBKR.
9. Golden tests are required for DSL ORB behavior and contract failsafe logic.
10. ORB day-low stop semantics: initial stop is session low so far at entry-time (or optional fill-time mode), not trailing.

## 3) Architecture and Module Responsibilities

### `app/main.py`
- Application entrypoint.
- Bootstraps config, logging, DB, broker client, order manager, API, CLI wiring.

### `app/config.py`
- Environment and runtime configuration model.
- Validates paper/live/replay constraints and safety gates.

### `app/logging_setup.py`
- Structured logging setup.
- Correlation IDs for intents, orders, and broker events.

### `app/domain/`
- `models.py`: domain entities (`OrderIntent`, `Trade`, `PositionSnapshot`, `PinnedContract`, etc.).
- `dsl_models.py`: Pydantic DSL schema objects.
- `enums.py`: canonical enums (`TradeState`, `BrokerOrderRole`, `Side`, `TimeInForce`, `EnvironmentMode`).
- `errors.py`: typed domain and policy exceptions.

### `app/dsl/`
- `parser.py`: YAML/JSON ingest into schema models.
- `validators.py`: semantic validation with explicit user-facing errors.
- `compiler.py`: converts schema to executable deterministic rule plan.
- `engine.py`: bar-driven strategy evaluation producing `OrderIntent` events only.

### `app/broker/`
- `ibkr_client.py`: IBKR socket adapter abstraction and implementation.
- `reconnect.py`: connection supervision and reconnect backoff.
- `ibkr_events.py`: normalization of IB callbacks into internal events.
- `order_builders.py`: broker order object creation (adaptive, bracket, breakeven adjustment).
- `contracts.py`: contract qualification, ambiguity checks, pin enforcement.

### `app/data/`
- `market_data.py`: bar ingestion and bounded-memory aggregation.
- `opening_range.py`: ORB metric computation (OR high/low, session high/low).
- `calendar.py`: US market session windows and trading-day checks.

### `app/risk/`
- `sizing.py`: quantity calculations from risk per trade.
- `limits.py`: account and strategy-level limits.
- `validation.py`: intent-level hard checks before transmission.

### `app/execution/`
- `order_manager.py`: centralized execution orchestrator; only broker order transmitter.
- `reconciliation.py`: post-reconnect and periodic synchronization of orders, fills, and positions.

### `app/persistence/`
- `db.py`: SQLAlchemy engine/session setup and migrations bootstrap strategy.
- `repositories.py`: persistence layer for trades, orders, fills, pins, logs, and snapshots.

### `app/api/`
- `http_server.py`: FastAPI app factory and lifecycle hooks.
- `routes.py`: endpoints for health, contracts, intents, strategy control, kill/live arm.
- `schemas.py`: API request/response models.

### `app/replay/`
- `replay_runner.py`: deterministic offline replay engine.
- `datasets/`: curated fixtures for golden tests.

### `app/cli/`
- `main.py`: command-line controls for local ops and replay.

### `app/tests/`
- Unit tests plus golden tests focused on trust-critical behavior.

## 4) Event Flow

### 4.1 Manual/API Intent Submission
1. Client submits intent (`/orders/intent` or CLI).
2. API validates request schema.
3. `OrderManager.submit_intent()` receives `OrderIntent`.
4. Contract failsafe resolves and enforces pinned conId.
5. Risk validation and sizing run.
6. Idempotency check prevents duplicate transmission.
7. `order_builders` generates broker-native order set.
8. `ibkr_client` transmits order(s).
9. Broker acknowledgments/events update persistence and state machine.
10. API returns accepted/rejected status with reason codes.

### 4.2 Strategy/DSL Triggered Flow
1. Market data updates bars.
2. DSL engine evaluates compiled rules deterministically.
3. Trigger emits `OrderIntent`.
4. Same `OrderManager.submit_intent()` pipeline as manual flow.

### 4.3 Reconnect Flow
1. Connectivity loss detected by IB event codes or heartbeat.
2. Reconnect supervisor enters degraded mode (new live transmissions blocked unless safe policy allows queued intent replay).
3. On reconnection, reconciliation requests open orders, executions, and positions.
4. Internal state converges to broker truth.
5. Execution resumes when reconciliation checkpoint completes.

## 5) Order Lifecycle State Machine

Canonical `TradeState` transitions:

1. `INTENT_RECEIVED`
2. `INTENT_VALIDATED`
3. `RISK_APPROVED`
4. `READY_TO_TRANSMIT`
5. `TRANSMIT_PENDING`
6. `BROKER_ACKNOWLEDGED`
7. `PARTIALLY_FILLED`
8. `FILLED`
9. `EXIT_PENDING`
10. `CLOSED`
11. `CANCEL_PENDING`
12. `CANCELLED`
13. `REJECTED`
14. `ERROR`

Rules:
- Invalid transitions are rejected and logged.
- Partial fill updates must adjust linked protective stop quantity to filled quantity.
- Breakeven stop adjustment is one-time per trade and auditable.
- Terminal states: `CLOSED`, `CANCELLED`, `REJECTED`, `ERROR`.

## 6) Reconnect and Reconciliation Sequence

1. Detect disconnect (`1100`, heartbeat timeout, socket drop).
2. Mark broker session `DISCONNECTED`; emit system alert.
3. Begin reconnect with bounded exponential backoff + jitter.
4. On reconnect success (`1101`/`1102`), mark session `RECONNECTING`.
5. Run reconciliation in strict order:
   - request open orders
   - request executions (since last known execution time)
   - request positions
6. Diff broker truth vs local persisted state.
7. Resolve mismatches:
   - unknown live order -> import and bind
   - missing expected order -> mark `ERROR` and escalate
   - fill mismatch -> apply correction records
8. Mark session `HEALTHY` only after reconciliation checkpoint success.

IB event handling expectations:
- `1100`: IB/TWS connectivity lost -> trading blocked.
- `1101`: connectivity restored, data lost -> resubscribe market data + reconcile.
- `1102`: connectivity restored, data maintained -> reconcile before full unfreeze.
- `1300`: socket connection changed -> restart connection path and reconcile.

## 7) Contract Failsafe and Pinned Contract Policy

### 7.1 Resolution Policy
1. Resolve user symbol to candidate contracts via IB qualification.
2. Enforce hard filters:
   - `secType == STK`
   - `currency == USD`
   - exchange/routing policy (default SMART-only for execution)
3. If zero candidates: reject with `ContractNotFoundError`.
4. If multiple candidates: return candidates and reject execution with `ContractAmbiguityError` until explicit selection.
5. If one candidate: eligible for pin flow.

### 7.2 Pinned Contract Flow
1. `POST /contracts/resolve` returns candidate list and metadata.
2. Operator selects conId and calls `POST /contracts/pin`.
3. Persist `pinned_contracts` row (symbol, conId, primaryExchange, exchange, currency, secType, timestamps, status).
4. `OrderManager` requires active pin for every tradable symbol.
5. Order placement always uses pinned conId-qualified `Contract` object.
6. Pin updates are versioned and auditable.

### 7.3 Enforcement
- Missing pin -> hard reject intent.
- Pin mismatch with new resolution result -> block trading until operator re-validates/re-pins.
- Replay mode may use fixture mapping but must still enforce deterministic contract identity semantics.

## 8) Safety Gates and Runtime Modes

### Modes
- `PAPER` (default): can transmit to paper account only.
- `LIVE`: only active when all live gates pass.
- `REPLAY`: offline deterministic mode; broker client disabled.
- `DRY_RUN` (optional safety overlay): full validation/risk path without transmission.

### Live Activation Gates
All must be true simultaneously:
1. `LIVE_TRADING=true`
2. `ACK_LIVE_TRADING="I_UNDERSTAND"`
3. Session arming call `POST /arm_live` within current process lifetime.

Additional rules:
- Arming resets on process restart.
- `POST /kill` immediately disarms and blocks new transmissions.
- Strategy start in live mode requires armed session check.

## 9) DSL Schema Outline

Supported format: YAML or JSON.

Top-level fields:
1. `version`
2. `strategy_id`
3. `symbol`
4. `enabled`
5. `timeframe` (1m required for ORB engine)
6. `session` (market open window, timezone)
7. `constraints`
8. `entry`
9. `risk`
10. `exit`
11. `actions`

Condition primitives:
- `crosses_above(level)`
- `within_time(start,end)`
- `once_per_day`
- `max_spread_cents`
- `min_volume`

Level primitives:
- `opening_range_high`
- `opening_range_low`
- `session_low`
- `session_high`
- `last_price`

Action primitives:
- `enter_long` with `initial_stop: session_low`
- `move_stop_to_breakeven` (one-time adjustment trigger)

Constraints:
- Deterministic evaluation order.
- No side effects except emitting typed `OrderIntent`.
- No broker calls inside DSL runtime.

## 10) Replay and Golden Testing Requirements

### Replay Runner
- Inputs: strategy file + CSV bars + config overrides.
- Outputs: deterministic JSON report containing:
  - triggers timeline
  - emitted intents
  - computed sizing
  - simulated order actions (`would_place`, `would_modify`, `would_cancel`)
- Must never create socket connections or call broker APIs.

### Golden Tests (Minimum)
1. ORB sample case produces exactly one trade intent.
2. Initial stop equals session low so far at entry event (not trailing).
3. Sizing uses `floor(risk / abs(entry - stop))`.
4. Breakeven adjustment fires once at configured trigger.
5. Contract ambiguity test blocks transmission and returns candidate list.
6. Pinned contract enforcement test rejects unpinned symbol.

Determinism rules:
- Fixed clock injection.
- Stable sorting for equal timestamps.
- Seeded/random-free execution path.

## 11) Persistence Model (Initial)

Required tables (minimum):
1. `trades`
2. `order_events`
3. `fills`
4. `positions`
5. `audit_log`
6. `pinned_contracts`
7. `strategy_runs`
8. `risk_events`

`audit_log` requirements:
- Immutable append-only behavior.
- Stores actor, action, target, payload hash, timestamp.

`pinned_contracts` requirements:
- Unique active pin per `(symbol, environment)`.
- Tracks conId and key contract attributes.
- Supports revocation and replacement history.

## 12) Risk and Validation Policy

Pre-transmit hard checks:
1. Symbol has active pinned contract.
2. Entry/stop valid and stop distance >= `MIN_STOP_DISTANCE`.
3. Quantity computed and positive.
4. Max positions not exceeded.
5. Max notional not exceeded.
6. Max daily loss not breached.
7. Orders-per-minute and trades-per-symbol-per-day within limits.
8. Idempotency key not already executed.

Position sizing formula:
- `qty = floor(risk_dollars / abs(entry_price - stop_price))`
- Reject if denominator is zero or resulting qty < 1.

## 13) API Surface (Planned)

Control plane endpoints:
1. `GET /health`
2. `GET /status`
3. `POST /contracts/resolve`
4. `POST /contracts/pin`
5. `GET /contracts/pinned`
6. `POST /orders/intent`
7. `POST /orders/cancel/{trade_id}`
8. `GET /orders/open`
9. `GET /strategies`
10. `POST /strategies/start`
11. `POST /strategies/stop`
12. `POST /kill`
13. `POST /arm_live`

All mutating endpoints require audit logging.

## 14) Observability and Operations

1. Structured logs with intent/trade/order correlation IDs.
2. Health includes broker connectivity, market data freshness, reconciliation status, and armed/live state.
3. Watchdog detects missing bars during market hours and triggers reconnect path.
4. Rate limiter protects against IB pacing violations.
5. Periodic memory usage metrics in logs.

## 15) Acceptance Criteria Checklist

A release candidate is acceptable only if all are true:

1. Architecture enforces single execution boundary (`OrderManager`).
2. US-equities-only constraints are technically enforced.
3. Contract failsafe blocks ambiguity and unpinned execution.
4. Live trading cannot occur without all three live gates.
5. Replay mode is fully offline and deterministic.
6. ORB stop semantics match "session low so far at entry/fill policy".
7. Reconnect path includes reconciliation before healthy/unfrozen state.
8. Bracket/adaptive/breakeven order building behavior is tested.
9. Golden tests pass for ORB and failsafe critical paths.
10. Auditability exists for intents, state transitions, and safety actions.
11. API and CLI paths both route through the same `OrderManager` pipeline.
12. Mypy-friendly typed interfaces exist for all core modules.

## 16) Phase Execution Contract

Implementation proceeds strictly by phases 1-11.
For each phase:
1. modify only required files,
2. provide created/modified file list,
3. provide concise run/verify steps,
4. stop and wait for explicit `NEXT`.
