"""Replay runner for deterministic offline strategy simulations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
import csv
import json
from pathlib import Path

from app.domain.models import MarketBar, OrderIntent
from app.dsl.compiler import compile_strategy
from app.dsl.engine import DslEngine
from app.dsl.parser import load_strategy_from_path
from app.risk.sizing import calculate_position_size


@dataclass(frozen=True)
class ReplayBar:
    """Input bar row with optional spread context for DSL constraints."""

    bar: MarketBar
    spread_cents: Decimal | None


class ReplayRunner:
    """Run deterministic strategy replay from bar datasets."""

    def run(self, strategy_path: Path, bars_path: Path, output_path: Path) -> None:
        """Execute replay and persist a JSON report."""
        strategy = load_strategy_from_path(strategy_path)
        compiled = compile_strategy(strategy)
        engine = DslEngine(compiled)

        bars = self._load_bars_csv(bars_path=bars_path, fallback_symbol=compiled.symbol)

        report: dict[str, object] = {
            "mode": "replay",
            "strategy_id": compiled.strategy_id,
            "symbol": compiled.symbol,
            "bars_processed": len(bars),
            "triggers": [],
            "intents": [],
            "would_orders": [],
        }

        entry_quantities_by_intent: dict[str, int] = {}

        for replay_bar in bars:
            emitted = engine.on_bar(replay_bar.bar, spread_cents=replay_bar.spread_cents)
            for intent in emitted:
                trigger: dict[str, object] = {
                    "timestamp": replay_bar.bar.timestamp.isoformat(),
                    "intent_id": intent.intent_id,
                    "intent_type": intent.intent_type,
                }
                self._append(report, "triggers", trigger)

                intent_json = self._intent_to_json(intent)
                self._append(report, "intents", intent_json)

                if intent.intent_type == "enter_long":
                    quantity = calculate_position_size(
                        risk_dollars=intent.risk_dollars,
                        entry_price=intent.entry_price,
                        stop_price=intent.stop_price,
                    )
                    entry_quantities_by_intent[intent.intent_id] = quantity
                    self._append(
                        report,
                        "would_orders",
                        {
                            "timestamp": replay_bar.bar.timestamp.isoformat(),
                            "event": "would_place_bracket",
                            "intent_id": intent.intent_id,
                            "symbol": intent.symbol,
                            "quantity": quantity,
                            "entry_price": str(intent.entry_price),
                            "stop_price": str(intent.stop_price),
                        },
                    )
                elif intent.intent_type == "move_stop_to_breakeven":
                    target_intent_id = intent.metadata.get("target_intent_id", "")
                    target_quantity: int | None = entry_quantities_by_intent.get(target_intent_id)
                    self._append(
                        report,
                        "would_orders",
                        {
                            "timestamp": replay_bar.bar.timestamp.isoformat(),
                            "event": "would_modify_stop_to_breakeven",
                            "intent_id": intent.intent_id,
                            "target_intent_id": target_intent_id,
                            "quantity": target_quantity,
                            "new_stop_price": str(intent.stop_price),
                        },
                    )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    def _load_bars_csv(self, *, bars_path: Path, fallback_symbol: str) -> list[ReplayBar]:
        rows: list[ReplayBar] = []
        with bars_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                timestamp = self._parse_timestamp(raw["timestamp"])
                symbol = (raw.get("symbol") or fallback_symbol).strip().upper()
                spread_cents_text = raw.get("spread_cents")
                spread_value = (
                    None
                    if spread_cents_text in {None, ""}
                    else Decimal(str(spread_cents_text))
                )
                rows.append(
                    ReplayBar(
                        bar=MarketBar(
                            symbol=symbol,
                            timestamp=timestamp,
                            open=Decimal(raw["open"]),
                            high=Decimal(raw["high"]),
                            low=Decimal(raw["low"]),
                            close=Decimal(raw["close"]),
                            volume=int(raw["volume"]),
                        ),
                        spread_cents=spread_value,
                    )
                )

        rows.sort(key=lambda item: item.bar.timestamp)
        return rows

    @staticmethod
    def _append(report: dict[str, object], key: str, payload: dict[str, object]) -> None:
        values = report[key]
        assert isinstance(values, list)
        values.append(payload)

    @staticmethod
    def _intent_to_json(intent: OrderIntent) -> dict[str, object]:
        return {
            "intent_id": intent.intent_id,
            "intent_type": intent.intent_type,
            "strategy_id": intent.strategy_id,
            "symbol": intent.symbol,
            "side": intent.side.value,
            "entry_price": str(intent.entry_price),
            "stop_price": str(intent.stop_price),
            "risk_dollars": str(intent.risk_dollars),
            "metadata": dict(intent.metadata),
            "created_at": intent.created_at.isoformat(),
        }

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
