"""Golden replay test for ORB DSL behavior and contract-free offline simulation."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from app.replay.replay_runner import ReplayRunner


def _tmp_output_path() -> Path:
    base = Path(".tmp_tests")
    base.mkdir(parents=True, exist_ok=True)
    return base / f"replay_report_{uuid4().hex}.json"


def test_orb_replay_golden_sample() -> None:
    runner = ReplayRunner()
    strategy_path = Path("app/replay/datasets/orb_strategy.yaml")
    bars_path = Path("app/replay/datasets/orb_sample.csv")
    output_path = _tmp_output_path()

    runner.run(strategy_path=strategy_path, bars_path=bars_path, output_path=output_path)

    report = json.loads(output_path.read_text(encoding="utf-8"))

    assert report["mode"] == "replay"
    assert report["bars_processed"] == 6

    intents = report["intents"]
    enter_intents = [item for item in intents if item["intent_type"] == "enter_long"]
    breakeven_intents = [item for item in intents if item["intent_type"] == "move_stop_to_breakeven"]

    assert len(enter_intents) == 1
    assert len(breakeven_intents) == 1

    entry_intent = enter_intents[0]
    assert entry_intent["entry_price"] == "100.30"
    assert entry_intent["stop_price"] == "99.40"

    would_orders = report["would_orders"]
    place_events = [item for item in would_orders if item["event"] == "would_place_bracket"]
    breakeven_events = [item for item in would_orders if item["event"] == "would_modify_stop_to_breakeven"]

    assert len(place_events) == 1
    assert place_events[0]["quantity"] == 111
    assert place_events[0]["stop_price"] == "99.40"

    assert len(breakeven_events) == 1
    assert breakeven_events[0]["timestamp"] == "2026-01-05T14:35:00+00:00"
    assert breakeven_events[0]["quantity"] == 111
    assert breakeven_events[0]["new_stop_price"] == "100.30"
