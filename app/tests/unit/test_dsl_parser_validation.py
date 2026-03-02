"""Unit tests for DSL parsing, schema loading, and semantic validation."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from app.domain.errors import ValidationError
from app.dsl.compiler import compile_strategy
from app.dsl.parser import load_strategy_from_path, load_strategy_from_text
from app.dsl.validators import validate_strategy


_VALID_YAML = """
version: "1"
strategy_id: "orb_strategy"
symbol: "AAPL"
enabled: true
timeframe: "1m"
session:
  timezone: "UTC"
  market_open: "14:30"
  market_close: "21:00"
  opening_range_minutes: 3
constraints:
  - kind: once_per_day
  - kind: within_time
    params:
      start: "14:30"
      end: "16:00"
  - kind: max_spread_cents
    params:
      cents: 5
  - kind: min_volume
    params:
      value: 100
entry:
  - kind: crosses_above
    params:
      level: opening_range_high
risk:
  risk_dollars: 100
exit:
  breakeven_trigger_r: 1
actions:
  - kind: enter_long
    params:
      initial_stop: session_low
  - kind: move_stop_to_breakeven
    params:
      trigger_r: 1
"""


def _write_temp_file(content: str, suffix: str) -> Path:
    base = Path(".tmp_tests")
    base.mkdir(parents=True, exist_ok=True)
    path = base / f"dsl_{uuid4().hex}{suffix}"
    path.write_text(content, encoding="utf-8")
    return path


def test_load_strategy_from_text_yaml_and_json() -> None:
    yaml_strategy = load_strategy_from_text(_VALID_YAML, source_format="yaml")

    json_payload = (
        "{"
        '"version":"1",'
        '"strategy_id":"orb_strategy",'
        '"symbol":"AAPL",'
        '"enabled":true,'
        '"timeframe":"1m",'
        '"session":{"timezone":"UTC","market_open":"14:30","market_close":"21:00","opening_range_minutes":3},'
        '"constraints":[{"kind":"once_per_day"}],'
        '"entry":[{"kind":"crosses_above","params":{"level":"opening_range_high"}}],'
        '"risk":{"risk_dollars":100},'
        '"actions":[{"kind":"enter_long","params":{"initial_stop":"session_low"}}]'
        "}"
    )
    json_strategy = load_strategy_from_text(json_payload, source_format="json")

    assert yaml_strategy.symbol == "AAPL"
    assert json_strategy.strategy_id == "orb_strategy"


def test_load_strategy_from_path_supports_yaml_and_json() -> None:
    yaml_path = _write_temp_file(_VALID_YAML, ".yaml")
    json_path = _write_temp_file(
        '{"version":"1","strategy_id":"s1","symbol":"AAPL","timeframe":"1m","risk":{"risk_dollars":100},"actions":[{"kind":"enter_long","params":{"initial_stop":"session_low"}}]}',
        ".json",
    )

    yaml_strategy = load_strategy_from_path(yaml_path)
    json_strategy = load_strategy_from_path(json_path)

    assert yaml_strategy.strategy_id == "orb_strategy"
    assert json_strategy.strategy_id == "s1"


def test_validate_strategy_rejects_invalid_enter_long_initial_stop() -> None:
    bad_yaml = _VALID_YAML.replace("initial_stop: session_low", "initial_stop: opening_range_low")
    strategy = load_strategy_from_text(bad_yaml, source_format="yaml")

    with pytest.raises(ValidationError):
        validate_strategy(strategy)


def test_validate_strategy_rejects_invalid_timeframe() -> None:
    bad_yaml = _VALID_YAML.replace("timeframe: \"1m\"", "timeframe: \"5m\"")
    strategy = load_strategy_from_text(bad_yaml, source_format="yaml")

    with pytest.raises(ValidationError):
        validate_strategy(strategy)


def test_compile_strategy_rejects_unknown_timezone() -> None:
    bad_yaml = _VALID_YAML.replace('timezone: "UTC"', 'timezone: "Mars/Base"')
    strategy = load_strategy_from_text(bad_yaml, source_format="yaml")

    with pytest.raises(ValidationError):
        compile_strategy(strategy)
