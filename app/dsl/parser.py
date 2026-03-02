"""Parse strategy definitions from YAML or JSON sources."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import ValidationError as PydanticValidationError
import yaml

from app.domain.dsl_models import StrategyModel
from app.domain.errors import ValidationError


def load_strategy_from_path(path: Path) -> StrategyModel:
    """Load and parse a strategy definition from disk."""
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        source_format: Literal["yaml", "json"] = "yaml"
    elif suffix == ".json":
        source_format = "json"
    else:
        raise ValidationError("Strategy file must have .yaml, .yml, or .json extension")

    try:
        payload = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValidationError(f"Unable to read strategy file: {exc}") from exc

    return load_strategy_from_text(payload=payload, source_format=source_format)


def load_strategy_from_text(payload: str, source_format: Literal["yaml", "json"]) -> StrategyModel:
    """Load and parse a strategy definition from text."""
    raw_data = _parse_payload(payload=payload, source_format=source_format)

    try:
        return StrategyModel.model_validate(raw_data)
    except PydanticValidationError as exc:
        raise ValidationError(f"DSL schema validation failed: {exc}") from exc


def _parse_payload(payload: str, source_format: Literal["yaml", "json"]) -> dict[str, Any]:
    if source_format == "json":
        try:
            parsed: Any = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"Invalid JSON DSL: {exc}") from exc
    else:
        try:
            parsed = yaml.safe_load(payload)
        except yaml.YAMLError as exc:
            raise ValidationError(f"Invalid YAML DSL: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValidationError("Strategy payload must be a mapping/object")

    return parsed
