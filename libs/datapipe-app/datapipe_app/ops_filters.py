from __future__ import annotations

import json
import re
from typing import Any, Literal, Sequence

from datapipe_app.errors import OpsSpecValidationError
from datapipe_app.specs import OpsFilterRule

OpsFilterOperator = Literal[
    "contains",
    "not_contains",
    "regex",
    "equal",
    "not_equal",
    "is_empty",
]

OpsFilterMode = Literal["or", "and"]

VALID_OPERATORS: frozenset[str] = frozenset(
    {"contains", "not_contains", "regex", "equal", "not_equal", "is_empty"}
)


def parse_filter_rules(filters: str | None) -> list[OpsFilterRule]:
    if not filters:
        return []
    try:
        payload = json.loads(filters)
    except (TypeError, ValueError) as exc:
        raise OpsSpecValidationError("Invalid filters JSON") from exc
    if not isinstance(payload, list):
        raise OpsSpecValidationError("filters must be a JSON array")
    rules: list[OpsFilterRule] = []
    for item in payload:
        if not isinstance(item, dict):
            raise OpsSpecValidationError("Each filter rule must be an object")
        column_id = item.get("column_id")
        operator = item.get("operator")
        if not column_id or not isinstance(column_id, str):
            raise OpsSpecValidationError('Filter rule requires string "column_id"')
        if operator not in VALID_OPERATORS:
            raise OpsSpecValidationError(f'Unknown filter operator "{operator}"')
        value = item.get("value")
        if value is not None and not isinstance(value, str):
            value = str(value)
        if operator != "is_empty":
            if value is None or not str(value).strip():
                raise OpsSpecValidationError(f'Filter operator "{operator}" requires a non-empty value')
        else:
            value = None
        rules.append(OpsFilterRule(column_id=column_id, operator=operator, value=value))
    return rules


def legacy_filters_to_rules(
  *,
  model: Sequence[str] | str | None = None,
  subset: Sequence[str] | str | None = None,
) -> list[OpsFilterRule]:
    rules: list[OpsFilterRule] = []
    for key, raw in (("model", model), ("subset", subset)):
        if raw is None:
            continue
        values = raw if isinstance(raw, (list, tuple)) else [raw]
        for value in values:
            if value not in {None, ""}:
                rules.append(OpsFilterRule(column_id=key, operator="equal", value=str(value)))
    return rules


def merge_filter_rules(
    filter_rules: Sequence[OpsFilterRule] | None,
    *,
    model: Sequence[str] | str | None = None,
    subset: Sequence[str] | str | None = None,
) -> list[OpsFilterRule]:
    rules = list(filter_rules or [])
    rules.extend(legacy_filters_to_rules(model=model, subset=subset))
    return rules


def compile_regex_pattern(value: str) -> re.Pattern[str]:
    try:
        return re.compile(value)
    except re.error as exc:
        raise OpsSpecValidationError(f"Invalid regex pattern: {exc}") from exc
