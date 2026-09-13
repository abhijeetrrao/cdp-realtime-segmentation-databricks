from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Any


def referenced_properties(rule: dict[str, Any]) -> set[str]:
    op = rule["op"]
    if op in {"and", "or"}:
        out: set[str] = set()
        for child in rule["rules"]:
            out.update(referenced_properties(child))
        return out
    if op == "not":
        return referenced_properties(rule["rule"])
    return {rule["property"]}


def leaf_rules(rule: dict[str, Any]) -> list[dict[str, Any]]:
    op = rule["op"]
    if op in {"and", "or"}:
        out: list[dict[str, Any]] = []
        for child in rule["rules"]:
            out.extend(leaf_rules(child))
        return out
    if op == "not":
        return leaf_rules(rule["rule"])
    return [rule]


def account_properties(rule: dict[str, Any]) -> set[str]:
    return {
        leaf["property"]
        for leaf in leaf_rules(rule)
        if leaf.get("source") == "ACCOUNTS" or leaf.get("property", "").startswith("AZ_A_")
    }


def trigger_properties(rule: dict[str, Any]) -> set[str]:
    return {
        leaf["property"]
        for leaf in leaf_rules(rule)
        if leaf.get("source") != "ACCOUNTS" and not leaf.get("property", "").startswith("AZ_A_")
    }


def operators_by_property(rule: dict[str, Any]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for leaf in leaf_rules(rule):
        out.setdefault(leaf["property"], set()).add(leaf["op"])
    return out


def eval_rule(rule: dict[str, Any], attrs: dict[str, Any], event: dict[str, Any] | None = None) -> bool:
    event = event or {}
    op = rule["op"]
    if op == "and":
        return all(eval_rule(child, attrs, event) for child in rule["rules"])
    if op == "or":
        return any(eval_rule(child, attrs, event) for child in rule["rules"])
    if op == "not":
        return not eval_rule(rule["rule"], attrs, event)

    prop = rule["property"]
    left = event.get(prop, attrs.get(prop))
    right = rule.get("value")

    if op == "eq":
        return left == right
    if op == "neq":
        return left != right
    if op == "gt":
        return left is not None and left > right
    if op == "gte":
        return left is not None and left >= right
    if op == "lt":
        return left is not None and left < right
    if op == "lte":
        return left is not None and left <= right
    if op == "in":
        return left in right
    if op == "exists":
        return _exists(left)
    if op == "contains":
        return _contains(left, right)
    if op == "not_contains":
        return not _contains(left, right)
    if op == "between":
        low, high = right
        try:
            left_num = float(left)
        except (TypeError, ValueError):
            return False
        return float(low) <= left_num <= float(high)
    if op == "after":
        left_dt = _parse_datetime(left)
        right_dt = _parse_datetime(right)
        return bool(left_dt and right_dt and left_dt > right_dt)
    if op == "within_last":
        left_dt = _parse_datetime(left)
        delta = _parse_duration(right)
        return bool(left_dt and delta and datetime.now(timezone.utc) - delta <= left_dt <= datetime.now(timezone.utc))
    if op == "within_next":
        left_dt = _parse_datetime(left)
        delta = _parse_duration(right)
        return bool(left_dt and delta and datetime.now(timezone.utc) <= left_dt <= datetime.now(timezone.utc) + delta)
    raise ValueError(f"Unsupported rule operator: {op}")


@lru_cache(maxsize=4096)
def parse_rule_json(rule_json: str) -> dict[str, Any]:
    return json.loads(rule_json)


def event_trigger_properties_from_json(rule_json: str, attribute_mapping: dict[str, Any] | None = None) -> list[str]:
    return event_trigger_properties(parse_rule_json(rule_json), attribute_mapping)


def eval_mapped_rule_from_json(
    rule_json: str,
    event_attrs: dict[str, Any] | None,
    profile_attrs: dict[str, Any] | None,
    account_attrs: dict[str, Any] | None,
    attribute_mapping: dict[str, Any] | None = None,
) -> bool:
    return eval_mapped_rule(parse_rule_json(rule_json), event_attrs, profile_attrs, account_attrs, attribute_mapping)


def event_trigger_properties(rule: dict[str, Any], attribute_mapping: dict[str, Any] | None = None) -> list[str]:
    attribute_mapping = attribute_mapping or {}
    props: set[str] = set()
    for leaf in leaf_rules(rule):
        rule_property = str(leaf.get("property", ""))
        if _leaf_source(leaf, attribute_mapping) == "EVENT":
            props.add(rule_property)
            column_name = _leaf_column_name(rule_property, attribute_mapping)
            if column_name:
                props.add(column_name)
    return sorted(props)


def eval_mapped_rule(
    rule: dict[str, Any],
    event_attrs: dict[str, Any] | None,
    profile_attrs: dict[str, Any] | None,
    account_attrs: dict[str, Any] | None,
    attribute_mapping: dict[str, Any] | None = None,
) -> bool:
    attribute_mapping = attribute_mapping or {}
    event_attrs = event_attrs or {}
    profile_attrs = profile_attrs or {}
    account_attrs = account_attrs or {}

    def value_for_leaf(leaf: dict[str, Any]) -> Any:
        rule_property = str(leaf["property"])
        source = _leaf_source(leaf, attribute_mapping)
        column_name = _leaf_column_name(rule_property, attribute_mapping)
        if source == "EVENT":
            return event_attrs.get(column_name, event_attrs.get(rule_property))
        if source == "ACCOUNT":
            return account_attrs.get(column_name, account_attrs.get(rule_property))
        return profile_attrs.get(column_name, profile_attrs.get(rule_property))

    def evaluate(node: dict[str, Any]) -> bool:
        op = node["op"]
        if op == "and":
            return all(evaluate(child) for child in node["rules"])
        if op == "or":
            return any(evaluate(child) for child in node["rules"])
        if op == "not":
            return not evaluate(node["rule"])

        left = value_for_leaf(node)
        right = node.get("value")

        if op == "eq":
            return _string_or_native(left) == _string_or_native(right)
        if op == "neq":
            return _string_or_native(left) != _string_or_native(right)
        if op == "gt":
            return _compare_numeric(left, right, lambda l, r: l > r)
        if op == "gte":
            return _compare_numeric(left, right, lambda l, r: l >= r)
        if op == "lt":
            return _compare_numeric(left, right, lambda l, r: l < r)
        if op == "lte":
            return _compare_numeric(left, right, lambda l, r: l <= r)
        if op == "in":
            return str(left) in {str(item) for item in right or []}
        if op == "exists":
            return _exists(left)
        if op == "contains":
            return _contains(left, right)
        if op == "not_contains":
            return not _contains(left, right)
        if op == "between":
            low, high = right
            return _compare_numeric(left, low, lambda l, r: l >= r) and _compare_numeric(left, high, lambda l, r: l <= r)
        if op == "after":
            left_dt = _parse_datetime(left)
            right_dt = _parse_datetime(right)
            return bool(left_dt and right_dt and left_dt > right_dt)
        if op == "within_last":
            left_dt = _parse_datetime(left)
            delta = _parse_duration(right)
            now = datetime.now(timezone.utc)
            return bool(left_dt and delta and now - delta <= left_dt <= now)
        if op == "within_next":
            left_dt = _parse_datetime(left)
            delta = _parse_duration(right)
            now = datetime.now(timezone.utc)
            return bool(left_dt and delta and now <= left_dt <= now + delta)
        raise ValueError(f"Unsupported rule operator: {op}")

    return evaluate(rule)


def rule_to_sql(rule: dict[str, Any]) -> str:
    op = rule["op"]
    if op == "and":
        return "(" + " AND ".join(rule_to_sql(child) for child in rule["rules"]) + ")"
    if op == "or":
        return "(" + " OR ".join(rule_to_sql(child) for child in rule["rules"]) + ")"
    if op == "not":
        return f"(NOT {rule_to_sql(rule['rule'])})"

    col = _quote_identifier(rule["property"])
    if op == "in":
        values = ", ".join(_sql_literal(v) for v in rule["value"])
        return f"{col} IN ({values})"

    value = _sql_literal(rule.get("value"))
    if op == "eq":
        return f"{col} = {value}"
    if op == "neq":
        return f"{col} <> {value}"
    if op == "gt":
        return f"{col} > {value}"
    if op == "gte":
        return f"{col} >= {value}"
    if op == "lt":
        return f"{col} < {value}"
    if op == "lte":
        return f"{col} <= {value}"
    if op == "exists":
        return f"{col} IS NOT NULL"
    if op == "contains":
        return f"LOWER(CAST({col} AS STRING)) LIKE LOWER('%' || {value} || '%')"
    if op == "not_contains":
        return f"NOT (LOWER(CAST({col} AS STRING)) LIKE LOWER('%' || {value} || '%'))"
    if op == "between":
        low, high = rule["value"]
        return f"CAST({col} AS DOUBLE) BETWEEN {float(low)} AND {float(high)}"
    raise ValueError(f"Unsupported rule operator: {op}")


def _quote_identifier(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {name}")
    return f"`{name}`"


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"Unsupported SQL literal: {value!r}")


def _exists(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def _contains(left: Any, right: Any) -> bool:
    if left is None:
        return False
    needle = str(right).lower()
    if isinstance(left, (list, tuple, set)):
        return any(needle in str(item).lower() for item in left if item is not None)
    return needle in str(left).lower()


def _string_or_native(value: Any) -> Any:
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def _compare_numeric(left: Any, right: Any, predicate) -> bool:
    try:
        return predicate(float(left), float(right))
    except (TypeError, ValueError):
        return False


def _mapping_value(mapping: dict[str, Any], rule_property: str, field: str) -> str | None:
    value = mapping.get(rule_property)
    if value is None:
        return None
    if isinstance(value, dict):
        return value.get(field)
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True).get(field)
    return getattr(value, field, None)


def _normalize_source(source: Any) -> str:
    normalized = str(source or "").strip().upper()
    return "ACCOUNT" if normalized == "ACCOUNTS" else normalized


def _leaf_source(leaf: dict[str, Any], attribute_mapping: dict[str, Any]) -> str:
    if _normalize_source(leaf.get("source")) == "ACCOUNT":
        return "ACCOUNT"
    mapped_source = _mapping_value(attribute_mapping, str(leaf["property"]), "source")
    if mapped_source:
        return _normalize_source(mapped_source)
    rule_property = str(leaf["property"])
    if rule_property.startswith("AZ_A_"):
        return "ACCOUNT"
    if rule_property.startswith("DL_") or rule_property.startswith("beh_"):
        return "EVENT"
    return "PROFILE"


def _leaf_column_name(rule_property: str, attribute_mapping: dict[str, Any]) -> str:
    return _mapping_value(attribute_mapping, rule_property, "column_name") or rule_property


def _parse_duration(value: Any) -> timedelta | None:
    if value is None:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|hour|hours|minute|minutes)", str(value).lower())
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2)
    if unit.startswith("day"):
        return timedelta(days=amount)
    if unit.startswith("hour"):
        return timedelta(hours=amount)
    if unit.startswith("minute"):
        return timedelta(minutes=amount)
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %I:%M %p"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
