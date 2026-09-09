from __future__ import annotations

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
    raise ValueError(f"Unsupported rule operator: {op}")


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
