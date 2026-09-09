# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import time
from typing import Any

c = cfg()
schema = "cdp_rt"


def quote_identifier(name: str) -> str:
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {name}")
    return f"`{name}`"


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise ValueError(f"Unsupported SQL literal: {value!r}")


def rule_to_sql(rule: dict[str, Any]) -> str:
    op = rule["op"]
    if op == "and":
        return "(" + " AND ".join(rule_to_sql(child) for child in rule["rules"]) + ")"
    if op == "or":
        return "(" + " OR ".join(rule_to_sql(child) for child in rule["rules"]) + ")"
    if op == "not":
        return f"(NOT {rule_to_sql(rule['rule'])})"

    col = quote_identifier(rule["property"])
    if op == "in":
        values = ", ".join(sql_literal(v) for v in rule["value"])
        return f"{col} IN ({values})"

    value = sql_literal(rule.get("value"))
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

segments = [r.asDict() for r in spark.table(fq(c, "segment_definitions_delta")).where("mode = 'batch'").collect()]
profile_table = fq(c, "profile_attributes_delta")

out = []
for s in segments:
    predicate = rule_to_sql(json.loads(s["rule_json"]))
    qualified = spark.sql(f"SELECT profile_id, '{s['segment_id']}' AS segment_id, true AS is_member FROM {profile_table} WHERE {predicate}")
    out.append(qualified)

result = out[0]
for df in out[1:]:
    result = result.unionByName(df)

qualified_table = fq(c, "batch_memberships_delta")
(
    result.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(qualified_table)
)

start = time.time()
conn = get_lakebase_connection(c["lakebase_endpoint"], c["lakebase_database"])
try:
    sql = f"""
    INSERT INTO {schema}.membership_flags
      (profile_id, segment_id, path, is_member, qualified_at, source_event_id)
    VALUES (%s, %s, 'batch', %s, now(), NULL)
    ON CONFLICT (profile_id, segment_id, path) DO UPDATE SET
      is_member = EXCLUDED.is_member,
      qualified_at = EXCLUDED.qualified_at
    """
    count = 0
    buffer = []
    with conn.cursor() as cur:
        for r in spark.table(qualified_table).toLocalIterator():
            buffer.append((r.profile_id, r.segment_id, r.is_member))
            count += 1
            if len(buffer) >= 5000:
                cur.executemany(sql, buffer)
                conn.commit()
                buffer.clear()
        if buffer:
            cur.executemany(sql, buffer)
        conn.commit()
finally:
    conn.close()

print(json.dumps({"batch_segments": len(segments), "qualified_rows_upserted": count, "wall_clock_ms": int((time.time() - start) * 1000)}, indent=2))
