from __future__ import annotations

import json
import os
import re
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    _src = os.path.join(spark.conf.get("bundle_file_path"), "src")
except Exception:
    _src = os.path.abspath(os.path.join(os.getcwd(), "src"))
if _src and _src not in sys.path:
    sys.path.append(_src)

from cdp_engine.rules import account_properties, leaf_rules, trigger_properties


LAKEBASE_ENDPOINT = spark.conf.get("lakebase_endpoint")
LAKEBASE_DATABASE = spark.conf.get("lakebase_database", "databricks_postgres")
EVENTHUB_BOOTSTRAP = spark.conf.get("eventhub_bootstrap")
EVENTHUB_NAME = spark.conf.get("eventhub_name")
EVENTHUB_SECRET_SCOPE = spark.conf.get("eventhub_secret_scope")
EVENTHUB_SECRET_KEY = spark.conf.get("eventhub_secret_key")
CDP_SCHEMA = spark.conf.get("cdp_schema", "cdp_rt")
EVENT_RUN_ID = spark.conf.get("event_run_id", "")
SOURCE_EVENT_TABLE_NAME = spark.conf.get("source_event_table_name", "")
SOURCE_PROFILE_ID_COL = spark.conf.get("source_profile_id_col", "profile_id")
SOURCE_EVENT_ID_COL = spark.conf.get("source_event_id_col", "event_id")
SOURCE_EVENT_TS_COL = spark.conf.get("source_event_ts_col", "event_ts")
SOURCE_CHANGED_PROPERTIES_COL = spark.conf.get("source_changed_properties_col", "changed_properties")
PROFILE_TABLE_NAME = spark.conf.get("profile_table_name", "profile_attributes_delta")
PROFILE_ID_COL = spark.conf.get("profile_id_col", "profile_id")
ACCOUNT_TABLE_NAME = spark.conf.get("account_table_name", "account_attributes_delta")
ACCOUNT_PROFILE_ID_COL = spark.conf.get("account_profile_id_col", "profile_id")
ACCOUNT_ID_COL = spark.conf.get("account_id_col", "account_id")
SEGMENT_DEFINITIONS_TABLE_NAME = spark.conf.get("segment_definitions_table_name", "segment_definitions_delta")
SDP_EVENT_TABLE_NAME = spark.conf.get("sdp_event_table_name", "tealium_eventhub_events")
SDP_MEMBERSHIP_TABLE_NAME = spark.conf.get("sdp_membership_table_name", "evaluated_realtime_memberships")
SDP_MEMBERSHIP_CURRENT_TABLE_NAME = spark.conf.get("sdp_membership_current_table_name", "membership_flags_current")
SDP_FLOW_NAME = spark.conf.get("sdp_flow_name", "qualify_tealium_events")
APP_ID = "cdp_realtime_segmentation_sdp"
REALTIME_EVENT_PROPERTIES = [
    "beh_page_type",
    "beh_intent",
    "beh_device",
    "beh_referrer_class",
]

EVENT_SCHEMA = """
event_id STRING,
run_id STRING,
profile_id STRING,
event_ts LONG,
page_url STRING,
beh_page_type STRING,
beh_intent STRING,
beh_device STRING,
beh_referrer_class STRING,
changed_properties ARRAY<STRING>
"""

def chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _as_dict(value):
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        return value.as_dict()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Cannot convert {type(value).__name__} to dict")


def _get_lakebase_connection_info() -> dict[str, str]:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    postgres = getattr(w, "postgres", None)
    if postgres is not None:
        endpoint = _as_dict(postgres.get_endpoint(name=LAKEBASE_ENDPOINT))
        credential = postgres.generate_database_credential(endpoint=LAKEBASE_ENDPOINT)
        token = getattr(credential, "token", None) or credential["token"]
    else:
        endpoint = w.api_client.do(method="GET", path=f"/api/2.0/postgres/{LAKEBASE_ENDPOINT}")
        credential = w.api_client.do(
            method="POST",
            path="/api/2.0/postgres/credentials",
            body={"endpoint": LAKEBASE_ENDPOINT},
        )
        token = credential["token"]

    return {
        "host": endpoint["status"]["hosts"]["host"],
        "database": LAKEBASE_DATABASE,
        "user": w.current_user.me().user_name,
        "password": token,
    }


def _connect_lakebase_psycopg2(connection_info: dict[str, str]):
    import psycopg2

    return psycopg2.connect(
        host=connection_info["host"],
        dbname=connection_info["database"],
        user=connection_info["user"],
        password=connection_info["password"],
        sslmode="require",
    )


def lakebase_endpoint_name() -> str:
    return LAKEBASE_ENDPOINT.replace("projects/", "").replace("/branches/", ".").replace("/endpoints/", ".")


def postgresql_options(dbtable: str) -> dict[str, str]:
    return {
        "endpoint": lakebase_endpoint_name(),
        "database": LAKEBASE_DATABASE,
        "dbtable": dbtable,
        "batchsize": "10000",
    }


def eventhub_options() -> dict[str, str]:
    from cdp_engine.eventhub import eventhub_kafka_options

    options = eventhub_kafka_options(
        bootstrap_servers=EVENTHUB_BOOTSTRAP,
        eventhub_name=EVENTHUB_NAME,
        connection_string=dbutils.secrets.get(EVENTHUB_SECRET_SCOPE, EVENTHUB_SECRET_KEY),
        starting_offsets="latest",
    )
    return options


def fetch_reverse_index(cur) -> dict[str, list[str]]:
    cur.execute(f"SELECT property_name, segment_ids FROM {CDP_SCHEMA}.segment_reverse_index")
    return {row["property_name"]: list(row["segment_ids"]) for row in cur.fetchall()}


def fetch_rules(cur, segment_ids: list[str]) -> dict[str, dict]:
    if not segment_ids:
        return {}
    rules = {}
    for ids in chunked(segment_ids, 500):
        cur.execute(
            f"""
            SELECT segment_id, rule_json
            FROM {CDP_SCHEMA}.segment_definitions
            WHERE mode = 'realtime' AND segment_id = ANY(%s)
            """,
            (ids,),
        )
        for row in cur.fetchall():
            val = row["rule_json"]
            rules[row["segment_id"]] = json.loads(val) if isinstance(val, str) else val
    return rules


def fetch_profiles(cur, profile_ids: list[str]) -> dict[str, dict]:
    out = {}
    for ids in chunked(profile_ids, 5000):
        cur.execute(
            f"SELECT profile_id, attrs FROM {CDP_SCHEMA}.profile_attributes WHERE profile_id = ANY(%s)",
            (ids,),
        )
        for row in cur.fetchall():
            attrs = row["attrs"]
            out[row["profile_id"]] = json.loads(attrs) if isinstance(attrs, str) else attrs
    return out


def fetch_memberships(cur, profile_ids: list[str], segment_ids: list[str]) -> dict[tuple[str, str], bool]:
    out = {}
    for pids in chunked(profile_ids, 5000):
        for sids in chunked(segment_ids, 500):
            cur.execute(
                f"""
                SELECT profile_id, segment_id, is_member
                FROM {CDP_SCHEMA}.membership_flags
                WHERE path = 'realtime'
                  AND profile_id = ANY(%s)
                  AND segment_id = ANY(%s)
                """,
                (pids, sids),
            )
            for row in cur.fetchall():
                out[(row["profile_id"], row["segment_id"])] = row["is_member"]
    return out


def upsert_changes(cur, rows: list[tuple]) -> None:
    if not rows:
        return
    cur.executemany(
        f"""
        INSERT INTO {CDP_SCHEMA}.membership_flags
          (profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at)
        VALUES (%s, %s, 'realtime', %s, to_timestamp(%s / 1000.0), %s, now())
        ON CONFLICT (profile_id, segment_id, path) DO UPDATE SET
          is_member = EXCLUDED.is_member,
          qualified_at = EXCLUDED.qualified_at,
          source_event_id = EXCLUDED.source_event_id,
          processed_at = EXCLUDED.processed_at
        """,
        rows,
    )


def upsert_changes_from_stage(cur, stage_id: str) -> None:
    cur.execute(
        f"""
        WITH latest AS (
          SELECT DISTINCT ON (profile_id, segment_id, path)
            profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at
          FROM {CDP_SCHEMA}.membership_flags_stage
          WHERE stage_id = %s
          ORDER BY profile_id, segment_id, path, qualified_at DESC, processed_at DESC
        )
        INSERT INTO {CDP_SCHEMA}.membership_flags
          (profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at)
        SELECT profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at
        FROM latest
        ON CONFLICT (profile_id, segment_id, path) DO UPDATE SET
          is_member = EXCLUDED.is_member,
          qualified_at = EXCLUDED.qualified_at,
          source_event_id = EXCLUDED.source_event_id,
          processed_at = EXCLUDED.processed_at
        WHERE {CDP_SCHEMA}.membership_flags.qualified_at <= EXCLUDED.qualified_at
        """,
        (stage_id,),
    )
    cur.execute(f"DELETE FROM {CDP_SCHEMA}.membership_flags_stage WHERE stage_id = %s", (stage_id,))


def write_metrics(cur, metrics: dict) -> None:
    cur.execute(
        f"""
        INSERT INTO {CDP_SCHEMA}.rt_batch_metrics
          (batch_id, events_in, events_after_reverse_index, active_profiles,
           candidate_segments, segment_evaluations, memberships_changed,
           lakebase_profile_reads, lakebase_membership_reads,
           lakebase_membership_writes, wall_clock_ms, trigger_interval,
           lakebase_profile_read_ms, lakebase_membership_read_ms,
           lakebase_membership_write_ms, event_to_lakebase_write_lag_ms_avg,
           event_to_lakebase_write_lag_ms_max)
        VALUES (%(batch_id)s, %(events_in)s, %(events_after_reverse_index)s,
                %(active_profiles)s, %(candidate_segments)s, %(segment_evaluations)s,
                %(memberships_changed)s, %(lakebase_profile_reads)s,
                %(lakebase_membership_reads)s, %(lakebase_membership_writes)s,
                %(wall_clock_ms)s, 'sdp-continuous',
                %(lakebase_profile_read_ms)s, %(lakebase_membership_read_ms)s,
                %(lakebase_membership_write_ms)s, %(event_to_lakebase_write_lag_ms_avg)s,
                %(event_to_lakebase_write_lag_ms_max)s)
        ON CONFLICT (batch_id) DO UPDATE SET
          events_in = EXCLUDED.events_in,
          events_after_reverse_index = EXCLUDED.events_after_reverse_index,
          active_profiles = EXCLUDED.active_profiles,
          candidate_segments = EXCLUDED.candidate_segments,
          segment_evaluations = EXCLUDED.segment_evaluations,
          memberships_changed = EXCLUDED.memberships_changed,
          lakebase_profile_reads = EXCLUDED.lakebase_profile_reads,
          lakebase_membership_reads = EXCLUDED.lakebase_membership_reads,
          lakebase_membership_writes = EXCLUDED.lakebase_membership_writes,
          wall_clock_ms = EXCLUDED.wall_clock_ms,
          trigger_interval = EXCLUDED.trigger_interval,
          lakebase_profile_read_ms = EXCLUDED.lakebase_profile_read_ms,
          lakebase_membership_read_ms = EXCLUDED.lakebase_membership_read_ms,
          lakebase_membership_write_ms = EXCLUDED.lakebase_membership_write_ms,
          event_to_lakebase_write_lag_ms_avg = EXCLUDED.event_to_lakebase_write_lag_ms_avg,
          event_to_lakebase_write_lag_ms_max = EXCLUDED.event_to_lakebase_write_lag_ms_max
        """,
        metrics,
    )


def _value_for_property(property_col, mapping: dict[str, F.Column]) -> F.Column:
    value = F.lit(None).cast("string")
    for name, column in mapping.items():
        value = F.when(property_col == F.lit(name), column.cast("string")).otherwise(value)
    return value


def _compare(left: F.Column, op: F.Column, right: F.Column) -> F.Column:
    return (
        F.when(op == F.lit("eq"), left == right)
        .when(op == F.lit("neq"), left != right)
        .when(op == F.lit("gt"), left.cast("double") > right.cast("double"))
        .when(op == F.lit("gte"), left.cast("double") >= right.cast("double"))
        .when(op == F.lit("lt"), left.cast("double") < right.cast("double"))
        .when(op == F.lit("lte"), left.cast("double") <= right.cast("double"))
        .otherwise(F.lit(False))
    )


NUMERIC_ACCOUNT_OPS = {"gt", "gte", "lt", "lte", "between"}


def _rule_from_row(row) -> dict:
    value = row["rule_json"]
    return json.loads(value) if isinstance(value, str) else value


def _load_realtime_rules() -> list[dict]:
    rows = (
        spark.read.table(SEGMENT_DEFINITIONS_TABLE_NAME)
        .where(F.col("mode") == F.lit("realtime"))
        .select("segment_id", "segment_name", "rule_json")
        .collect()
    )
    return [
        {
            "segment_id": row["segment_id"],
            "segment_name": row["segment_name"],
            "rule": _rule_from_row(row),
        }
        for row in rows
    ]


def _rule_trigger_properties(rules: list[dict]) -> list[str]:
    props: set[str] = set()
    for segment in rules:
        props.update(trigger_properties(segment["rule"]))
    return sorted(props)


def _segment_trigger_dataframe(rules: list[dict]):
    rows = []
    for segment in rules:
        triggers = sorted(trigger_properties(segment["rule"]))
        if triggers:
            rows.append((segment["segment_id"], triggers))
    return spark.createDataFrame(rows, "segment_id string, trigger_properties array<string>")


def _as_millis(column: F.Column, data_type: T.DataType) -> F.Column:
    if isinstance(data_type, T.TimestampType):
        return (F.unix_timestamp(column) * F.lit(1000)).cast("long")
    if isinstance(data_type, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)):
        numeric_value = column.cast("double")
        return (
            F.when(numeric_value > F.lit(10_000_000_000), numeric_value)
            .otherwise(numeric_value * F.lit(1000.0))
            .cast("long")
        )
    text_value = column.cast("string")
    parsed_ts = F.coalesce(
        F.to_timestamp(text_value),
        F.to_timestamp(text_value, "M/d/yyyy h:mm a"),
        F.to_timestamp(text_value, "M/d/yyyy H:mm"),
    )
    return (F.unix_timestamp(parsed_ts) * F.lit(1000)).cast("long")


def _array_from_changed_properties(column: F.Column, is_array: bool) -> F.Column:
    if is_array:
        return column.cast("array<string>")
    return F.coalesce(
        F.from_json(column.cast("string"), "array<string>"),
        F.array(column.cast("string")),
    )


def _ensure_event_contract(events, trigger_props: list[str]):
    if SOURCE_PROFILE_ID_COL not in events.columns:
        raise ValueError(f"Source event table must contain profile id column {SOURCE_PROFILE_ID_COL!r}")

    out = events.withColumn("profile_id", F.col(f"`{SOURCE_PROFILE_ID_COL}`").cast("string"))
    if SOURCE_EVENT_ID_COL in events.columns:
        out = out.withColumn("event_id", F.col(f"`{SOURCE_EVENT_ID_COL}`").cast("string"))
    else:
        out = out.withColumn(
            "event_id",
            F.sha2(F.concat_ws("|", F.col("profile_id"), F.monotonically_increasing_id().cast("string")), 256),
        )

    if SOURCE_EVENT_TS_COL in events.columns:
        out = out.withColumn("event_ts", _as_millis(F.col(f"`{SOURCE_EVENT_TS_COL}`"), events.schema[SOURCE_EVENT_TS_COL].dataType))
    else:
        out = out.withColumn("event_ts", (F.unix_timestamp(F.current_timestamp()) * F.lit(1000)).cast("long"))

    if SOURCE_CHANGED_PROPERTIES_COL in events.columns:
        field = events.schema[SOURCE_CHANGED_PROPERTIES_COL]
        out = out.withColumn(
            "changed_properties",
            _array_from_changed_properties(F.col(f"`{SOURCE_CHANGED_PROPERTIES_COL}`"), isinstance(field.dataType, T.ArrayType)),
        )
    else:
        out = out.withColumn("changed_properties", F.array(*[F.lit(prop) for prop in (trigger_props or ["__NO_TRIGGER__"])]))

    if "run_id" not in out.columns:
        out = out.withColumn("run_id", F.lit(EVENT_RUN_ID or "source_table"))
    return out


def _read_source_events(trigger_props: list[str]):
    if SOURCE_EVENT_TABLE_NAME:
        return _ensure_event_contract(spark.readStream.table(SOURCE_EVENT_TABLE_NAME), trigger_props)

    return (
        spark.readStream.format("kafka")
        .options(**eventhub_options())
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )


def _column(alias: str, prop: str, columns: set[str]) -> F.Column | None:
    if prop not in columns:
        return None
    return F.col(f"{alias}.`{prop}`")


def _timestamp(column: F.Column) -> F.Column:
    text_value = column.cast("string")
    parsed_ts = F.coalesce(
        F.to_timestamp(text_value),
        F.to_timestamp(text_value, "M/d/yyyy h:mm a"),
        F.to_timestamp(text_value, "M/d/yyyy H:mm"),
    )
    numeric_value = text_value.cast("double")
    numeric_ts = F.to_timestamp(
        F.from_unixtime(
            F.when(numeric_value > F.lit(10_000_000_000), numeric_value / F.lit(1000.0)).otherwise(numeric_value)
        )
    )
    return F.coalesce(numeric_ts, parsed_ts)


def _duration_interval(value: str) -> str:
    match = re.search(r"(\d+(?:\.\d+)?)\s*(day|days|hour|hours|minute|minutes)", str(value).lower())
    if not match:
        raise ValueError(f"Unsupported duration value: {value!r}")
    amount = int(float(match.group(1)))
    unit = match.group(2)
    if unit.startswith("day"):
        return f"INTERVAL {amount} DAY"
    if unit.startswith("hour"):
        return f"INTERVAL {amount} HOUR"
    if unit.startswith("minute"):
        return f"INTERVAL {amount} MINUTE"
    raise ValueError(f"Unsupported duration unit: {unit!r}")


def _contains_expr(left: F.Column, right) -> F.Column:
    return F.coalesce(F.lower(left.cast("string")).contains(str(right).lower()), F.lit(False))


def _array_contains_expr(left: F.Column, right) -> F.Column:
    return F.coalesce(F.exists(left, lambda item: F.lower(item.cast("string")).contains(str(right).lower())), F.lit(False))


def _date_array_expr(left: F.Column, predicate) -> F.Column:
    return F.coalesce(F.exists(left, lambda item: predicate(_timestamp(item))), F.lit(False))


def _resolve_rule_value(leaf: dict, event_cols: set[str], profile_cols: set[str], account_cols: set[str], account_array_props: set[str]) -> tuple[F.Column, bool]:
    prop = leaf["property"]
    is_account = leaf.get("source") == "ACCOUNTS" or prop.startswith("AZ_A_")
    if is_account:
        column = _column("a", prop, account_cols)
        if column is not None:
            return column, prop in account_array_props

    column = _column("e", prop, event_cols)
    if column is not None:
        return column, False

    column = _column("p", prop, profile_cols)
    if column is not None:
        return column, False

    return F.lit(None).cast("string"), False


def _compile_leaf(leaf: dict, event_cols: set[str], profile_cols: set[str], account_cols: set[str], account_array_props: set[str]) -> F.Column:
    op = leaf["op"]
    left, is_array = _resolve_rule_value(leaf, event_cols, profile_cols, account_cols, account_array_props)
    right = leaf.get("value")

    if op == "exists":
        if is_array:
            return F.coalesce(F.size(left) > F.lit(0), F.lit(False))
        return F.coalesce(left.isNotNull() & (left.cast("string") != F.lit("")), F.lit(False))
    if op == "contains":
        return _array_contains_expr(left, right) if is_array else _contains_expr(left, right)
    if op == "not_contains":
        return ~(_array_contains_expr(left, right) if is_array else _contains_expr(left, right))
    if op == "eq":
        return F.coalesce(left.cast("string") == F.lit(str(right)), F.lit(False))
    if op == "neq":
        return F.coalesce(left.cast("string") != F.lit(str(right)), F.lit(False))
    if op == "in":
        values = [str(item) for item in right]
        if is_array:
            return F.coalesce(F.exists(left, lambda item: item.cast("string").isin(values)), F.lit(False))
        return F.coalesce(left.cast("string").isin(values), F.lit(False))
    if op == "gt":
        return F.coalesce(left.cast("double") > F.lit(float(right)), F.lit(False))
    if op == "gte":
        return F.coalesce(left.cast("double") >= F.lit(float(right)), F.lit(False))
    if op == "lt":
        return F.coalesce(left.cast("double") < F.lit(float(right)), F.lit(False))
    if op == "lte":
        return F.coalesce(left.cast("double") <= F.lit(float(right)), F.lit(False))
    if op == "between":
        low, high = right
        return F.coalesce(left.cast("double").between(float(low), float(high)), F.lit(False))
    if op == "after":
        predicate = lambda value: value > F.to_timestamp(F.lit(str(right)), "M/d/yyyy h:mm a")
        return _date_array_expr(left, predicate) if is_array else F.coalesce(predicate(_timestamp(left)), F.lit(False))
    if op == "within_last":
        interval = _duration_interval(str(right))
        predicate = lambda value: (value >= F.current_timestamp() - F.expr(interval)) & (value <= F.current_timestamp())
        return _date_array_expr(left, predicate) if is_array else F.coalesce(predicate(_timestamp(left)), F.lit(False))
    if op == "within_next":
        interval = _duration_interval(str(right))
        predicate = lambda value: (value >= F.current_timestamp()) & (value <= F.current_timestamp() + F.expr(interval))
        return _date_array_expr(left, predicate) if is_array else F.coalesce(predicate(_timestamp(left)), F.lit(False))
    raise ValueError(f"Unsupported rule operator for Spark evaluation: {op}")


def _compile_rule(rule: dict, event_cols: set[str], profile_cols: set[str], account_cols: set[str], account_array_props: set[str]) -> F.Column:
    op = rule["op"]
    if op == "and":
        children = [_compile_rule(child, event_cols, profile_cols, account_cols, account_array_props) for child in rule["rules"]]
        out = F.lit(True)
        for child in children:
            out = out & child
        return out
    if op == "or":
        children = [_compile_rule(child, event_cols, profile_cols, account_cols, account_array_props) for child in rule["rules"]]
        out = F.lit(False)
        for child in children:
            out = out | child
        return out
    if op == "not":
        return ~_compile_rule(rule["rule"], event_cols, profile_cols, account_cols, account_array_props)
    return _compile_leaf(rule, event_cols, profile_cols, account_cols, account_array_props)


def _account_property_modes(rules: list[dict]) -> tuple[set[str], set[str]]:
    numeric_props: set[str] = set()
    array_props: set[str] = set()
    for segment in rules:
        account_props = account_properties(segment["rule"])
        for leaf in leaf_rules(segment["rule"]):
            prop = leaf["property"]
            if prop not in account_props:
                continue
            if leaf["op"] in NUMERIC_ACCOUNT_OPS:
                numeric_props.add(prop)
            else:
                array_props.add(prop)
    return numeric_props, array_props - numeric_props


def _aggregate_accounts(rules: list[dict]):
    accounts = spark.read.table(ACCOUNT_TABLE_NAME)
    account_cols = set(accounts.columns)
    numeric_props, array_props = _account_property_modes(rules)
    aggregations = []
    for prop in sorted(numeric_props):
        if prop in account_cols:
            aggregations.append(F.sum(F.col(f"`{prop}`").cast("double")).alias(prop))
    for prop in sorted(array_props):
        if prop in account_cols:
            aggregations.append(F.collect_set(F.col(f"`{prop}`").cast("string")).alias(prop))
    if not aggregations:
        return None
    if ACCOUNT_PROFILE_ID_COL in account_cols:
        return accounts.groupBy(F.col(f"`{ACCOUNT_PROFILE_ID_COL}`").cast("string").alias("profile_id")).agg(*aggregations)
    if ACCOUNT_ID_COL in account_cols:
        return accounts.groupBy(F.col(f"`{ACCOUNT_ID_COL}`").cast("string").alias("account_id")).agg(*aggregations)
    raise ValueError(
        f"Account table {ACCOUNT_TABLE_NAME!r} must contain {ACCOUNT_PROFILE_ID_COL!r} or {ACCOUNT_ID_COL!r}"
    )


def _compiled_membership_expr(rules: list[dict], event_cols: set[str], profile_cols: set[str], account_cols: set[str], account_array_props: set[str]) -> F.Column:
    out = F.lit(False)
    for segment in rules:
        out = F.when(
            F.col("e.segment_id") == F.lit(segment["segment_id"]),
            _compile_rule(segment["rule"], event_cols, profile_cols, account_cols, account_array_props),
        ).otherwise(out)
    return F.coalesce(out, F.lit(False))


@dp.table(
    name=SDP_EVENT_TABLE_NAME,
    comment="Parsed source-table events for the active SDP sizing run after realtime property filtering.",
    cluster_by=["run_id", "profile_id"],
)
def tealium_eventhub_events():
    trigger_props = _rule_trigger_properties(_load_realtime_rules())
    events = (
        _read_source_events(trigger_props)
        .where("event_id IS NOT NULL AND profile_id IS NOT NULL")
        .where(
            F.arrays_overlap(
                F.col("changed_properties"),
                F.array(*[F.lit(prop) for prop in (trigger_props or ["__NO_TRIGGER__"])]),
            )
        )
    )
    if EVENT_RUN_ID:
        events = events.where(F.col("run_id") == F.lit(EVENT_RUN_ID))
    return events


@dp.table(
    name=SDP_MEMBERSHIP_TABLE_NAME,
    comment="Spark-evaluated candidate realtime segment memberships for the active SDP sizing run.",
    cluster_by=["profile_id", "segment_id"],
)
def evaluated_realtime_memberships():
    rules = _load_realtime_rules()
    segment_triggers = F.broadcast(_segment_trigger_dataframe(rules)).alias("st")
    events = spark.readStream.table(SDP_EVENT_TABLE_NAME).alias("e")
    profiles = (
        spark.read.table(PROFILE_TABLE_NAME)
        .withColumn("profile_id", F.col(f"`{PROFILE_ID_COL}`").cast("string"))
        .alias("p")
    )
    account_agg = _aggregate_accounts(rules)
    account_array_props = _account_property_modes(rules)[1]

    joined = events.join(
        segment_triggers,
        F.arrays_overlap(F.col("e.changed_properties"), F.col("st.trigger_properties")),
    ).select("e.*", F.col("st.segment_id"))

    joined = joined.alias("e").join(profiles, F.col("e.profile_id") == F.col("p.profile_id"))
    if account_agg is not None:
        account_cols = set(account_agg.columns)
        if "profile_id" in account_cols:
            joined = joined.join(account_agg.alias("a"), F.col("e.profile_id") == F.col("a.profile_id"), "left")
        else:
            joined = joined.join(account_agg.alias("a"), F.col("p.account_id").cast("string") == F.col("a.account_id"), "left")
    else:
        account_cols = set()

    event_cols = set(events.columns)
    profile_cols = set(profiles.columns)
    membership_expr = _compiled_membership_expr(rules, event_cols, profile_cols, account_cols, account_array_props)
    return joined.select(
        F.col("e.event_id"),
        F.col("e.run_id"),
        F.col("e.profile_id"),
        F.col("e.segment_id"),
        F.lit("realtime").alias("path"),
        F.col("e.event_ts"),
        F.to_timestamp(F.from_unixtime(F.col("e.event_ts") / F.lit(1000))).alias("qualified_at"),
        F.col("e.event_id").alias("source_event_id"),
        F.current_timestamp().alias("processed_at"),
        membership_expr.alias("is_member"),
    )


dp.create_streaming_table(
    name=SDP_MEMBERSHIP_CURRENT_TABLE_NAME,
    comment="Current realtime segment membership flags maintained by Auto CDC SCD Type 1 for Lakebase sync.",
    table_properties={"delta.enableChangeDataFeed": "true"},
    cluster_by=["profile_id", "segment_id"],
    schema="""
      event_id STRING,
      run_id STRING,
      profile_id STRING,
      segment_id STRING,
      path STRING,
      event_ts LONG,
      qualified_at TIMESTAMP,
      source_event_id STRING,
      processed_at TIMESTAMP,
      is_member BOOLEAN
    """,
)

dp.create_auto_cdc_flow(
    target=SDP_MEMBERSHIP_CURRENT_TABLE_NAME,
    source=SDP_MEMBERSHIP_TABLE_NAME,
    keys=["profile_id", "segment_id", "path"],
    sequence_by=F.struct("event_ts", "event_id"),
    stored_as_scd_type=1,
    name=f"{SDP_FLOW_NAME}_current_state",
)
