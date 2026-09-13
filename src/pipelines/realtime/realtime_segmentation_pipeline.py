from __future__ import annotations

import os
import sys

import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    _src = os.path.join(spark.conf.get("bundle_file_path"), "src")
except Exception:
    _src = os.path.abspath(os.path.join(os.getcwd(), "src"))
if _src and _src not in sys.path:
    sys.path.append(_src)

from cdp_engine.rules import eval_mapped_rule_from_json, event_trigger_properties_from_json


LAKEBASE_ENDPOINT = spark.conf.get("lakebase_endpoint")
LAKEBASE_DATABASE = spark.conf.get("lakebase_database", "databricks_postgres")
EVENTHUB_BOOTSTRAP = spark.conf.get("eventhub_bootstrap")
EVENTHUB_NAME = spark.conf.get("eventhub_name")
EVENTHUB_SECRET_SCOPE = spark.conf.get("eventhub_secret_scope")
EVENTHUB_SECRET_KEY = spark.conf.get("eventhub_secret_key")
CDP_SCHEMA = spark.conf.get("cdp_schema", "cdp_rt")
EVENT_RUN_ID = spark.conf.get("event_run_id", "")
SOURCE_EVENT_TABLE_NAME = spark.conf.get("source_event_table_name", "")
SOURCE_READ_CHANGE_FEED = spark.conf.get("source_read_change_feed", "false").lower() == "true"
SOURCE_CDF_STARTING_VERSION = spark.conf.get("source_cdf_starting_version", "")
SOURCE_PROFILE_ID_COL = spark.conf.get("source_profile_id_col", "profile_id")
SOURCE_EVENT_ID_COL = spark.conf.get("source_event_id_col", "event_id")
SOURCE_EVENT_TS_COL = spark.conf.get("source_event_ts_col", "event_ts")
SOURCE_CHANGED_PROPERTIES_COL = spark.conf.get("source_changed_properties_col", "changed_properties")
PROFILE_TABLE_NAME = spark.conf.get("profile_table_name", "profile_attributes_delta")
PROFILE_ID_COL = spark.conf.get("profile_id_col", "profile_id")
PROFILE_ACCOUNTS_COL = spark.conf.get("profile_accounts_col", "accounts")
ACCOUNT_TABLE_NAME = spark.conf.get("account_table_name", "account_attributes_delta")
ACCOUNT_PROFILE_ID_COL = spark.conf.get("account_profile_id_col", "profile_id")
ACCOUNT_ID_COL = spark.conf.get("account_id_col", "account_group_id")
SEGMENT_DEFINITIONS_TABLE_NAME = spark.conf.get("segment_definitions_table_name", "segment_definitions_delta")
ATTRIBUTE_MAPPING_TABLE_NAME = spark.conf.get("attribute_mapping_table_name", "segment_attribute_mapping")
SDP_EVENT_TABLE_NAME = spark.conf.get("sdp_event_table_name", "tealium_eventhub_events")
SDP_MEMBERSHIP_TABLE_NAME = spark.conf.get("sdp_membership_table_name", "evaluated_realtime_memberships")
SDP_MEMBERSHIP_CURRENT_TABLE_NAME = spark.conf.get("sdp_membership_current_table_name", "membership_flags_current")
ALL_TRIGGER_PROPERTIES_SENTINEL = "__ALL_TRIGGER_PROPERTIES__"
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


def _normalize_table_name(table_name: str) -> str:
    return table_name.replace("`", "").strip().lower()


def _source_role_expr(source_col: F.Column) -> F.Column:
    legacy = F.upper(F.trim(source_col))
    source_table = F.lower(F.regexp_replace(F.trim(source_col), "`", ""))
    return (
        F.when(legacy == F.lit("ACCOUNTS"), F.lit("ACCOUNT"))
        .when(legacy.isin("EVENT", "PROFILE", "ACCOUNT"), legacy)
        .when(source_table == F.lit(_normalize_table_name(SOURCE_EVENT_TABLE_NAME)), F.lit("EVENT"))
        .when(source_table == F.lit(_normalize_table_name(PROFILE_TABLE_NAME)), F.lit("PROFILE"))
        .when(source_table == F.lit(_normalize_table_name(ACCOUNT_TABLE_NAME)), F.lit("ACCOUNT"))
    )


MAPPING_VALUE_SCHEMA = T.StructType(
    [
        T.StructField("source", T.StringType()),
        T.StructField("column_name", T.StringType()),
    ]
)
ATTRIBUTE_MAPPING_SCHEMA = T.MapType(T.StringType(), MAPPING_VALUE_SCHEMA)


def _dict_or_empty(value):
    return value if isinstance(value, dict) else {}


@F.pandas_udf(T.ArrayType(T.StringType()))
def _event_trigger_properties_udf(rule_json: pd.Series, attribute_mapping: pd.Series) -> pd.Series:
    return pd.Series(
        [
            event_trigger_properties_from_json(raw_rule, _dict_or_empty(mapping)) if isinstance(raw_rule, str) and raw_rule else []
            for raw_rule, mapping in zip(rule_json, attribute_mapping)
        ]
    )


@F.pandas_udf(T.BooleanType())
def _evaluate_mapped_rule_udf(
    rule_json: pd.Series,
    event_attrs: pd.Series,
    profile_attrs: pd.Series,
    account_attrs: pd.Series,
    attribute_mapping: pd.Series,
) -> pd.Series:
    return pd.Series(
        [
            bool(
                eval_mapped_rule_from_json(
                    raw_rule,
                    _dict_or_empty(event),
                    _dict_or_empty(profile),
                    _dict_or_empty(account),
                    _dict_or_empty(mapping),
                )
            )
            if isinstance(raw_rule, str) and raw_rule
            else False
            for raw_rule, event, profile, account, mapping in zip(
                rule_json,
                event_attrs,
                profile_attrs,
                account_attrs,
                attribute_mapping,
            )
        ]
    )


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


def _ensure_event_contract(events):
    if SOURCE_PROFILE_ID_COL not in events.columns:
        raise ValueError(f"Source event table must contain profile id column {SOURCE_PROFILE_ID_COL!r}")

    out = events.withColumn("profile_id", F.col(f"`{SOURCE_PROFILE_ID_COL}`").cast("string"))
    if SOURCE_EVENT_ID_COL in events.columns:
        out = out.withColumn("event_id", F.col(f"`{SOURCE_EVENT_ID_COL}`").cast("string"))
    elif "_commit_version" in events.columns:
        out = out.withColumn(
            "event_id",
            F.sha2(F.concat_ws("|", F.col("profile_id"), F.col("_commit_version").cast("string")), 256),
        )
    else:
        out = out.withColumn(
            "event_id",
            F.sha2(F.concat_ws("|", F.col("profile_id"), F.expr("uuid()")), 256),
        )

    if SOURCE_EVENT_TS_COL in events.columns:
        out = out.withColumn("event_ts", _as_millis(F.col(f"`{SOURCE_EVENT_TS_COL}`"), events.schema[SOURCE_EVENT_TS_COL].dataType))
    elif "_commit_timestamp" in events.columns:
        out = out.withColumn("event_ts", (F.unix_timestamp(F.col("_commit_timestamp")) * F.lit(1000)).cast("long"))
    else:
        out = out.withColumn("event_ts", (F.unix_timestamp(F.current_timestamp()) * F.lit(1000)).cast("long"))

    if SOURCE_CHANGED_PROPERTIES_COL in events.columns:
        field = events.schema[SOURCE_CHANGED_PROPERTIES_COL]
        out = out.withColumn(
            "changed_properties",
            _array_from_changed_properties(F.col(f"`{SOURCE_CHANGED_PROPERTIES_COL}`"), isinstance(field.dataType, T.ArrayType)),
        )
    else:
        out = out.withColumn("changed_properties", F.array(F.lit(ALL_TRIGGER_PROPERTIES_SENTINEL)))

    if "run_id" not in out.columns:
        out = out.withColumn("run_id", F.lit(EVENT_RUN_ID or "source_table"))
    return out


def _read_source_events():
    if SOURCE_EVENT_TABLE_NAME:
        reader = spark.readStream
        if SOURCE_READ_CHANGE_FEED:
            reader = reader.option("readChangeFeed", "true")
            if SOURCE_CDF_STARTING_VERSION:
                reader = reader.option("startingVersion", SOURCE_CDF_STARTING_VERSION)
            events = reader.table(SOURCE_EVENT_TABLE_NAME).where(F.col("_change_type").isin("insert", "update_postimage"))
        else:
            events = reader.table(SOURCE_EVENT_TABLE_NAME)
        return _ensure_event_contract(events)

    return (
        spark.readStream.format("kafka")
        .options(**eventhub_options())
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )


def _empty_string_map() -> F.Column:
    return F.map_from_arrays(F.array().cast("array<string>"), F.array().cast("array<string>"))


def _string_map(alias: str, columns: list[str], exclude: set[str] | None = None) -> F.Column:
    exclude = exclude or set()
    selected = [column for column in columns if column not in exclude and not column.startswith("_")]
    if not selected:
        return _empty_string_map()
    return F.map_from_arrays(
        F.array(*[F.lit(column) for column in selected]),
        F.array(*[F.col(f"{alias}.`{column}`").cast("string") for column in selected]),
    )


def _attribute_mapping_snapshot():
    if not ATTRIBUTE_MAPPING_TABLE_NAME:
        schema = T.StructType([T.StructField("attribute_mapping", ATTRIBUTE_MAPPING_SCHEMA)])
        return spark.createDataFrame([({} ,)], schema)
    mapping_rows = (
        spark.read.table(ATTRIBUTE_MAPPING_TABLE_NAME)
        .select(
            F.col("rule_property").cast("string").alias("rule_property"),
            _source_role_expr(F.col("source")).alias("source"),
            F.col("column_name").cast("string").alias("column_name"),
        )
        .where("rule_property IS NOT NULL AND source IS NOT NULL AND column_name IS NOT NULL")
    )
    return mapping_rows.groupBy().agg(
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("rule_property").alias("key"),
                    F.struct(F.col("source"), F.col("column_name")).alias("value"),
                )
            )
        ).alias("attribute_mapping")
    )


def _rules_with_triggers():
    rules = (
        spark.read.table(SEGMENT_DEFINITIONS_TABLE_NAME)
        .where(F.col("mode") == F.lit("realtime"))
        .select(
            F.col("segment_id").cast("string").alias("segment_id"),
            F.col("segment_name").cast("string").alias("segment_name"),
            F.col("rule_json").cast("string").alias("rule_json"),
        )
    )
    mapping = _attribute_mapping_snapshot()
    return (
        rules.crossJoin(mapping)
        .withColumn("trigger_properties", _event_trigger_properties_udf(F.col("rule_json"), F.col("attribute_mapping")))
        .where(F.size(F.col("trigger_properties")) > F.lit(0))
    )


def _profile_account_bridge(profiles):
    profile_cols = set(profiles.columns)
    if PROFILE_ACCOUNTS_COL not in profile_cols:
        return None
    return (
        profiles.select(
            F.col("profile_id"),
            F.explode(F.split(F.col(f"`{PROFILE_ACCOUNTS_COL}`").cast("string"), ";")).alias("_account_id"),
        )
        .select("profile_id", F.trim(F.col("_account_id")).alias("account_id"))
        .where("account_id IS NOT NULL AND account_id <> ''")
    )


def _effective_account_id_col(account_cols: set[str]) -> str | None:
    if ACCOUNT_ID_COL in account_cols:
        return ACCOUNT_ID_COL
    if "account_id" in account_cols:
        return "account_id"
    return None


def _account_attributes_by_profile(profiles):
    accounts = spark.read.table(ACCOUNT_TABLE_NAME)
    account_cols = set(accounts.columns)
    account_id_col = _effective_account_id_col(account_cols)
    value_cols = [
        field
        for field in accounts.schema.fields
        if field.name not in {ACCOUNT_ID_COL, ACCOUNT_PROFILE_ID_COL, "account_id", "profile_id"}
    ]
    aggregations = []
    for field in value_cols:
        if isinstance(field.dataType, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)):
            aggregations.append(F.sum(F.col(f"`{field.name}`").cast("double")).cast("string").alias(field.name))
        else:
            aggregations.append(F.concat_ws("\u001f", F.collect_set(F.col(f"`{field.name}`").cast("string"))).alias(field.name))
    if not aggregations:
        return None
    if ACCOUNT_PROFILE_ID_COL in account_cols:
        account_values = accounts.select(
            F.col(f"`{ACCOUNT_PROFILE_ID_COL}`").cast("string").alias("profile_id"),
            *[F.col(f"`{field.name}`") for field in value_cols],
        )
        return account_values.groupBy("profile_id").agg(*aggregations)
    if account_id_col:
        bridge = _profile_account_bridge(profiles)
        if bridge is not None:
            account_values = accounts.select(
                F.col(f"`{account_id_col}`").cast("string").alias("account_id"),
                *[F.col(f"`{field.name}`") for field in value_cols],
            )
            joined = bridge.alias("b").join(
                account_values.alias("acct"),
                F.col("b.account_id") == F.col("acct.account_id"),
                "inner",
            )
            return joined.groupBy("profile_id").agg(*aggregations)
    if account_id_col:
        account_values = accounts.select(
            F.col(f"`{account_id_col}`").cast("string").alias("account_id"),
            *[F.col(f"`{field.name}`") for field in value_cols],
        )
        return account_values.groupBy("account_id").agg(*aggregations)
    raise ValueError(
        f"Account table {ACCOUNT_TABLE_NAME!r} must contain {ACCOUNT_PROFILE_ID_COL!r} or {ACCOUNT_ID_COL!r}"
    )


@dp.table(
    name=SDP_EVENT_TABLE_NAME,
    comment="Normalized source-table events for the active SDP sizing run.",
    cluster_by=["run_id", "profile_id"],
)
def tealium_eventhub_events():
    events = (
        _read_source_events()
        .where("event_id IS NOT NULL AND profile_id IS NOT NULL")
    )
    if EVENT_RUN_ID:
        events = events.where(F.col("run_id") == F.lit(EVENT_RUN_ID))
    return events


@dp.table(
    name=SDP_MEMBERSHIP_TABLE_NAME,
    comment="Spark-evaluated candidate realtime segment memberships.",
    cluster_by=["profile_id", "segment_id"],
)
def evaluated_realtime_memberships():
    events = spark.readStream.table(SDP_EVENT_TABLE_NAME).alias("e")
    rules = F.broadcast(_rules_with_triggers()).alias("r")
    profiles_base = spark.read.table(PROFILE_TABLE_NAME).withColumn("profile_id", F.col(f"`{PROFILE_ID_COL}`").cast("string"))
    profiles = profiles_base.alias("p")
    account_agg = _account_attributes_by_profile(profiles_base)

    joined = events.join(
        rules,
        F.array_contains(F.col("e.changed_properties"), ALL_TRIGGER_PROPERTIES_SENTINEL)
        | F.arrays_overlap(F.col("e.changed_properties"), F.col("r.trigger_properties")),
    ).select("e.*", F.col("r.segment_id"), F.col("r.rule_json"), F.col("r.attribute_mapping"))

    joined = joined.alias("e").join(profiles, F.col("e.profile_id") == F.col("p.profile_id"))
    if account_agg is not None:
        account_cols = set(account_agg.columns)
        if "profile_id" in account_cols:
            joined = joined.join(account_agg.alias("a"), F.col("e.profile_id") == F.col("a.profile_id"), "left")
        else:
            joined = joined.join(account_agg.alias("a"), F.col("p.account_id").cast("string") == F.col("a.account_id"), "left")
        account_attrs = _string_map("a", account_agg.columns, {"profile_id", "account_id"})
    else:
        account_attrs = _empty_string_map()

    event_attrs = _string_map("e", events.columns)
    profile_attrs = _string_map("p", profiles.columns, {"profile_id"})
    return joined.select(
        F.col("e.run_id"),
        F.col("e.profile_id"),
        F.col("e.segment_id"),
        F.lit("realtime").alias("path"),
        F.col("e.event_id").alias("source_event_id"),
        F.col("e.event_ts").alias("source_event_ts_ms"),
        F.to_timestamp(F.from_unixtime(F.col("e.event_ts") / F.lit(1000))).alias("qualified_at"),
        F.current_timestamp().alias("processed_at"),
        _evaluate_mapped_rule_udf(
            F.col("e.rule_json"),
            event_attrs,
            profile_attrs,
            account_attrs,
            F.col("e.attribute_mapping"),
        ).alias("is_member"),
    )


dp.create_streaming_table(
    name=SDP_MEMBERSHIP_CURRENT_TABLE_NAME,
    comment="Current realtime segment membership flags maintained by Auto CDC SCD Type 1 for Lakebase sync.",
    table_properties={"delta.enableChangeDataFeed": "true"},
    cluster_by=["profile_id", "segment_id"],
    schema="""
      run_id STRING,
      profile_id STRING,
      segment_id STRING,
      path STRING,
      source_event_id STRING,
      source_event_ts_ms LONG,
      qualified_at TIMESTAMP,
      processed_at TIMESTAMP,
      is_member BOOLEAN
    """,
)

dp.create_auto_cdc_flow(
    target=SDP_MEMBERSHIP_CURRENT_TABLE_NAME,
    source=SDP_MEMBERSHIP_TABLE_NAME,
    keys=["profile_id", "segment_id", "path"],
    sequence_by=F.struct("source_event_ts_ms", "source_event_id"),
    stored_as_scd_type=1,
    name="qualify_tealium_events_current_state",
)
