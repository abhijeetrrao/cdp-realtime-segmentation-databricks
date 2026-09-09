from __future__ import annotations

import json
import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F

try:
    _src = os.path.join(spark.conf.get("bundle_file_path"), "src")
except Exception:
    _src = os.path.abspath(os.path.join(os.getcwd(), "src"))
if _src and _src not in sys.path:
    sys.path.append(_src)


LAKEBASE_ENDPOINT = spark.conf.get("lakebase_endpoint")
LAKEBASE_DATABASE = spark.conf.get("lakebase_database", "databricks_postgres")
EVENTHUB_BOOTSTRAP = spark.conf.get("eventhub_bootstrap")
EVENTHUB_NAME = spark.conf.get("eventhub_name")
EVENTHUB_SECRET_SCOPE = spark.conf.get("eventhub_secret_scope")
EVENTHUB_SECRET_KEY = spark.conf.get("eventhub_secret_key")
EVENTHUB_CONNECTION_STRING = dbutils.secrets.get(EVENTHUB_SECRET_SCOPE, EVENTHUB_SECRET_KEY)
CDP_SCHEMA = spark.conf.get("cdp_schema", "cdp_rt")
EVENT_RUN_ID = spark.conf.get("event_run_id", "")
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
        connection_string=EVENTHUB_CONNECTION_STRING,
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


@dp.table(
    name=SDP_EVENT_TABLE_NAME,
    comment="Parsed Event Hub events for the active SDP sizing run after realtime property filtering.",
    cluster_by=["run_id", "profile_id"],
)
def tealium_eventhub_events():
    events = (
        spark.readStream.format("kafka")
        .options(**eventhub_options())
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .where("event_id IS NOT NULL AND profile_id IS NOT NULL")
        .where(
            F.arrays_overlap(
                F.col("changed_properties"),
                F.array(*[F.lit(prop) for prop in REALTIME_EVENT_PROPERTIES]),
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
    events = spark.readStream.table(SDP_EVENT_TABLE_NAME).alias("e")
    profiles = spark.read.table("profile_attributes_delta").alias("p")
    segments = (
        spark.read.table("segment_definitions_delta")
        .where(F.col("mode") == F.lit("realtime"))
        .select(
            "segment_id",
            F.get_json_object("rule_json", "$.rules[0].property").alias("event_property"),
            F.get_json_object("rule_json", "$.rules[0].op").alias("event_op"),
            F.get_json_object("rule_json", "$.rules[0].value").alias("event_value"),
            F.get_json_object("rule_json", "$.rules[1].property").alias("attr_property"),
            F.get_json_object("rule_json", "$.rules[1].op").alias("attr_op"),
            F.get_json_object("rule_json", "$.rules[1].value").alias("attr_value"),
        )
        .where(F.col("event_property").isin(REALTIME_EVENT_PROPERTIES))
        .alias("s")
    )

    joined = (
        events.join(F.broadcast(segments), F.array_contains(F.col("e.changed_properties"), F.col("s.event_property")))
        .join(profiles, F.col("e.profile_id") == F.col("p.profile_id"))
    )
    event_value = _value_for_property(
        F.col("s.event_property"),
        {
            "beh_page_type": F.col("e.beh_page_type"),
            "beh_intent": F.col("e.beh_intent"),
            "beh_device": F.col("e.beh_device"),
            "beh_referrer_class": F.col("e.beh_referrer_class"),
        },
    )
    attr_value = _value_for_property(
        F.col("s.attr_property"),
        {
            "acct_segment_type": F.col("p.acct_segment_type"),
            "acct_region": F.col("p.acct_region"),
            "acct_revenue_band": F.col("p.acct_revenue_band"),
            "prof_visits_30d": F.col("p.prof_visits_30d"),
            "prof_pageviews_30d": F.col("p.prof_pageviews_30d"),
            "prof_logged_in": F.col("p.prof_logged_in"),
            "prof_preferred_channel": F.col("p.prof_preferred_channel"),
            "prof_last_intent": F.col("p.prof_last_intent"),
            "prof_engagement_score": F.col("p.prof_engagement_score"),
        },
    )
    return joined.select(
        F.col("e.event_id"),
        F.col("e.run_id"),
        F.col("e.profile_id"),
        F.col("s.segment_id"),
        F.lit("realtime").alias("path"),
        F.col("e.event_ts"),
        F.to_timestamp(F.from_unixtime(F.col("e.event_ts") / F.lit(1000))).alias("qualified_at"),
        F.col("e.event_id").alias("source_event_id"),
        F.current_timestamp().alias("processed_at"),
        (_compare(event_value, F.col("s.event_op"), F.col("s.event_value")) & _compare(attr_value, F.col("s.attr_op"), F.col("s.attr_value"))).alias("is_member"),
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
