# Databricks notebook source
# MAGIC %pip install -q psycopg2-binary

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from cdp_engine.lakebase import chunked, connect_lakebase, resolve_lakebase_connection_info
from cdp_engine.rules import eval_rule

c = cfg()
schema = "cdp_rt"
endpoint = c["lakebase_endpoint"]
database = c["lakebase_database"]
trigger_interval = c["trigger_interval"]
starting_offsets = widget("starting_offsets", "latest")
starting_timestamp_ms = widget("starting_timestamp_ms", "")
stream_trigger_mode = widget("stream_trigger_mode", "availableNow")
max_offsets_per_trigger = widget("max_offsets_per_trigger", "5000")
membership_write_mode = widget("membership_write_mode", "direct_psycopg")
lakebase_write_partitions = int(widget("lakebase_write_partitions", "1"))
stream_run_id = widget("sizing_run_id", f"stream_{int(time.time())}")
event_run_id_filter = widget("event_run_id", "")

event_schema = """
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

reverse_index_rows = spark.table(fq(c, "segment_reverse_index_delta")).collect()
reverse_index = {r.property_name: list(r.segment_ids) for r in reverse_index_rows}
realtime_event_properties = ["beh_page_type", "beh_intent", "beh_device", "beh_referrer_class"]
lakebase_connection_info = resolve_lakebase_connection_info(endpoint, database)

raw = (
    spark.readStream.format("kafka")
    .options(**kafka_eventhub_options(c))
    .option("startingOffsets", starting_offsets)
    .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
    .option("minPartitions", "8")
)
if starting_timestamp_ms:
    raw = raw.option("startingTimestamp", starting_timestamp_ms)
raw = raw.load()

events = (
    raw.select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select("e.*")
    .where("event_id IS NOT NULL AND profile_id IS NOT NULL")
    .where(
        F.arrays_overlap(
            F.col("changed_properties"),
            F.array(*[F.lit(prop) for prop in realtime_event_properties]),
        )
    )
)

if event_run_id_filter:
    events = events.where(F.col("run_id") == F.lit(event_run_id_filter))

# COMMAND ----------

def fetch_profiles(cur, profile_ids: list[str]) -> dict[str, dict]:
    out = {}
    for ids in chunked(profile_ids, 5000):
        cur.execute(
            f"SELECT profile_id, attrs FROM {schema}.profile_attributes WHERE profile_id = ANY(%s)",
            (ids,),
        )
        for row in cur.fetchall():
            attrs = row["attrs"]
            if isinstance(attrs, str):
                attrs = json.loads(attrs)
            out[row["profile_id"]] = attrs
    return out


def fetch_memberships(cur, profile_ids: list[str], segment_ids: list[str]) -> dict[tuple[str, str], bool]:
    out = {}
    if not profile_ids or not segment_ids:
        return out
    for pids in chunked(profile_ids, 5000):
        for sids in chunked(segment_ids, 500):
            cur.execute(
                f"""
                SELECT profile_id, segment_id, is_member
                FROM {schema}.membership_flags
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
        INSERT INTO {schema}.membership_flags
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


def lakebase_endpoint_name() -> str:
    return endpoint.replace("projects/", "").replace("/branches/", ".").replace("/endpoints/", ".")


def postgresql_options(dbtable: str, *, upsertkey: str | None = None) -> dict[str, str]:
    out = {
        "endpoint": lakebase_endpoint_name(),
        "database": database,
        "dbtable": dbtable,
        "batchsize": "10000",
    }
    if upsertkey:
        out["upsertkey"] = upsertkey
    return out


def jdbc_options() -> dict[str, str]:
    return {
        "url": f"jdbc:postgresql://{lakebase_connection_info['host']}:5432/{lakebase_connection_info['database']}?sslmode=require",
        "user": lakebase_connection_info["user"],
        "password": lakebase_connection_info["password"],
        "driver": "org.postgresql.Driver",
        "dbtable": f"{schema}.membership_flags_stage",
        "batchsize": "10000",
    }


membership_stage_schema = StructType(
    [
        StructField("stage_id", StringType(), False),
        StructField("batch_id", LongType(), False),
        StructField("profile_id", StringType(), False),
        StructField("segment_id", StringType(), False),
        StructField("path", StringType(), False),
        StructField("is_member", BooleanType(), False),
        StructField("qualified_at", TimestampType(), False),
        StructField("source_event_id", StringType(), True),
        StructField("processed_at", TimestampType(), False),
    ]
)


def upsert_changes_from_stage(cur, stage_id: str) -> None:
    cur.execute(
        f"""
        WITH latest AS (
          SELECT DISTINCT ON (profile_id, segment_id, path)
            profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at
          FROM {schema}.membership_flags_stage
          WHERE stage_id = %s
          ORDER BY profile_id, segment_id, path, processed_at DESC
        )
        INSERT INTO {schema}.membership_flags
          (profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at)
        SELECT profile_id, segment_id, path, is_member, qualified_at, source_event_id, processed_at
        FROM latest
        ON CONFLICT (profile_id, segment_id, path) DO UPDATE SET
          is_member = EXCLUDED.is_member,
          qualified_at = EXCLUDED.qualified_at,
          source_event_id = EXCLUDED.source_event_id,
          processed_at = EXCLUDED.processed_at
        """,
        (stage_id,),
    )
    cur.execute(f"DELETE FROM {schema}.membership_flags_stage WHERE stage_id = %s", (stage_id,))


def write_changes(cur, rows: list[tuple], batch_id: int) -> None:
    if not rows:
        return
    if membership_write_mode == "direct_psycopg":
        upsert_changes(cur, rows)
        return

    stage_id = f"{stream_run_id}_{batch_id}"
    processed_at = datetime.now(timezone.utc)
    flag_rows = [
        (
            stage_id,
            int(batch_id),
            profile_id,
            segment_id,
            "realtime",
            is_member,
            datetime.fromtimestamp(int(event_ts) / 1000.0, tz=timezone.utc),
            source_event_id,
            processed_at,
        )
        for profile_id, segment_id, is_member, event_ts, source_event_id in rows
    ]
    stage_df = spark.createDataFrame(flag_rows, schema=membership_stage_schema)
    writer_df = stage_df.coalesce(max(lakebase_write_partitions, 1))
    if membership_write_mode == "postgresql_upsert":
        (
            writer_df.select(
                "profile_id",
                "segment_id",
                "path",
                "is_member",
                "qualified_at",
                "source_event_id",
                "processed_at",
            )
            .write.format("postgresql")
            .options(**postgresql_options(f"{schema}.membership_flags", upsertkey="profile_id,segment_id,path"))
            .mode("append")
            .save()
        )
        return

    writer_df.write.format("postgresql").options(**postgresql_options(f"{schema}.membership_flags_stage")).mode("append").save()
    upsert_changes_from_stage(cur, stage_id)


def write_metrics(cur, metrics: dict) -> None:
    cur.execute(
        f"""
        INSERT INTO {schema}.rt_batch_metrics
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
                %(wall_clock_ms)s, %(trigger_interval)s,
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


def qualify_batch(batch_df, batch_id: int) -> None:
    start = time.time()
    raw_rows = [r.asDict(recursive=True) for r in batch_df.collect()]
    events_in = len(raw_rows)
    if events_in == 0:
        return

    conn = connect_lakebase(lakebase_connection_info)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            rows = []
            for event in raw_rows:
                candidate_ids = set()
                matched = []
                for prop in event.get("changed_properties") or []:
                    ids = reverse_index.get(prop)
                    if ids:
                        matched.append(prop)
                        candidate_ids.update(ids)
                if candidate_ids:
                    event["candidate_segment_ids"] = sorted(candidate_ids)
                    event["matched_properties"] = matched
                    rows.append(event)

            events_after = len(rows)

            if events_in == 0 or events_after == 0:
                write_metrics(cur, {
                    "batch_id": batch_id,
                    "events_in": events_in,
                    "events_after_reverse_index": events_after,
                    "active_profiles": 0,
                    "candidate_segments": 0,
                    "segment_evaluations": 0,
                    "memberships_changed": 0,
                    "lakebase_profile_reads": 0,
                    "lakebase_membership_reads": 0,
                    "lakebase_membership_writes": 0,
                    "lakebase_profile_read_ms": 0,
                    "lakebase_membership_read_ms": 0,
                    "lakebase_membership_write_ms": 0,
                    "event_to_lakebase_write_lag_ms_avg": 0,
                    "event_to_lakebase_write_lag_ms_max": 0,
                    "wall_clock_ms": int((time.time() - start) * 1000),
                    "trigger_interval": trigger_interval,
                })
                conn.commit()
                return

            profile_ids = sorted({r["profile_id"] for r in rows})
            segment_ids = sorted({sid for r in rows for sid in r["candidate_segment_ids"]})

            cur.execute(
                f"""
                SELECT segment_id, rule_json
                FROM {schema}.segment_definitions
                WHERE mode = 'realtime' AND segment_id = ANY(%s)
                """,
                (segment_ids,),
            )
            rules = {}
            for row in cur.fetchall():
                val = row["rule_json"]
                rules[row["segment_id"]] = json.loads(val) if isinstance(val, str) else val

            profile_read_start = time.time()
            profiles = fetch_profiles(cur, profile_ids)
            profile_read_ms = int((time.time() - profile_read_start) * 1000)
            membership_read_start = time.time()
            current = fetch_memberships(cur, profile_ids, segment_ids)
            membership_read_ms = int((time.time() - membership_read_start) * 1000)

            changes = []
            evaluations = 0
            seen_pair_event = set()
            for event in rows:
                attrs = profiles.get(event["profile_id"])
                if not attrs:
                    continue
                event_props = {k: event.get(k) for k in ["beh_page_type", "beh_intent", "beh_device", "beh_referrer_class"]}
                for sid in event["candidate_segment_ids"]:
                    key = (event["profile_id"], sid)
                    pair_event = (event["event_id"], event["profile_id"], sid)
                    if pair_event in seen_pair_event or sid not in rules:
                        continue
                    seen_pair_event.add(pair_event)
                    evaluations += 1
                    is_member = eval_rule(rules[sid], attrs, event_props)
                    if current.get(key) is not is_member:
                        changes.append((event["profile_id"], sid, is_member, event["event_ts"], event["event_id"]))

            write_start = time.time()
            write_changes(cur, changes, batch_id)
            write_finished_ms = int(time.time() * 1000)
            write_ms = int((time.time() - write_start) * 1000)
            changed_event_ts = [int(r[3]) for r in changes if r[3] is not None]
            write_lags = [max(0, write_finished_ms - ts) for ts in changed_event_ts]
            write_metrics(cur, {
                "batch_id": batch_id,
                "events_in": events_in,
                "events_after_reverse_index": events_after,
                "active_profiles": len(profile_ids),
                "candidate_segments": len(segment_ids),
                "segment_evaluations": evaluations,
                "memberships_changed": len(changes),
                "lakebase_profile_reads": len(profile_ids),
                "lakebase_membership_reads": len(profile_ids) * len(segment_ids),
                "lakebase_membership_writes": len(changes),
                "lakebase_profile_read_ms": profile_read_ms,
                "lakebase_membership_read_ms": membership_read_ms,
                "lakebase_membership_write_ms": write_ms,
                "event_to_lakebase_write_lag_ms_avg": int(sum(write_lags) / max(len(write_lags), 1)),
                "event_to_lakebase_write_lag_ms_max": max(write_lags) if write_lags else 0,
                "wall_clock_ms": int((time.time() - start) * 1000),
                "trigger_interval": trigger_interval,
            })
        conn.commit()
    finally:
        conn.close()


checkpoint_suffix = widget("checkpoint_suffix", "realtime_segmentation")
writer = (
    events.writeStream
    .foreachBatch(qualify_batch)
    .outputMode("update")
    .option("checkpointLocation", volume_path(c, f"checkpoints/{checkpoint_suffix}"))
    .queryName("cdp_rt_realtime_segmentation")
)

if stream_trigger_mode == "processingTime":
    query = writer.trigger(processingTime=trigger_interval).start()
    deadline = time.time() + int(widget("stream_duration_seconds", "600"))
    while query.isActive and time.time() < deadline:
        print(json.dumps({"stream_status": query.status, "recent_progress": query.recentProgress[-1:]}, default=str))
        query.awaitTermination(10)
    query.stop()
else:
    query = writer.trigger(availableNow=True).start()
    while query.isActive:
        print(json.dumps({"stream_status": query.status, "recent_progress": query.recentProgress[-1:]}, default=str))
        query.awaitTermination(10)
