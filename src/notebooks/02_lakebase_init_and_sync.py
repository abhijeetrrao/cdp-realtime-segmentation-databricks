# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json

from psycopg2.extras import Json, RealDictCursor

c = cfg()
endpoint = c["lakebase_endpoint"]
database = c["lakebase_database"]

conn = get_lakebase_connection(endpoint, database)
conn.autocommit = False

schema = "cdp_rt"

ddl = f"""
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.profile_attributes (
  profile_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  attrs JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS profile_attributes_account_idx ON {schema}.profile_attributes(account_id);
CREATE INDEX IF NOT EXISTS profile_attributes_attrs_gin ON {schema}.profile_attributes USING GIN(attrs);

CREATE TABLE IF NOT EXISTS {schema}.segment_definitions (
  segment_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  segment_name TEXT NOT NULL,
  rule_json JSONB NOT NULL,
  referenced_properties TEXT[] NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS segment_definitions_mode_idx ON {schema}.segment_definitions(mode);

CREATE TABLE IF NOT EXISTS {schema}.segment_reverse_index (
  property_name TEXT PRIMARY KEY,
  segment_ids TEXT[] NOT NULL,
  fanout INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {schema}.membership_flags (
  profile_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  path TEXT NOT NULL,
  is_member BOOLEAN NOT NULL,
  qualified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_event_id TEXT,
  PRIMARY KEY(profile_id, segment_id, path)
);
CREATE INDEX IF NOT EXISTS membership_flags_profile_idx ON {schema}.membership_flags(profile_id, is_member);
CREATE INDEX IF NOT EXISTS membership_flags_segment_idx ON {schema}.membership_flags(segment_id, is_member);

CREATE TABLE IF NOT EXISTS {schema}.membership_flags_stage (
  stage_id TEXT NOT NULL,
  batch_id BIGINT NOT NULL,
  profile_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  path TEXT NOT NULL,
  is_member BOOLEAN NOT NULL,
  qualified_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT,
  processed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS membership_flags_stage_stage_idx ON {schema}.membership_flags_stage(stage_id);
CREATE INDEX IF NOT EXISTS membership_flags_stage_profile_idx ON {schema}.membership_flags_stage(profile_id, segment_id, path);

CREATE TABLE IF NOT EXISTS {schema}.rt_batch_metrics (
  batch_id BIGINT PRIMARY KEY,
  batch_started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  events_in BIGINT NOT NULL,
  events_after_reverse_index BIGINT NOT NULL,
  active_profiles BIGINT NOT NULL,
  candidate_segments BIGINT NOT NULL,
  segment_evaluations BIGINT NOT NULL,
  memberships_changed BIGINT NOT NULL,
  lakebase_profile_reads BIGINT NOT NULL,
  lakebase_membership_reads BIGINT NOT NULL,
  lakebase_membership_writes BIGINT NOT NULL,
  wall_clock_ms BIGINT NOT NULL,
  trigger_interval TEXT NOT NULL,
  lakebase_profile_read_ms BIGINT NOT NULL DEFAULT 0,
  lakebase_membership_read_ms BIGINT NOT NULL DEFAULT 0,
  lakebase_membership_write_ms BIGINT NOT NULL DEFAULT 0,
  event_to_lakebase_write_lag_ms_avg BIGINT NOT NULL DEFAULT 0,
  event_to_lakebase_write_lag_ms_max BIGINT NOT NULL DEFAULT 0
);

ALTER TABLE {schema}.rt_batch_metrics
  ADD COLUMN IF NOT EXISTS lakebase_profile_read_ms BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS lakebase_membership_read_ms BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS lakebase_membership_write_ms BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS event_to_lakebase_write_lag_ms_avg BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS event_to_lakebase_write_lag_ms_max BIGINT NOT NULL DEFAULT 0;

ALTER TABLE {schema}.membership_flags
  ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS {schema}.membership_flags_stage (
  stage_id TEXT NOT NULL,
  batch_id BIGINT NOT NULL,
  profile_id TEXT NOT NULL,
  segment_id TEXT NOT NULL,
  path TEXT NOT NULL,
  is_member BOOLEAN NOT NULL,
  qualified_at TIMESTAMPTZ NOT NULL,
  source_event_id TEXT,
  processed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS membership_flags_stage_stage_idx ON {schema}.membership_flags_stage(stage_id);
CREATE INDEX IF NOT EXISTS membership_flags_stage_profile_idx ON {schema}.membership_flags_stage(profile_id, segment_id, path);

CREATE TABLE IF NOT EXISTS {schema}.read_path_metrics (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT NOT NULL,
  worker_id INTEGER NOT NULL,
  window_started_at TIMESTAMPTZ NOT NULL,
  window_ended_at TIMESTAMPTZ NOT NULL,
  read_requests BIGINT NOT NULL,
  memberships_returned BIGINT NOT NULL,
  errors BIGINT NOT NULL,
  avg_latency_ms DOUBLE PRECISION NOT NULL,
  p50_latency_ms DOUBLE PRECISION NOT NULL,
  p95_latency_ms DOUBLE PRECISION NOT NULL,
  p99_latency_ms DOUBLE PRECISION NOT NULL,
  max_latency_ms DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS read_path_metrics_run_idx ON {schema}.read_path_metrics(run_id, window_started_at);
"""

with conn.cursor() as cur:
    cur.execute(ddl)
conn.commit()

# COMMAND ----------

def upsert_segments() -> int:
    rows = spark.table(fq(c, "segment_definitions_delta")).collect()
    sql = f"""
    INSERT INTO {schema}.segment_definitions
      (segment_id, mode, segment_name, rule_json, referenced_properties, updated_at)
    VALUES (%s, %s, %s, %s, %s, now())
    ON CONFLICT (segment_id) DO UPDATE SET
      mode = EXCLUDED.mode,
      segment_name = EXCLUDED.segment_name,
      rule_json = EXCLUDED.rule_json,
      referenced_properties = EXCLUDED.referenced_properties,
      updated_at = now()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, [(r.segment_id, r.mode, r.segment_name, Json(json.loads(r.rule_json)), r.referenced_properties) for r in rows])
    conn.commit()
    return len(rows)


def upsert_reverse_index() -> int:
    rows = spark.table(fq(c, "segment_reverse_index_delta")).collect()
    sql = f"""
    INSERT INTO {schema}.segment_reverse_index
      (property_name, segment_ids, fanout, updated_at)
    VALUES (%s, %s, %s, now())
    ON CONFLICT (property_name) DO UPDATE SET
      segment_ids = EXCLUDED.segment_ids,
      fanout = EXCLUDED.fanout,
      updated_at = now()
    """
    with conn.cursor() as cur:
        cur.executemany(sql, [(r.property_name, r.segment_ids, r.fanout) for r in rows])
    conn.commit()
    return len(rows)


def upsert_profile_attributes(limit_rows: int | None = None, chunk_size: int = 2000) -> int:
    df = spark.table(fq(c, "profile_attributes_delta"))
    if limit_rows:
        df = df.limit(limit_rows)
    cols = df.columns
    payload_cols = [x for x in cols if x != "profile_id"]
    rows = (
        df.select(
            "profile_id",
            "account_id",
            F.to_json(F.struct(*[F.col(x) for x in payload_cols])).alias("attrs_json"),
        )
        .toLocalIterator()
    )
    sql = f"""
    INSERT INTO {schema}.profile_attributes
      (profile_id, account_id, attrs, updated_at)
    VALUES (%s, %s, %s, now())
    ON CONFLICT (profile_id) DO UPDATE SET
      account_id = EXCLUDED.account_id,
      attrs = EXCLUDED.attrs,
      updated_at = now()
    """
    count = 0
    buffer = []
    with conn.cursor() as cur:
        for r in rows:
            buffer.append((r.profile_id, r.account_id, Json(json.loads(r.attrs_json))))
            if len(buffer) >= chunk_size:
                cur.executemany(sql, buffer)
                conn.commit()
                count += len(buffer)
                buffer.clear()
        if buffer:
            cur.executemany(sql, buffer)
            conn.commit()
            count += len(buffer)
    return count


seg_count = upsert_segments()
idx_count = upsert_reverse_index()
profile_sync_limit = int(widget("profile_sync_limit", c["profile_count"]))
profile_count_synced = upsert_profile_attributes(profile_sync_limit)

with conn.cursor(cursor_factory=RealDictCursor) as cur:
    cur.execute(f"SELECT COUNT(*) AS n FROM {schema}.profile_attributes")
    serving_profiles = cur.fetchone()["n"]
conn.close()

print(json.dumps({
    "segments_synced": seg_count,
    "reverse_index_rows_synced": idx_count,
    "profiles_synced_this_run": profile_count_synced,
    "serving_profiles": serving_profiles,
}, indent=2))
