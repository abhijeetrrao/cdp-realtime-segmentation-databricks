# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json

c = cfg()
schema = "cdp_rt"
checkpoint_suffix = widget("checkpoint_suffix", "realtime_segmentation_serverless_job")
checkpoint_path = volume_path(c, f"checkpoints/{checkpoint_suffix}")
sizing_run_id = widget("sizing_run_id", "manual")

try:
    dbutils.fs.rm(checkpoint_path, recurse=True)
    checkpoint_removed = True
except Exception:
    checkpoint_removed = False

conn = get_lakebase_connection(c["lakebase_endpoint"], c["lakebase_database"])
try:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE {schema}.rt_batch_metrics
              ADD COLUMN IF NOT EXISTS lakebase_profile_read_ms BIGINT NOT NULL DEFAULT 0,
              ADD COLUMN IF NOT EXISTS lakebase_membership_read_ms BIGINT NOT NULL DEFAULT 0,
              ADD COLUMN IF NOT EXISTS lakebase_membership_write_ms BIGINT NOT NULL DEFAULT 0,
              ADD COLUMN IF NOT EXISTS event_to_lakebase_write_lag_ms_avg BIGINT NOT NULL DEFAULT 0,
              ADD COLUMN IF NOT EXISTS event_to_lakebase_write_lag_ms_max BIGINT NOT NULL DEFAULT 0
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {schema}.membership_flags
              ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            """
        )
        cur.execute(
            f"""
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
            )
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS membership_flags_stage_stage_idx ON {schema}.membership_flags_stage(stage_id)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS membership_flags_stage_profile_idx ON {schema}.membership_flags_stage(profile_id, segment_id, path)")
        cur.execute(
            f"""
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
            )
            """
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS read_path_metrics_run_idx ON {schema}.read_path_metrics(run_id, window_started_at)")
        cur.execute(f"TRUNCATE TABLE {schema}.rt_batch_metrics")
        cur.execute(f"DELETE FROM {schema}.membership_flags_stage WHERE created_at < now() - interval '1 day'")
        cur.execute(f"DELETE FROM {schema}.read_path_metrics WHERE run_id = %s", (sizing_run_id,))
    conn.commit()
finally:
    conn.close()

print(
    json.dumps(
        {
            "rt_batch_metrics_truncated": True,
            "read_path_metrics_cleared_for_run_id": sizing_run_id,
            "checkpoint_path": checkpoint_path,
            "checkpoint_removed": checkpoint_removed,
        },
        indent=2,
    )
)
