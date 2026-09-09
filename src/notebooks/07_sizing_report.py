# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json

from psycopg2.extras import RealDictCursor

c = cfg()
schema = "cdp_rt"
sizing_run_id = widget("sizing_run_id", "manual")
conn = get_lakebase_connection(c["lakebase_endpoint"], c["lakebase_database"])
try:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT * FROM {schema}.rt_batch_metrics WHERE events_after_reverse_index > 0 ORDER BY batch_id DESC LIMIT 50")
        metrics = cur.fetchall()
        cur.execute(f"SELECT property_name, fanout FROM {schema}.segment_reverse_index ORDER BY fanout DESC")
        fanout = cur.fetchall()
        cur.execute(
            f"""
            SELECT
              COALESCE(SUM(read_requests), 0) AS read_requests,
              COALESCE(SUM(memberships_returned), 0) AS memberships_returned,
              COALESCE(SUM(errors), 0) AS read_errors,
              COALESCE(AVG(avg_latency_ms), 0) AS avg_latency_ms,
              COALESCE(MAX(p95_latency_ms), 0) AS p95_latency_ms,
              COALESCE(MAX(p99_latency_ms), 0) AS p99_latency_ms,
              COALESCE(MAX(max_latency_ms), 0) AS max_latency_ms
            FROM {schema}.read_path_metrics
            WHERE run_id = %s
            """,
            (sizing_run_id,),
        )
        read_metrics = dict(cur.fetchone())
finally:
    conn.close()

if not metrics:
    raise RuntimeError("No non-empty real-time metrics found. Run the stream against Event Hub first.")

def avg(name: str) -> float:
    vals = [float(m[name]) for m in metrics]
    return sum(vals) / len(vals)

avg_events = avg("events_in")
avg_survivors = avg("events_after_reverse_index")
avg_profiles = avg("active_profiles")
avg_evals = avg("segment_evaluations")
avg_wall_ms = avg("wall_clock_ms")
avg_profile_read_ms = avg("lakebase_profile_read_ms")
avg_membership_read_ms = avg("lakebase_membership_read_ms")
avg_membership_write_ms = avg("lakebase_membership_write_ms")
avg_write_lag_ms = avg("event_to_lakebase_write_lag_ms_avg")
max_write_lag_ms = max(float(m["event_to_lakebase_write_lag_ms_max"]) for m in metrics)
survival_rate = avg_survivors / max(avg_events, 1)
fanout_per_survivor = avg_evals / max(avg_survivors, 1)
ms_per_active_profile = avg_wall_ms / max(avg_profiles, 1)
ms_per_eval = avg_wall_ms / max(avg_evals, 1)

prod_peak_eps = 3010.0
trigger_seconds = 5.0
try:
    trigger_seconds = float(c["trigger_interval"].split()[0])
except Exception:
    pass

prod_peak_events_per_batch = prod_peak_eps * trigger_seconds
prod_survivors = prod_peak_events_per_batch * survival_rate
prod_evals = prod_survivors * fanout_per_survivor
prod_active_profiles = min(prod_peak_events_per_batch, prod_survivors)
estimated_wall_ms = max(prod_active_profiles * ms_per_active_profile, prod_evals * ms_per_eval)

report = {
    "sizing_run_id": sizing_run_id,
    "measured_batches": len(metrics),
    "measured": {
        "avg_events_per_batch": avg_events,
        "avg_surviving_events_per_batch": avg_survivors,
        "avg_active_profiles_per_batch": avg_profiles,
        "avg_segment_evaluations_per_batch": avg_evals,
        "avg_wall_clock_ms": avg_wall_ms,
        "avg_lakebase_profile_read_ms_per_batch": avg_profile_read_ms,
        "avg_lakebase_membership_read_ms_per_batch": avg_membership_read_ms,
        "avg_lakebase_membership_write_ms_per_batch": avg_membership_write_ms,
        "avg_event_to_lakebase_write_lag_ms": avg_write_lag_ms,
        "max_event_to_lakebase_write_lag_ms": max_write_lag_ms,
        "reverse_index_survival_rate": survival_rate,
        "fanout_per_surviving_event": fanout_per_survivor,
        "ms_per_active_profile": ms_per_active_profile,
        "ms_per_segment_evaluation": ms_per_eval,
    },
    "lakebase_page_read_load": read_metrics,
    "reference_peak_extrapolation": {
        "profiles_at_rest": 1_000_000_000,
        "peak_events_per_second": prod_peak_eps,
        "trigger_seconds": trigger_seconds,
        "peak_events_per_batch": prod_peak_events_per_batch,
        "estimated_surviving_events_per_batch": prod_survivors,
        "estimated_active_profiles_per_batch": prod_active_profiles,
        "estimated_segment_evaluations_per_batch": prod_evals,
        "estimated_batch_wall_clock_ms": estimated_wall_ms,
        "cluster_guidance": "Run this path as a continuous serverless Lakeflow Declarative Pipeline. Keep Event Hub ingestion uncapped for the synced-table architecture, monitor Kafka backlog and current-membership sync lag, and size Lakebase CU until serving freshness stays inside the target latency budget.",
    },
    "naive_contrast": {
        "naive_peak_checks_per_second": prod_peak_eps * 800,
        "indexed_peak_checks_per_second": prod_peak_eps * survival_rate * fanout_per_survivor,
        "full_base_scan_rows_per_batch": 1_000_000_000,
        "indexed_profile_reads_per_batch": prod_active_profiles,
    },
    "reverse_index_fanout": fanout,
}

print(json.dumps(report, default=str, indent=2))
