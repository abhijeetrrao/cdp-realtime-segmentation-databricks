# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import time

from psycopg2.extras import RealDictCursor

c = cfg()
schema = "cdp_rt"
timeout_seconds = int(widget("rt_metrics_timeout_seconds", "600"))
poll_seconds = int(widget("rt_metrics_poll_seconds", "10"))
min_non_empty_batches = int(widget("min_non_empty_rt_batches", "1"))

deadline = time.time() + timeout_seconds
last = {}

while time.time() < deadline:
    conn = get_lakebase_connection(c["lakebase_endpoint"], c["lakebase_database"])
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT
                  COUNT(*) AS total_batches,
                  COUNT(*) FILTER (WHERE events_after_reverse_index > 0) AS non_empty_batches,
                  COALESCE(SUM(events_in), 0) AS events_in,
                  COALESCE(SUM(events_after_reverse_index), 0) AS events_after_reverse_index,
                  COALESCE(SUM(memberships_changed), 0) AS memberships_changed
                FROM {schema}.rt_batch_metrics
                """
            )
            last = dict(cur.fetchone())
    finally:
        conn.close()

    if int(last.get("non_empty_batches") or 0) >= min_non_empty_batches:
        print(json.dumps({"status": "ready", **last}, default=str, indent=2))
        break
    time.sleep(poll_seconds)
else:
    raise RuntimeError(
        "Timed out waiting for non-empty real-time metrics. "
        f"Last observed metrics summary: {json.dumps(last, default=str)}"
    )
