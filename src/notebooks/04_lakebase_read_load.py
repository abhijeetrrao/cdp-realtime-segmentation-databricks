# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import random
import re
import statistics
import threading
import time
from datetime import datetime, timezone

import psycopg2

from databricks.sdk import WorkspaceClient

c = cfg()
schema = "cdp_rt"
profile_count = int(c["profile_count"])
duration_seconds = int(widget("read_duration_seconds", "1800"))
read_qps_param = float(widget("read_qps", "0"))
read_concurrency = int(widget("read_concurrency", "8"))
flush_seconds = int(widget("read_metrics_flush_seconds", "10"))
sizing_run_id = widget("sizing_run_id", "manual")
if not re.fullmatch(r"[A-Za-z0-9_]+", sizing_run_id):
    raise ValueError(f"Invalid sizing_run_id for table suffix: {sizing_run_id}")
membership_table = f"membership_flags_current_{sizing_run_id}"

reference_average_eps = 815.0
target_qps = read_qps_param
if target_qps <= 0:
    target_qps = reference_average_eps * float(c["event_scale_fraction"]) * float(c["burst_multiplier"])


def as_dict(value):
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        return value.as_dict()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Cannot convert {type(value).__name__} to dict")


def resolve_connection_info(endpoint_path: str, database: str) -> dict[str, str]:
    w = WorkspaceClient()
    postgres = getattr(w, "postgres", None)
    if postgres is not None:
        endpoint = as_dict(postgres.get_endpoint(name=endpoint_path))
        credential = postgres.generate_database_credential(endpoint=endpoint_path)
        token = getattr(credential, "token", None) or credential["token"]
    else:
        endpoint = w.api_client.do(method="GET", path=f"/api/2.0/postgres/{endpoint_path}")
        credential = w.api_client.do(
            method="POST",
            path="/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_path},
        )
        token = credential["token"]

    return {
        "host": endpoint["status"]["hosts"]["host"],
        "database": database,
        "user": w.current_user.me().user_name,
        "password": token,
    }


def connect_lakebase(connection_info: dict[str, str]):
    return psycopg2.connect(
        host=connection_info["host"],
        dbname=connection_info["database"],
        user=connection_info["user"],
        password=connection_info["password"],
        sslmode="require",
    )


conn_info = resolve_connection_info(c["lakebase_endpoint"], c["lakebase_database"])


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * pct))))
    return ordered[idx]


def write_window_metrics(worker_id: int, started_at: datetime, ended_at: datetime, latencies: list[float], returned: int, errors: int) -> None:
    if not latencies and errors == 0:
        return
    conn = connect_lakebase(conn_info)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {schema}.read_path_metrics
                  (run_id, worker_id, window_started_at, window_ended_at,
                   read_requests, memberships_returned, errors, avg_latency_ms,
                   p50_latency_ms, p95_latency_ms, p99_latency_ms, max_latency_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    sizing_run_id,
                    worker_id,
                    started_at,
                    ended_at,
                    len(latencies),
                    returned,
                    errors,
                    statistics.fmean(latencies) if latencies else 0.0,
                    percentile(latencies, 0.50),
                    percentile(latencies, 0.95),
                    percentile(latencies, 0.99),
                    max(latencies) if latencies else 0.0,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def worker(worker_id: int, qps: float) -> None:
    rng = random.Random(worker_id + int(time.time()))
    period = 1.0 / max(qps, 0.001)
    deadline = time.monotonic() + duration_seconds
    next_due = time.monotonic()
    window_started = datetime.now(timezone.utc)
    window_deadline = time.monotonic() + flush_seconds
    latencies: list[float] = []
    returned = 0
    errors = 0
    conn = connect_lakebase(conn_info)
    try:
        while time.monotonic() < deadline:
            profile_id = f"prof_{rng.randrange(profile_count):012d}"
            start = time.perf_counter()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT segment_id
                        FROM {schema}.{membership_table}
                        WHERE profile_id = %s AND is_member = true
                        ORDER BY segment_id
                        """,
                        (profile_id,),
                    )
                    returned += len(cur.fetchall())
                latencies.append((time.perf_counter() - start) * 1000.0)
            except Exception:
                errors += 1
                try:
                    conn.close()
                except Exception:
                    pass
                conn = connect_lakebase(conn_info)

            now = time.monotonic()
            if now >= window_deadline:
                write_window_metrics(worker_id, window_started, datetime.now(timezone.utc), latencies, returned, errors)
                window_started = datetime.now(timezone.utc)
                window_deadline = now + flush_seconds
                latencies = []
                returned = 0
                errors = 0

            next_due += period
            sleep_for = next_due - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
    finally:
        if latencies or errors:
            write_window_metrics(worker_id, window_started, datetime.now(timezone.utc), latencies, returned, errors)
        conn.close()


threads = []
qps_per_worker = target_qps / max(read_concurrency, 1)
for worker_id in range(read_concurrency):
    thread = threading.Thread(target=worker, args=(worker_id, qps_per_worker), daemon=False)
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

print(
    json.dumps(
        {
            "sizing_run_id": sizing_run_id,
            "duration_seconds": duration_seconds,
            "target_qps": target_qps,
            "read_concurrency": read_concurrency,
        },
        indent=2,
    )
)
