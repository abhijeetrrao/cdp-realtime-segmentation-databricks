# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import time

import psycopg2
from psycopg2.extras import RealDictCursor

from databricks.sdk import WorkspaceClient

c = cfg()
profile_id = widget("read_profile_id", "prof_000000000001")
schema = "cdp_rt"

start = time.time()

w = WorkspaceClient()
postgres = getattr(w, "postgres", None)
if postgres is not None:
    endpoint = postgres.get_endpoint(name=c["lakebase_endpoint"])
    endpoint_dict = endpoint.as_dict() if hasattr(endpoint, "as_dict") else endpoint
    credential = postgres.generate_database_credential(endpoint=c["lakebase_endpoint"])
    token = getattr(credential, "token", None) or credential["token"]
else:
    endpoint_dict = w.api_client.do(method="GET", path=f"/api/2.0/postgres/{c['lakebase_endpoint']}")
    token = w.api_client.do(
        method="POST",
        path="/api/2.0/postgres/credentials",
        body={"endpoint": c["lakebase_endpoint"]},
    )["token"]

conn = psycopg2.connect(
    host=endpoint_dict["status"]["hosts"]["host"],
    dbname=c["lakebase_database"],
    user=w.current_user.me().user_name,
    password=token,
    sslmode="require",
)
try:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT segment_id, path, qualified_at, source_event_id
            FROM {schema}.membership_flags
            WHERE profile_id = %s AND is_member = true
            ORDER BY path, segment_id
            """,
            (profile_id,),
        )
        rows = cur.fetchall()
finally:
    conn.close()

print(json.dumps({
    "profile_id": profile_id,
    "lookup_ms": round((time.time() - start) * 1000, 2),
    "segments": rows,
}, default=str, indent=2))
