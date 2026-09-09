# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import os
import sys

from pyspark.sql import functions as F

try:
    _src = os.path.join(dbutils.widgets.get("bundle_file_path"), "src")
except Exception:
    _src = ""
if _src and _src not in sys.path:
    sys.path.append(_src)

from cdp_engine.eventhub import eventhub_kafka_options

c = cfg()
run_id = widget("sizing_run_id", "")
starting_offsets = widget("starting_offsets", "earliest")
limit_rows = int(widget("limit_rows", "0"))

connection_string = dbutils.secrets.get(c["eventhub_secret_scope"], c["eventhub_secret_key"]).strip()

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

options = eventhub_kafka_options(
    bootstrap_servers=c["eventhub_bootstrap"],
    eventhub_name=c["eventhub_name"],
    connection_string=connection_string,
    starting_offsets=starting_offsets,
)
options["endingOffsets"] = "latest"

raw = spark.read.format("kafka").options(**options).load()
if limit_rows > 0:
    raw = raw.limit(limit_rows)

parsed = (
    raw.select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp"),
        F.col("value").cast("string").alias("raw_value"),
    )
    .withColumn("e", F.from_json(F.col("raw_value"), event_schema))
    .select("topic", "partition", "offset", "timestamp", "raw_value", "e.*")
)

matched = parsed.where(F.col("run_id") == F.lit(run_id)) if run_id else parsed
filter_passing = matched.where(
    F.arrays_overlap(
        F.col("changed_properties"),
        F.array(
            F.lit("beh_page_type"),
            F.lit("beh_intent"),
            F.lit("beh_device"),
            F.lit("beh_referrer_class"),
        ),
    )
)

sample = [
    row.asDict(recursive=True)
    for row in matched.select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        "event_id",
        "run_id",
        "profile_id",
        "changed_properties",
    ).limit(5).collect()
]

result = {
    "run_id": run_id,
    "raw_count": raw.count(),
    "parsed_non_null_event_id": parsed.where(F.col("event_id").isNotNull()).count(),
    "matched_run_id_count": matched.count(),
    "filter_passing_count": filter_passing.count(),
    "sample": sample,
}
print(json.dumps(result, default=str, indent=2))
dbutils.notebook.exit(json.dumps(result, default=str))
