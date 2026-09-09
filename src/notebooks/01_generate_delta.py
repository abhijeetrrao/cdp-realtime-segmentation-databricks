# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json

from pyspark.sql import functions as F

c = cfg()
create_namespace(c)

profile_count = int(c["profile_count"])
account_count = int(c["account_count"])

# COMMAND ----------

# Accounts: production is about 17M rows / 200 columns. The default sandbox row
# count is lower, but the shape and deterministic distributions are retained.
acct = spark.range(account_count).select(
    F.col("id").alias("_row_id"),
    F.format_string("acct_%012d", F.col("id")).alias("account_id"),
    F.element_at(F.array(F.lit("enterprise"), F.lit("midmarket"), F.lit("smb")), (F.col("id") % 3 + 1).cast("int")).alias("acct_segment_type"),
    F.element_at(F.array(F.lit("na"), F.lit("emea"), F.lit("latam"), F.lit("apac")), (F.col("id") % 4 + 1).cast("int")).alias("acct_region"),
    (F.col("id") % 1000000).cast("int").alias("acct_revenue_band"),
    (F.col("id") % 1000).cast("int").alias("acct_employee_band"),
)

for i in range(1, 196):
    if i % 4 == 0:
        acct = acct.withColumn(f"acct_num_{i:03d}", ((F.col("_row_id") * (i + 17)) % 10000).cast("int"))
    elif i % 4 == 1:
        acct = acct.withColumn(f"acct_flag_{i:03d}", ((F.col("_row_id") + i) % 11 == 0))
    elif i % 4 == 2:
        acct = acct.withColumn(f"acct_score_{i:03d}", ((F.col("_row_id") % (i + 37)) / F.lit(i + 37)).cast("double"))
    else:
        acct = acct.withColumn(f"acct_str_{i:03d}", F.rpad(F.concat(F.lit(f"a{i}_"), F.col("_row_id").cast("string")), 18, "x"))

acct = acct.drop("_row_id")
(
    acct.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "account_attributes_delta"))
)

# COMMAND ----------

# Profiles: about 450 logical columns. The generated string columns intentionally
# add width so the simulation keeps row-density pressure visible.
profiles = spark.range(profile_count).select(
    F.col("id").alias("_row_id"),
    F.format_string("prof_%012d", F.col("id")).alias("profile_id"),
    F.format_string("acct_%012d", (F.col("id") % account_count)).alias("account_id"),
    (F.col("id") % 40).cast("int").alias("prof_visits_30d"),
    (F.col("id") % 200).cast("int").alias("prof_pageviews_30d"),
    (F.col("id") % 17 == 0).alias("prof_logged_in"),
    F.element_at(F.array(F.lit("web"), F.lit("mobile"), F.lit("partner")), (F.col("id") % 3 + 1).cast("int")).alias("prof_preferred_channel"),
    F.element_at(F.array(F.lit("quote"), F.lit("tracking"), F.lit("returns"), F.lit("billing")), (F.col("id") % 4 + 1).cast("int")).alias("prof_last_intent"),
    ((F.col("id") * 13) % 1000).cast("int").alias("prof_engagement_score"),
)

for i in range(1, 443):
    if i % 5 == 0:
        profiles = profiles.withColumn(f"prof_num_{i:03d}", ((F.col("_row_id") * (i + 3)) % 100000).cast("int"))
    elif i % 5 == 1:
        profiles = profiles.withColumn(f"prof_flag_{i:03d}", ((F.col("_row_id") + i) % 13 == 0))
    elif i % 5 == 2:
        profiles = profiles.withColumn(f"prof_score_{i:03d}", ((F.col("_row_id") % (i + 101)) / F.lit(i + 101)).cast("double"))
    elif i % 5 == 3:
        profiles = profiles.withColumn(f"prof_ts_{i:03d}", F.timestamp_seconds(F.lit(1700000000) + (F.col("_row_id") % 1000000)))
    else:
        profiles = profiles.withColumn(f"prof_str_{i:03d}", F.rpad(F.concat(F.lit(f"p{i}_"), F.col("_row_id").cast("string")), 22, "z"))

profiles = profiles.drop("_row_id")
profiles = profiles.join(
    spark.table(fq(c, "account_attributes_delta")).select(
        "account_id", "acct_segment_type", "acct_region", "acct_revenue_band", "acct_employee_band"
    ),
    "account_id",
)

(
    profiles.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "profile_attributes_delta"))
)

spark.sql(f"ALTER TABLE {fq(c, 'profile_attributes_delta')} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"ALTER TABLE {fq(c, 'account_attributes_delta')} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

realtime_event_props = ["beh_page_type", "beh_intent", "beh_device", "beh_referrer_class"]
attribute_props = [
    "acct_segment_type",
    "acct_region",
    "acct_revenue_band",
    "prof_visits_30d",
    "prof_pageviews_30d",
    "prof_logged_in",
    "prof_preferred_channel",
    "prof_last_intent",
    "prof_engagement_score",
]

def make_attr_rule(prop: str, i: int) -> dict:
    values = {
        "acct_segment_type": ["enterprise", "midmarket", "smb"],
        "acct_region": ["na", "emea", "latam", "apac"],
        "prof_preferred_channel": ["web", "mobile", "partner"],
        "prof_last_intent": ["quote", "tracking", "returns", "billing"],
    }
    if prop in values:
        return {"op": "eq", "property": prop, "value": values[prop][i % len(values[prop])]}
    if prop == "prof_logged_in":
        return {"op": "eq", "property": prop, "value": bool(i % 2)}
    return {"op": "gte", "property": prop, "value": 1 + (i % 4)}

segments = []
for i in range(800):
    event_prop = realtime_event_props[i % len(realtime_event_props)]
    if event_prop == "beh_page_type":
        event_rule = {"op": "eq", "property": event_prop, "value": ["pricing", "tracking", "returns", "support"][i % 4]}
    elif event_prop == "beh_intent":
        event_rule = {"op": "eq", "property": event_prop, "value": ["ship", "track", "return", "pay"][i % 4]}
    elif event_prop == "beh_device":
        event_rule = {"op": "eq", "property": event_prop, "value": ["desktop", "mobile"][i % 2]}
    else:
        event_rule = {"op": "eq", "property": event_prop, "value": ["search", "direct", "campaign", "partner"][i % 4]}

    attr_rule = make_attr_rule(attribute_props[i % len(attribute_props)], i)

    rule = {"op": "and", "rules": [event_rule, attr_rule]}
    props = sorted({event_rule["property"], attr_rule["property"]})
    segments.append((f"rt_{i:03d}", "realtime", f"Realtime segment {i:03d}", json.dumps(rule), props))

for i in range(300):
    attr_a = attribute_props[i % len(attribute_props)]
    attr_b = attribute_props[(i + 3) % len(attribute_props)]
    rule = {"op": "and", "rules": [make_attr_rule(attr_a, i), make_attr_rule(attr_b, i + 1)]}
    segments.append((f"bt_{i:03d}", "batch", f"Batch segment {i:03d}", json.dumps(rule), sorted({attr_a, attr_b})))

seg_df = spark.createDataFrame(segments, "segment_id string, mode string, segment_name string, rule_json string, referenced_properties array<string>")
(
    seg_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "segment_definitions_delta"))
)
spark.sql(f"ALTER TABLE {fq(c, 'segment_definitions_delta')} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

reverse_index = (
    seg_df.where("mode = 'realtime'")
    .select(F.explode("referenced_properties").alias("property_name"), "segment_id")
    .groupBy("property_name")
    .agg(F.collect_set("segment_id").alias("segment_ids"), F.count("*").alias("fanout"))
)
(
    reverse_index.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "segment_reverse_index_delta"))
)

display(reverse_index.orderBy(F.desc("fanout")))
