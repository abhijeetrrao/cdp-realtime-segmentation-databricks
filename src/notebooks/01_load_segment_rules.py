# Databricks notebook source
# Load customer segment definitions from a CSV file into Delta and rebuild the reverse index.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from cdp_engine.rules import trigger_properties

c = cfg()
rules_csv_path = widget("rules_csv_path", volume_path(c, "rules/FrequentShipAbandonEmail_Segment_JSON.csv"))

create_namespace(c)

raw_rules = (
    spark.read.format("csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .load(rules_csv_path)
)

required = {"segment_id", "mode", "segment_name", "rule_json"}
missing = sorted(required - set(raw_rules.columns))
if missing:
    raise ValueError(f"Rules CSV is missing required column(s): {missing}")


def parse_referenced_properties(rule_json: str) -> list[str]:
    if not rule_json:
        return []
    rule = json.loads(rule_json)
    props = set()

    def walk(node):
        if "property" in node:
            props.add(node["property"])
        for child in node.get("rules", []) or []:
            walk(child)
        if "rule" in node:
            walk(node["rule"])

    walk(rule)
    return sorted(props)


def parse_trigger_properties(rule_json: str) -> list[str]:
    if not rule_json:
        return []
    return sorted(trigger_properties(json.loads(rule_json)))


referenced_udf = F.udf(parse_referenced_properties, T.ArrayType(T.StringType()))
trigger_udf = F.udf(parse_trigger_properties, T.ArrayType(T.StringType()))

rules = (
    raw_rules.select(
        F.col("segment_id").cast("string"),
        F.col("mode").cast("string"),
        F.col("segment_name").cast("string"),
        F.col("rule_json").cast("string"),
        F.when(
            F.col("referenced_properties").isNotNull(),
            F.from_json(F.col("referenced_properties"), "array<string>"),
        )
        .otherwise(referenced_udf(F.col("rule_json")))
        .alias("referenced_properties"),
        F.current_timestamp().alias("updated_at"),
    )
    .where("segment_id IS NOT NULL AND mode IS NOT NULL AND rule_json IS NOT NULL")
)

(
    rules.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "segment_definitions_delta"))
)
spark.sql(f"ALTER TABLE {fq(c, 'segment_definitions_delta')} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

rules_with_triggers = rules.withColumn("trigger_properties", trigger_udf(F.col("rule_json")))
reverse_index = (
    rules_with_triggers.where(F.col("mode") == F.lit("realtime"))
    .select(F.explode("trigger_properties").alias("property_name"), "segment_id")
    .groupBy("property_name")
    .agg(F.collect_set("segment_id").alias("segment_ids"), F.count("*").alias("fanout"))
)

(
    reverse_index.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(fq(c, "segment_reverse_index_delta"))
)

print(
    json.dumps(
        {
            "rules_csv_path": rules_csv_path,
            "segment_rules_loaded": rules.count(),
            "reverse_index_rows": reverse_index.count(),
        },
        indent=2,
    )
)
