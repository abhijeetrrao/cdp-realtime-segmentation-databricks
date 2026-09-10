# Databricks notebook source
# Load customer segment definitions from a CSV file into Delta and rebuild the reverse index.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from cdp_engine.rules import leaf_rules

c = cfg()
rules_csv_path = widget("rules_csv_path", volume_path(c, "rules/FrequentShipAbandonEmail_Segment_JSON.csv"))
attribute_mapping_table_name = widget("attribute_mapping_table_name", "segment_attribute_mapping")

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
    rule = json.loads(rule_json)
    props = set()
    for leaf in leaf_rules(rule):
        rule_property = leaf["property"]
        mapped = attribute_mapping.get(rule_property)
        if mapped:
            if mapped["source"] == "EVENT":
                props.add(rule_property)
                props.add(mapped["column_name"])
            continue
        if rule_property.startswith("DL_") or rule_property.startswith("beh_"):
            props.add(rule_property)
    return sorted(props)


def load_attribute_mapping() -> dict[str, dict[str, str]]:
    try:
        rows = (
            spark.read.table(attribute_mapping_table_name)
            .select("rule_property", "source", "column_name")
            .where("rule_property IS NOT NULL AND source IS NOT NULL AND column_name IS NOT NULL")
            .collect()
        )
    except Exception:
        return {}
    return {
        str(row["rule_property"]): {
            "source": "ACCOUNT" if str(row["source"]).upper() == "ACCOUNTS" else str(row["source"]).upper(),
            "column_name": str(row["column_name"]),
        }
        for row in rows
    }


referenced_udf = F.udf(parse_referenced_properties, T.ArrayType(T.StringType()))
attribute_mapping = load_attribute_mapping()
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
            "attribute_mapping_table_name": attribute_mapping_table_name,
            "mapped_properties": len(attribute_mapping),
            "segment_rules_loaded": rules.count(),
            "reverse_index_rows": reverse_index.count(),
        },
        indent=2,
    )
)
