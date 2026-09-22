# Databricks notebook source
# Build profile-level account features for realtime segment evaluation.

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json

from pyspark.sql import functions as F
from pyspark.sql import types as T

from cdp_engine.rules import leaf_rules

c = cfg()

profile_table_name = widget("profile_table_name", "profile_attributes_delta")
profile_id_col = widget("profile_id_col", "profile_id")
profile_accounts_col = widget("profile_accounts_col", "accounts")
account_table_name = widget("account_table_name", "account_attributes_delta")
account_profile_id_col = widget("account_profile_id_col", "profile_id")
account_id_col = widget("account_id_col", "account_group_id")
profile_account_features_table_name = widget("profile_account_features_table_name", "profile_account_features_delta")
segment_definitions_table_name = widget("segment_definitions_table_name", "segment_definitions_delta")
attribute_mapping_table_name = widget("attribute_mapping_table_name", "segment_attribute_mapping")

create_namespace(c)


def resolve_table_name(table_name: str) -> str:
    table_name = table_name.strip()
    if "." in table_name:
        return table_name.replace("`", "")
    return f"{c['catalog']}.{c['schema']}.{table_name.replace('`', '')}"


def quote_table_name(table_name: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in resolve_table_name(table_name).split("."))


def effective_account_id_col(account_cols: set[str]) -> str | None:
    if account_id_col in account_cols:
        return account_id_col
    if "account_id" in account_cols:
        return "account_id"
    return None


def normalize_table_name(table_name: str) -> str:
    return table_name.replace("`", "").strip().lower()


def source_role(source: str) -> str:
    legacy_role = source.strip().upper()
    if legacy_role == "ACCOUNTS":
        return "ACCOUNT"
    if legacy_role in {"EVENT", "PROFILE", "ACCOUNT"}:
        return legacy_role

    source_table = normalize_table_name(source)
    table_roles = {
        normalize_table_name(resolve_table_name(profile_table_name)): "PROFILE",
        normalize_table_name(resolve_table_name(account_table_name)): "ACCOUNT",
    }
    try:
        source_event_table_name = widget("source_event_table_name", "")
        if source_event_table_name:
            table_roles[normalize_table_name(resolve_table_name(source_event_table_name))] = "EVENT"
    except Exception:
        pass
    return table_roles.get(source_table, "")


def load_attribute_mapping() -> dict[str, dict[str, str]]:
    try:
        rows = (
            spark.read.table(resolve_table_name(attribute_mapping_table_name))
            .select("rule_property", "source", "column_name")
            .where("rule_property IS NOT NULL AND source IS NOT NULL AND column_name IS NOT NULL")
            .collect()
        )
    except Exception:
        return {}

    mapping = {}
    for row in rows:
        role = source_role(str(row["source"]))
        if role:
            mapping[str(row["rule_property"])] = {
                "source": role,
                "column_name": str(row["column_name"]),
            }
    return mapping


def required_account_features(attribute_mapping: dict[str, dict[str, str]]) -> tuple[dict[str, str], set[str]]:
    rules = (
        spark.read.table(resolve_table_name(segment_definitions_table_name))
        .where(F.col("mode") == F.lit("realtime"))
        .select("segment_id", "rule_json")
        .where("segment_id IS NOT NULL AND rule_json IS NOT NULL")
        .collect()
    )
    features: dict[str, str] = {}
    segment_ids: set[str] = set()
    for row in rules:
        raw_rule = row["rule_json"]
        rule = json.loads(raw_rule) if isinstance(raw_rule, str) else raw_rule
        for leaf in leaf_rules(rule):
            rule_property = str(leaf.get("property", ""))
            mapped = attribute_mapping.get(rule_property)
            is_account = mapped["source"] == "ACCOUNT" if mapped else leaf.get("source") == "ACCOUNTS" or rule_property.startswith("AZ_A_")
            if is_account:
                features[rule_property] = mapped["column_name"] if mapped else rule_property
                segment_ids.add(str(row["segment_id"]))
    return features, segment_ids


profiles = spark.read.table(resolve_table_name(profile_table_name))
accounts = spark.read.table(resolve_table_name(account_table_name))
account_cols = set(accounts.columns)
effective_account_id = effective_account_id_col(account_cols)

if profile_id_col not in profiles.columns:
    raise ValueError(f"Profile table {profile_table_name!r} must contain {profile_id_col!r}")

attribute_mapping = load_attribute_mapping()
account_feature_mapping, account_segment_ids = required_account_features(attribute_mapping)
missing_account_columns = sorted({column for column in account_feature_mapping.values() if column not in account_cols})
if missing_account_columns:
    raise ValueError(
        f"Account table {account_table_name!r} is missing columns referenced by realtime rules: {missing_account_columns}"
    )

value_fields = [
    field
    for field in accounts.schema.fields
    if field.name in set(account_feature_mapping.values())
    and field.name not in {account_id_col, account_profile_id_col, "account_id", "profile_id"}
    and not field.name.startswith("_")
]
if not value_fields:
    empty_schema = T.StructType(
        [
            T.StructField("profile_id", T.StringType()),
            T.StructField("account_attrs", T.MapType(T.StringType(), T.StringType())),
            T.StructField("updated_at", T.TimestampType()),
        ]
    )
    spark.createDataFrame([], empty_schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        resolve_table_name(profile_account_features_table_name)
    )
    summary = {
        "profile_table_name": resolve_table_name(profile_table_name),
        "account_table_name": resolve_table_name(account_table_name),
        "segment_definitions_table_name": resolve_table_name(segment_definitions_table_name),
        "attribute_mapping_table_name": resolve_table_name(attribute_mapping_table_name),
        "profile_account_features_table_name": resolve_table_name(profile_account_features_table_name),
        "required_account_rule_properties": 0,
        "account_feature_columns": 0,
        "segments_with_account_features": 0,
        "profile_account_feature_rows": 0,
    }
    print(json.dumps(summary, indent=2))
    dbutils.notebook.exit(json.dumps(summary))

aggregations = []
for field in value_fields:
    column = F.col(f"`{field.name}`")
    if isinstance(field.dataType, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)):
        aggregations.append(F.sum(column.cast("double")).cast("string").alias(field.name))
    else:
        aggregations.append(
            F.concat_ws("\u001f", F.sort_array(F.collect_set(column.cast("string")))).alias(field.name)
        )

if account_profile_id_col in account_cols:
    account_values = accounts.select(
        F.col(f"`{account_profile_id_col}`").cast("string").alias("profile_id"),
        *[F.col(f"`{field.name}`") for field in value_fields],
    ).where("profile_id IS NOT NULL AND profile_id <> ''")
    aggregated = account_values.groupBy("profile_id").agg(*aggregations)
elif effective_account_id:
    if profile_accounts_col not in profiles.columns:
        raise ValueError(
            f"Profile table {profile_table_name!r} must contain {profile_accounts_col!r} "
            f"when account table {account_table_name!r} does not contain {account_profile_id_col!r}"
        )
    profile_account_bridge = (
        profiles.select(
            F.col(f"`{profile_id_col}`").cast("string").alias("profile_id"),
            F.explode(F.split(F.col(f"`{profile_accounts_col}`").cast("string"), ";")).alias("_account_id"),
        )
        .select("profile_id", F.trim(F.col("_account_id")).alias("account_id"))
        .where("profile_id IS NOT NULL AND profile_id <> '' AND account_id IS NOT NULL AND account_id <> ''")
    )
    account_values = accounts.select(
        F.col(f"`{effective_account_id}`").cast("string").alias("account_id"),
        *[F.col(f"`{field.name}`") for field in value_fields],
    ).where("account_id IS NOT NULL AND account_id <> ''")
    aggregated = (
        profile_account_bridge.alias("b")
        .join(account_values.alias("acct"), F.col("b.account_id") == F.col("acct.account_id"), "inner")
        .groupBy("profile_id")
        .agg(*aggregations)
    )
else:
    raise ValueError(
        f"Account table {account_table_name!r} must contain {account_profile_id_col!r}, "
        f"{account_id_col!r}, or account_id"
    )

account_attrs = F.map_from_arrays(
    F.array(*[F.lit(field.name) for field in value_fields]),
    F.array(*[F.col(f"`{field.name}`").cast("string") for field in value_fields]),
)

profile_account_features = aggregated.select(
    F.col("profile_id"),
    account_attrs.alias("account_attrs"),
    F.current_timestamp().alias("updated_at"),
)

output_table = resolve_table_name(profile_account_features_table_name)
(
    profile_account_features.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(output_table)
)

quoted_output_table = quote_table_name(profile_account_features_table_name)
spark.sql(f"ALTER TABLE {quoted_output_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

try:
    spark.sql(f"ALTER TABLE {quoted_output_table} CLUSTER BY (profile_id)")
except Exception as exc:
    print(f"Skipping liquid clustering setup for {quoted_output_table}: {exc}")

summary = {
    "profile_table_name": resolve_table_name(profile_table_name),
    "account_table_name": resolve_table_name(account_table_name),
    "segment_definitions_table_name": resolve_table_name(segment_definitions_table_name),
    "attribute_mapping_table_name": resolve_table_name(attribute_mapping_table_name),
    "profile_account_features_table_name": output_table,
    "required_account_rule_properties": len(account_feature_mapping),
    "account_feature_columns": len(value_fields),
    "segments_with_account_features": len(account_segment_ids),
    "profile_account_feature_rows": spark.table(output_table).count(),
}
print(json.dumps(summary, indent=2))
