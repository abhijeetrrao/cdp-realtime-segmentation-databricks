# Databricks notebook source
# Optional pattern for DBR 18.3+ native Lakebase streaming sink.
# The default stream writes directly from the controlled foreachBatch hot path.
# Use this only if you first change the hot path to append changed rows to Delta.

# MAGIC %run ./00_common

# COMMAND ----------

c = cfg()
source_table = fq(c, "changed_memberships_delta")
checkpoint = volume_path(c, "checkpoints/native_lakebase_membership_sink")

(
    spark.readStream.table(source_table)
    .writeStream
    .format("postgresql")
    .outputMode("append")
    .option("endpoint", c["lakebase_endpoint"].replace("projects/", "").replace("/branches/", ".").replace("/endpoints/", "."))
    .option("database", c["lakebase_database"])
    .option("dbtable", "cdp_rt.membership_flags")
    .option("upsertkey", "profile_id,segment_id,path")
    .option("batchsize", "1000")
    .option("batchinterval", "100 milliseconds")
    .option("checkpointLocation", checkpoint)
    .trigger(realTime="5 minutes")
    .start()
)
