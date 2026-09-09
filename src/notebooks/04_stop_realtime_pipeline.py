# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import time

from databricks.sdk import WorkspaceClient

pipeline_id = widget("pipeline_id", "")
delay_seconds = int(widget("stop_delay_seconds", "120"))
if not pipeline_id:
    raise ValueError("pipeline_id is required")

if delay_seconds > 0:
    time.sleep(delay_seconds)

w = WorkspaceClient()
try:
    w.pipelines.stop(pipeline_id=pipeline_id)
except Exception:
    w.api_client.do(method="POST", path=f"/api/2.0/pipelines/{pipeline_id}/stop", body={})

print(json.dumps({"pipeline_id": pipeline_id, "stopped": True, "delay_seconds": delay_seconds}, indent=2))
