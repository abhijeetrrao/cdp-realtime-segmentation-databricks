# Databricks notebook source
# MAGIC %run ./00_common

# COMMAND ----------

import json
import re
import time

from databricks.sdk import WorkspaceClient

pipeline_id = widget("pipeline_id", "")
force_continuous = widget("force_continuous", "true").lower() == "true"
event_run_id = widget("event_run_id", "")
full_refresh = widget("full_refresh", "false").lower() == "true"
wait_until_running = widget("wait_until_running", "true").lower() == "true"
wait_timeout_seconds = int(widget("wait_timeout_seconds", "600"))
if not pipeline_id:
    raise ValueError("pipeline_id is required")

w = WorkspaceClient()

if force_continuous:
    pipeline = w.api_client.do(method="GET", path=f"/api/2.0/pipelines/{pipeline_id}")
    spec = pipeline["spec"]
    configuration = dict(spec.get("configuration", {}))
    if event_run_id:
        configuration["event_run_id"] = event_run_id
        flow_suffix = re.sub(r"[^A-Za-z0-9_]", "_", event_run_id)[-48:]
        configuration["sdp_event_table_name"] = f"tealium_eventhub_events_{flow_suffix}"
        configuration["sdp_membership_table_name"] = f"evaluated_realtime_memberships_{flow_suffix}"
        configuration["sdp_membership_current_table_name"] = f"membership_flags_current_{flow_suffix}"
        configuration["sdp_flow_name"] = f"qualify_tealium_events_{flow_suffix}"
    configuration.pop("max_offsets_per_trigger", None)

    update_body = {
        "name": spec["name"],
        "catalog": spec.get("catalog"),
        "schema": spec.get("schema"),
        "serverless": spec.get("serverless", True),
        "continuous": True,
        "channel": spec.get("channel", "PREVIEW"),
        "development": spec.get("development", True),
        "edition": spec.get("edition", "ADVANCED"),
        "libraries": spec.get("libraries", []),
        "environment": spec.get("environment"),
        "configuration": configuration,
    }
    w.api_client.do(
        method="PUT",
        path=f"/api/2.0/pipelines/{pipeline_id}",
        body={key: value for key, value in update_body.items() if value is not None},
    )

try:
    update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
    update_id = update.update_id
except Exception as exc:
    message = str(exc)
    active_update = re.search(r"active update '([^']+)'", message)
    if active_update:
        update_id = active_update.group(1)
    else:
        try:
            response = w.api_client.do(
                method="POST",
                path=f"/api/2.0/pipelines/{pipeline_id}/updates",
                body={"full_refresh": full_refresh},
            )
            update_id = response["update_id"]
        except Exception as fallback_exc:
            fallback_message = str(fallback_exc)
            active_update = re.search(r"active update '([^']+)'", fallback_message)
            if not active_update:
                raise
            update_id = active_update.group(1)

last_state = None
if wait_until_running:
    deadline = time.time() + wait_timeout_seconds
    terminal_failures = {"FAILED", "CANCELED"}
    while time.time() < deadline:
        update_state = w.api_client.do(
            method="GET",
            path=f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}",
        )
        update = update_state.get("update", update_state)
        last_state = update.get("state")
        if last_state == "RUNNING":
            break
        if last_state in terminal_failures:
            raise RuntimeError(f"Pipeline update {update_id} ended in {last_state}")
        time.sleep(10)
    if last_state != "RUNNING":
        raise TimeoutError(f"Pipeline update {update_id} did not reach RUNNING within {wait_timeout_seconds}s; last_state={last_state}")

print(
    json.dumps(
        {
            "pipeline_id": pipeline_id,
            "update_id": update_id,
            "continuous": force_continuous,
            "event_run_id": event_run_id,
            "sdp_event_table_name": configuration.get("sdp_event_table_name"),
            "sdp_membership_table_name": configuration.get("sdp_membership_table_name"),
            "sdp_membership_current_table_name": configuration.get("sdp_membership_current_table_name"),
            "sdp_flow_name": configuration.get("sdp_flow_name"),
            "full_refresh": full_refresh,
            "waited_until_running": wait_until_running,
            "last_state": last_state,
        },
        indent=2,
    )
)
