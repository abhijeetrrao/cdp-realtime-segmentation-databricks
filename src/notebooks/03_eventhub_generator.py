# Databricks notebook source
# MAGIC %pip install -q azure-eventhub

# COMMAND ----------

# MAGIC %run ./00_common

# COMMAND ----------

import json
import random
import time
import uuid

from azure.eventhub import EventData, EventHubProducerClient

c = cfg()
connection_string = dbutils.secrets.get(c["eventhub_secret_scope"], c["eventhub_secret_key"]).strip()
profile_count = int(c["profile_count"])

reference_average_eps = 815.0
target_eps = reference_average_eps * float(c["event_scale_fraction"]) * float(c["burst_multiplier"])
duration_seconds = int(widget("generator_duration_seconds", "180"))
batch_size = int(widget("generator_send_batch_size", "100"))
initial_sleep_seconds = int(widget("generator_initial_sleep_seconds", "30"))
throttle_backoff_seconds = float(widget("generator_throttle_backoff_seconds", "4"))
sleep_seconds = batch_size / max(target_eps, 1.0)
sizing_run_id = widget("sizing_run_id", f"generator_{int(time.time())}")

page_types = ["home", "pricing", "tracking", "returns", "support", "pickup"]
intents = ["ship", "track", "return", "pay", "browse"]
devices = ["desktop", "mobile"]
referrers = ["search", "direct", "campaign", "partner", "unknown"]

producer = EventHubProducerClient.from_connection_string(connection_string, eventhub_name=c["eventhub_name"])
sent = 0
throttles = 0
start = time.time()
try:
    if initial_sleep_seconds > 0:
        time.sleep(initial_sleep_seconds)
    while time.time() - start < duration_seconds:
        batch = producer.create_batch()
        for _ in range(batch_size):
            pid_num = random.randrange(profile_count)
            event = {
                "event_id": str(uuid.uuid4()),
                "run_id": sizing_run_id,
                "profile_id": f"prof_{pid_num:012d}",
                "event_ts": int(time.time() * 1000),
                "page_url": f"/{random.choice(page_types)}",
                "beh_page_type": random.choice(page_types),
                "beh_intent": random.choice(intents),
                "beh_device": random.choice(devices),
                "beh_referrer_class": random.choice(referrers),
                "changed_properties": random.sample(
                    ["beh_page_type", "beh_intent", "beh_device", "beh_referrer_class", "noise_scroll_depth", "noise_campaign_id"],
                    random.randint(1, 3),
                ),
            }
            batch.add(EventData(json.dumps(event, separators=(",", ":"))))
        try:
            producer.send_batch(batch)
            sent += batch_size
        except Exception as exc:
            message = str(exc).lower()
            if "server-busy" not in message and "throttl" not in message:
                raise
            throttles += 1
            time.sleep(throttle_backoff_seconds)
        time.sleep(sleep_seconds)
finally:
    producer.close()

result = {
    "run_id": sizing_run_id,
    "target_eps": target_eps,
    "duration_seconds": duration_seconds,
    "events_sent": sent,
    "throttles": throttles,
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
