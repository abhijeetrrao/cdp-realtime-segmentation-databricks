# CDP Real-Time Segmentation on Databricks

This repository contains a deployable Databricks Asset Bundle for a real-time CDP segmentation sizing test. It simulates a Tealium-like event stream, evaluates realtime segment membership in Spark/SDP, maintains current membership state in Delta, and serves the current state from Databricks Lakebase/Postgres.

The implementation is intended for a customer-site deployment where we need to prove the end-to-end shape quickly:

```text
Tealium listener Delta table -> Databricks SDP -> Delta current membership -> Lakebase synced table -> serving reads
```

## Current Architecture

The current optimized path is the SDP pipeline in `src/pipelines/realtime/realtime_segmentation_pipeline.py`.

1. Attribute-change events are read from a physical Delta table with `spark.readStream.table(...)`.
2. The source table is expected to contain `profile_id`, event time, event ID, `changed_properties`, and the listener/behavioral attributes being updated.
3. Rows with no realtime-relevant changed properties are filtered out.
4. Realtime segment definitions are read from Delta.
5. The row's `changed_properties` are used as a reverse-index trigger to identify candidate segments.
6. Candidate segment rows are joined with profile attributes from Delta.
7. Account-sourced rule properties are aggregated to profile level before evaluation. The customer profile table's `accounts` column is treated as a semicolon-delimited list of account IDs. Numeric comparison fields are summed across those accounts; string/date-style account fields are collected so `contains`/date predicates can match any account value.
8. Spark evaluates the stateless nested boolean rule:

   ```text
   listener/behavioral conditions AND profile conditions AND aggregated account conditions
   ```

9. Auto CDC / SCD Type 1 maintains the latest current membership per:

   ```text
   profile_id, segment_id, path
   ```

10. The current membership table is synced to Lakebase/Postgres for keyed serving reads.

The qualification computation is stateless. The pipeline does not keep session windows or cross-event state. The only stateful piece is the current-state table that stores the latest membership flag for each profile/segment/path key.

## Data Model

Delta/Unity Catalog tables:

| Table | Purpose |
| --- | --- |
| `profile_attributes_delta` | Synthetic wide profile table. The generated schema contains deterministic profile, account, and filler attributes so the table behaves like a wide customer profile record. |
| `account_attributes_delta` | Synthetic account attributes. Profiles reference accounts by `account_id`. |
| `segment_definitions_delta` | Segment definitions as JSON rules. Each row includes `segment_id`, `mode`, `rule_json`, and `referenced_properties`. |
| `segment_attribute_mapping` | Maps customer rule attribute names to physical Databricks columns and source tables. |
| `segment_reverse_index_delta` | Precomputed `property_name -> segment_ids` fanout table for realtime rules. Useful for observability and for implementations that explicitly join through a reverse index. |
| customer listener table | Physical Delta table produced by the Tealium listener process. Configure through `source_event_table_name`. This replaces direct Event Hub reads for the customer flow. |
| `tealium_eventhub_events_<run_id>` | SDP streaming table containing parsed and filtered Event Hub events for a sizing run. |
| `evaluated_realtime_memberships_<run_id>` | SDP table with one evaluated row per event/profile/candidate segment. |
| `membership_flags_current_<run_id>` | Auto CDC current-state table. This is the table intended to sync into Lakebase. |

Lakebase/Postgres tables:

| Table | Purpose |
| --- | --- |
| `cdp_rt.profile_attributes` | Serving copy of profile attributes for the older direct-write notebook path. |
| `cdp_rt.segment_definitions` | Serving copy of segment definitions for the older direct-write notebook path. |
| `cdp_rt.segment_reverse_index` | Serving copy of the reverse index for the older direct-write notebook path and debugging. |
| `cdp_rt.membership_flags` | Direct-write membership table used by the older notebook path and batch segment demo. |
| `cdp_rt.membership_flags_current_<run_id>` | Lakebase synced-table target for current realtime memberships, if configured as a Lakebase sync table. |
| `cdp_rt.rt_batch_metrics` | Metrics emitted by the older direct-write notebook path. |
| `cdp_rt.read_path_metrics` | Read-load metrics written by the serving read test. |

## Reverse Index

The reverse index is created in `src/notebooks/01_generate_delta.py`.

For each realtime segment, the generator stores every property referenced by the rule in `referenced_properties`. The reverse-index table is then built with:

```python
reverse_index = (
    seg_df.where("mode = 'realtime'")
    .select(F.explode("referenced_properties").alias("property_name"), "segment_id")
    .groupBy("property_name")
    .agg(F.collect_set("segment_id").alias("segment_ids"), F.count("*").alias("fanout"))
)
```

In production, keep this as a first-class Delta table. It answers:

```text
When property X changes, which segment IDs might need re-evaluation?
```

The current SDP path implements the same idea inline by extracting each realtime rule's event property and joining:

```python
events.join(
    F.broadcast(segments),
    F.array_contains(F.col("e.changed_properties"), F.col("s.event_property")),
)
```

For a customer deployment with many behavioral attributes, the recommended production shape is:

```text
events
  -> explode changed_properties
  -> join segment_reverse_index_delta on property_name
  -> explode segment_ids
  -> join segment_definitions_delta by segment_id
  -> evaluate rules
```

That avoids reparsing rules in the hot path and makes fanout directly observable.

## Attribute Mapping Table

Customer rules can keep customer/CDP attribute names such as `AZ_A_NetRev13Week`, `AZ_C_EmailAddress`, and `DL_C_PageCountryCode`. The pipeline resolves those names through a Delta mapping table before reading physical Databricks columns.

Expected schema:

```sql
CREATE TABLE IF NOT EXISTS cdp_prd.aap_processed_data.segment_attribute_mapping (
  rule_property STRING NOT NULL,
  source STRING NOT NULL,
  column_name STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
TBLPROPERTIES (
  delta.enableChangeDataFeed = true
);
```

Configure the table through:

```text
attribute_mapping_table_name = cdp_prd.aap_processed_data.segment_attribute_mapping
```

Example mapping rows:

| rule_property | source | column_name |
| --- | --- |
| `DL_C_PageCountryCode` | `cdp_prd.aap_processed_data.<listener_attribute_changes_table>` | `page_country_code` |
| `AZ_C_EmailAddress` | `cdp_prd.aap_processed_data.segments_aap_profiles` | `email_address` |
| `AZ_A_NetRev13Week` | `cdp_prd.aap_processed_data.segments_aap_accounts` | `net_rev_13_week` |

Mapping semantics:

| Mapping source table | Runtime behavior |
| --- | --- |
| Same as `source_event_table_name` | Read `column_name` from the streaming listener Delta table. The reverse-index trigger accepts either `rule_property` or `column_name` in `changed_properties`. |
| Same as `profile_table_name` | Read `column_name` from the profile table. |
| Same as `account_table_name` | Read `column_name` from the account table, join through the profile table's semicolon-delimited `accounts` list to `account_group_id`, and aggregate to profile level before evaluation. |

The table names in `source` are compared to the configured pipeline table names. For the customer deployment, make sure these match:

```text
source_event_table_name = cdp_prd.aap_processed_data.<listener_attribute_changes_table>
profile_table_name = cdp_prd.aap_processed_data.segments_aap_profiles
account_table_name = cdp_prd.aap_processed_data.segments_aap_accounts
```

If a rule property is missing from the mapping table, the pipeline falls back to naming conventions:

```text
AZ_A_* -> ACCOUNT
DL_* or beh_* -> EVENT
everything else -> PROFILE
```

For compatibility, mapping `source` values of `EVENT`, `PROFILE`, `ACCOUNT`, and `ACCOUNTS` are still accepted, but the customer table should use physical Databricks table names.

## Repository Layout

```text
.
├── databricks.yml
├── resources/
│   ├── cdp_jobs.yml
│   └── cdp_pipeline.yml
├── src/
│   ├── cdp_engine/
│   ├── notebooks/
│   └── pipelines/
├── scripts/
├── tests/
└── docs/
```

## Code Map

### Bundle and Resource Files

| File | What it does |
| --- | --- |
| `databricks.yml` | Main Databricks Asset Bundle definition. Contains variables for catalog, schema, volume, Lakebase endpoint, Event Hub settings, and target workspace. Replace the `REPLACE_ME` defaults or pass variables at deploy/run time. |
| `resources/cdp_pipeline.yml` | Defines the serverless continuous SDP pipeline. It points at `src/pipelines/realtime/**` and passes Event Hub and Lakebase configuration into the pipeline. |
| `resources/cdp_jobs.yml` | Defines setup, sizing, and legacy performance jobs. The main job for the current SDP sizing flow is `cdp_sdp_lakebase_sizing`. |

### Current SDP Pipeline

| File | What it does |
| --- | --- |
| `src/pipelines/realtime/realtime_segmentation_pipeline.py` | Current realtime segmentation pipeline. Streams from the configured Delta listener table, filters actionable rows, evaluates nested customer segment rules in Spark, and uses Auto CDC SCD Type 1 to maintain current membership. This is the main customer-demo path. |

Key functions/tables in the SDP pipeline:

| Object | Purpose |
| --- | --- |
| rule trigger properties | Non-account properties extracted from realtime segment rules. These are matched against `changed_properties` to decide which segments to evaluate. |
| `EVENT_SCHEMA` | Schema used to parse Event Hub messages. |
| `tealium_eventhub_events()` | Streaming table reading the configured Delta listener table, normalizing the event contract, filtering invalid rows, filtering rows whose `changed_properties` have no realtime rule overlap, and optionally filtering to a `run_id`. Event Hub remains only as a synthetic fallback when `source_event_table_name` is empty. |
| `evaluated_realtime_memberships()` | Joins filtered rows to candidate realtime segment rules, profile attributes, and aggregated account attributes, evaluates the nested rule, and emits one row per evaluated candidate membership. |
| `membership_flags_current_<run_id>` | Auto CDC target table holding the current latest membership state. |
| `dp.create_auto_cdc_flow(...)` | Maintains SCD Type 1 current membership keyed by `profile_id`, `segment_id`, and `path`. |

### Setup and Data Generation Notebooks

| File | What it does |
| --- | --- |
| `src/notebooks/00_common.py` | Shared notebook utilities for widgets, config, Unity Catalog names, volume paths, Event Hub Kafka options, and Lakebase credentials. |
| `src/notebooks/01_generate_delta.py` | Creates synthetic accounts, profiles, realtime segment definitions, batch segment definitions, and the Delta reverse-index table. Use this for synthetic sandbox runs. |
| `src/notebooks/01_load_segment_rules.py` | Loads customer segment rules from a CSV into `segment_definitions_delta` and rebuilds `segment_reverse_index_delta`. Use this for the customer rules CSV. |
| `src/notebooks/02_lakebase_init_and_sync.py` | Creates Lakebase/Postgres schemas and tables, then syncs segment definitions, reverse index, and a configurable profile subset into Lakebase. Required for legacy direct-write tests and useful for debugging. |
| `src/notebooks/03_eventhub_generator.py` | Generates synthetic Tealium-like events and sends them to Azure Event Hub. The event payload includes behavioral properties and `changed_properties`. |
| `src/notebooks/05_batch_segments.py` | Demonstrates batch segment evaluation and writes batch membership flags into Lakebase. This is separate from realtime SDP qualification. |

### Pipeline Control and Test Notebooks

| File | What it does |
| --- | --- |
| `src/notebooks/04_reset_realtime_run.py` | Clears metrics for a run, removes older staged rows, creates supporting Lakebase metrics tables, and removes the legacy notebook checkpoint path. |
| `src/notebooks/04_start_realtime_pipeline.py` | Starts the SDP pipeline. When passed a `sizing_run_id`, it rewrites pipeline config so the generated SDP table names are run-specific. |
| `src/notebooks/04_stop_realtime_pipeline.py` | Stops the SDP pipeline after an optional delay. |
| `src/notebooks/04_wait_for_rt_metrics.py` | Waits until enough realtime metrics have landed for reporting. Mostly applies to the older notebook path. |
| `src/notebooks/04_lakebase_read_load.py` | Runs concurrent Lakebase serving reads against the synced current membership table and writes latency/error metrics. |
| `src/notebooks/06_read_profile_segments.py` | Smoke test for serving reads: reads memberships for a sample profile from Lakebase. |
| `src/notebooks/07_sizing_report.py` | Produces sizing/cost/fanout summaries from metrics. |
| `src/notebooks/09_eventhub_spark_probe.py` | Debug probe for Event Hub. Reads raw Kafka offsets, parses events, filters by run ID, and reports how many messages pass the realtime changed-property filter. |

### Legacy / Optional Notebook Path

| File | What it does |
| --- | --- |
| `src/notebooks/04_realtime_segmentation_stream.py` | Older bounded Spark Structured Streaming notebook. It performs direct Lakebase reads/writes inside `foreachBatch`. Keep this for comparison/debugging, but do not use it as the primary customer demo path. |
| `src/notebooks/08_changed_memberships_native_lakebase_sink.py` | Optional native Lakebase streaming sink pattern for a `changed_memberships_delta` table. This is not the current primary path. |

### Python Helper Package

| File | What it does |
| --- | --- |
| `src/cdp_engine/eventhub.py` | Parses Event Hub connection strings and builds Kafka SASL/SSL options for Spark. |
| `src/cdp_engine/lakebase.py` | Resolves Lakebase endpoint host and generated database credentials using the Databricks SDK, then opens psycopg2 connections. |
| `src/cdp_engine/rules.py` | Local rule helpers: identify referenced properties, evaluate JSON rules in Python, and translate rules to Spark SQL-compatible predicates for batch demos. |
| `src/cdp_engine/__init__.py` | Exposes rule helpers from the package. |

### Scripts and Tests

| File | What it does |
| --- | --- |
| `scripts/create_eventhub.sh` | Helper script to create Azure Event Hub infrastructure. Review and adapt before customer use. |
| `tests/test_eventhub.py` | Unit tests for Event Hub connection-string and Kafka-option generation. |
| `tests/test_rules.py` | Unit tests for rule parsing/evaluation helpers. |

## Customer Deployment Checklist

Prerequisites:

- Databricks CLI authenticated to the customer workspace.
- A Unity Catalog catalog/schema where the demo can create tables and a volume.
- A Lakebase project, branch, database, and endpoint.
- Azure Event Hub namespace and event hub with Kafka endpoint enabled.
- A Databricks secret containing the Event Hub connection string.
- Permission to create/run serverless Jobs and serverless SDP pipelines.
- Permission to create Lakebase synced tables from the Delta current membership table.

Configure these bundle variables before deployment:

| Variable | Example |
| --- | --- |
| `catalog` | `main` |
| `schema` | `cdp_rt_sim` |
| `volume` | `cdp_rt_volume` |
| `lakebase_endpoint` | `projects/<project>/branches/<branch>/endpoints/<endpoint>` |
| `lakebase_database` | `databricks_postgres` |
| `eventhub_bootstrap` | `<namespace>.servicebus.windows.net:9093` |
| `eventhub_name` | `tealium-events` |
| `eventhub_secret_scope` | `cdp-rt` |
| `eventhub_secret_key` | `eventhub-connection-string` |
| `source_event_table_name` | `cdp_prd.aap_processed_data.<listener_attribute_changes_table>` |
| `profile_table_name` | `cdp_prd.aap_processed_data.segments_aap_profiles` |
| `profile_id_col` | `profile_id` |
| `profile_accounts_col` | `accounts` |
| `account_table_name` | `cdp_prd.aap_processed_data.segments_aap_accounts` |
| `account_profile_id_col` | `profile_id` |
| `account_id_col` | `account_group_id` |
| `segment_definitions_table_name` | `<catalog>.<schema>.segment_definitions_delta` or `segment_definitions_delta` |
| `attribute_mapping_table_name` | `<catalog>.<schema>.segment_attribute_mapping` or `segment_attribute_mapping` |

Update `databricks.yml` for the target workspace host, or pass `--profile` with a profile whose host points to the customer workspace.

## Deployment Commands

Authenticate:

```bash
databricks auth login --profile customer --host https://<workspace-host>
```

Validate the bundle:

```bash
databricks bundle validate -t dev --profile customer
```

Deploy:

```bash
databricks bundle deploy -t dev --profile customer
```

Run setup:

```bash
databricks bundle run cdp_realtime_segmentation_setup -t dev --profile customer
```

Load customer segment rules from a CSV staged in a UC volume:

```bash
databricks bundle run cdp_load_customer_segment_rules -t dev --profile customer
```

Run the current SDP + Lakebase sizing flow:

```bash
databricks bundle run cdp_sdp_lakebase_sizing -t dev --profile customer
```

For a custom sizing run, override parameters from the Databricks Jobs UI or by editing `resources/cdp_jobs.yml` before deployment:

| Parameter | Meaning |
| --- | --- |
| `sizing_run_id` | Run identifier. Also used as a suffix for run-specific SDP table names. Use only letters, numbers, and underscores. |
| `sizing_duration_seconds` | How long the generator/read-load jobs run. |
| `event_scale_fraction` | Fraction of average production volume to generate. |
| `burst_multiplier` | Multiplier on top of average production event rate. |
| `read_qps` | Lakebase serving read QPS. If `0`, it uses generated event-rate settings. |
| `read_concurrency` | Number of concurrent Lakebase read workers. |

## Lakebase Synced Table Setup

The SDP pipeline creates the Delta current membership table:

```text
<catalog>.<schema>.membership_flags_current_<sizing_run_id>
```

Create or configure a Lakebase synced table from that Delta table to a Postgres table such as:

```text
cdp_rt.membership_flags_current_<sizing_run_id>
```

Use `profile_id`, `segment_id`, and `path` as the logical serving key. The read-load notebook expects this table naming pattern:

```text
cdp_rt.membership_flags_current_<sizing_run_id>
```

## Event Payload Contract

For the customer flow, the SDP pipeline reads a Delta table rather than Event Hub. The source table should contain these logical fields, with configurable column names:

| Logical field | Default column | Purpose |
| --- | --- | --- |
| Profile key | `profile_id` | Profile-level evaluation key. |
| Event/update ID | `event_id` | Stable ID for ordering/tie-breaking and lineage. |
| Event/update timestamp | `event_ts` | Milliseconds since epoch or timestamp/string parseable by Spark. |
| Changed properties | `changed_properties` | `ARRAY<STRING>` or JSON string array naming attributes changed by the listener. |
| Listener attributes | property names from rules | Values such as `DL_C_LastShippingCompleted` or `DL_C_PageCountryCode`. |

The old Event Hub synthetic harness used JSON messages shaped like:

```json
{
  "event_id": "uuid",
  "run_id": "sdp_clean_001",
  "profile_id": "prof_000000000123",
  "event_ts": 1788233105000,
  "page_url": "/tracking",
  "beh_page_type": "tracking",
  "beh_intent": "track",
  "beh_device": "mobile",
  "beh_referrer_class": "search",
  "changed_properties": ["beh_intent", "noise_campaign_id"]
}
```

Only rows with `event_id`, `profile_id`, and overlap between `changed_properties` and rule trigger properties enter the evaluation path. If the source table does not contain `changed_properties`, the pipeline treats each row as potentially changing every trigger property, which is useful for smoke tests but not recommended for production sizing.

## Segment Rule Contract

Segment definitions are loaded into:

```text
<catalog>.<schema>.segment_definitions_delta
```

The rule JSON can be nested. Example:

```json
{
  "op": "and",
  "rules": [
    {"op": "eq", "property": "beh_intent", "value": "ship"},
    {"op": "eq", "property": "acct_region", "value": "na"}
  ]
}
```

The current Spark evaluator supports nested:

```text
and, or, not
```

Supported leaf operators in the Spark path:

```text
exists, contains, not_contains, eq, neq, gt, gte, lt, lte, between, after, within_last, within_next
```

Property source handling:

| Rule property | Source behavior |
| --- | --- |
| Mapped `source` equals `source_event_table_name` | Read the mapped `column_name` from the streaming listener/source table. |
| Mapped `source` equals `profile_table_name` | Read the mapped `column_name` from `cdp_prd.aap_processed_data.segments_aap_profiles`. |
| Mapped `source` equals `account_table_name` | Read the mapped `column_name` from `cdp_prd.aap_processed_data.segments_aap_accounts`, bridge through profile `accounts`, and aggregate to profile level. |
| Unmapped `source: "ACCOUNTS"` or `AZ_A_*` | Fallback to account table using the rule property as the physical column name. |
| Unmapped `DL_*` or `beh_*` | Fallback to event/source table using the rule property as the physical column name. |
| Other unmapped properties | Fallback to profile table using the rule property as the physical column name. |

For the customer sample, configure:

```text
profile_table_name = cdp_prd.aap_processed_data.segments_aap_profiles
account_table_name = cdp_prd.aap_processed_data.segments_aap_accounts
```

For the customer profile table, set `profile_accounts_col = accounts`. If `segments_aap_accounts` has a direct profile key, set `account_profile_id_col`; otherwise the pipeline joins `segments_aap_profiles.accounts` to `segments_aap_accounts.account_group_id` and aggregates account metrics across the matched accounts.

## Sizing Notes From the Stress Test

The high-fanout stress run measured approximately:

| Metric | Value |
| --- | ---: |
| Generated events | 2.841M |
| Accepted SDP events | 2.462M |
| Evaluated rows | 172.357M |
| Current/Postgres rows | 111.471M |
| Fanout per generated event | ~60.7x |
| Fanout per accepted/actionable event | ~70.0x |
| Total measured runtime cost | ~$7.49 |
| Generated-event unit cost | ~$2.64 / 1M generated events |
| Accepted-event unit cost | ~$3.04 / 1M accepted events |

Customer average-volume extrapolation from 27.5B hits over 13 months:

| Metric | Projection |
| --- | ---: |
| Average generated hits/month | ~2.115B |
| Average events/sec | ~804 |
| Accepted SDP events/month, using observed acceptance ratio | ~1.833B |
| Evaluated membership rows/month, using stress fanout | ~128.3B |
| Current membership rows/month, using stress ratio | ~83.0B |
| Runtime cost/month, generated basis | ~$5.6K |
| Runtime cost/month, accepted-all basis | ~$6.4K |

These projections assume the high-fanout stress model. If the real customer distribution has only about 50 behavioral traits materially affecting the 800 segments, fanout should be measured again with customer-like rules because evaluated/current/synced row volumes can change materially.

## Troubleshooting

No events in SDP table:

- Confirm Event Hub has messages.
- Run `09_eventhub_spark_probe.py` with the expected `sizing_run_id`.
- Confirm `run_id` in events matches the pipeline `event_run_id`.
- Confirm `changed_properties` includes at least one realtime behavioral property.

Events arrive but no memberships are evaluated:

- Check that `segment_definitions_delta` contains `mode = 'realtime'` rows.
- Check that realtime rules have non-account trigger properties present in the source table's `changed_properties`.
- Check that `profile_attributes_delta` contains matching `profile_id` values.

Membership current table updates but Lakebase is empty:

- Confirm the Lakebase synced table exists and targets `membership_flags_current_<sizing_run_id>`.
- Check sync-table lag and errors in the Lakebase UI.
- Confirm the read-load notebook is querying the same run-specific table name.

Read-load failures:

- Confirm the Lakebase endpoint is running and the bundle variable `lakebase_endpoint` is the full endpoint path.
- Confirm the caller has Lakebase database credentials.
- Confirm `cdp_rt.membership_flags_current_<sizing_run_id>` exists.

High latency:

- Check SDP update metrics for Event Hub read lag, evaluation lag, and Auto CDC current-state lag.
- Check Lakebase sync lag separately from SDP evaluation latency.
- Inspect fanout from `segment_reverse_index_delta`; high fanout directly increases evaluated rows.

## What Is Not Implemented

- Customer Tealium connector setup.
- Customer Adobe integration.
- Customer identity resolution.
- Consent/privacy policy enforcement.
- Segment authoring UI.
- Stateful/session-window segment logic.
- Arbitrary customer identity resolution between profile IDs and account IDs when no profile-account key exists.

Those are deliberate boundaries for the sizing test. Add them only after the realtime evaluation and Lakebase serving path are validated with customer-like event and segment distributions.
