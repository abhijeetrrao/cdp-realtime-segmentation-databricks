# Databricks notebook source
# Shared helpers for the CDP segmentation simulation.

import json
import os
import sys
import time
from typing import Any

from pyspark.sql import functions as F


def _add_repo_src_to_path() -> None:
    candidates = []
    try:
        bundle_src = os.path.join(dbutils.widgets.get("bundle_file_path"), "src")
        candidates.append(bundle_src)
    except Exception:
        pass
    try:
        nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_nb_path = nb_path if nb_path.startswith("/Workspace/") else f"/Workspace{nb_path}"
        candidates.extend(
            [
                os.path.dirname(os.path.dirname(nb_path)),
                os.path.dirname(os.path.dirname(workspace_nb_path)),
            ]
        )
    except Exception:
        pass

    cwd = os.getcwd()
    candidates.extend(
        [
            cwd,
            os.path.join(cwd, "src"),
            os.path.abspath(os.path.join(cwd, "..")),
            os.path.abspath(os.path.join(cwd, "..", "src")),
        ]
    )

    for candidate in candidates:
        if os.path.isdir(os.path.join(candidate, "cdp_engine")) and candidate not in sys.path:
            sys.path.insert(0, candidate)


try:
    _add_repo_src_to_path()
except Exception:
    for candidate in ["..", "../src"]:
        if candidate not in sys.path:
            sys.path.append(candidate)

def widget(name: str, default: str) -> str:
    try:
        return dbutils.widgets.get(name)
    except Exception:
        try:
            dbutils.widgets.text(name, default)
            return dbutils.widgets.get(name)
        except Exception:
            return default


def cfg() -> dict[str, str]:
    return {
        "catalog": widget("catalog", "main"),
        "schema": widget("schema", "cdp_rt_sim"),
        "volume": widget("volume", "cdp_rt_volume"),
        "profile_count": widget("profile_count", "1000000"),
        "account_count": widget("account_count", "1000000"),
        "lakebase_endpoint": widget("lakebase_endpoint", "projects/REPLACE_ME/branches/production/endpoints/primary"),
        "lakebase_database": widget("lakebase_database", "databricks_postgres"),
        "eventhub_bootstrap": widget("eventhub_bootstrap", "REPLACE_ME.servicebus.windows.net:9093"),
        "eventhub_name": widget("eventhub_name", "tealium-events"),
        "eventhub_secret_scope": widget("eventhub_secret_scope", "cdp-rt"),
        "eventhub_secret_key": widget("eventhub_secret_key", "eventhub-connection-string"),
        "event_scale_fraction": widget("event_scale_fraction", "0.05"),
        "burst_multiplier": widget("burst_multiplier", "1.0"),
        "trigger_interval": widget("trigger_interval", "5 seconds"),
    }


def fq(c: dict[str, str], table: str) -> str:
    return f"`{c['catalog']}`.`{c['schema']}`.`{table}`"


def volume_path(c: dict[str, str], suffix: str = "") -> str:
    base = f"/Volumes/{c['catalog']}/{c['schema']}/{c['volume']}"
    return f"{base}/{suffix}" if suffix else base


def create_namespace(c: dict[str, str]) -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{c['catalog']}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{c['catalog']}`.`{c['schema']}`")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{c['catalog']}`.`{c['schema']}`.`{c['volume']}`")


def kafka_eventhub_options(c: dict[str, str]) -> dict[str, str]:
    from cdp_engine.eventhub import eventhub_kafka_options

    connection_string = dbutils.secrets.get(
        c["eventhub_secret_scope"],
        c["eventhub_secret_key"],
    )
    return eventhub_kafka_options(
        bootstrap_servers=c["eventhub_bootstrap"],
        eventhub_name=c["eventhub_name"],
        connection_string=connection_string,
    )


def rule_json(op: str, **kwargs: Any) -> str:
    out = {"op": op}
    out.update(kwargs)
    return json.dumps(out, separators=(",", ":"), sort_keys=True)


def now_ms() -> int:
    return int(time.time() * 1000)


def as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        return value.as_dict()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Cannot convert {type(value).__name__} to dict")


def resolve_lakebase_connection_info(endpoint_path: str, database: str) -> dict[str, str]:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    postgres = getattr(w, "postgres", None)
    if postgres is not None:
        endpoint = as_dict(postgres.get_endpoint(name=endpoint_path))
        credential = postgres.generate_database_credential(endpoint=endpoint_path)
        token = getattr(credential, "token", None) or credential["token"]
    else:
        endpoint = w.api_client.do(method="GET", path=f"/api/2.0/postgres/{endpoint_path}")
        credential = w.api_client.do(
            method="POST",
            path="/api/2.0/postgres/credentials",
            body={"endpoint": endpoint_path},
        )
        token = credential["token"]

    return {
        "host": endpoint["status"]["hosts"]["host"],
        "database": database,
        "user": w.current_user.me().user_name,
        "password": token,
    }


def get_lakebase_connection(endpoint_path: str, database: str):
    import psycopg2

    connection_info = resolve_lakebase_connection_info(endpoint_path, database)
    return psycopg2.connect(
        host=connection_info["host"],
        dbname=connection_info["database"],
        user=connection_info["user"],
        password=connection_info["password"],
        sslmode="require",
    )


def chunked(items: list[Any], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
