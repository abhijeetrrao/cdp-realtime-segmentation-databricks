from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterable


def install_runtime_packages() -> None:
    try:
        import psycopg2  # noqa: F401
        from psycopg2.extras import RealDictCursor  # noqa: F401
    except Exception as exc:  # pragma: no cover - notebook runtime path
        raise RuntimeError(
            "Install psycopg2 in the Databricks cluster."
        ) from exc


def get_lakebase_connection(endpoint_path: str, database: str):
    return connect_lakebase(resolve_lakebase_connection_info(endpoint_path, database))


def resolve_lakebase_connection_info(endpoint_path: str, database: str) -> dict[str, str]:
    install_runtime_packages()
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    endpoint = _get_endpoint(w, endpoint_path)
    token = _generate_database_credential(w, endpoint_path)
    user = w.current_user.me().user_name
    hosts = endpoint["status"]["hosts"]
    return {
        "host": hosts["host"],
        "database": database,
        "user": user,
        "password": token,
    }


def connect_lakebase(connection_info: dict[str, str]):
    install_runtime_packages()
    import psycopg2

    return psycopg2.connect(
        host=connection_info["host"],
        dbname=connection_info["database"],
        user=connection_info["user"],
        password=connection_info["password"],
        sslmode="require",
    )


def _get_endpoint(w: Any, endpoint_path: str) -> dict[str, Any]:
    postgres = getattr(w, "postgres", None)
    if postgres is not None:
        endpoint = postgres.get_endpoint(name=endpoint_path)
        return _as_dict(endpoint)

    return w.api_client.do(
        method="GET",
        path=f"/api/2.0/postgres/{endpoint_path}",
    )


def _generate_database_credential(w: Any, endpoint_path: str) -> str:
    postgres = getattr(w, "postgres", None)
    if postgres is not None:
        credential = postgres.generate_database_credential(endpoint=endpoint_path)
        return getattr(credential, "token", None) or credential["token"]

    credential = w.api_client.do(
        method="POST",
        path="/api/2.0/postgres/credentials",
        body={"endpoint": endpoint_path},
    )
    return credential["token"]


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "as_dict"):
        return value.as_dict()
    if hasattr(value, "to_dict"):
        return value.to_dict()
    raise TypeError(f"Cannot convert {type(value).__name__} to dict")


@contextmanager
def lakebase_cursor(endpoint_path: str, database: str, row_factory=None):
    conn = get_lakebase_connection(endpoint_path, database)
    try:
        if row_factory is None:
            cursor_kwargs = {}
        else:
            from psycopg2.extras import RealDictCursor

            cursor_kwargs = {"cursor_factory": RealDictCursor}
        with conn.cursor(**cursor_kwargs) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def chunked(items: list[Any], size: int) -> Iterable[list[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
