from __future__ import annotations


def parse_connection_string(connection_string: str) -> dict[str, str]:
    """Parse an Azure Event Hubs connection string into key/value parts."""
    parts: dict[str, str] = {}
    for token in connection_string.strip().split(";"):
        token = token.strip()
        if not token:
            continue
        key, sep, value = token.partition("=")
        if not sep or not key:
            raise ValueError(f"Invalid Event Hub connection string token: {token!r}")
        parts[key] = value
    required = {"Endpoint", "SharedAccessKeyName", "SharedAccessKey"}
    missing = sorted(required - set(parts))
    if missing:
        raise ValueError(f"Event Hub connection string missing required key(s): {missing}")
    return parts


def build_connection_string(connection_string: str, *, entity_path: str | None = None) -> str:
    """Return a canonical single-line connection string for Kafka SASL/PLAIN."""
    parts = parse_connection_string(connection_string)
    ordered = [
        ("Endpoint", parts["Endpoint"].strip()),
        ("SharedAccessKeyName", parts["SharedAccessKeyName"].strip()),
        ("SharedAccessKey", parts["SharedAccessKey"].strip()),
    ]
    existing_entity_path = parts.get("EntityPath", "").strip()
    final_entity_path = existing_entity_path or (entity_path or "").strip()
    if final_entity_path:
        ordered.append(("EntityPath", final_entity_path))
    return ";".join(f"{key}={value}" for key, value in ordered)


def java_quote(value: str) -> str:
    """Escape only characters that are special inside a JAAS quoted string."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def eventhub_kafka_options(
    *,
    bootstrap_servers: str,
    eventhub_name: str,
    connection_string: str,
    starting_offsets: str | None = None,
    include_entity_path: bool = False,
) -> dict[str, str]:
    password = build_connection_string(
        connection_string,
        entity_path=eventhub_name if include_entity_path else None,
    )
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{java_quote(password)}";'
    )
    options = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": eventhub_name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
        "failOnDataLoss": "false",
    }
    if starting_offsets:
        options["startingOffsets"] = starting_offsets
    return options
