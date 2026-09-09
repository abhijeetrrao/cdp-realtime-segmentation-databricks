from cdp_engine.eventhub import build_connection_string, eventhub_kafka_options


def test_build_connection_string_strips_secret_newline_without_entity_path():
    raw = (
        "Endpoint=sb://ns.servicebus.windows.net/;"
        "SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=abc=\n"
    )

    assert build_connection_string(raw) == (
        "Endpoint=sb://ns.servicebus.windows.net/;"
        "SharedAccessKeyName=RootManageSharedAccessKey;"
        "SharedAccessKey=abc="
    )


def test_eventhub_kafka_options_build_shaded_single_line_jaas():
    opts = eventhub_kafka_options(
        bootstrap_servers="ns.servicebus.windows.net:9093",
        eventhub_name="tealium-events",
        connection_string=(
            "Endpoint=sb://ns.servicebus.windows.net/;"
            "SharedAccessKeyName=RootManageSharedAccessKey;"
            "SharedAccessKey=abc=\n"
        ),
        starting_offsets="latest",
    )

    jaas = opts["kafka.sasl.jaas.config"]
    assert "\n" not in jaas
    assert "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required" in jaas
    assert 'username="$ConnectionString"' in jaas
    assert 'password="Endpoint=sb://ns.servicebus.windows.net/;' in jaas
    assert "EntityPath=" not in jaas
    assert opts["startingOffsets"] == "latest"
