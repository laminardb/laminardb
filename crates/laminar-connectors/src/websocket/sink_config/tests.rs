use super::*;

#[test]
fn parses_server_runtime_options() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("bind.address", "127.0.0.1:8080");
    config.set("max.connections", "2000");
    config.set("ping.interval.ms", "5000");
    config.set("ping.timeout.ms", "1000");

    let parsed = WebSocketSinkConfig::from_config(&config).unwrap();
    assert!(matches!(
        parsed,
        WebSocketSinkConfig::Server {
            max_connections: 2000,
            ..
        }
    ));
}

#[test]
fn parses_client_runtime_options() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("mode", "client");
    config.set("url", "wss://sink.example/events");

    assert!(matches!(
        WebSocketSinkConfig::from_config(&config).unwrap(),
        WebSocketSinkConfig::Client { ref url } if url == "wss://sink.example/events"
    ));
}

#[test]
fn rejects_invalid_server_bounds() {
    let missing_bind = ConnectorConfig::new("websocket");
    assert!(WebSocketSinkConfig::from_config(&missing_bind).is_err());

    for (key, value) in [
        ("max.connections", (MAX_SERVER_CONNECTIONS + 1).to_string()),
        ("ping.interval.ms", "999".into()),
        ("ping.timeout.ms", "999".into()),
    ] {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "127.0.0.1:8080");
        config.set(key, value);
        let error = WebSocketSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(if key == "max.connections" {
                key
            } else {
                "ping"
            }),
            "{error}"
        );
    }
}

#[test]
fn rejects_invalid_client_url() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("mode", "client");
    config.set("url", "https://sink.example/events");
    assert!(WebSocketSinkConfig::from_config(&config).is_err());
}

#[test]
fn rejects_options_from_the_other_mode() {
    let mut server = ConnectorConfig::new("websocket");
    server.set("bind.address", "127.0.0.1:8080");
    server.set("url", "wss://sink.example/events");
    assert!(WebSocketSinkConfig::from_config(&server)
        .unwrap_err()
        .to_string()
        .contains("url"));

    for key in [
        "bind.address",
        "max.connections",
        "ping.interval.ms",
        "ping.timeout.ms",
    ] {
        let mut client = ConnectorConfig::new("websocket");
        client.set("mode", "client");
        client.set("url", "wss://sink.example/events");
        client.set(key, "1000");
        assert!(WebSocketSinkConfig::from_config(&client)
            .unwrap_err()
            .to_string()
            .contains(key));
    }
}

#[test]
fn rejects_removed_options() {
    for key in [
        "format",
        "reconnect.enabled",
        "per.client.buffer.bytes",
        "slow.client.policy",
        "replay.buffer.size",
        "buffer.on.disconnect",
        "path",
        "batch.max.size",
        "auth.token",
    ] {
        let mut config = ConnectorConfig::new("websocket");
        config.set("bind.address", "127.0.0.1:8080");
        config.set(key, "removed");
        assert!(WebSocketSinkConfig::from_config(&config)
            .unwrap_err()
            .to_string()
            .contains(key));
    }
}

#[test]
fn accepts_engine_options_and_rejects_typos() {
    let mut config = ConnectorConfig::new("websocket");
    config.set("bind.address", "127.0.0.1:8080");
    config.set("_arrow_schema", "engine-injected");
    config.set("delivery.guarantee", "at-least-once");
    config.set("sink.write.timeout.ms", "5000");
    WebSocketSinkConfig::from_config(&config).unwrap();

    config.set("max.conections", "1");
    assert!(WebSocketSinkConfig::from_config(&config)
        .unwrap_err()
        .to_string()
        .contains("max.conections"));
}

#[test]
fn typed_configuration_uses_the_same_validation() {
    let invalid = WebSocketSinkConfig::Server {
        bind_address: "127.0.0.1:8080".into(),
        max_connections: 0,
        ping_interval: Duration::from_secs(1),
        ping_timeout: Duration::from_secs(1),
    };
    assert!(invalid.validate().is_err());
}
