use super::*;

fn valid_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("websocket");
    config.set("url", "wss://one.example/events");
    config
}

#[test]
fn defaults_are_bounded_and_client_only() {
    let config = WebSocketSourceConfig::default();
    assert!(config.urls.is_empty());
    assert!(matches!(config.format, MessageFormat::Json));
    assert!(matches!(config.on_backpressure, WsBackpressure::Block));
    assert_eq!(config.max_message_size, 64 * 1024 * 1024);
    assert!(config.reconnect.enabled);
}

#[test]
fn parses_runtime_source_options() {
    let mut config = valid_config();
    config.set(
        "url",
        "wss://one.example/events, ws://two.example:8080/feed",
    );
    config.set("format", "csv");
    config.set("subscribe.message", r#"{"op":"subscribe"}"#);
    config.set("on.backpressure", "drop_newest");
    config.set("max.message.size", "4096");
    config.set("reconnect.enabled", "false");
    config.set("reconnect.initial.delay.ms", "200");
    config.set("reconnect.max.delay.ms", "5000");
    config.set("reconnect.max.retries", "7");

    let parsed = WebSocketSourceConfig::from_config(&config).unwrap();
    assert_eq!(parsed.urls.len(), 2);
    assert!(matches!(parsed.format, MessageFormat::Csv { .. }));
    assert!(matches!(parsed.on_backpressure, WsBackpressure::DropNewest));
    assert_eq!(parsed.max_message_size, 4096);
    assert!(!parsed.reconnect.enabled);
    assert_eq!(parsed.reconnect.max_retries, Some(7));
    assert_eq!(
        parsed.subscribe_message.as_deref(),
        Some(r#"{"op":"subscribe"}"#)
    );
}

#[test]
fn rejects_missing_or_invalid_urls() {
    let missing = ConnectorConfig::new("websocket");
    assert!(WebSocketSourceConfig::from_config(&missing).is_err());

    for value in ["", "https://example.test/events", "wss://ok.example,"] {
        let mut config = ConnectorConfig::new("websocket");
        config.set("url", value);
        assert!(
            WebSocketSourceConfig::from_config(&config).is_err(),
            "accepted {value:?}"
        );
    }
}

#[test]
fn rejects_invalid_limits_and_enums() {
    for (key, value) in [
        ("format", "jsonlines"),
        ("on.backpressure", "drop"),
        ("on.backpressure", "drop_oldest"),
        ("max.message.size", "0"),
        ("max.message.size", "67108865"),
        ("reconnect.initial.delay.ms", "0"),
    ] {
        let mut config = valid_config();
        config.set(key, value);
        assert!(
            WebSocketSourceConfig::from_config(&config).is_err(),
            "accepted {key}={value}"
        );
    }

    let mut inverted = valid_config();
    inverted.set("reconnect.initial.delay.ms", "200");
    inverted.set("reconnect.max.delay.ms", "100");
    assert!(WebSocketSourceConfig::from_config(&inverted).is_err());
}

#[test]
fn rejects_removed_event_time_and_server_options() {
    for key in [
        "event.time.field",
        "event.time.format",
        "mode",
        "bind.address",
        "max.connections",
        "path",
        "ping.interval.ms",
        "ping.timeout.ms",
        "auth.type",
        "auth.token",
        "auth.username",
        "auth.password",
        "auth.api.key",
        "auth.secret",
    ] {
        let mut config = valid_config();
        config.set(key, "removed");
        let error = WebSocketSourceConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{error}");
    }
}

#[test]
fn accepts_engine_and_json_decoder_options() {
    let mut config = valid_config();
    config.set("_arrow_schema", "engine-injected");
    config.set("laminar.source.name", "orders");
    config.set("json.path", "payload");
    config.set("json.column.ts", "metadata.timestamp");
    config.set("json.column.ts.epoch_unit", "micros");
    config.set("json.explode", "id,value");
    config.set("schema.enforcement", "strict");
    config.set("nested.as.jsonb", "true");

    WebSocketSourceConfig::from_config(&config).unwrap();
}

#[test]
fn rejects_unknown_and_malformed_json_column_options() {
    for key in [
        "max.message.szie",
        "json.colum.ts",
        "json.column.",
        "json.column.ts.epcoh_unit",
    ] {
        let mut config = valid_config();
        config.set(key, "1");

        let error = WebSocketSourceConfig::from_config(&config)
            .unwrap_err()
            .to_string();
        assert!(error.contains(key), "{error}");
    }
}

#[test]
fn rejects_json_options_for_csv_and_binary() {
    for format in ["csv", "binary"] {
        for key in [
            "json.path",
            "json.column.ts",
            "json.column.ts.epoch_unit",
            "json.explode",
            "schema.enforcement",
            "nested.as.jsonb",
        ] {
            let mut config = valid_config();
            config.set("format", format);
            config.set(key, "value");
            let error = WebSocketSourceConfig::from_config(&config)
                .unwrap_err()
                .to_string();
            assert!(error.contains(key), "{error}");
            assert!(error.contains("format = 'json'"), "{error}");
        }
    }
}
