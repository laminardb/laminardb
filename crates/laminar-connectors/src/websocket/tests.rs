use super::*;
use arrow_schema::{DataType, Field, Schema};
use std::thread;

fn config_with_schema(mode: &str) -> crate::config::ConnectorConfig {
    let mut config = crate::config::ConnectorConfig::new("websocket");
    config.set("mode", mode);
    let schema = Schema::new(vec![Field::new("payload", DataType::Utf8, false)]);
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(&schema),
    );
    config
}

fn factory_error(registry: &ConnectorRegistry, config: &crate::config::ConnectorConfig) -> String {
    match registry.create_sink(config, None) {
        Ok(_) => panic!("expected sink factory error"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn sink_factory_dispatches_server_without_opening_a_socket() {
    let registry = ConnectorRegistry::new();
    register_websocket_sink(&registry).unwrap();
    let mut config = config_with_schema("server");
    config.set("bind.address", "127.0.0.1:0");

    let sink = registry.create_sink(&config, None).unwrap();

    assert!(sink.contract(&config).is_ok());
    assert_eq!(sink.schema().field(0).name(), "payload");
}

#[test]
fn sink_factory_dispatches_client_without_opening_a_socket() {
    let registry = ConnectorRegistry::new();
    register_websocket_sink(&registry).unwrap();
    let mut config = config_with_schema("client");
    config.set("url", "wss://example.test/events");

    let sink = registry.create_sink(&config, None).unwrap();

    assert!(sink.contract(&config).is_ok());
    assert_eq!(sink.schema().field(0).name(), "payload");
}

#[test]
fn sink_factory_rejects_missing_or_invalid_mode_specific_config() {
    let registry = ConnectorRegistry::new();
    register_websocket_sink(&registry).unwrap();

    let missing_server_bind = config_with_schema("server");
    assert!(factory_error(&registry, &missing_server_bind).contains("bind.address"));

    let mut invalid_server_bind = config_with_schema("server");
    invalid_server_bind.set("bind.address", "not-a-socket-address");
    assert!(factory_error(&registry, &invalid_server_bind).contains("invalid WebSocket server"));

    let mut missing_client_url = config_with_schema("client");
    assert!(factory_error(&registry, &missing_client_url).contains("url"));

    missing_client_url.set("url", "https://example.test/not-websocket");
    assert!(factory_error(&registry, &missing_client_url).contains("expected ws:// or wss://"));

    let mut invalid_schema = config_with_schema("server");
    invalid_schema.set("bind.address", "127.0.0.1:0");
    invalid_schema.set("_arrow_schema", "not-arrow-ipc");
    assert!(factory_error(&registry, &invalid_schema).contains("_arrow_schema"));

    let mut missing_schema = crate::config::ConnectorConfig::new("websocket");
    missing_schema.set("mode", "server");
    missing_schema.set("bind.address", "127.0.0.1:0");
    assert!(factory_error(&registry, &missing_schema).contains("declared Arrow schema"));
}

#[test]
fn bind_address_metadata_is_mode_conditional() {
    let registry = ConnectorRegistry::new();
    register_websocket_sink(&registry).unwrap();
    let info = registry.sink_info("websocket").unwrap();
    let bind = info
        .config_keys
        .iter()
        .find(|key| key.key == "bind.address")
        .unwrap();
    assert!(!bind.required);
}

#[test]
fn source_metadata_exposes_only_runtime_options() {
    let registry = ConnectorRegistry::new();
    register_websocket_source(&registry).unwrap();
    let info = registry.source_info("websocket").unwrap();
    let keys: std::collections::HashSet<&str> = info
        .config_keys
        .iter()
        .map(|key| key.key.as_str())
        .collect();
    let expected: std::collections::HashSet<&str> = [
        "url",
        "format",
        "subscribe.message",
        "reconnect.enabled",
        "reconnect.initial.delay.ms",
        "reconnect.max.delay.ms",
        "reconnect.max.retries",
        "on.backpressure",
        "max.message.size",
    ]
    .into_iter()
    .collect();
    assert_eq!(keys, expected);
}

#[test]
fn sink_metadata_exposes_only_runtime_options() {
    let registry = ConnectorRegistry::new();
    register_websocket_sink(&registry).unwrap();
    let info = registry.sink_info("websocket").unwrap();
    let keys: std::collections::HashSet<&str> = info
        .config_keys
        .iter()
        .map(|key| key.key.as_str())
        .collect();
    let expected: std::collections::HashSet<&str> = [
        "bind.address",
        "mode",
        "max.connections",
        "ping.interval.ms",
        "ping.timeout.ms",
        "url",
    ]
    .into_iter()
    .collect();
    assert_eq!(keys, expected);
}

#[test]
fn source_factory_registers_one_shared_metric_family() {
    let connectors = ConnectorRegistry::new();
    register_websocket_source(&connectors).unwrap();
    let metrics = Arc::new(prometheus::Registry::new());
    let config = crate::config::ConnectorConfig::new("websocket");

    connectors.create_source(&config, Some(&metrics)).unwrap();
    connectors.create_source(&config, Some(&metrics)).unwrap();

    let families = metrics.gather();
    assert_eq!(
        families
            .iter()
            .filter(|family| family.name().starts_with("websocket_source_"))
            .count(),
        5
    );
}

#[test]
fn metric_family_cache_is_concurrent_and_shared() {
    let cache = Arc::new(MetricFamilyCache::<WebSocketSourceMetrics>::default());
    let registry = Arc::new(prometheus::Registry::new());
    let workers = (0..16)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let registry = Arc::clone(&registry);
            thread::spawn(move || {
                let metrics = cache
                    .get_or_try_init(&registry, || WebSocketSourceMetrics::register(&registry))
                    .unwrap();
                metrics.record_message(1);
            })
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker.join().unwrap();
    }

    assert_eq!(
        cache.family.get().unwrap().metrics.messages_received.get(),
        16
    );
}

#[test]
fn source_factory_rejects_a_different_metrics_registry() {
    let connectors = ConnectorRegistry::new();
    register_websocket_source(&connectors).unwrap();
    let first = Arc::new(prometheus::Registry::new());
    let second = Arc::new(prometheus::Registry::new());
    let config = crate::config::ConnectorConfig::new("websocket");

    connectors.create_source(&config, Some(&first)).unwrap();
    let error = match connectors.create_source(&config, Some(&second)) {
        Ok(_) => panic!("expected metrics registry mismatch"),
        Err(error) => error.to_string(),
    };

    assert!(error.contains("different Prometheus registry"), "{error}");
}
