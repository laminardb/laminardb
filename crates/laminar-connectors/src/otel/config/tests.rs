use super::*;

#[test]
fn test_default_config() {
    let cfg = OtelSourceConfig::default();
    assert_eq!(cfg.port, 4317);
    assert_eq!(cfg.bind_address, "0.0.0.0");
    assert_eq!(cfg.signals, OtelSignal::Traces);
    assert_eq!(cfg.batch_size, 1024);
    assert_eq!(cfg.channel_capacity, 64);
}

#[test]
fn test_from_config_all_fields() {
    let config = ConnectorConfig::with_properties(
        "otel",
        [
            ("port".to_string(), "4318".to_string()),
            ("bind.address".to_string(), "127.0.0.1".to_string()),
            ("signals".to_string(), "metrics".to_string()),
            ("batch_size".to_string(), "2048".to_string()),
            ("channel_capacity".to_string(), "128".to_string()),
        ]
        .into_iter()
        .collect(),
    );
    let cfg = OtelSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.port, 4318);
    assert_eq!(cfg.bind_address, "127.0.0.1");
    assert_eq!(cfg.signals, OtelSignal::Metrics);
    assert_eq!(cfg.batch_size, 2048);
    assert_eq!(cfg.channel_capacity, 128);
}

#[test]
fn test_from_config_defaults() {
    let config = ConnectorConfig::new("otel");
    let cfg = OtelSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.port, 4317);
    assert_eq!(cfg.signals, OtelSignal::Traces);
}

#[test]
fn test_invalid_port() {
    let config = ConnectorConfig::with_properties(
        "otel",
        [("port".to_string(), "not_a_number".to_string())]
            .into_iter()
            .collect(),
    );
    assert!(OtelSourceConfig::from_config(&config).is_err());
}
