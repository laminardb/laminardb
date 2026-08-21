use super::*;

fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("delta-lake-source");
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

#[test]
fn test_defaults() {
    let cfg = DeltaSourceConfig::default();
    assert!(cfg.table_path.is_empty());
    assert!(cfg.starting_version.is_none());
    assert_eq!(cfg.poll_interval, Duration::from_secs(1));
}

#[test]
fn test_new_helper() {
    let cfg = DeltaSourceConfig::new("/tmp/test_table");
    assert_eq!(cfg.table_path, "/tmp/test_table");
}

#[test]
fn test_parse_required_fields() {
    let config = make_config(&[("table.path", "/data/warehouse/trades")]);
    let cfg = DeltaSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.table_path, "/data/warehouse/trades");
    assert!(cfg.starting_version.is_none());
}

#[test]
fn test_missing_table_path() {
    let config = ConnectorConfig::new("delta-lake-source");
    assert!(DeltaSourceConfig::from_config(&config).is_err());
}

#[test]
fn test_parse_optional_fields() {
    let config = make_config(&[
        ("table.path", "/data/test"),
        ("starting.version", "5"),
        ("poll.interval.ms", "500"),
    ]);
    let cfg = DeltaSourceConfig::from_config(&config).unwrap();
    assert_eq!(cfg.starting_version, Some(5));
    assert_eq!(cfg.poll_interval, Duration::from_millis(500));
}

#[test]
fn test_invalid_starting_version() {
    for value in ["abc", "-1"] {
        let config = make_config(&[("table.path", "/data/test"), ("starting.version", value)]);
        assert!(DeltaSourceConfig::from_config(&config).is_err());
    }
}

#[test]
fn test_empty_table_path_rejected() {
    let mut cfg = DeltaSourceConfig::default();
    cfg.table_path = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn removed_options_fail_closed() {
    for (key, value) in [
        ("read.mode", "incremental"),
        ("cdf.enabled", "true"),
        ("partition.filter", ""),
        ("partition.filter", "date = '2024-01-01'"),
        ("schema.evolution.action", "warn"),
    ] {
        assert!(DeltaSourceConfig::from_config(&make_config(&[
            ("table.path", "/data/test"),
            (key, value)
        ]))
        .is_err());
    }
}

#[cfg(feature = "delta-lake")]
#[test]
fn stable_storage_options_exclude_environment_fallbacks() {
    let mut explicit = HashMap::new();
    explicit.insert("aws_region".into(), "eu-west-2".into());
    let resolved = StorageCredentialResolver::resolve_with_env(
        "s3://bucket/table",
        &explicit,
        |key| match key {
            "AWS_ACCESS_KEY_ID" => Some("rotating-key".into()),
            "AWS_SECRET_ACCESS_KEY" => Some("rotating-secret".into()),
            _ => None,
        },
    );
    let config = DeltaSourceConfig {
        table_path: "s3://bucket/table".into(),
        storage_options: resolved.options,
        env_resolved_storage_keys: resolved.env_resolved_keys,
        ..DeltaSourceConfig::default()
    };

    let stable = config.stable_storage_options();
    assert_eq!(
        stable.get("aws_region").map(String::as_str),
        Some("eu-west-2")
    );
    assert!(!stable.contains_key("aws_access_key_id"));
    assert!(!stable.contains_key("aws_secret_access_key"));
}
