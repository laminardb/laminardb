use super::*;

fn make_config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres-sink");
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

fn required_pairs() -> Vec<(&'static str, &'static str)> {
    vec![
        ("hostname", "localhost"),
        ("database", "mydb"),
        ("username", "writer"),
        ("table.name", "events"),
    ]
}

#[test]
fn test_parse_required_fields() {
    let config = make_config(&required_pairs());
    let cfg = PostgresSinkConfig::from_config(&config).unwrap();
    assert_eq!(cfg.hostname, "localhost");
    assert_eq!(cfg.database, "mydb");
    assert_eq!(cfg.username, "writer");
    assert_eq!(cfg.table_name, "events");
    assert_eq!(cfg.port, 5432);
    assert_eq!(cfg.schema_name, "public");
    assert_eq!(cfg.write_mode, WriteMode::Append);
}

#[test]
fn test_missing_hostname() {
    let config = make_config(&[("database", "db"), ("username", "u"), ("table.name", "t")]);
    assert!(PostgresSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_missing_database() {
    let config = make_config(&[("hostname", "h"), ("username", "u"), ("table.name", "t")]);
    assert!(PostgresSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_missing_username() {
    let config = make_config(&[("hostname", "h"), ("database", "db"), ("table.name", "t")]);
    assert!(PostgresSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_missing_table_name() {
    let config = make_config(&[("hostname", "h"), ("database", "db"), ("username", "u")]);
    assert!(PostgresSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_parse_all_optional_fields() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("password", "secret"),
        ("port", "5433"),
        ("schema.name", "analytics"),
        ("write.mode", "upsert"),
        ("primary.key", "id, region"),
        ("flush.interval.ms", "500"),
        ("connect.timeout.ms", "5000"),
        ("ssl.mode", "verify-full"),
        ("ssl.ca.cert.path", "/certs/ca.pem"),
        ("auto.create.table", "true"),
        ("changelog.mode", "true"),
    ]);
    let config = make_config(&pairs);
    let cfg = PostgresSinkConfig::from_config(&config).unwrap();

    assert_eq!(cfg.password, "secret");
    assert_eq!(cfg.port, 5433);
    assert_eq!(cfg.schema_name, "analytics");
    assert_eq!(cfg.write_mode, WriteMode::Upsert);
    assert_eq!(cfg.primary_key_columns, vec!["id", "region"]);
    assert_eq!(cfg.flush_interval, Duration::from_millis(500));
    assert_eq!(cfg.connect_timeout, Duration::from_secs(5));
    assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
    assert_eq!(cfg.ssl_ca_cert_path, Some(PathBuf::from("/certs/ca.pem")));
    assert!(cfg.auto_create_table);
    assert!(cfg.changelog_mode);
}

#[test]
fn test_upsert_requires_primary_key() {
    let mut pairs = required_pairs();
    pairs.push(("write.mode", "upsert"));
    let config = make_config(&pairs);
    let result = PostgresSinkConfig::from_config(&config);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("primary.key"), "error: {err}");
}

#[test]
fn test_changelog_requires_upsert() {
    let mut pairs = required_pairs();
    pairs.push(("changelog.mode", "true"));
    let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
    assert!(error.to_string().contains("write.mode = 'upsert'"));
}

#[test]
fn removed_buffer_and_pool_options_are_rejected() {
    for key in REMOVED_CONFIG_KEYS {
        let mut pairs = required_pairs();
        pairs.push((*key, "1"));
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn test_qualified_table_name() {
    let cfg = PostgresSinkConfig::new("localhost", "db", "events");
    assert_eq!(cfg.qualified_table_name(), "\"public\".\"events\"");

    let mut cfg2 = cfg;
    cfg2.schema_name = "analytics".to_string();
    cfg2.table_name = "order\"items".to_string();
    assert_eq!(
        cfg2.qualified_table_name(),
        "\"analytics\".\"order\"\"items\""
    );
}

#[test]
fn identifiers_fail_closed_before_sql_generation() {
    for invalid in ["", "bad\0name"] {
        let mut cfg = PostgresSinkConfig::new("localhost", "db", invalid);
        assert!(cfg.validate().is_err(), "table identifier {invalid:?}");
        cfg.table_name = "events".into();
        cfg.schema_name = invalid.into();
        assert!(cfg.validate().is_err(), "schema identifier {invalid:?}");
    }

    let mut cfg = PostgresSinkConfig::new("localhost", "db", "events");
    cfg.write_mode = WriteMode::Upsert;
    cfg.primary_key_columns = vec!["id".into(), "id".into()];
    assert!(cfg
        .validate()
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
}

#[test]
fn test_defaults() {
    let cfg = PostgresSinkConfig::default();
    assert_eq!(cfg.hostname, "localhost");
    assert_eq!(cfg.port, 5432);
    assert_eq!(cfg.schema_name, "public");
    assert_eq!(cfg.write_mode, WriteMode::Append);
    assert_eq!(cfg.flush_interval, Duration::from_millis(250));
    assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
    assert!(!cfg.auto_create_table);
    assert!(!cfg.changelog_mode);
}

#[test]
fn test_write_mode_parse() {
    assert_eq!("append".parse::<WriteMode>().unwrap(), WriteMode::Append);
    assert_eq!("copy".parse::<WriteMode>().unwrap(), WriteMode::Append);
    assert_eq!("upsert".parse::<WriteMode>().unwrap(), WriteMode::Upsert);
    assert_eq!("insert".parse::<WriteMode>().unwrap(), WriteMode::Upsert);
    assert!("unknown".parse::<WriteMode>().is_err());
}

#[test]
fn test_write_mode_display() {
    assert_eq!(WriteMode::Append.to_string(), "append");
    assert_eq!(WriteMode::Upsert.to_string(), "upsert");
}

#[test]
fn legacy_ssl_modes_are_rejected() {
    for mode in ["off", "prefer", "require", "verify-ca"] {
        let mut pairs = required_pairs();
        pairs.push(("ssl.mode", mode));
        let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("invalid ssl.mode"), "{message}");
    }
}

#[test]
fn plaintext_rejects_unused_ca_path() {
    let mut pairs = required_pairs();
    pairs.extend_from_slice(&[
        ("ssl.mode", "disable"),
        ("ssl.ca.cert.path", "/certs/ca.pem"),
    ]);
    let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
    assert!(error.to_string().contains("ssl.mode=verify-full"));
}

#[test]
fn test_invalid_port() {
    let mut pairs = required_pairs();
    pairs.push(("port", "not_a_number"));
    let config = make_config(&pairs);
    assert!(PostgresSinkConfig::from_config(&config).is_err());
}

#[test]
fn test_zero_flush_interval_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("flush.interval.ms", "0"));
    let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
    assert!(error.to_string().contains("flush.interval.ms must be > 0"));
}

#[test]
fn zero_connect_timeout_is_rejected() {
    let mut pairs = required_pairs();
    pairs.push(("connect.timeout.ms", "0"));
    let error = PostgresSinkConfig::from_config(&make_config(&pairs)).unwrap_err();
    assert!(error.to_string().contains("connect.timeout.ms must be > 0"));
}

#[test]
fn statement_timeout_is_a_bounded_integer_startup_setting() {
    let mut cfg = PostgresSinkConfig::new("localhost", "db", "events");
    cfg.statement_timeout = Duration::from_millis(30_000);
    cfg.validate().unwrap();
    assert_eq!(
        cfg.statement_timeout_startup_option(),
        "-c statement_timeout=30000"
    );

    cfg.statement_timeout =
        Duration::from_millis(u64::try_from(MAX_POSTGRES_STATEMENT_TIMEOUT_MS).unwrap());
    cfg.validate().unwrap();
    cfg.statement_timeout =
        Duration::from_millis(u64::try_from(MAX_POSTGRES_STATEMENT_TIMEOUT_MS + 1).unwrap());
    assert!(cfg.validate().is_err());

    cfg.statement_timeout = Duration::from_secs(1) + Duration::from_micros(1);
    assert!(cfg.validate().is_err());
}
