use super::*;

fn connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "localhost");
    config.set("database", "db");
    config.set("slot.name", "s");
    config.set("publication", "p");
    config.set("ssl.mode", "disable");
    config
}

#[test]
fn test_default_config() {
    let cfg = PostgresCdcConfig::default();
    assert_eq!(cfg.host, "localhost");
    assert_eq!(cfg.port, 5432);
    assert_eq!(cfg.database, "postgres");
    assert_eq!(cfg.slot_name, "laminar_slot");
    assert_eq!(cfg.publication, "laminar_pub");
    assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
    assert!(cfg.validate().is_ok());
}

#[test]
fn replication_identity_rejects_invalid_slot_and_nul() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.slot_name = "Mixed-Case".into();
    assert!(cfg
        .validate()
        .unwrap_err()
        .to_string()
        .contains("slot.name"));

    cfg.slot_name = "valid_slot".into();
    cfg.publication = "bad\0publication".into();
    assert!(cfg.validate().unwrap_err().to_string().contains("NUL"));

    cfg.publication = "Mixed-Publication".into();
    assert!(cfg
        .validate()
        .unwrap_err()
        .to_string()
        .contains("publication"));
}

#[test]
fn test_new_config() {
    let cfg = PostgresCdcConfig::new("db.example.com", "mydb", "my_slot", "my_pub");
    assert_eq!(cfg.host, "db.example.com");
    assert_eq!(cfg.database, "mydb");
    assert_eq!(cfg.slot_name, "my_slot");
    assert_eq!(cfg.publication, "my_pub");
    assert_eq!(cfg.ssl_mode, SslMode::VerifyFull);
}

#[test]
fn typed_control_config_preserves_adversarial_values() {
    let mut cfg =
        PostgresCdcConfig::new(" db\\host' ", " db name'\\ ", "valid_slot", "publication");
    cfg.username = " user name'\\ ".into();
    cfg.password = Some(" password with 'quotes' and \\slashes\\ ".into());

    let control = cfg.control_connection_config().unwrap();
    assert_eq!(
        control.get_hosts(),
        &[tokio_postgres::config::Host::Tcp(" db\\host' ".into())]
    );
    assert_eq!(control.get_ports(), &[5432]);
    assert_eq!(control.get_dbname(), Some(" db name'\\ "));
    assert_eq!(control.get_user(), Some(" user name'\\ "));
    assert_eq!(
        control.get_password(),
        Some(" password with 'quotes' and \\slashes\\ ".as_bytes())
    );
    assert_eq!(
        control.get_ssl_mode(),
        tokio_postgres::config::SslMode::Require
    );
    assert_eq!(
        control.get_connect_timeout().copied(),
        Some(crate::postgres::cdc::postgres_io::CONNECT_TIMEOUT)
    );
}

#[test]
fn typed_control_config_maps_disabled_tls_exactly() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.ssl_mode = SslMode::Disable;
    assert_eq!(
        cfg.control_connection_config().unwrap().get_ssl_mode(),
        tokio_postgres::config::SslMode::Disable
    );
}

#[test]
fn test_from_connector_config() {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "pg.local");
    config.set("database", "testdb");
    config.set("slot.name", "test_slot");
    config.set("publication", "test_pub");
    config.set("ssl.mode", "disable");
    config.set("port", "5433");
    config.set("max.buffered.bytes", "67108864");

    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.host, "pg.local");
    assert_eq!(cfg.port, 5433);
    assert_eq!(cfg.database, "testdb");
    assert_eq!(cfg.max_buffered_bytes, 64 * 1024 * 1024);
}

#[test]
fn total_byte_budget_is_partitioned_without_loss() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
    assert_eq!(
        cfg.decoded_high_watermark_bytes(),
        cfg.decoded_event_bytes() - cfg.decoded_event_bytes() / 5
    );
    assert_eq!(
        cfg.raw_wal_bytes() + cfg.decoded_event_bytes() + cfg.arrow_build_bytes(),
        MIN_BUFFERED_BYTES
    );
}

#[test]
fn total_byte_budget_rejects_values_outside_the_operational_range() {
    for bytes in [MIN_BUFFERED_BYTES - 1, MAX_BUFFERED_BYTES + 1] {
        let mut cfg = PostgresCdcConfig::default();
        cfg.max_buffered_bytes = bytes;
        assert!(cfg.validate().is_err(), "{bytes}");
    }
}

#[test]
fn test_from_config_missing_required() {
    let config = ConnectorConfig::new("postgres-cdc");
    assert!(PostgresCdcConfig::from_config(&config).is_err());
}

#[test]
fn test_from_config_invalid_port() {
    let mut config = connector_config();
    config.set("port", "not_a_number");
    assert!(PostgresCdcConfig::from_config(&config).is_err());
}

#[test]
fn omitted_ssl_mode_uses_verified_tls() {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "localhost");
    config.set("database", "db");
    config.set("slot.name", "s");
    config.set("publication", "p");
    let config = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(config.ssl_mode, SslMode::VerifyFull);
}

#[test]
fn unknown_properties_are_rejected_deterministically() {
    let mut config = connector_config();
    config.set("z.invalid", "1");
    config.set("a.invalid", "2");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("a.invalid"), "{error}");
}

#[test]
fn engine_metadata_properties_are_admitted() {
    let mut config = connector_config();
    config.set("laminar.source.name", "orders");
    config.set("_arrow_schema", "engine-owned");
    PostgresCdcConfig::from_config(&config).unwrap();
}

#[test]
fn test_validate_empty_host() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.host = String::new();
    assert!(cfg.validate().is_err());
}

#[test]
fn removed_properties_are_rejected_explicitly() {
    for key in REMOVED_CONFIG_KEYS {
        let mut config = ConnectorConfig::new("postgres-cdc");
        config.set("host", "localhost");
        config.set("database", "db");
        config.set("slot.name", "s");
        config.set("publication", "p");
        config.set(*key, "removed-value");
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn test_table_filtering() {
    let mut cfg = PostgresCdcConfig::default();
    // No filters → include all
    assert!(cfg.should_include_table("public.users"));

    // Include list
    cfg.table_include = vec!["public.users".to_string(), "public.orders".to_string()];
    cfg.normalize_table_filters();
    assert!(cfg.should_include_table("public.users"));
    assert!(!cfg.should_include_table("public.logs"));

    // Exclude overrides include
    cfg.table_exclude = vec!["public.users".to_string()];
    assert!(!cfg.should_include_table("public.users"));
}

#[test]
fn manual_start_lsn_is_rejected() {
    let mut config = connector_config();
    config.set("start.lsn", "0/1234ABCD");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("start.lsn"));
}

#[test]
fn test_from_config_table_include() {
    let mut config = connector_config();
    config.set("table.include", "public.users, public.orders");

    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.table_include, vec!["public.orders", "public.users"]);
}

#[test]
fn table_filters_are_trimmed_nonempty_sorted_and_deduplicated_once() {
    let mut config = connector_config();
    config.set(
        "table.include",
        " public.users,public.orders,, public.users,   ",
    );
    config.set(
        "table.exclude",
        " public.audit, ,public.archive,public.audit ",
    );

    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.table_include, vec!["public.orders", "public.users"]);
    assert_eq!(cfg.table_exclude, vec!["public.archive", "public.audit"]);
    assert!(cfg.should_include_table("public.users"));
    assert!(!cfg.should_include_table("public.audit"));
    assert!(!cfg.should_include_table("users"));
}

#[test]
fn table_filters_reject_unqualified_or_empty_components() {
    for table in ["users", ".users", "public."] {
        let mut config = connector_config();
        config.set("table.include", table);
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("schema"), "{table}: {error}");
    }
}

#[test]
fn custom_ca_is_admitted_for_verified_tls() {
    let mut config = connector_config();
    config.set("ssl.mode", "verify-full");
    config.set("ssl.ca.cert.path", "/certs/ca.pem");
    let parsed = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(
        parsed.ssl_ca_cert_path,
        Some(PathBuf::from("/certs/ca.pem"))
    );
}

#[test]
fn plaintext_rejects_unused_ca_path() {
    let mut config = connector_config();
    config.set("ssl.ca.cert.path", "/certs/ca.pem");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("ssl.mode=verify-full"));
}
