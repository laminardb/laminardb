use super::*;

fn connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "localhost");
    config.set("database", "db");
    config.set("slot.name", "s");
    config.set("publication", "p");
    config.set("table", "public.orders");
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
    assert_eq!(cfg.snapshot_mode, SnapshotMode::Initial);
    assert_eq!(cfg.output_mode, OutputMode::Upsert);
    assert!(cfg.validate().unwrap_err().to_string().contains("table"));
}

#[test]
fn replication_identity_rejects_invalid_slot_and_nul() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.table = TableName::parse("public.orders").unwrap();
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
    config.set("table", "sales.Orders");
    config.set("ssl.mode", "disable");
    config.set("port", "5433");
    config.set("max.buffered.bytes", "67108864");

    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.host, "pg.local");
    assert_eq!(cfg.port, 5433);
    assert_eq!(cfg.database, "testdb");
    assert_eq!(cfg.max_buffered_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg.table.schema, "sales");
    assert_eq!(cfg.table.name, "Orders");
}

#[test]
fn modes_parse_and_changelog_requires_a_snapshot() {
    let mut config = connector_config();
    config.set("output.mode", "changelog");
    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.output_mode, OutputMode::Changelog);
    assert_eq!(cfg.snapshot_mode, SnapshotMode::Initial);

    config.set("snapshot.mode", "never");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(
        error.to_string().contains("snapshot.mode=initial"),
        "{error}"
    );

    config.set("output.mode", "upsert");
    let cfg = PostgresCdcConfig::from_config(&config).unwrap();
    assert_eq!(cfg.snapshot_mode, SnapshotMode::Never);

    config.set("output.mode", "document");
    assert!(PostgresCdcConfig::from_config(&config).is_err());
}

#[test]
fn table_must_be_one_schema_qualified_name() {
    for table in ["users", ".users", "public.", "a.b.c", ""] {
        let mut config = connector_config();
        config.set("table", table);
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains("schema"), "{table}: {error}");
    }
    let mut properties = connector_config().properties().clone();
    properties.remove("table");
    let config = ConnectorConfig::with_properties("postgres-cdc", properties);
    assert!(PostgresCdcConfig::from_config(&config).is_err());
}

#[test]
fn relation_metadata_is_a_bounded_slice_of_the_decoded_stage() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
    assert!(cfg.relation_metadata_bytes() * RELATION_METADATA_DIVISOR <= cfg.decoded_event_bytes());
    assert!(cfg.relation_metadata_bytes() < cfg.decoded_event_bytes() / 2);
}

#[test]
fn total_byte_budget_is_partitioned_without_loss() {
    let mut cfg = PostgresCdcConfig::default();
    cfg.max_buffered_bytes = MIN_BUFFERED_BYTES;
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
    config.set("table", "public.orders");
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
    config.set("_primary_key_columns", "id");
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
        let mut config = connector_config();
        config.set(*key, "removed-value");
        let error = PostgresCdcConfig::from_config(&config).unwrap_err();
        assert!(error.to_string().contains(key));
    }
}

#[test]
fn manual_start_lsn_is_rejected() {
    let mut config = connector_config();
    config.set("start.lsn", "0/1234ABCD");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("start.lsn"));
}

#[test]
fn table_filters_point_at_the_single_table_key() {
    let mut config = connector_config();
    config.set("table.include", "public.users");
    let error = PostgresCdcConfig::from_config(&config).unwrap_err();
    assert!(error.to_string().contains("exactly one table"), "{error}");
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
