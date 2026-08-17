use super::{
    build_replication_config, source_config_digest, validate_replication_slot,
    validate_server_version_num,
};
use crate::postgres::cdc::{PostgresCdcConfig, SslMode};

#[test]
fn replication_config_disables_tls() {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = SslMode::Disable;
    let replication = build_replication_config(&config);
    assert_eq!(replication.tls.mode, pgwire_replication::SslMode::Disable);
}

#[test]
fn replication_config_maps_verified_tls_and_custom_ca() {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = SslMode::VerifyFull;
    config.ssl_ca_cert_path = Some("/certs/ca.pem".into());

    let replication = build_replication_config(&config);
    assert_eq!(
        replication.tls.mode,
        pgwire_replication::SslMode::VerifyFull
    );
    assert_eq!(replication.tls.ca_pem_path, Some("/certs/ca.pem".into()));
    assert_eq!(
        replication.status_interval,
        std::time::Duration::from_secs(1)
    );
    assert_eq!(
        replication.idle_wakeup_interval,
        std::time::Duration::from_secs(1)
    );
    assert_eq!(replication.max_message_bytes, config.raw_wal_bytes());
    assert_eq!(replication.max_in_flight_bytes, config.raw_wal_bytes());
}

#[test]
fn replication_config_maps_connection_identity() {
    let mut config = PostgresCdcConfig::new("pg.example.com", "mydb", "my_slot", "my_pub");
    config.ssl_mode = SslMode::Disable;
    config.port = 5433;
    config.username = "replicator".to_string();
    config.password = Some("secret".to_string());

    let replication = build_replication_config(&config);
    assert_eq!(replication.host, "pg.example.com");
    assert_eq!(replication.port, 5433);
    assert_eq!(replication.user, "replicator");
    assert_eq!(replication.password, "secret");
    assert_eq!(replication.database, "mydb");
    assert_eq!(replication.slot, "my_slot");
    assert_eq!(replication.publication, "my_pub");
}

#[test]
fn existing_slot_must_match_the_durable_logical_identity() {
    validate_replication_slot(
        "slot",
        "pgoutput",
        "app",
        Some("pgoutput"),
        "logical",
        Some("app"),
        false,
        None,
    )
    .unwrap();

    for error in [
        validate_replication_slot(
            "slot",
            "pgoutput",
            "app",
            Some("test_decoding"),
            "logical",
            Some("app"),
            false,
            None,
        )
        .unwrap_err(),
        validate_replication_slot(
            "slot",
            "pgoutput",
            "app",
            Some("pgoutput"),
            "logical",
            Some("other"),
            false,
            None,
        )
        .unwrap_err(),
        validate_replication_slot(
            "slot",
            "pgoutput",
            "app",
            Some("pgoutput"),
            "logical",
            Some("app"),
            true,
            None,
        )
        .unwrap_err(),
        validate_replication_slot(
            "slot",
            "pgoutput",
            "app",
            Some("pgoutput"),
            "logical",
            Some("app"),
            false,
            Some("wal_removed"),
        )
        .unwrap_err(),
    ] {
        assert!(error.to_string().contains("slot"));
    }
}

#[test]
fn source_config_digest_is_canonical_but_semantic() {
    let mut first = PostgresCdcConfig::default();
    first.table_include = vec!["public.b".into(), "public.a".into(), "public.a".into()];
    first.table_exclude = vec!["public.audit".into()];

    let mut reordered = first.clone();
    reordered.table_include = vec!["public.a".into(), "public.b".into()];
    reordered.host = "replacement-primary".into();
    reordered.max_buffered_bytes = 64 * 1024 * 1024;
    assert_eq!(
        source_config_digest(&first),
        source_config_digest(&reordered),
        "endpoint, capacity, order, and duplicates do not change filtering semantics"
    );

    reordered.table_exclude.push("public.private".into());
    assert_ne!(
        source_config_digest(&first),
        source_config_digest(&reordered)
    );
}

#[test]
fn server_version_is_admitted_before_pg17_slot_columns_are_used() {
    let error = validate_server_version_num(160_012).unwrap_err();
    assert!(error.to_string().contains("PostgreSQL 17"), "{error}");
    validate_server_version_num(170_000).unwrap();
    validate_server_version_num(180_001).unwrap();
}
