use super::slots::SlotAttributes;
use super::{
    build_replication_config, is_connection_failure, source_config_digest,
    validate_server_version_num,
};
use crate::postgres::cdc::config::{OutputMode, SnapshotMode, TableName};
use crate::postgres::cdc::{PostgresCdcConfig, SslMode};

#[test]
fn replication_config_disables_tls() {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = SslMode::Disable;
    let replication = build_replication_config(&config, "slot", "laminar");
    assert_eq!(replication.tls.mode, pgwire_replication::SslMode::Disable);
}

#[test]
fn replication_config_maps_verified_tls_and_custom_ca() {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = SslMode::VerifyFull;
    config.ssl_ca_cert_path = Some("/certs/ca.pem".into());

    let replication = build_replication_config(&config, "slot", "laminar");
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

    let replication = build_replication_config(
        &config,
        "my_slot_0123456789abcdef",
        "laminar:0123456789abcdef:89abcdef",
    );
    assert_eq!(replication.host, "pg.example.com");
    assert_eq!(replication.port, 5433);
    assert_eq!(replication.user, "replicator");
    assert_eq!(replication.password, "secret");
    assert_eq!(replication.database, "mydb");
    assert_eq!(replication.slot, "my_slot_0123456789abcdef");
    assert_eq!(
        replication.application_name,
        "laminar:0123456789abcdef:89abcdef"
    );
    assert_eq!(replication.publication, "my_pub");
}

#[test]
fn adoption_checks_accept_only_the_slots_the_source_creates() {
    let created = SlotAttributes {
        slot_type: Some("logical".into()),
        plugin: Some("pgoutput".into()),
        database: Some("app".into()),
        database_oid: Some(5),
        wal_status: Some("reserved".into()),
        ..SlotAttributes::default()
    };
    assert_eq!(created.problem("app", 5), None);
    let unreserved = SlotAttributes {
        wal_status: Some("unreserved".into()),
        ..created.clone()
    };
    assert_eq!(unreserved.problem("app", 5), None, "unreserved only warns");

    let cases = [
        (
            SlotAttributes {
                plugin: Some("test_decoding".into()),
                ..created.clone()
            },
            "pgoutput",
        ),
        (
            SlotAttributes {
                slot_type: Some("physical".into()),
                ..created.clone()
            },
            "pgoutput",
        ),
        (
            SlotAttributes {
                database: Some("other".into()),
                ..created.clone()
            },
            "database",
        ),
        (
            SlotAttributes {
                database_oid: Some(6),
                ..created.clone()
            },
            "database",
        ),
        (
            SlotAttributes {
                temporary: true,
                ..created.clone()
            },
            "temporary",
        ),
        (
            SlotAttributes {
                two_phase: true,
                ..created.clone()
            },
            "two_phase",
        ),
        (
            SlotAttributes {
                failover: true,
                ..created.clone()
            },
            "failover",
        ),
        (
            SlotAttributes {
                synced: true,
                ..created.clone()
            },
            "synced",
        ),
        (
            SlotAttributes {
                invalidation_reason: Some("idle_timeout".into()),
                ..created.clone()
            },
            "idle_timeout",
        ),
        (
            SlotAttributes {
                conflicting: true,
                ..created.clone()
            },
            "conflicted",
        ),
        (
            SlotAttributes {
                wal_status: Some("lost".into()),
                ..created.clone()
            },
            "lost",
        ),
    ];
    for (attributes, needle) in cases {
        let problem = attributes.problem("app", 5).expect("rejected");
        assert!(problem.contains(needle), "{needle}: {problem}");
    }
}

#[test]
fn source_config_digest_covers_only_emission_semantics() {
    let mut first = PostgresCdcConfig::default();
    first.table = TableName::parse("public.orders").unwrap();

    let mut restartable = first.clone();
    restartable.host = "replacement-primary".into();
    restartable.max_buffered_bytes = 64 * 1024 * 1024;
    restartable.snapshot_mode = SnapshotMode::Never;
    assert_eq!(
        source_config_digest(&first),
        source_config_digest(&restartable),
        "endpoint, capacity, and fresh-start mode do not change what a resumed slot emits"
    );

    let mut other_table = first.clone();
    other_table.table = TableName::parse("public.order_lines").unwrap();
    let mut other_mode = first.clone();
    other_mode.output_mode = OutputMode::Changelog;
    for changed in [other_table, other_mode] {
        assert_ne!(source_config_digest(&first), source_config_digest(&changed));
    }
}

#[test]
fn replication_session_uses_canonical_value_settings() {
    let replication = build_replication_config(&PostgresCdcConfig::default(), "slot", "laminar");
    assert_eq!(
        replication.session_options.as_deref(),
        Some(crate::postgres::cdc::typed_rows::SESSION_OPTIONS)
    );
}

#[test]
fn server_version_is_admitted_before_pg17_slot_columns_are_used() {
    let error = validate_server_version_num(160_012).unwrap_err();
    assert!(error.to_string().contains("PostgreSQL 17"), "{error}");
    validate_server_version_num(170_000).unwrap();
    validate_server_version_num(180_001).unwrap();
}

#[test]
fn only_lost_connections_count_as_retryable_statement_failures() {
    for retryable in [
        None,
        Some("08006"),
        Some("08001"),
        Some("57P01"),
        Some("57P03"),
    ] {
        assert!(is_connection_failure(retryable), "{retryable:?}");
    }
    for permanent in [Some("42501"), Some("42P01"), Some("25P02"), Some("22023")] {
        assert!(!is_connection_failure(permanent), "{permanent:?}");
    }
}
