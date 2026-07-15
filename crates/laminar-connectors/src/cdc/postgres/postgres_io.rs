//! `PostgreSQL` logical replication connections and slot administration.

#[cfg(not(test))]
use super::lsn::Lsn;
use crate::error::ConnectorError;
use sha2::{Digest, Sha256};

pub(super) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const MINIMUM_SERVER_VERSION_NUM: u32 = 170_000;

/// Database-side identity that makes an engine checkpoint safe to resume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PostgresCheckpointBinding {
    pub system_identifier: u64,
    pub timeline_id: u32,
    pub database_oid: u32,
    pub publication_oid: u32,
    pub publication_definition_sha256: String,
    pub source_config_sha256: String,
    pub slot_plugin: String,
    pub slot_two_phase: bool,
    pub slot_failover: bool,
}

/// Read-only projection of the recovery fields on an existing slot.
#[cfg(not(test))]
pub(super) struct InspectedReplicationSlot {
    pub confirmed_flush_lsn: Option<Lsn>,
    pub binding: PostgresCheckpointBinding,
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

/// Hashes only settings that change which logical changes Laminar emits.
/// Connection endpoints and buffering limits deliberately remain restartable.
#[must_use]
pub(super) fn source_config_digest(config: &super::config::PostgresCdcConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-postgres-cdc-source-v1\0");
    digest_field(&mut digest, b"pgoutput");
    digest_field(&mut digest, b"proto_version=1");
    digest_field(&mut digest, b"messages=false");

    for tables in [&config.table_include, &config.table_exclude] {
        let mut canonical: Vec<&str> = tables.iter().map(String::as_str).collect();
        canonical.sort_unstable();
        canonical.dedup();
        digest.update(
            u64::try_from(canonical.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        for table in canonical {
            digest_field(&mut digest, table.as_bytes());
        }
    }

    format!("{:x}", digest.finalize())
}

/// Cancellation-safe control-plane connection and driver task.
#[cfg(not(test))]
pub(super) struct ControlConnection {
    client: tokio_postgres::Client,
    handle: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(not(test))]
impl ControlConnection {
    #[must_use]
    pub(super) fn client(&self) -> &tokio_postgres::Client {
        &self.client
    }

    pub(super) async fn close(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

#[cfg(not(test))]
impl Drop for ControlConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Opens the regular connection used for version and recovery-identity checks.
///
/// # Errors
///
/// Returns an error when TLS configuration is invalid or the connection cannot be opened.
#[cfg(not(test))]
pub(super) async fn connect(
    config: &super::config::PostgresCdcConfig,
) -> Result<ControlConnection, ConnectorError> {
    use super::config::SslMode;

    let pg_config = config.control_connection_config()?;
    match config.ssl_mode {
        SslMode::Disable => {
            let (client, connection) =
                tokio::time::timeout(CONNECT_TIMEOUT, pg_config.connect(tokio_postgres::NoTls))
                    .await
                    .map_err(|_| {
                        ConnectorError::ConnectionFailed(
                            "PostgreSQL connect timed out after 10 seconds".into(),
                        )
                    })?
                    .map_err(|error| {
                        ConnectorError::ConnectionFailed(format!("PostgreSQL connect: {error}"))
                    })?;
            let handle = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::error!(%error, "PostgreSQL control-plane connection error");
                }
            });
            Ok(ControlConnection {
                client,
                handle: Some(handle),
            })
        }
        SslMode::VerifyFull => {
            let tls =
                crate::postgres_tls::make_rustls_connector(config.ssl_ca_cert_path.as_deref())?;
            let (client, connection) =
                tokio::time::timeout(CONNECT_TIMEOUT, pg_config.connect(tls))
                    .await
                    .map_err(|_| {
                        ConnectorError::ConnectionFailed(
                            "PostgreSQL TLS connect timed out after 10 seconds".into(),
                        )
                    })?
                    .map_err(|error| {
                        ConnectorError::ConnectionFailed(format!("PostgreSQL TLS connect: {error}"))
                    })?;
            let handle = tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::error!(%error, "PostgreSQL control-plane TLS connection error");
                }
            });
            Ok(ControlConnection {
                client,
                handle: Some(handle),
            })
        }
    }
}

/// Inspects an existing logical replication slot and returns its durable cursor.
///
/// This operation never creates, replaces, advances, or drops a slot. Recovery
/// owns an exact engine checkpoint and must fail closed when the corresponding
/// PostgreSQL slot is absent.
///
/// # Errors
///
/// Returns an error when slot lookup, identity validation, or LSN parsing fails.
#[allow(clippy::too_many_lines)]
#[cfg(not(test))]
pub(super) async fn inspect_replication_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
    plugin: &str,
    database: &str,
    publication: &str,
    source_config_sha256: String,
) -> Result<Option<InspectedReplicationSlot>, ConnectorError> {
    let version_row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_one("SELECT current_setting('server_version_num')", &[]),
    )
    .await
    .map_err(|_| {
        ConnectorError::ConnectionFailed(
            "query PostgreSQL server version timed out after 10 seconds".into(),
        )
    })?
    .map_err(|error| {
        ConnectorError::ConnectionFailed(format!("query PostgreSQL server version: {error}"))
    })?;
    let version_text: &str = version_row.get(0);
    let version_num = version_text.parse::<u32>().map_err(|error| {
        ConnectorError::ReadError(format!(
            "invalid PostgreSQL server_version_num '{version_text}': {error}"
        ))
    })?;
    validate_server_version_num(version_num)?;

    let control_row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_one(
            "SELECT control_system.system_identifier::text, control_checkpoint.timeline_id::text \
             FROM pg_catalog.pg_control_system() AS control_system \
             CROSS JOIN pg_catalog.pg_control_checkpoint() AS control_checkpoint",
            &[],
        ),
    )
    .await
    .map_err(|_| {
        ConnectorError::ConnectionFailed(
            "query PostgreSQL system identifier and timeline timed out after 10 seconds".into(),
        )
    })?
    .map_err(map_control_system_query_error)?;
    let system_identifier = parse_decimal_identity::<u64>(control_row.get(0), "system identifier")?;
    let timeline_id = parse_decimal_identity::<u32>(control_row.get(1), "timeline ID")?;

    // Keep the database, publication, and slot projection in one statement so
    // its catalog rows come from one PostgreSQL snapshot. The JSONB rendering
    // is deterministic and automatically includes new publication properties.
    let row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_opt(
            "WITH publication_identity AS ( \
                 SELECT p.oid::text AS publication_oid, p.pubtruncate, \
                        jsonb_build_object( \
                            'properties', to_jsonb(p) - ARRAY['oid', 'pubname', 'pubowner']::text[], \
                            'tables', COALESCE( \
                                (SELECT jsonb_agg( \
                                     jsonb_build_array( \
                                         c.oid::text, pt.schemaname, pt.tablename, \
                                         pt.attnames, pt.rowfilter \
                                     ) \
                                     ORDER BY pt.schemaname, pt.tablename, c.oid \
                                 ) \
                                 FROM pg_catalog.pg_publication_tables AS pt \
                                 LEFT JOIN pg_catalog.pg_namespace AS n \
                                        ON n.nspname = pt.schemaname \
                                 LEFT JOIN pg_catalog.pg_class AS c \
                                        ON c.relnamespace = n.oid AND c.relname = pt.tablename \
                                 WHERE pt.pubname = p.pubname), \
                                '[]'::jsonb \
                            ) \
                        )::text AS definition \
                 FROM pg_catalog.pg_publication AS p \
                 WHERE p.pubname = $2 \
             ) \
             SELECT s.confirmed_flush_lsn::text, s.plugin, s.slot_type, \
                    s.database::text, s.temporary, s.two_phase, s.failover, \
                    s.invalidation_reason, db.oid::text, publication_identity.publication_oid, \
                    publication_identity.definition, publication_identity.pubtruncate \
             FROM pg_catalog.pg_replication_slots AS s \
             CROSS JOIN pg_catalog.pg_database AS db \
             LEFT JOIN publication_identity ON TRUE \
             WHERE s.slot_name = $1 AND db.datname = current_database()",
            &[&slot_name, &publication],
        ),
    )
    .await
    .map_err(|_| {
        ConnectorError::ConnectionFailed("query replication slot timed out after 10 seconds".into())
    })?
    .map_err(|error| {
        ConnectorError::ConnectionFailed(format!(
            "query PostgreSQL replication identity: {error}"
        ))
    })?;

    let Some(row) = row else {
        return Ok(None);
    };
    let configured_plugin: Option<&str> = row.get(1);
    let slot_type: &str = row.get(2);
    let configured_database: Option<&str> = row.get(3);
    let temporary: bool = row.get(4);
    let two_phase: bool = row.get(5);
    let failover: bool = row.get(6);
    let invalidation_reason: Option<&str> = row.get(7);
    validate_replication_slot(
        slot_name,
        plugin,
        database,
        configured_plugin,
        slot_type,
        configured_database,
        temporary,
        invalidation_reason,
    )?;
    let database_oid = parse_decimal_identity::<u32>(row.get(8), "database OID")?;
    let publication_oid_text: Option<&str> = row.get(9);
    let publication_oid = publication_oid_text
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "PostgreSQL publication '{publication}' does not exist"
            ))
        })
        .and_then(|value| parse_decimal_identity::<u32>(value, "publication OID"))?;
    let publication_definition: Option<&str> = row.get(10);
    let publication_definition = publication_definition.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' has no readable definition"
        ))
    })?;
    let publication_truncates: Option<bool> = row.get(11);
    if publication_truncates != Some(false) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' publishes TRUNCATE, which this CDC source cannot represent; recreate or alter it with publish='insert,update,delete'"
        )));
    }
    let mut publication_digest = Sha256::new();
    publication_digest.update(b"laminardb-postgres-publication-v1\0");
    digest_field(&mut publication_digest, publication_definition.as_bytes());
    tracing::info!(
        slot = slot_name,
        two_phase,
        failover,
        "using logical replication slot"
    );

    let lsn: Option<&str> = row.get(0);
    let confirmed_flush_lsn = lsn
        .map(|value| {
            value.parse().map_err(|error| {
                ConnectorError::ReadError(format!("invalid confirmed_flush_lsn: {error}"))
            })
        })
        .transpose()?;
    Ok(Some(InspectedReplicationSlot {
        confirmed_flush_lsn,
        binding: PostgresCheckpointBinding {
            system_identifier,
            timeline_id,
            database_oid,
            publication_oid,
            publication_definition_sha256: format!("{:x}", publication_digest.finalize()),
            source_config_sha256,
            slot_plugin: configured_plugin.unwrap_or_default().to_string(),
            slot_two_phase: two_phase,
            slot_failover: failover,
        },
    }))
}

fn validate_server_version_num(version_num: u32) -> Result<(), ConnectorError> {
    if version_num < MINIMUM_SERVER_VERSION_NUM {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC requires PostgreSQL 17 or newer; server_version_num is {version_num}"
        )));
    }
    Ok(())
}

#[cfg(not(test))]
fn parse_decimal_identity<T>(value: &str, label: &str) -> Result<T, ConnectorError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|error| {
        ConnectorError::ReadError(format!("invalid PostgreSQL {label} '{value}': {error}"))
    })
}

#[cfg(not(test))]
fn map_control_system_query_error(error: tokio_postgres::Error) -> ConnectorError {
    if error.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE) {
        return ConnectorError::ConfigurationError(
            "PostgreSQL CDC must call pg_catalog.pg_control_system() and pg_catalog.pg_control_checkpoint() to bind checkpoints to a physical cluster and WAL timeline; grant the replication role pg_monitor or EXECUTE on both functions"
                .into(),
        );
    }
    ConnectorError::ConnectionFailed(format!(
        "query PostgreSQL system identifier and timeline: {error}"
    ))
}

#[allow(clippy::too_many_arguments)]
fn validate_replication_slot(
    slot_name: &str,
    expected_plugin: &str,
    expected_database: &str,
    configured_plugin: Option<&str>,
    slot_type: &str,
    configured_database: Option<&str>,
    temporary: bool,
    invalidation_reason: Option<&str>,
) -> Result<(), ConnectorError> {
    if slot_type != "logical" || configured_plugin != Some(expected_plugin) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL replication slot '{slot_name}' is not a logical {expected_plugin} slot"
        )));
    }
    if configured_database != Some(expected_database) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL replication slot '{slot_name}' belongs to database '{}', not configured database '{expected_database}'",
            configured_database.unwrap_or("<none>")
        )));
    }
    if temporary {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL replication slot '{slot_name}' is temporary and cannot provide durable recovery"
        )));
    }
    if let Some(reason) = invalidation_reason {
        return Err(ConnectorError::ReadError(format!(
            "PostgreSQL replication slot '{slot_name}' is invalidated: {reason}"
        )));
    }
    Ok(())
}

/// Builds the replication client configuration from the validated source config.
#[must_use]
pub(super) fn build_replication_config(
    config: &super::config::PostgresCdcConfig,
) -> pgwire_replication::ReplicationConfig {
    pgwire_replication::ReplicationConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.username.clone(),
        password: config.password.clone().unwrap_or_default(),
        database: config.database.clone(),
        tls: match config.ssl_mode {
            super::config::SslMode::Disable => pgwire_replication::TlsConfig::disabled(),
            super::config::SslMode::VerifyFull => {
                pgwire_replication::TlsConfig::verify_full(config.ssl_ca_cert_path.clone())
            }
        },
        slot: config.slot_name.clone(),
        publication: config.publication.clone(),
        // The exact slot/checkpoint cursor is installed by `PostgresCdcSource::start` after it
        // validates the durable slot. A user-supplied cursor is never accepted as configuration.
        start_lsn: pgwire_replication::Lsn::ZERO,
        expected_recovery_identity: None,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(1),
        buffer_events: 8192,
        max_message_bytes: config.raw_wal_bytes(),
        max_in_flight_bytes: config.raw_wal_bytes(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_replication_config, source_config_digest, validate_replication_slot,
        validate_server_version_num,
    };
    use crate::cdc::postgres::config::{PostgresCdcConfig, SslMode};

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
}
