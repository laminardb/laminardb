//! `PostgreSQL` logical replication connections and slot administration.

mod catalog;
pub(super) mod slots;

pub(super) use catalog::{inspect_capture_table, unreadable_columns, CaptureTable};

use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use sha2::{Digest, Sha256};
use slots::{SlotAttributes, SlotFacts, SlotHolder};

pub(super) const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const MINIMUM_SERVER_VERSION_NUM: u32 = 170_000;

/// Database-side identity that makes an engine checkpoint safe to resume. The slot's own
/// attributes are fixed by the adoption checks rather than recorded here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct PostgresCheckpointBinding {
    pub system_identifier: u64,
    pub timeline_id: u32,
    pub database_oid: u32,
    pub publication_oid: u32,
    pub publication_definition_sha256: String,
    pub source_config_sha256: String,
}

/// Cluster, publication, and optional slot identity read from one catalog snapshot.
#[derive(Debug, Clone)]
pub(super) struct InspectedSource {
    pub system_identifier: u64,
    pub timeline_id: u32,
    pub database_oid: u32,
    pub publication_oid: u32,
    pub publication_definition_sha256: String,
    pub slot: Option<SlotFacts>,
}

impl InspectedSource {
    /// The checkpoint binding of the live database and publication.
    pub(super) fn binding(
        &self,
        config: &super::config::PostgresCdcConfig,
    ) -> PostgresCheckpointBinding {
        PostgresCheckpointBinding {
            system_identifier: self.system_identifier,
            timeline_id: self.timeline_id,
            database_oid: self.database_oid,
            publication_oid: self.publication_oid,
            publication_definition_sha256: self.publication_definition_sha256.clone(),
            source_config_sha256: source_config_digest(config),
        }
    }
}

fn digest_field(digest: &mut Sha256, value: &[u8]) {
    digest.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(value);
}

/// Hashes only settings that change which logical changes Laminar emits and how.
/// Connection endpoints, snapshot mode, and buffering limits deliberately remain restartable.
#[must_use]
pub(super) fn source_config_digest(config: &super::config::PostgresCdcConfig) -> String {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-postgres-cdc-source-v2\0");
    digest_field(&mut digest, b"pgoutput");
    digest_field(&mut digest, b"proto_version=1");
    digest_field(&mut digest, b"messages=false");
    digest_field(&mut digest, super::typed_rows::SESSION_OPTIONS.as_bytes());
    digest_field(&mut digest, config.table.schema.as_bytes());
    digest_field(&mut digest, config.table.name.as_bytes());
    digest_field(&mut digest, config.output_mode.to_string().as_bytes());
    format!("{:x}", digest.finalize())
}

/// Cancellation-safe control-plane connection and driver task.
pub(super) struct ControlConnection {
    client: tokio_postgres::Client,
    handle: Option<tokio::task::JoinHandle<()>>,
}

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

impl Drop for ControlConnection {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// Opens a regular connection with the canonical CDC session settings.
///
/// # Errors
///
/// Returns an error when TLS configuration is invalid or the connection cannot be opened.
pub(super) async fn connect(
    config: &super::config::PostgresCdcConfig,
    application_name: &str,
    driver_guard: ConnectorTaskGuard,
) -> Result<ControlConnection, ConnectorError> {
    use crate::postgres::SslMode;

    let pg_config = config.control_connection_config(application_name)?;
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
                let _driver_guard = driver_guard;
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
            let tls = crate::postgres::make_rustls_connector(config.ssl_ca_cert_path.as_deref())?;
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
                let _driver_guard = driver_guard;
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

/// Inspects the cluster, the publication, and the slot named `slot` without mutating any of them.
///
/// A present slot carries its holder and the adoption checks' verdict.
///
/// # Errors
///
/// Returns an error when identity validation, publication admission, or LSN parsing fails.
pub(super) async fn inspect_source(
    client: &tokio_postgres::Client,
    config: &super::config::PostgresCdcConfig,
    slot: Option<&str>,
) -> Result<InspectedSource, ConnectorError> {
    let (system_identifier, timeline_id) = read_system_identity(client).await?;

    // Keep the database, publication, and slot projection in one statement so its catalog rows
    // come from one PostgreSQL snapshot. The JSONB rendering is deterministic and automatically
    // includes new publication properties.
    let row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_one(
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
             SELECT db.oid::text, publication_identity.publication_oid, \
                    publication_identity.definition, publication_identity.pubtruncate, \
                    s.slot_name IS NOT NULL, s.confirmed_flush_lsn::text, s.slot_type, s.plugin, \
                    s.database::text, s.datoid::text, s.temporary, s.two_phase, s.failover, \
                    s.synced, s.invalidation_reason, s.conflicting, s.wal_status, \
                    s.active_pid, a.application_name, a.client_addr::text \
             FROM pg_catalog.pg_database AS db \
             LEFT JOIN pg_catalog.pg_replication_slots AS s ON s.slot_name = $1 \
             LEFT JOIN pg_catalog.pg_stat_activity AS a ON a.pid = s.active_pid \
             LEFT JOIN publication_identity ON TRUE \
             WHERE db.datname = current_database()",
            &[&slot, &config.publication],
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

    let (database_oid, publication_oid, publication_definition_sha256) =
        read_publication_identity(&row, &config.publication)?;
    let slot = match slot {
        Some(name) if row.get::<_, bool>(4) => {
            Some(read_slot(&row, name, &config.database, database_oid)?)
        }
        _ => None,
    };
    Ok(InspectedSource {
        system_identifier,
        timeline_id,
        database_oid,
        publication_oid,
        publication_definition_sha256,
        slot,
    })
}

/// The slot columns of [`inspect_source`]'s row, from column 5 on.
fn read_slot(
    row: &tokio_postgres::Row,
    name: &str,
    database: &str,
    database_oid: u32,
) -> Result<SlotFacts, ConnectorError> {
    let confirmed: Option<&str> = row.get(5);
    let confirmed_flush_lsn = confirmed
        .map(|value| {
            value.parse().map_err(|error| {
                ConnectorError::ReadError(format!("invalid confirmed_flush_lsn: {error}"))
            })
        })
        .transpose()?;
    let datoid: Option<&str> = row.get(9);
    let attributes = SlotAttributes {
        slot_type: row.get(6),
        plugin: row.get(7),
        database: row.get(8),
        database_oid: datoid
            .map(|oid| parse_decimal_identity::<u32>(oid, "slot database OID"))
            .transpose()?,
        temporary: row.get::<_, Option<bool>>(10).unwrap_or(false),
        two_phase: row.get::<_, Option<bool>>(11).unwrap_or(false),
        failover: row.get::<_, Option<bool>>(12).unwrap_or(false),
        synced: row.get::<_, Option<bool>>(13).unwrap_or(false),
        invalidation_reason: row.get(14),
        conflicting: row.get::<_, Option<bool>>(15).unwrap_or(false),
        wal_status: row.get(16),
    };
    let holder = row.get::<_, Option<i32>>(17).map(|pid| SlotHolder {
        pid,
        application_name: row.get::<_, Option<String>>(18).unwrap_or_default(),
        client_addr: row.get(19),
    });
    if attributes.wal_status.as_deref() == Some("unreserved") {
        tracing::warn!(
            slot = name,
            "PostgreSQL replication slot no longer reserves all the WAL it needs; the next \
             checkpoint removes some of it unless the slot catches up (wal_status = unreserved, \
             max_slot_wal_keep_size)"
        );
    }
    Ok(SlotFacts {
        confirmed_flush_lsn,
        holder,
        unusable: attributes.problem(database, database_oid),
    })
}

fn read_publication_identity(
    row: &tokio_postgres::Row,
    publication: &str,
) -> Result<(u32, u32, String), ConnectorError> {
    let database_oid = parse_decimal_identity::<u32>(row.get(0), "database OID")?;
    let publication_oid_text: Option<&str> = row.get(1);
    let publication_oid = publication_oid_text
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "PostgreSQL publication '{publication}' does not exist"
            ))
        })
        .and_then(|value| parse_decimal_identity::<u32>(value, "publication OID"))?;
    let publication_definition: Option<&str> = row.get(2);
    let publication_definition = publication_definition.ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' has no readable definition"
        ))
    })?;
    let publication_truncates: Option<bool> = row.get(3);
    if publication_truncates != Some(true) {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL publication '{publication}' must publish TRUNCATE so a source truncation \
             stops the pipeline instead of silently diverging the target; use the default \
             publish='insert, update, delete, truncate'"
        )));
    }
    let mut publication_digest = Sha256::new();
    publication_digest.update(b"laminardb-postgres-publication-v1\0");
    digest_field(&mut publication_digest, publication_definition.as_bytes());
    Ok((
        database_oid,
        publication_oid,
        format!("{:x}", publication_digest.finalize()),
    ))
}

async fn read_system_identity(
    client: &tokio_postgres::Client,
) -> Result<(u64, u32), ConnectorError> {
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
    let version_text: &str = version_row.try_get(0).map_err(|error| {
        ConnectorError::ReadError(format!("read PostgreSQL server version: {error}"))
    })?;
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
    .map_err(|error| map_control_system_query_error(&error))?;
    let system_identifier = parse_decimal_identity::<u64>(
        control_row.try_get(0).map_err(|error| {
            ConnectorError::ReadError(format!("read PostgreSQL system identifier: {error}"))
        })?,
        "system identifier",
    )?;
    let timeline_id = parse_decimal_identity::<u32>(
        control_row.try_get(1).map_err(|error| {
            ConnectorError::ReadError(format!("read PostgreSQL timeline: {error}"))
        })?,
        "timeline ID",
    )?;

    Ok((system_identifier, timeline_id))
}

/// WAL retention state of a slot (`pg_replication_slots.wal_status`, `safe_wal_size`).
///
/// # Errors
///
/// Returns an error when the slot is missing or the query fails.
pub(super) async fn slot_wal_status(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<(String, Option<i64>), ConnectorError> {
    let row = tokio::time::timeout(
        CONNECT_TIMEOUT,
        client.query_opt(
            "SELECT wal_status, safe_wal_size FROM pg_catalog.pg_replication_slots \
             WHERE slot_name = $1",
            &[&slot_name],
        ),
    )
    .await
    .map_err(|_| {
        ConnectorError::ConnectionFailed("query slot WAL status timed out after 10 seconds".into())
    })?
    .map_err(|error| ConnectorError::ConnectionFailed(format!("query slot WAL status: {error}")))?
    .ok_or_else(|| {
        ConnectorError::ReadError(format!(
            "PostgreSQL replication slot '{slot_name}' disappeared"
        ))
    })?;
    Ok((
        row.get::<_, Option<String>>(0).unwrap_or_default(),
        row.get(1),
    ))
}

fn validate_server_version_num(version_num: u32) -> Result<(), ConnectorError> {
    if version_num < MINIMUM_SERVER_VERSION_NUM {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL CDC requires PostgreSQL 17 or newer; server_version_num is {version_num}"
        )));
    }
    Ok(())
}

/// Whether a failed statement may succeed on a new connection: no SQLSTATE (the connection was
/// lost), a connection exception (`08`), or a server shutdown or restart (`57P0`).
pub(super) fn is_connection_failure(sqlstate: Option<&str>) -> bool {
    sqlstate.is_none_or(|code| code.starts_with("08") || code.starts_with("57P0"))
}

fn parse_decimal_identity<T>(value: &str, label: &str) -> Result<T, ConnectorError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|error| {
        ConnectorError::ReadError(format!("invalid PostgreSQL {label} '{value}': {error}"))
    })
}

fn map_control_system_query_error(error: &tokio_postgres::Error) -> ConnectorError {
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

/// Builds the replication client configuration for `slot` from the validated source config.
#[must_use]
pub(super) fn build_replication_config(
    config: &super::config::PostgresCdcConfig,
    slot: &str,
    application_name: &str,
) -> pgwire_replication::ReplicationConfig {
    pgwire_replication::ReplicationConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.username.clone(),
        password: config.password.clone().unwrap_or_default(),
        database: config.database.clone(),
        tls: match config.ssl_mode {
            crate::postgres::SslMode::Disable => pgwire_replication::TlsConfig::disabled(),
            crate::postgres::SslMode::VerifyFull => {
                pgwire_replication::TlsConfig::verify_full(config.ssl_ca_cert_path.clone())
            }
        },
        slot: slot.to_string(),
        publication: config.publication.clone(),
        // The reader launch sets the position once the source has adopted or created its slot;
        // a user-supplied cursor is never accepted as configuration.
        start_lsn: pgwire_replication::Lsn::ZERO,
        expected_recovery_identity: None,
        stop_at_lsn: None,
        status_interval: std::time::Duration::from_secs(1),
        idle_wakeup_interval: std::time::Duration::from_secs(1),
        buffer_events: 8192,
        max_message_bytes: config.raw_wal_bytes(),
        session_options: Some(super::typed_rows::SESSION_OPTIONS.into()),
        application_name: application_name.to_string(),
        max_in_flight_bytes: config.raw_wal_bytes(),
    }
}

#[cfg(test)]
mod tests;
