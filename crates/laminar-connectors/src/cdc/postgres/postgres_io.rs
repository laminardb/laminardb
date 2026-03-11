//! `PostgreSQL` logical replication I/O functions.
//!
//! Provides low-level functions for connecting to `PostgreSQL`, managing
//! replication slots, and parsing/encoding replication wire messages.
//!
//! # Architecture
//!
//! - **Control-plane connection** (`connect`, `ensure_replication_slot`):
//!   Feature-gated behind `postgres-cdc`, uses `tokio-postgres` for slot
//!   management and metadata queries.
//! - **Replication streaming**: WAL streaming uses `pgwire-replication` which
//!   implements the `CopyBoth` sub-protocol natively. See
//!   `build_replication_config()` for config conversion.
//! - **Wire format** (`parse_replication_message`, `encode_standby_status`,
//!   `build_start_replication_query`): Always available, retained as test
//!   utilities and protocol documentation.
//!
//! # Wire Format
//!
//! Replication messages inside `CopyData` frames:
//!
//! - `XLogData` (tag `w`): 25-byte header + WAL payload
//! - `PrimaryKeepalive` (tag `k`): 18 bytes with WAL position and reply flag
//! - `StandbyStatusUpdate` (tag `r`): 34 bytes sent from client to server

use super::lsn::Lsn;
use crate::error::ConnectorError;

// ── Wire format types (always available) ──

/// A message received from the `PostgreSQL` replication stream.
///
/// These are the two message types sent by the server over the
/// streaming replication protocol (inside `CopyData` messages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    /// WAL data payload (tag `w`).
    ///
    /// Contains the raw pgoutput bytes that should be decoded by the
    /// pgoutput decoder.
    XLogData {
        /// WAL start position of this message.
        wal_start: Lsn,
        /// WAL end position (server's current write position).
        wal_end: Lsn,
        /// Server timestamp in microseconds since 2000-01-01.
        server_time_us: i64,
        /// Raw pgoutput payload bytes.
        data: Vec<u8>,
    },

    /// Primary keepalive message (tag `k`).
    ///
    /// Sent periodically by the server, and whenever the server needs
    /// a status update from the client.
    PrimaryKeepalive {
        /// Server's current WAL end position.
        wal_end: Lsn,
        /// Server timestamp in microseconds since 2000-01-01.
        server_time_us: i64,
        /// If `true`, the client must reply immediately with a standby
        /// status update, otherwise the server may disconnect.
        reply_requested: bool,
    },
}

/// Parses a raw replication message from byte data.
///
/// The input should be the payload of a `CopyData` message (after
/// stripping the `CopyData` framing).
///
/// # Wire Format
///
/// - `w` (0x77): `XLogData` — 1 + 8 + 8 + 8 bytes header + variable payload
/// - `k` (0x6B): `PrimaryKeepalive` — 1 + 8 + 8 + 1 = 18 bytes
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the message is empty,
/// has an unknown tag, or is truncated.
#[allow(clippy::missing_panics_doc)] // slice indexing is bounds-checked above
pub fn parse_replication_message(data: &[u8]) -> Result<ReplicationMessage, ConnectorError> {
    if data.is_empty() {
        return Err(ConnectorError::ReadError(
            "empty replication message".to_string(),
        ));
    }

    match data[0] {
        b'w' => {
            // XLogData: tag(1) + wal_start(8) + wal_end(8) + server_time(8) + data(N)
            const HEADER_LEN: usize = 1 + 8 + 8 + 8; // 25 bytes
            if data.len() < HEADER_LEN {
                return Err(ConnectorError::ReadError(format!(
                    "truncated XLogData: {} bytes (need at least {HEADER_LEN})",
                    data.len()
                )));
            }

            // SAFETY: length checked by `data.len() < HEADER_LEN` guard above
            let wal_start = Lsn::new(u64::from_be_bytes(data[1..9].try_into().unwrap()));
            let wal_end = Lsn::new(u64::from_be_bytes(data[9..17].try_into().unwrap()));
            let server_time_us = i64::from_be_bytes(data[17..25].try_into().unwrap());
            let payload = data[HEADER_LEN..].to_vec();

            Ok(ReplicationMessage::XLogData {
                wal_start,
                wal_end,
                server_time_us,
                data: payload,
            })
        }
        b'k' => {
            // PrimaryKeepalive: tag(1) + wal_end(8) + server_time(8) + reply(1) = 18
            const KEEPALIVE_LEN: usize = 1 + 8 + 8 + 1; // 18 bytes
            if data.len() < KEEPALIVE_LEN {
                return Err(ConnectorError::ReadError(format!(
                    "truncated PrimaryKeepalive: {} bytes (need {KEEPALIVE_LEN})",
                    data.len()
                )));
            }

            // SAFETY: length checked by `data.len() < KEEPALIVE_LEN` guard above
            let wal_end = Lsn::new(u64::from_be_bytes(data[1..9].try_into().unwrap()));
            let server_time_us = i64::from_be_bytes(data[9..17].try_into().unwrap());
            let reply_requested = data[17] != 0;

            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end,
                server_time_us,
                reply_requested,
            })
        }
        tag => Err(ConnectorError::ReadError(format!(
            "unknown replication message tag: 0x{tag:02X}"
        ))),
    }
}

/// Encodes a standby status update message.
///
/// Returns the 34-byte message suitable for sending via `CopyData`.
///
/// # Wire Format
///
/// ```text
/// Byte  0:       'r' (0x72) — StandbyStatusUpdate tag
/// Bytes 1-8:     write LSN (last WAL position received)
/// Bytes 9-16:    flush LSN (last WAL position flushed to disk)
/// Bytes 17-24:   apply LSN (last WAL position applied)
/// Bytes 25-32:   client timestamp (microseconds since 2000-01-01)
/// Byte  33:      reply requested (always 0 from client)
/// ```
#[must_use]
pub fn encode_standby_status(write_lsn: Lsn, flush_lsn: Lsn, apply_lsn: Lsn) -> Vec<u8> {
    let mut buf = Vec::with_capacity(34);
    buf.push(b'r');
    buf.extend_from_slice(&write_lsn.as_u64().to_be_bytes());
    buf.extend_from_slice(&flush_lsn.as_u64().to_be_bytes());
    buf.extend_from_slice(&apply_lsn.as_u64().to_be_bytes());
    // Client timestamp: 0 (server doesn't require it)
    buf.extend_from_slice(&0_i64.to_be_bytes());
    // Reply requested: always 0 from client
    buf.push(0);
    buf
}

/// Validates that a string is a safe `PostgreSQL` identifier (alphanumeric + underscore).
fn validate_pg_identifier(value: &str, field: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(ConnectorError::ConfigurationError(format!(
            "{field} must not be empty"
        )));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "{field} contains unsafe characters (only [a-zA-Z0-9_] allowed): {value:?}"
        )));
    }
    Ok(())
}

/// Builds the `START_REPLICATION` SQL command.
///
/// This returns the query string to be sent via the `CopyBoth` protocol.
/// Currently used for documentation; will be used directly once
/// `CopyBoth` support is available in `tokio-postgres`.
///
/// # Errors
///
/// Returns `ConnectorError::ConfigurationError` if `slot_name` or
/// `publication` contain characters outside `[a-zA-Z0-9_]`.
pub fn build_start_replication_query(
    slot_name: &str,
    start_lsn: Lsn,
    publication: &str,
) -> Result<String, ConnectorError> {
    validate_pg_identifier(slot_name, "slot_name")?;
    validate_pg_identifier(publication, "publication")?;
    Ok(format!(
        "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn} \
         (proto_version '1', publication_names '{publication}')"
    ))
}

// ── Feature-gated I/O functions ──

/// Connects to `PostgreSQL` as a regular (control-plane) connection.
///
/// This connection is used for slot management and metadata queries.
/// WAL streaming uses a separate `pgwire-replication` client (see
/// `build_replication_config()`).
///
/// Spawns a background task to drive the connection. The caller must
/// keep the returned `JoinHandle` alive; dropping it will close the
/// connection.
///
/// # TLS
///
/// Currently only supports `NoTls`. Non-`Disable` SSL modes will log
/// a warning and fall back to `NoTls`. TLS support is planned as a
/// follow-up.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the connection fails.
#[cfg(feature = "postgres-cdc")]
pub async fn connect(
    config: &super::config::PostgresCdcConfig,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), ConnectorError> {
    use super::config::SslMode;

    let conn_str = config.connection_string();

    match config.ssl_mode {
        SslMode::Disable => {}
        SslMode::Prefer => {
            tracing::info!(
                ssl_mode = %config.ssl_mode,
                "TLS not yet implemented for control-plane connections; using NoTls (Prefer mode)"
            );
        }
        mode => {
            return Err(ConnectorError::ConfigurationError(format!(
                "ssl.mode={mode} requires TLS support which is not yet implemented \
                 for control-plane connections"
            )));
        }
    }

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("PostgreSQL connect: {e}")))?;

    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "PostgreSQL control-plane connection error");
        }
    });

    Ok((client, handle))
}

/// Ensures the replication slot exists, creating it if necessary.
///
/// Returns the slot's `confirmed_flush_lsn` if the slot already exists
/// (useful for resuming replication from the last acknowledged position).
///
/// Uses a regular (non-replication) connection. Slot creation uses the
/// `pg_create_logical_replication_slot()` SQL function which works on
/// standard connections.
///
/// # Errors
///
/// Returns `ConnectorError` if the slot query or creation fails.
#[cfg(feature = "postgres-cdc")]
pub async fn ensure_replication_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
    plugin: &str,
) -> Result<Option<Lsn>, ConnectorError> {
    // Check if slot already exists (parameterized to prevent SQL injection)
    let rows = client
        .query(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("query replication slots: {e}")))?;

    if let Some(row) = rows.first() {
        let lsn_str: Option<&str> = row.get(0);
        if let Some(lsn_str) = lsn_str {
            let lsn: Lsn = lsn_str.parse().map_err(|e| {
                ConnectorError::ReadError(format!("invalid confirmed_flush_lsn: {e}"))
            })?;
            tracing::info!(slot = slot_name, lsn = %lsn, "replication slot exists");
            return Ok(Some(lsn));
        }
        // Row exists but LSN column is NULL
        tracing::info!(slot = slot_name, "replication slot exists (no flush LSN)");
        return Ok(None);
    }

    // Slot doesn't exist — create it via SQL function (parameterized)
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, $2)",
            &[&slot_name, &plugin],
        )
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("create replication slot: {e}")))?;

    tracing::info!(
        slot = slot_name,
        plugin = plugin,
        "created replication slot"
    );
    Ok(None)
}

/// Drop a `PostgreSQL` logical replication slot.
///
/// Use this for administrative cleanup when a pipeline is permanently
/// deleted and the slot is no longer needed.  Do NOT call this on normal
/// shutdown — the slot must survive restarts for resume-from-checkpoint.
///
/// An unconsumed slot holds WAL on the server until it is either consumed
/// or dropped.  Monitor `pg_replication_slots` for stale slots.
///
/// # Errors
///
/// Returns `ConnectorError` if the DROP fails (e.g., slot does not exist,
/// or it is still active).
#[cfg(feature = "postgres-cdc")]
pub async fn drop_replication_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<(), ConnectorError> {
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await
        .map_err(|e| {
            ConnectorError::ConnectionFailed(format!("drop replication slot '{slot_name}': {e}"))
        })?;
    tracing::info!(slot = slot_name, "dropped replication slot");
    Ok(())
}

/// Builds a [`pgwire_replication::ReplicationConfig`] from a
/// [`PostgresCdcConfig`](super::config::PostgresCdcConfig).
///
/// Maps connection parameters, replication slot, publication, start LSN,
/// keepalive interval, and TLS settings.
///
/// # TLS Mapping
///
/// | `SslMode`    | `TlsConfig` method                     |
/// |--------------|----------------------------------------|
/// | `Disable`    | `disabled()`                           |
/// | `Prefer`     | `require()` (no prefer in upstream)    |
/// | `Require`    | `require()`                            |
/// | `VerifyCa`   | `verify_ca(ca_path)`                   |
/// | `VerifyFull` | `verify_full(ca_path)`                 |
///
/// Optional SNI hostname and mTLS client cert/key are chained as
/// builder steps when their config fields are set.
///
/// # Note
///
/// `pgwire-replication` manages slot creation externally — use
/// `ensure_replication_slot()` before calling this.
#[cfg(feature = "postgres-cdc")]
pub fn build_replication_config(
    config: &super::config::PostgresCdcConfig,
) -> pgwire_replication::ReplicationConfig {
    use std::path::PathBuf;

    use super::config::SslMode;

    let ca_path = config.ca_cert_path.as_ref().map(PathBuf::from);

    let tls = match config.ssl_mode {
        SslMode::Disable => pgwire_replication::TlsConfig::disabled(),
        SslMode::Prefer | SslMode::Require => pgwire_replication::TlsConfig::require(),
        SslMode::VerifyCa => pgwire_replication::TlsConfig::verify_ca(ca_path),
        SslMode::VerifyFull => pgwire_replication::TlsConfig::verify_full(ca_path),
    };

    // Apply optional SNI hostname
    let tls = if let Some(ref hostname) = config.sni_hostname {
        tls.with_sni_hostname(hostname)
    } else {
        tls
    };

    // Apply optional mTLS client certificate
    let tls = match (&config.client_cert_path, &config.client_key_path) {
        (Some(cert), Some(key)) => tls.with_client_cert(PathBuf::from(cert), PathBuf::from(key)),
        _ => tls,
    };

    let start_lsn = config
        .start_lsn
        .map_or(pgwire_replication::Lsn::ZERO, |lsn| {
            pgwire_replication::Lsn::from_u64(lsn.as_u64())
        });

    pgwire_replication::ReplicationConfig {
        host: config.host.clone(),
        port: config.port,
        user: config.username.clone(),
        password: config.password.clone().unwrap_or_default(),
        database: config.database.clone(),
        tls,
        slot: config.slot_name.clone(),
        publication: config.publication.clone(),
        start_lsn,
        stop_at_lsn: None,
        status_interval: config.keepalive_interval,
        idle_wakeup_interval: config.poll_timeout,
        buffer_events: 8192,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── XLogData parsing ──

    #[test]
    fn test_parse_xlog_data() {
        let mut msg = vec![b'w'];
        msg.extend_from_slice(&0x0000_0001_0000_0100_u64.to_be_bytes());
        msg.extend_from_slice(&0x0000_0001_0000_0200_u64.to_be_bytes());
        msg.extend_from_slice(&1_234_567_890_i64.to_be_bytes());
        msg.extend_from_slice(b"hello pgoutput");

        let parsed = parse_replication_message(&msg).unwrap();
        match parsed {
            ReplicationMessage::XLogData {
                wal_start,
                wal_end,
                server_time_us,
                data,
            } => {
                assert_eq!(wal_start, Lsn::new(0x0000_0001_0000_0100));
                assert_eq!(wal_end, Lsn::new(0x0000_0001_0000_0200));
                assert_eq!(server_time_us, 1_234_567_890);
                assert_eq!(data, b"hello pgoutput");
            }
            ReplicationMessage::PrimaryKeepalive { .. } => panic!("expected XLogData"),
        }
    }

    #[test]
    fn test_parse_xlog_data_empty_payload() {
        let mut msg = vec![b'w'];
        msg.extend_from_slice(&0_u64.to_be_bytes());
        msg.extend_from_slice(&0_u64.to_be_bytes());
        msg.extend_from_slice(&0_i64.to_be_bytes());

        let parsed = parse_replication_message(&msg).unwrap();
        match parsed {
            ReplicationMessage::XLogData { data, .. } => {
                assert!(data.is_empty());
            }
            ReplicationMessage::PrimaryKeepalive { .. } => panic!("expected XLogData"),
        }
    }

    // ── PrimaryKeepalive parsing ──

    #[test]
    fn test_parse_keepalive_reply_requested() {
        let mut msg = vec![b'k'];
        msg.extend_from_slice(&0x0000_0002_0000_0500_u64.to_be_bytes());
        msg.extend_from_slice(&9_876_543_210_i64.to_be_bytes());
        msg.push(1);

        let parsed = parse_replication_message(&msg).unwrap();
        match parsed {
            ReplicationMessage::PrimaryKeepalive {
                wal_end,
                server_time_us,
                reply_requested,
            } => {
                assert_eq!(wal_end, Lsn::new(0x0000_0002_0000_0500));
                assert_eq!(server_time_us, 9_876_543_210);
                assert!(reply_requested);
            }
            ReplicationMessage::XLogData { .. } => panic!("expected PrimaryKeepalive"),
        }
    }

    #[test]
    fn test_parse_keepalive_no_reply() {
        let mut msg = vec![b'k'];
        msg.extend_from_slice(&0x100_u64.to_be_bytes());
        msg.extend_from_slice(&0_i64.to_be_bytes());
        msg.push(0);

        let parsed = parse_replication_message(&msg).unwrap();
        match parsed {
            ReplicationMessage::PrimaryKeepalive {
                reply_requested, ..
            } => {
                assert!(!reply_requested);
            }
            ReplicationMessage::XLogData { .. } => panic!("expected PrimaryKeepalive"),
        }
    }

    // ── Error cases ──

    #[test]
    fn test_parse_empty_message() {
        let err = parse_replication_message(&[]).unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_parse_unknown_tag() {
        let err = parse_replication_message(&[0xFF]).unwrap_err();
        assert!(err.to_string().contains("unknown"));
        assert!(err.to_string().contains("0xFF"));
    }

    #[test]
    fn test_parse_truncated_xlog_data() {
        let msg = vec![b'w', 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let err = parse_replication_message(&msg).unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }

    #[test]
    fn test_parse_truncated_keepalive() {
        let msg = vec![b'k', 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let err = parse_replication_message(&msg).unwrap_err();
        assert!(err.to_string().contains("truncated"));
    }

    // ── Standby status encoding ──

    #[test]
    fn test_encode_standby_status_layout() {
        let write_lsn = Lsn::new(0x0000_0001_0000_0100);
        let flush_lsn = Lsn::new(0x0000_0001_0000_0080);
        let apply_lsn = Lsn::new(0x0000_0001_0000_0080);

        let buf = encode_standby_status(write_lsn, flush_lsn, apply_lsn);

        assert_eq!(buf.len(), 34, "standby status must be exactly 34 bytes");
        assert_eq!(buf[0], b'r', "tag must be 'r'");

        // write LSN at bytes 1-8
        let w = u64::from_be_bytes(buf[1..9].try_into().unwrap());
        assert_eq!(w, 0x0000_0001_0000_0100);

        // flush LSN at bytes 9-16
        let f = u64::from_be_bytes(buf[9..17].try_into().unwrap());
        assert_eq!(f, 0x0000_0001_0000_0080);

        // apply LSN at bytes 17-24
        let a = u64::from_be_bytes(buf[17..25].try_into().unwrap());
        assert_eq!(a, 0x0000_0001_0000_0080);

        // client timestamp at bytes 25-32 (we send 0)
        let ts = i64::from_be_bytes(buf[25..33].try_into().unwrap());
        assert_eq!(ts, 0);

        // reply requested at byte 33
        assert_eq!(buf[33], 0);
    }

    // ── START_REPLICATION query builder ──

    #[test]
    fn test_build_start_replication_query() {
        let query =
            build_start_replication_query("my_slot", "0/1234ABCD".parse().unwrap(), "my_pub")
                .unwrap();
        assert!(query.contains("START_REPLICATION SLOT my_slot LOGICAL 0/1234ABCD"));
        assert!(query.contains("proto_version '1'"));
        assert!(query.contains("publication_names 'my_pub'"));
    }

    #[test]
    fn test_build_start_replication_query_rejects_injection() {
        let result = build_start_replication_query(
            "slot'; DROP TABLE users; --",
            "0/0".parse().unwrap(),
            "pub",
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsafe characters"));
    }

    #[test]
    fn test_build_start_replication_query_rejects_empty() {
        let result = build_start_replication_query("", "0/0".parse().unwrap(), "pub");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_pg_identifier_accepts_valid() {
        assert!(validate_pg_identifier("my_slot_123", "test").is_ok());
    }

    // ── build_replication_config TLS mapping ──

    #[cfg(feature = "postgres-cdc")]
    mod tls_mapping_tests {
        use super::super::build_replication_config;
        use crate::cdc::postgres::config::{PostgresCdcConfig, SslMode};

        #[test]
        fn test_disable_maps_to_disabled() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.ssl_mode = SslMode::Disable;
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.mode, pgwire_replication::SslMode::Disable);
        }

        #[test]
        fn test_prefer_maps_to_require() {
            let cfg = PostgresCdcConfig::default(); // default is Prefer
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.mode, pgwire_replication::SslMode::Require);
        }

        #[test]
        fn test_require_maps_to_require() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.ssl_mode = SslMode::Require;
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.mode, pgwire_replication::SslMode::Require);
        }

        #[test]
        fn test_verify_ca_maps_with_ca_path() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.ssl_mode = SslMode::VerifyCa;
            cfg.ca_cert_path = Some("/certs/ca.pem".to_string());
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.mode, pgwire_replication::SslMode::VerifyCa);
            assert_eq!(
                repl.tls.ca_pem_path.as_deref(),
                Some(std::path::Path::new("/certs/ca.pem"))
            );
        }

        #[test]
        fn test_verify_full_maps_with_ca_path() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.ssl_mode = SslMode::VerifyFull;
            cfg.ca_cert_path = Some("/certs/ca.pem".to_string());
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.mode, pgwire_replication::SslMode::VerifyFull);
            assert_eq!(
                repl.tls.ca_pem_path.as_deref(),
                Some(std::path::Path::new("/certs/ca.pem"))
            );
        }

        #[test]
        fn test_sni_hostname_applied() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.sni_hostname = Some("db.example.com".to_string());
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.tls.sni_hostname.as_deref(), Some("db.example.com"));
        }

        #[test]
        fn test_mtls_client_cert_applied() {
            let mut cfg = PostgresCdcConfig::default();
            cfg.ssl_mode = SslMode::Require;
            cfg.client_cert_path = Some("/certs/client.pem".to_string());
            cfg.client_key_path = Some("/certs/client-key.pem".to_string());
            let repl = build_replication_config(&cfg);
            assert_eq!(
                repl.tls.client_cert_pem_path.as_deref(),
                Some(std::path::Path::new("/certs/client.pem"))
            );
            assert_eq!(
                repl.tls.client_key_pem_path.as_deref(),
                Some(std::path::Path::new("/certs/client-key.pem"))
            );
        }

        #[test]
        fn test_no_client_cert_when_not_set() {
            let cfg = PostgresCdcConfig::default();
            let repl = build_replication_config(&cfg);
            assert!(repl.tls.client_cert_pem_path.is_none());
            assert!(repl.tls.client_key_pem_path.is_none());
        }

        #[test]
        fn test_connection_fields_mapped() {
            let mut cfg = PostgresCdcConfig::new("pg.example.com", "mydb", "my_slot", "my_pub");
            cfg.port = 5433;
            cfg.username = "replicator".to_string();
            cfg.password = Some("secret".to_string());
            let repl = build_replication_config(&cfg);
            assert_eq!(repl.host, "pg.example.com");
            assert_eq!(repl.port, 5433);
            assert_eq!(repl.user, "replicator");
            assert_eq!(repl.password, "secret");
            assert_eq!(repl.database, "mydb");
            assert_eq!(repl.slot, "my_slot");
            assert_eq!(repl.publication, "my_pub");
        }
    }
}
