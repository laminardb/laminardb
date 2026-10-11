//! Logical replication slot creation over a replication-protocol session.
//!
//! The session ends before [`create_logical_slot`] returns, so a temporary slot exists only for
//! the duration of the call: it serves to read the visibility snapshot at a fresh consistent point.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;

use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::write_terminate;

use super::worker::{
    authenticate, execute_command, query_single_text_row, required_text_column, startup,
};

/// What `CREATE_REPLICATION_SLOT` does with the snapshot taken at the slot's consistent point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSnapshot {
    /// Read it as `pg_current_snapshot()` in the creating session's read-only transaction.
    Use,
    /// Discard it; the slot only streams changes after its consistent point.
    Nothing,
}

/// A newly created logical slot and the server identity observed before creation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedSlot {
    /// First LSN the slot streams.
    pub consistent_point: Lsn,
    /// `pg_current_snapshot()` text of the slot's snapshot, present only for
    /// [`SlotSnapshot::Use`]: it sees exactly the transactions the slot does not stream.
    pub visibility_snapshot: Option<String>,
    /// `IDENTIFY_SYSTEM` system identifier of the server that created the slot.
    pub system_identifier: u64,
    /// `IDENTIFY_SYSTEM` timeline of the server that created the slot.
    pub timeline_id: u32,
}

/// Create the logical `pgoutput` slot named by `cfg.slot`, never two-phase or failover-enabled.
///
/// A `temporary` slot is dropped when this call's session ends, before it returns. If this future
/// fails or is cancelled the session is closed, but the server may already have created a
/// persistent slot; the caller must reconcile that.
pub async fn create_logical_slot(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    temporary: bool,
) -> Result<CreatedSlot> {
    super::tokio_client::validate_config(cfg)?;

    #[cfg(unix)]
    if cfg.is_unix_socket() {
        if cfg.tls.mode.requires_tls() {
            return Err(PgWireError::Tls(
                "TLS is not supported over Unix domain sockets".into(),
            ));
        }
        let mut stream = UnixStream::connect(cfg.unix_socket_path()).await?;
        return create_and_close(cfg, snapshot, temporary, &mut stream).await;
    }

    let tcp = TcpStream::connect((cfg.host.as_str(), cfg.port)).await?;
    tcp.set_nodelay(true)?;

    #[cfg(feature = "tls-rustls")]
    {
        let mut stream = crate::tls::rustls::maybe_upgrade_to_tls(tcp, &cfg.tls, &cfg.host).await?;
        create_and_close(cfg, snapshot, temporary, &mut stream).await
    }

    #[cfg(not(feature = "tls-rustls"))]
    {
        if !matches!(cfg.tls.mode, crate::config::SslMode::Disable) {
            return Err(PgWireError::Tls("tls-rustls feature not enabled".into()));
        }
        let mut stream = tcp;
        create_and_close(cfg, snapshot, temporary, &mut stream).await
    }
}

async fn create_and_close<S: AsyncRead + AsyncWrite + Unpin>(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    temporary: bool,
    stream: &mut S,
) -> Result<CreatedSlot> {
    let created = create_on_stream(cfg, snapshot, temporary, stream).await;
    let _ = write_terminate(stream).await;
    created
}

async fn create_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    temporary: bool,
    stream: &mut S,
) -> Result<CreatedSlot> {
    startup(cfg, stream).await?;
    authenticate(cfg, stream).await?;
    let identity = query_single_text_row(cfg, stream, "IDENTIFY_SYSTEM", "IDENTIFY_SYSTEM").await?;
    let system_identifier = required_text_column(&identity, 0, "system identifier")?
        .parse::<u64>()
        .map_err(|error| PgWireError::Protocol(format!("invalid system identifier: {error}")))?;
    let timeline_id = required_text_column(&identity, 1, "timeline")?
        .parse::<u32>()
        .map_err(|error| PgWireError::Protocol(format!("invalid timeline: {error}")))?;
    let database = required_text_column(&identity, 3, "database name")?;
    if database != cfg.database {
        return Err(PgWireError::Configuration(format!(
            "PostgreSQL replication socket connected to database '{database}', expected '{}'",
            cfg.database
        )));
    }

    // `SNAPSHOT 'use'` must be the first command of a read-only repeatable-read transaction.
    if snapshot == SlotSnapshot::Use {
        execute_command(
            cfg,
            stream,
            "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ",
            "BEGIN",
        )
        .await?;
    }
    let row = query_single_text_row(
        cfg,
        stream,
        &create_slot_query(cfg, snapshot, temporary),
        "CREATE_REPLICATION_SLOT",
    )
    .await?;
    if row.len() != 4 {
        return Err(PgWireError::Protocol(format!(
            "CREATE_REPLICATION_SLOT returned {} columns, expected 4",
            row.len()
        )));
    }
    let consistent_point = required_text_column(&row, 1, "slot consistent point")?
        .parse::<Lsn>()
        .map_err(|error| PgWireError::Protocol(format!("invalid consistent point: {error}")))?;
    if row[2].is_some() {
        return Err(PgWireError::Protocol(
            "CREATE_REPLICATION_SLOT exported a snapshot it was not asked for".into(),
        ));
    }
    let visibility_snapshot = match snapshot {
        SlotSnapshot::Use => {
            let visible = query_single_text_row(
                cfg,
                stream,
                "SELECT pg_catalog.pg_current_snapshot()::text",
                "slot visibility snapshot",
            )
            .await?;
            let text = required_text_column(&visible, 0, "slot visibility snapshot")?.to_owned();
            execute_command(cfg, stream, "COMMIT", "COMMIT").await?;
            Some(text)
        }
        SlotSnapshot::Nothing => None,
    };
    Ok(CreatedSlot {
        consistent_point,
        visibility_snapshot,
        system_identifier,
        timeline_id,
    })
}

fn create_slot_query(cfg: &ReplicationConfig, snapshot: SlotSnapshot, temporary: bool) -> String {
    let action = match snapshot {
        SlotSnapshot::Use => "use",
        SlotSnapshot::Nothing => "nothing",
    };
    let temporary = if temporary { " TEMPORARY" } else { "" };
    // `validate_config` restricts the slot name to lower-case ASCII, digits, and underscore.
    format!(
        "CREATE_REPLICATION_SLOT {}{temporary} LOGICAL pgoutput (SNAPSHOT '{action}')",
        cfg.slot
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_creation_uses_the_validated_slot_name_and_snapshot_action() {
        let cfg = ReplicationConfig {
            slot: "orders_slot".into(),
            ..ReplicationConfig::default()
        };
        assert_eq!(
            create_slot_query(&cfg, SlotSnapshot::Use, false),
            "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput (SNAPSHOT 'use')"
        );
        assert_eq!(
            create_slot_query(&cfg, SlotSnapshot::Use, true),
            "CREATE_REPLICATION_SLOT orders_slot TEMPORARY LOGICAL pgoutput (SNAPSHOT 'use')"
        );
        assert_eq!(
            create_slot_query(&cfg, SlotSnapshot::Nothing, false),
            "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput (SNAPSHOT 'nothing')"
        );
    }

    async fn read_frontend(peer: &mut tokio::io::DuplexStream, tagged: bool) -> (u8, Vec<u8>) {
        use tokio::io::AsyncReadExt;

        let mut tag = [0_u8; 1];
        if tagged {
            peer.read_exact(&mut tag).await.unwrap();
        }
        let mut length = [0_u8; 4];
        peer.read_exact(&mut length).await.unwrap();
        let mut payload = vec![0_u8; usize::try_from(i32::from_be_bytes(length) - 4).unwrap()];
        peer.read_exact(&mut payload).await.unwrap();
        (tag[0], payload)
    }

    async fn write_backend(peer: &mut tokio::io::DuplexStream, tag: u8, payload: &[u8]) {
        use tokio::io::AsyncWriteExt;

        peer.write_all(&[tag]).await.unwrap();
        let length = i32::try_from(payload.len() + 4).unwrap();
        peer.write_all(&length.to_be_bytes()).await.unwrap();
        peer.write_all(payload).await.unwrap();
    }

    fn data_row(values: &[Option<&str>]) -> Vec<u8> {
        let mut payload = i16::try_from(values.len()).unwrap().to_be_bytes().to_vec();
        for value in values {
            match value {
                Some(value) => {
                    payload.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
                    payload.extend_from_slice(value.as_bytes());
                }
                None => payload.extend_from_slice(&(-1_i32).to_be_bytes()),
            }
        }
        payload
    }

    async fn reply_row(peer: &mut tokio::io::DuplexStream, values: &[Option<&str>]) {
        write_backend(peer, b'D', &data_row(values)).await;
        write_backend(peer, b'C', b"OK\0").await;
        write_backend(peer, b'Z', b"T").await;
    }

    async fn reply_command(peer: &mut tokio::io::DuplexStream, tag: &[u8]) {
        write_backend(peer, b'C', tag).await;
        write_backend(peer, b'Z', b"T").await;
    }

    async fn expect_query(peer: &mut tokio::io::DuplexStream, expected: &str) {
        let (tag, query) = read_frontend(peer, true).await;
        assert_eq!(tag, b'Q');
        assert_eq!(
            std::str::from_utf8(&query[..query.len() - 1]).unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn a_temporary_use_slot_reads_its_snapshot_in_one_read_only_transaction() {
        let cfg = ReplicationConfig {
            database: "orders".into(),
            slot: "orders_probe".into(),
            ..ReplicationConfig::default()
        };
        let (mut client, mut peer) = tokio::io::duplex(4096);
        let created = tokio::spawn(async move {
            create_and_close(&cfg, SlotSnapshot::Use, true, &mut client).await
        });

        read_frontend(&mut peer, false).await;
        write_backend(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend(&mut peer, b'Z', b"I").await;
        expect_query(&mut peer, "IDENTIFY_SYSTEM").await;
        reply_row(
            &mut peer,
            &[Some("42"), Some("1"), Some("0/200"), Some("orders")],
        )
        .await;
        expect_query(&mut peer, "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ").await;
        reply_command(&mut peer, b"BEGIN\0").await;
        expect_query(
            &mut peer,
            "CREATE_REPLICATION_SLOT orders_probe TEMPORARY LOGICAL pgoutput (SNAPSHOT 'use')",
        )
        .await;
        reply_row(
            &mut peer,
            &[Some("orders_probe"), Some("0/300"), None, Some("pgoutput")],
        )
        .await;
        expect_query(&mut peer, "SELECT pg_catalog.pg_current_snapshot()::text").await;
        reply_row(&mut peer, &[Some("907:909:907")]).await;
        expect_query(&mut peer, "COMMIT").await;
        reply_command(&mut peer, b"COMMIT\0").await;
        let (tag, _) = read_frontend(&mut peer, true).await;
        assert_eq!(tag, b'X', "the session ends, dropping the temporary slot");

        let created = created.await.unwrap().unwrap();
        assert_eq!(
            created,
            CreatedSlot {
                consistent_point: Lsn::from_u64(0x300),
                visibility_snapshot: Some("907:909:907".into()),
                system_identifier: 42,
                timeline_id: 1,
            }
        );
    }

    #[tokio::test]
    async fn a_server_error_inside_the_transaction_fails_the_creation() {
        let cfg = ReplicationConfig {
            database: "orders".into(),
            slot: "orders_slot".into(),
            ..ReplicationConfig::default()
        };
        let (mut client, mut peer) = tokio::io::duplex(4096);
        let created = tokio::spawn(async move {
            create_and_close(&cfg, SlotSnapshot::Use, false, &mut client).await
        });
        read_frontend(&mut peer, false).await;
        write_backend(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend(&mut peer, b'Z', b"I").await;
        expect_query(&mut peer, "IDENTIFY_SYSTEM").await;
        reply_row(
            &mut peer,
            &[Some("42"), Some("1"), Some("0/200"), Some("orders")],
        )
        .await;
        expect_query(&mut peer, "BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ").await;
        write_backend(
            &mut peer,
            b'E',
            b"SERROR\0C25001\0Mmust be called before any query\0\0",
        )
        .await;
        write_backend(&mut peer, b'Z', b"E").await;
        let (tag, _) = read_frontend(&mut peer, true).await;
        assert_eq!(tag, b'X');
        let error = created.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("25001"), "{error}");
    }

    #[tokio::test]
    async fn startup_announces_the_configured_application_name() {
        let cfg = ReplicationConfig {
            application_name: "laminar:0123456789abcdef:89abcdef".into(),
            ..ReplicationConfig::default()
        };
        let (mut client, mut peer) = tokio::io::duplex(4096);
        startup(&cfg, &mut client).await.unwrap();
        let (_, payload) = read_frontend(&mut peer, false).await;
        let expected = b"application_name\0laminar:0123456789abcdef:89abcdef\0";
        assert!(
            payload
                .windows(expected.len())
                .any(|window| window == expected),
            "{}",
            String::from_utf8_lossy(&payload)
        );
    }

    #[tokio::test]
    async fn invalid_slot_name_fails_before_connecting() {
        let cfg = ReplicationConfig {
            slot: "Bad Slot".into(),
            ..ReplicationConfig::default()
        };
        let error = create_logical_slot(&cfg, SlotSnapshot::Use, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("slot"), "{error}");
    }
}
