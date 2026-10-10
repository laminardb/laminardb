//! Logical replication slot creation over a replication-protocol session.
//!
//! An exported snapshot is importable only while the session that created the
//! slot stays open and idle, so a background task owns that session and keeps
//! it open until [`CreatedSlot::release`] or drop.

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::ReplicationConfig;
use crate::error::{PgWireError, Result};
use crate::lsn::Lsn;
use crate::protocol::framing::write_terminate;

use super::worker::{authenticate, query_single_text_row, required_text_column, startup};

/// What `CREATE_REPLICATION_SLOT` does with the snapshot taken at the slot's consistent point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotSnapshot {
    /// Export it for import by `SET TRANSACTION SNAPSHOT` in another session.
    Export,
    /// Discard it; the slot only streams changes after its consistent point.
    Nothing,
}

/// A newly created logical slot and the server identity observed before creation.
pub struct CreatedSlot {
    /// First LSN the slot streams; the exported snapshot sees exactly the
    /// transactions committed before it.
    pub consistent_point: Lsn,
    /// Name of the exported snapshot, present only for [`SlotSnapshot::Export`].
    pub snapshot_name: Option<String>,
    /// `IDENTIFY_SYSTEM` system identifier of the server that created the slot.
    pub system_identifier: u64,
    /// `IDENTIFY_SYSTEM` timeline of the server that created the slot.
    pub timeline_id: u32,
    release: Option<oneshot::Sender<()>>,
    session: Option<JoinHandle<Result<()>>>,
}

impl CreatedSlot {
    /// Close the creating session. An exported snapshot stops being importable,
    /// so call this only after every importer ran `SET TRANSACTION SNAPSHOT`.
    pub async fn release(mut self) -> Result<()> {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        match self.session.take() {
            Some(session) => session
                .await
                .map_err(|error| PgWireError::Task(format!("slot session task: {error}")))?,
            None => Ok(()),
        }
    }
}

impl Drop for CreatedSlot {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        if let Some(session) = self.session.take() {
            session.abort();
        }
    }
}

impl std::fmt::Debug for CreatedSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CreatedSlot")
            .field("consistent_point", &self.consistent_point)
            .field("snapshot_name", &self.snapshot_name)
            .field("system_identifier", &self.system_identifier)
            .field("timeline_id", &self.timeline_id)
            .finish_non_exhaustive()
    }
}

/// Create the persistent logical `pgoutput` slot named by `cfg.slot`.
///
/// The slot is never temporary, two-phase, or failover-enabled. If this future
/// fails or is cancelled the session is closed, but the server may already have
/// created the slot; the caller must reconcile that.
pub async fn create_logical_slot(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
) -> Result<CreatedSlot> {
    super::tokio_client::validate_config(cfg)?;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let task_cfg = cfg.clone();
    let session = SessionTask(Some(tokio::spawn(async move {
        run_slot_session(&task_cfg, snapshot, ready_tx, release_rx).await
    })));
    match ready_rx.await {
        Ok(Ok(mut created)) => {
            created.release = Some(release_tx);
            created.session = session.into_inner();
            Ok(created)
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(PgWireError::Task(
            "slot session ended before reporting slot creation".into(),
        )),
    }
}

/// Aborts the session task unless ownership moved into a [`CreatedSlot`].
struct SessionTask(Option<JoinHandle<Result<()>>>);

impl SessionTask {
    fn into_inner(mut self) -> Option<JoinHandle<Result<()>>> {
        self.0.take()
    }
}

impl Drop for SessionTask {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

async fn run_slot_session(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    ready: oneshot::Sender<Result<CreatedSlot>>,
    release: oneshot::Receiver<()>,
) -> Result<()> {
    #[cfg(unix)]
    if cfg.is_unix_socket() {
        if cfg.tls.mode.requires_tls() {
            return publish_failure(
                ready,
                PgWireError::Tls("TLS is not supported over Unix domain sockets".into()),
            );
        }
        let mut stream = match UnixStream::connect(cfg.unix_socket_path()).await {
            Ok(stream) => stream,
            Err(error) => return publish_failure(ready, error.into()),
        };
        return hold_slot_session(cfg, snapshot, &mut stream, ready, release).await;
    }

    let tcp = match TcpStream::connect((cfg.host.as_str(), cfg.port)).await {
        Ok(tcp) => tcp,
        Err(error) => return publish_failure(ready, error.into()),
    };
    if let Err(error) = tcp.set_nodelay(true) {
        return publish_failure(ready, error.into());
    }

    #[cfg(feature = "tls-rustls")]
    {
        let mut stream =
            match crate::tls::rustls::maybe_upgrade_to_tls(tcp, &cfg.tls, &cfg.host).await {
                Ok(stream) => stream,
                Err(error) => return publish_failure(ready, error),
            };
        hold_slot_session(cfg, snapshot, &mut stream, ready, release).await
    }

    #[cfg(not(feature = "tls-rustls"))]
    {
        if !matches!(cfg.tls.mode, crate::config::SslMode::Disable) {
            return publish_failure(
                ready,
                PgWireError::Tls("tls-rustls feature not enabled".into()),
            );
        }
        let mut stream = tcp;
        hold_slot_session(cfg, snapshot, &mut stream, ready, release).await
    }
}

fn publish_failure(ready: oneshot::Sender<Result<CreatedSlot>>, error: PgWireError) -> Result<()> {
    let _ = ready.send(Err(error.clone()));
    Err(error)
}

async fn hold_slot_session<S: AsyncRead + AsyncWrite + Unpin>(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    stream: &mut S,
    ready: oneshot::Sender<Result<CreatedSlot>>,
    release: oneshot::Receiver<()>,
) -> Result<()> {
    let created = create_on_stream(cfg, snapshot, stream).await;
    let failure = created.as_ref().err().cloned();
    if ready.send(created).is_err() {
        let _ = write_terminate(stream).await;
        return Ok(());
    }
    if let Some(error) = failure {
        return Err(error);
    }
    // Any further command would end the exported snapshot, so the session idles until released.
    let _ = release.await;
    write_terminate(stream).await
}

async fn create_on_stream<S: AsyncRead + AsyncWrite + Unpin>(
    cfg: &ReplicationConfig,
    snapshot: SlotSnapshot,
    stream: &mut S,
) -> Result<CreatedSlot> {
    startup(cfg, stream).await?;
    authenticate(cfg, stream).await?;
    // IDENTIFY_SYSTEM must precede slot creation: any later command ends the exported snapshot.
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

    let row = query_single_text_row(
        cfg,
        stream,
        &create_slot_query(cfg, snapshot),
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
    let snapshot_name = row[2].clone();
    if (snapshot == SlotSnapshot::Export) != snapshot_name.is_some() {
        return Err(PgWireError::Protocol(
            "CREATE_REPLICATION_SLOT snapshot name does not match the requested snapshot action"
                .into(),
        ));
    }
    Ok(CreatedSlot {
        consistent_point,
        snapshot_name,
        system_identifier,
        timeline_id,
        release: None,
        session: None,
    })
}

fn create_slot_query(cfg: &ReplicationConfig, snapshot: SlotSnapshot) -> String {
    let action = match snapshot {
        SlotSnapshot::Export => "export",
        SlotSnapshot::Nothing => "nothing",
    };
    // `validate_config` restricts the slot name to lower-case ASCII, digits, and underscore.
    format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT '{action}')",
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
            create_slot_query(&cfg, SlotSnapshot::Export),
            "CREATE_REPLICATION_SLOT orders_slot LOGICAL pgoutput (SNAPSHOT 'export')"
        );
        assert_eq!(
            create_slot_query(&cfg, SlotSnapshot::Nothing),
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
        write_backend(peer, b'C', b"OK ").await;
        write_backend(peer, b'Z', b"I").await;
    }

    #[tokio::test]
    async fn exporting_session_idles_until_release_then_terminates() {
        let cfg = ReplicationConfig {
            database: "orders".into(),
            slot: "orders_slot".into(),
            ..ReplicationConfig::default()
        };
        let (mut client, mut peer) = tokio::io::duplex(4096);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let session = tokio::spawn(async move {
            hold_slot_session(
                &cfg,
                SlotSnapshot::Export,
                &mut client,
                ready_tx,
                release_rx,
            )
            .await
        });

        read_frontend(&mut peer, false).await;
        write_backend(&mut peer, b'R', &0_i32.to_be_bytes()).await;
        write_backend(&mut peer, b'Z', b"I").await;
        let (_, identify) = read_frontend(&mut peer, true).await;
        assert_eq!(&identify[..identify.len() - 1], b"IDENTIFY_SYSTEM");
        reply_row(
            &mut peer,
            &[Some("42"), Some("1"), Some("0/200"), Some("orders")],
        )
        .await;
        let (_, create) = read_frontend(&mut peer, true).await;
        assert!(std::str::from_utf8(&create)
            .unwrap()
            .contains("SNAPSHOT 'export'"));
        reply_row(
            &mut peer,
            &[
                Some("orders_slot"),
                Some("0/300"),
                Some("00000003-1"),
                Some("pgoutput"),
            ],
        )
        .await;

        let created = ready_rx.await.unwrap().unwrap();
        assert_eq!(created.consistent_point, Lsn::from_u64(0x300));
        assert_eq!(created.snapshot_name.as_deref(), Some("00000003-1"));
        assert_eq!(created.system_identifier, 42);
        let idle = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            read_frontend(&mut peer, true),
        )
        .await;
        assert!(
            idle.is_err(),
            "no command may follow the export before release"
        );

        release_tx.send(()).unwrap();
        let (tag, _) = read_frontend(&mut peer, true).await;
        assert_eq!(tag, b'X');
        session.await.unwrap().unwrap();
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
        let error = create_logical_slot(&cfg, SlotSnapshot::Export)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("slot"), "{error}");
    }
}
