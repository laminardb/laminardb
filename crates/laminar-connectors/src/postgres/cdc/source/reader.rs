//! Raw WAL reader task, bounded queue admission, and terminal error delivery.

use super::{Arc, Bytes, Notify, OwnedSemaphorePermit, Semaphore};

/// Single-consumer async receiver for the WAL reader → `poll_batch` queue.
pub(super) type WalPayloadRx = crossfire::AsyncRx<crossfire::mpsc::Array<OwnedWalPayload>>;

pub(super) type WalPayloadTx = crossfire::MAsyncTx<crossfire::mpsc::Array<OwnedWalPayload>>;

pub(super) type WalTerminalError = Arc<std::sync::Mutex<Option<String>>>;

/// WAL event payload sent from the background reader task to `PostgresCdcSource::poll_batch`.
pub(super) enum WalPayload {
    Begin {
        final_lsn: u64,
        commit_ts_us: i64,
    },
    Commit {
        end_lsn: u64,
        commit_ts_us: i64,
        lsn: u64,
    },
    XLogData {
        wal_end: u64,
        data: Bytes,
    },
    KeepAlive {
        wal_end: u64,
    },
}

pub(super) struct OwnedWalPayload {
    pub(super) payload: WalPayload,
    pub(super) _byte_permit: OwnedSemaphorePermit,
    /// Held only to keep the replication worker's in-flight reservation until decoding ends.
    pub(super) _wire_bytes: Option<pgwire_replication::WireBytesGuard>,
}

pub(super) fn retained_wal_payload_bytes(payload: &WalPayload) -> usize {
    let dynamic_bytes = match payload {
        WalPayload::XLogData { data, .. } => data.len(),
        WalPayload::Begin { .. } | WalPayload::Commit { .. } | WalPayload::KeepAlive { .. } => 0,
    };
    std::mem::size_of::<OwnedWalPayload>()
        .saturating_add(dynamic_bytes)
        .max(1)
}

pub(super) fn logical_wal_payload_bytes(payload: &WalPayload) -> usize {
    match payload {
        WalPayload::Begin { .. } => 1 + 8 + 8 + 4,
        WalPayload::Commit { .. } => 1 + 1 + 8 + 8 + 8,
        WalPayload::XLogData { data, .. } => data.len(),
        WalPayload::KeepAlive { .. } => 0,
    }
}

#[cfg(test)]
pub(super) async fn send_wal_or_shutdown(
    tx: &WalPayloadTx,
    payload: WalPayload,
    byte_budget: &Arc<Semaphore>,
    max_payload_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, String> {
    send_wal_with_wire_guard(
        tx,
        payload,
        None,
        byte_budget,
        max_payload_bytes,
        shutdown_rx,
    )
    .await
}

pub(super) async fn send_wal_with_wire_guard(
    tx: &WalPayloadTx,
    payload: WalPayload,
    wire_bytes: Option<pgwire_replication::WireBytesGuard>,
    byte_budget: &Arc<Semaphore>,
    max_payload_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, String> {
    if *shutdown_rx.borrow() {
        return Ok(false);
    }

    let retained_bytes = retained_wal_payload_bytes(&payload);
    if retained_bytes > max_payload_bytes {
        return Err(format!(
            "PostgreSQL CDC WAL payload exceeds the hard raw buffer limit \
             (retained bytes: {retained_bytes}/{max_payload_bytes})"
        ));
    }
    let permits = u32::try_from(retained_bytes)
        .map_err(|_| "PostgreSQL CDC raw WAL byte budget exceeds semaphore capacity".to_string())?;
    let permit = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return Ok(false),
        result = Arc::clone(byte_budget).acquire_many_owned(permits) => result.map_err(|_| {
            "PostgreSQL CDC raw WAL byte budget closed unexpectedly".to_string()
        })?,
    };
    let owned = OwnedWalPayload {
        payload,
        _byte_permit: permit,
        _wire_bytes: wire_bytes,
    };
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => Ok(false),
        result = tx.send(owned) => Ok(result.is_ok()),
    }
}

pub(super) fn publish_terminal_wal_error(
    error: &WalTerminalError,
    message: String,
    data_ready: &Notify,
) {
    let mut slot = error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if slot.is_none() {
        *slot = Some(message);
    }
    drop(slot);
    data_ready.notify_one();
}

type ReaderPayload = (WalPayload, Option<pgwire_replication::WireBytesGuard>);

/// Map one replication event to the payload handed to `poll_batch`, or the terminal reason
/// the stream cannot continue.
fn reader_payload(event: pgwire_replication::ReplicationEvent) -> Result<ReaderPayload, String> {
    use pgwire_replication::ReplicationEvent;

    Ok(match event {
        ReplicationEvent::Begin {
            final_lsn,
            commit_time_micros,
            ..
        } => (
            WalPayload::Begin {
                final_lsn: final_lsn.as_u64(),
                commit_ts_us: commit_time_micros,
            },
            None,
        ),
        ReplicationEvent::Commit {
            end_lsn,
            commit_time_micros,
            lsn,
        } => (
            WalPayload::Commit {
                end_lsn: end_lsn.as_u64(),
                commit_ts_us: commit_time_micros,
                lsn: lsn.as_u64(),
            },
            None,
        ),
        ReplicationEvent::XLogData {
            wal_end,
            data,
            wire_bytes,
            ..
        } => (
            WalPayload::XLogData {
                wal_end: wal_end.as_u64(),
                data,
            },
            Some(wire_bytes),
        ),
        ReplicationEvent::KeepAlive { wal_end, .. } => (
            WalPayload::KeepAlive {
                wal_end: wal_end.as_u64(),
            },
            None,
        ),
        ReplicationEvent::Message { .. } => {
            return Err(
                "PostgreSQL emitted a logical decoding message even though replication \
                 was started with messages=false"
                    .to_string(),
            );
        }
        ReplicationEvent::StoppedAt { reached } => {
            return Err(format!(
                "PostgreSQL replication stopped unexpectedly at {reached}; no stop LSN was \
                 configured"
            ));
        }
    })
}

pub(super) async fn run_wal_reader(
    mut client: pgwire_replication::ReplicationClient,
    wal_tx: WalPayloadTx,
    byte_budget: Arc<Semaphore>,
    byte_limit: usize,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    terminal_error: WalTerminalError,
    data_ready: Arc<Notify>,
    _reader_guard: crate::connector::ConnectorTaskGuard,
) {
    let terminal = loop {
        tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break None;
                }
            }
            event = client.recv() => {
                let (payload, wire_bytes) = match event {
                    Ok(Some(event)) => match reader_payload(event) {
                        Ok(payload) => payload,
                        Err(reason) => break Some(reason),
                    },
                    Ok(None) => {
                        break Some("PostgreSQL replication stream ended unexpectedly".into());
                    }
                    Err(error) => {
                        break Some(format!("PostgreSQL replication stream failed: {error}"));
                    }
                };
                match send_wal_with_wire_guard(
                    &wal_tx,
                    payload,
                    wire_bytes,
                    &byte_budget,
                    byte_limit,
                    &mut shutdown_rx,
                )
                .await
                {
                    Ok(true) => data_ready.notify_one(),
                    Ok(false) => break None,
                    Err(message) => break Some(message),
                }
            }
        }
    };
    if let Some(message) = terminal {
        publish_terminal_wal_error(&terminal_error, message, &data_ready);
    }
    let _ = client.shutdown().await;
}
