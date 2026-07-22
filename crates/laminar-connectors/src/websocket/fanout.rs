//! Bounded shared fan-out for the WebSocket server sink.

use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::broadcast;
use tungstenite::Utf8Bytes;

use crate::error::{ConnectorError, SerdeError};

pub(super) const MAX_SERVER_FRAME_BYTES: usize = 256 * 1024;
pub(super) const SERVER_BROADCAST_CAPACITY: usize = 32;

const DATA_PREFIX: &[u8] = b"{\"type\":\"data\",\"data\":[";
const SEQUENCE_PREFIX: &[u8] = b"],\"sequence\":";
const MAX_SEQUENCE_DIGITS: usize = 20;

/// Shared bounded broadcast ring. Each encoded frame is retained once and
/// cloned cheaply for every subscribed socket.
pub struct FanoutManager {
    sender: broadcast::Sender<Utf8Bytes>,
    next_client_id: AtomicU64,
    sequence: AtomicU64,
    frame_bytes: usize,
    capacity: usize,
}

impl FanoutManager {
    /// Creates the production broadcast ring.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(MAX_SERVER_FRAME_BYTES, SERVER_BROADCAST_CAPACITY)
    }

    fn with_limits(frame_bytes: usize, capacity: usize) -> Self {
        let (sender, receiver) = broadcast::channel(capacity);
        drop(receiver);
        Self {
            sender,
            next_client_id: AtomicU64::new(1),
            sequence: AtomicU64::new(0),
            frame_bytes,
            capacity,
        }
    }

    /// Subscribes a client to future frames.
    pub fn subscribe(&self) -> (u64, broadcast::Receiver<Utf8Bytes>) {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        (id, self.sender.subscribe())
    }

    #[must_use]
    pub fn client_count(&self) -> usize {
        self.sender.receiver_count()
    }

    /// Preflights, encodes, and publishes one logical batch as ordered frames.
    /// No frame is published if a row or the complete burst exceeds a bound.
    pub fn publish_rows(&self, rows: &[Vec<u8>]) -> Result<BroadcastResult, ConnectorError> {
        if rows.is_empty() {
            return Ok(BroadcastResult {
                sequence: self.sequence.load(Ordering::Relaxed),
                frames: 0,
                payload_bytes: 0,
                transport_bytes: 0,
                receiver_enqueues: 0,
            });
        }

        for (index, row) in rows.iter().enumerate() {
            std::str::from_utf8(row).map_err(|error| {
                ConnectorError::Serde(SerdeError::MalformedInput(format!(
                    "WebSocket sink row {index} is not UTF-8: {error}"
                )))
            })?;
        }

        let mut boundaries = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let end = self.next_frame_end(rows, start)?;
            boundaries.push((start, end));
            if boundaries.len() > self.capacity {
                return Err(ConnectorError::WriteError(format!(
                    "WebSocket server batch requires more than {} frames; split the input batch",
                    self.capacity
                )));
            }
            start = end;
        }

        let first_sequence = self.reserve_sequences(boundaries.len())?;
        let mut frames = Vec::with_capacity(boundaries.len());
        let mut total_bytes = 0usize;
        for (index, (start, end)) in boundaries.into_iter().enumerate() {
            let offset = u64::try_from(index).map_err(|_| {
                ConnectorError::WriteError("WebSocket server sequence range is too large".into())
            })?;
            let sequence = first_sequence.checked_add(offset).ok_or_else(|| {
                ConnectorError::WriteError("WebSocket server sequence exhausted".into())
            })?;
            let frame = encode_data_frame(&rows[start..end], sequence)?;
            total_bytes = total_bytes
                .checked_add(frame.len())
                .ok_or_else(wire_size_overflow)?;
            frames.push(frame);
        }

        let frame_count = frames.len();
        let mut receiver_enqueues = 0u64;
        let mut transport_bytes = 0u64;
        for frame in frames {
            let frame_bytes = u64::try_from(frame.len()).unwrap_or(u64::MAX);
            if let Ok(receivers) = self.sender.send(frame) {
                let receivers = u64::try_from(receivers).unwrap_or(u64::MAX);
                receiver_enqueues = receiver_enqueues.saturating_add(receivers);
                transport_bytes =
                    transport_bytes.saturating_add(frame_bytes.saturating_mul(receivers));
            }
        }

        let frame_count_u64 = u64::try_from(frame_count).map_err(|_| {
            ConnectorError::WriteError("WebSocket server frame count is too large".into())
        })?;
        Ok(BroadcastResult {
            sequence: first_sequence + frame_count_u64 - 1,
            frames: frame_count,
            payload_bytes: total_bytes,
            transport_bytes,
            receiver_enqueues,
        })
    }

    fn next_frame_end(&self, rows: &[Vec<u8>], start: usize) -> Result<usize, ConnectorError> {
        let envelope_bytes = DATA_PREFIX
            .len()
            .checked_add(SEQUENCE_PREFIX.len())
            .and_then(|bytes| bytes.checked_add(MAX_SEQUENCE_DIGITS + 1))
            .ok_or_else(wire_size_overflow)?;
        let mut bytes = envelope_bytes;
        let mut end = start;

        while end < rows.len() {
            let separator = usize::from(end > start);
            let next_bytes = bytes
                .checked_add(separator)
                .and_then(|size| size.checked_add(rows[end].len()))
                .ok_or_else(wire_size_overflow)?;
            if next_bytes > self.frame_bytes {
                if end == start {
                    return Err(ConnectorError::WriteError(format!(
                        "WebSocket server row {start} requires {next_bytes} bytes, exceeding the {}-byte frame limit",
                        self.frame_bytes
                    )));
                }
                break;
            }
            bytes = next_bytes;
            end += 1;
        }
        Ok(end)
    }

    fn reserve_sequences(&self, frame_count: usize) -> Result<u64, ConnectorError> {
        let amount = u64::try_from(frame_count).map_err(|_| {
            ConnectorError::WriteError("WebSocket server sequence range is too large".into())
        })?;
        let previous = self
            .sequence
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(amount)
            })
            .map_err(|_| {
                ConnectorError::WriteError("WebSocket server sequence exhausted".into())
            })?;
        previous
            .checked_add(1)
            .ok_or_else(|| ConnectorError::WriteError("WebSocket server sequence exhausted".into()))
    }
}

impl Default for FanoutManager {
    fn default() -> Self {
        Self::new()
    }
}

fn encode_data_frame(rows: &[Vec<u8>], sequence: u64) -> Result<Utf8Bytes, ConnectorError> {
    let row_bytes = rows.iter().try_fold(0usize, |total, row| {
        total.checked_add(row.len()).ok_or_else(wire_size_overflow)
    })?;
    let sequence = sequence.to_string();
    let capacity = DATA_PREFIX
        .len()
        .checked_add(row_bytes)
        .and_then(|size| size.checked_add(rows.len().saturating_sub(1)))
        .and_then(|size| size.checked_add(SEQUENCE_PREFIX.len()))
        .and_then(|size| size.checked_add(sequence.len() + 1))
        .ok_or_else(wire_size_overflow)?;
    let mut data = Vec::with_capacity(capacity);
    data.extend_from_slice(DATA_PREFIX);
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            data.push(b',');
        }
        data.extend_from_slice(row);
    }
    data.extend_from_slice(SEQUENCE_PREFIX);
    data.extend_from_slice(sequence.as_bytes());
    data.push(b'}');
    Utf8Bytes::try_from(Bytes::from(data)).map_err(|error| {
        ConnectorError::Serde(SerdeError::MalformedInput(format!(
            "WebSocket sink produced invalid UTF-8: {error}"
        )))
    })
}

fn wire_size_overflow() -> ConnectorError {
    ConnectorError::Serde(SerdeError::MalformedInput(
        "WebSocket sink wire message size overflow".into(),
    ))
}

impl std::fmt::Debug for FanoutManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutManager")
            .field("clients", &self.client_count())
            .field("sequence", &self.sequence.load(Ordering::Relaxed))
            .field("frame_bytes", &self.frame_bytes)
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastResult {
    /// Sequence assigned to the final frame.
    pub sequence: u64,
    /// Number of frames published for the logical batch.
    pub frames: usize,
    /// Encoded payload bytes retained once by the shared ring.
    pub payload_bytes: usize,
    /// Estimated transport bytes across subscribed receivers.
    pub transport_bytes: u64,
    /// Sum of receiver counts returned by each frame publish.
    pub receiver_enqueues: u64,
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn row(value: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({ "value": value })).unwrap()
    }

    fn payload(message: &Utf8Bytes) -> serde_json::Value {
        serde_json::from_str::<serde_json::Value>(message.as_ref()).unwrap()
    }

    #[tokio::test]
    async fn one_shared_publish_reaches_every_subscriber() {
        let manager = FanoutManager::with_limits(1024, 4);
        let (_first_id, mut first) = manager.subscribe();
        let (_second_id, mut second) = manager.subscribe();

        let result = manager.publish_rows(&[row("hello")]).unwrap();

        assert_eq!(result.frames, 1);
        assert_eq!(result.receiver_enqueues, 2);
        assert_eq!(
            payload(&first.recv().await.unwrap())["data"][0]["value"],
            "hello"
        );
        assert_eq!(payload(&second.recv().await.unwrap())["sequence"], 1);
    }

    #[tokio::test]
    async fn multi_frame_batch_does_not_evict_itself() {
        let rows = [row("a"), row("b"), row("c")];
        let two_row_bytes = encode_data_frame(&rows[..2], u64::MAX).unwrap().len();
        let manager = FanoutManager::with_limits(two_row_bytes, 2);
        let (_id, mut receiver) = manager.subscribe();

        let result = manager.publish_rows(&rows).unwrap();

        assert_eq!(result.frames, 2);
        assert_eq!(payload(&receiver.recv().await.unwrap())["sequence"], 1);
        assert_eq!(payload(&receiver.recv().await.unwrap())["sequence"], 2);
    }

    #[tokio::test]
    async fn oversized_row_fails_before_delivery_or_sequence_reservation() {
        let rows = [row("small"), row(&"x".repeat(128))];
        let first_row_bytes = encode_data_frame(&rows[..1], u64::MAX).unwrap().len();
        let manager = FanoutManager::with_limits(first_row_bytes, 2);
        let (_id, mut receiver) = manager.subscribe();

        let error = manager.publish_rows(&rows).unwrap_err().to_string();

        assert!(error.contains("row 1"), "{error}");
        assert_eq!(manager.sequence.load(Ordering::Relaxed), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn over_capacity_burst_fails_before_delivery() {
        let rows = [row("a"), row("b"), row("c")];
        let one_row_bytes = encode_data_frame(&rows[..1], u64::MAX).unwrap().len();
        let manager = FanoutManager::with_limits(one_row_bytes, 2);
        let (_id, mut receiver) = manager.subscribe();

        let error = manager.publish_rows(&rows).unwrap_err().to_string();

        assert!(error.contains("more than 2 frames"), "{error}");
        assert_eq!(manager.sequence.load(Ordering::Relaxed), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn lag_is_reported_by_the_shared_ring() {
        let manager = FanoutManager::with_limits(1024, 2);
        let (_id, mut receiver) = manager.subscribe();
        manager.publish_rows(&[row("a")]).unwrap();
        manager.publish_rows(&[row("b")]).unwrap();
        manager.publish_rows(&[row("c")]).unwrap();

        assert!(matches!(
            receiver.recv().await,
            Err(broadcast::error::RecvError::Lagged(1))
        ));
    }
}
