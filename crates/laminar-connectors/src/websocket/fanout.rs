//! Per-client fan-out manager for WebSocket sink server mode.
//!
//! Manages per-client state, bounded send buffers, and slow client
//! eviction. Each connected client gets its own `tokio::sync::mpsc`
//! channel so that a slow client cannot block or affect other clients.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use tracing::{debug, warn};

use super::sink_config::SlowClientPolicy;

/// Unique identifier for a connected WebSocket client.
pub type ClientId = u64;

/// Per-client state within the fan-out manager.
#[derive(Debug)]
pub struct ClientState {
    /// Bounded send channel for this client.
    pub tx: tokio::sync::mpsc::Sender<Bytes>,
    /// Client's subscription filter expression (if any).
    pub filter: Option<String>,
    /// Subscription ID assigned to this client.
    pub subscription_id: String,
    /// Desired output format (reserved for per-client format negotiation).
    pub format: Option<super::sink_config::SinkFormat>,
    /// Number of messages dropped for this client.
    pub messages_dropped: AtomicU64,
}

/// Circular replay buffer for client resume support.
#[derive(Debug)]
pub struct ReplayBuffer {
    /// Ring buffer of `(sequence, serialized_message)`.
    buffer: VecDeque<(u64, Bytes)>,
    /// Maximum number of entries.
    max_size: usize,
}

impl ReplayBuffer {
    /// Creates a new replay buffer with the given capacity.
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(max_size.min(10_000)),
            max_size,
        }
    }

    /// Appends a message to the replay buffer, evicting the oldest if full.
    pub fn push(&mut self, sequence: u64, data: Bytes) {
        if self.buffer.len() >= self.max_size {
            self.buffer.pop_front();
        }
        self.buffer.push_back((sequence, data));
    }

    /// Returns all messages with sequence number > `from_sequence`.
    #[must_use]
    pub fn replay_from(&self, from_sequence: u64) -> Vec<(u64, Bytes)> {
        self.buffer
            .iter()
            .filter(|(seq, _)| *seq > from_sequence)
            .cloned()
            .collect()
    }

    /// Returns the lowest sequence number in the buffer, if any.
    #[must_use]
    pub fn oldest_sequence(&self) -> Option<u64> {
        self.buffer.front().map(|(seq, _)| *seq)
    }

    /// Returns the number of entries in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Returns whether the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}

/// Fan-out manager that distributes messages to connected WebSocket clients.
///
/// Each client gets an independent bounded channel. The fan-out loop
/// serializes once, then attempts to send to every client. If a client's
/// channel is full, the configured [`SlowClientPolicy`] is applied.
pub struct FanoutManager {
    /// Connected clients keyed by ID.
    clients: Arc<RwLock<HashMap<ClientId, ClientState>>>,
    /// Slow client eviction policy.
    policy: SlowClientPolicy,
    /// Per-client send buffer capacity (in messages).
    buffer_capacity: usize,
    /// Next client ID.
    next_id: AtomicU64,
    /// Global sequence counter for messages.
    sequence: AtomicU64,
    /// Optional replay buffer.
    replay_buffer: Option<parking_lot::Mutex<ReplayBuffer>>,
}

impl FanoutManager {
    /// Creates a new fan-out manager.
    ///
    /// # Arguments
    ///
    /// * `policy` - Slow client eviction policy.
    /// * `buffer_capacity` - Max queued messages per client.
    /// * `replay_buffer_size` - If `Some`, enables a replay buffer of this size.
    #[must_use]
    pub fn new(
        policy: SlowClientPolicy,
        buffer_capacity: usize,
        replay_buffer_size: Option<usize>,
    ) -> Self {
        let replay_buffer =
            replay_buffer_size.map(|size| parking_lot::Mutex::new(ReplayBuffer::new(size)));
        Self {
            clients: Arc::new(RwLock::new(HashMap::new())),
            policy,
            buffer_capacity: buffer_capacity.max(1),
            next_id: AtomicU64::new(1),
            sequence: AtomicU64::new(0),
            replay_buffer,
        }
    }

    /// Registers a new client and returns its ID and receive channel.
    pub fn add_client(
        &self,
        subscription_id: String,
        filter: Option<String>,
        format: Option<super::sink_config::SinkFormat>,
    ) -> (ClientId, tokio::sync::mpsc::Receiver<Bytes>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(self.buffer_capacity);

        let state = ClientState {
            tx,
            filter,
            subscription_id,
            format,
            messages_dropped: AtomicU64::new(0),
        };

        self.clients.write().insert(id, state);
        debug!(client_id = id, "client registered");
        (id, rx)
    }

    /// Removes a client by ID, returning whether it existed.
    pub fn remove_client(&self, id: ClientId) -> bool {
        let removed = self.clients.write().remove(&id).is_some();
        if removed {
            debug!(client_id = id, "client removed");
        }
        removed
    }

    /// Returns the number of connected clients.
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.clients.read().len()
    }

    /// Returns the current global sequence number.
    #[must_use]
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::Relaxed)
    }

    /// Returns a clone of the clients map handle for external use.
    #[must_use]
    pub fn clients(&self) -> Arc<RwLock<HashMap<ClientId, ClientState>>> {
        Arc::clone(&self.clients)
    }

    /// Broadcasts serialized data to all connected clients.
    ///
    /// Applies the slow client policy for clients that can't keep up.
    /// Returns the number of clients that received the message successfully.
    #[allow(clippy::needless_pass_by_value)]
    pub fn broadcast(&self, data: Bytes) -> BroadcastResult {
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;

        // Store in replay buffer if enabled.
        if let Some(ref replay) = self.replay_buffer {
            replay.lock().push(seq, data.clone());
        }

        let clients = self.clients.read();
        let mut sent = 0u64;
        let mut dropped = 0u64;
        let mut disconnected: Vec<ClientId> = Vec::new();

        for (&id, state) in clients.iter() {
            match state.tx.try_send(data.clone()) {
                Ok(()) => {
                    sent += 1;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    match &self.policy {
                        SlowClientPolicy::DropOldest | SlowClientPolicy::DropNewest => {
                            state.messages_dropped.fetch_add(1, Ordering::Relaxed);
                            dropped += 1;
                        }
                        SlowClientPolicy::Disconnect { .. } => {
                            disconnected.push(id);
                        }
                        SlowClientPolicy::WarnThenDisconnect {
                            warn_pct: _,
                            disconnect_pct: _,
                        } => {
                            // Approximate: if drops exceed threshold, disconnect.
                            let total_drops =
                                state.messages_dropped.fetch_add(1, Ordering::Relaxed) + 1;
                            if total_drops > self.buffer_capacity as u64 {
                                disconnected.push(id);
                            } else {
                                dropped += 1;
                            }
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    disconnected.push(id);
                }
            }
        }
        drop(clients);

        // Remove disconnected clients outside the read lock.
        if !disconnected.is_empty() {
            let mut clients = self.clients.write();
            for id in &disconnected {
                clients.remove(id);
                warn!(client_id = id, "slow/disconnected client removed");
            }
        }

        BroadcastResult {
            sequence: seq,
            sent,
            dropped,
            disconnected: disconnected.len() as u64,
        }
    }

    /// Returns replay messages for a client resuming from a given sequence.
    #[must_use]
    pub fn replay_from(&self, from_sequence: u64) -> Vec<(u64, Bytes)> {
        match &self.replay_buffer {
            Some(replay) => replay.lock().replay_from(from_sequence),
            None => Vec::new(),
        }
    }
}

impl std::fmt::Debug for FanoutManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FanoutManager")
            .field("clients", &self.client_count())
            .field("sequence", &self.current_sequence())
            .field("buffer_capacity", &self.buffer_capacity)
            .finish_non_exhaustive()
    }
}

/// Result of a broadcast operation.
#[derive(Debug, Clone)]
pub struct BroadcastResult {
    /// Sequence number assigned to this message.
    pub sequence: u64,
    /// Number of clients that received the message.
    pub sent: u64,
    /// Number of clients where the message was dropped.
    pub dropped: u64,
    /// Number of clients that were disconnected.
    pub disconnected: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_buffer() {
        let mut buf = ReplayBuffer::new(3);
        buf.push(1, Bytes::from("a"));
        buf.push(2, Bytes::from("b"));
        buf.push(3, Bytes::from("c"));

        assert_eq!(buf.len(), 3);
        assert_eq!(buf.oldest_sequence(), Some(1));

        let replay = buf.replay_from(1);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].0, 2);
        assert_eq!(replay[1].0, 3);
    }

    #[test]
    fn test_replay_buffer_eviction() {
        let mut buf = ReplayBuffer::new(2);
        buf.push(1, Bytes::from("a"));
        buf.push(2, Bytes::from("b"));
        buf.push(3, Bytes::from("c")); // evicts seq 1

        assert_eq!(buf.len(), 2);
        assert_eq!(buf.oldest_sequence(), Some(2));

        let replay = buf.replay_from(0);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].0, 2);
    }

    #[test]
    fn test_fanout_add_remove_client() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropOldest, 10, None);

        let (id1, _rx1) = mgr.add_client("sub1".into(), None, None);
        let (id2, _rx2) = mgr.add_client("sub2".into(), None, None);
        assert_eq!(mgr.client_count(), 2);

        assert!(mgr.remove_client(id1));
        assert_eq!(mgr.client_count(), 1);

        assert!(!mgr.remove_client(id1)); // already removed
        assert!(mgr.remove_client(id2));
        assert_eq!(mgr.client_count(), 0);
    }

    #[tokio::test]
    async fn test_fanout_broadcast() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropOldest, 10, None);
        let (_id1, mut rx1) = mgr.add_client("sub1".into(), None, None);
        let (_id2, mut rx2) = mgr.add_client("sub2".into(), None, None);

        let result = mgr.broadcast(Bytes::from("hello"));
        assert_eq!(result.sent, 2);
        assert_eq!(result.dropped, 0);
        assert_eq!(result.sequence, 1);

        let msg1 = rx1.recv().await.unwrap();
        assert_eq!(msg1.as_ref(), b"hello");

        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg2.as_ref(), b"hello");
    }

    #[test]
    fn test_fanout_slow_client_drop() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropNewest, 2, None);
        let (_id, _rx) = mgr.add_client("sub1".into(), None, None);

        // Fill the buffer.
        mgr.broadcast(Bytes::from("a"));
        mgr.broadcast(Bytes::from("b"));

        // This should be dropped (buffer full, DropNewest policy).
        let result = mgr.broadcast(Bytes::from("c"));
        assert_eq!(result.dropped, 1);
    }

    #[test]
    fn test_fanout_disconnect_policy() {
        let mgr = FanoutManager::new(SlowClientPolicy::Disconnect { threshold_pct: 80 }, 2, None);
        let (_id, _rx) = mgr.add_client("sub1".into(), None, None);

        mgr.broadcast(Bytes::from("a"));
        mgr.broadcast(Bytes::from("b"));

        // Buffer full → client disconnected.
        let result = mgr.broadcast(Bytes::from("c"));
        assert_eq!(result.disconnected, 1);
        assert_eq!(mgr.client_count(), 0);
    }

    #[test]
    fn test_fanout_with_replay() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropOldest, 10, Some(5));

        mgr.broadcast(Bytes::from("a"));
        mgr.broadcast(Bytes::from("b"));
        mgr.broadcast(Bytes::from("c"));

        let replay = mgr.replay_from(1);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].1.as_ref(), b"b");
        assert_eq!(replay[1].1.as_ref(), b"c");
    }

    #[tokio::test]
    async fn test_fanout_closed_client_removed() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropOldest, 10, None);
        let (_id, rx) = mgr.add_client("sub1".into(), None, None);

        // Drop the receiver to simulate client disconnect.
        drop(rx);

        let result = mgr.broadcast(Bytes::from("hello"));
        assert_eq!(result.disconnected, 1);
        assert_eq!(mgr.client_count(), 0);
    }

    #[test]
    fn test_fanout_sequence_increments() {
        let mgr = FanoutManager::new(SlowClientPolicy::DropOldest, 10, None);

        let r1 = mgr.broadcast(Bytes::from("a"));
        let r2 = mgr.broadcast(Bytes::from("b"));
        let r3 = mgr.broadcast(Bytes::from("c"));

        assert_eq!(r1.sequence, 1);
        assert_eq!(r2.sequence, 2);
        assert_eq!(r3.sequence, 3);
    }
}
