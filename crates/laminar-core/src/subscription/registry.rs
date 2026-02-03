//! Subscription Registry — Ring 2 lifecycle management.
//!
//! Manages the lifecycle of all active subscriptions: create, pause, resume,
//! cancel. Provides the mapping from `source_id` (used by Ring 0 notifications)
//! to the set of active subscriptions and their broadcast channels (used by the
//! Ring 1 dispatcher).
//!
//! # Thread Safety
//!
//! The registry is designed for concurrent access:
//! - **Read** operations (`get_senders_for_source`, `metrics`) take a read lock
//!   and can proceed concurrently with each other.
//! - **Write** operations (`create`, `cancel`, `pause`, `resume`) take a write
//!   lock but are infrequent Ring 2 operations with no latency requirements.
//!
//! # Architecture
//!
//! ```text
//! Ring 2 (Control Plane)          Ring 1 (Dispatcher)
//! ┌─────────────────────┐         ┌──────────────────────┐
//! │ create / cancel     │         │ get_senders_for_source│
//! │ pause / resume      │  ────►  │ (read lock, O(1))     │
//! │ (write lock, rare)  │         └──────────────────────┘
//! └─────────────────────┘
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use tokio::sync::broadcast;

use crate::subscription::event::ChangeEvent;

// ---------------------------------------------------------------------------
// SubscriptionId
// ---------------------------------------------------------------------------

/// Unique subscription identifier.
///
/// Monotonically assigned by [`SubscriptionRegistry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sub-{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// SubscriptionState
// ---------------------------------------------------------------------------

/// Lifecycle state of a subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Actively receiving events.
    Active,
    /// Temporarily paused (events are buffered or dropped per config).
    Paused,
    /// Cancelled and pending cleanup.
    Cancelled,
}

// ---------------------------------------------------------------------------
// BackpressureStrategy
// ---------------------------------------------------------------------------

/// Strategy applied when a subscription's channel buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// Drop oldest events when buffer full (real-time priority).
    DropOldest,
    /// Drop newest events when buffer full (completeness priority).
    DropNewest,
    /// Block the dispatcher (only blocks Ring 1 dispatch, NOT Ring 0).
    Block,
    /// Sample: deliver every Nth event.
    Sample(usize),
}

// ---------------------------------------------------------------------------
// SubscriptionConfig
// ---------------------------------------------------------------------------

/// Configuration for a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionConfig {
    /// Channel buffer capacity.
    pub buffer_size: usize,
    /// Backpressure strategy when buffer is full.
    pub backpressure: BackpressureStrategy,
    /// Optional filter predicate (evaluated in Ring 1).
    pub filter: Option<String>,
    /// Whether to send an initial snapshot on subscribe.
    pub send_snapshot: bool,
    /// Maximum batch size for delivery.
    pub max_batch_size: usize,
    /// Maximum batch delay before flushing (microseconds).
    pub max_batch_delay_us: u64,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,
            backpressure: BackpressureStrategy::DropOldest,
            filter: None,
            send_snapshot: false,
            max_batch_size: 64,
            max_batch_delay_us: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// SubscriptionEntry
// ---------------------------------------------------------------------------

/// A registered subscription entry with state, config, channel, and metrics.
#[derive(Debug)]
pub struct SubscriptionEntry {
    /// Unique ID.
    pub id: SubscriptionId,
    /// Target source/MV name.
    pub source_name: String,
    /// Source ID for Ring 0 notification matching.
    pub source_id: u32,
    /// Current lifecycle state.
    pub state: SubscriptionState,
    /// Configuration.
    pub config: SubscriptionConfig,
    /// Broadcast channel sender for this subscription.
    pub sender: broadcast::Sender<ChangeEvent>,
    /// Creation timestamp.
    pub created_at: Instant,
    /// Total events delivered.
    pub events_delivered: u64,
    /// Total events dropped (backpressure).
    pub events_dropped: u64,
    /// Current lag (events pending in channel).
    pub current_lag: u64,
}

// ---------------------------------------------------------------------------
// SubscriptionMetrics
// ---------------------------------------------------------------------------

/// Point-in-time metrics snapshot for a subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionMetrics {
    /// Subscription ID.
    pub id: SubscriptionId,
    /// Source name.
    pub source_name: String,
    /// Current state.
    pub state: SubscriptionState,
    /// Total events delivered.
    pub events_delivered: u64,
    /// Total events dropped.
    pub events_dropped: u64,
    /// Current lag.
    pub current_lag: u64,
    /// Time since creation.
    pub age: Duration,
}

// ---------------------------------------------------------------------------
// SubscriptionRegistry
// ---------------------------------------------------------------------------

/// Registry managing all active subscriptions.
///
/// Thread-safe via internal [`RwLock`]. Read operations (used by the Ring 1
/// dispatcher) take a read lock. Write operations (create/cancel) take a write
/// lock but are rare Ring 2 operations.
///
/// Three indices provide O(1) lookups:
/// - `subscriptions`: by [`SubscriptionId`]
/// - `by_source`: by `source_id` (u32) — used by the dispatcher
/// - `by_name`: by source name (String) — used by admin API
///
/// # Panics
///
/// All methods on this type panic if an internal `RwLock` has been poisoned
/// (i.e., a thread panicked while holding the lock). This should not occur
/// under normal operation.
pub struct SubscriptionRegistry {
    /// All subscriptions by ID.
    subscriptions: RwLock<HashMap<SubscriptionId, SubscriptionEntry>>,
    /// Index: `source_id` → subscription IDs.
    by_source: RwLock<HashMap<u32, Vec<SubscriptionId>>>,
    /// Index: source name → subscription IDs.
    by_name: RwLock<HashMap<String, Vec<SubscriptionId>>>,
    /// Next subscription ID (monotonically increasing).
    next_id: AtomicU64,
}

#[allow(clippy::missing_panics_doc)] // All methods panic only on poisoned RwLock
impl SubscriptionRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            by_source: RwLock::new(HashMap::new()),
            by_name: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Creates a new subscription for the given source.
    ///
    /// Returns the subscription ID and a broadcast [`Receiver`](broadcast::Receiver)
    /// that the subscriber uses to receive [`ChangeEvent`]s.
    ///
    /// # Arguments
    ///
    /// * `source_name` — Name of the MV or streaming query.
    /// * `source_id` — Ring 0 source identifier (from [`NotificationHub`]).
    /// * `config` — Subscription configuration (buffer size, backpressure, etc.).
    pub fn create(
        &self,
        source_name: String,
        source_id: u32,
        config: SubscriptionConfig,
    ) -> (SubscriptionId, broadcast::Receiver<ChangeEvent>) {
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = broadcast::channel(config.buffer_size);

        let entry = SubscriptionEntry {
            id,
            source_name: source_name.clone(),
            source_id,
            state: SubscriptionState::Active,
            config,
            sender: tx,
            created_at: Instant::now(),
            events_delivered: 0,
            events_dropped: 0,
            current_lag: 0,
        };

        // Insert into main map
        self.subscriptions.write().unwrap().insert(id, entry);

        // Insert into source index
        self.by_source
            .write()
            .unwrap()
            .entry(source_id)
            .or_default()
            .push(id);

        // Insert into name index
        self.by_name
            .write()
            .unwrap()
            .entry(source_name)
            .or_default()
            .push(id);

        (id, rx)
    }

    /// Pauses an active subscription.
    ///
    /// Returns `true` if the subscription was `Active` and is now `Paused`.
    /// Returns `false` if the subscription does not exist or is not active.
    pub fn pause(&self, id: SubscriptionId) -> bool {
        let mut subs = self.subscriptions.write().unwrap();
        if let Some(entry) = subs.get_mut(&id) {
            if entry.state == SubscriptionState::Active {
                entry.state = SubscriptionState::Paused;
                return true;
            }
        }
        false
    }

    /// Resumes a paused subscription.
    ///
    /// Returns `true` if the subscription was `Paused` and is now `Active`.
    /// Returns `false` if the subscription does not exist or is not paused.
    pub fn resume(&self, id: SubscriptionId) -> bool {
        let mut subs = self.subscriptions.write().unwrap();
        if let Some(entry) = subs.get_mut(&id) {
            if entry.state == SubscriptionState::Paused {
                entry.state = SubscriptionState::Active;
                return true;
            }
        }
        false
    }

    /// Cancels a subscription and removes it from all indices.
    ///
    /// Returns `true` if the subscription existed and was removed.
    pub fn cancel(&self, id: SubscriptionId) -> bool {
        let entry = self.subscriptions.write().unwrap().remove(&id);

        if let Some(entry) = entry {
            // Remove from source index
            if let Some(ids) = self.by_source.write().unwrap().get_mut(&entry.source_id) {
                ids.retain(|&i| i != id);
            }

            // Remove from name index
            if let Some(ids) = self.by_name.write().unwrap().get_mut(&entry.source_name) {
                ids.retain(|&i| i != id);
            }

            true
        } else {
            false
        }
    }

    /// Returns broadcast senders for all active subscriptions of a source.
    ///
    /// Called by the Ring 1 dispatcher on every notification. Uses a read lock
    /// for fast concurrent access.
    #[must_use]
    pub fn get_senders_for_source(&self, source_id: u32) -> Vec<broadcast::Sender<ChangeEvent>> {
        let by_source = self.by_source.read().unwrap();
        let Some(ids) = by_source.get(&source_id) else {
            return Vec::new();
        };

        let subs = self.subscriptions.read().unwrap();
        ids.iter()
            .filter_map(|id| {
                subs.get(id).and_then(|entry| {
                    if entry.state == SubscriptionState::Active {
                        Some(entry.sender.clone())
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Returns subscription IDs for the given source name.
    #[must_use]
    pub fn get_subscriptions_by_name(&self, name: &str) -> Vec<SubscriptionId> {
        let by_name = self.by_name.read().unwrap();
        by_name.get(name).cloned().unwrap_or_default()
    }

    /// Returns the total number of registered subscriptions.
    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().unwrap().len()
    }

    /// Returns the number of active subscriptions.
    #[must_use]
    pub fn active_count(&self) -> usize {
        self.subscriptions
            .read()
            .unwrap()
            .values()
            .filter(|e| e.state == SubscriptionState::Active)
            .count()
    }

    /// Returns a metrics snapshot for the given subscription.
    #[must_use]
    pub fn metrics(&self, id: SubscriptionId) -> Option<SubscriptionMetrics> {
        let subs = self.subscriptions.read().unwrap();
        subs.get(&id).map(|entry| SubscriptionMetrics {
            id: entry.id,
            source_name: entry.source_name.clone(),
            state: entry.state,
            events_delivered: entry.events_delivered,
            events_dropped: entry.events_dropped,
            current_lag: entry.current_lag,
            age: entry.created_at.elapsed(),
        })
    }

    /// Returns the state of a subscription.
    #[must_use]
    pub fn state(&self, id: SubscriptionId) -> Option<SubscriptionState> {
        self.subscriptions.read().unwrap().get(&id).map(|e| e.state)
    }

    /// Increments the delivered event count for a subscription.
    ///
    /// Called by the Ring 1 dispatcher after successful delivery.
    pub fn record_delivery(&self, id: SubscriptionId, count: u64) {
        if let Some(entry) = self.subscriptions.write().unwrap().get_mut(&id) {
            entry.events_delivered += count;
        }
    }

    /// Increments the dropped event count for a subscription.
    ///
    /// Called by the Ring 1 dispatcher on backpressure.
    pub fn record_drop(&self, id: SubscriptionId, count: u64) {
        if let Some(entry) = self.subscriptions.write().unwrap().get_mut(&id) {
            entry.events_dropped += count;
        }
    }
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    // --- Config tests ---

    #[test]
    fn test_registry_config_default() {
        let cfg = SubscriptionConfig::default();
        assert_eq!(cfg.buffer_size, 1024);
        assert_eq!(cfg.backpressure, BackpressureStrategy::DropOldest);
        assert!(cfg.filter.is_none());
        assert!(!cfg.send_snapshot);
        assert_eq!(cfg.max_batch_size, 64);
        assert_eq!(cfg.max_batch_delay_us, 100);
    }

    // --- Create tests ---

    #[test]
    fn test_registry_create() {
        let reg = SubscriptionRegistry::new();
        let (id, _rx) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());
        assert_eq!(id.0, 1);
        assert_eq!(reg.subscription_count(), 1);
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn test_registry_create_multiple() {
        let reg = SubscriptionRegistry::new();
        let (id1, _rx1) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());
        let (id2, _rx2) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());
        let (id3, _rx3) = reg.create("mv_trades".into(), 1, SubscriptionConfig::default());

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_eq!(reg.subscription_count(), 3);

        // Two subs for source 0, one for source 1
        let senders_0 = reg.get_senders_for_source(0);
        assert_eq!(senders_0.len(), 2);
        let senders_1 = reg.get_senders_for_source(1);
        assert_eq!(senders_1.len(), 1);
    }

    // --- Pause / Resume tests ---

    #[test]
    fn test_registry_pause_resume() {
        let reg = SubscriptionRegistry::new();
        let (id, _rx) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());

        // Pause active -> true
        assert!(reg.pause(id));
        assert_eq!(reg.state(id), Some(SubscriptionState::Paused));
        assert_eq!(reg.active_count(), 0);

        // Pause again -> false (already paused)
        assert!(!reg.pause(id));

        // Resume paused -> true
        assert!(reg.resume(id));
        assert_eq!(reg.state(id), Some(SubscriptionState::Active));
        assert_eq!(reg.active_count(), 1);

        // Resume again -> false (already active)
        assert!(!reg.resume(id));
    }

    // --- Cancel tests ---

    #[test]
    fn test_registry_cancel() {
        let reg = SubscriptionRegistry::new();
        let (id, _rx) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());
        assert_eq!(reg.subscription_count(), 1);

        assert!(reg.cancel(id));
        assert_eq!(reg.subscription_count(), 0);
        assert_eq!(reg.active_count(), 0);

        // Source index cleaned up
        let senders = reg.get_senders_for_source(0);
        assert!(senders.is_empty());

        // Name index cleaned up
        let by_name = reg.get_subscriptions_by_name("mv_orders");
        assert!(by_name.is_empty());
    }

    #[test]
    fn test_registry_cancel_nonexistent() {
        let reg = SubscriptionRegistry::new();
        assert!(!reg.cancel(SubscriptionId(999)));
    }

    // --- Sender lookup tests ---

    #[test]
    fn test_registry_get_senders() {
        let reg = SubscriptionRegistry::new();
        let (_, _rx1) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, _rx2) = reg.create("mv_b".into(), 0, SubscriptionConfig::default());

        let senders = reg.get_senders_for_source(0);
        assert_eq!(senders.len(), 2);
    }

    #[test]
    fn test_registry_get_senders_paused_excluded() {
        let reg = SubscriptionRegistry::new();
        let (id1, _rx1) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, _rx2) = reg.create("mv_b".into(), 0, SubscriptionConfig::default());

        reg.pause(id1);
        let senders = reg.get_senders_for_source(0);
        assert_eq!(senders.len(), 1);
    }

    #[test]
    fn test_registry_get_senders_no_source() {
        let reg = SubscriptionRegistry::new();
        let senders = reg.get_senders_for_source(42);
        assert!(senders.is_empty());
    }

    // --- Metrics tests ---

    #[test]
    fn test_registry_subscription_count() {
        let reg = SubscriptionRegistry::new();
        assert_eq!(reg.subscription_count(), 0);
        assert_eq!(reg.active_count(), 0);

        let (id1, _rx1) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, _rx2) = reg.create("mv_b".into(), 1, SubscriptionConfig::default());
        assert_eq!(reg.subscription_count(), 2);
        assert_eq!(reg.active_count(), 2);

        reg.pause(id1);
        assert_eq!(reg.subscription_count(), 2);
        assert_eq!(reg.active_count(), 1);
    }

    #[test]
    fn test_registry_metrics() {
        let reg = SubscriptionRegistry::new();
        let (id, _rx) = reg.create("mv_orders".into(), 0, SubscriptionConfig::default());

        let m = reg.metrics(id).unwrap();
        assert_eq!(m.id, id);
        assert_eq!(m.source_name, "mv_orders");
        assert_eq!(m.state, SubscriptionState::Active);
        assert_eq!(m.events_delivered, 0);
        assert_eq!(m.events_dropped, 0);
        assert_eq!(m.current_lag, 0);

        // Nonexistent subscription
        assert!(reg.metrics(SubscriptionId(999)).is_none());
    }

    #[test]
    fn test_registry_record_delivery_and_drop() {
        let reg = SubscriptionRegistry::new();
        let (id, _rx) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());

        reg.record_delivery(id, 10);
        reg.record_delivery(id, 5);
        reg.record_drop(id, 2);

        let m = reg.metrics(id).unwrap();
        assert_eq!(m.events_delivered, 15);
        assert_eq!(m.events_dropped, 2);
    }

    // --- Thread safety tests ---

    #[test]
    fn test_registry_thread_safety() {
        let reg = Arc::new(SubscriptionRegistry::new());
        let mut handles = Vec::new();

        // Spawn 4 threads, each creating 100 subscriptions
        for t in 0..4u32 {
            let reg = Arc::clone(&reg);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..100u32 {
                    let name = format!("mv_{t}_{i}");
                    let (id, _rx) = reg.create(name, t, SubscriptionConfig::default());
                    ids.push(id);
                }
                ids
            }));
        }

        let all_ids: Vec<Vec<SubscriptionId>> =
            handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All 400 subscriptions created
        assert_eq!(reg.subscription_count(), 400);

        // All IDs unique
        let mut flat: Vec<u64> = all_ids.iter().flatten().map(|id| id.0).collect();
        flat.sort_unstable();
        flat.dedup();
        assert_eq!(flat.len(), 400);

        // Each source has 100 senders
        for t in 0..4u32 {
            let senders = reg.get_senders_for_source(t);
            assert_eq!(senders.len(), 100);
        }

        // Cancel half from thread 0
        for id in &all_ids[0][..50] {
            assert!(reg.cancel(*id));
        }
        assert_eq!(reg.subscription_count(), 350);
        assert_eq!(reg.get_senders_for_source(0).len(), 50);
    }

    // --- Integration tests ---

    #[test]
    fn test_registry_with_notification_hub() {
        use crate::subscription::NotificationHub;

        let mut hub = NotificationHub::new(4, 64);
        let reg = SubscriptionRegistry::new();

        // Register a source in the hub, then subscribe to it in the registry
        let source_id = hub.register_source().unwrap();
        let (sub_id, _rx) =
            reg.create("mv_orders".into(), source_id, SubscriptionConfig::default());

        // Verify the mapping works
        let senders = reg.get_senders_for_source(source_id);
        assert_eq!(senders.len(), 1);

        // Notify through hub
        assert!(hub.notify_source(
            source_id,
            crate::subscription::EventType::Insert,
            10,
            1000,
            0,
        ));

        // Drain from hub
        let mut count = 0;
        hub.drain_notifications(|_n| count += 1);
        assert_eq!(count, 1);

        // Cleanup
        reg.cancel(sub_id);
        assert!(reg.get_senders_for_source(source_id).is_empty());
    }

    #[test]
    fn test_registry_broadcast_delivery() {
        let reg = SubscriptionRegistry::new();
        let (_, mut rx1) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, mut rx2) = reg.create("mv_a".into(), 0, SubscriptionConfig::default());

        // Get senders and broadcast
        let senders = reg.get_senders_for_source(0);
        assert_eq!(senders.len(), 2);

        let batch = Arc::new(make_batch(5));
        let event = ChangeEvent::insert(batch, 1000, 1);

        for sender in &senders {
            sender.send(event.clone()).unwrap();
        }

        // Both receivers get the event
        let e1 = rx1.try_recv().unwrap();
        assert_eq!(e1.timestamp(), 1000);
        assert_eq!(e1.sequence(), Some(1));
        assert_eq!(e1.row_count(), 5);

        let e2 = rx2.try_recv().unwrap();
        assert_eq!(e2.timestamp(), 1000);
        assert_eq!(e2.sequence(), Some(1));
    }

    #[test]
    fn test_subscription_id_display() {
        let id = SubscriptionId(42);
        assert_eq!(format!("{id}"), "sub-42");
    }
}
