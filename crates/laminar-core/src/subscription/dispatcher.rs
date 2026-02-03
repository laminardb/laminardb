//! Subscription Dispatcher — Ring 1 notification routing.
//!
//! Bridges Ring 0 [`NotificationRef`] signals to subscriber broadcast channels.
//! Runs as an async task that:
//!
//! 1. **Drains** all [`NotificationRing`]s from Ring 0 (one per core).
//! 2. **Batches** notifications by `source_id` for efficient lookup.
//! 3. **Resolves** each [`NotificationRef`] into a [`ChangeEvent`] via
//!    [`NotificationDataSource`].
//! 4. **Broadcasts** the event to all active subscribers found in the
//!    [`SubscriptionRegistry`].
//!
//! # Architecture
//!
//! ```text
//! Ring 0 (per core)           Ring 1 (Dispatcher)           Ring 2 (Subscribers)
//! ┌──────────────┐            ┌───────────────────┐         ┌──────────────┐
//! │ NotifRing[0] │──drain──►  │                   │──send──►│ Receiver A   │
//! │ NotifRing[1] │──drain──►  │  Dispatcher loop  │──send──►│ Receiver B   │
//! │ NotifRing[N] │──drain──►  │                   │──send──►│ Receiver C   │
//! └──────────────┘            └───────────────────┘         └──────────────┘
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;

use crate::subscription::event::{ChangeEvent, NotificationRef};
use crate::subscription::notification::NotificationRing;
use crate::subscription::registry::SubscriptionRegistry;

// ---------------------------------------------------------------------------
// NotificationDataSource — resolver trait
// ---------------------------------------------------------------------------

/// Resolves a [`NotificationRef`] into a full [`ChangeEvent`].
///
/// Implemented by the DAG executor or streaming pipeline to provide zero-copy
/// access to the source data referenced by [`NotificationRef::batch_offset`].
pub trait NotificationDataSource: Send + Sync {
    /// Resolves a notification reference to a full change event.
    ///
    /// Returns `None` if the referenced data is no longer available (e.g.,
    /// the ring buffer has been overwritten). The dispatcher will skip the
    /// notification and increment the drop counter.
    fn resolve(&self, notif: &NotificationRef) -> Option<ChangeEvent>;
}

// ---------------------------------------------------------------------------
// DispatcherConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`SubscriptionDispatcher`].
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Maximum notifications to drain per poll cycle.
    pub max_drain_per_cycle: usize,
    /// Idle sleep duration when no notifications are available.
    pub idle_sleep: Duration,
    /// Number of spin iterations before yielding on idle.
    pub spin_iterations: usize,
    /// Whether to batch notifications by source before dispatch.
    pub batch_by_source: bool,
    /// Maximum batch size per source per cycle.
    pub max_batch_per_source: usize,
}

impl Default for DispatcherConfig {
    fn default() -> Self {
        Self {
            max_drain_per_cycle: 4096,
            idle_sleep: Duration::from_micros(10),
            spin_iterations: 100,
            batch_by_source: true,
            max_batch_per_source: 256,
        }
    }
}

// ---------------------------------------------------------------------------
// DispatcherMetrics
// ---------------------------------------------------------------------------

/// Atomic counters for monitoring the dispatcher.
#[derive(Debug, Default)]
pub struct DispatcherMetrics {
    /// Total notifications drained from Ring 0.
    pub notifications_drained: AtomicU64,
    /// Total events dispatched to subscribers.
    pub events_dispatched: AtomicU64,
    /// Total events dropped (resolve failure or no receivers).
    pub events_dropped: AtomicU64,
    /// Total dispatch cycles.
    pub dispatch_cycles: AtomicU64,
    /// Total idle cycles (nothing to dispatch).
    pub idle_cycles: AtomicU64,
    /// Maximum dispatch latency observed (nanoseconds).
    pub max_dispatch_latency_ns: AtomicU64,
}

impl DispatcherMetrics {
    /// Returns total notifications drained.
    #[must_use]
    pub fn notifications_drained(&self) -> u64 {
        self.notifications_drained.load(Ordering::Relaxed)
    }

    /// Returns total events dispatched.
    #[must_use]
    pub fn events_dispatched(&self) -> u64 {
        self.events_dispatched.load(Ordering::Relaxed)
    }

    /// Returns total events dropped.
    #[must_use]
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
    }

    /// Returns total dispatch cycles.
    #[must_use]
    pub fn dispatch_cycles(&self) -> u64 {
        self.dispatch_cycles.load(Ordering::Relaxed)
    }

    /// Returns total idle cycles.
    #[must_use]
    pub fn idle_cycles(&self) -> u64 {
        self.idle_cycles.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// SubscriptionDispatcher
// ---------------------------------------------------------------------------

/// Ring 1 dispatcher that routes notifications to subscriber channels.
///
/// The dispatcher drains [`NotificationRing`]s from Ring 0, resolves
/// [`NotificationRef`] values to [`ChangeEvent`] instances, and broadcasts
/// them to active subscriber channels via [`SubscriptionRegistry`].
///
/// # Usage
///
/// ```rust,ignore
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
/// let dispatcher = SubscriptionDispatcher::new(
///     rings, registry, data_source, config, shutdown_rx,
/// );
/// let handle = tokio::spawn(dispatcher.run());
///
/// // ... later ...
/// shutdown_tx.send(true).unwrap();
/// handle.await.unwrap();
/// ```
pub struct SubscriptionDispatcher {
    /// Notification rings from Ring 0 (one per core).
    notification_rings: Vec<Arc<NotificationRing>>,
    /// Subscription registry for subscriber lookup.
    registry: Arc<SubscriptionRegistry>,
    /// Data source for resolving notifications to events.
    data_source: Arc<dyn NotificationDataSource>,
    /// Configuration.
    config: DispatcherConfig,
    /// Metrics.
    metrics: Arc<DispatcherMetrics>,
    /// Shutdown signal receiver.
    shutdown: watch::Receiver<bool>,
}

impl SubscriptionDispatcher {
    /// Creates a new dispatcher.
    #[must_use]
    pub fn new(
        notification_rings: Vec<Arc<NotificationRing>>,
        registry: Arc<SubscriptionRegistry>,
        data_source: Arc<dyn NotificationDataSource>,
        config: DispatcherConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            notification_rings,
            registry,
            data_source,
            config,
            metrics: Arc::new(DispatcherMetrics::default()),
            shutdown,
        }
    }

    /// Runs the dispatcher loop until shutdown.
    ///
    /// Should be spawned as a tokio task.
    pub async fn run(self) {
        let mut batch_buffer: Vec<(u32, Vec<NotificationRef>)> = Vec::new();

        loop {
            if *self.shutdown.borrow() {
                break;
            }

            let drained = self.drain_and_dispatch(&mut batch_buffer);
            self.metrics.dispatch_cycles.fetch_add(1, Ordering::Relaxed);

            if drained == 0 {
                self.metrics.idle_cycles.fetch_add(1, Ordering::Relaxed);

                // Adaptive wait: spin then sleep
                for _ in 0..self.config.spin_iterations {
                    std::hint::spin_loop();
                }
                tokio::time::sleep(self.config.idle_sleep).await;
            }
        }
    }

    /// Drains all notification rings and dispatches events.
    ///
    /// Returns the total number of notifications drained.
    pub fn drain_and_dispatch(&self, batch_buffer: &mut Vec<(u32, Vec<NotificationRef>)>) -> usize {
        batch_buffer.clear();
        let mut total_drained: usize = 0;

        // Phase 1: Drain all notification rings
        for ring in &self.notification_rings {
            ring.drain_into(|notif| {
                if total_drained >= self.config.max_drain_per_cycle {
                    return;
                }
                total_drained += 1;

                if self.config.batch_by_source {
                    let source_id = notif.source_id;
                    if let Some((_, batch)) =
                        batch_buffer.iter_mut().find(|(id, _)| *id == source_id)
                    {
                        if batch.len() < self.config.max_batch_per_source {
                            batch.push(notif);
                        }
                    } else {
                        batch_buffer.push((source_id, vec![notif]));
                    }
                } else {
                    self.dispatch_single(&notif);
                }
            });
        }

        #[allow(clippy::cast_possible_truncation)]
        let drained_u64 = total_drained as u64;
        self.metrics
            .notifications_drained
            .fetch_add(drained_u64, Ordering::Relaxed);

        // Phase 2: Dispatch batched notifications
        if self.config.batch_by_source {
            for (source_id, notifs) in batch_buffer.drain(..) {
                self.dispatch_batch(source_id, &notifs);
            }
        }

        total_drained
    }

    /// Dispatches a single notification (unbatched mode).
    fn dispatch_single(&self, notif: &NotificationRef) {
        let Some(event) = self.data_source.resolve(notif) else {
            self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };

        let senders = self.registry.get_senders_for_source(notif.source_id);
        if senders.is_empty() {
            return;
        }

        for sender in senders {
            match sender.send(event.clone()) {
                Ok(_) => {
                    self.metrics
                        .events_dispatched
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Dispatches a batch of notifications for one source.
    fn dispatch_batch(&self, source_id: u32, notifs: &[NotificationRef]) {
        let senders = self.registry.get_senders_for_source(source_id);
        if senders.is_empty() {
            return;
        }

        for notif in notifs {
            let Some(event) = self.data_source.resolve(notif) else {
                self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            };

            for sender in &senders {
                match sender.send(event.clone()) {
                    Ok(_) => {
                        self.metrics
                            .events_dispatched
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Returns the dispatcher metrics.
    #[must_use]
    pub fn metrics(&self) -> &Arc<DispatcherMetrics> {
        &self.metrics
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

    use crate::subscription::event::EventType;
    use crate::subscription::notification::NotificationRing;
    use crate::subscription::registry::{SubscriptionConfig, SubscriptionRegistry};

    // -- helpers --

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    /// Mock data source that creates a `ChangeEvent::Insert` from the
    /// notification metadata.
    struct MockDataSource;

    impl NotificationDataSource for MockDataSource {
        fn resolve(&self, notif: &NotificationRef) -> Option<ChangeEvent> {
            let batch = Arc::new(make_batch(notif.row_count as usize));
            Some(ChangeEvent::insert(batch, notif.timestamp, notif.sequence))
        }
    }

    /// Mock data source that always returns `None` (simulates expired data).
    struct FailingDataSource;

    impl NotificationDataSource for FailingDataSource {
        fn resolve(&self, _notif: &NotificationRef) -> Option<ChangeEvent> {
            None
        }
    }

    fn make_notif(seq: u64, source_id: u32, rows: u32, ts: i64) -> NotificationRef {
        NotificationRef::new(seq, source_id, EventType::Insert, rows, ts, 0)
    }

    fn make_dispatcher(
        rings: Vec<Arc<NotificationRing>>,
        registry: Arc<SubscriptionRegistry>,
        data_source: Arc<dyn NotificationDataSource>,
        config: DispatcherConfig,
    ) -> SubscriptionDispatcher {
        let (_tx, rx) = watch::channel(false);
        SubscriptionDispatcher::new(rings, registry, data_source, config, rx)
    }

    // -- Config tests --

    #[test]
    fn test_dispatcher_config_default() {
        let cfg = DispatcherConfig::default();
        assert_eq!(cfg.max_drain_per_cycle, 4096);
        assert_eq!(cfg.idle_sleep, Duration::from_micros(10));
        assert_eq!(cfg.spin_iterations, 100);
        assert!(cfg.batch_by_source);
        assert_eq!(cfg.max_batch_per_source, 256);
    }

    // -- Drain tests --

    #[test]
    fn test_dispatcher_drain_single_ring() {
        let ring = Arc::new(NotificationRing::new(64));
        for i in 0..5u64 {
            ring.push(make_notif(i, 0, 1, 1000 + i as i64));
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![Arc::clone(&ring)],
            registry,
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        let drained = dispatcher.drain_and_dispatch(&mut buf);
        assert_eq!(drained, 5);
        assert_eq!(dispatcher.metrics().notifications_drained(), 5);
    }

    #[test]
    fn test_dispatcher_drain_multiple_rings() {
        let ring0 = Arc::new(NotificationRing::new(64));
        let ring1 = Arc::new(NotificationRing::new(64));

        for i in 0..3u64 {
            ring0.push(make_notif(i, 0, 1, 1000));
        }
        for i in 0..4u64 {
            ring1.push(make_notif(i, 1, 1, 2000));
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring0, ring1],
            registry,
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        let drained = dispatcher.drain_and_dispatch(&mut buf);
        assert_eq!(drained, 7);
        assert_eq!(dispatcher.metrics().notifications_drained(), 7);
    }

    // -- Dispatch tests --

    #[test]
    fn test_dispatcher_dispatch_single() {
        let ring = Arc::new(NotificationRing::new(64));
        ring.push(make_notif(1, 0, 5, 1000));

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, mut rx) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;

        // Use unbatched mode
        let mut cfg = DispatcherConfig::default();
        cfg.batch_by_source = false;
        let dispatcher = make_dispatcher(vec![ring], Arc::clone(&registry), ds, cfg);

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        assert_eq!(dispatcher.metrics().events_dispatched(), 1);

        let event = rx.try_recv().unwrap();
        assert_eq!(event.timestamp(), 1000);
        assert_eq!(event.sequence(), Some(1));
        assert_eq!(event.row_count(), 5);
    }

    #[test]
    fn test_dispatcher_dispatch_batch() {
        let ring = Arc::new(NotificationRing::new(64));
        for i in 0..3u64 {
            ring.push(make_notif(i + 1, 0, 2, 1000 + i as i64));
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, mut rx) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        let drained = dispatcher.drain_and_dispatch(&mut buf);
        assert_eq!(drained, 3);
        assert_eq!(dispatcher.metrics().events_dispatched(), 3);

        // All 3 events arrive in order
        for i in 0..3u64 {
            let event = rx.try_recv().unwrap();
            assert_eq!(event.sequence(), Some(i + 1));
        }
    }

    #[test]
    fn test_dispatcher_no_subscribers() {
        let ring = Arc::new(NotificationRing::new(64));
        ring.push(make_notif(1, 0, 1, 1000));

        let registry = Arc::new(SubscriptionRegistry::new());
        // No subscriptions registered
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(vec![ring], registry, ds, DispatcherConfig::default());

        let mut buf = Vec::new();
        let drained = dispatcher.drain_and_dispatch(&mut buf);
        assert_eq!(drained, 1);
        // Drained but not dispatched (no subscribers)
        assert_eq!(dispatcher.metrics().events_dispatched(), 0);
        assert_eq!(dispatcher.metrics().events_dropped(), 0);
    }

    #[test]
    fn test_dispatcher_paused_subscriber_skipped() {
        let ring = Arc::new(NotificationRing::new(64));
        ring.push(make_notif(1, 0, 1, 1000));

        let registry = Arc::new(SubscriptionRegistry::new());
        let (id, mut rx) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        registry.pause(id);

        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        // Paused subscriber does not receive
        assert_eq!(dispatcher.metrics().events_dispatched(), 0);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_dispatcher_metrics() {
        let ring = Arc::new(NotificationRing::new(64));
        for i in 0..10u64 {
            ring.push(make_notif(i, 0, 1, 1000));
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, _rx) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        let m = dispatcher.metrics();
        assert_eq!(m.notifications_drained(), 10);
        assert_eq!(m.events_dispatched(), 10);
        assert_eq!(m.events_dropped(), 0);
        assert_eq!(m.dispatch_cycles(), 0); // drain_and_dispatch doesn't bump this
    }

    #[test]
    fn test_dispatcher_resolve_failure() {
        let ring = Arc::new(NotificationRing::new(64));
        ring.push(make_notif(1, 0, 1, 1000));
        ring.push(make_notif(2, 0, 1, 2000));

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, _rx) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let ds = Arc::new(FailingDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        let drained = dispatcher.drain_and_dispatch(&mut buf);
        assert_eq!(drained, 2);
        assert_eq!(dispatcher.metrics().events_dispatched(), 0);
        assert_eq!(dispatcher.metrics().events_dropped(), 2);
    }

    #[test]
    fn test_dispatcher_lagged_subscriber() {
        // Use a very small buffer so the receiver lags
        let ring = Arc::new(NotificationRing::new(64));
        for i in 0..10u64 {
            ring.push(make_notif(i, 0, 1, 1000));
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let mut cfg = SubscriptionConfig::default();
        cfg.buffer_size = 2; // very small buffer
        let (_, _rx) = registry.create("mv_a".into(), 0, cfg);

        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        // All 10 drained. broadcast::send succeeds even when the internal
        // buffer overwrites old entries (it returns Ok with receiver count).
        // The lagged receiver will get RecvError::Lagged on next recv.
        assert_eq!(dispatcher.metrics().notifications_drained(), 10);
        assert_eq!(dispatcher.metrics().events_dispatched(), 10);
    }

    // -- Shutdown test --

    #[tokio::test]
    async fn test_dispatcher_shutdown() {
        let ring = Arc::new(NotificationRing::new(64));
        let registry = Arc::new(SubscriptionRegistry::new());
        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut cfg = DispatcherConfig::default();
        cfg.idle_sleep = Duration::from_millis(1); // fast idle for test
        cfg.spin_iterations = 0;

        let dispatcher = SubscriptionDispatcher::new(vec![ring], registry, ds, cfg, shutdown_rx);

        let metrics = Arc::clone(dispatcher.metrics());

        let handle = tokio::spawn(dispatcher.run());

        // Let it run a few cycles
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Signal shutdown
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();

        // Ran at least a few cycles
        assert!(metrics.dispatch_cycles() > 0);
        assert!(metrics.idle_cycles() > 0);
    }

    // -- Integration tests --

    #[test]
    fn test_end_to_end_notification_to_subscriber() {
        use crate::subscription::notification::NotificationHub;

        let mut hub = NotificationHub::new(4, 64);
        let source_id = hub.register_source().unwrap();

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, mut rx) =
            registry.create("mv_orders".into(), source_id, SubscriptionConfig::default());

        // Notify through the hub
        hub.notify_source(source_id, EventType::Insert, 10, 5000, 0);
        hub.notify_source(source_id, EventType::Delete, 3, 6000, 64);

        // Drain hub into a standalone ring for the dispatcher
        let ring = Arc::new(NotificationRing::new(64));
        hub.drain_notifications(|n| {
            ring.push(n);
        });

        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        assert_eq!(dispatcher.metrics().events_dispatched(), 2);

        let e1 = rx.try_recv().unwrap();
        assert_eq!(e1.timestamp(), 5000);
        assert_eq!(e1.row_count(), 10);

        let e2 = rx.try_recv().unwrap();
        assert_eq!(e2.timestamp(), 6000);
        assert_eq!(e2.row_count(), 3);
    }

    #[test]
    fn test_dispatcher_multiple_subscribers_same_source() {
        let ring = Arc::new(NotificationRing::new(64));
        ring.push(make_notif(1, 0, 5, 1000));

        let registry = Arc::new(SubscriptionRegistry::new());
        let (_, mut rx1) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, mut rx2) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());
        let (_, mut rx3) = registry.create("mv_a".into(), 0, SubscriptionConfig::default());

        let ds = Arc::new(MockDataSource) as Arc<dyn NotificationDataSource>;
        let dispatcher = make_dispatcher(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        // 1 notification × 3 subscribers = 3 dispatched
        assert_eq!(dispatcher.metrics().events_dispatched(), 3);

        // All three receive
        for rx in [&mut rx1, &mut rx2, &mut rx3] {
            let event = rx.try_recv().unwrap();
            assert_eq!(event.timestamp(), 1000);
            assert_eq!(event.row_count(), 5);
        }
    }
}
