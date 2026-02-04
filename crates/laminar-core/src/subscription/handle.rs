//! Push-based subscription handle — the primary subscriber-facing API.
//!
//! [`PushSubscription`] wraps a `tokio::sync::broadcast::Receiver<ChangeEvent>`
//! and provides ergonomic async receive with lifecycle management (pause, resume,
//! cancel). Dropping the handle automatically cancels the subscription.
//!
//! # Usage
//!
//! ```rust,ignore
//! let sub = PushSubscription::new(id, receiver, registry, "SELECT * FROM trades".into());
//!
//! // Async receive loop
//! while let Ok(event) = sub.recv().await {
//!     match event {
//!         ChangeEvent::Insert { data, .. } => process(data),
//!         ChangeEvent::Watermark { timestamp } => advance(timestamp),
//!         _ => {}
//!     }
//! }
//! ```
//!
//! # API Styles
//!
//! This is the **channel-based** style (Option B from the research). Other styles
//! build on top of it:
//! - F-SUB-006: Callback subscriptions (`on_event` closure)
//! - F-SUB-007: Async `Stream` subscriptions (`StreamExt` compatible)

use std::sync::Arc;

use tokio::sync::broadcast;

use crate::subscription::event::ChangeEvent;
use crate::subscription::registry::{SubscriptionId, SubscriptionMetrics, SubscriptionRegistry};

// ---------------------------------------------------------------------------
// PushSubscriptionError
// ---------------------------------------------------------------------------

/// Errors from push subscription operations.
#[derive(Debug, thiserror::Error)]
pub enum PushSubscriptionError {
    /// The subscription's source was closed (all senders dropped).
    #[error("subscription closed")]
    Closed,
    /// Events were missed due to slow consumption.
    #[error("lagged behind by {0} events")]
    Lagged(u64),
    /// The subscription was cancelled.
    #[error("subscription cancelled")]
    Cancelled,
    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

// ---------------------------------------------------------------------------
// PushSubscription
// ---------------------------------------------------------------------------

/// A push-based subscription handle.
///
/// Receives [`ChangeEvent`]s as they are pushed by the Ring 1 dispatcher.
/// Dropping this handle automatically cancels the subscription in the
/// [`SubscriptionRegistry`].
///
/// Works naturally with `tokio::select!` and `while let`:
///
/// ```rust,ignore
/// tokio::select! {
///     event = sub.recv() => handle(event),
///     _ = shutdown.recv() => break,
/// }
/// ```
pub struct PushSubscription {
    /// Subscription ID.
    id: SubscriptionId,
    /// Broadcast receiver for change events.
    receiver: broadcast::Receiver<ChangeEvent>,
    /// Registry reference for lifecycle management.
    registry: Arc<SubscriptionRegistry>,
    /// Query or MV name that created this subscription.
    query: String,
    /// Whether the subscription has been explicitly cancelled.
    cancelled: bool,
}

impl PushSubscription {
    /// Creates a new push subscription.
    ///
    /// This is `pub(crate)` — external callers create subscriptions via the
    /// pipeline or engine API, which resolves the query to a `source_id` and
    /// registers in the [`SubscriptionRegistry`].
    pub fn new(
        id: SubscriptionId,
        receiver: broadcast::Receiver<ChangeEvent>,
        registry: Arc<SubscriptionRegistry>,
        query: String,
    ) -> Self {
        Self {
            id,
            receiver,
            registry,
            query,
            cancelled: false,
        }
    }

    /// Receives the next change event.
    ///
    /// Awaits until an event is available or the subscription is closed.
    ///
    /// # Errors
    ///
    /// - [`PushSubscriptionError::Cancelled`] if the subscription was cancelled.
    /// - [`PushSubscriptionError::Lagged`] if `n` events were missed due to
    ///   slow consumption. The receiver skips ahead and subsequent `recv()`
    ///   calls return newer events.
    /// - [`PushSubscriptionError::Closed`] if all senders have been dropped.
    pub async fn recv(&mut self) -> Result<ChangeEvent, PushSubscriptionError> {
        if self.cancelled {
            return Err(PushSubscriptionError::Cancelled);
        }

        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(PushSubscriptionError::Lagged(n)),
            Err(broadcast::error::RecvError::Closed) => Err(PushSubscriptionError::Closed),
        }
    }

    /// Tries to receive without blocking.
    ///
    /// Returns `None` if no event is immediately available.
    /// Returns `Some(Err(...))` on lagged, closed, or cancelled.
    pub fn try_recv(&mut self) -> Option<Result<ChangeEvent, PushSubscriptionError>> {
        if self.cancelled {
            return Some(Err(PushSubscriptionError::Cancelled));
        }

        match self.receiver.try_recv() {
            Ok(event) => Some(Ok(event)),
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Some(Err(PushSubscriptionError::Lagged(n)))
            }
            Err(broadcast::error::TryRecvError::Closed) => Some(Err(PushSubscriptionError::Closed)),
            Err(broadcast::error::TryRecvError::Empty) => None,
        }
    }

    /// Pauses the subscription.
    ///
    /// While paused, events are either buffered or dropped depending on the
    /// backpressure configuration. Returns `true` if the subscription was
    /// active and is now paused.
    #[must_use]
    pub fn pause(&self) -> bool {
        self.registry.pause(self.id)
    }

    /// Resumes a paused subscription.
    ///
    /// Returns `true` if the subscription was paused and is now active.
    #[must_use]
    pub fn resume(&self) -> bool {
        self.registry.resume(self.id)
    }

    /// Cancels the subscription.
    ///
    /// After cancellation, `recv()` returns [`PushSubscriptionError::Cancelled`].
    /// The subscription is removed from the registry.
    pub fn cancel(&mut self) {
        if !self.cancelled {
            self.cancelled = true;
            self.registry.cancel(self.id);
        }
    }

    /// Returns the subscription ID.
    #[must_use]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns the query string for this subscription.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns `true` if the subscription has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Returns subscription metrics from the registry.
    #[must_use]
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.registry.metrics(self.id)
    }
}

impl Drop for PushSubscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.registry.cancel(self.id);
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::field_reassign_with_default)]
#[allow(clippy::ignored_unit_patterns)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use crate::subscription::registry::{SubscriptionConfig, SubscriptionState};

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    /// Helper: create a registry + push subscription pair.
    fn make_sub(query: &str) -> (Arc<SubscriptionRegistry>, PushSubscription) {
        let registry = Arc::new(SubscriptionRegistry::new());
        let (id, rx) = registry.create(query.into(), 0, SubscriptionConfig::default());
        let sub = PushSubscription::new(id, rx, Arc::clone(&registry), query.into());
        (registry, sub)
    }

    /// Helper: create a registry + push sub + sender for the same source.
    fn make_sub_with_sender(
        query: &str,
    ) -> (
        Arc<SubscriptionRegistry>,
        PushSubscription,
        broadcast::Sender<ChangeEvent>,
    ) {
        let registry = Arc::new(SubscriptionRegistry::new());
        let (id, rx) = registry.create(query.into(), 0, SubscriptionConfig::default());
        // Get a sender from the registry for the same source
        let senders = registry.get_senders_for_source(0);
        let sender = senders.into_iter().next().unwrap();
        let sub = PushSubscription::new(id, rx, Arc::clone(&registry), query.into());
        (registry, sub, sender)
    }

    // --- Basic receive ---

    #[tokio::test]
    async fn test_push_subscription_recv() {
        let (_reg, mut sub, sender) = make_sub_with_sender("SELECT * FROM trades");

        let batch = Arc::new(make_batch(5));
        let event = ChangeEvent::insert(batch, 1000, 1);
        sender.send(event).unwrap();

        let received = sub.recv().await.unwrap();
        assert_eq!(received.timestamp(), 1000);
        assert_eq!(received.sequence(), Some(1));
        assert_eq!(received.row_count(), 5);
    }

    #[test]
    fn test_push_subscription_try_recv() {
        let (_reg, mut sub, sender) = make_sub_with_sender("trades");

        // Empty initially
        assert!(sub.try_recv().is_none());

        // Send an event
        let batch = Arc::new(make_batch(3));
        sender.send(ChangeEvent::insert(batch, 2000, 2)).unwrap();

        let result = sub.try_recv().unwrap().unwrap();
        assert_eq!(result.timestamp(), 2000);
        assert_eq!(result.sequence(), Some(2));
    }

    #[test]
    fn test_push_subscription_try_recv_empty() {
        let (_reg, mut sub) = make_sub("trades");
        assert!(sub.try_recv().is_none());
    }

    // --- Lifecycle ---

    #[test]
    fn test_push_subscription_pause_resume() {
        let (reg, sub) = make_sub("trades");

        // Starts active
        assert_eq!(reg.state(sub.id()), Some(SubscriptionState::Active));

        // Pause
        assert!(sub.pause());
        assert_eq!(reg.state(sub.id()), Some(SubscriptionState::Paused));

        // Pause again — already paused
        assert!(!sub.pause());

        // Resume
        assert!(sub.resume());
        assert_eq!(reg.state(sub.id()), Some(SubscriptionState::Active));

        // Resume again — already active
        assert!(!sub.resume());
    }

    #[tokio::test]
    async fn test_push_subscription_cancel() {
        let (reg, mut sub, sender) = make_sub_with_sender("trades");

        // Cancel
        sub.cancel();
        assert!(sub.is_cancelled());

        // recv returns Cancelled
        let err = sub.recv().await.unwrap_err();
        assert!(matches!(err, PushSubscriptionError::Cancelled));

        // try_recv returns Cancelled
        let err = sub.try_recv().unwrap().unwrap_err();
        assert!(matches!(err, PushSubscriptionError::Cancelled));

        // Registry cleaned up
        assert_eq!(reg.subscription_count(), 0);

        // Idempotent — cancel again is fine
        sub.cancel();

        // Sender still works (just no receivers)
        drop(sender);
    }

    #[test]
    fn test_push_subscription_drop_cancels() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let (id, rx) = registry.create("trades".into(), 0, SubscriptionConfig::default());
        assert_eq!(registry.subscription_count(), 1);

        {
            let _sub = PushSubscription::new(id, rx, Arc::clone(&registry), "trades".into());
            // sub is active
            assert_eq!(registry.subscription_count(), 1);
        }
        // Dropped — should be cancelled
        assert_eq!(registry.subscription_count(), 0);
    }

    // --- Error handling ---

    #[tokio::test]
    async fn test_push_subscription_lagged() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let mut cfg = SubscriptionConfig::default();
        cfg.buffer_size = 4; // small buffer
        let (id, rx) = registry.create("trades".into(), 0, cfg);

        let senders = registry.get_senders_for_source(0);
        let sender = senders.into_iter().next().unwrap();

        let mut sub = PushSubscription::new(id, rx, Arc::clone(&registry), "trades".into());

        // Fill past capacity to cause lag
        for i in 0..10u64 {
            let batch = Arc::new(make_batch(1));
            sender
                .send(ChangeEvent::insert(batch, i as i64, i))
                .unwrap();
        }

        // First recv should report lag
        let err = sub.recv().await.unwrap_err();
        match err {
            PushSubscriptionError::Lagged(n) => {
                assert!(n > 0, "expected non-zero lag count, got {n}");
            }
            other => panic!("expected Lagged, got {other:?}"),
        }

        // Subsequent recv should succeed with a newer event
        let event = sub.recv().await.unwrap();
        assert!(event.sequence().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_push_subscription_closed() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let (id, rx) = registry.create("trades".into(), 0, SubscriptionConfig::default());

        let mut sub = PushSubscription::new(id, rx, Arc::clone(&registry), "trades".into());

        // Cancel the subscription from the registry side to drop the sender
        // (broadcast::Sender is inside the registry entry, cancel removes it)
        registry.cancel(id);

        let err = sub.recv().await.unwrap_err();
        assert!(matches!(err, PushSubscriptionError::Closed));
    }

    // --- Accessors ---

    #[test]
    fn test_push_subscription_id_and_query() {
        let (_reg, sub) = make_sub("SELECT * FROM trades");
        assert_eq!(sub.id(), SubscriptionId(1));
        assert_eq!(sub.query(), "SELECT * FROM trades");
        assert!(!sub.is_cancelled());
    }

    #[test]
    fn test_push_subscription_metrics() {
        let (reg, sub) = make_sub("trades");
        let m = sub.metrics().unwrap();
        assert_eq!(m.id, sub.id());
        assert_eq!(m.source_name, "trades");
        assert_eq!(m.state, SubscriptionState::Active);
        assert_eq!(m.events_delivered, 0);

        // Record some deliveries
        reg.record_delivery(sub.id(), 5);
        let m = sub.metrics().unwrap();
        assert_eq!(m.events_delivered, 5);
    }

    // --- tokio::select! compatibility ---

    #[tokio::test]
    async fn test_push_subscription_with_select() {
        let (_reg, mut sub, sender) = make_sub_with_sender("trades");

        let batch = Arc::new(make_batch(1));
        sender.send(ChangeEvent::insert(batch, 9000, 42)).unwrap();

        let result = tokio::select! {
            event = sub.recv() => event,
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("timeout — event should be immediate");
            }
        };

        let event = result.unwrap();
        assert_eq!(event.timestamp(), 9000);
        assert_eq!(event.sequence(), Some(42));
    }

    // --- Multiple subscribers ---

    #[tokio::test]
    async fn test_push_subscription_multiple_subscribers() {
        let registry = Arc::new(SubscriptionRegistry::new());

        let (id1, rx1) = registry.create("trades".into(), 0, SubscriptionConfig::default());
        let (id2, rx2) = registry.create("trades".into(), 0, SubscriptionConfig::default());

        let mut sub1 = PushSubscription::new(id1, rx1, Arc::clone(&registry), "trades".into());
        let mut sub2 = PushSubscription::new(id2, rx2, Arc::clone(&registry), "trades".into());

        // Get senders and broadcast
        let senders = registry.get_senders_for_source(0);
        let batch = Arc::new(make_batch(3));
        let event = ChangeEvent::insert(batch, 5000, 10);
        for sender in &senders {
            sender.send(event.clone()).unwrap();
        }

        // Both subscribers receive
        let e1 = sub1.recv().await.unwrap();
        let e2 = sub2.recv().await.unwrap();
        assert_eq!(e1.timestamp(), 5000);
        assert_eq!(e2.timestamp(), 5000);
    }

    // --- Integration: end-to-end with dispatcher ---

    #[test]
    fn test_end_to_end_push_subscribe() {
        use crate::subscription::dispatcher::{
            DispatcherConfig, NotificationDataSource, SubscriptionDispatcher,
        };
        use crate::subscription::event::EventType;
        use crate::subscription::notification::{NotificationHub, NotificationRing};

        struct TestDataSource;

        impl NotificationDataSource for TestDataSource {
            fn resolve(
                &self,
                notif: &crate::subscription::event::NotificationRef,
            ) -> Option<ChangeEvent> {
                let batch = Arc::new(make_batch(notif.row_count as usize));
                Some(ChangeEvent::insert(batch, notif.timestamp, notif.sequence))
            }
        }

        // 1. Set up hub + registry
        let mut hub = NotificationHub::new(4, 64);
        let source_id = hub.register_source().unwrap();

        let registry = Arc::new(SubscriptionRegistry::new());

        // 2. Create push subscription
        let (sub_id, rx) =
            registry.create("mv_trades".into(), source_id, SubscriptionConfig::default());
        let mut sub = PushSubscription::new(sub_id, rx, Arc::clone(&registry), "mv_trades".into());

        // 3. Notify through the hub (Ring 0)
        hub.notify_source(source_id, EventType::Insert, 10, 7000, 0);

        // 4. Transfer to dispatcher ring + dispatch (Ring 1)
        let ring = Arc::new(NotificationRing::new(64));
        hub.drain_notifications(|n| {
            ring.push(n);
        });

        let ds = Arc::new(TestDataSource) as Arc<dyn NotificationDataSource>;
        let (_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let dispatcher = SubscriptionDispatcher::new(
            vec![ring],
            Arc::clone(&registry),
            ds,
            DispatcherConfig::default(),
            shutdown_rx,
        );

        let mut buf = Vec::new();
        dispatcher.drain_and_dispatch(&mut buf);

        // 5. Receive via PushSubscription (Ring 2)
        let event = sub.try_recv().unwrap().unwrap();
        assert_eq!(event.timestamp(), 7000);
        assert_eq!(event.row_count(), 10);
    }

    // --- Error Display ---

    #[test]
    fn test_push_subscription_error_display() {
        let e = PushSubscriptionError::Closed;
        assert_eq!(format!("{e}"), "subscription closed");

        let e = PushSubscriptionError::Lagged(42);
        assert_eq!(format!("{e}"), "lagged behind by 42 events");

        let e = PushSubscriptionError::Cancelled;
        assert_eq!(format!("{e}"), "subscription cancelled");

        let e = PushSubscriptionError::Internal("oops".into());
        assert_eq!(format!("{e}"), "internal error: oops");
    }
}
