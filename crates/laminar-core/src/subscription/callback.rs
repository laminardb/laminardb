//! Callback-based subscriptions — [`SubscriptionCallback`] trait and
//! [`CallbackSubscriptionHandle`].
//!
//! Provides a callback-based subscription API where users register a callback
//! function or trait object that is invoked for every change event. The callback
//! runs on a dedicated tokio task, wrapping the channel-based broadcast receiver
//! from the [`SubscriptionRegistry`] internally.
//!
//! # API Styles
//!
//! - **Trait-based**: Implement [`SubscriptionCallback`] for full control over
//!   change, error, and completion events.
//! - **Closure-based**: Use [`subscribe_fn`] for simple cases where only
//!   `on_change` is needed.
//!
//! # Panic Safety
//!
//! Panics in the callback's [`on_change`](SubscriptionCallback::on_change) are
//! caught via [`std::panic::catch_unwind`] and forwarded to
//! [`on_error`](SubscriptionCallback::on_error) as
//! [`PushSubscriptionError::Internal`].
//!
//! # Lifecycle
//!
//! Dropping a [`CallbackSubscriptionHandle`] automatically cancels the
//! subscription and aborts the callback task.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use tokio::sync::broadcast;

use crate::subscription::event::ChangeEvent;
use crate::subscription::handle::PushSubscriptionError;
use crate::subscription::registry::{
    SubscriptionConfig, SubscriptionId, SubscriptionMetrics, SubscriptionRegistry,
};

// ---------------------------------------------------------------------------
// SubscriptionCallback
// ---------------------------------------------------------------------------

/// Callback trait for push-based subscriptions.
///
/// Implement this trait to receive change events via callback. The callback
/// runs on a dedicated tokio task and is invoked for every event pushed by
/// the Ring 1 dispatcher.
///
/// # Example
///
/// ```rust,ignore
/// struct MyHandler;
///
/// impl SubscriptionCallback for MyHandler {
///     fn on_change(&self, event: ChangeEvent) {
///         match event {
///             ChangeEvent::Insert { data, .. } => println!("{} rows", data.num_rows()),
///             _ => {}
///         }
///     }
/// }
/// ```
pub trait SubscriptionCallback: Send + Sync + 'static {
    /// Called for each change event.
    fn on_change(&self, event: ChangeEvent);

    /// Called when an error occurs (e.g., lagged behind, internal error).
    ///
    /// Default implementation logs the error via `tracing::warn!`.
    fn on_error(&self, error: PushSubscriptionError) {
        tracing::warn!("subscription callback error: {}", error);
    }

    /// Called when the subscription is closed (source dropped or cancelled).
    ///
    /// Default implementation is a no-op.
    fn on_complete(&self) {}
}

// ---------------------------------------------------------------------------
// FnCallback (private adapter)
// ---------------------------------------------------------------------------

/// Adapter that wraps a closure into a [`SubscriptionCallback`].
struct FnCallback<F>(F);

impl<F: Fn(ChangeEvent) + Send + Sync + 'static> SubscriptionCallback for FnCallback<F> {
    fn on_change(&self, event: ChangeEvent) {
        (self.0)(event);
    }
}

// ---------------------------------------------------------------------------
// CallbackSubscriptionHandle
// ---------------------------------------------------------------------------

/// Handle for a callback-based subscription.
///
/// Provides lifecycle management (pause/resume/cancel) for the callback task.
/// The handle and the callback task share the same [`SubscriptionEntry`] in
/// the registry (via [`SubscriptionId`]), so `pause()` / `cancel()` on the
/// handle directly affects the task's event delivery.
///
/// Dropping the handle automatically cancels the subscription and aborts the
/// callback task.
pub struct CallbackSubscriptionHandle {
    /// Subscription ID (shared with the callback task).
    id: SubscriptionId,
    /// Registry reference for lifecycle management.
    registry: Arc<SubscriptionRegistry>,
    /// Join handle for the callback runner task.
    task: Option<tokio::task::JoinHandle<()>>,
    /// Whether the subscription has been explicitly cancelled.
    cancelled: bool,
}

impl CallbackSubscriptionHandle {
    /// Pauses the subscription.
    ///
    /// While paused, events are buffered or dropped per the backpressure
    /// configuration. Returns `true` if the subscription was active and is
    /// now paused.
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

    /// Cancels the subscription and aborts the callback task.
    ///
    /// The subscription is removed from the registry (dropping the broadcast
    /// sender) and the task is aborted as a safety net.
    pub fn cancel(&mut self) {
        if !self.cancelled {
            self.cancelled = true;
            self.registry.cancel(self.id);
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }

    /// Returns the subscription ID.
    #[must_use]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns subscription metrics from the registry.
    #[must_use]
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.registry.metrics(self.id)
    }

    /// Returns `true` if the subscription has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }
}

impl Drop for CallbackSubscriptionHandle {
    fn drop(&mut self) {
        if !self.cancelled {
            self.registry.cancel(self.id);
            if let Some(task) = self.task.take() {
                task.abort();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Factory Functions
// ---------------------------------------------------------------------------

/// Creates a callback-based subscription.
///
/// Registers a subscription in the registry, then spawns a tokio task that
/// calls `callback.on_change()` for every event. Panics in the callback are
/// caught and forwarded to `callback.on_error()`.
///
/// When the broadcast sender is dropped (e.g., via cancel or registry
/// cleanup), the task calls `callback.on_complete()` and exits.
///
/// # Arguments
///
/// * `registry` — Subscription registry for lifecycle management.
/// * `source_name` — Name of the source MV or query.
/// * `source_id` — Ring 0 source identifier.
/// * `config` — Subscription configuration.
/// * `callback` — Implementation of [`SubscriptionCallback`].
pub fn subscribe_callback<C: SubscriptionCallback>(
    registry: Arc<SubscriptionRegistry>,
    source_name: String,
    source_id: u32,
    config: SubscriptionConfig,
    callback: C,
) -> CallbackSubscriptionHandle {
    let (id, receiver) = registry.create(source_name, source_id, config);
    let callback = Arc::new(callback);

    let task = tokio::spawn(callback_runner(receiver, callback));

    CallbackSubscriptionHandle {
        id,
        registry,
        task: Some(task),
        cancelled: false,
    }
}

/// Creates a closure-based subscription (convenience wrapper).
///
/// Equivalent to [`subscribe_callback`] with a closure wrapped in an internal
/// adapter that implements [`SubscriptionCallback`].
///
/// # Example
///
/// ```rust,ignore
/// let handle = subscribe_fn(registry, "trades".into(), 0, config, |event| {
///     println!("Got: {:?}", event.event_type());
/// });
/// ```
pub fn subscribe_fn<F>(
    registry: Arc<SubscriptionRegistry>,
    source_name: String,
    source_id: u32,
    config: SubscriptionConfig,
    f: F,
) -> CallbackSubscriptionHandle
where
    F: Fn(ChangeEvent) + Send + Sync + 'static,
{
    subscribe_callback(registry, source_name, source_id, config, FnCallback(f))
}

// ---------------------------------------------------------------------------
// Callback Runner (internal)
// ---------------------------------------------------------------------------

/// Internal task that receives events from the broadcast channel and calls
/// the callback. Panics in `on_change` are caught and forwarded to `on_error`.
async fn callback_runner<C: SubscriptionCallback>(
    mut receiver: broadcast::Receiver<ChangeEvent>,
    callback: Arc<C>,
) {
    loop {
        match receiver.recv().await {
            Ok(event) => {
                let cb = Arc::clone(&callback);
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| cb.on_change(event)));
                if let Err(panic) = result {
                    let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                        format!("callback panicked: {s}")
                    } else if let Some(s) = panic.downcast_ref::<String>() {
                        format!("callback panicked: {s}")
                    } else {
                        "callback panicked".to_string()
                    };
                    callback.on_error(PushSubscriptionError::Internal(msg));
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                callback.on_error(PushSubscriptionError::Lagged(n));
                // Continue receiving after lag
            }
            Err(broadcast::error::RecvError::Closed) => {
                callback.on_complete();
                break;
            }
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use crate::subscription::registry::SubscriptionState;

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    // --- Test callback implementation ---

    #[derive(Clone)]
    struct TestCallback {
        events: Arc<Mutex<Vec<i64>>>,
        errors: Arc<Mutex<Vec<String>>>,
        completed: Arc<Mutex<bool>>,
    }

    impl TestCallback {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
                errors: Arc::new(Mutex::new(Vec::new())),
                completed: Arc::new(Mutex::new(false)),
            }
        }
    }

    impl SubscriptionCallback for TestCallback {
        fn on_change(&self, event: ChangeEvent) {
            self.events.lock().unwrap().push(event.timestamp());
        }

        fn on_error(&self, error: PushSubscriptionError) {
            self.errors.lock().unwrap().push(format!("{error}"));
        }

        fn on_complete(&self) {
            *self.completed.lock().unwrap() = true;
        }
    }

    // --- Tests ---

    #[tokio::test]
    async fn test_callback_receives_events() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();
        let events = Arc::clone(&cb.events);

        let _handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        let senders = registry.get_senders_for_source(0);
        assert_eq!(senders.len(), 1);

        for i in 0..5i64 {
            let batch = Arc::new(make_batch(1));
            senders[0]
                .send(ChangeEvent::insert(batch, i * 1000, i as u64))
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = events.lock().unwrap();
        assert_eq!(received.len(), 5);
        assert_eq!(*received, vec![0, 1000, 2000, 3000, 4000]);
    }

    #[tokio::test]
    async fn test_callback_on_error_lagged() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let mut cfg = SubscriptionConfig::default();
        cfg.buffer_size = 4;
        let cb = TestCallback::new();
        let errors = Arc::clone(&cb.errors);
        let events = Arc::clone(&cb.events);

        let _handle = subscribe_callback(Arc::clone(&registry), "trades".into(), 0, cfg, cb);

        let senders = registry.get_senders_for_source(0);

        // Overflow the buffer to cause lag
        for i in 0..20i64 {
            let batch = Arc::new(make_batch(1));
            let _ = senders[0].send(ChangeEvent::insert(batch, i * 100, i as u64));
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let errs = errors.lock().unwrap();
        assert!(!errs.is_empty(), "expected at least one lag error");
        assert!(errs[0].contains("lagged behind"));

        // Should still receive events after lag recovery
        let evts = events.lock().unwrap();
        assert!(!evts.is_empty(), "should receive events after lag");
    }

    #[tokio::test]
    async fn test_callback_on_complete() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();
        let completed = Arc::clone(&cb.completed);

        let handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        // Cancel from registry side — drops sender → task gets Closed → on_complete
        registry.cancel(handle.id());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(*completed.lock().unwrap());
    }

    #[tokio::test]
    async fn test_callback_panic_caught() {
        struct PanickingCallback {
            errors: Arc<Mutex<Vec<String>>>,
        }

        impl SubscriptionCallback for PanickingCallback {
            fn on_change(&self, _event: ChangeEvent) {
                panic!("deliberate test panic");
            }

            fn on_error(&self, error: PushSubscriptionError) {
                self.errors.lock().unwrap().push(format!("{error}"));
            }
        }

        let registry = Arc::new(SubscriptionRegistry::new());
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        let _handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            PanickingCallback {
                errors: Arc::clone(&errors),
            },
        );

        let senders = registry.get_senders_for_source(0);
        let batch = Arc::new(make_batch(1));
        senders[0]
            .send(ChangeEvent::insert(batch, 1000, 1))
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let errs = errors.lock().unwrap();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("callback panicked"));
        assert!(errs[0].contains("deliberate test panic"));
    }

    #[tokio::test]
    async fn test_callback_handle_pause_resume() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();

        let handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        assert!(handle.pause());
        assert_eq!(registry.state(handle.id()), Some(SubscriptionState::Paused));

        // Already paused
        assert!(!handle.pause());

        assert!(handle.resume());
        assert_eq!(registry.state(handle.id()), Some(SubscriptionState::Active));

        // Already active
        assert!(!handle.resume());
    }

    #[tokio::test]
    async fn test_callback_handle_cancel() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();

        let mut handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        assert_eq!(registry.subscription_count(), 1);
        assert!(!handle.is_cancelled());

        handle.cancel();

        assert!(handle.is_cancelled());
        assert_eq!(registry.subscription_count(), 0);

        // Idempotent
        handle.cancel();
        assert_eq!(registry.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_callback_handle_drop_cancels() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();

        {
            let _handle = subscribe_callback(
                Arc::clone(&registry),
                "trades".into(),
                0,
                SubscriptionConfig::default(),
                cb,
            );
            assert_eq!(registry.subscription_count(), 1);
        }
        // Dropped — should be cancelled
        assert_eq!(registry.subscription_count(), 0);
    }

    #[tokio::test]
    async fn test_subscribe_fn() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let events: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
        let events_clone = Arc::clone(&events);

        let _handle = subscribe_fn(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            move |event| {
                events_clone.lock().unwrap().push(event.timestamp());
            },
        );

        let senders = registry.get_senders_for_source(0);
        let batch = Arc::new(make_batch(1));
        senders[0]
            .send(ChangeEvent::insert(batch, 5000, 1))
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = events.lock().unwrap();
        assert_eq!(*received, vec![5000]);
    }

    #[tokio::test]
    async fn test_callback_ordering() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();
        let events = Arc::clone(&cb.events);

        let _handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        let senders = registry.get_senders_for_source(0);

        for i in 0..10i64 {
            let batch = Arc::new(make_batch(1));
            senders[0]
                .send(ChangeEvent::insert(batch, i, i as u64))
                .unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let received = events.lock().unwrap();
        assert_eq!(received.len(), 10);
        let expected: Vec<i64> = (0..10).collect();
        assert_eq!(*received, expected);
    }

    #[tokio::test]
    async fn test_callback_handle_metrics() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let cb = TestCallback::new();

        let handle = subscribe_callback(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
            cb,
        );

        let m = handle.metrics().unwrap();
        assert_eq!(m.id, handle.id());
        assert_eq!(m.source_name, "trades");
        assert_eq!(m.state, SubscriptionState::Active);
    }
}
