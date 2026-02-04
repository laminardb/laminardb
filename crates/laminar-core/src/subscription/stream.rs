//! Async `Stream` subscriptions — [`ChangeEventStream`] and
//! [`ChangeEventResultStream`].
//!
//! Wraps the broadcast channel from the [`SubscriptionRegistry`] in a
//! `tokio_stream`-compatible async `Stream`, enabling idiomatic consumption
//! with combinators like `.filter()`, `.map()`, `.take()`, and
//! `.buffer_unordered()`.
//!
//! # API Styles
//!
//! - [`ChangeEventStream`] — `Stream<Item = ChangeEvent>`, silently skips lag.
//! - [`ChangeEventResultStream`] — `Stream<Item = Result<ChangeEvent, _>>`,
//!   surfaces lag errors for explicit handling.
//!
//! # Usage
//!
//! ```rust,ignore
//! use tokio_stream::StreamExt;
//!
//! let mut stream = subscribe_stream(registry, "trades".into(), 0, config);
//!
//! while let Some(event) = stream.next().await {
//!     process(event);
//! }
//!
//! // With combinators
//! let inserts = subscribe_stream(registry, "trades".into(), 0, config)
//!     .filter(|e| e.event_type() == EventType::Insert)
//!     .take(100);
//! tokio::pin!(inserts);
//! while let Some(event) = inserts.next().await {
//!     process(event);
//! }
//! ```
//!
//! # Implementation Note
//!
//! Uses [`BroadcastStream`](tokio_stream::wrappers::BroadcastStream) internally
//! for correct async wakeup semantics. A naive manual `poll_next` with
//! `try_recv` + `cx.waker().wake_by_ref()` causes a busy-spin loop at 100%
//! CPU. `BroadcastStream` integrates with tokio's async machinery — it only
//! wakes the task when new data is actually available.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::Stream;

use crate::subscription::event::ChangeEvent;
use crate::subscription::handle::PushSubscriptionError;
use crate::subscription::registry::{
    SubscriptionConfig, SubscriptionId, SubscriptionMetrics, SubscriptionRegistry,
};

// ---------------------------------------------------------------------------
// ChangeEventStream
// ---------------------------------------------------------------------------

/// Async stream wrapper for push subscriptions.
///
/// Implements `Stream<Item = ChangeEvent>`, silently skipping lagged events
/// (with a `tracing::debug!` log). The stream terminates when the source is
/// closed or the subscription is cancelled.
///
/// All fields are `Unpin` (including `BroadcastStream`), so the struct is
/// `Unpin` and works directly with `tokio::select!` without explicit pinning.
///
/// Dropping the stream automatically cancels the subscription in the registry.
pub struct ChangeEventStream {
    /// Subscription ID for lifecycle management.
    id: SubscriptionId,
    /// Registry reference for pause/resume/cancel.
    registry: Arc<SubscriptionRegistry>,
    /// Query or source name for diagnostics.
    query: String,
    /// Inner `BroadcastStream` that handles proper async wakeup.
    inner: BroadcastStream<ChangeEvent>,
    /// Whether the stream has terminated.
    terminated: bool,
}

impl ChangeEventStream {
    /// Returns the subscription ID.
    #[must_use]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns the query or source name for this subscription.
    #[must_use]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// Returns `true` if the stream has terminated.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// Pauses the underlying subscription.
    ///
    /// While paused, events are buffered or dropped per the backpressure
    /// configuration. Returns `true` if the subscription was active and is
    /// now paused.
    #[must_use]
    pub fn pause(&self) -> bool {
        self.registry.pause(self.id)
    }

    /// Resumes the underlying subscription.
    ///
    /// Returns `true` if the subscription was paused and is now active.
    #[must_use]
    pub fn resume(&self) -> bool {
        self.registry.resume(self.id)
    }

    /// Cancels the subscription and terminates the stream.
    ///
    /// Subsequent calls to `poll_next` / `next()` return `None`.
    pub fn cancel(&mut self) {
        if !self.terminated {
            self.terminated = true;
            self.registry.cancel(self.id);
        }
    }

    /// Returns subscription metrics from the registry.
    #[must_use]
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.registry.metrics(self.id)
    }
}

impl Stream for ChangeEventStream {
    type Item = ChangeEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: All fields are Unpin (BroadcastStream stores a ReusableBoxFuture
        // which is Unpin), so Pin::get_mut is safe.
        let this = self.get_mut();

        if this.terminated {
            return Poll::Ready(None);
        }

        // Delegate to BroadcastStream, looping on lag errors.
        loop {
            match Pin::new(&mut this.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(event))) => return Poll::Ready(Some(event)),
                Poll::Ready(Some(Err(_lagged))) => {
                    // BroadcastStreamRecvError — silently skip lagged events.
                    tracing::debug!("stream subscription lagged, skipping missed events");
                }
                Poll::Ready(None) => {
                    this.terminated = true;
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for ChangeEventStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.registry.cancel(self.id);
        }
    }
}

// ---------------------------------------------------------------------------
// ChangeEventResultStream
// ---------------------------------------------------------------------------

/// Async stream that also yields lag errors.
///
/// Implements `Stream<Item = Result<ChangeEvent, PushSubscriptionError>>`,
/// allowing explicit handling of lag and error conditions. Use this when
/// you need to react to missed events rather than silently skipping them.
///
/// # Usage
///
/// ```rust,ignore
/// use tokio_stream::StreamExt;
///
/// let mut stream = subscribe_stream_with_errors(registry, "trades".into(), 0, config);
///
/// while let Some(result) = stream.next().await {
///     match result {
///         Ok(event) => process(event),
///         Err(PushSubscriptionError::Lagged(n)) => {
///             eprintln!("Missed {n} events");
///         }
///         Err(e) => break,
///     }
/// }
/// ```
pub struct ChangeEventResultStream {
    /// Subscription ID.
    id: SubscriptionId,
    /// Registry reference.
    registry: Arc<SubscriptionRegistry>,
    /// Inner `BroadcastStream`.
    inner: BroadcastStream<ChangeEvent>,
    /// Whether the stream has terminated.
    terminated: bool,
}

impl ChangeEventResultStream {
    /// Returns the subscription ID.
    #[must_use]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    /// Returns `true` if the stream has terminated.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }

    /// Cancels the subscription and terminates the stream.
    pub fn cancel(&mut self) {
        if !self.terminated {
            self.terminated = true;
            self.registry.cancel(self.id);
        }
    }
}

impl Stream for ChangeEventResultStream {
    type Item = Result<ChangeEvent, PushSubscriptionError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.terminated {
            return Poll::Ready(None);
        }

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(event))) => Poll::Ready(Some(Ok(event))),
            Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                Poll::Ready(Some(Err(PushSubscriptionError::Lagged(n))))
            }
            Poll::Ready(None) => {
                this.terminated = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ChangeEventResultStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.registry.cancel(self.id);
        }
    }
}

// ---------------------------------------------------------------------------
// Factory Functions
// ---------------------------------------------------------------------------

/// Creates an async `Stream` subscription.
///
/// Returns a [`ChangeEventStream`] that yields [`ChangeEvent`]s. Lagged
/// events are silently skipped; the stream terminates when the source is
/// closed or the subscription is cancelled.
///
/// # Arguments
///
/// * `registry` — Subscription registry.
/// * `source_name` — Name of the source MV or query.
/// * `source_id` — Ring 0 source identifier.
/// * `config` — Subscription configuration.
pub fn subscribe_stream(
    registry: Arc<SubscriptionRegistry>,
    source_name: String,
    source_id: u32,
    config: SubscriptionConfig,
) -> ChangeEventStream {
    let (id, receiver) = registry.create(source_name.clone(), source_id, config);
    ChangeEventStream {
        id,
        registry,
        query: source_name,
        inner: BroadcastStream::new(receiver),
        terminated: false,
    }
}

/// Creates an async `Stream` that also yields errors.
///
/// Returns a [`ChangeEventResultStream`] that yields
/// `Result<ChangeEvent, PushSubscriptionError>`, allowing explicit handling
/// of lagged events.
///
/// # Arguments
///
/// * `registry` — Subscription registry.
/// * `source_name` — Name of the source MV or query.
/// * `source_id` — Ring 0 source identifier.
/// * `config` — Subscription configuration.
pub fn subscribe_stream_with_errors(
    registry: Arc<SubscriptionRegistry>,
    source_name: String,
    source_id: u32,
    config: SubscriptionConfig,
) -> ChangeEventResultStream {
    let (id, receiver) = registry.create(source_name, source_id, config);
    ChangeEventResultStream {
        id,
        registry,
        inner: BroadcastStream::new(receiver),
        terminated: false,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
#[allow(clippy::cast_sign_loss)]
#[allow(clippy::field_reassign_with_default)]
#[allow(clippy::ignored_unit_patterns)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use tokio_stream::StreamExt;

    use crate::subscription::event::EventType;
    use crate::subscription::registry::SubscriptionState;

    fn make_batch(n: usize) -> arrow_array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let values: Vec<i64> = (0..n as i64).collect();
        let array = Int64Array::from(values);
        arrow_array::RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    /// Helper: create a stream + get senders for pushing events.
    fn make_stream(
        name: &str,
    ) -> (
        Arc<SubscriptionRegistry>,
        ChangeEventStream,
        Vec<tokio::sync::broadcast::Sender<ChangeEvent>>,
    ) {
        let registry = Arc::new(SubscriptionRegistry::new());
        let stream = subscribe_stream(
            Arc::clone(&registry),
            name.into(),
            0,
            SubscriptionConfig::default(),
        );
        let senders = registry.get_senders_for_source(0);
        (registry, stream, senders)
    }

    /// Helper: send N insert events to the first sender.
    fn send_events(senders: &[tokio::sync::broadcast::Sender<ChangeEvent>], count: usize) {
        for i in 0..count {
            let batch = Arc::new(make_batch(1));
            senders[0]
                .send(ChangeEvent::insert(batch, i as i64 * 1000, i as u64))
                .unwrap();
        }
    }

    // --- Basic stream consumption ---

    #[tokio::test]
    async fn test_stream_receives_events() {
        let (_reg, mut stream, senders) = make_stream("trades");

        send_events(&senders, 5);

        for i in 0..5i64 {
            let event = stream.next().await.unwrap();
            assert_eq!(event.timestamp(), i * 1000);
            assert_eq!(event.sequence(), Some(i as u64));
        }
    }

    #[tokio::test]
    async fn test_stream_terminates_on_close() {
        let (reg, mut stream, senders) = make_stream("trades");

        // Send one event then close
        send_events(&senders, 1);
        let event = stream.next().await.unwrap();
        assert_eq!(event.timestamp(), 0);

        // Close the channel — drop cloned senders AND cancel the registry
        // entry (which drops the entry's broadcast::Sender).
        drop(senders);
        reg.cancel(stream.id());

        let result = stream.next().await;
        assert!(result.is_none());
        assert!(stream.is_terminated());
    }

    #[tokio::test]
    async fn test_stream_cancel() {
        let (reg, mut stream, _senders) = make_stream("trades");
        assert_eq!(reg.subscription_count(), 1);

        stream.cancel();

        assert!(stream.is_terminated());
        assert_eq!(reg.subscription_count(), 0);

        // Subsequent next() returns None
        let result = stream.next().await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_stream_drop_cancels() {
        let registry = Arc::new(SubscriptionRegistry::new());
        {
            let _stream = subscribe_stream(
                Arc::clone(&registry),
                "trades".into(),
                0,
                SubscriptionConfig::default(),
            );
            assert_eq!(registry.subscription_count(), 1);
        }
        // Dropped — should be cancelled
        assert_eq!(registry.subscription_count(), 0);
    }

    // --- Combinator tests ---

    #[tokio::test]
    async fn test_stream_filter_combinator() {
        let (reg, stream, senders) = make_stream("trades");
        let id = stream.id();

        // Send mixed events: inserts and watermarks
        let batch = Arc::new(make_batch(1));
        senders[0]
            .send(ChangeEvent::insert(Arc::clone(&batch), 1000, 1))
            .unwrap();
        senders[0].send(ChangeEvent::watermark(2000)).unwrap();
        senders[0]
            .send(ChangeEvent::insert(Arc::clone(&batch), 3000, 3))
            .unwrap();

        // Close the channel so .collect() terminates
        drop(senders);
        reg.cancel(id);

        // Filter to inserts only
        let inserts: Vec<_> = stream
            .filter(|e| e.event_type() == EventType::Insert)
            .collect()
            .await;

        assert_eq!(inserts.len(), 2);
        assert_eq!(inserts[0].timestamp(), 1000);
        assert_eq!(inserts[1].timestamp(), 3000);
    }

    #[tokio::test]
    async fn test_stream_map_combinator() {
        let (reg, stream, senders) = make_stream("trades");
        let id = stream.id();

        send_events(&senders, 3);

        // Close the channel so .collect() terminates
        drop(senders);
        reg.cancel(id);

        // Map to timestamps
        let timestamps: Vec<i64> = stream.map(|e| e.timestamp()).collect().await;
        assert_eq!(timestamps, vec![0, 1000, 2000]);
    }

    #[tokio::test]
    async fn test_stream_take_combinator() {
        let (_reg, stream, senders) = make_stream("trades");

        send_events(&senders, 10);

        // Take only 3
        let events: Vec<_> = stream.take(3).collect().await;
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].timestamp(), 0);
        assert_eq!(events[1].timestamp(), 1000);
        assert_eq!(events[2].timestamp(), 2000);
    }

    // --- select! compatibility ---

    #[tokio::test]
    async fn test_stream_with_select() {
        let (_reg, mut stream, senders) = make_stream("trades");

        send_events(&senders, 1);

        let result = tokio::select! {
            event = stream.next() => event,
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                panic!("timeout — event should be immediate");
            }
        };

        let event = result.unwrap();
        assert_eq!(event.timestamp(), 0);
    }

    // --- Result stream ---

    #[tokio::test]
    async fn test_result_stream_yields_errors() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let mut cfg = SubscriptionConfig::default();
        cfg.buffer_size = 4;

        let mut stream =
            subscribe_stream_with_errors(Arc::clone(&registry), "trades".into(), 0, cfg);

        let senders = registry.get_senders_for_source(0);

        // Overflow to cause lag
        for i in 0..20i64 {
            let batch = Arc::new(make_batch(1));
            let _ = senders[0].send(ChangeEvent::insert(batch, i * 100, i as u64));
        }

        // Close the channel so the loop terminates
        drop(senders);
        registry.cancel(stream.id());

        // Collect results — should include at least one Lagged error
        let mut had_error = false;
        let mut had_ok = false;

        while let Some(result) = stream.next().await {
            match result {
                Ok(_) => had_ok = true,
                Err(PushSubscriptionError::Lagged(n)) => {
                    assert!(n > 0);
                    had_error = true;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert!(had_error, "expected at least one lag error");
        assert!(had_ok, "expected at least one successful event");
    }

    #[tokio::test]
    async fn test_result_stream_terminates_on_close() {
        let registry = Arc::new(SubscriptionRegistry::new());
        let mut stream = subscribe_stream_with_errors(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
        );

        let senders = registry.get_senders_for_source(0);
        let batch = Arc::new(make_batch(1));
        senders[0]
            .send(ChangeEvent::insert(batch, 1000, 1))
            .unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.timestamp(), 1000);

        // Close the channel — drop clones AND cancel entry
        drop(senders);
        registry.cancel(stream.id());

        assert!(stream.next().await.is_none());
        assert!(stream.is_terminated());
    }

    // --- Lifecycle ---

    #[tokio::test]
    async fn test_stream_pause_resume() {
        let (reg, stream, _senders) = make_stream("trades");

        assert!(stream.pause());
        assert_eq!(reg.state(stream.id()), Some(SubscriptionState::Paused));

        assert!(!stream.pause()); // already paused

        assert!(stream.resume());
        assert_eq!(reg.state(stream.id()), Some(SubscriptionState::Active));

        assert!(!stream.resume()); // already active
    }

    // --- Accessors ---

    #[tokio::test]
    async fn test_stream_accessors() {
        let (reg, stream, _senders) = make_stream("trades");

        assert_eq!(stream.query(), "trades");
        assert!(!stream.is_terminated());

        let m = stream.metrics().unwrap();
        assert_eq!(m.id, stream.id());
        assert_eq!(m.source_name, "trades");
        assert_eq!(m.state, SubscriptionState::Active);

        drop(reg);
    }

    // --- Multiple consumers ---

    #[tokio::test]
    async fn test_stream_multiple_consumers() {
        let registry = Arc::new(SubscriptionRegistry::new());

        let mut s1 = subscribe_stream(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
        );
        let mut s2 = subscribe_stream(
            Arc::clone(&registry),
            "trades".into(),
            0,
            SubscriptionConfig::default(),
        );

        let senders = registry.get_senders_for_source(0);
        let batch = Arc::new(make_batch(1));
        let event = ChangeEvent::insert(batch, 5000, 10);
        for sender in &senders {
            sender.send(event.clone()).unwrap();
        }

        let e1 = s1.next().await.unwrap();
        let e2 = s2.next().await.unwrap();
        assert_eq!(e1.timestamp(), 5000);
        assert_eq!(e2.timestamp(), 5000);
    }
}
