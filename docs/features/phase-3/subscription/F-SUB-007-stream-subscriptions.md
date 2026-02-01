# F-SUB-007: Stream Subscriptions (Async Stream)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-007 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (1-3 days) |
| **Dependencies** | F-SUB-005 (Push Subscription API) |
| **Created** | 2026-02-01 |

## Summary

Provides a `subscribe_stream()` API that returns an `impl Stream<Item = ChangeEvent>`, enabling idiomatic Rust async stream consumption with combinators like `.filter()`, `.map()`, `.take()`, and `.buffer_unordered()`. Wraps the channel-based `PushSubscription` (F-SUB-005) in a `tokio_stream`-compatible async Stream adapter.

This is the most Rust-idiomatic consumption pattern and integrates naturally with the broader tokio/async ecosystem.

**Research Reference**: [Reactive Subscriptions Research - Option C: Stream-based](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `subscribe_stream()` returns `impl Stream<Item = ChangeEvent>`
- **FR-2**: Stream terminates when source closes or subscription is cancelled
- **FR-3**: Lagged events are skipped with a warning (continues streaming)
- **FR-4**: Stream is `Unpin` and cancel-safe (works with `select!`)
- **FR-5**: Companion `subscribe_stream_with_errors()` returns `Result<ChangeEvent, _>`
- **FR-6**: Filter/map combinators work: `stream.filter(|e| e.has_data())`

### Non-Functional Requirements

- **NFR-1**: Stream poll latency < 2us from event broadcast
- **NFR-2**: No additional allocation per event (re-uses broadcast channel)
- **NFR-3**: Pin-safe for use in async contexts
- **NFR-4**: Minimal overhead vs direct channel recv (~50-100ns wrapper cost)

## Technical Design

### Data Structures

```rust
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

/// Async stream wrapper for push subscriptions.
///
/// Implements `Stream<Item = ChangeEvent>`, dropping errors
/// and lagged events silently.
///
/// # Usage
///
/// ```rust,ignore
/// use tokio_stream::StreamExt;
///
/// let mut stream = pipeline.subscribe_stream("SELECT * FROM trades")?;
///
/// // Idiomatic stream consumption
/// while let Some(event) = stream.next().await {
///     match event {
///         ChangeEvent::Insert { data, .. } => process(data),
///         ChangeEvent::Watermark { timestamp } => advance(timestamp),
///         _ => {}
///     }
/// }
///
/// // With combinators
/// let inserts = pipeline.subscribe_stream("SELECT * FROM trades")?
///     .filter(|e| e.event_type() == EventType::Insert)
///     .take(100);
///
/// tokio::pin!(inserts);
/// while let Some(event) = inserts.next().await {
///     process(event);
/// }
/// ```
pub struct ChangeEventStream {
    /// Underlying push subscription.
    inner: PushSubscription,
    /// Whether the stream has terminated.
    terminated: bool,
}

impl ChangeEventStream {
    /// Creates a new stream from a push subscription.
    pub(crate) fn new(inner: PushSubscription) -> Self {
        Self {
            inner,
            terminated: false,
        }
    }

    /// Returns the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.inner.id()
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        self.inner.query()
    }

    /// Pauses the underlying subscription.
    pub fn pause(&self) -> bool {
        self.inner.pause()
    }

    /// Resumes the underlying subscription.
    pub fn resume(&self) -> bool {
        self.inner.resume()
    }

    /// Cancels the underlying subscription.
    pub fn cancel(&mut self) {
        self.terminated = true;
        self.inner.cancel();
    }
}

impl Stream for ChangeEventStream {
    type Item = ChangeEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        // Poll the broadcast receiver
        match self.inner.receiver.try_recv() {
            Ok(event) => Poll::Ready(Some(event)),
            Err(broadcast::error::TryRecvError::Empty) => {
                // Register waker and return Pending
                // Note: broadcast::Receiver doesn't natively support
                // polling, so we use a small wrapper that bridges
                // the async recv to Poll-based Stream.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                tracing::debug!("Stream lagged behind by {} events", n);
                // Continue - try to get next available
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                self.terminated = true;
                Poll::Ready(None)
            }
        }
    }
}

impl Drop for ChangeEventStream {
    fn drop(&mut self) {
        if !self.terminated {
            self.inner.cancel();
        }
    }
}

/// Stream wrapper that also yields errors.
///
/// Use this when you need to handle lagged/error conditions explicitly.
pub struct ChangeEventResultStream {
    inner: PushSubscription,
    terminated: bool,
}

impl ChangeEventResultStream {
    pub(crate) fn new(inner: PushSubscription) -> Self {
        Self {
            inner,
            terminated: false,
        }
    }
}

impl Stream for ChangeEventResultStream {
    type Item = Result<ChangeEvent, PushSubscriptionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        match self.inner.receiver.try_recv() {
            Ok(event) => Poll::Ready(Some(Ok(event))),
            Err(broadcast::error::TryRecvError::Empty) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(broadcast::error::TryRecvError::Lagged(n)) => {
                Poll::Ready(Some(Err(PushSubscriptionError::Lagged(n))))
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                self.terminated = true;
                Poll::Ready(None)
            }
        }
    }
}

impl Drop for ChangeEventResultStream {
    fn drop(&mut self) {
        // inner.drop() handles cancel via PushSubscription's Drop
    }
}
```

### Pipeline API Extension

```rust
impl Pipeline {
    /// Creates an async Stream subscription.
    ///
    /// Returns a `Stream<Item = ChangeEvent>` that yields change events.
    /// Lagged events are silently skipped; the stream terminates when
    /// the source is closed.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = pipeline.subscribe_stream("ohlc_1s")?;
    ///
    /// while let Some(event) = stream.next().await {
    ///     process(event);
    /// }
    /// ```
    pub fn subscribe_stream(
        &self,
        query: &str,
    ) -> Result<ChangeEventStream, PushSubscriptionError> {
        let sub = self.subscribe_push(query)?;
        Ok(ChangeEventStream::new(sub))
    }

    /// Creates an async Stream that also yields errors.
    ///
    /// Use this when you need to handle lagged events explicitly.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = pipeline.subscribe_stream_with_errors("trades")?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok(event) => process(event),
    ///         Err(PushSubscriptionError::Lagged(n)) => {
    ///             eprintln!("Missed {} events", n);
    ///         }
    ///         Err(e) => break,
    ///     }
    /// }
    /// ```
    pub fn subscribe_stream_with_errors(
        &self,
        query: &str,
    ) -> Result<ChangeEventResultStream, PushSubscriptionError> {
        let sub = self.subscribe_push(query)?;
        Ok(ChangeEventResultStream::new(sub))
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Pipeline | `laminar-core/src/subscription/handle.rs` | Add stream methods |
| PushSubscription | `laminar-core/src/subscription/handle.rs` | Expose receiver field |

### New Files

- `crates/laminar-core/src/subscription/stream.rs` - ChangeEventStream, ChangeEventResultStream

### Dependencies

- `tokio-stream` crate (add to Cargo.toml if not present)
- `pin-project-lite` for safe pinning (optional)

## Test Plan

### Unit Tests

- [ ] `test_stream_receives_events` - Basic stream consumption
- [ ] `test_stream_terminates_on_close` - Stream ends when source closes
- [ ] `test_stream_cancel` - Cancel stops stream
- [ ] `test_stream_drop_cancels` - Drop auto-cancels
- [ ] `test_stream_filter_combinator` - .filter() works
- [ ] `test_stream_map_combinator` - .map() works
- [ ] `test_stream_take_combinator` - .take(n) works
- [ ] `test_stream_with_select` - Works with tokio::select!
- [ ] `test_result_stream_yields_errors` - Lagged error surfaces
- [ ] `test_result_stream_terminates_on_close` - Clean termination
- [ ] `test_stream_pause_resume` - Lifecycle through stream handle

### Integration Tests

- [ ] `test_stream_end_to_end` - Push -> stream consumption
- [ ] `test_stream_multiple_consumers` - Multiple streams same query

### Benchmarks

- [ ] `bench_stream_next_latency` - Target: < 2us
- [ ] `bench_stream_throughput` - Target: > 3M events/sec

## Completion Checklist

- [ ] ChangeEventStream implementing Stream<Item = ChangeEvent>
- [ ] ChangeEventResultStream implementing Stream<Item = Result<...>>
- [ ] Pipeline::subscribe_stream() entry point
- [ ] Pipeline::subscribe_stream_with_errors() entry point
- [ ] Pin-safe and cancel-safe
- [ ] Works with tokio_stream combinators
- [ ] Drop-based cleanup
- [ ] Unit tests passing (11+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with combinator examples
- [ ] Code reviewed

## References

- [F-SUB-005: Push Subscription API](F-SUB-005-push-subscription-api.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [tokio-stream docs](https://docs.rs/tokio-stream/)
- [async-stream crate](https://docs.rs/async-stream/)
