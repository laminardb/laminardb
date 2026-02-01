# F-SUB-006: Callback Subscriptions

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-006 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | S (1-3 days) |
| **Dependencies** | F-SUB-005 (Push Subscription API) |
| **Created** | 2026-02-01 |

## Summary

Provides a callback-based subscription API where users register a callback function/trait object that is invoked on every change event. This is the lowest-latency consumption pattern as it avoids channel overhead. Internally wraps the channel-based `PushSubscription` (F-SUB-005) and spawns a tokio task that calls the callback for each received event.

**Research Reference**: [Reactive Subscriptions Research - Option A: Callback-based](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: `SubscriptionCallback` trait with `on_change`, `on_error`, `on_complete`
- **FR-2**: `subscribe_with_callback()` API on Pipeline
- **FR-3**: Closure-based shorthand: `subscribe_fn(query, |event| { ... })`
- **FR-4**: Callback runs on a dedicated tokio task (not on dispatcher thread)
- **FR-5**: Subscription handle for lifecycle management (pause/resume/cancel)
- **FR-6**: Error in callback does not crash the dispatcher

### Non-Functional Requirements

- **NFR-1**: Callback invocation within 2us of event broadcast
- **NFR-2**: Callback panic is caught and converted to `on_error`
- **NFR-3**: No additional channel allocation (wraps existing broadcast channel)

## Technical Design

### Data Structures

```rust
use std::sync::Arc;
use arrow::array::RecordBatch;

/// Callback trait for push-based subscriptions.
///
/// Implement this trait to receive change events via callback.
///
/// # Example
///
/// ```rust,ignore
/// struct MyHandler;
///
/// impl SubscriptionCallback for MyHandler {
///     fn on_change(&self, event: ChangeEvent) {
///         match event {
///             ChangeEvent::Insert { data, .. } => println!("Insert: {} rows", data.num_rows()),
///             ChangeEvent::Delete { data, .. } => println!("Delete: {} rows", data.num_rows()),
///             _ => {}
///         }
///     }
///
///     fn on_error(&self, error: PushSubscriptionError) {
///         eprintln!("Subscription error: {}", error);
///     }
///
///     fn on_complete(&self) {
///         println!("Subscription closed");
///     }
/// }
/// ```
pub trait SubscriptionCallback: Send + Sync + 'static {
    /// Called for each change event.
    fn on_change(&self, event: ChangeEvent);

    /// Called when an error occurs.
    ///
    /// Default implementation logs the error. Override for custom handling.
    fn on_error(&self, error: PushSubscriptionError) {
        tracing::warn!("Subscription error: {}", error);
    }

    /// Called when the subscription is closed (source dropped or cancelled).
    ///
    /// Default implementation is a no-op.
    fn on_complete(&self) {}
}

/// Handle for a callback-based subscription.
///
/// Provides lifecycle management for the callback subscription.
/// Dropping this handle cancels the subscription and stops the callback task.
pub struct CallbackSubscriptionHandle {
    /// Underlying push subscription handle.
    inner: PushSubscription,
    /// Task handle for the callback runner.
    task: tokio::task::JoinHandle<()>,
}

impl CallbackSubscriptionHandle {
    /// Pauses the subscription (events buffered/dropped per config).
    pub fn pause(&self) -> bool {
        self.inner.pause()
    }

    /// Resumes the subscription.
    pub fn resume(&self) -> bool {
        self.inner.resume()
    }

    /// Cancels the subscription and stops the callback task.
    pub fn cancel(mut self) {
        self.inner.cancel();
        self.task.abort();
    }

    /// Returns the subscription ID.
    pub fn id(&self) -> SubscriptionId {
        self.inner.id()
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        self.inner.query()
    }

    /// Returns subscription metrics.
    pub fn metrics(&self) -> Option<SubscriptionMetrics> {
        self.inner.metrics()
    }
}

impl Drop for CallbackSubscriptionHandle {
    fn drop(&mut self) {
        self.task.abort();
        // inner.drop() will cancel via registry
    }
}
```

### Pipeline API Extension

```rust
impl Pipeline {
    /// Creates a callback-based subscription.
    ///
    /// The callback is invoked for each change event on a dedicated
    /// tokio task. The callback must be `Send + Sync + 'static`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let handle = pipeline.subscribe_with_callback(
    ///     "SELECT * FROM trades",
    ///     MyTradeHandler::new(),
    /// )?;
    ///
    /// // Later...
    /// handle.cancel();
    /// ```
    pub fn subscribe_with_callback<C: SubscriptionCallback>(
        &self,
        query: &str,
        callback: C,
    ) -> Result<CallbackSubscriptionHandle, PushSubscriptionError> {
        let mut sub = self.subscribe_push(query)?;
        let callback = Arc::new(callback);

        let task = tokio::spawn({
            let callback = Arc::clone(&callback);
            async move {
                loop {
                    match sub.recv().await {
                        Ok(event) => {
                            // Catch panics in user callback
                            let cb = Arc::clone(&callback);
                            let result = std::panic::catch_unwind(
                                std::panic::AssertUnwindSafe(|| cb.on_change(event))
                            );
                            if let Err(panic) = result {
                                let msg = format!("callback panicked: {:?}", panic);
                                callback.on_error(
                                    PushSubscriptionError::Internal(msg)
                                );
                            }
                        }
                        Err(PushSubscriptionError::Lagged(n)) => {
                            callback.on_error(PushSubscriptionError::Lagged(n));
                            // Continue receiving after lag
                        }
                        Err(e) => {
                            callback.on_error(e);
                            callback.on_complete();
                            break;
                        }
                    }
                }
            }
        });

        // Note: we need a second PushSubscription for the handle since
        // the first was moved into the task. Re-create with same config.
        let handle_sub = self.subscribe_push(query)?;

        Ok(CallbackSubscriptionHandle {
            inner: handle_sub,
            task,
        })
    }

    /// Creates a closure-based subscription (convenience method).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let handle = pipeline.subscribe_fn("SELECT * FROM trades", |event| {
    ///     println!("Got event: {:?}", event.event_type());
    /// })?;
    /// ```
    pub fn subscribe_fn<F>(
        &self,
        query: &str,
        f: F,
    ) -> Result<CallbackSubscriptionHandle, PushSubscriptionError>
    where
        F: Fn(ChangeEvent) + Send + Sync + 'static,
    {
        struct FnCallback<F>(F);
        impl<F: Fn(ChangeEvent) + Send + Sync + 'static> SubscriptionCallback for FnCallback<F> {
            fn on_change(&self, event: ChangeEvent) {
                (self.0)(event);
            }
        }

        self.subscribe_with_callback(query, FnCallback(f))
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| Pipeline | `laminar-core/src/subscription/handle.rs` | Add callback methods |
| PushSubscription | `laminar-core/src/subscription/handle.rs` | Used internally |

### New Files

- `crates/laminar-core/src/subscription/callback.rs` - SubscriptionCallback trait, CallbackSubscriptionHandle

## Test Plan

### Unit Tests

- [ ] `test_callback_receives_events` - Events delivered to callback
- [ ] `test_callback_on_error` - Error callback invoked on lag
- [ ] `test_callback_on_complete` - Complete callback on close
- [ ] `test_callback_panic_caught` - Panic in callback -> on_error
- [ ] `test_callback_handle_pause_resume` - Lifecycle through handle
- [ ] `test_callback_handle_cancel` - Cancel stops task
- [ ] `test_callback_handle_drop_cancels` - Drop auto-cancels
- [ ] `test_subscribe_fn` - Closure shorthand
- [ ] `test_callback_ordering` - Events delivered in order

### Integration Tests

- [ ] `test_callback_end_to_end` - Insert -> callback invoked

### Benchmarks

- [ ] `bench_callback_latency` - Target: < 2us from event send to callback
- [ ] `bench_callback_throughput` - Target: > 2M callbacks/sec

## Completion Checklist

- [ ] SubscriptionCallback trait defined
- [ ] CallbackSubscriptionHandle with lifecycle management
- [ ] subscribe_with_callback() on Pipeline
- [ ] subscribe_fn() convenience method
- [ ] Panic catching in callback runner
- [ ] Drop-based cleanup
- [ ] Unit tests passing (9+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with examples
- [ ] Code reviewed

## References

- [F-SUB-005: Push Subscription API](F-SUB-005-push-subscription-api.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [Observer Pattern in Rust](https://refactoring.guru/design-patterns/observer/rust/example)
