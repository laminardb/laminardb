# F-SUB-004: Subscription Dispatcher (Ring 1)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-004 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SUB-001, F-SUB-002 (Notification Slot), F-SUB-003 (Registry) |
| **Blocks** | F-SUB-005, F-SUB-006, F-SUB-007 |
| **Created** | 2026-02-01 |

## Summary

The Subscription Dispatcher is the Ring 1 component that bridges Ring 0 notifications to subscriber channels. It runs as an async task that drains the `NotificationRing` from Ring 0, resolves `NotificationRef` values into full `ChangeEvent` instances (by fetching data via zero-copy from the source ring buffer), looks up the target subscriptions in the `SubscriptionRegistry`, and broadcasts events to their `tokio::sync::broadcast` channels.

This is the central routing component of the reactive subscription system.

**Research Reference**: [Reactive Subscriptions Research - Three-Tier Push Architecture](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: Drain NotificationRing(s) from Ring 0 on every poll cycle
- **FR-2**: Resolve NotificationRef to ChangeEvent by fetching data from source buffer
- **FR-3**: Look up target subscriptions via SubscriptionRegistry
- **FR-4**: Broadcast ChangeEvent to all active subscribers' channels
- **FR-5**: Handle broadcast errors (lagged receivers, closed channels)
- **FR-6**: Support per-core notification rings (thread-per-core mode)
- **FR-7**: Configurable poll interval (adaptive: busy-wait -> yield -> park)
- **FR-8**: Metrics: events dispatched, events dropped, dispatch latency

### Non-Functional Requirements

- **NFR-1**: Dispatch latency < 500ns per event (from ring drain to broadcast send)
- **NFR-2**: Support 10M+ events/sec aggregate throughput with batching
- **NFR-3**: Never blocks Ring 0 (SPSC ring is non-blocking)
- **NFR-4**: Graceful handling of slow subscribers (per BackpressureStrategy)
- **NFR-5**: Runs as tokio task(s) in Ring 1

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RING 0 (per core)                             │
│                                                                 │
│  Core 0 NotifRing ──┐                                           │
│  Core 1 NotifRing ──┤                                           │
│  Core 2 NotifRing ──┤                                           │
│  Core N NotifRing ──┘                                           │
│                     │                                           │
├─────────────────────┼───────────────────────────────────────────┤
│                     │  RING 1                                   │
│                     ▼                                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              SubscriptionDispatcher                       │  │
│  │                                                           │  │
│  │  1. Drain all NotificationRings                           │  │
│  │  2. Batch notifications by source_id                      │  │
│  │  3. Resolve NotificationRef → ChangeEvent                 │  │
│  │  4. Lookup subscriptions in Registry                      │  │
│  │  5. Broadcast to subscriber channels                      │  │
│  │                                                           │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │  │
│  │  │ source_id=0 │  │ source_id=1 │  │ source_id=n │      │  │
│  │  │ → subs: [A] │  │ → subs:[B,C]│  │ → subs: [D] │      │  │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘      │  │
│  │         │                │                  │             │  │
│  │         ▼                ▼                  ▼             │  │
│  │  broadcast::send  broadcast::send   broadcast::send      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                    RING 2                                        │
│  Subscribers receive via broadcast::Receiver                    │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Dispatcher configuration.
#[derive(Debug, Clone)]
pub struct DispatcherConfig {
    /// Maximum notifications to drain per poll cycle.
    pub max_drain_per_cycle: usize,
    /// Idle sleep duration when no notifications available.
    pub idle_sleep: Duration,
    /// Number of spin iterations before yielding.
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

/// Dispatcher metrics.
#[derive(Debug, Default)]
pub struct DispatcherMetrics {
    /// Total notifications drained from Ring 0.
    pub notifications_drained: std::sync::atomic::AtomicU64,
    /// Total events dispatched to subscribers.
    pub events_dispatched: std::sync::atomic::AtomicU64,
    /// Total events dropped due to backpressure.
    pub events_dropped: std::sync::atomic::AtomicU64,
    /// Total dispatch cycles.
    pub dispatch_cycles: std::sync::atomic::AtomicU64,
    /// Total idle cycles (nothing to dispatch).
    pub idle_cycles: std::sync::atomic::AtomicU64,
    /// Maximum dispatch latency observed (nanoseconds).
    pub max_dispatch_latency_ns: std::sync::atomic::AtomicU64,
}

/// Data source for resolving NotificationRef to ChangeEvent.
///
/// Implemented by the DAG executor or streaming pipeline to provide
/// zero-copy access to the source data referenced by NotificationRef.
pub trait NotificationDataSource: Send + Sync {
    /// Resolves a notification reference to a full ChangeEvent.
    ///
    /// The implementation should use `batch_offset` to find the
    /// RecordBatch in the source's ring buffer without copying.
    fn resolve(&self, notif: &NotificationRef) -> Option<ChangeEvent>;
}

/// The Ring 1 subscription dispatcher.
///
/// Runs as an async task that bridges Ring 0 notifications to
/// subscriber broadcast channels.
pub struct SubscriptionDispatcher {
    /// Notification rings from Ring 0 (one per core).
    notification_rings: Vec<Arc<NotificationRing>>,
    /// Subscription registry.
    registry: Arc<SubscriptionRegistry>,
    /// Data source for resolving notifications.
    data_source: Arc<dyn NotificationDataSource>,
    /// Configuration.
    config: DispatcherConfig,
    /// Metrics.
    metrics: Arc<DispatcherMetrics>,
    /// Shutdown signal.
    shutdown: tokio::sync::watch::Receiver<bool>,
}

impl SubscriptionDispatcher {
    /// Creates a new dispatcher.
    pub fn new(
        notification_rings: Vec<Arc<NotificationRing>>,
        registry: Arc<SubscriptionRegistry>,
        data_source: Arc<dyn NotificationDataSource>,
        config: DispatcherConfig,
        shutdown: tokio::sync::watch::Receiver<bool>,
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

    /// Runs the dispatcher loop.
    ///
    /// This should be spawned as a tokio task:
    /// ```rust,ignore
    /// tokio::spawn(dispatcher.run());
    /// ```
    pub async fn run(mut self) {
        let mut batch_buffer: Vec<(u32, Vec<NotificationRef>)> = Vec::new();

        loop {
            // Check shutdown
            if *self.shutdown.borrow() {
                break;
            }

            let drained = self.drain_and_dispatch(&mut batch_buffer);

            self.metrics.dispatch_cycles.fetch_add(
                1, std::sync::atomic::Ordering::Relaxed
            );

            if drained == 0 {
                self.metrics.idle_cycles.fetch_add(
                    1, std::sync::atomic::Ordering::Relaxed
                );

                // Adaptive wait: spin -> yield -> sleep
                for _ in 0..self.config.spin_iterations {
                    std::hint::spin_loop();
                }
                tokio::time::sleep(self.config.idle_sleep).await;
            }
        }
    }

    /// Drains all notification rings and dispatches events.
    fn drain_and_dispatch(
        &self,
        batch_buffer: &mut Vec<(u32, Vec<NotificationRef>)>,
    ) -> usize {
        batch_buffer.clear();
        let mut total_drained = 0;

        // Phase 1: Drain all notification rings
        for ring in &self.notification_rings {
            ring.drain_into(|notif| {
                total_drained += 1;

                if self.config.batch_by_source {
                    // Batch by source_id for efficient dispatch
                    let source_id = notif.source_id;
                    if let Some((_, batch)) = batch_buffer.iter_mut()
                        .find(|(id, _)| *id == source_id)
                    {
                        batch.push(notif);
                    } else {
                        batch_buffer.push((source_id, vec![notif]));
                    }
                } else {
                    // Dispatch immediately
                    self.dispatch_single(notif);
                }
            });
        }

        self.metrics.notifications_drained.fetch_add(
            total_drained as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        // Phase 2: Dispatch batched notifications
        if self.config.batch_by_source {
            for (source_id, notifs) in batch_buffer.drain(..) {
                self.dispatch_batch(source_id, &notifs);
            }
        }

        total_drained
    }

    /// Dispatches a single notification.
    fn dispatch_single(&self, notif: NotificationRef) {
        let event = match self.data_source.resolve(&notif) {
            Some(e) => e,
            None => return,
        };

        let senders = self.registry.get_senders_for_source(notif.source_id);

        for sender in senders {
            match sender.send(event.clone()) {
                Ok(_) => {
                    self.metrics.events_dispatched.fetch_add(
                        1, std::sync::atomic::Ordering::Relaxed
                    );
                }
                Err(_) => {
                    self.metrics.events_dropped.fetch_add(
                        1, std::sync::atomic::Ordering::Relaxed
                    );
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
            let event = match self.data_source.resolve(notif) {
                Some(e) => e,
                None => continue,
            };

            for sender in &senders {
                match sender.send(event.clone()) {
                    Ok(_) => {
                        self.metrics.events_dispatched.fetch_add(
                            1, std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    Err(_) => {
                        self.metrics.events_dropped.fetch_add(
                            1, std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                }
            }
        }
    }

    /// Returns dispatcher metrics.
    pub fn metrics(&self) -> &Arc<DispatcherMetrics> {
        &self.metrics
    }
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| DagExecutor | `laminar-core/src/dag/executor.rs` | Implement `NotificationDataSource` |
| TPC Reactor | `laminar-core/src/tpc/reactor.rs` | Spawn dispatcher task per core |
| ConnectorBridgeRuntime | `laminar-connectors/src/bridge/runtime.rs` | Wire notifications |

### New Files

- `crates/laminar-core/src/subscription/dispatcher.rs` - SubscriptionDispatcher

## Test Plan

### Unit Tests

- [ ] `test_dispatcher_drain_single_ring` - Single ring drain
- [ ] `test_dispatcher_drain_multiple_rings` - Multi-core drain
- [ ] `test_dispatcher_dispatch_single` - Single notification dispatch
- [ ] `test_dispatcher_dispatch_batch` - Batched dispatch
- [ ] `test_dispatcher_no_subscribers` - No subs = no work
- [ ] `test_dispatcher_paused_subscriber_skipped` - Paused subs excluded
- [ ] `test_dispatcher_metrics` - Counter accuracy
- [ ] `test_dispatcher_shutdown` - Clean shutdown
- [ ] `test_dispatcher_resolve_failure` - Handle missing data gracefully
- [ ] `test_dispatcher_lagged_subscriber` - Broadcast lagged handling

### Integration Tests

- [ ] `test_end_to_end_notification_to_subscriber` - Full pipeline
- [ ] `test_dispatcher_with_multiple_cores` - TPC integration
- [ ] `test_dispatcher_throughput` - Sustained throughput test

### Benchmarks

- [ ] `bench_dispatcher_drain_100` - Target: < 1us for 100 notifications
- [ ] `bench_dispatcher_dispatch_to_1_sub` - Target: < 200ns per event
- [ ] `bench_dispatcher_dispatch_to_10_subs` - Target: < 500ns per event
- [ ] `bench_dispatcher_throughput` - Target: > 5M events/sec

## Completion Checklist

- [ ] SubscriptionDispatcher implemented with async run loop
- [ ] NotificationDataSource trait defined
- [ ] Multi-core notification ring draining
- [ ] Batched dispatch by source_id
- [ ] Adaptive wait strategy (spin -> yield -> sleep)
- [ ] Metrics for monitoring
- [ ] Graceful shutdown
- [ ] Unit tests passing (10+ tests)
- [ ] Benchmarks meeting targets
- [ ] Documentation with architecture diagram
- [ ] Code reviewed

## References

- [F-SUB-002: Notification Slot](F-SUB-002-notification-slot.md)
- [F-SUB-003: Subscription Registry](F-SUB-003-subscription-registry.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [tokio::sync::broadcast](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)
