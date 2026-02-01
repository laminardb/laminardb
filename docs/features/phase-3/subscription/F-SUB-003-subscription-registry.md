# F-SUB-003: Subscription Registry

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SUB-003 |
| **Status** | 📝 Draft |
| **Phase** | 3 |
| **Priority** | P0 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SUB-001 (ChangeEvent Types), F060 (Cascading MVs) |
| **Blocks** | F-SUB-004 (Dispatcher), F-SUB-005, F-SUB-006, F-SUB-007 |
| **Created** | 2026-02-01 |

## Summary

The Subscription Registry manages the lifecycle of all active subscriptions. It is a Ring 2 component that tracks which queries/MVs have subscribers, their configuration (backpressure strategy, buffer sizes, filters), and handles create/pause/resume/cancel operations. The registry provides the mapping from source_id (used by Ring 0 notifications) to the set of active subscriptions and their broadcast channels (used by Ring 1 dispatcher).

**Research Reference**: [Reactive Subscriptions Research - Section 7: Observer Pattern](../../../research/reactive-subscriptions-research-2026.md)

## Requirements

### Functional Requirements

- **FR-1**: Register subscriptions keyed by query/MV name
- **FR-2**: Track subscription state: Active, Paused, Cancelled
- **FR-3**: Map source_id to list of active subscription IDs
- **FR-4**: Support subscription configuration: buffer size, backpressure strategy, filter
- **FR-5**: Lifecycle operations: create, pause, resume, cancel
- **FR-6**: Automatic cleanup on subscriber disconnect/drop
- **FR-7**: Integration with MvRegistry for MV-based subscriptions
- **FR-8**: Subscription metadata: creation time, event count, lag metrics

### Non-Functional Requirements

- **NFR-1**: All lifecycle operations are Ring 2 (no latency requirements)
- **NFR-2**: Source-to-subscription lookup must be O(1) for dispatcher
- **NFR-3**: Support 10,000+ concurrent subscriptions
- **NFR-4**: Thread-safe: concurrent create/cancel from multiple threads

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RING 2: CONTROL PLANE                         │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                  SubscriptionRegistry                     │  │
│  │                                                           │  │
│  │  subscriptions: HashMap<SubscriptionId, SubscriptionEntry>│  │
│  │  by_source: HashMap<u32, Vec<SubscriptionId>>             │  │
│  │  by_name: HashMap<String, Vec<SubscriptionId>>            │  │
│  │                                                           │  │
│  │  create() ──────────────────────────────────────────►     │  │
│  │  pause(id) ─────────────────────────────────────────►     │  │
│  │  resume(id) ────────────────────────────────────────►     │  │
│  │  cancel(id) ────────────────────────────────────────►     │  │
│  │  get_subscribers_for_source(source_id) ──────────────►    │  │
│  │                                                           │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              │                                  │
│                              │ Provides subscription list       │
│                              ▼ to Dispatcher (F-SUB-004)        │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│                    RING 1: BACKGROUND                            │
│                                                                 │
│  Dispatcher reads: registry.get_subscribers_for_source(id)      │
│  to route notifications to the correct broadcast channels       │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Unique subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub u64);

/// Subscription state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionState {
    /// Actively receiving events.
    Active,
    /// Temporarily paused (events are buffered or dropped per config).
    Paused,
    /// Cancelled and pending cleanup.
    Cancelled,
}

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
    /// Maximum batch delay before flushing.
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

/// Backpressure strategy for subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureStrategy {
    /// Drop oldest events when buffer full (real-time priority).
    DropOldest,
    /// Drop newest events when buffer full (completeness priority).
    DropNewest,
    /// Block the dispatcher (NOT for Ring 0 - only blocks Ring 1 dispatch).
    Block,
    /// Sample: deliver every Nth event.
    Sample(usize),
}

/// A registered subscription entry.
#[derive(Debug)]
pub struct SubscriptionEntry {
    /// Unique ID.
    pub id: SubscriptionId,
    /// Target source/MV name.
    pub source_name: String,
    /// Source ID for Ring 0 notification matching.
    pub source_id: u32,
    /// Current state.
    pub state: SubscriptionState,
    /// Configuration.
    pub config: SubscriptionConfig,
    /// Broadcast channel sender for this subscription.
    pub sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    /// Creation timestamp.
    pub created_at: Instant,
    /// Total events delivered.
    pub events_delivered: u64,
    /// Total events dropped (backpressure).
    pub events_dropped: u64,
    /// Current lag (events pending in channel).
    pub current_lag: u64,
}

/// Subscription registry managing all active subscriptions.
///
/// Thread-safe via internal `RwLock`. Read operations (used by
/// the dispatcher on Ring 1) take a read lock. Write operations
/// (create/cancel) take a write lock but are rare (Ring 2 only).
pub struct SubscriptionRegistry {
    /// All subscriptions by ID.
    subscriptions: RwLock<HashMap<SubscriptionId, SubscriptionEntry>>,
    /// Index: source_id -> subscription IDs.
    by_source: RwLock<HashMap<u32, Vec<SubscriptionId>>>,
    /// Index: source name -> subscription IDs.
    by_name: RwLock<HashMap<String, Vec<SubscriptionId>>>,
    /// Next subscription ID.
    next_id: std::sync::atomic::AtomicU64,
}

impl SubscriptionRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            subscriptions: RwLock::new(HashMap::new()),
            by_source: RwLock::new(HashMap::new()),
            by_name: RwLock::new(HashMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Creates a new subscription.
    ///
    /// Returns the subscription ID and a broadcast Receiver.
    pub fn create(
        &self,
        source_name: String,
        source_id: u32,
        config: SubscriptionConfig,
    ) -> (SubscriptionId, tokio::sync::broadcast::Receiver<ChangeEvent>) {
        let id = SubscriptionId(
            self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );

        let (tx, rx) = tokio::sync::broadcast::channel(config.buffer_size);

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

        {
            let mut subs = self.subscriptions.write().unwrap();
            subs.insert(id, entry);
        }

        {
            let mut by_source = self.by_source.write().unwrap();
            by_source.entry(source_id).or_default().push(id);
        }

        {
            let mut by_name = self.by_name.write().unwrap();
            by_name.entry(source_name).or_default().push(id);
        }

        (id, rx)
    }

    /// Pauses a subscription.
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

    /// Cancels a subscription and cleans up.
    pub fn cancel(&self, id: SubscriptionId) -> bool {
        let entry = {
            let mut subs = self.subscriptions.write().unwrap();
            subs.remove(&id)
        };

        if let Some(entry) = entry {
            // Remove from source index
            {
                let mut by_source = self.by_source.write().unwrap();
                if let Some(ids) = by_source.get_mut(&entry.source_id) {
                    ids.retain(|&i| i != id);
                }
            }

            // Remove from name index
            {
                let mut by_name = self.by_name.write().unwrap();
                if let Some(ids) = by_name.get_mut(&entry.source_name) {
                    ids.retain(|&i| i != id);
                }
            }

            true
        } else {
            false
        }
    }

    /// Gets the broadcast senders for all active subscriptions of a source.
    ///
    /// This is called by the Ring 1 dispatcher on every notification.
    /// Uses a read lock for fast concurrent access.
    pub fn get_senders_for_source(
        &self,
        source_id: u32,
    ) -> Vec<tokio::sync::broadcast::Sender<ChangeEvent>> {
        let by_source = self.by_source.read().unwrap();
        let ids = match by_source.get(&source_id) {
            Some(ids) => ids,
            None => return Vec::new(),
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

    /// Returns the total number of subscriptions.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.read().unwrap().len()
    }

    /// Returns the number of active subscriptions.
    pub fn active_count(&self) -> usize {
        self.subscriptions.read().unwrap()
            .values()
            .filter(|e| e.state == SubscriptionState::Active)
            .count()
    }

    /// Returns subscription metrics for a specific subscription.
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
}

/// Subscription metrics snapshot.
#[derive(Debug, Clone)]
pub struct SubscriptionMetrics {
    pub id: SubscriptionId,
    pub source_name: String,
    pub state: SubscriptionState,
    pub events_delivered: u64,
    pub events_dropped: u64,
    pub current_lag: u64,
    pub age: std::time::Duration,
}
```

## Integration Points

| Component | File | Change |
|-----------|------|--------|
| MvRegistry | `laminar-core/src/mv/registry.rs` | Source ID allocation via NotificationHub |
| DagExecutor | `laminar-core/src/dag/executor.rs` | Register sources on DAG build |
| Admin API | `laminar-admin/src/api/` | Expose subscription CRUD endpoints |

### New Files

- `crates/laminar-core/src/subscription/registry.rs` - SubscriptionRegistry, SubscriptionEntry

## Test Plan

### Unit Tests

- [ ] `test_registry_create` - Create subscription, verify ID and receiver
- [ ] `test_registry_create_multiple` - Multiple subscriptions for same source
- [ ] `test_registry_pause_resume` - State transitions
- [ ] `test_registry_cancel` - Cleanup from all indices
- [ ] `test_registry_cancel_nonexistent` - Returns false
- [ ] `test_registry_get_senders` - Returns only active senders
- [ ] `test_registry_get_senders_paused_excluded` - Paused subs excluded
- [ ] `test_registry_get_senders_no_source` - Empty vec for unknown source
- [ ] `test_registry_subscription_count` - Total and active counts
- [ ] `test_registry_metrics` - Metrics snapshot
- [ ] `test_registry_config_default` - Default config values
- [ ] `test_registry_thread_safety` - Concurrent create/cancel

### Integration Tests

- [ ] `test_registry_with_notification_hub` - Source registration flow
- [ ] `test_registry_broadcast_delivery` - Events reach all subscribers

### Benchmarks

- [ ] `bench_registry_get_senders` - Target: < 500ns for 10 subs
- [ ] `bench_registry_create` - Target: < 5us (Ring 2, not latency-critical)

## Completion Checklist

- [ ] SubscriptionRegistry implemented with RwLock
- [ ] SubscriptionEntry with full lifecycle support
- [ ] SubscriptionConfig with backpressure strategy
- [ ] Source-to-subscription index (O(1) lookup by source_id)
- [ ] Thread-safe concurrent access
- [ ] SubscriptionMetrics for observability
- [ ] Unit tests passing (12+ tests)
- [ ] Integration with MvRegistry source IDs
- [ ] Documentation
- [ ] Code reviewed

## References

- [F060: Cascading Materialized Views](../../phase-2/F060-cascading-materialized-views.md)
- [Reactive Subscriptions Research](../../../research/reactive-subscriptions-research-2026.md)
- [Reactive Streams Specification](https://www.reactive-streams.org/)
