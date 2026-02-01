# LaminarDB Reactive Push-Based Subscriptions - Claude Code Steering Prompt

## Feature Overview

**Goal**: Transform LaminarDB's current poll-based subscription model into a reactive push-based system where data is automatically pushed to consumers when changes occur, eliminating the need for continuous polling.

**Target Latency**: Maintain sub-500ns for Ring 0 hot path operations while enabling efficient push notifications.

---

## Research Summary: 2025/2026 Best Practices

### 1. Push vs Pull Trade-offs in Streaming Databases

Modern streaming databases have converged on **hybrid push-pull models** rather than pure push or pure pull:

**Why Kafka chose Pull (and its limitations)**:
- Pull gives consumers control over consumption rate (backpressure handling)
- Enables aggressive batching without broker coordination
- Consumer catches up at its own pace
- BUT: Requires continuous polling loops, adds latency for real-time use cases

**Why Push is attractive for LaminarDB**:
- Sub-microsecond latency requirements favor immediate notification
- Embedded deployment means no network broker bottleneck
- Financial/real-time applications need instant data delivery
- Eliminates polling overhead and wasted CPU cycles

**2025 Industry Direction**: Companies like Uber and Wix have built push-based consumer proxies on top of Kafka to achieve lower latency. RisingWave explicitly uses a "push-based model where RisingWave pushes changes to subscribers."

### 2. Reactive Streams Specification (Foundation)

The Reactive Streams specification provides the theoretical foundation:

```
Publisher → Subscription ← Subscriber
         push data        request demand (backpressure)
```

Key principles:
- **Bounded buffers**: All buffer sizes must be known and controlled by subscribers
- **Backpressure**: Total onNext signals ≤ total elements requested
- **Non-blocking**: All methods return void for async implementation
- **Dynamic push-pull**: Hybrid model mixing async push with pull-based demand

### 3. Rust Implementation Patterns (Tokio Ecosystem)

**Channel Types for Push Notifications**:

| Channel | Use Case | LaminarDB Fit |
|---------|----------|---------------|
| `tokio::sync::broadcast` | Multi-producer, multi-consumer; all receivers see all values | ✅ Best for materialized view change notifications |
| `tokio::sync::watch` | Single value, only latest state matters | ✅ Good for "current state" subscriptions |
| `tokio::sync::mpsc` | Multi-producer, single-consumer with backpressure | ✅ Good for per-subscriber delivery queues |
| `crossfire` | Lock-free, tokio-agnostic, cancellation-safe | Consider for Ring 0 zero-lock requirement |

**Broadcast Channel Pattern** (recommended starting point):
```rust
use tokio::sync::broadcast;

// Database side: create channel per materialized view/query
let (tx, _) = broadcast::channel::<ChangeEvent>(1024);

// On data change in Ring 0:
let _ = tx.send(ChangeEvent { ... }); // Non-blocking, returns immediately

// Consumer side: subscribe and receive pushes
let mut rx = tx.subscribe();
while let Ok(event) = rx.recv().await {
    // Process pushed change
}
```

**Watch Channel Pattern** (for latest-value semantics):
```rust
use tokio::sync::watch;

let (tx, rx) = watch::channel(initial_state);

// On state change:
tx.send(new_state).unwrap();

// Consumer waits for changes:
while rx.changed().await.is_ok() {
    let current = rx.borrow();
    // Process current state
}
```

### 4. Zero-Copy Push Notifications (iceoryx2 Pattern)

For sub-500ns latency, consider iceoryx2's approach:

```rust
// Event-based push notification (zero-copy capable)
let notifier = event.notifier_builder().create()?;
notifier.notify_with_custom_event_id(EventId::new(subscription_id))?;

// Listener side:
let listener = event.listener_builder().create()?;
if let Ok(Some(event_id)) = listener.timed_wait_one(Duration::ZERO) {
    // Notification received, fetch data via zero-copy reference
}
```

Key insight: Separate the **notification** (lightweight signal) from the **data** (zero-copy reference). Don't push the actual data through the channel - push a notification, let consumer pull data via zero-copy mechanism.

### 5. io_uring Integration for Push Notifications

For Linux deployments, io_uring provides kernel-level push notification via eventfd:

```rust
// Register eventfd with io_uring for completion notifications
submitter.register_eventfd(eventfd)?;

// On data available, eventfd is signaled automatically
// Consumer can wait on eventfd instead of polling
```

This integrates naturally with LaminarDB's planned io_uring architecture.

### 6. Incremental View Maintenance (DBSP/Feldera Pattern)

For streaming SQL views, follow the DBSP model:

1. **Delta computation**: Only compute changes, not full results
2. **Push deltas**: Push incremental changes (inserts, deletes, updates) not full snapshots
3. **Subscriber maintains state**: Consumer applies deltas to local materialized view

```rust
enum ChangeEvent<T> {
    Insert(T),
    Delete(T),
    Update { old: T, new: T },
}

// Push delta, not full state
tx.send(ChangeEvent::Insert(new_row))?;
```

### 7. Observer Pattern Implementation

Classic observer with Rust safety:

```rust
pub trait Subscriber: Send + Sync {
    fn on_next(&self, data: &RecordBatch);
    fn on_error(&self, error: Error);
    fn on_complete(&self);
}

pub struct Subscription {
    id: SubscriptionId,
    subscriber: Arc<dyn Subscriber>,
    filter: Option<Filter>,
    // Backpressure: pending count
    pending: AtomicU64,
}

impl Subscription {
    fn request(&self, n: u64) {
        self.pending.fetch_add(n, Ordering::Relaxed);
    }
    
    fn can_push(&self) -> bool {
        self.pending.load(Ordering::Relaxed) > 0
    }
    
    fn push(&self, data: &RecordBatch) {
        if self.can_push() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            self.subscriber.on_next(data);
        }
        // Else: backpressure - consumer not ready
    }
}
```

---

## Recommended Architecture for LaminarDB

### Three-Tier Push Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Ring 0 (Hot Path)                        │
│  ┌─────────────┐     ┌──────────────────┐                       │
│  │ Data Change │ ──▶ │ Notification Hub │ ◀── Zero-allocation   │
│  │   Occurs    │     │ (eventfd/atomic) │     signal only       │
│  └─────────────┘     └────────┬─────────┘                       │
│                               │                                  │
│               ╔═══════════════╧═══════════════╗                 │
│               ║  Lock-free notification ring  ║                 │
│               ║  (per-core, SPSC)             ║                 │
│               ╚═══════════════╤═══════════════╝                 │
└───────────────────────────────┼─────────────────────────────────┘
                                │
┌───────────────────────────────┼─────────────────────────────────┐
│                         Ring 1 (Background)                      │
│               ┌───────────────▼───────────────┐                 │
│               │   Subscription Dispatcher     │                 │
│               │   - Route to subscribers      │                 │
│               │   - Handle backpressure       │                 │
│               │   - Batch notifications       │                 │
│               └───────────────┬───────────────┘                 │
│                               │                                  │
│         ┌─────────────────────┼─────────────────────────┐       │
│         ▼                     ▼                         ▼       │
│  ┌──────────────┐     ┌──────────────┐         ┌──────────────┐ │
│  │ broadcast    │     │ broadcast    │   ...   │ broadcast    │ │
│  │ channel (MV1)│     │ channel (MV2)│         │ channel (MVn)│ │
│  └──────┬───────┘     └──────┬───────┘         └──────┬───────┘ │
└─────────┼────────────────────┼───────────────────────┬┼─────────┘
          │                    │                       ││
┌─────────┼────────────────────┼───────────────────────┼┼─────────┐
│         │              Ring 2 (Consumer)             ││         │
│         ▼                    ▼                       ▼▼         │
│  ┌──────────────┐     ┌──────────────┐         ┌──────────────┐ │
│  │ Subscriber A │     │ Subscriber B │   ...   │ Subscriber N │ │
│  │ (callback)   │     │ (async recv) │         │ (Stream)     │ │
│  └──────────────┘     └──────────────┘         └──────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### API Design Options

**Option A: Callback-based (Lowest Latency)**
```rust
pub trait SubscriptionCallback: Send + Sync + 'static {
    fn on_change(&self, batch: &RecordBatch);
    fn on_error(&self, error: LaminarError);
    fn on_complete(&self);
}

impl Pipeline {
    pub fn subscribe_with_callback<F>(&self, query: &str, callback: F) -> Subscription
    where
        F: SubscriptionCallback;
}
```

**Option B: Channel-based (Most Flexible)**
```rust
impl Pipeline {
    pub fn subscribe(&self, query: &str) -> SubscriptionHandle {
        // Returns handle with receiver channel
    }
}

pub struct SubscriptionHandle {
    pub receiver: broadcast::Receiver<ChangeEvent>,
    pub cancel: oneshot::Sender<()>,
}

// Consumer usage:
let sub = pipeline.subscribe("SELECT * FROM trades WHERE symbol = 'AAPL'");
while let Ok(event) = sub.receiver.recv().await {
    process(event);
}
```

**Option C: Stream-based (Rust Idiomatic)**
```rust
impl Pipeline {
    pub fn subscribe_stream(&self, query: &str) -> impl Stream<Item = ChangeEvent> {
        // Returns async Stream
    }
}

// Consumer usage:
let mut stream = pipeline.subscribe_stream(query);
while let Some(event) = stream.next().await {
    process(event);
}
```

**Recommendation**: Implement all three, layered:
1. Core: Lock-free notification in Ring 0
2. Middle: Channel-based dispatch in Ring 1  
3. Surface: All three API styles (callback, channel, stream)

### Key Implementation Considerations

#### 1. Ring 0 Zero-Allocation Notification

```rust
// Per-core notification slot (cache-line aligned)
#[repr(align(64))]
struct NotificationSlot {
    sequence: AtomicU64,
    // Subscribers check sequence number, not actual data
}

impl NotificationSlot {
    #[inline(always)]
    fn notify(&self) {
        self.sequence.fetch_add(1, Ordering::Release);
    }
}

// Ring 1 polls notification slots, dispatches to channels
// Ring 0 never blocks, never allocates
```

#### 2. Backpressure Strategy

```rust
pub enum BackpressureStrategy {
    /// Drop oldest events when buffer full (real-time priority)
    DropOldest,
    /// Drop newest events when buffer full (completeness priority)  
    DropNewest,
    /// Block producer (not for Ring 0!)
    Block,
    /// Sample: deliver every Nth event
    Sample(usize),
}
```

For LaminarDB's financial use case, recommend `DropOldest` - stale data is worse than missing data.

#### 3. Subscription Filtering (Push Predicates Down)

```rust
pub struct Subscription {
    query: CompiledQuery,
    // Push predicate evaluation to Ring 0 to minimize notifications
    filter: Option<CompiledPredicate>,
}

// In Ring 0, before notification:
if subscription.filter.map_or(true, |f| f.evaluate(&row)) {
    notify(subscription.id);
}
```

#### 4. Batching for Throughput

```rust
pub struct BatchConfig {
    /// Max events per batch
    pub max_batch_size: usize,
    /// Max time to wait for batch to fill
    pub max_batch_delay: Duration,
}

// Ring 1 accumulates events, delivers batches
// Reduces per-event overhead for high-throughput scenarios
```

---

## Implementation Phases

### Phase 1: Core Notification Infrastructure
- [ ] Define `ChangeEvent` enum (Insert, Delete, Update, Watermark)
- [ ] Implement per-query broadcast channels in Ring 1
- [ ] Add lock-free notification mechanism in Ring 0
- [ ] Wire: data mutation → Ring 0 notify → Ring 1 dispatch

### Phase 2: Subscription API
- [ ] `subscribe()` returning channel receiver
- [ ] `subscribe_with_callback()` for callback style
- [ ] `subscribe_stream()` returning async Stream
- [ ] Subscription lifecycle (create, pause, resume, cancel)

### Phase 3: Backpressure & Reliability
- [ ] Implement backpressure strategies
- [ ] Add subscription buffer configuration
- [ ] Handle slow consumers (lagging detection)
- [ ] Subscription checkpointing for recovery

### Phase 4: Optimization
- [ ] Push predicate evaluation to Ring 0
- [ ] Implement batching in Ring 1
- [ ] NUMA-aware subscription routing
- [ ] Benchmark and tune buffer sizes

### Phase 5: Advanced Features
- [ ] Subscription sharing (multiple consumers, one computation)
- [ ] Subscription composition (combine multiple queries)
- [ ] Historical replay from checkpoint
- [ ] Exactly-once delivery semantics

---

## Testing Strategy

### Unit Tests
- Subscription creation/cancellation
- Event delivery ordering
- Backpressure behavior
- Filter predicate evaluation

### Integration Tests
- End-to-end: insert data → receive notification
- Multiple subscribers to same query
- Subscriber failure handling
- Checkpoint/recovery

### Benchmarks
- Notification latency (target: <1μs from Ring 0 to subscriber callback)
- Throughput (target: 10M+ events/sec with batching)
- Memory overhead per subscription
- CPU overhead vs polling baseline

---

## Code References

### Tokio Broadcast Channel
```rust
use tokio::sync::broadcast;

let (tx, mut rx1) = broadcast::channel(1024);
let mut rx2 = tx.subscribe();

// Send to all subscribers
tx.send(event)?;

// Receive (async)
let event = rx1.recv().await?;

// Handle lagging
match rx.recv().await {
    Ok(event) => process(event),
    Err(RecvError::Lagged(n)) => log!("Missed {} events", n),
    Err(RecvError::Closed) => break,
}
```

### Async Stream Pattern
```rust
use tokio_stream::{Stream, StreamExt};

fn subscribe_stream(tx: broadcast::Sender<Event>) -> impl Stream<Item = Event> {
    let rx = tx.subscribe();
    async_stream::stream! {
        let mut rx = rx;
        while let Ok(event) = rx.recv().await {
            yield event;
        }
    }
}
```

### Lock-Free SPSC Notification
```rust
use std::sync::atomic::{AtomicU64, Ordering};

struct NotificationRing {
    write_pos: AtomicU64,
    read_pos: AtomicU64,
    buffer: Box<[AtomicU64]>,
}

impl NotificationRing {
    fn notify(&self, subscription_id: u64) {
        let pos = self.write_pos.fetch_add(1, Ordering::AcqRel);
        self.buffer[pos as usize % self.buffer.len()]
            .store(subscription_id, Ordering::Release);
    }
    
    fn poll(&self) -> Option<u64> {
        let read = self.read_pos.load(Ordering::Acquire);
        let write = self.write_pos.load(Ordering::Acquire);
        if read < write {
            let id = self.buffer[read as usize % self.buffer.len()]
                .load(Ordering::Acquire);
            self.read_pos.store(read + 1, Ordering::Release);
            Some(id)
        } else {
            None
        }
    }
}
```

---

## Success Criteria

1. **Latency**: Notification reaches subscriber callback in <1μs (excluding subscriber processing time)
2. **Throughput**: Support 10M+ events/second with batching enabled
3. **Zero-allocation in Ring 0**: No heap allocations on hot path
4. **Backpressure**: Slow consumers don't block Ring 0
5. **API Ergonomics**: All three patterns (callback, channel, stream) available
6. **Reliability**: No lost events under normal operation (with appropriate backpressure config)

---

## Files to Create/Modify

### New Files
- `crates/laminar-core/src/subscription/mod.rs` - Module root
- `crates/laminar-core/src/subscription/event.rs` - ChangeEvent types
- `crates/laminar-core/src/subscription/channel.rs` - Channel management
- `crates/laminar-core/src/subscription/dispatcher.rs` - Ring 1 dispatcher
- `crates/laminar-core/src/subscription/notification.rs` - Ring 0 notification
- `crates/laminar-core/src/subscription/handle.rs` - Subscription handles/API
- `crates/laminar-core/src/subscription/backpressure.rs` - Backpressure strategies

### Modify
- `crates/laminar-core/src/pipeline.rs` - Add subscribe methods
- `crates/laminar-core/src/engine.rs` - Wire notifications on data change
- `Cargo.toml` - Add tokio-stream if not present

---

## Notes for Implementation

1. Start with Option B (channel-based) - it's the most testable and debuggable
2. Add callback wrapper on top for Option A
3. Add stream wrapper on top for Option C
4. Keep Ring 0 notification mechanism minimal - just sequence numbers
5. All complex logic (filtering, batching, delivery) lives in Ring 1
6. Use `#[inline(always)]` on Ring 0 notification path
7. Benchmark early and often - measure actual latency, not just throughput
