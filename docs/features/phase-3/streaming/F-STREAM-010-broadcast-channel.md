# F-STREAM-010: Broadcast Channel

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-010 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001, F-STREAM-005 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

Broadcast channel for multi-consumer scenarios. Automatically derived when multiple materialized views read from the same source - users never specify broadcast mode.

**Key Design Principle**: Broadcast is derived from query plan analysis, not user configuration.

## Goals

- Single producer, multiple consumers
- Independent read cursors per subscriber
- Slowest consumer determines retention
- Automatic derivation from topology

## Non-Goals

- User-specified broadcast mode (always automatic)
- Consumer-side filtering (use SQL WHERE)
- Acknowledgment semantics (separate feature)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/broadcast.rs`

```
┌─────────────────────────────────────────────────────────────────┐
│                   Broadcast Channel Derivation                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Query Plan Analysis:                                            │
│                                                                  │
│  CREATE SOURCE trades (...);                                     │
│                                                                  │
│  -- Single consumer → SPSC                                       │
│  CREATE MATERIALIZED VIEW ohlc AS SELECT ... FROM trades;        │
│                                                                  │
│  -- Adding second consumer → Upgrade to Broadcast                │
│  CREATE MATERIALIZED VIEW vwap AS SELECT ... FROM trades;        │
│                                                                  │
│                      trades (Source)                             │
│                           │                                      │
│                           ▼                                      │
│              ┌────────────────────────┐                         │
│              │    Broadcast Channel   │◄── Auto-derived         │
│              │                        │    when 2+ consumers     │
│              │  ┌──────────────────┐  │                         │
│              │  │   Ring Buffer    │  │                         │
│              │  │   (shared data)  │  │                         │
│              │  └──────────────────┘  │                         │
│              │                        │                         │
│              │  Cursor[0]: ohlc MV    │                         │
│              │  Cursor[1]: vwap MV    │                         │
│              └────────────────────────┘                         │
│                     │           │                                │
│                     ▼           ▼                                │
│                ┌────────┐  ┌────────┐                           │
│                │  ohlc  │  │  vwap  │                           │
│                │   MV   │  │   MV   │                           │
│                └────────┘  └────────┘                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// Broadcast channel for multi-consumer scenarios.
///
/// # Derivation
///
/// Users never specify broadcast mode. It's derived automatically:
/// - 1 consumer from source → SPSC
/// - 2+ consumers from source → Broadcast
///
/// This derivation happens at query plan optimization time.
pub struct BroadcastChannel<T> {
    /// Shared ring buffer (single copy of data)
    buffer: RingBuffer<T>,

    /// Write sequence (single producer)
    write_seq: CachePadded<AtomicU64>,

    /// Per-subscriber read cursors
    cursors: RwLock<Vec<SubscriberCursor>>,

    /// Maximum subscribers allowed
    max_subscribers: usize,

    /// Configuration
    config: BroadcastConfig,
}

/// Per-subscriber cursor state.
struct SubscriberCursor {
    /// Unique subscriber ID
    id: usize,

    /// Read position
    read_seq: AtomicU64,

    /// Whether this cursor is active
    active: AtomicBool,

    /// Subscriber name (for debugging)
    name: String,
}

/// Broadcast configuration.
#[derive(Debug, Clone)]
pub struct BroadcastConfig {
    /// Buffer capacity (power of 2)
    pub capacity: usize,

    /// Maximum allowed subscribers
    pub max_subscribers: usize,

    /// Policy when slowest subscriber is too far behind
    pub slow_subscriber_policy: SlowSubscriberPolicy,
}

/// Policy for handling slow subscribers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowSubscriberPolicy {
    /// Block producer until slow subscriber catches up (default)
    Block,
    /// Drop slow subscriber and continue
    DropSlow,
    /// Skip messages for slow subscriber
    SkipForSlow,
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self {
            capacity: 65536,
            max_subscribers: 16,
            slow_subscriber_policy: SlowSubscriberPolicy::Block,
        }
    }
}
```

### API/Interface

```rust
impl<T: Clone> BroadcastChannel<T> {
    /// Create a new broadcast channel.
    pub fn new(config: BroadcastConfig) -> Self {
        Self {
            buffer: RingBuffer::new(config.capacity),
            write_seq: CachePadded::new(AtomicU64::new(0)),
            cursors: RwLock::new(Vec::new()),
            max_subscribers: config.max_subscribers,
            config,
        }
    }

    /// Broadcast a value to all subscribers.
    ///
    /// The value is written once to the ring buffer.
    /// Each subscriber reads from their own cursor position.
    ///
    /// # Blocking Behavior
    ///
    /// With `SlowSubscriberPolicy::Block`, this will block if
    /// the slowest subscriber hasn't read values that would be
    /// overwritten.
    pub fn broadcast(&self, value: T) -> Result<(), BroadcastError> {
        let write = self.write_seq.get().load(Ordering::Acquire);

        // Check if any subscriber is too slow
        let min_cursor = self.slowest_cursor();

        if write.saturating_sub(min_cursor) >= self.buffer.capacity() as u64 {
            match self.config.slow_subscriber_policy {
                SlowSubscriberPolicy::Block => {
                    // Wait for slowest subscriber
                    self.wait_for_slowest(write)?;
                }
                SlowSubscriberPolicy::DropSlow => {
                    // Find and drop the slowest subscriber
                    self.drop_slowest_subscriber()?;
                }
                SlowSubscriberPolicy::SkipForSlow => {
                    // Just overwrite - slow subscribers lose data
                }
            }
        }

        // Write value to buffer
        unsafe {
            self.buffer.write_slot(write, value);
        }

        // Advance write sequence (makes visible to all subscribers)
        self.write_seq.get().store(write + 1, Ordering::Release);

        Ok(())
    }

    /// Register a new subscriber.
    ///
    /// Returns a subscriber ID for reading.
    pub fn subscribe(&self, name: String) -> Result<usize, BroadcastError> {
        let mut cursors = self.cursors.write().unwrap();

        if cursors.len() >= self.max_subscribers {
            return Err(BroadcastError::MaxSubscribersReached);
        }

        let id = cursors.len();
        let current_write = self.write_seq.get().load(Ordering::Acquire);

        cursors.push(SubscriberCursor {
            id,
            read_seq: AtomicU64::new(current_write),
            active: AtomicBool::new(true),
            name,
        });

        Ok(id)
    }

    /// Unsubscribe a subscriber.
    pub fn unsubscribe(&self, id: usize) {
        let cursors = self.cursors.read().unwrap();
        if id < cursors.len() {
            cursors[id].active.store(false, Ordering::Release);
        }
    }

    /// Read the next value for a subscriber.
    ///
    /// Returns `None` if no new data is available.
    pub fn read(&self, subscriber_id: usize) -> Option<T> {
        let cursors = self.cursors.read().unwrap();
        let cursor = cursors.get(subscriber_id)?;

        if !cursor.active.load(Ordering::Acquire) {
            return None;
        }

        let read = cursor.read_seq.load(Ordering::Acquire);
        let write = self.write_seq.get().load(Ordering::Acquire);

        if read >= write {
            return None;
        }

        // Clone value from buffer (broadcast semantics)
        let value = unsafe {
            self.buffer.read_slot_cloned(read)
        };

        // Advance cursor
        cursor.read_seq.store(read + 1, Ordering::Release);

        Some(value)
    }

    /// Read without cloning (for types that don't need it).
    ///
    /// # Safety
    ///
    /// Caller must ensure the value is not accessed after
    /// the cursor advances past the buffer capacity.
    pub unsafe fn read_ref(&self, subscriber_id: usize) -> Option<&T> {
        let cursors = self.cursors.read().unwrap();
        let cursor = cursors.get(subscriber_id)?;

        let read = cursor.read_seq.load(Ordering::Acquire);
        let write = self.write_seq.get().load(Ordering::Acquire);

        if read >= write {
            return None;
        }

        let value_ptr = self.buffer.slot_ptr(read);
        cursor.read_seq.store(read + 1, Ordering::Release);

        Some(&*value_ptr)
    }

    /// Get the slowest subscriber's cursor position.
    fn slowest_cursor(&self) -> u64 {
        self.cursors.read().unwrap()
            .iter()
            .filter(|c| c.active.load(Ordering::Acquire))
            .map(|c| c.read_seq.load(Ordering::Acquire))
            .min()
            .unwrap_or(0)
    }

    /// Get subscriber lag (write_seq - read_seq).
    pub fn subscriber_lag(&self, subscriber_id: usize) -> u64 {
        let cursors = self.cursors.read().unwrap();
        let cursor = match cursors.get(subscriber_id) {
            Some(c) => c,
            None => return 0,
        };

        let write = self.write_seq.get().load(Ordering::Acquire);
        let read = cursor.read_seq.load(Ordering::Acquire);

        write.saturating_sub(read)
    }

    /// Get number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.cursors.read().unwrap()
            .iter()
            .filter(|c| c.active.load(Ordering::Acquire))
            .count()
    }

    fn wait_for_slowest(&self, target_write: u64) -> Result<(), BroadcastError> {
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        loop {
            let min = self.slowest_cursor();
            if target_write.saturating_sub(min) < self.buffer.capacity() as u64 {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(BroadcastError::SlowSubscriberTimeout);
            }

            std::thread::yield_now();
        }
    }

    fn drop_slowest_subscriber(&self) -> Result<(), BroadcastError> {
        let cursors = self.cursors.read().unwrap();

        let slowest = cursors.iter()
            .filter(|c| c.active.load(Ordering::Acquire))
            .min_by_key(|c| c.read_seq.load(Ordering::Acquire));

        if let Some(slow) = slowest {
            slow.active.store(false, Ordering::Release);
            Ok(())
        } else {
            Err(BroadcastError::NoSubscribers)
        }
    }
}

/// Broadcast errors.
#[derive(Debug)]
pub enum BroadcastError {
    /// Maximum subscribers reached
    MaxSubscribersReached,
    /// Slow subscriber timeout
    SlowSubscriberTimeout,
    /// No subscribers
    NoSubscribers,
}
```

### Derivation Logic

```rust
/// Query plan analysis to determine channel type.
impl QueryPlanner {
    /// Analyze query plan and derive channel types.
    pub fn derive_channel_types(&self, plan: &LogicalPlan) -> HashMap<String, ChannelType> {
        let mut channel_types = HashMap::new();
        let consumer_counts = self.count_consumers_per_source(plan);

        for (source_name, count) in consumer_counts {
            let channel_type = if count <= 1 {
                ChannelType::Spsc
            } else {
                ChannelType::Broadcast
            };
            channel_types.insert(source_name, channel_type);
        }

        channel_types
    }

    /// Count how many MVs read from each source.
    fn count_consumers_per_source(&self, plan: &LogicalPlan) -> HashMap<String, usize> {
        let mut counts = HashMap::new();

        // Walk the plan and count source references
        self.visit_plan(plan, &mut |node| {
            if let LogicalPlan::TableScan { table_name, .. } = node {
                *counts.entry(table_name.clone()).or_insert(0) += 1;
            }
        });

        counts
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `MaxSubscribersReached` | Too many MVs from source | Increase limit or consolidate |
| `SlowSubscriberTimeout` | Subscriber too slow | Check subscriber, increase buffer |
| `NoSubscribers` | All subscribers dropped | Normal shutdown |

## Test Plan

### Unit Tests

- [ ] `test_broadcast_single_subscriber`
- [ ] `test_broadcast_multiple_subscribers`
- [ ] `test_slowest_cursor_calculation`
- [ ] `test_subscriber_lag`
- [ ] `test_slow_subscriber_block_policy`
- [ ] `test_slow_subscriber_drop_policy`
- [ ] `test_unsubscribe`
- [ ] `test_max_subscribers`

### Integration Tests

- [ ] Two MVs from same source (auto-broadcast)
- [ ] Add MV triggers upgrade from SPSC to Broadcast
- [ ] Slow subscriber handling

### Benchmarks

- [ ] `bench_broadcast_2_subscribers` - Target: < 100ns
- [ ] `bench_broadcast_4_subscribers` - Target: < 150ns
- [ ] `bench_broadcast_throughput` - Target: > 5M ops/sec

## Completion Checklist

- [ ] Code implemented
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] Merged to main

---

## Notes

**Zero-Config**: Users never write `channel = 'broadcast'`. The query planner analyzes how many MVs read from each source and automatically upgrades to broadcast when needed.

**Clone Semantics**: In broadcast mode, values are cloned for each subscriber. For RecordBatch (Arc-based), this is cheap (Arc clone).

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-005: Sink](F-STREAM-005-sink.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
