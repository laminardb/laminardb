# F-STREAM-002: SPSC Channel

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-002 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

Lock-free single-producer single-consumer (SPSC) channel built on the ring buffer primitive. The SPSC channel is the fast path for data transfer - used when only one producer exists. When `source.clone()` is called, the channel automatically upgrades to MPSC (F-STREAM-003).

## Goals

- Lock-free operation via sequence counters
- Acquire/Release memory ordering for minimal synchronization overhead
- Sub-50ns push/pop latency uncontended
- Foundation for automatic MPSC upgrade
- Configurable backpressure strategies

## Non-Goals

- Multi-producer support (handled by MPSC upgrade)
- Multi-consumer support (handled by Broadcast channel)
- Dynamic resizing
- Remote/distributed channels

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/spsc.rs`

The SPSC channel wraps a RingBuffer and provides separate Producer and Consumer handles with proper ownership semantics.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPSC Channel Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐                       ┌──────────────┐        │
│  │   Producer   │                       │   Consumer   │        │
│  │              │                       │              │        │
│  │ • push()     │                       │ • pop()      │        │
│  │ • try_push() │                       │ • try_pop()  │        │
│  │ • batch()    │                       │ • batch()    │        │
│  └──────┬───────┘                       └───────▲──────┘        │
│         │                                       │               │
│         │  Owns write access                    │ Owns read     │
│         ▼                                       │ access        │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Arc<ChannelInner>                      │  │
│  │  ┌─────────────────────────────────────────────────────┐ │  │
│  │  │                   RingBuffer<T>                      │ │  │
│  │  │  [Slot0][Slot1][Slot2] ... [SlotN-1]                 │ │  │
│  │  │  write_seq ─────────────────► read_seq               │ │  │
│  │  └─────────────────────────────────────────────────────┘ │  │
│  │  config: ChannelConfig                                    │  │
│  │  producer_count: AtomicUsize (for upgrade detection)      │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use crate::streaming::ring_buffer::{RingBuffer, TryPushError, TryPopError};
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Backpressure strategy when buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Backpressure {
    /// Block until space is available (default)
    #[default]
    Block,
    /// Drop the oldest unread item to make room
    DropOldest,
    /// Reject the push and return error
    Reject,
}

/// Wait strategy when buffer is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    /// Busy spin (lowest latency, highest CPU)
    Spin,
    /// Spin with yield after N iterations (default)
    SpinYield(u32),
    /// OS park/wake (lowest CPU, higher latency)
    Park,
}

impl Default for WaitStrategy {
    fn default() -> Self {
        WaitStrategy::SpinYield(100)
    }
}

/// Channel configuration.
#[derive(Debug, Clone)]
pub struct ChannelConfig {
    /// Buffer capacity (must be power of 2)
    pub capacity: usize,
    /// Backpressure strategy
    pub backpressure: Backpressure,
    /// Wait strategy for blocking operations
    pub wait_strategy: WaitStrategy,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            capacity: 65536,
            backpressure: Backpressure::Block,
            wait_strategy: WaitStrategy::SpinYield(100),
        }
    }
}

/// Shared channel state.
struct ChannelInner<T> {
    /// The underlying ring buffer
    buffer: RingBuffer<T>,
    /// Configuration
    config: ChannelConfig,
    /// Number of active producers (1 for SPSC, >1 after upgrade to MPSC)
    producer_count: AtomicUsize,
    /// Flag indicating channel is closed
    closed: AtomicBool,
    /// Flag indicating upgrade to MPSC has occurred
    upgraded: AtomicBool,
}

/// Producer handle for SPSC channel.
///
/// Only one Producer exists initially. Cloning triggers MPSC upgrade.
pub struct Producer<T> {
    inner: Arc<ChannelInner<T>>,
    /// Cached write sequence for fast path
    cached_write_seq: u64,
    /// Cached read sequence (refreshed periodically)
    cached_read_seq: u64,
}

/// Consumer handle for SPSC channel.
///
/// Only one Consumer can exist (not Clone).
pub struct Consumer<T> {
    inner: Arc<ChannelInner<T>>,
    /// Cached read sequence for fast path
    cached_read_seq: u64,
    /// Cached write sequence (refreshed periodically)
    cached_write_seq: u64,
}

// Producer is Send but not Sync (only one thread should use it at a time)
unsafe impl<T: Send> Send for Producer<T> {}

// Consumer is Send but not Sync
unsafe impl<T: Send> Send for Consumer<T> {}
```

### API/Interface

```rust
/// Create a new SPSC channel with the given capacity.
///
/// # Panics
///
/// Panics if capacity is not a power of 2.
///
/// # Example
///
/// ```rust
/// let (producer, consumer) = spsc_channel::<u64>(65536);
/// producer.push(42)?;
/// assert_eq!(consumer.pop()?, 42);
/// ```
pub fn spsc_channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    spsc_channel_with_config(ChannelConfig {
        capacity,
        ..Default::default()
    })
}

/// Create a new SPSC channel with custom configuration.
pub fn spsc_channel_with_config<T>(config: ChannelConfig) -> (Producer<T>, Consumer<T>) {
    let inner = Arc::new(ChannelInner {
        buffer: RingBuffer::new(config.capacity),
        config,
        producer_count: AtomicUsize::new(1),
        closed: AtomicBool::new(false),
        upgraded: AtomicBool::new(false),
    });

    let producer = Producer {
        inner: inner.clone(),
        cached_write_seq: 0,
        cached_read_seq: 0,
    };

    let consumer = Consumer {
        inner,
        cached_read_seq: 0,
        cached_write_seq: 0,
    };

    (producer, consumer)
}

impl<T> Producer<T> {
    /// Push an item to the channel, blocking if full (default).
    ///
    /// Behavior depends on configured backpressure strategy:
    /// - `Block`: Wait until space is available
    /// - `DropOldest`: Drop oldest item and push
    /// - `Reject`: Return error if full
    ///
    /// # Errors
    ///
    /// Returns error if channel is closed or backpressure rejects.
    pub fn push(&mut self, value: T) -> Result<(), ChannelError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ChannelError::Closed(value));
        }

        loop {
            // Fast path: check if we have capacity using cached read_seq
            let available = self.cached_read_seq + self.inner.buffer.capacity() as u64;
            if self.cached_write_seq < available {
                // SAFETY: We own exclusive write access in SPSC
                unsafe {
                    self.inner.buffer.write_slot(self.cached_write_seq, value);
                    self.cached_write_seq += 1;
                    self.inner.buffer.advance_write_seq(self.cached_write_seq);
                }
                return Ok(());
            }

            // Slow path: refresh cached read_seq
            self.cached_read_seq = self.inner.buffer.read_seq();

            // Still no space?
            if self.cached_write_seq >= self.cached_read_seq + self.inner.buffer.capacity() as u64 {
                match self.inner.config.backpressure {
                    Backpressure::Block => {
                        self.wait_for_space();
                    }
                    Backpressure::DropOldest => {
                        // Advance read pointer (drops oldest)
                        // SAFETY: Consumer may not have read this yet
                        unsafe {
                            self.inner.buffer.advance_read_seq(self.cached_read_seq + 1);
                        }
                        self.cached_read_seq += 1;
                    }
                    Backpressure::Reject => {
                        return Err(ChannelError::Full(value));
                    }
                }
            }
        }
    }

    /// Try to push an item without blocking.
    ///
    /// Returns the value if the channel is full.
    pub fn try_push(&mut self, value: T) -> Result<(), TryPushError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TryPushError::Full(value));
        }

        // Refresh read sequence
        self.cached_read_seq = self.inner.buffer.read_seq();

        let available = self.cached_read_seq + self.inner.buffer.capacity() as u64;
        if self.cached_write_seq >= available {
            return Err(TryPushError::Full(value));
        }

        // SAFETY: We own exclusive write access in SPSC
        unsafe {
            self.inner.buffer.write_slot(self.cached_write_seq, value);
            self.cached_write_seq += 1;
            self.inner.buffer.advance_write_seq(self.cached_write_seq);
        }

        Ok(())
    }

    /// Push a batch of items.
    ///
    /// More efficient than individual pushes due to reduced
    /// synchronization overhead.
    pub fn push_batch(&mut self, values: &mut Vec<T>) -> Result<usize, ChannelError<()>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ChannelError::Closed(()));
        }

        let mut pushed = 0;

        while !values.is_empty() {
            self.cached_read_seq = self.inner.buffer.read_seq();
            let available = (self.cached_read_seq + self.inner.buffer.capacity() as u64
                - self.cached_write_seq) as usize;

            if available == 0 {
                match self.inner.config.backpressure {
                    Backpressure::Block => {
                        self.wait_for_space();
                        continue;
                    }
                    Backpressure::Reject => break,
                    Backpressure::DropOldest => {
                        unsafe {
                            self.inner.buffer.advance_read_seq(self.cached_read_seq + 1);
                        }
                        self.cached_read_seq += 1;
                        continue;
                    }
                }
            }

            let batch_size = available.min(values.len());

            // Write batch
            for _ in 0..batch_size {
                let value = values.pop().unwrap();
                unsafe {
                    self.inner.buffer.write_slot(self.cached_write_seq, value);
                    self.cached_write_seq += 1;
                }
            }

            // Single sequence update for entire batch
            unsafe {
                self.inner.buffer.advance_write_seq(self.cached_write_seq);
            }

            pushed += batch_size;
        }

        Ok(pushed)
    }

    /// Check if there's space available for pushing.
    #[inline]
    pub fn has_capacity(&self) -> bool {
        !self.inner.buffer.is_full()
    }

    /// Close the channel.
    ///
    /// After closing, pushes will fail and consumer will drain remaining items.
    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
    }

    /// Returns true if the channel has been upgraded to MPSC.
    #[inline]
    pub fn is_upgraded(&self) -> bool {
        self.inner.upgraded.load(Ordering::Acquire)
    }

    fn wait_for_space(&self) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin => {
                std::hint::spin_loop();
            }
            WaitStrategy::SpinYield(iterations) => {
                for _ in 0..iterations {
                    std::hint::spin_loop();
                }
                std::thread::yield_now();
            }
            WaitStrategy::Park => {
                std::thread::park_timeout(Duration::from_micros(1));
            }
        }
    }
}

impl<T> Consumer<T> {
    /// Pop an item from the channel, blocking if empty.
    ///
    /// Returns `None` if the channel is closed and drained.
    pub fn pop(&mut self) -> Option<T> {
        loop {
            // Fast path: check if data available using cached write_seq
            if self.cached_read_seq < self.cached_write_seq {
                // SAFETY: We own exclusive read access
                let value = unsafe {
                    self.inner.buffer.read_slot(self.cached_read_seq)
                };
                self.cached_read_seq += 1;
                unsafe {
                    self.inner.buffer.advance_read_seq(self.cached_read_seq);
                }
                return Some(value);
            }

            // Slow path: refresh cached write_seq
            self.cached_write_seq = self.inner.buffer.write_seq();

            if self.cached_read_seq >= self.cached_write_seq {
                // Check if closed
                if self.inner.closed.load(Ordering::Acquire) {
                    // Double-check for any final items
                    self.cached_write_seq = self.inner.buffer.write_seq();
                    if self.cached_read_seq >= self.cached_write_seq {
                        return None;
                    }
                } else {
                    self.wait_for_data();
                }
            }
        }
    }

    /// Try to pop an item without blocking.
    pub fn try_pop(&mut self) -> Option<T> {
        self.cached_write_seq = self.inner.buffer.write_seq();

        if self.cached_read_seq >= self.cached_write_seq {
            return None;
        }

        // SAFETY: We own exclusive read access
        let value = unsafe {
            self.inner.buffer.read_slot(self.cached_read_seq)
        };
        self.cached_read_seq += 1;
        unsafe {
            self.inner.buffer.advance_read_seq(self.cached_read_seq);
        }

        Some(value)
    }

    /// Pop a batch of items.
    ///
    /// Appends up to `max` items to the output vector.
    /// Returns the number of items popped.
    pub fn pop_batch(&mut self, output: &mut Vec<T>, max: usize) -> usize {
        self.cached_write_seq = self.inner.buffer.write_seq();

        let available = (self.cached_write_seq - self.cached_read_seq) as usize;
        let batch_size = available.min(max);

        if batch_size == 0 {
            return 0;
        }

        output.reserve(batch_size);

        for _ in 0..batch_size {
            let value = unsafe {
                self.inner.buffer.read_slot(self.cached_read_seq)
            };
            self.cached_read_seq += 1;
            output.push(value);
        }

        unsafe {
            self.inner.buffer.advance_read_seq(self.cached_read_seq);
        }

        batch_size
    }

    /// Pop with timeout.
    ///
    /// Returns `None` if timeout expires without data.
    pub fn pop_timeout(&mut self, timeout: Duration) -> Option<T> {
        let start = std::time::Instant::now();

        loop {
            if let Some(value) = self.try_pop() {
                return Some(value);
            }

            if self.inner.closed.load(Ordering::Acquire) {
                return self.try_pop();
            }

            if start.elapsed() >= timeout {
                return None;
            }

            self.wait_for_data();
        }
    }

    /// Check if there's data available.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.buffer.is_empty()
    }

    /// Returns approximate number of items available.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.buffer.len()
    }

    fn wait_for_data(&self) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin => {
                std::hint::spin_loop();
            }
            WaitStrategy::SpinYield(iterations) => {
                for _ in 0..iterations {
                    std::hint::spin_loop();
                }
                std::thread::yield_now();
            }
            WaitStrategy::Park => {
                std::thread::park_timeout(Duration::from_micros(1));
            }
        }
    }
}

/// Make Consumer an iterator.
impl<T> Iterator for Consumer<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.pop()
    }
}

/// Channel errors.
#[derive(Debug)]
pub enum ChannelError<T> {
    /// Channel is closed
    Closed(T),
    /// Channel is full (with Reject backpressure)
    Full(T),
}
```

### Algorithm/Flow

#### Push Fast Path

```
Producer.push(value):
1. Check closed flag (Acquire)
2. Calculate available = cached_read_seq + capacity
3. If cached_write_seq < available:
   a. Write value to slot[cached_write_seq & mask]
   b. Increment cached_write_seq
   c. Store write_seq (Release)
   d. Return Ok
4. Else: Fall through to slow path
```

#### Push Slow Path

```
1. Refresh cached_read_seq from buffer (Acquire)
2. Recalculate available
3. If still full:
   - Block: wait_for_space() and retry
   - DropOldest: advance read_seq and retry
   - Reject: return error with value
4. Else: continue to fast path
```

#### Pop Fast Path

```
Consumer.pop():
1. If cached_read_seq < cached_write_seq:
   a. Read value from slot[cached_read_seq & mask]
   b. Increment cached_read_seq
   c. Store read_seq (Release)
   d. Return Some(value)
2. Else: Fall through to slow path
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ChannelError::Closed` | Channel closed by producer | Drain remaining items |
| `ChannelError::Full` | Buffer full with Reject backpressure | Caller handles |
| `TryPushError::Full` | Buffer full on try_push | Caller retries or drops |

## Test Plan

### Unit Tests

- [ ] `test_spsc_channel_creation`
- [ ] `test_push_pop_single_item`
- [ ] `test_push_pop_multiple_items`
- [ ] `test_try_push_when_full_returns_error`
- [ ] `test_try_pop_when_empty_returns_none`
- [ ] `test_push_batch_partial`
- [ ] `test_pop_batch_available`
- [ ] `test_close_drains_remaining`
- [ ] `test_backpressure_block`
- [ ] `test_backpressure_drop_oldest`
- [ ] `test_backpressure_reject`
- [ ] `test_wait_strategy_spin_yield`
- [ ] `test_consumer_iterator`
- [ ] `test_pop_timeout`

### Integration Tests

- [ ] Producer and consumer on separate threads
- [ ] High-throughput stress test (1M items)
- [ ] Mixed batch and single operations

### Property Tests

- [ ] `prop_no_data_loss` - All pushed items are popped
- [ ] `prop_fifo_order` - Items popped in push order
- [ ] `prop_closed_channel_drains` - Close allows drain

### Benchmarks

- [ ] `bench_push_pop_spsc` - Target: < 50ns
- [ ] `bench_batch_push_1000` - Target: < 10ns/item
- [ ] `bench_batch_pop_1000` - Target: < 10ns/item
- [ ] `bench_throughput_single_thread` - Target: > 20M ops/sec
- [ ] `bench_throughput_cross_thread` - Target: > 10M ops/sec

## Rollout Plan

1. **Phase 1**: Core SPSC implementation
2. **Phase 2**: Backpressure strategies
3. **Phase 3**: Wait strategies
4. **Phase 4**: Batch operations
5. **Phase 5**: Integration + benchmarks

## Open Questions

- [x] **Thread wakeup**: Use eventfd or condvar for Park strategy?
  - Decision: Simple park_timeout for now, eventfd in Phase 3
- [ ] **Credit-based flow control**: Add credit system like Flink?
  - Leaning: Defer, current backpressure sufficient for Phase 3

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/spsc.rs`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

---

## Notes

**Memory Ordering Strategy**:
- `Acquire` on sequence reads ensures we see prior writes
- `Release` on sequence writes ensures data is visible
- This is the minimum ordering required for correctness

**Cached Sequences**:
The producer and consumer each cache their counterpart's sequence number to reduce cross-core cache traffic. The cache is refreshed only on slow path.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-003: MPSC Auto-Upgrade](F-STREAM-003-mpsc-upgrade.md)
- [F014: SPSC Queue Communication](../../phase-2/F014-spsc-queues.md)
- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/)
