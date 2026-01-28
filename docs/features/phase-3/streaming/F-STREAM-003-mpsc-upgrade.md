# F-STREAM-003: MPSC Auto-Upgrade

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-STREAM-003 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-001, F-STREAM-002 |
| **Owner** | TBD |
| **Created** | 2026-01-28 |
| **Updated** | 2026-01-28 |

## Summary

Automatic SPSC to MPSC channel upgrade when a Source is cloned. The user never specifies channel type - it's derived automatically. This maintains the zero-config philosophy while enabling multi-producer use cases transparently.

**Key Insight**: Start with the fastest path (SPSC), upgrade to MPSC only when needed (on first clone).

## Goals

- Automatic upgrade on `source.clone()`
- One-time upgrade cost (not per-push)
- Zero allocation during upgrade (reuse existing buffer)
- Preserve in-flight data during upgrade
- CAS-based multi-producer coordination post-upgrade
- Transparent to user code

## Non-Goals

- User-specified channel types (always automatic)
- Downgrade from MPSC back to SPSC
- MPMC (multi-consumer handled separately by Broadcast)
- Runtime channel type introspection in user code

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path)
**Crate**: `laminar-core`
**Module**: `laminar-core/src/streaming/channel.rs`

The upgrade mechanism is embedded in the Source's Clone implementation.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SPSC → MPSC Upgrade Flow                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Initial State (SPSC):                                          │
│  ┌─────────────────┐                                            │
│  │   Source<T>     │                                            │
│  │   (sole owner)  │                                            │
│  └────────┬────────┘                                            │
│           │                                                      │
│           ▼                                                      │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    ChannelInner                            │ │
│  │  producer_count: 1                                         │ │
│  │  channel_mode: ChannelMode::Spsc                           │ │
│  │  buffer: RingBuffer<T>                                     │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                  │
│  ═══════════════════════════════════════════════════════════   │
│                                                                  │
│  After source.clone():                                          │
│  ┌────────────┐      ┌────────────┐                             │
│  │  Source<T> │      │  Source<T> │  (clone)                    │
│  │ (original) │      │  (cloned)  │                             │
│  └─────┬──────┘      └──────┬─────┘                             │
│        │                    │                                    │
│        │  CAS coordination  │                                    │
│        └────────┬───────────┘                                    │
│                 ▼                                                │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                    ChannelInner                            │ │
│  │  producer_count: 2                                         │ │
│  │  channel_mode: ChannelMode::Mpsc                           │ │
│  │  buffer: RingBuffer<T> (same buffer, reused)               │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Data Structures

```rust
use std::sync::atomic::{AtomicUsize, AtomicU8, Ordering};
use std::sync::Arc;

/// Channel mode - starts as SPSC, upgrades to MPSC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChannelMode {
    /// Single-producer single-consumer (fast path)
    Spsc = 0,
    /// Multi-producer single-consumer (after clone)
    Mpsc = 1,
}

impl From<u8> for ChannelMode {
    fn from(v: u8) -> Self {
        match v {
            0 => ChannelMode::Spsc,
            _ => ChannelMode::Mpsc,
        }
    }
}

/// Shared channel state that supports SPSC → MPSC upgrade.
pub struct UpgradableChannel<T> {
    /// The underlying ring buffer
    buffer: RingBuffer<T>,

    /// Number of active producers
    /// - 1 = SPSC mode
    /// - >1 = MPSC mode
    producer_count: AtomicUsize,

    /// Current channel mode
    mode: AtomicU8,

    /// Configuration
    config: ChannelConfig,

    /// Closed flag
    closed: AtomicBool,

    /// MPSC: Per-producer claim counter for CAS coordination
    /// Only used after upgrade to MPSC
    claim_counter: AtomicU64,
}

/// Producer handle with automatic MPSC upgrade on clone.
pub struct Producer<T> {
    inner: Arc<UpgradableChannel<T>>,
    /// Cached write position (SPSC mode only)
    cached_write_seq: u64,
    /// Cached read position
    cached_read_seq: u64,
    /// Producer ID (assigned on clone)
    producer_id: usize,
}

// Clone triggers the upgrade
impl<T: Send> Clone for Producer<T> {
    fn clone(&self) -> Self {
        // Atomically increment producer count
        let prev_count = self.inner.producer_count.fetch_add(1, Ordering::SeqCst);

        // First clone triggers upgrade from SPSC to MPSC
        if prev_count == 1 {
            self.inner.upgrade_to_mpsc();
        }

        Self {
            inner: self.inner.clone(),
            cached_write_seq: self.inner.buffer.write_seq(),
            cached_read_seq: self.inner.buffer.read_seq(),
            producer_id: prev_count + 1,
        }
    }
}
```

### API/Interface

```rust
impl<T> UpgradableChannel<T> {
    /// Create a new channel starting in SPSC mode.
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            buffer: RingBuffer::new(config.capacity),
            producer_count: AtomicUsize::new(1),
            mode: AtomicU8::new(ChannelMode::Spsc as u8),
            config,
            closed: AtomicBool::new(false),
            claim_counter: AtomicU64::new(0),
        }
    }

    /// Get the current channel mode.
    #[inline]
    pub fn mode(&self) -> ChannelMode {
        ChannelMode::from(self.mode.load(Ordering::Acquire))
    }

    /// Check if channel has been upgraded to MPSC.
    #[inline]
    pub fn is_mpsc(&self) -> bool {
        self.mode() == ChannelMode::Mpsc
    }

    /// Get the number of active producers.
    #[inline]
    pub fn producer_count(&self) -> usize {
        self.producer_count.load(Ordering::Acquire)
    }

    /// Upgrade from SPSC to MPSC mode.
    ///
    /// This is called once on the first clone. It:
    /// 1. Ensures all in-flight writes are visible
    /// 2. Initializes MPSC coordination state
    /// 3. Atomically switches the mode flag
    ///
    /// # Safety
    ///
    /// This must only be called when producer_count transitions from 1 to 2.
    fn upgrade_to_mpsc(&self) {
        // Memory barrier to ensure all prior SPSC writes are visible
        std::sync::atomic::fence(Ordering::SeqCst);

        // Initialize claim counter to current write position
        let current_write = self.buffer.write_seq();
        self.claim_counter.store(current_write, Ordering::SeqCst);

        // Switch to MPSC mode
        // SeqCst ensures all producers see the mode change
        self.mode.store(ChannelMode::Mpsc as u8, Ordering::SeqCst);

        // Note: Existing data in buffer is preserved
        // The mode switch just changes how future pushes work
    }
}

impl<T> Producer<T> {
    /// Push an item to the channel.
    ///
    /// Automatically uses SPSC or MPSC path based on mode.
    pub fn push(&mut self, value: T) -> Result<(), ChannelError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(ChannelError::Closed(value));
        }

        match self.inner.mode() {
            ChannelMode::Spsc => self.push_spsc(value),
            ChannelMode::Mpsc => self.push_mpsc(value),
        }
    }

    /// SPSC push path (fast path, no CAS needed).
    fn push_spsc(&mut self, value: T) -> Result<(), ChannelError<T>> {
        loop {
            let available = self.cached_read_seq + self.inner.buffer.capacity() as u64;

            if self.cached_write_seq < available {
                // SAFETY: Single producer owns write access
                unsafe {
                    self.inner.buffer.write_slot(self.cached_write_seq, value);
                    self.cached_write_seq += 1;
                    self.inner.buffer.advance_write_seq(self.cached_write_seq);
                }
                return Ok(());
            }

            // Refresh read seq and check again
            self.cached_read_seq = self.inner.buffer.read_seq();

            if self.cached_write_seq >= self.cached_read_seq + self.inner.buffer.capacity() as u64 {
                self.handle_backpressure()?;
            }
        }
    }

    /// MPSC push path (CAS-based coordination).
    fn push_mpsc(&mut self, value: T) -> Result<(), ChannelError<T>> {
        loop {
            // Claim a slot using CAS on claim counter
            let claim_seq = self.inner.claim_counter.load(Ordering::Acquire);
            let read_seq = self.inner.buffer.read_seq();

            // Check if buffer is full
            if claim_seq >= read_seq + self.inner.buffer.capacity() as u64 {
                self.cached_read_seq = read_seq;
                match self.inner.config.backpressure {
                    Backpressure::Block => {
                        self.wait_for_space();
                        continue;
                    }
                    Backpressure::Reject => {
                        return Err(ChannelError::Full(value));
                    }
                    Backpressure::DropOldest => {
                        // In MPSC, DropOldest is tricky - skip for now
                        self.wait_for_space();
                        continue;
                    }
                }
            }

            // Try to claim the slot
            match self.inner.claim_counter.compare_exchange(
                claim_seq,
                claim_seq + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Slot claimed successfully, write the value
                    // SAFETY: We own this slot via successful CAS
                    unsafe {
                        self.inner.buffer.write_slot(claim_seq, value);
                    }

                    // Wait for our turn to advance write_seq
                    // This ensures in-order visibility
                    self.wait_for_write_seq(claim_seq);

                    // Advance write sequence
                    unsafe {
                        self.inner.buffer.advance_write_seq(claim_seq + 1);
                    }

                    return Ok(());
                }
                Err(_) => {
                    // Another producer claimed this slot, retry
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Wait until write_seq reaches our claimed position.
    /// This ensures writes are visible in order.
    fn wait_for_write_seq(&self, target: u64) {
        loop {
            let current = self.inner.buffer.write_seq();
            if current >= target {
                return;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to push without blocking.
    pub fn try_push(&mut self, value: T) -> Result<(), TryPushError<T>> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err(TryPushError::Full(value));
        }

        match self.inner.mode() {
            ChannelMode::Spsc => self.try_push_spsc(value),
            ChannelMode::Mpsc => self.try_push_mpsc(value),
        }
    }

    fn try_push_spsc(&mut self, value: T) -> Result<(), TryPushError<T>> {
        self.cached_read_seq = self.inner.buffer.read_seq();
        let available = self.cached_read_seq + self.inner.buffer.capacity() as u64;

        if self.cached_write_seq >= available {
            return Err(TryPushError::Full(value));
        }

        unsafe {
            self.inner.buffer.write_slot(self.cached_write_seq, value);
            self.cached_write_seq += 1;
            self.inner.buffer.advance_write_seq(self.cached_write_seq);
        }

        Ok(())
    }

    fn try_push_mpsc(&mut self, value: T) -> Result<(), TryPushError<T>> {
        let claim_seq = self.inner.claim_counter.load(Ordering::Acquire);
        let read_seq = self.inner.buffer.read_seq();

        if claim_seq >= read_seq + self.inner.buffer.capacity() as u64 {
            return Err(TryPushError::Full(value));
        }

        // Try to claim once
        match self.inner.claim_counter.compare_exchange(
            claim_seq,
            claim_seq + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                unsafe {
                    self.inner.buffer.write_slot(claim_seq, value);
                }
                self.wait_for_write_seq(claim_seq);
                unsafe {
                    self.inner.buffer.advance_write_seq(claim_seq + 1);
                }
                Ok(())
            }
            Err(_) => Err(TryPushError::Full(value)),
        }
    }

    fn handle_backpressure(&mut self) -> Result<(), ChannelError<()>> {
        match self.inner.config.backpressure {
            Backpressure::Block => {
                self.wait_for_space();
                Ok(())
            }
            Backpressure::Reject => {
                Err(ChannelError::Full(()))
            }
            Backpressure::DropOldest => {
                // In SPSC mode, we can advance read
                unsafe {
                    self.inner.buffer.advance_read_seq(self.cached_read_seq + 1);
                }
                self.cached_read_seq += 1;
                Ok(())
            }
        }
    }

    fn wait_for_space(&self) {
        match self.inner.config.wait_strategy {
            WaitStrategy::Spin => std::hint::spin_loop(),
            WaitStrategy::SpinYield(n) => {
                for _ in 0..n {
                    std::hint::spin_loop();
                }
                std::thread::yield_now();
            }
            WaitStrategy::Park => {
                std::thread::park_timeout(std::time::Duration::from_micros(1));
            }
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        let remaining = self.inner.producer_count.fetch_sub(1, Ordering::SeqCst);
        if remaining == 1 {
            // Last producer dropped, close the channel
            self.inner.closed.store(true, Ordering::Release);
        }
    }
}
```

### Algorithm/Flow

#### Upgrade Sequence

```
1. Producer.clone() called
2. fetch_add(producer_count, 1)
3. If previous count was 1:
   a. Memory barrier (SeqCst fence)
   b. Initialize claim_counter to current write_seq
   c. Set mode to MPSC (SeqCst store)
4. Return new Producer with incremented producer_id
```

#### MPSC Push Algorithm

```
1. Load claim_counter (Acquire)
2. Load read_seq to check capacity
3. If buffer full: backpressure handling
4. CAS claim_counter: old → old+1
   - Success: Slot claimed, proceed to step 5
   - Failure: Retry from step 1
5. Write value to claimed slot
6. Wait for write_seq to reach claimed position
   (ensures in-order visibility)
7. Advance write_seq (Release)
```

### Upgrade Timing Diagram

```
Time ──────────────────────────────────────────────────────────────►

Producer A:  push() push() push() clone()
                                    │
                                    ▼
             ┌──────────────────────────────────────────────────┐
             │           UPGRADE POINT                          │
             │  1. producer_count: 1 → 2                        │
             │  2. SeqCst fence                                 │
             │  3. claim_counter = write_seq                    │
             │  4. mode: SPSC → MPSC                            │
             └──────────────────────────────────────────────────┘
                                    │
                                    ▼
Producer A:  push() ──► (MPSC path, CAS-based)
Producer B:  push() ──► (MPSC path, CAS-based)
             │     │
             └──┬──┘
                │
             Concurrent CAS coordination
```

### Performance Characteristics

| Metric | SPSC | MPSC | Notes |
|--------|------|------|-------|
| Push latency | ~20-50ns | ~50-150ns | CAS contention in MPSC |
| Throughput/producer | ~50M/sec | ~10-20M/sec | Per producer |
| Memory overhead | 0 | 0 | Same buffer reused |
| Upgrade cost | N/A | ~200ns | One-time, on first clone |

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| CAS failure | Concurrent push | Automatic retry |
| Buffer full | Capacity exceeded | Backpressure policy |
| Channel closed | All producers dropped | Consumer drains remaining |

## Test Plan

### Unit Tests

- [ ] `test_single_producer_stays_spsc`
- [ ] `test_clone_triggers_upgrade`
- [ ] `test_upgrade_preserves_data`
- [ ] `test_second_clone_no_upgrade`
- [ ] `test_mpsc_push_works`
- [ ] `test_concurrent_producers`
- [ ] `test_producer_drop_decrements_count`
- [ ] `test_last_producer_drop_closes_channel`
- [ ] `test_mode_query_correct`

### Integration Tests

- [ ] Two producers pushing concurrently
- [ ] Three+ producers stress test
- [ ] Upgrade mid-stream (data in buffer)
- [ ] Mixed SPSC/MPSC sequence

### Property Tests

- [ ] `prop_upgrade_never_loses_data`
- [ ] `prop_all_pushes_eventually_visible`
- [ ] `prop_fifo_order_per_producer`

### Benchmarks

- [ ] `bench_spsc_push` - Target: < 50ns
- [ ] `bench_mpsc_push_2_producers` - Target: < 150ns
- [ ] `bench_mpsc_push_4_producers` - Target: < 200ns
- [ ] `bench_upgrade_latency` - Target: < 500ns

## Rollout Plan

1. **Phase 1**: Upgrade mechanism in UpgradableChannel
2. **Phase 2**: Clone implementation for Producer
3. **Phase 3**: MPSC push path with CAS
4. **Phase 4**: Integration tests
5. **Phase 5**: Benchmarks + optimization

## Open Questions

- [x] **MPSC ordering**: Strict FIFO or per-producer FIFO?
  - Decision: Strict FIFO via write_seq wait loop
- [ ] **Batch MPSC**: How to handle batch push in MPSC mode?
  - Leaning: Claim batch slots atomically, or serialize

## Completion Checklist

- [ ] Code implemented in `laminar-core/src/streaming/channel.rs`
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

**Zero-Config Philosophy**: The user never writes `Channel::Mpsc` or similar. The channel type is always derived:
- Start with SPSC (fastest)
- Upgrade to MPSC on `source.clone()`
- This is transparent to user code

**Upgrade is One-Way**: Once upgraded to MPSC, the channel stays MPSC even if producers are dropped. This avoids complex downgrade logic.

**CAS Contention**: Under high contention, MPSC push may retry multiple times. The wait-for-write-seq loop ensures strict ordering but adds latency. For very high-throughput multi-producer scenarios, consider producer-local batching.

## References

- [F-STREAM-001: Ring Buffer](F-STREAM-001-ring-buffer.md)
- [F-STREAM-002: SPSC Channel](F-STREAM-002-spsc-channel.md)
- [docs/research/laminardb-streaming-api-research.md](../../../research/laminardb-streaming-api-research.md)
- [LMAX Disruptor Multi-Producer](https://lmax-exchange.github.io/disruptor/disruptor.html#_multiple_producers)
