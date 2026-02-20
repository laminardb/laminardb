# F-DCKP-001: Checkpoint Barrier Protocol

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-STREAM-002 (SPSC Channel) |
| **Blocks** | F-DCKP-002 (Barrier Alignment) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/checkpoint/barrier.rs` |

## Summary

Implements the Chandy-Lamport distributed snapshot barrier protocol for LaminarDB's distributed checkpoint system. Checkpoint barriers are lightweight, zero-allocation markers injected into source streams that flow through the dataflow graph alongside regular events. When a barrier reaches an operator, it triggers a consistent snapshot of that operator's state. The barrier protocol is the foundational building block for all distributed checkpointing: every checkpoint begins with barrier injection and completes when all barriers have traversed the entire DAG.

**Note**: The existing Phase 3 codebase uses an out-of-band barrier model where `DagCheckpointCoordinator` orchestrates checkpoints separately from the event channel. This feature introduces the Flink-style in-band barrier model where barriers flow through SPSC channels alongside events via a new `StreamMessage<T>` wrapper enum. This is an architectural evolution — the existing `DagCheckpointCoordinator` continues to work for single-node DAGs, while `StreamMessage<T>` is required for multi-partition and delta checkpoint coordination. The two models coexist: embedded pipelines using `DagExecutor` keep the existing model, while partitioned pipelines use `StreamMessage<T>`.

## Goals

- Define `CheckpointBarrier` as a compact 24-byte struct (checkpoint_id + epoch + flags) with zero heap allocation
- Define `StreamMessage<T>` enum that unifies events, watermarks, and barriers in a single type flowing through SPSC channels
- Implement `CheckpointBarrierInjector` component that sits in source readers and injects barriers on timer or coordinator signal
- Barrier injection must not allocate or block the hot path
- Support both periodic (timer-driven) and coordinator-triggered barrier injection modes
- Barriers travel through Ring 0 channels identically to data events with no special routing

## Non-Goals

- Multi-input barrier alignment (covered by F-DCKP-002)
- Unaligned checkpoint barriers (covered by F-DCKP-006)
- Actual state snapshotting logic (covered by F-DCKP-004)
- Cross-node barrier propagation (covered by F-DCKP-008)
- Checkpoint storage or persistence (covered by F-DCKP-003, F-DCKP-004)

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) -- barriers are in-band messages on the same SPSC channels as data events.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/checkpoint/barrier.rs`

Barriers are injected at the source boundary (where external data enters the system) and flow downstream through the operator DAG. Each operator processes the barrier by snapshotting its state and forwarding the barrier to all downstream channels. The barrier protocol guarantees that no post-barrier events from any source are included in the snapshot, establishing a consistent cut across all operators.

```
                     Coordinator (Ring 1)
                        │ trigger_checkpoint(id, epoch)
                        ▼
┌──────────────────────────────────────────────────────────┐
│  Source Reader + BarrierInjector                          │
│  ┌─────────┐   ┌──────────────────┐   ┌──────────────┐  │
│  │  Source  │──>│ BarrierInjector  │──>│ SPSC Channel │──>│ Downstream
│  │  (Kafka) │   │ (injects barrier │   │ (Ring 0)     │  │  Operators
│  └─────────┘   │  between events) │   └──────────────┘  │
│                 └──────────────────┘                      │
└──────────────────────────────────────────────────────────┘
```

### API/Interface

```rust
use std::sync::atomic::{AtomicU128, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Unique identifier for a checkpoint, using UUID v7 for time-sortability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(transparent)]
pub struct CheckpointId(pub u128);

impl CheckpointId {
    /// Generate a new time-sortable checkpoint ID (UUID v7).
    pub fn new() -> Self {
        Self(uuid::Uuid::now_v7().as_u128())
    }

    /// Create from raw value (for deserialization / testing).
    pub const fn from_raw(raw: u128) -> Self {
        Self(raw)
    }
}

/// A checkpoint barrier marker that flows through the dataflow graph.
///
/// **Evolution from Phase 3**: The existing `CheckpointBarrier` in
/// `laminar_core::dag::checkpoint` has fields: `checkpoint_id`, `epoch`,
/// `timestamp`, `barrier_type`. This revised version drops `timestamp`
/// (not needed in-flight) and replaces `barrier_type: BarrierType` with
/// a packed `flags` field for extensibility. The existing barrier struct
/// is kept for `DagCheckpointCoordinator` compatibility; this new version
/// is used exclusively in `StreamMessage<T>` for partitioned pipelines.
///
/// This struct is exactly 24 bytes and requires zero heap allocation.
/// It is `Copy` so it can be freely duplicated when fanning out to
/// multiple downstream channels.
///
/// Layout: checkpoint_id (8 bytes) + epoch (8 bytes) + flags (8 bytes)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct CheckpointBarrier {
    /// Monotonically increasing checkpoint identifier.
    /// Uses a compact u64 sequence rather than the full UUID v7 for
    /// in-flight barriers; the UUID is used in the manifest on disk.
    pub checkpoint_id: u64,
    /// The epoch number for this checkpoint (monotonically increasing).
    pub epoch: u64,
    /// Packed flags field.
    /// Bit 0: is_unaligned (1 = unaligned checkpoint mode)
    /// Bits 1-63: reserved for future use
    flags: u64,
}

impl CheckpointBarrier {
    /// Create a new aligned checkpoint barrier.
    #[inline]
    pub const fn new(checkpoint_id: u64, epoch: u64) -> Self {
        Self {
            checkpoint_id,
            epoch,
            flags: 0,
        }
    }

    /// Create a new unaligned checkpoint barrier.
    #[inline]
    pub const fn new_unaligned(checkpoint_id: u64, epoch: u64) -> Self {
        Self {
            checkpoint_id,
            epoch,
            flags: 1,
        }
    }

    /// Returns true if this is an unaligned checkpoint barrier.
    #[inline]
    pub const fn is_unaligned(&self) -> bool {
        self.flags & 1 != 0
    }

    /// Returns true if this is an aligned checkpoint barrier.
    #[inline]
    pub const fn is_aligned(&self) -> bool {
        self.flags & 1 == 0
    }
}

/// A message that flows through the streaming dataflow graph.
///
/// **NEW in Delta**: The existing codebase does NOT have a
/// `StreamMessage` wrapper — events flow as bare `Event` structs
/// through `Producer<Event>`/`Consumer<Event>` channels, and barriers
/// are handled out-of-band by `DagCheckpointCoordinator`.
///
/// `StreamMessage<T>` introduces the Flink-style in-band barrier model
/// for partitioned pipelines. Channels carrying `StreamMessage<T>` send
/// barriers and watermarks inline with data events, enabling the
/// Chandy-Lamport distributed snapshot protocol.
///
/// For backward compatibility, existing `DagExecutor`-based pipelines
/// continue using bare `Event` channels. The `StreamMessage<T>` model
/// is used by the new partition-aware executor.
///
/// All inter-operator communication uses this enum. Barriers are
/// in-band with data events and watermarks, ensuring causal ordering.
#[derive(Debug, Clone)]
pub enum StreamMessage<T> {
    /// A batch of data events to be processed.
    Event(T),
    /// A watermark advancing the event-time frontier.
    Watermark(Watermark),
    /// A checkpoint barrier triggering a consistent snapshot.
    Barrier(CheckpointBarrier),
}

impl<T> StreamMessage<T> {
    /// Returns true if this message is a checkpoint barrier.
    #[inline]
    pub fn is_barrier(&self) -> bool {
        matches!(self, StreamMessage::Barrier(_))
    }

    /// Returns the barrier if this message is one, or None.
    #[inline]
    pub fn as_barrier(&self) -> Option<&CheckpointBarrier> {
        match self {
            StreamMessage::Barrier(b) => Some(b),
            _ => None,
        }
    }

    /// Map the event payload, preserving watermarks and barriers.
    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> StreamMessage<U> {
        match self {
            StreamMessage::Event(e) => StreamMessage::Event(f(e)),
            StreamMessage::Watermark(w) => StreamMessage::Watermark(w),
            StreamMessage::Barrier(b) => StreamMessage::Barrier(b),
        }
    }
}

/// Watermark indicating that no events with timestamp <= this value
/// will arrive on this stream.
// NOTE: Reuse the existing `laminar_core::time::Watermark` type.
// The existing `Watermark(pub i64)` tuple struct is preserved.
// Do NOT redefine — import from `crate::time::Watermark`.
//
// use crate::time::Watermark;
//
// The existing Watermark(i64) stores milliseconds since epoch
// and implements Debug, Clone, Copy, PartialEq, Eq, PartialOrd,
// Ord, and Hash.

/// Configuration for the barrier injector.
#[derive(Debug, Clone)]
pub struct BarrierInjectorConfig {
    /// Interval between periodic checkpoint barriers.
    /// Set to `None` to disable periodic injection (coordinator-only mode).
    pub periodic_interval: Option<Duration>,
    /// Source identifier for logging and metrics.
    pub source_id: String,
}

impl Default for BarrierInjectorConfig {
    fn default() -> Self {
        Self {
            periodic_interval: Some(Duration::from_secs(10)),
            source_id: String::new(),
        }
    }
}

/// Signal from the checkpoint coordinator to inject a barrier.
#[derive(Debug)]
pub struct InjectBarrierCommand {
    /// The checkpoint ID to inject.
    pub checkpoint_id: u64,
    /// The epoch for this checkpoint.
    pub epoch: u64,
    /// Whether to use unaligned mode.
    pub is_unaligned: bool,
}

/// **AUDIT FIX (C8)**: Packed barrier command for lock-free single-atomic transfer.
///
/// Replaces three separate atomics (`pending_checkpoint_id`, `pending_epoch`,
/// `pending_unaligned`) with a single `AtomicU128` to eliminate the race window
/// where `poll_barrier()` could read an inconsistent mix of old/new values
/// during rapid consecutive `inject()` calls.
///
/// Layout: `[checkpoint_id: u64 (bits 127-64)][epoch: u48 (bits 63-16)][unaligned: u1 (bit 15)][reserved: u15 (bits 14-0)]`
///
/// - `checkpoint_id == 0` means no pending barrier (EMPTY sentinel).
/// - Epoch is stored in 48 bits (supports up to 2^48 epochs, ~281 trillion).
/// - The entire struct is stored/loaded in a single atomic operation,
///   guaranteeing consistency between all three fields.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PackedBarrierCmd(u128);

impl PackedBarrierCmd {
    /// Sentinel value: no pending barrier.
    const EMPTY: Self = Self(0);

    /// Pack checkpoint_id, epoch, and unaligned flag into a single u128.
    fn new(checkpoint_id: u64, epoch: u64, unaligned: bool) -> Self {
        let packed = (checkpoint_id as u128) << 64
            | ((epoch & 0xFFFF_FFFF_FFFF) as u128) << 16
            | (unaligned as u128) << 15;
        Self(packed)
    }

    /// Extract checkpoint_id (upper 64 bits).
    fn checkpoint_id(self) -> u64 {
        (self.0 >> 64) as u64
    }

    /// Extract epoch (bits 63-16, 48-bit range).
    fn epoch(self) -> u64 {
        ((self.0 >> 16) & 0xFFFF_FFFF_FFFF) as u64
    }

    /// Extract unaligned flag (bit 15).
    fn is_unaligned(self) -> bool {
        (self.0 >> 15) & 1 != 0
    }

    /// Check if this is the empty sentinel (no pending barrier).
    fn is_empty(self) -> bool {
        self.0 == 0
    }
}
```

### Data Structures

```rust
/// Checkpoint barrier injector that sits between a source reader and
/// the downstream SPSC channel.
///
/// The injector intercepts the event stream and inserts barrier messages
/// at the appropriate points. It supports two trigger modes:
///
/// 1. **Periodic**: A timer fires at a configured interval, injecting
///    a barrier after the current event batch completes.
/// 2. **Coordinator**: An external signal (via `inject()`) requests
///    a barrier with a specific checkpoint ID.
///
/// The injector never allocates on the hot path. The `CheckpointBarrier`
/// struct is `Copy` and stack-allocated.
///
/// **AUDIT FIX (C8)**: Uses a single `AtomicU128` for coordinator-triggered
/// barriers instead of three separate atomics. This eliminates the race
/// window where rapid consecutive `inject()` calls could cause
/// `poll_barrier()` to read an inconsistent mix of checkpoint_id/epoch/flags.
pub struct CheckpointBarrierInjector {
    /// Configuration for this injector.
    config: BarrierInjectorConfig,

    /// Monotonically increasing checkpoint sequence for periodic mode.
    next_checkpoint_id: u64,

    /// Current epoch counter.
    current_epoch: u64,

    /// Timestamp of the last barrier injection (for periodic mode).
    last_injection: Instant,

    /// AUDIT FIX (C8): Single packed atomic for coordinator-triggered barriers.
    /// Replaces three separate atomics (`pending_checkpoint_id`, `pending_epoch`,
    /// `pending_unaligned`). A single atomic load/store guarantees consistency
    /// across all fields. `PackedBarrierCmd::EMPTY` (0) = no pending barrier.
    pending: Arc<AtomicU128>,

    /// Metrics: total barriers injected.
    barriers_injected: u64,
}

impl CheckpointBarrierInjector {
    /// Create a new barrier injector with the given configuration.
    pub fn new(config: BarrierInjectorConfig) -> Self {
        Self {
            config,
            next_checkpoint_id: 1,
            current_epoch: 0,
            last_injection: Instant::now(),
            pending: Arc::new(AtomicU128::new(PackedBarrierCmd::EMPTY.0)),
            barriers_injected: 0,
        }
    }

    /// Returns a handle that the coordinator can use to trigger barrier
    /// injection from another thread.
    pub fn coordinator_handle(&self) -> BarrierInjectorHandle {
        BarrierInjectorHandle {
            pending: self.pending.clone(),
        }
    }

    /// Check whether a barrier should be emitted now.
    ///
    /// Called on each iteration of the source read loop.
    /// Returns `Some(barrier)` if a barrier should be injected,
    /// `None` otherwise.
    ///
    /// This method is `#[inline]` and performs zero allocations.
    ///
    /// **AUDIT FIX (C8)**: Reads all barrier fields from a single
    /// `AtomicU128` load, guaranteeing that checkpoint_id, epoch, and
    /// unaligned flag are always consistent with each other.
    #[inline]
    pub fn poll_barrier(&mut self) -> Option<CheckpointBarrier> {
        // Fast path: check for coordinator-triggered barrier (single atomic load)
        let packed = PackedBarrierCmd(self.pending.load(Ordering::Acquire));
        if !packed.is_empty() {
            // Clear the pending signal (single atomic store)
            self.pending.store(PackedBarrierCmd::EMPTY.0, Ordering::Release);

            let checkpoint_id = packed.checkpoint_id();
            let epoch = packed.epoch();
            let unaligned = packed.is_unaligned();

            self.current_epoch = epoch;
            self.barriers_injected += 1;
            self.last_injection = Instant::now();

            return Some(if unaligned {
                CheckpointBarrier::new_unaligned(checkpoint_id, epoch)
            } else {
                CheckpointBarrier::new(checkpoint_id, epoch)
            });
        }

        // Slow path: check periodic timer
        if let Some(interval) = self.config.periodic_interval {
            if self.last_injection.elapsed() >= interval {
                let id = self.next_checkpoint_id;
                self.next_checkpoint_id += 1;
                self.current_epoch += 1;
                self.barriers_injected += 1;
                self.last_injection = Instant::now();

                return Some(CheckpointBarrier::new(id, self.current_epoch));
            }
        }

        None
    }

    /// Returns the total number of barriers injected.
    #[inline]
    pub fn barriers_injected(&self) -> u64 {
        self.barriers_injected
    }
}

/// Thread-safe handle for the coordinator to trigger barrier injection.
///
/// The coordinator sends this signal from Ring 1; the injector reads it
/// on Ring 0. Communication is lock-free via a single `AtomicU128`.
///
/// **AUDIT FIX (C8)**: Uses a single packed `AtomicU128` instead of three
/// separate atomics. A single `store()` atomically publishes all three
/// fields (checkpoint_id, epoch, unaligned), eliminating the race window
/// where `poll_barrier()` could observe an inconsistent mix of values
/// from two consecutive `inject()` calls.
#[derive(Clone)]
pub struct BarrierInjectorHandle {
    pending: Arc<AtomicU128>,
}

impl BarrierInjectorHandle {
    /// Signal the injector to emit a barrier with the given parameters.
    ///
    /// This is safe to call from any thread. If a previous signal has
    /// not been consumed yet, this overwrites it (latest-wins semantics).
    ///
    /// **AUDIT FIX (C8)**: Single atomic store guarantees that the injector
    /// sees a consistent (checkpoint_id, epoch, unaligned) tuple — no
    /// partial reads are possible.
    pub fn inject(&self, cmd: InjectBarrierCommand) {
        let packed = PackedBarrierCmd::new(
            cmd.checkpoint_id,
            cmd.epoch,
            cmd.is_unaligned,
        );
        self.pending.store(packed.0, Ordering::Release);
    }
}
```

### Algorithm/Flow

#### Barrier Injection Flow

```
Source Read Loop (Ring 0):

1. source.poll_next() → Option<EventBatch>
2. If event available:
   a. Emit StreamMessage::Event(batch) to downstream channel
   b. Call injector.poll_barrier()
      - Check pending_checkpoint_id (atomic Acquire load)
      - If non-zero: create barrier from pending values, clear pending, return Some
      - Else if periodic mode and timer elapsed: create barrier, return Some
      - Else: return None (fast path: single atomic load + branch)
   c. If barrier returned: emit StreamMessage::Barrier(barrier) to downstream
3. If no event: check for pending barrier anyway (quiescent injection)
4. Loop
```

#### Barrier Propagation Through Operators

```
Operator Processing Loop:

1. Receive StreamMessage from upstream channel
2. Match on message type:
   a. Event(batch):
      - Process batch through operator logic
      - Emit results downstream
   b. Watermark(wm):
      - Update operator watermark
      - Forward downstream
   c. Barrier(barrier):
      - Snapshot operator state (trigger async state save)
      - Forward barrier to ALL downstream channels
      - (For multi-input operators, delegate to BarrierAligner - F-DCKP-002)
```

**Compatibility Note**: The existing `Operator` trait has NO `on_barrier()` method.
Barrier handling in `StreamMessage<T>` pipelines is performed by the partition
executor (not the operator itself). The executor:
1. Receives `StreamMessage::Barrier(barrier)` from the channel
2. Calls `operator.checkpoint()` (existing method) to snapshot state
3. Forwards the barrier to all downstream channels
4. Emits `Output::CheckpointComplete` (existing variant) for coordination

This preserves the existing `Operator` trait without modification. Operators
are unaware of whether they run in the old `DagExecutor` or new partitioned
executor — they just implement `checkpoint()` and `restore()` as before.

#### Barrier Ordering Guarantees

1. Barriers are injected BETWEEN event batches, never mid-batch
2. All events emitted before the barrier are part of checkpoint N
3. All events emitted after the barrier are part of checkpoint N+1
4. Multiple downstream channels receive the barrier in sequence (fan-out)

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `BarrierInjectionFailed` | Downstream channel closed when attempting to emit barrier | Log error, mark source as failed, notify coordinator |
| `DuplicateBarrier` | Coordinator sends barrier with same checkpoint_id twice | Ignore duplicate (idempotent), log warning |
| `BarrierTimeout` | Barrier not acknowledged by downstream within timeout | Coordinator aborts checkpoint, starts new one |
| `EpochMismatch` | Barrier epoch does not match expected sequence | Log warning, accept barrier (coordinator is authoritative) |
| `ChannelFull` | SPSC channel full when injecting barrier | Apply backpressure (block), barrier has priority over events |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| `CheckpointBarrier` size | Exactly 24 bytes | `static_assert!(size_of::<CheckpointBarrier>() == 24)` |
| Barrier processing latency | < 50ns | `bench_barrier_processing` |
| `poll_barrier()` fast path (no barrier) | < 10ns | `bench_poll_barrier_no_pending` |
| `poll_barrier()` with pending barrier | < 30ns | `bench_poll_barrier_with_pending` |
| Barrier injection throughput | > 50M barriers/sec | `bench_barrier_injection_throughput` |
| Zero heap allocations on hot path | 0 allocs | `#[global_allocator]` counting test |
| `StreamMessage` enum size | <= 128 bytes | `static_assert!(size_of::<StreamMessage<EventBatch>>() <= 128)` |

## Existing Code Integration

### Types Reused from Existing Codebase

| Type | Existing Location | Usage in This Feature |
|------|------------------|-----------------------|
| `Watermark` | `laminar_core::time::Watermark(i64)` | Reused in `StreamMessage::Watermark` |
| `Event` | `laminar_core::operator::Event` | Generic parameter `T` in `StreamMessage<T>` |
| `Output::CheckpointComplete` | `laminar_core::operator::Output` | Coordinator notification after barrier |
| `OperatorState` | `laminar_core::operator::OperatorState` | Snapshot data from `operator.checkpoint()` |
| `HotPathGuard` | `laminar_core::alloc::HotPathGuard` | Zero-alloc verification with `allocation-tracking` |
| `AtomicU128` | `std::sync::atomic::AtomicU128` | AUDIT FIX (C8): packed barrier command for lock-free cross-thread signaling |

### Coexistence with DagCheckpointCoordinator

The existing `DagCheckpointCoordinator` and `DagExecutor` continue to work
for single-node DAG pipelines. `StreamMessage<T>` is used ONLY by the new
partition-aware executor introduced in the Delta Architecture:

- **Embedded mode**: `DagExecutor` + `DagCheckpointCoordinator` (unchanged)
- **Partitioned mode**: `StreamMessage<T>` channels + `CheckpointBarrierInjector`
- **Delta mode**: `StreamMessage<T>` + cross-node barrier forwarding (F-DCKP-008)

## Test Plan

### Unit Tests

- [ ] `test_checkpoint_barrier_new_creates_aligned` - Verify default barrier is aligned
- [ ] `test_checkpoint_barrier_new_unaligned_flag` - Verify unaligned flag is set
- [ ] `test_checkpoint_barrier_size_is_24_bytes` - Static size assertion
- [ ] `test_checkpoint_barrier_is_copy` - Verify Copy semantics
- [ ] `test_stream_message_is_barrier_returns_true` - Discriminant check
- [ ] `test_stream_message_as_barrier_returns_some` - Pattern matching
- [ ] `test_stream_message_map_preserves_barrier` - Map does not affect barriers
- [ ] `test_injector_periodic_fires_after_interval` - Timer-based injection
- [ ] `test_injector_periodic_does_not_fire_early` - Timer not yet elapsed
- [ ] `test_injector_coordinator_trigger_overrides_periodic` - Coordinator priority
- [ ] `test_injector_coordinator_handle_inject` - Cross-thread signal
- [ ] `test_injector_poll_barrier_fast_path_no_alloc` - Zero allocation verification
- [ ] `test_injector_increments_epoch_on_periodic` - Monotonic epoch counter
- [ ] `test_injector_barriers_injected_counter` - Metrics counter
- [ ] `test_injector_duplicate_coordinator_signal_latest_wins` - Overwrite semantics
- [ ] `test_packed_barrier_cmd_round_trip` - (C8) Pack and unpack checkpoint_id, epoch, unaligned — verify all fields match
- [ ] `test_packed_barrier_cmd_empty_sentinel` - (C8) EMPTY.is_empty() == true, non-zero is not empty
- [ ] `test_packed_barrier_cmd_epoch_48bit_max` - (C8) Epoch at 2^48 - 1 round-trips correctly
- [ ] `test_inject_poll_concurrent_consistency` - (C8) Spawn two threads: one calling inject() rapidly, one calling poll_barrier(). Every returned barrier must have consistent (checkpoint_id, epoch) pairs — never a mix from two different inject() calls

### Integration Tests

- [ ] `test_barrier_flows_through_single_operator` - End-to-end single-stage pipeline
- [ ] `test_barrier_flows_through_linear_chain` - Multi-stage linear pipeline
- [ ] `test_barrier_fan_out_to_multiple_channels` - Barrier duplicated on fan-out
- [ ] `test_barrier_interleaved_with_events` - Events before and after barrier
- [ ] `test_periodic_injection_under_load` - Barriers injected during sustained throughput
- [ ] `test_coordinator_triggered_injection` - External trigger via handle

### Benchmarks

- [ ] `bench_poll_barrier_no_pending` - Target: < 10ns (single atomic load)
- [ ] `bench_poll_barrier_with_pending` - Target: < 30ns
- [ ] `bench_barrier_processing` - Target: < 50ns (receive + forward)
- [ ] `bench_stream_message_push_pop` - Target: < 60ns (through SPSC channel)
- [ ] `bench_barrier_injection_throughput` - Target: > 50M/sec sustained

## Rollout Plan

1. **Phase 1**: `CheckpointBarrier` and `StreamMessage` types + unit tests
2. **Phase 2**: `CheckpointBarrierInjector` with periodic mode + tests
3. **Phase 3**: `BarrierInjectorHandle` for coordinator signaling + tests
4. **Phase 4**: Integration tests with SPSC channels and operators
5. **Phase 5**: Benchmarks + optimization (ensure < 50ns target)
6. **Phase 6**: Documentation and code review

## Open Questions

- [ ] Should `StreamMessage` carry a generic `EventBatch` type or be specialized for Arrow `RecordBatch`? Currently generic to support multiple batch formats.
- [ ] Should periodic injection be phase-locked across sources? Initial design is per-source timers; coordinator-triggered mode provides global synchronization.
- [ ] Should barriers carry a timestamp for debugging/observability? Currently omitted to maintain 24-byte size.
- [ ] Should `poll_barrier()` check the timer on every call or only every N events? Checking `Instant::now()` on every call may have measurable cost on some platforms.

## Completion Checklist

- [ ] `CheckpointBarrier` struct implemented (24 bytes, `Copy`)
- [ ] `StreamMessage<T>` enum implemented
- [ ] `CheckpointBarrierInjector` implemented with periodic and coordinator modes
- [ ] `BarrierInjectorHandle` for cross-thread coordinator signaling
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets (< 50ns barrier processing)
- [ ] Zero-allocation property verified
- [ ] Documentation updated (`#![deny(missing_docs)]` clean)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm) - Original distributed snapshot algorithm
- [Apache Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#checkpointing) - Production implementation this design follows
- [F-STREAM-002: SPSC Channel](../../phase-3/streaming/F-STREAM-002-spsc-channel.md) - Channel primitive carrying StreamMessage
- [F-DAG-004: DAG Checkpointing](../../phase-3/dag/F-DAG-004-dag-checkpointing.md) - Single-node checkpointing predecessor
- [F-DCKP-002: Barrier Alignment](F-DCKP-002-barrier-alignment.md) - Multi-input barrier handling
