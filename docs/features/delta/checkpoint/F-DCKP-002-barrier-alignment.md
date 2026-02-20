# F-DCKP-002: Barrier Alignment

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-002 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6a |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DCKP-001 (Checkpoint Barrier Protocol) |
| **Blocks** | F-DCKP-006 (Unaligned Checkpoints) |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/checkpoint/alignment.rs` |

## Summary

Implements barrier alignment for multi-input operators (joins, unions, merge nodes) in the Chandy-Lamport distributed snapshot protocol. When a checkpoint barrier arrives on one input channel, that channel must be paused (buffered) until matching barriers arrive on all other input channels. This ensures a consistent snapshot: no post-barrier events from one input can mix with pre-barrier events from another input. The `BarrierAligner` component sits inside every multi-input operator and manages the alignment state machine with bounded buffering and configurable memory limits.

**Evolution from Phase 3**: The existing codebase already has a `BarrierAligner` at `laminar_core::dag::checkpoint` that buffers events at fan-in (MPSC) nodes until all upstream inputs have delivered their barrier. It tracks `expected_inputs`, `barriers_received` (keyed by source node), and `current_checkpoint_id`. This feature EXTENDS the existing aligner with `StreamMessage<T>` awareness for partitioned pipelines: the existing `BarrierAligner` operates on `DagCheckpointCoordinator` barriers (out-of-band), while this version handles in-band `StreamMessage::Barrier` from SPSC channels. The core alignment algorithm is identical; the difference is the message framing.

## Goals

- Implement `BarrierAligner` that manages barrier alignment across N input channels
- Buffer events from already-aligned channels while waiting for remaining channels
- Provide bounded memory for alignment buffers with configurable limits
- Trigger operator snapshot callback exactly once when all inputs have aligned
- Forward the barrier downstream after snapshot completes
- Unblock all input channels and drain buffered events after alignment
- Support both 2-input (joins) and N-input (unions, merge) operators
- Zero heap allocation during the alignment fast path (single-input operators)

## Non-Goals

- Unaligned checkpoint mode (covered by F-DCKP-006, which builds on this component)
- Actual state snapshot logic (the aligner calls a callback, not the snapshot itself)
- Cross-node barrier coordination (covered by F-DCKP-008)
- Barrier injection (covered by F-DCKP-001)
- Dynamic addition/removal of input channels during alignment

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) -- alignment is on the critical data processing path.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/checkpoint/alignment.rs`

The `BarrierAligner` is embedded inside each multi-input operator. It wraps the operator's input processing loop and intercepts `StreamMessage::Barrier` messages. Single-input operators bypass the aligner entirely (forwarding the barrier directly).

```
Input 0 ──────┐
              │
Input 1 ──────┤    ┌──────────────────────────────────────┐
              ├───>│          BarrierAligner                │
Input 2 ──────┤    │                                        │
              │    │  State machine per checkpoint_id:      │
Input N ──────┘    │  - Track which inputs have barriers    │
                   │  - Buffer events from aligned inputs   │
                   │  - When all aligned: trigger snapshot   │
                   │  - Drain buffers, forward barrier       │
                   └──────────────┬─────────────────────────┘
                                  │
                                  ▼
                           Operator Logic ──> Downstream
```

### API/Interface

```rust
use crate::checkpoint::barrier::{CheckpointBarrier, StreamMessage};
use std::collections::VecDeque;

/// Result of processing a message through the barrier aligner.
///
/// > **AUDIT FIX (C4):** Added `WatermarkPassThrough` variant. The original
/// > design used `unreachable!()` for watermarks, which would panic Ring 0
/// > if any watermark reached the aligner. The new variant makes watermark
/// > handling type-safe and explicit.
#[derive(Debug)]
pub enum AlignmentAction<T> {
    /// Pass the event through to the operator for normal processing.
    PassThrough(T),

    /// A watermark passes through regardless of alignment state.
    /// Watermarks don't carry event data and don't affect snapshot
    /// consistency. The operator handles watermark advancement separately.
    WatermarkPassThrough(Watermark),

    /// The event has been buffered because this input is blocked
    /// waiting for other inputs to deliver their barriers.
    Buffered,

    /// All inputs have delivered their barriers. The operator should
    /// snapshot its state now, then call `complete_alignment()`.
    TriggerSnapshot {
        /// The barrier that triggered this snapshot.
        barrier: CheckpointBarrier,
    },

    /// A previously buffered event is being drained after alignment.
    /// Process it through the operator as normal.
    DrainBuffered(T),
}

/// Callback trait for operators that participate in barrier alignment.
pub trait BarrierAlignmentCallback {
    /// Called when all input barriers have aligned.
    /// The operator must snapshot its state synchronously or initiate
    /// an async snapshot and return.
    fn on_alignment_complete(&mut self, barrier: &CheckpointBarrier);
}

/// Configuration for the barrier aligner.
#[derive(Debug, Clone)]
pub struct BarrierAlignerConfig {
    /// Number of input channels this operator has.
    pub num_inputs: usize,

    /// Maximum number of events to buffer per input channel during
    /// alignment. If exceeded, the aligner will apply backpressure
    /// to the source (stop reading from the channel).
    pub max_buffer_per_input: usize,

    /// Maximum total memory (in bytes) across all alignment buffers.
    /// When exceeded, the checkpoint is aborted or switched to
    /// unaligned mode (if F-DCKP-006 is enabled).
    pub max_buffer_memory_bytes: usize,

    /// Timeout for alignment. If not all barriers arrive within this
    /// duration, the checkpoint is aborted.
    pub alignment_timeout: std::time::Duration,
}

impl Default for BarrierAlignerConfig {
    fn default() -> Self {
        Self {
            num_inputs: 2,
            max_buffer_per_input: 100_000,
            max_buffer_memory_bytes: 256 * 1024 * 1024, // 256 MiB
            alignment_timeout: std::time::Duration::from_secs(60),
        }
    }
}
```

### Data Structures

```rust
use std::time::Instant;

/// Tracks the alignment state for a single in-progress checkpoint.
#[derive(Debug)]
struct AlignmentState<T> {
    /// The checkpoint barrier we are aligning for.
    barrier: CheckpointBarrier,

    /// Bitset tracking which inputs have delivered their barrier.
    /// Bit i is set when input i has delivered its barrier.
    ///
    /// AUDIT FIX (H4): Expanded from u64 to u128 to support up to 128
    /// input channels (previously limited to 64).
    inputs_aligned: u128,

    /// Total number of inputs expected.
    num_inputs: usize,

    /// Buffered events from inputs that have already delivered their
    /// barrier. Indexed by input channel ID.
    buffers: Vec<VecDeque<T>>,

    /// Total buffered event count across all inputs.
    total_buffered: usize,

    /// Approximate total memory consumed by buffers (bytes).
    buffered_bytes: usize,

    /// Timestamp when the first barrier for this checkpoint arrived.
    started_at: Instant,
}

impl<T> AlignmentState<T> {
    fn new(barrier: CheckpointBarrier, num_inputs: usize) -> Self {
        let buffers = (0..num_inputs)
            .map(|_| VecDeque::with_capacity(1024))
            .collect();

        Self {
            barrier,
            inputs_aligned: 0,
            num_inputs,
            buffers,
            total_buffered: 0,
            buffered_bytes: 0,
            started_at: Instant::now(),
        }
    }

    /// Mark input `input_id` as having delivered its barrier.
    #[inline]
    fn mark_aligned(&mut self, input_id: usize) {
        self.inputs_aligned |= 1u128 << input_id;
    }

    /// Returns true if input `input_id` has already delivered its barrier.
    #[inline]
    fn is_input_aligned(&self, input_id: usize) -> bool {
        self.inputs_aligned & (1u128 << input_id) != 0
    }

    /// Returns true if all inputs have delivered their barriers.
    ///
    /// AUDIT FIX (H4): Uses u128 mask to support up to 128 inputs.
    #[inline]
    fn is_fully_aligned(&self) -> bool {
        let expected_mask = if self.num_inputs == 128 {
            u128::MAX
        } else {
            (1u128 << self.num_inputs) - 1
        };
        self.inputs_aligned == expected_mask
    }

    /// Number of inputs that have delivered their barrier so far.
    #[inline]
    fn aligned_count(&self) -> usize {
        self.inputs_aligned.count_ones() as usize
    }

    /// Buffer an event from an already-aligned input.
    fn buffer_event(&mut self, input_id: usize, event: T, event_size_bytes: usize) {
        self.buffers[input_id].push_back(event);
        self.total_buffered += 1;
        self.buffered_bytes += event_size_bytes;
    }

    /// Drain all buffered events into a reusable output collection.
    ///
    /// AUDIT FIX (H5): Changed from returning a new Vec to draining into
    /// a caller-provided VecDeque, eliminating O(n) allocation per alignment.
    fn drain_into(&mut self, out: &mut VecDeque<(usize, T)>) {
        // Drain in round-robin across inputs to maintain fairness
        let mut has_remaining = true;
        while has_remaining {
            has_remaining = false;
            for (input_id, buffer) in self.buffers.iter_mut().enumerate() {
                if let Some(event) = buffer.pop_front() {
                    out.push_back((input_id, event));
                    has_remaining = has_remaining || !buffer.is_empty();
                }
            }
        }
        self.total_buffered = 0;
        self.buffered_bytes = 0;
    }
}

/// The barrier aligner manages barrier alignment for a multi-input operator.
///
/// It supports at most one in-progress alignment at a time (checkpoints
/// do not overlap for aligned mode).
pub struct BarrierAligner<T> {
    /// Configuration.
    config: BarrierAlignerConfig,

    /// Current alignment state, if a checkpoint is in progress.
    current: Option<AlignmentState<T>>,

    /// Events drained from alignment buffers, pending delivery to operator.
    drain_queue: VecDeque<(usize, T)>,

    /// Metrics: total alignments completed.
    alignments_completed: u64,

    /// Metrics: total events buffered across all alignments.
    total_events_buffered: u64,

    /// Metrics: maximum alignment duration observed.
    max_alignment_duration: std::time::Duration,
}

impl<T> BarrierAligner<T> {
    /// Create a new barrier aligner with the given configuration.
    pub fn new(config: BarrierAlignerConfig) -> Self {
        assert!(config.num_inputs > 0, "must have at least one input");
        assert!(config.num_inputs <= 128, "maximum 128 inputs supported (u128 bitset limit)");

        Self {
            config,
            current: None,
            drain_queue: VecDeque::new(),
            alignments_completed: 0,
            total_events_buffered: 0,
            max_alignment_duration: std::time::Duration::ZERO,
        }
    }

    /// Process a message from the given input channel.
    ///
    /// Returns an `AlignmentAction` indicating what the operator should do.
    pub fn process_message(
        &mut self,
        input_id: usize,
        message: StreamMessage<T>,
    ) -> AlignmentAction<T> {
        debug_assert!(input_id < self.config.num_inputs);

        match message {
            StreamMessage::Event(event) => {
                if let Some(ref mut state) = self.current {
                    if state.is_input_aligned(input_id) {
                        // This input already sent its barrier; buffer the event
                        let event_size = std::mem::size_of::<T>();
                        state.buffer_event(input_id, event, event_size);
                        self.total_events_buffered += 1;
                        return AlignmentAction::Buffered;
                    }
                }
                // No alignment in progress, or this input hasn't sent barrier yet
                AlignmentAction::PassThrough(event)
            }

            StreamMessage::Watermark(wm) => {
                // Watermarks pass through regardless of alignment state.
                // They are not subject to barrier alignment — they don't
                // carry event data and don't affect snapshot consistency.
                //
                // AUDIT FIX (C4): Previously used unreachable!() which would
                // panic Ring 0 if any watermark reached the aligner. Now
                // handled gracefully with a dedicated WatermarkPassThrough variant.
                AlignmentAction::WatermarkPassThrough(wm)
            }

            StreamMessage::Barrier(barrier) => {
                self.handle_barrier(input_id, barrier)
            }
        }
    }

    /// Handle a barrier arrival from the given input.
    fn handle_barrier(
        &mut self,
        input_id: usize,
        barrier: CheckpointBarrier,
    ) -> AlignmentAction<T> {
        match self.current {
            None => {
                // First barrier for a new checkpoint
                let mut state = AlignmentState::new(barrier, self.config.num_inputs);
                state.mark_aligned(input_id);

                if state.is_fully_aligned() {
                    // Single-input operator: immediately aligned
                    self.alignments_completed += 1;
                    return AlignmentAction::TriggerSnapshot { barrier };
                }

                self.current = Some(state);
                AlignmentAction::Buffered // Signal that this input is now blocked
            }

            Some(ref mut state) => {
                if barrier.checkpoint_id != state.barrier.checkpoint_id {
                    // Barrier for a different checkpoint - this shouldn't happen
                    // in aligned mode. Log warning and treat as current.
                    // In practice, the coordinator ensures sequential checkpoints.
                }

                state.mark_aligned(input_id);

                if state.is_fully_aligned() {
                    // All inputs aligned! Trigger snapshot.
                    let duration = state.started_at.elapsed();
                    if duration > self.max_alignment_duration {
                        self.max_alignment_duration = duration;
                    }
                    self.alignments_completed += 1;

                    AlignmentAction::TriggerSnapshot {
                        barrier: state.barrier,
                    }
                } else {
                    AlignmentAction::Buffered
                }
            }
        }
    }

    /// Called after the operator has completed its snapshot.
    ///
    /// Drains all buffered events and resets alignment state.
    /// Returns a slice of (input_id, event) pairs to be processed.
    ///
    /// AUDIT FIX (H5): Reuses the internal `drain_queue` VecDeque across
    /// alignments to avoid O(n) allocation on every alignment completion.
    /// The previous design allocated a new `Vec<(usize, T)>` each time.
    pub fn complete_alignment(&mut self) -> &VecDeque<(usize, T)> {
        self.drain_queue.clear();
        if let Some(mut state) = self.current.take() {
            state.drain_into(&mut self.drain_queue);
        }
        &self.drain_queue
    }

    /// Check if alignment has timed out.
    pub fn check_timeout(&self) -> Option<std::time::Duration> {
        if let Some(ref state) = self.current {
            let elapsed = state.started_at.elapsed();
            if elapsed > self.config.alignment_timeout {
                return Some(elapsed);
            }
        }
        None
    }

    /// Check if buffer memory limit has been exceeded.
    pub fn is_buffer_limit_exceeded(&self) -> bool {
        if let Some(ref state) = self.current {
            state.buffered_bytes > self.config.max_buffer_memory_bytes
                || state.total_buffered
                    > self.config.max_buffer_per_input * self.config.num_inputs
        } else {
            false
        }
    }

    /// Returns true if an alignment is currently in progress.
    #[inline]
    pub fn is_aligning(&self) -> bool {
        self.current.is_some()
    }

    /// Returns the number of inputs that have aligned so far.
    pub fn aligned_input_count(&self) -> usize {
        self.current
            .as_ref()
            .map(|s| s.aligned_count())
            .unwrap_or(0)
    }

    /// Returns total events currently buffered.
    pub fn buffered_event_count(&self) -> usize {
        self.current
            .as_ref()
            .map(|s| s.total_buffered)
            .unwrap_or(0)
    }

    /// Returns metrics for observability.
    pub fn metrics(&self) -> BarrierAlignerMetrics {
        BarrierAlignerMetrics {
            alignments_completed: self.alignments_completed,
            total_events_buffered: self.total_events_buffered,
            max_alignment_duration: self.max_alignment_duration,
            current_buffered: self.buffered_event_count(),
            is_aligning: self.is_aligning(),
        }
    }
}

/// Metrics exposed by the barrier aligner for observability.
#[derive(Debug, Clone)]
pub struct BarrierAlignerMetrics {
    /// Total number of successful alignments.
    pub alignments_completed: u64,
    /// Total events buffered across all alignments.
    pub total_events_buffered: u64,
    /// Maximum alignment duration observed.
    pub max_alignment_duration: std::time::Duration,
    /// Events currently buffered in an in-progress alignment.
    pub current_buffered: usize,
    /// Whether an alignment is currently in progress.
    pub is_aligning: bool,
}
```

### Algorithm/Flow

#### Aligned Checkpoint Flow (Chandy-Lamport)

```
Multi-input operator with N inputs:

1. IDLE state: No checkpoint in progress
   - All inputs deliver events normally
   - Events pass through to operator logic

2. BARRIER on Input i:
   a. Create AlignmentState if first barrier for this checkpoint_id
   b. Mark input i as aligned (set bit i in inputs_aligned)
   c. If single-input operator or all inputs aligned → goto step 5
   d. Otherwise → enter ALIGNING state for input i

3. ALIGNING state for input i:
   - Events from input i are BUFFERED (not processed)
   - Events from non-aligned inputs pass through normally
   - Monitor buffer size against max_buffer_per_input
   - Monitor total memory against max_buffer_memory_bytes

4. BARRIER on Input j (j ≠ i):
   a. Mark input j as aligned
   b. Buffer events from input j going forward
   c. If all inputs aligned → goto step 5
   d. Otherwise → continue in ALIGNING state

5. ALL ALIGNED:
   a. Invoke operator.on_alignment_complete(barrier)
      - Operator snapshots its state
   b. Call complete_alignment() to drain buffered events
   c. Process all drained events through operator logic
      (in round-robin order across inputs for fairness)
   d. Forward barrier to all downstream channels
   e. Return to IDLE state

Invariant: At the snapshot point, all events from ALL inputs with
sequence number <= barrier have been processed, and NO events with
sequence number > barrier from ANY input have been processed.
```

#### Buffer Memory Management

```
Buffer growth strategy:
1. Initial allocation: 1024 events per input VecDeque
2. Growth: VecDeque doubles (amortized O(1) push)
3. Limit check on each buffer_event():
   - If total_buffered > max_buffer_per_input * num_inputs → abort
   - If buffered_bytes > max_buffer_memory_bytes → abort
4. On abort: cancel checkpoint, notify coordinator
5. With F-DCKP-006: switch to unaligned mode instead of aborting
```

#### Timeout Handling

```
The operator's event loop periodically calls check_timeout():
1. If alignment_timeout exceeded:
   a. Log warning with details (aligned inputs, pending inputs, buffered count)
   b. Notify coordinator of timeout
   c. Coordinator decides: abort checkpoint or switch to unaligned
   d. Aligner discards alignment state and drains buffers as normal events
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `AlignmentTimeout` | Not all barriers arrived within `alignment_timeout` | Abort checkpoint, drain buffers, notify coordinator |
| `BufferLimitExceeded` | Buffered events exceed `max_buffer_per_input` or `max_buffer_memory_bytes` | Abort checkpoint or switch to unaligned mode (F-DCKP-006) |
| `UnexpectedBarrier` | Barrier with mismatched checkpoint_id during alignment | Log warning, treat as new checkpoint (cancel current) |
| `InputIdOutOfRange` | `input_id >= num_inputs` in `process_message` | Debug assert; panic in debug, ignore in release |
| `DuplicateBarrier` | Same input delivers barrier twice for same checkpoint | Ignore duplicate, log warning |
| `AlignmentAborted` | Coordinator sends cancel signal during alignment | Drain buffers as normal events, reset state |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Barrier alignment overhead (single-input, no-op) | < 20ns | `bench_single_input_alignment` |
| Barrier alignment overhead (2-input, aligned) | < 100ns | `bench_two_input_alignment` |
| Event buffering during alignment | < 50ns/event | `bench_event_buffering` |
| Buffer drain throughput | > 20M events/sec | `bench_buffer_drain` |
| Alignment state memory overhead | < 200 bytes (excluding buffers) | Static analysis |
| Maximum supported inputs | 128 (u128 bitset limit) | Compile-time assert |

## Test Plan

### Unit Tests

- [ ] `test_single_input_immediate_alignment` - One input, barrier triggers immediate snapshot
- [ ] `test_two_input_alignment_order_1_2` - Barrier on input 0 first, then input 1
- [ ] `test_two_input_alignment_order_2_1` - Barrier on input 1 first, then input 0
- [ ] `test_three_input_alignment` - All three barriers arrive sequentially
- [ ] `test_events_buffered_from_aligned_input` - Events after barrier are buffered
- [ ] `test_events_pass_through_unaligned_input` - Events before barrier pass through
- [ ] `test_drain_after_alignment` - Buffered events returned in correct order
- [ ] `test_drain_round_robin_fairness` - Events drained fairly across inputs
- [ ] `test_alignment_timeout_detection` - Timeout detected when exceeded
- [ ] `test_buffer_limit_exceeded` - Memory limit triggers abort signal
- [ ] `test_complete_alignment_resets_state` - State cleared after completion
- [ ] `test_duplicate_barrier_ignored` - Same input, same checkpoint_id
- [ ] `test_metrics_updated_on_alignment` - Counters increment correctly
- [ ] `test_max_alignment_duration_tracked` - Worst-case duration recorded
- [ ] `test_128_input_alignment` - Maximum input count supported (u128 bitset)
- [ ] `test_watermark_passthrough_during_alignment` - Watermark returns WatermarkPassThrough, not panic
- [ ] `test_watermark_passthrough_no_alignment` - Watermark passes through when idle
- [ ] `test_complete_alignment_reuses_drain_queue` - No allocation on second alignment

### Integration Tests

- [ ] `test_join_operator_with_barrier_alignment` - Stream-stream join receives barriers on both inputs
- [ ] `test_union_operator_with_three_inputs` - Union aligns three input barriers
- [ ] `test_alignment_under_backpressure` - Slow input causes buffering on fast input
- [ ] `test_alignment_timeout_triggers_abort` - Timeout causes checkpoint abort
- [ ] `test_alignment_then_normal_processing` - Processing resumes correctly after alignment
- [ ] `test_consecutive_checkpoints` - Multiple checkpoints back-to-back

### Benchmarks

- [ ] `bench_single_input_alignment` - Target: < 20ns
- [ ] `bench_two_input_alignment` - Target: < 100ns
- [ ] `bench_event_buffering` - Target: < 50ns/event
- [ ] `bench_buffer_drain_10k` - Target: > 20M events/sec
- [ ] `bench_alignment_with_100k_buffered` - Drain latency for large buffers

## Rollout Plan

1. **Phase 1**: `AlignmentState` and bitset tracking + unit tests
2. **Phase 2**: `BarrierAligner` core logic (process_message, handle_barrier)
3. **Phase 3**: Buffer management and memory limits
4. **Phase 4**: Timeout detection and abort logic
5. **Phase 5**: Integration with join and union operators
6. **Phase 6**: Benchmarks + optimization
7. **Phase 7**: Documentation and code review

## Open Questions

- [ ] Should the drain order be round-robin or sequential (drain all of input 0, then input 1, etc.)? Round-robin is fairer but sequential may be simpler and have better cache behavior.
- [ ] Should the aligner support overlapping checkpoints (checkpoint N+1 starts before N completes)? Currently no -- aligned mode is sequential by nature.
- [ ] Should we track per-input buffer sizes separately or only the aggregate? Per-input tracking enables better diagnostics but adds overhead.
- [ ] When buffer limits are exceeded, should we abort the checkpoint or switch to unaligned mode automatically? This depends on whether F-DCKP-006 is enabled.

## Completion Checklist

- [ ] `AlignmentState` implemented with bitset tracking
- [ ] `BarrierAligner` implemented with `process_message` and `complete_alignment`
- [ ] Buffer memory management with configurable limits
- [ ] Timeout detection implemented
- [ ] Metrics and observability exposed
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests with join/union operators
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Chandy-Lamport Algorithm](https://en.wikipedia.org/wiki/Chandy%E2%80%93Lamport_algorithm) - Consistent cut via marker propagation
- [Apache Flink Barrier Alignment](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/#barriers) - Production implementation
- [F-DCKP-001: Checkpoint Barrier Protocol](F-DCKP-001-barrier-protocol.md) - Barrier types and injection
- [F-DCKP-006: Unaligned Checkpoints](F-DCKP-006-unaligned-checkpoints.md) - Fallback when alignment stalls
- [F019: Stream-Stream Joins](../../phase-2/F019-stream-stream-joins.md) - Primary consumer of barrier alignment
