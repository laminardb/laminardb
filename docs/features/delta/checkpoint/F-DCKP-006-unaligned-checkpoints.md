# F-DCKP-006: Unaligned Checkpoints

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DCKP-006 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6c |
| **Effort** | XL (8-13 days) |
| **Dependencies** | F-DCKP-002 (Barrier Alignment) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/checkpoint/unaligned.rs` |

## Summary

Implements unaligned checkpoints as a backpressure-tolerant fallback for the aligned Chandy-Lamport protocol. When aligned checkpoint duration exceeds a configurable threshold (default: 30 seconds), the system automatically switches to unaligned mode for that checkpoint. In unaligned mode, barriers overtake in-flight data: upon receiving a barrier on any input, the operator immediately snapshots its state plus all in-flight buffers from channels that have not yet delivered their barrier. This makes checkpoint duration independent of backpressure at the cost of larger checkpoint size (includes buffered events) and slightly longer recovery (buffered events must be replayed). This follows the approach proven in Apache Flink 1.11+.

## Goals

- Implement unaligned checkpoint mode where barriers can overtake in-flight data
- Automatic threshold-based switching from aligned to unaligned mode
- On first barrier receipt: immediately snapshot operator state + capture in-flight channel buffers
- Persist in-flight buffers alongside operator state in checkpoint
- Recovery procedure that restores both state and in-flight buffers
- Configurable threshold for aligned-to-unaligned switching
- Support for disabling unaligned mode entirely (pure Chandy-Lamport)
- Observable metrics: unaligned checkpoint count, average checkpoint size increase

## Non-Goals

- Modifying the barrier protocol itself (F-DCKP-001 barriers are reused)
- Implementing a fully new checkpoint algorithm (this extends aligned checkpoints)
- Optimizing in-flight buffer size (deferred to incremental checkpoints, F-DCKP-007)
- Cross-node coordination changes (F-DCKP-008 handles both modes transparently)
- Buffered event deduplication during recovery

## Technical Design

### Architecture

**Ring**: Ring 0 (Hot Path) for barrier handling; Ring 2 for persistence of in-flight buffers.

**Crate**: `laminar-core`

**Module**: `laminar-core/src/checkpoint/unaligned.rs`

Unaligned checkpoints modify the behavior of the `BarrierAligner` (F-DCKP-002). Instead of blocking inputs and waiting for all barriers, the operator acts immediately on the first barrier it receives. The key insight is that by capturing the in-flight data in channels that have not yet delivered their barrier, we effectively "cut" the channel at the barrier position without waiting.

```
ALIGNED MODE (normal):                    UNALIGNED MODE (backpressure fallback):

Input 0: [E E E B E E E]                 Input 0: [E E E B E E E]
Input 1: [E E E E E E B] ← slow          Input 1: [E E E E E E B] ← slow

Action: Block input 0 at B,               Action: On first B (input 0):
        wait for input 1's B                 1. Snapshot state immediately
        Events buffer up...                  2. Capture in-flight events from
                                                input 1 that are BEFORE its B
        When input 1's B arrives:            3. Emit barrier downstream
        → snapshot state                     4. No blocking, no waiting
        → emit barrier
        → drain buffered events

Trade-off: checkpoint duration depends    Trade-off: checkpoint size includes
on slowest input                          in-flight data, recovery replays it
```

### API/Interface

```rust
use crate::checkpoint::alignment::{BarrierAligner, BarrierAlignerConfig};
use crate::checkpoint::barrier::{CheckpointBarrier, StreamMessage};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Configuration for unaligned checkpoint behavior.
#[derive(Debug, Clone)]
pub struct UnalignedCheckpointConfig {
    /// Enable unaligned checkpoints.
    /// When false, aligned mode is used exclusively.
    pub enabled: bool,

    /// Threshold for switching from aligned to unaligned mode.
    /// If a checkpoint's alignment phase exceeds this duration,
    /// the system switches to unaligned mode for that checkpoint.
    pub alignment_timeout_threshold: Duration,

    /// Maximum in-flight buffer size (bytes) to capture per channel.
    /// If exceeded, the checkpoint fails rather than consuming
    /// unbounded memory.
    pub max_inflight_buffer_bytes: usize,

    /// Whether to proactively use unaligned mode (skip aligned attempt).
    /// Useful when backpressure is known to be persistent.
    pub force_unaligned: bool,
}

impl Default for UnalignedCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alignment_timeout_threshold: Duration::from_secs(30),
            max_inflight_buffer_bytes: 512 * 1024 * 1024, // 512 MiB
            force_unaligned: false,
        }
    }
}

/// In-flight data captured from a channel during unaligned checkpoint.
#[derive(Debug, Clone)]
pub struct InFlightChannelData {
    /// Input channel ID this data was captured from.
    pub input_id: usize,

    /// Events that were in the channel BEFORE the barrier.
    /// These are serialized and included in the checkpoint.
    pub events: Vec<Vec<u8>>,

    /// Total size of captured events in bytes.
    pub size_bytes: usize,
}

/// Complete unaligned checkpoint data for a single operator.
#[derive(Debug)]
pub struct UnalignedSnapshot {
    /// The checkpoint barrier that triggered this snapshot.
    pub barrier: CheckpointBarrier,

    /// Operator state snapshot (rkyv serialized).
    pub operator_state: Vec<u8>,

    /// In-flight data from each input channel.
    pub inflight_data: Vec<InFlightChannelData>,

    /// Total size of operator state + in-flight data.
    pub total_size_bytes: usize,

    /// Whether this snapshot was triggered by threshold switching
    /// (vs being requested as unaligned from the start).
    pub was_threshold_triggered: bool,
}

/// Result of an unaligned checkpoint action.
#[derive(Debug)]
pub enum UnalignedAction<T> {
    /// Take an immediate snapshot: operator state + in-flight buffers.
    TakeSnapshot {
        barrier: CheckpointBarrier,
        /// Events from channels that have not yet delivered their barrier.
        /// These must be serialized and included in the checkpoint.
        inflight_events: Vec<(usize, Vec<T>)>,
    },

    /// Event from an already-checkpointed channel: process normally
    /// (no buffering in unaligned mode).
    PassThrough(T),

    /// Late barrier from a channel: just consume it, snapshot already taken.
    LateBarrier { input_id: usize },
}
```

### Data Structures

```rust
/// Manages the unaligned checkpoint state machine for a multi-input operator.
pub struct UnalignedCheckpointer<T> {
    /// Configuration.
    config: UnalignedCheckpointConfig,

    /// Number of input channels.
    num_inputs: usize,

    /// Current checkpoint state.
    state: UnalignedState<T>,

    /// Metrics: number of unaligned checkpoints taken.
    unaligned_count: u64,

    /// Metrics: number of threshold-triggered switches.
    threshold_switches: u64,

    /// Metrics: total in-flight bytes captured.
    total_inflight_bytes: u64,
}

/// Internal state of the unaligned checkpointer.
#[derive(Debug)]
enum UnalignedState<T> {
    /// No checkpoint in progress.
    Idle,

    /// Aligned mode attempt in progress. Will switch to unaligned
    /// if threshold is exceeded.
    AligningWithFallback {
        /// The aligned barrier aligner.
        aligner_started: Instant,
        /// The checkpoint barrier.
        barrier: CheckpointBarrier,
        /// Inputs that have delivered their barrier.
        aligned_inputs: u64,
        /// Buffered events from aligned inputs (same as regular alignment).
        buffers: Vec<VecDeque<T>>,
    },

    /// Unaligned snapshot has been taken; waiting for remaining barriers
    /// from channels that had not yet delivered theirs.
    WaitingForLateBarriers {
        /// The checkpoint barrier.
        barrier: CheckpointBarrier,
        /// Inputs that have delivered their barrier.
        received_barriers: u64,
    },
}

impl<T: Clone> UnalignedCheckpointer<T> {
    /// Create a new unaligned checkpointer.
    pub fn new(config: UnalignedCheckpointConfig, num_inputs: usize) -> Self {
        assert!(num_inputs > 0 && num_inputs <= 64);
        Self {
            config,
            num_inputs,
            state: UnalignedState::Idle,
            unaligned_count: 0,
            threshold_switches: 0,
            total_inflight_bytes: 0,
        }
    }

    /// Process a message, handling both aligned and unaligned modes.
    pub fn process_message(
        &mut self,
        input_id: usize,
        message: StreamMessage<T>,
        // Callback to read in-flight events from non-aligned channels.
        // Returns events currently buffered in the channel.
        read_inflight: impl Fn(usize) -> Vec<T>,
    ) -> UnalignedAction<T> {
        match message {
            StreamMessage::Barrier(barrier) => {
                self.handle_barrier(input_id, barrier, read_inflight)
            }
            StreamMessage::Event(event) => {
                self.handle_event(input_id, event)
            }
            StreamMessage::Watermark(_) => {
                unreachable!("watermarks handled before checkpointer")
            }
        }
    }

    /// Handle a barrier arrival.
    fn handle_barrier(
        &mut self,
        input_id: usize,
        barrier: CheckpointBarrier,
        read_inflight: impl Fn(usize) -> Vec<T>,
    ) -> UnalignedAction<T> {
        match &mut self.state {
            UnalignedState::Idle => {
                if barrier.is_unaligned() || self.config.force_unaligned {
                    // Immediate unaligned snapshot
                    self.take_unaligned_snapshot(input_id, barrier, read_inflight)
                } else {
                    // Start aligned attempt with fallback
                    let mut buffers: Vec<VecDeque<T>> = (0..self.num_inputs)
                        .map(|_| VecDeque::new())
                        .collect();

                    let mut aligned_inputs = 0u64;
                    aligned_inputs |= 1 << input_id;

                    if self.num_inputs == 1 {
                        // Single input: immediate alignment
                        self.state = UnalignedState::Idle;
                        return UnalignedAction::TakeSnapshot {
                            barrier,
                            inflight_events: Vec::new(),
                        };
                    }

                    self.state = UnalignedState::AligningWithFallback {
                        aligner_started: Instant::now(),
                        barrier,
                        aligned_inputs,
                        buffers,
                    };
                    UnalignedAction::PassThrough(
                        // Signal that this input is now considered aligned
                        // but no event to pass through; return a no-op
                        unreachable!("barrier handled, no event to pass")
                    )
                }
            }

            UnalignedState::AligningWithFallback {
                aligner_started,
                barrier: current_barrier,
                aligned_inputs,
                buffers,
            } => {
                *aligned_inputs |= 1 << input_id;

                let expected_mask = (1u64 << self.num_inputs) - 1;
                if *aligned_inputs == expected_mask {
                    // All inputs aligned normally
                    let barrier = *current_barrier;
                    self.state = UnalignedState::Idle;
                    return UnalignedAction::TakeSnapshot {
                        barrier,
                        inflight_events: Vec::new(),
                    };
                }

                // Check threshold for switching to unaligned
                if self.config.enabled
                    && aligner_started.elapsed() > self.config.alignment_timeout_threshold
                {
                    self.threshold_switches += 1;
                    let barrier = *current_barrier;
                    self.take_unaligned_snapshot(input_id, barrier, read_inflight)
                } else {
                    // Continue waiting (event buffering handled by handle_event)
                    UnalignedAction::LateBarrier { input_id }
                }
            }

            UnalignedState::WaitingForLateBarriers {
                barrier,
                received_barriers,
            } => {
                *received_barriers |= 1 << input_id;

                let expected_mask = (1u64 << self.num_inputs) - 1;
                if *received_barriers == expected_mask {
                    // All late barriers received; checkpoint is fully complete
                    self.state = UnalignedState::Idle;
                }

                UnalignedAction::LateBarrier { input_id }
            }
        }
    }

    /// Handle an event during checkpoint processing.
    fn handle_event(
        &mut self,
        input_id: usize,
        event: T,
    ) -> UnalignedAction<T> {
        match &mut self.state {
            UnalignedState::Idle => {
                UnalignedAction::PassThrough(event)
            }

            UnalignedState::AligningWithFallback {
                aligned_inputs,
                buffers,
                ..
            } => {
                let is_aligned = *aligned_inputs & (1 << input_id) != 0;
                if is_aligned {
                    // Input already sent barrier; buffer event (aligned mode)
                    buffers[input_id].push_back(event);
                    // Check if we should switch to unaligned mode due to timeout
                    // (threshold check happens on next barrier arrival)
                    UnalignedAction::PassThrough(
                        // This is actually buffered in aligned mode
                        // The caller should check the aligner state
                        unreachable!("buffered events not passed through")
                    )
                } else {
                    // Input has not sent barrier yet; process normally
                    UnalignedAction::PassThrough(event)
                }
            }

            UnalignedState::WaitingForLateBarriers { .. } => {
                // In unaligned mode, all events pass through after snapshot
                UnalignedAction::PassThrough(event)
            }
        }
    }

    /// Take an unaligned snapshot: capture state + in-flight events.
    fn take_unaligned_snapshot(
        &mut self,
        trigger_input: usize,
        barrier: CheckpointBarrier,
        read_inflight: impl Fn(usize) -> Vec<T>,
    ) -> UnalignedAction<T> {
        self.unaligned_count += 1;

        // Capture in-flight events from channels that have NOT sent their barrier
        let mut inflight_events = Vec::new();
        let mut received_barriers = 0u64;
        received_barriers |= 1 << trigger_input;

        for input_id in 0..self.num_inputs {
            if input_id == trigger_input {
                continue;
            }
            // Read in-flight events from this channel
            let events = read_inflight(input_id);
            if !events.is_empty() {
                self.total_inflight_bytes += events.len() * std::mem::size_of::<T>();
                inflight_events.push((input_id, events));
            }
        }

        self.state = UnalignedState::WaitingForLateBarriers {
            barrier,
            received_barriers,
        };

        UnalignedAction::TakeSnapshot {
            barrier,
            inflight_events,
        }
    }

    /// Check if the current aligned attempt should switch to unaligned.
    ///
    /// Called periodically by the operator's event loop.
    pub fn should_switch_to_unaligned(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        match &self.state {
            UnalignedState::AligningWithFallback {
                aligner_started, ..
            } => aligner_started.elapsed() > self.config.alignment_timeout_threshold,
            _ => false,
        }
    }

    /// Returns metrics for observability.
    pub fn metrics(&self) -> UnalignedCheckpointMetrics {
        UnalignedCheckpointMetrics {
            unaligned_count: self.unaligned_count,
            threshold_switches: self.threshold_switches,
            total_inflight_bytes: self.total_inflight_bytes,
            is_in_unaligned_mode: matches!(
                self.state,
                UnalignedState::WaitingForLateBarriers { .. }
            ),
        }
    }
}

/// Metrics for unaligned checkpoint observability.
#[derive(Debug, Clone)]
pub struct UnalignedCheckpointMetrics {
    /// Total unaligned checkpoints taken.
    pub unaligned_count: u64,
    /// Number of times aligned mode switched to unaligned via threshold.
    pub threshold_switches: u64,
    /// Total in-flight bytes captured across all unaligned checkpoints.
    pub total_inflight_bytes: u64,
    /// Whether currently in unaligned mode waiting for late barriers.
    pub is_in_unaligned_mode: bool,
}
```

### Algorithm/Flow

#### Threshold-Based Switching

```
1. Checkpoint initiated with aligned barriers (default)
2. First barrier arrives on input i
3. Start alignment timer
4. If remaining barriers arrive within threshold (30s):
   → Complete aligned checkpoint (normal Chandy-Lamport)
5. If threshold exceeded:
   a. Switch to unaligned mode
   b. Immediately snapshot operator state
   c. For each input j that has NOT delivered its barrier:
      - Read all events currently in channel j's buffer
      - These events are BEFORE the barrier in causal order
      - Serialize and include in checkpoint
   d. Emit barrier downstream (barriers overtake data)
   e. Mark snapshot as is_unaligned = true
   f. Continue processing all inputs without blocking
   g. When late barriers arrive on remaining inputs:
      - Just consume them (snapshot already taken)
      - No additional action needed
```

#### In-Flight Buffer Persistence Format

```
For each input channel with captured in-flight data:

InFlightChannelData:
  input_id: u32           # Which input channel
  event_count: u64         # Number of events captured
  events: [               # rkyv-serialized events
    { len: u32, data: [u8] },  # Length-prefixed event bytes
    ...
  ]

Stored at:
  {checkpoint-dir}/operators/{operator-id}/inflight_{input_id}.buf

The manifest includes an additional field for unaligned checkpoints:
  "is_unaligned": true,
  "inflight_data": [
    { "operator_id": "...", "input_id": 0, "path": "...", "event_count": N }
  ]
```

#### Recovery Procedure for Unaligned Checkpoints

```
1. Load manifest (same as aligned recovery)
2. Check is_unaligned flag in manifest
3. Restore operator state (same as aligned)
4. If is_unaligned:
   a. Download in-flight buffer files
   b. For each input channel with in-flight data:
      - Deserialize events from buffer
      - Re-inject events into the input channel
        (they will be processed before any new events from source)
   c. The re-injected events are followed by the barrier
      (logically, the barrier was after these events)
5. Seek sources to checkpoint offsets (same as aligned)
6. Resume processing

The in-flight events are processed FIRST, then new events from sources.
This reconstructs the exact state the operator would have been in if
the aligned checkpoint had completed normally.
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `InFlightBufferTooLarge` | Captured in-flight data exceeds `max_inflight_buffer_bytes` | Abort checkpoint; increase threshold or buffer limit |
| `InFlightSerializationFailed` | Failed to serialize in-flight events | Abort checkpoint; fall back to aligned mode for next attempt |
| `InFlightDeserializationFailed` | Corrupt in-flight buffer during recovery | Skip in-flight data; events will be replayed from source (may cause duplicates without exactly-once sink) |
| `ThresholdTooLow` | Threshold set too low, every checkpoint goes unaligned | Warning log; recommend increasing threshold |
| `ChannelReadFailed` | Failed to read in-flight events from channel | Abort unaligned checkpoint; retry as aligned |
| `LateBarrierTimeout` | Late barriers never arrive (operator crashed) | Coordinator detects and handles; checkpoint already committed |

## Performance Targets

| Metric | Target | Validation |
|--------|--------|------------|
| Unaligned snapshot trigger latency | < 100ns (from threshold detection to snapshot start) | `bench_unaligned_trigger` |
| In-flight buffer capture (10K events) | < 1ms | `bench_inflight_capture` |
| In-flight buffer serialization | > 200 MB/s | `bench_inflight_serialize` |
| Checkpoint size increase (unaligned vs aligned) | 10-100% depending on buffer depth | Measurement test |
| Recovery overhead (unaligned vs aligned) | < 20% additional time for in-flight replay | `bench_unaligned_recovery` |
| Aligned-to-unaligned switch | < 1ms | `bench_mode_switch` |

## Test Plan

### Unit Tests

- [ ] `test_unaligned_config_defaults` - Default threshold is 30s
- [ ] `test_immediate_unaligned_with_flag` - Barrier with is_unaligned flag
- [ ] `test_force_unaligned_mode` - Config force_unaligned skips aligned attempt
- [ ] `test_threshold_switch_from_aligned` - Aligned mode times out, switches
- [ ] `test_inflight_capture_single_channel` - Capture in-flight from one channel
- [ ] `test_inflight_capture_multiple_channels` - Capture from multiple channels
- [ ] `test_late_barrier_consumed_after_snapshot` - Late barriers do not re-trigger
- [ ] `test_all_late_barriers_received_resets_state` - State returns to Idle
- [ ] `test_events_pass_through_after_unaligned_snapshot` - No blocking
- [ ] `test_inflight_buffer_size_limit` - Exceeding limit aborts checkpoint
- [ ] `test_disabled_unaligned_stays_aligned` - Config enabled=false
- [ ] `test_metrics_count_unaligned_checkpoints` - Counter increments
- [ ] `test_metrics_count_threshold_switches` - Switch counter increments

### Integration Tests

- [ ] `test_unaligned_checkpoint_with_backpressure` - Slow input triggers unaligned
- [ ] `test_unaligned_checkpoint_save_and_load` - Full round-trip with in-flight data
- [ ] `test_unaligned_recovery_replays_inflight` - In-flight events re-injected on recovery
- [ ] `test_unaligned_then_aligned_sequence` - Mixed checkpoint modes in sequence
- [ ] `test_unaligned_with_join_operator` - Two-input join with backpressure

### Benchmarks

- [ ] `bench_unaligned_trigger` - Target: < 100ns
- [ ] `bench_inflight_capture_10k` - Target: < 1ms
- [ ] `bench_inflight_serialize_1mb` - Target: < 5ms
- [ ] `bench_mode_switch` - Target: < 1ms
- [ ] `bench_unaligned_recovery_10k_inflight` - Target: < 50ms

## Rollout Plan

1. **Phase 1**: `UnalignedCheckpointConfig` and `UnalignedCheckpointer` skeleton
2. **Phase 2**: Threshold-based switching from `AligningWithFallback` state
3. **Phase 3**: In-flight buffer capture via `read_inflight` callback
4. **Phase 4**: `InFlightChannelData` serialization and persistence format
5. **Phase 5**: Recovery procedure for unaligned checkpoints
6. **Phase 6**: Integration with `BarrierAligner` (F-DCKP-002) as fallback
7. **Phase 7**: Integration tests with backpressure scenarios
8. **Phase 8**: Benchmarks and optimization
9. **Phase 9**: Documentation and code review

## Open Questions

- [ ] Should the threshold be adaptive (increase if unaligned checkpoints are frequent)? An adaptive threshold could reduce unnecessary unaligned checkpoints.
- [ ] Should we support "partially unaligned" checkpoints where some operators use aligned and others use unaligned? This adds complexity but could reduce checkpoint size.
- [ ] How should the in-flight buffer capture interact with the SPSC channel? Options: (a) drain the channel, (b) snapshot the ring buffer in place, (c) use a shadow buffer.
- [ ] Should in-flight buffers be compressed before persistence? Compression would reduce checkpoint size but add CPU overhead.

## Completion Checklist

- [ ] `UnalignedCheckpointer` state machine implemented
- [ ] Threshold-based switching from aligned to unaligned
- [ ] In-flight buffer capture and serialization
- [ ] In-flight buffer persistence format defined
- [ ] Recovery procedure for unaligned checkpoints
- [ ] Integration with `BarrierAligner`
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Benchmarks meet targets
- [ ] Documentation updated
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [Flink FLIP-76: Unaligned Checkpoints](https://cwiki.apache.org/confluence/display/FLINK/FLIP-76%3A+Unaligned+Checkpoints) - Original design proposal
- [Apache Flink Unaligned Checkpoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/checkpointing/#unaligned-checkpoints) - Production documentation
- [F-DCKP-002: Barrier Alignment](F-DCKP-002-barrier-alignment.md) - Aligned mode this extends
- [F-DCKP-001: Checkpoint Barrier Protocol](F-DCKP-001-barrier-protocol.md) - Barrier types
- [F-DCKP-004: Object Store Checkpointer](F-DCKP-004-object-store-checkpointer.md) - Persistence backend
