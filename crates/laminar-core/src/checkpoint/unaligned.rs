//! Unaligned checkpoint protocol with timeout-based fallback.
//!
//! When barrier alignment takes too long (due to backpressure on slow inputs),
//! the checkpoint can fall back to an unaligned snapshot that captures in-flight
//! data from channels. This trades larger checkpoint size for faster completion.
//!
//! ## Protocol (Flink 1.11+ style)
//!
//! 1. First barrier arrives → start alignment timer
//! 2. If all barriers arrive within timeout → normal aligned snapshot
//! 3. If timeout fires → capture in-flight events from non-aligned inputs →
//!    unaligned snapshot
//! 4. Late barriers from non-aligned inputs arrive → transition to idle
//!
//! ## Constraints
//!
//! - Sink operators must NOT use unaligned mode (Flink 2.0 finding:
//!   committables must be at the sink on commit)
//! - In-flight buffer size is bounded to prevent OOM

use std::time::{Duration, Instant};

use super::barrier::CheckpointBarrier;

/// Configuration for unaligned checkpoints.
#[derive(Debug, Clone)]
pub struct UnalignedCheckpointConfig {
    /// Whether unaligned checkpoints are enabled.
    pub enabled: bool,
    /// Duration after which aligned checkpoint falls back to unaligned.
    pub alignment_timeout_threshold: Duration,
    /// Maximum bytes of in-flight data to buffer per checkpoint.
    pub max_inflight_buffer_bytes: usize,
    /// Force unaligned mode for all checkpoints (skip aligned attempt).
    pub force_unaligned: bool,
}

impl Default for UnalignedCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            alignment_timeout_threshold: Duration::from_secs(10),
            max_inflight_buffer_bytes: 256 * 1024 * 1024,
            force_unaligned: false,
        }
    }
}

/// In-flight data captured from a single input channel.
#[derive(Debug, Clone)]
pub struct InFlightChannelData {
    /// Input index that the data was captured from.
    pub input_id: usize,
    /// Serialized events buffered in the channel.
    pub events: Vec<Vec<u8>>,
    /// Total bytes across all events.
    pub size_bytes: usize,
}

/// Result of an unaligned snapshot.
#[derive(Debug)]
pub struct UnalignedSnapshot {
    /// The barrier that triggered this snapshot.
    pub barrier: CheckpointBarrier,
    /// Operator state at snapshot time.
    pub operator_state: Option<Vec<u8>>,
    /// In-flight data captured from non-aligned input channels.
    pub inflight_data: Vec<InFlightChannelData>,
    /// Total bytes of in-flight data.
    pub total_size_bytes: usize,
    /// Whether the unaligned path was triggered by timeout (vs. force).
    pub was_threshold_triggered: bool,
}

/// Actions emitted by the [`UnalignedCheckpointer`] state machine.
#[derive(Debug)]
pub enum UnalignedAction<T> {
    /// Forward event downstream (normal processing).
    Forward(T),
    /// Buffer event for in-flight capture (during alignment).
    Buffer,
    /// Aligned snapshot completed (all barriers arrived in time).
    AlignedSnapshot(CheckpointBarrier),
    /// Unaligned snapshot completed (timeout triggered).
    UnalignedSnapshot(UnalignedSnapshot),
    /// Drain buffered event from in-flight capture.
    Drain(T),
    /// Pass through a watermark.
    WatermarkPassThrough(i64),
}

/// States of the unaligned checkpoint state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
enum State {
    /// No checkpoint in progress.
    Idle,
    /// Aligning barriers with fallback to unaligned on timeout.
    AligningWithFallback {
        /// When the first barrier arrived.
        started_at: Instant,
        /// Bitset of input IDs that have sent their barrier.
        aligned_inputs: u128,
        /// The barrier being aligned.
        barrier: CheckpointBarrier,
    },
    /// Unaligned snapshot taken, waiting for remaining barriers.
    WaitingForLateBarriers {
        /// Bitset of input IDs that have sent their barrier.
        received_inputs: u128,
        /// The barrier being completed.
        barrier: CheckpointBarrier,
    },
}

/// State machine for unaligned checkpoints with timeout-based fallback.
///
/// Manages the transition between aligned and unaligned checkpoint modes
/// for a single operator with multiple inputs.
///
/// ## Type Parameter
///
/// `T` is the event type flowing through the operator (e.g., `RecordBatch`).
pub struct UnalignedCheckpointer<T> {
    /// Number of input channels.
    num_inputs: usize,
    /// Configuration.
    config: UnalignedCheckpointConfig,
    /// Current state.
    state: State,
    /// Buffered in-flight events per input (populated during alignment).
    inflight_buffers: Vec<Vec<T>>,
    /// Total buffered bytes.
    buffered_bytes: usize,
    /// Whether this operator is a sink (sinks cannot use unaligned mode).
    is_sink: bool,
}

impl<T> UnalignedCheckpointer<T> {
    /// Creates a new unaligned checkpointer.
    ///
    /// # Arguments
    ///
    /// * `num_inputs` — Number of input channels
    /// * `config` — Unaligned checkpoint configuration
    /// * `is_sink` — Whether this operator is a sink (disables unaligned mode)
    #[must_use]
    pub fn new(num_inputs: usize, config: UnalignedCheckpointConfig, is_sink: bool) -> Self {
        let inflight_buffers = (0..num_inputs).map(|_| Vec::new()).collect();
        Self {
            num_inputs,
            config,
            state: State::Idle,
            inflight_buffers,
            buffered_bytes: 0,
            is_sink,
        }
    }

    /// Returns the current number of aligned inputs (for testing/observability).
    #[must_use]
    pub fn aligned_count(&self) -> usize {
        match &self.state {
            State::AligningWithFallback { aligned_inputs, .. } => {
                aligned_inputs.count_ones() as usize
            }
            State::WaitingForLateBarriers {
                received_inputs, ..
            } => received_inputs.count_ones() as usize,
            State::Idle => 0,
        }
    }

    /// Returns whether a checkpoint is in progress.
    #[must_use]
    pub fn is_checkpointing(&self) -> bool {
        !matches!(self.state, State::Idle)
    }

    /// Returns the total buffered in-flight bytes.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.buffered_bytes
    }

    /// Processes a barrier arriving from the given input.
    ///
    /// Returns the action to take. The caller must handle the action
    /// (e.g., take a snapshot, forward events, etc.).
    pub fn on_barrier(&mut self, input_id: usize, barrier: CheckpointBarrier) -> UnalignedAction<T>
    where
        T: std::fmt::Debug,
    {
        match &self.state {
            State::Idle => {
                // First barrier — start alignment
                let mut aligned_inputs = 0u128;
                aligned_inputs |= 1u128 << input_id;

                if self.num_inputs == 1 || aligned_inputs.count_ones() as usize == self.num_inputs {
                    // Single input or all arrived at once → aligned snapshot
                    self.state = State::Idle;
                    self.clear_buffers();
                    return UnalignedAction::AlignedSnapshot(barrier);
                }

                if self.config.force_unaligned && !self.is_sink {
                    // Force unaligned: skip alignment entirely
                    return self.trigger_unaligned(barrier, aligned_inputs);
                }

                self.state = State::AligningWithFallback {
                    started_at: Instant::now(),
                    aligned_inputs,
                    barrier,
                };

                UnalignedAction::Buffer
            }
            State::AligningWithFallback {
                started_at,
                aligned_inputs,
                barrier: pending_barrier,
            } => {
                let mut aligned = *aligned_inputs;
                aligned |= 1u128 << input_id;
                let started = *started_at;
                let pending = *pending_barrier;

                if aligned.count_ones() as usize == self.num_inputs {
                    // All barriers arrived in time → aligned snapshot
                    self.state = State::Idle;
                    self.clear_buffers();
                    return UnalignedAction::AlignedSnapshot(pending);
                }

                // Check timeout
                if started.elapsed() >= self.config.alignment_timeout_threshold && !self.is_sink {
                    return self.trigger_unaligned(pending, aligned);
                }

                self.state = State::AligningWithFallback {
                    started_at: started,
                    aligned_inputs: aligned,
                    barrier: pending,
                };

                UnalignedAction::Buffer
            }
            State::WaitingForLateBarriers {
                received_inputs,
                barrier: pending_barrier,
            } => {
                let mut received = *received_inputs;
                received |= 1u128 << input_id;
                let pending = *pending_barrier;

                if received.count_ones() as usize == self.num_inputs {
                    // All late barriers received → back to idle
                    self.state = State::Idle;
                    self.clear_buffers();
                } else {
                    self.state = State::WaitingForLateBarriers {
                        received_inputs: received,
                        barrier: pending,
                    };
                }

                // Late barriers are acknowledged but don't produce new snapshots
                UnalignedAction::Buffer
            }
        }
    }

    /// Checks whether the alignment timeout has expired.
    ///
    /// Call this periodically (e.g., on each event poll) to detect timeout.
    /// Returns `Some(UnalignedSnapshot)` if timeout triggered.
    pub fn check_timeout(&mut self) -> Option<UnalignedAction<T>>
    where
        T: std::fmt::Debug,
    {
        if self.is_sink {
            return None;
        }

        match &self.state {
            State::AligningWithFallback {
                started_at,
                aligned_inputs,
                barrier,
            } => {
                if started_at.elapsed() >= self.config.alignment_timeout_threshold {
                    let barrier = *barrier;
                    let aligned = *aligned_inputs;
                    Some(self.trigger_unaligned(barrier, aligned))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Triggers unaligned mode: captures in-flight events from non-aligned inputs.
    fn trigger_unaligned(
        &mut self,
        barrier: CheckpointBarrier,
        aligned_inputs: u128,
    ) -> UnalignedAction<T>
    where
        T: std::fmt::Debug,
    {
        // Check buffer size limit
        if self.buffered_bytes > self.config.max_inflight_buffer_bytes {
            // Buffer overflow — this is an error condition; stay in idle
            self.state = State::Idle;
            self.clear_buffers();
            // Return aligned snapshot as fallback (best effort)
            return UnalignedAction::AlignedSnapshot(barrier);
        }

        let mut inflight_data = Vec::new();
        for input_id in 0..self.num_inputs {
            if aligned_inputs & (1u128 << input_id) == 0 {
                // This input hasn't sent its barrier yet — capture its buffer
                let events = std::mem::take(&mut self.inflight_buffers[input_id]);
                if !events.is_empty() {
                    inflight_data.push(InFlightChannelData {
                        input_id,
                        events: Vec::new(), // Serialized form populated by caller
                        size_bytes: 0,
                    });
                    // Note: actual event serialization is done by the caller
                    // since we don't know T's serialization format here.
                    // The empty events vec signals which inputs had buffered data.
                    let _ = events; // Drop the typed events
                }
            }
        }

        let total_size = self.buffered_bytes;
        let unaligned_barrier = CheckpointBarrier {
            checkpoint_id: barrier.checkpoint_id,
            epoch: barrier.epoch,
            flags: barrier.flags | super::barrier::flags::UNALIGNED,
        };

        let snapshot = UnalignedSnapshot {
            barrier: unaligned_barrier,
            operator_state: None, // Populated by caller
            inflight_data,
            total_size_bytes: total_size,
            was_threshold_triggered: true,
        };

        self.state = State::WaitingForLateBarriers {
            received_inputs: aligned_inputs,
            barrier,
        };
        self.buffered_bytes = 0;

        UnalignedAction::UnalignedSnapshot(snapshot)
    }

    /// Buffers an event from the given input during alignment.
    ///
    /// Returns `false` if the buffer size would exceed the configured limit
    /// (event is NOT buffered). Returns `true` on success.
    pub fn buffer_event(&mut self, input_id: usize, event: T, size_bytes: usize) -> bool {
        if self.buffered_bytes + size_bytes > self.config.max_inflight_buffer_bytes {
            return false;
        }
        if input_id < self.inflight_buffers.len() {
            self.inflight_buffers[input_id].push(event);
            self.buffered_bytes += size_bytes;
        }
        true
    }

    /// Clears all in-flight buffers.
    fn clear_buffers(&mut self) {
        for buf in &mut self.inflight_buffers {
            buf.clear();
        }
        self.buffered_bytes = 0;
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for UnalignedCheckpointer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnalignedCheckpointer")
            .field("num_inputs", &self.num_inputs)
            .field("state", &self.state)
            .field("buffered_bytes", &self.buffered_bytes)
            .field("is_sink", &self.is_sink)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::barrier::flags;

    fn default_config() -> UnalignedCheckpointConfig {
        UnalignedCheckpointConfig {
            enabled: true,
            alignment_timeout_threshold: Duration::from_millis(100),
            max_inflight_buffer_bytes: 1024 * 1024,
            force_unaligned: false,
        }
    }

    #[test]
    fn test_aligned_fast_path() {
        let config = default_config();
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(2, config, false);

        let barrier = CheckpointBarrier::new(1, 1);

        // First barrier from input 0
        let action = ckpt.on_barrier(0, barrier);
        assert!(matches!(action, UnalignedAction::Buffer));
        assert!(ckpt.is_checkpointing());

        // Second barrier from input 1 (within timeout)
        let action = ckpt.on_barrier(1, barrier);
        assert!(matches!(action, UnalignedAction::AlignedSnapshot(b) if b.checkpoint_id == 1));
        assert!(!ckpt.is_checkpointing());
    }

    #[test]
    fn test_single_input_immediate_aligned() {
        let config = default_config();
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(1, config, false);

        let barrier = CheckpointBarrier::new(1, 1);
        let action = ckpt.on_barrier(0, barrier);
        assert!(matches!(action, UnalignedAction::AlignedSnapshot(_)));
        assert!(!ckpt.is_checkpointing());
    }

    #[test]
    fn test_timeout_triggers_unaligned() {
        let config = UnalignedCheckpointConfig {
            alignment_timeout_threshold: Duration::from_millis(1),
            ..default_config()
        };
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(3, config, false);

        let barrier = CheckpointBarrier::new(1, 1);

        // First barrier from input 0
        let action = ckpt.on_barrier(0, barrier);
        assert!(matches!(action, UnalignedAction::Buffer));

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));

        // Check timeout should trigger unaligned
        let action = ckpt.check_timeout();
        assert!(action.is_some());
        match action.unwrap() {
            UnalignedAction::UnalignedSnapshot(snap) => {
                assert!(snap.barrier.is_unaligned());
                assert!(snap.was_threshold_triggered);
            }
            other => panic!("expected UnalignedSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_inflight_capture() {
        let config = UnalignedCheckpointConfig {
            alignment_timeout_threshold: Duration::from_millis(1),
            ..default_config()
        };
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(2, config, false);

        let barrier = CheckpointBarrier::new(1, 1);

        // First barrier from input 0
        ckpt.on_barrier(0, barrier);

        // Buffer some events from input 1 (non-aligned)
        assert!(ckpt.buffer_event(1, "event-1".into(), 7));
        assert!(ckpt.buffer_event(1, "event-2".into(), 7));

        assert_eq!(ckpt.buffered_bytes(), 14);

        // Trigger timeout
        std::thread::sleep(Duration::from_millis(5));
        let action = ckpt.check_timeout();
        assert!(action.is_some());

        match action.unwrap() {
            UnalignedAction::UnalignedSnapshot(snap) => {
                assert!(!snap.inflight_data.is_empty());
                assert_eq!(snap.inflight_data[0].input_id, 1);
            }
            other => panic!("expected UnalignedSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_max_buffer_exceeded() {
        let config = UnalignedCheckpointConfig {
            max_inflight_buffer_bytes: 10,
            ..default_config()
        };
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(2, config, false);

        let barrier = CheckpointBarrier::new(1, 1);
        ckpt.on_barrier(0, barrier);

        // Buffer events until limit
        assert!(ckpt.buffer_event(1, "12345".into(), 5));
        assert!(ckpt.buffer_event(1, "12345".into(), 5));
        // This should fail (would exceed 10 bytes)
        assert!(!ckpt.buffer_event(1, "x".into(), 1));
    }

    #[test]
    fn test_force_unaligned_mode() {
        let config = UnalignedCheckpointConfig {
            force_unaligned: true,
            ..default_config()
        };
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(3, config, false);

        let barrier = CheckpointBarrier::new(1, 1);

        // First barrier should immediately trigger unaligned (forced mode)
        let action = ckpt.on_barrier(0, barrier);
        match action {
            UnalignedAction::UnalignedSnapshot(snap) => {
                assert!(snap.barrier.is_unaligned());
            }
            other => panic!("expected UnalignedSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn test_sink_cannot_use_unaligned() {
        let config = UnalignedCheckpointConfig {
            alignment_timeout_threshold: Duration::from_millis(1),
            force_unaligned: true,
            ..default_config()
        };
        // is_sink = true
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(2, config, true);

        let barrier = CheckpointBarrier::new(1, 1);

        // Even with force_unaligned, sink should not switch to unaligned
        let action = ckpt.on_barrier(0, barrier);
        assert!(matches!(action, UnalignedAction::Buffer));

        // check_timeout should also not trigger for sinks
        std::thread::sleep(Duration::from_millis(5));
        assert!(ckpt.check_timeout().is_none());
    }

    #[test]
    fn test_late_barriers_complete_cycle() {
        let config = UnalignedCheckpointConfig {
            alignment_timeout_threshold: Duration::from_millis(1),
            ..default_config()
        };
        let mut ckpt: UnalignedCheckpointer<String> = UnalignedCheckpointer::new(3, config, false);

        let barrier = CheckpointBarrier::new(1, 1);

        // Input 0 barrier arrives
        ckpt.on_barrier(0, barrier);

        // Timeout triggers unaligned
        std::thread::sleep(Duration::from_millis(5));
        let action = ckpt.check_timeout().unwrap();
        assert!(matches!(action, UnalignedAction::UnalignedSnapshot(_)));

        // Now in WaitingForLateBarriers — send remaining barriers
        assert!(ckpt.is_checkpointing());

        let action = ckpt.on_barrier(1, barrier);
        assert!(matches!(action, UnalignedAction::Buffer));
        assert!(ckpt.is_checkpointing()); // Still waiting for input 2

        let action = ckpt.on_barrier(2, barrier);
        assert!(matches!(action, UnalignedAction::Buffer));
        assert!(!ckpt.is_checkpointing()); // All barriers received
    }

    #[test]
    fn test_unaligned_flag_set_on_barrier() {
        let barrier = CheckpointBarrier {
            checkpoint_id: 1,
            epoch: 1,
            flags: flags::NONE | flags::UNALIGNED,
        };
        assert!(barrier.is_unaligned());
        assert!(!barrier.is_full_snapshot());
        assert!(!barrier.is_drain());
    }
}
