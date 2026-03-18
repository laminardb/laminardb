//! Unaligned checkpoint protocol with timeout-based fallback.
//!
//! When barrier alignment takes too long (due to backpressure on slow inputs),
//! the checkpoint can fall back to an unaligned snapshot that captures in-flight
//! data from channels. This trades larger checkpoint size for faster completion.
//!
//! ## Protocol (Flink 1.11+ style)
//!
//! 1. First barrier arrives -> start alignment timer
//! 2. If all barriers arrive within timeout -> normal aligned snapshot
//! 3. If timeout fires -> capture in-flight events from non-aligned inputs ->
//!    unaligned snapshot
//! 4. Late barriers from non-aligned inputs arrive -> transition to idle
//!
//! ## Constraints
//!
//! - Sink operators must NOT use unaligned mode (Flink 2.0 finding:
//!   committables must be at the sink on commit)
//! - In-flight buffer size is bounded to prevent OOM

use std::time::{Duration, Instant};

use rustc_hash::FxHashSet;

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

/// Actions for the unaligned checkpoint protocol.
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

// ---------------------------------------------------------------------------
// Unaligned Barrier Tracker — state machine for the streaming coordinator
// ---------------------------------------------------------------------------

/// Phase of the unaligned barrier tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackerPhase {
    /// No checkpoint in progress.
    Idle,
    /// Waiting for all sources to deliver their barrier (aligned attempt).
    Aligning,
    /// Alignment timed out; switched to unaligned mode.
    Unaligned,
}

/// Events emitted by [`UnalignedBarrierTracker`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerEvent {
    /// A barrier is now pending and alignment has started.
    Pending,
    /// All barriers arrived within the timeout (aligned checkpoint).
    Aligned,
    /// Alignment timed out; switched to unaligned mode.
    SwitchedToUnaligned {
        /// Source indices that had NOT yet delivered a barrier.
        missing_sources: Vec<usize>,
    },
    /// A late barrier arrived after switching to unaligned mode.
    /// Contains the source index.
    LateBarrier(usize),
    /// All barriers (including late) have arrived after unaligned switch.
    UnalignedComplete,
}

/// Tracks barrier alignment across sources and manages the aligned -> unaligned
/// fallback transition.
///
/// The coordinator creates one tracker per checkpoint cycle. The tracker is
/// purely synchronous (no async, no I/O) and emits [`TrackerEvent`]s that the
/// coordinator interprets.
#[derive(Debug)]
pub struct UnalignedBarrierTracker {
    config: UnalignedCheckpointConfig,
    phase: TrackerPhase,
    /// Total number of sources participating in this checkpoint.
    sources_total: usize,
    /// Source indices that have delivered their barrier.
    aligned_sources: FxHashSet<usize>,
    /// Checkpoint ID being tracked.
    checkpoint_id: u64,
    /// When alignment began.
    started_at: Option<Instant>,
}

impl UnalignedBarrierTracker {
    /// Create a new tracker with the given configuration.
    #[must_use]
    pub fn new(config: UnalignedCheckpointConfig) -> Self {
        Self {
            config,
            phase: TrackerPhase::Idle,
            sources_total: 0,
            aligned_sources: FxHashSet::default(),
            checkpoint_id: 0,
            started_at: None,
        }
    }

    /// Current phase.
    #[must_use]
    pub fn phase(&self) -> TrackerPhase {
        self.phase
    }

    /// Whether unaligned checkpoints are enabled in the configuration.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Begin tracking a new checkpoint barrier.
    ///
    /// Returns [`TrackerEvent::Pending`]. If `force_unaligned` is set in the
    /// config, immediately transitions to [`TrackerPhase::Unaligned`] and
    /// returns [`TrackerEvent::SwitchedToUnaligned`] with all sources missing.
    pub fn begin(&mut self, checkpoint_id: u64, sources_total: usize) -> Vec<TrackerEvent> {
        self.checkpoint_id = checkpoint_id;
        self.sources_total = sources_total;
        self.aligned_sources.clear();
        self.started_at = Some(Instant::now());
        self.phase = TrackerPhase::Aligning;

        let mut events = vec![TrackerEvent::Pending];

        if self.config.force_unaligned {
            self.phase = TrackerPhase::Unaligned;
            let missing: Vec<usize> = (0..sources_total).collect();
            events.push(TrackerEvent::SwitchedToUnaligned {
                missing_sources: missing,
            });
        }

        events
    }

    /// Record a barrier from the given source index.
    ///
    /// Returns events describing the state transition (if any).
    pub fn barrier_received(&mut self, source_idx: usize) -> Vec<TrackerEvent> {
        let mut events = Vec::new();

        if self.phase == TrackerPhase::Idle {
            return events;
        }

        self.aligned_sources.insert(source_idx);

        match self.phase {
            TrackerPhase::Aligning => {
                if self.aligned_sources.len() >= self.sources_total {
                    self.phase = TrackerPhase::Idle;
                    self.started_at = None;
                    events.push(TrackerEvent::Aligned);
                }
            }
            TrackerPhase::Unaligned => {
                events.push(TrackerEvent::LateBarrier(source_idx));
                if self.aligned_sources.len() >= self.sources_total {
                    self.phase = TrackerPhase::Idle;
                    self.started_at = None;
                    events.push(TrackerEvent::UnalignedComplete);
                }
            }
            TrackerPhase::Idle => {}
        }

        events
    }

    /// Check whether the alignment timeout has fired.
    ///
    /// Should be called periodically by the coordinator. If the timeout has
    /// elapsed while in `Aligning` phase, transitions to `Unaligned` and
    /// returns the [`TrackerEvent::SwitchedToUnaligned`] event.
    pub fn check_timeout(&mut self) -> Option<TrackerEvent> {
        if self.phase != TrackerPhase::Aligning {
            return None;
        }

        let started = self.started_at?;
        if started.elapsed() < self.config.alignment_timeout_threshold {
            return None;
        }

        self.phase = TrackerPhase::Unaligned;
        let missing: Vec<usize> = (0..self.sources_total)
            .filter(|idx| !self.aligned_sources.contains(idx))
            .collect();

        Some(TrackerEvent::SwitchedToUnaligned {
            missing_sources: missing,
        })
    }

    /// Cancel the current tracking (e.g., on hard timeout from the coordinator).
    pub fn cancel(&mut self) {
        self.phase = TrackerPhase::Idle;
        self.aligned_sources.clear();
        self.started_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> UnalignedCheckpointConfig {
        UnalignedCheckpointConfig {
            enabled: true,
            alignment_timeout_threshold: Duration::from_millis(100),
            max_inflight_buffer_bytes: 1024,
            force_unaligned: false,
        }
    }

    #[test]
    fn test_aligned_checkpoint() {
        let mut tracker = UnalignedBarrierTracker::new(default_config());

        let events = tracker.begin(1, 3);
        assert_eq!(events, vec![TrackerEvent::Pending]);
        assert_eq!(tracker.phase(), TrackerPhase::Aligning);

        assert!(tracker.barrier_received(0).is_empty());
        assert!(tracker.barrier_received(1).is_empty());

        let events = tracker.barrier_received(2);
        assert_eq!(events, vec![TrackerEvent::Aligned]);
        assert_eq!(tracker.phase(), TrackerPhase::Idle);
    }

    #[test]
    fn test_unaligned_fallback_on_timeout() {
        let config = UnalignedCheckpointConfig {
            alignment_timeout_threshold: Duration::from_millis(0),
            ..default_config()
        };
        let mut tracker = UnalignedBarrierTracker::new(config);

        tracker.begin(1, 3);
        // Source 0 arrives in time.
        tracker.barrier_received(0);

        // Simulate timeout by using zero-duration threshold.
        // A small sleep ensures elapsed > 0.
        std::thread::sleep(Duration::from_millis(1));

        let event = tracker.check_timeout();
        assert!(event.is_some());
        let event = event.unwrap();
        match &event {
            TrackerEvent::SwitchedToUnaligned { missing_sources } => {
                assert_eq!(missing_sources.len(), 2);
                assert!(missing_sources.contains(&1));
                assert!(missing_sources.contains(&2));
            }
            other => panic!("expected SwitchedToUnaligned, got {other:?}"),
        }
        assert_eq!(tracker.phase(), TrackerPhase::Unaligned);

        // Late barriers.
        let events = tracker.barrier_received(1);
        assert_eq!(events, vec![TrackerEvent::LateBarrier(1)]);

        let events = tracker.barrier_received(2);
        assert_eq!(
            events,
            vec![
                TrackerEvent::LateBarrier(2),
                TrackerEvent::UnalignedComplete
            ]
        );
        assert_eq!(tracker.phase(), TrackerPhase::Idle);
    }

    #[test]
    fn test_force_unaligned() {
        let config = UnalignedCheckpointConfig {
            force_unaligned: true,
            ..default_config()
        };
        let mut tracker = UnalignedBarrierTracker::new(config);

        let events = tracker.begin(1, 2);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], TrackerEvent::Pending);
        match &events[1] {
            TrackerEvent::SwitchedToUnaligned { missing_sources } => {
                assert_eq!(missing_sources, &[0, 1]);
            }
            other => panic!("expected SwitchedToUnaligned, got {other:?}"),
        }
        assert_eq!(tracker.phase(), TrackerPhase::Unaligned);
    }

    #[test]
    fn test_cancel() {
        let mut tracker = UnalignedBarrierTracker::new(default_config());
        tracker.begin(1, 2);
        assert_eq!(tracker.phase(), TrackerPhase::Aligning);

        tracker.cancel();
        assert_eq!(tracker.phase(), TrackerPhase::Idle);
    }

    #[test]
    fn test_idle_barrier_ignored() {
        let mut tracker = UnalignedBarrierTracker::new(default_config());
        // No begin() — tracker is idle.
        let events = tracker.barrier_received(0);
        assert!(events.is_empty());
    }

    #[test]
    fn test_check_timeout_while_idle() {
        let mut tracker = UnalignedBarrierTracker::new(default_config());
        assert!(tracker.check_timeout().is_none());
    }
}
