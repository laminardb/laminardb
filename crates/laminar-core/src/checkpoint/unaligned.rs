//! Unaligned checkpoint protocol with timeout-based fallback.
//!
//! **Future feature:** This module provides configuration and data types for
//! unaligned checkpoints. The state machine implementation will be integrated in
//! Phase 4 of the checkpoint remediation plan. The [`UnalignedCheckpointConfig`]
//! is re-exported by `laminar_db::checkpoint_coordinator` for configuration.
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

use std::time::Duration;

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
