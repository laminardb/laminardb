//! Unaligned checkpoint configuration.
//!
//! Provides [`UnalignedCheckpointConfig`] for opting into unaligned checkpoints
//! when barrier alignment is too slow under backpressure. Re-exported by
//! `laminar_db::checkpoint_coordinator`.
//!
//! The state machine implementation is not yet integrated — only configuration
//! is available. When the protocol is implemented it will follow the
//! Flink 1.11+ design:
//!
//! 1. First barrier arrives -> start alignment timer
//! 2. All barriers arrive within timeout -> normal aligned snapshot
//! 3. Timeout fires -> capture in-flight events -> unaligned snapshot

use std::time::Duration;

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
            enabled: false,
            alignment_timeout_threshold: Duration::from_secs(10),
            max_inflight_buffer_bytes: 256 * 1024 * 1024,
            force_unaligned: false,
        }
    }
}
