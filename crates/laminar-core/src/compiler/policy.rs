//! Batching and backpressure policies for the Ring 0 / Ring 1 pipeline bridge.
//!
//! [`BatchPolicy`] controls when the [`BridgeConsumer`](super::pipeline_bridge::BridgeConsumer)
//! flushes accumulated rows into a `RecordBatch`. [`BackpressureStrategy`] determines how the
//! [`PipelineBridge`](super::pipeline_bridge::PipelineBridge) handles a full SPSC queue.

use std::time::Duration;

/// Controls when the bridge consumer flushes accumulated rows into a `RecordBatch`.
///
/// The consumer flushes when **any** of these conditions is met:
/// - The row count reaches [`max_rows`](Self::max_rows).
/// - The time since the first row exceeds [`max_latency`](Self::max_latency).
/// - A watermark message arrives and [`flush_on_watermark`](Self::flush_on_watermark) is `true`.
/// - A checkpoint barrier arrives (always flushes).
/// - An EOF message arrives (always flushes).
#[derive(Debug, Clone)]
pub struct BatchPolicy {
    /// Maximum number of rows before a flush.
    pub max_rows: usize,
    /// Maximum time a row may wait before flushing.
    pub max_latency: Duration,
    /// Whether a watermark advance triggers a flush of pending rows.
    ///
    /// When `true` (the default), rows accumulated before a watermark are flushed
    /// as a batch *before* the watermark is forwarded to Ring 1. This prevents
    /// partial-batch emissions tied to arbitrary batch boundaries (Issue #55).
    pub flush_on_watermark: bool,
}

impl Default for BatchPolicy {
    fn default() -> Self {
        Self {
            max_rows: 1024,
            max_latency: Duration::from_millis(10),
            flush_on_watermark: true,
        }
    }
}

impl BatchPolicy {
    /// Sets the maximum row count before a flush.
    #[must_use]
    pub const fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = max_rows;
        self
    }

    /// Sets the maximum latency before a flush.
    #[must_use]
    pub const fn with_max_latency(mut self, max_latency: Duration) -> Self {
        self.max_latency = max_latency;
        self
    }

    /// Sets whether watermark advances flush pending rows.
    #[must_use]
    pub const fn with_flush_on_watermark(mut self, flush_on_watermark: bool) -> Self {
        self.flush_on_watermark = flush_on_watermark;
        self
    }
}

/// How the bridge producer handles a full SPSC queue.
#[derive(Debug, Clone, Default)]
pub enum BackpressureStrategy {
    /// Drop the newest event and increment a drop counter (best-effort delivery).
    #[default]
    DropNewest,
    /// Signal the upstream source to pause until capacity is available (exactly-once).
    PauseSource,
    /// Spill events to the WAL on disk up to the given byte limit (burst-tolerant).
    SpillToDisk {
        /// Maximum number of bytes to spill before applying backpressure upstream.
        max_spill_bytes: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_policy_defaults() {
        let policy = BatchPolicy::default();
        assert_eq!(policy.max_rows, 1024);
        assert_eq!(policy.max_latency, Duration::from_millis(10));
        assert!(policy.flush_on_watermark);
    }

    #[test]
    fn batch_policy_builder() {
        let policy = BatchPolicy::default()
            .with_max_rows(512)
            .with_max_latency(Duration::from_millis(5))
            .with_flush_on_watermark(false);

        assert_eq!(policy.max_rows, 512);
        assert_eq!(policy.max_latency, Duration::from_millis(5));
        assert!(!policy.flush_on_watermark);
    }

    #[test]
    fn backpressure_strategy_default() {
        let strategy = BackpressureStrategy::default();
        assert!(matches!(strategy, BackpressureStrategy::DropNewest));
    }

    #[test]
    fn backpressure_strategy_spill() {
        let strategy = BackpressureStrategy::SpillToDisk {
            max_spill_bytes: 1024 * 1024,
        };
        match strategy {
            BackpressureStrategy::SpillToDisk { max_spill_bytes } => {
                assert_eq!(max_spill_bytes, 1024 * 1024);
            }
            _ => panic!("expected SpillToDisk"),
        }
    }

    #[test]
    fn batch_policy_debug() {
        let policy = BatchPolicy::default();
        let debug = format!("{policy:?}");
        assert!(debug.contains("BatchPolicy"));
        assert!(debug.contains("max_rows"));
        assert!(debug.contains("1024"));
    }

    #[test]
    fn batch_policy_clone() {
        let original = BatchPolicy::default().with_max_rows(256);
        let cloned = original.clone();
        assert_eq!(cloned.max_rows, 256);
        assert_eq!(cloned.max_latency, original.max_latency);
        assert_eq!(cloned.flush_on_watermark, original.flush_on_watermark);
    }
}
