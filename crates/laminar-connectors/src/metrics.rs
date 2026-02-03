//! Connector metrics types.
//!
//! Provides metrics reporting for connectors:
//! - [`ConnectorMetrics`]: Metrics reported by a connector implementation
//! - [`RuntimeMetrics`]: Metrics tracked by the connector runtime

use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics reported by a connector implementation.
///
/// Connectors return this from their `metrics()` method to expose
/// internal state to the runtime and monitoring systems.
#[derive(Debug, Clone, Default)]
pub struct ConnectorMetrics {
    /// Total number of records processed.
    pub records_total: u64,

    /// Total bytes processed.
    pub bytes_total: u64,

    /// Number of errors encountered.
    pub errors_total: u64,

    /// Current lag (records behind for sources, pending for sinks).
    pub lag: u64,

    /// Additional connector-specific metrics.
    pub custom: Vec<(String, f64)>,
}

impl ConnectorMetrics {
    /// Creates empty metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a custom metric.
    pub fn add_custom(&mut self, name: impl Into<String>, value: f64) {
        self.custom.push((name.into(), value));
    }
}

/// Metrics tracked by the connector runtime.
///
/// These are maintained by the runtime layer wrapping the connector,
/// providing consistent metrics across all connector types.
#[derive(Debug)]
pub struct RuntimeMetrics {
    /// Total records ingested (sources) or written (sinks).
    pub records_total: AtomicU64,

    /// Total bytes processed.
    pub bytes_total: AtomicU64,

    /// Total errors encountered.
    pub errors_total: AtomicU64,

    /// Total number of batches processed.
    pub batches_total: AtomicU64,

    /// Total number of checkpoint commits.
    pub checkpoints_total: AtomicU64,
}

impl RuntimeMetrics {
    /// Creates a new runtime metrics instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_total: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            batches_total: AtomicU64::new(0),
            checkpoints_total: AtomicU64::new(0),
        }
    }

    /// Records that a batch of records was processed.
    pub fn record_batch(&self, record_count: u64, byte_count: u64) {
        self.records_total
            .fetch_add(record_count, Ordering::Relaxed);
        self.bytes_total.fetch_add(byte_count, Ordering::Relaxed);
        self.batches_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a checkpoint commit.
    pub fn record_checkpoint(&self) {
        self.checkpoints_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Returns a snapshot of the current metrics.
    #[must_use]
    pub fn snapshot(&self) -> RuntimeMetricsSnapshot {
        RuntimeMetricsSnapshot {
            records_total: self.records_total.load(Ordering::Relaxed),
            bytes_total: self.bytes_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            batches_total: self.batches_total.load(Ordering::Relaxed),
            checkpoints_total: self.checkpoints_total.load(Ordering::Relaxed),
        }
    }
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of runtime metrics.
#[derive(Debug, Clone, Default)]
pub struct RuntimeMetricsSnapshot {
    /// Total records processed.
    pub records_total: u64,

    /// Total bytes processed.
    pub bytes_total: u64,

    /// Total errors.
    pub errors_total: u64,

    /// Total batches.
    pub batches_total: u64,

    /// Total checkpoints.
    pub checkpoints_total: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connector_metrics() {
        let mut metrics = ConnectorMetrics::new();
        metrics.records_total = 1000;
        metrics.bytes_total = 50_000;
        metrics.add_custom("kafka.lag", 42.0);

        assert_eq!(metrics.records_total, 1000);
        assert_eq!(metrics.custom.len(), 1);
        assert_eq!(metrics.custom[0].0, "kafka.lag");
    }

    #[test]
    fn test_runtime_metrics() {
        let metrics = RuntimeMetrics::new();
        metrics.record_batch(100, 5000);
        metrics.record_batch(200, 10000);
        metrics.record_error();
        metrics.record_checkpoint();

        let snap = metrics.snapshot();
        assert_eq!(snap.records_total, 300);
        assert_eq!(snap.bytes_total, 15000);
        assert_eq!(snap.errors_total, 1);
        assert_eq!(snap.batches_total, 2);
        assert_eq!(snap.checkpoints_total, 1);
    }
}
