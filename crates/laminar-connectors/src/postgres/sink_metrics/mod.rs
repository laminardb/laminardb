//! `PostgreSQL` sink connector metrics.
//!
//! [`PostgresSinkMetrics`] provides prometheus-backed counters for
//! tracking write statistics.

use prometheus::{IntCounter, Registry};

use crate::prom::reg_or_local;

/// Prometheus-backed counters for `PostgreSQL` sink connector statistics.
#[derive(Debug, Clone)]
pub struct PostgresSinkMetrics {
    /// Total records written to `PostgreSQL`.
    pub records_written: IntCounter,

    /// Total bytes written (estimated from `RecordBatch` sizes).
    pub bytes_written: IntCounter,

    /// Total errors encountered.
    pub errors_total: IntCounter,

    /// Total batches flushed.
    pub batches_flushed: IntCounter,

    /// Total COPY BINARY operations (append mode).
    pub copy_operations: IntCounter,

    /// Total upsert operations (upsert mode).
    pub upsert_operations: IntCounter,

    /// Total changelog deletes applied (Z-set weight -1).
    pub changelog_deletes: IntCounter,
}

impl PostgresSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            records_written: reg.counter(
                "postgres_sink_records_written_total",
                "Total records written to PostgreSQL",
            ),
            bytes_written: reg.counter(
                "postgres_sink_bytes_written_total",
                "Total bytes written to PostgreSQL",
            ),
            errors_total: reg.counter("postgres_sink_errors_total", "Total PostgreSQL sink errors"),
            batches_flushed: reg.counter(
                "postgres_sink_batches_flushed_total",
                "Total batches flushed",
            ),
            copy_operations: reg.counter(
                "postgres_sink_copy_operations_total",
                "Total COPY BINARY operations",
            ),
            upsert_operations: reg.counter(
                "postgres_sink_upsert_operations_total",
                "Total upsert operations",
            ),
            changelog_deletes: reg.counter(
                "postgres_sink_changelog_deletes_total",
                "Total changelog deletes applied",
            ),
        }
    }

    /// Records a successful write of `records` records totaling `bytes`.
    pub fn record_write(&self, records: u64, bytes: u64) {
        self.records_written.inc_by(records);
        self.bytes_written.inc_by(bytes);
    }

    /// Records a successful batch flush.
    pub fn record_flush(&self) {
        self.batches_flushed.inc();
    }

    /// Records a COPY BINARY operation.
    pub fn record_copy(&self) {
        self.copy_operations.inc();
    }

    /// Records an upsert operation.
    pub fn record_upsert(&self) {
        self.upsert_operations.inc();
    }

    /// Records a write or connection error.
    pub fn record_error(&self) {
        self.errors_total.inc();
    }

    /// Records changelog DELETE operations.
    pub fn record_deletes(&self, count: u64) {
        self.changelog_deletes.inc_by(count);
    }
}

impl Default for PostgresSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests;
