//! `MongoDB` connector metrics.
//!
//! Lock-free atomic counters for tracking CDC source and sink performance.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Metrics for the `MongoDB` CDC source connector.
///
/// All counters use relaxed atomic ordering for lock-free access
/// from the async runtime.
#[derive(Debug)]
pub struct MongoDbCdcMetrics {
    /// Total change events received.
    pub events_received: AtomicU64,

    /// Total bytes received from the change stream.
    pub bytes_received: AtomicU64,

    /// Total errors encountered.
    pub errors: AtomicU64,

    /// Total batches produced for downstream.
    pub batches_produced: AtomicU64,

    /// Total INSERT operations received.
    pub inserts: AtomicU64,

    /// Total UPDATE operations received.
    pub updates: AtomicU64,

    /// Total REPLACE operations received.
    pub replaces: AtomicU64,

    /// Total DELETE operations received.
    pub deletes: AtomicU64,

    /// Total lifecycle events (drop/rename/invalidate).
    pub lifecycle_events: AtomicU64,

    /// Total resume token persist operations.
    pub token_persists: AtomicU64,

    /// Total reconnection attempts.
    pub reconnects: AtomicU64,

    /// Total large event fragments received.
    pub large_event_fragments: AtomicU64,

    /// Total large events reassembled.
    pub large_events_reassembled: AtomicU64,
}

impl MongoDbCdcMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            batches_produced: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            replaces: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            lifecycle_events: AtomicU64::new(0),
            token_persists: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            large_event_fragments: AtomicU64::new(0),
            large_events_reassembled: AtomicU64::new(0),
        }
    }

    /// Records a received change event by operation type.
    pub fn record_event(&self, op: &str) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
        match op {
            "I" => {
                self.inserts.fetch_add(1, Ordering::Relaxed);
            }
            "U" => {
                self.updates.fetch_add(1, Ordering::Relaxed);
            }
            "R" => {
                self.replaces.fetch_add(1, Ordering::Relaxed);
            }
            "D" => {
                self.deletes.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.lifecycle_events.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Records bytes received from the change stream.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a batch produced for downstream.
    pub fn record_batch(&self) {
        self.batches_produced.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a resume token persistence operation.
    pub fn record_token_persist(&self) {
        self.token_persists.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a reconnection attempt.
    pub fn record_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a large event fragment received.
    pub fn record_large_event_fragment(&self) {
        self.large_event_fragments.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a large event reassembled.
    pub fn record_large_event_reassembled(&self) {
        self.large_events_reassembled
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.events_received.load(Ordering::Relaxed);
        m.bytes_total = self.bytes_received.load(Ordering::Relaxed);
        m.errors_total = self.errors.load(Ordering::Relaxed);

        m.add_custom("inserts", self.inserts.load(Ordering::Relaxed) as f64);
        m.add_custom("updates", self.updates.load(Ordering::Relaxed) as f64);
        m.add_custom("replaces", self.replaces.load(Ordering::Relaxed) as f64);
        m.add_custom("deletes", self.deletes.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "lifecycle_events",
            self.lifecycle_events.load(Ordering::Relaxed) as f64,
        );
        m.add_custom("reconnects", self.reconnects.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "large_events_reassembled",
            self.large_events_reassembled.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for MongoDbCdcMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics for the `MongoDB` sink connector.
#[derive(Debug)]
pub struct MongoDbSinkMetrics {
    /// Total records written.
    pub records_written: AtomicU64,

    /// Total bytes written.
    pub bytes_written: AtomicU64,

    /// Total errors encountered.
    pub errors: AtomicU64,

    /// Total batches flushed.
    pub batches_flushed: AtomicU64,

    /// Total bulk write operations issued.
    pub bulk_writes: AtomicU64,

    /// Total individual insert operations.
    pub inserts: AtomicU64,

    /// Total upsert operations.
    pub upserts: AtomicU64,

    /// Total delete operations.
    pub deletes: AtomicU64,
}

impl MongoDbSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            batches_flushed: AtomicU64::new(0),
            bulk_writes: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            upserts: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
        }
    }

    /// Records a successful batch flush.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.records_written.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.batches_flushed.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a bulk write operation.
    pub fn record_bulk_write(&self) {
        self.bulk_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records insert operations.
    pub fn record_inserts(&self, count: u64) {
        self.inserts.fetch_add(count, Ordering::Relaxed);
    }

    /// Records upsert operations.
    pub fn record_upserts(&self, count: u64) {
        self.upserts.fetch_add(count, Ordering::Relaxed);
    }

    /// Records delete operations.
    pub fn record_deletes(&self, count: u64) {
        self.deletes.fetch_add(count, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.records_written.load(Ordering::Relaxed);
        m.bytes_total = self.bytes_written.load(Ordering::Relaxed);
        m.errors_total = self.errors.load(Ordering::Relaxed);

        m.add_custom("inserts", self.inserts.load(Ordering::Relaxed) as f64);
        m.add_custom("upserts", self.upserts.load(Ordering::Relaxed) as f64);
        m.add_custom("deletes", self.deletes.load(Ordering::Relaxed) as f64);
        m.add_custom(
            "bulk_writes",
            self.bulk_writes.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "batches_flushed",
            self.batches_flushed.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for MongoDbSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metrics_record_events() {
        let m = MongoDbCdcMetrics::new();
        m.record_event("I");
        m.record_event("I");
        m.record_event("U");
        m.record_event("D");
        m.record_event("DROP");
        m.record_bytes(1024);
        m.record_error();
        m.record_batch();
        m.record_token_persist();
        m.record_reconnect();

        assert_eq!(m.events_received.load(Ordering::Relaxed), 5);
        assert_eq!(m.inserts.load(Ordering::Relaxed), 2);
        assert_eq!(m.updates.load(Ordering::Relaxed), 1);
        assert_eq!(m.deletes.load(Ordering::Relaxed), 1);
        assert_eq!(m.lifecycle_events.load(Ordering::Relaxed), 1);
        assert_eq!(m.bytes_received.load(Ordering::Relaxed), 1024);
        assert_eq!(m.errors.load(Ordering::Relaxed), 1);
        assert_eq!(m.batches_produced.load(Ordering::Relaxed), 1);
        assert_eq!(m.token_persists.load(Ordering::Relaxed), 1);
        assert_eq!(m.reconnects.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_source_metrics_to_connector_metrics() {
        let m = MongoDbCdcMetrics::new();
        m.record_event("I");
        m.record_bytes(512);
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 1);
        assert_eq!(cm.bytes_total, 512);
        assert_eq!(cm.errors_total, 1);
        assert!(!cm.custom.is_empty());
    }

    #[test]
    fn test_sink_metrics_record_flush() {
        let m = MongoDbSinkMetrics::new();
        m.record_flush(100, 5000);
        m.record_bulk_write();
        m.record_inserts(80);
        m.record_upserts(15);
        m.record_deletes(5);
        m.record_error();

        assert_eq!(m.records_written.load(Ordering::Relaxed), 100);
        assert_eq!(m.bytes_written.load(Ordering::Relaxed), 5000);
        assert_eq!(m.batches_flushed.load(Ordering::Relaxed), 1);
        assert_eq!(m.bulk_writes.load(Ordering::Relaxed), 1);
        assert_eq!(m.inserts.load(Ordering::Relaxed), 80);
        assert_eq!(m.upserts.load(Ordering::Relaxed), 15);
        assert_eq!(m.deletes.load(Ordering::Relaxed), 5);
        assert_eq!(m.errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_sink_metrics_to_connector_metrics() {
        let m = MongoDbSinkMetrics::new();
        m.record_flush(50, 2500);
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 50);
        assert_eq!(cm.bytes_total, 2500);
        assert_eq!(cm.errors_total, 1);
    }
}
