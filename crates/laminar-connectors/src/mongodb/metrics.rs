//! `MongoDB` connector metrics.
//!
//! Prometheus-backed counters for tracking CDC source and sink performance.

use prometheus::{IntCounter, Registry};

use crate::metrics::ConnectorMetrics;

/// Metrics for the `MongoDB` CDC source connector.
#[derive(Debug, Clone)]
pub struct MongoDbCdcMetrics {
    /// Total change events received.
    pub events_received: IntCounter,
    /// Total bytes received from the change stream.
    pub bytes_received: IntCounter,
    /// Total errors encountered.
    pub errors: IntCounter,
    /// Total batches produced for downstream.
    pub batches_produced: IntCounter,
    /// Total INSERT operations received.
    pub inserts: IntCounter,
    /// Total UPDATE operations received.
    pub updates: IntCounter,
    /// Total REPLACE operations received.
    pub replaces: IntCounter,
    /// Total DELETE operations received.
    pub deletes: IntCounter,
    /// Total lifecycle events (drop/rename/invalidate).
    pub lifecycle_events: IntCounter,
    /// Total resume token persist operations.
    pub token_persists: IntCounter,
    /// Total reconnection attempts.
    pub reconnects: IntCounter,
    /// Total large event fragments received.
    pub large_event_fragments: IntCounter,
    /// Total large events reassembled.
    pub large_events_reassembled: IntCounter,
}

impl MongoDbCdcMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let local;
        let reg = if let Some(r) = registry {
            r
        } else {
            local = Registry::new();
            &local
        };

        let events_received = IntCounter::new(
            "mongodb_cdc_events_received_total",
            "Total MongoDB CDC events received",
        )
        .unwrap();
        let bytes_received = IntCounter::new(
            "mongodb_cdc_bytes_received_total",
            "Total bytes from change stream",
        )
        .unwrap();
        let errors =
            IntCounter::new("mongodb_cdc_errors_total", "Total MongoDB CDC errors").unwrap();
        let batches_produced = IntCounter::new(
            "mongodb_cdc_batches_produced_total",
            "Total batches produced",
        )
        .unwrap();
        let inserts = IntCounter::new("mongodb_cdc_inserts_total", "Total INSERT events").unwrap();
        let updates = IntCounter::new("mongodb_cdc_updates_total", "Total UPDATE events").unwrap();
        let replaces =
            IntCounter::new("mongodb_cdc_replaces_total", "Total REPLACE events").unwrap();
        let deletes = IntCounter::new("mongodb_cdc_deletes_total", "Total DELETE events").unwrap();
        let lifecycle_events = IntCounter::new(
            "mongodb_cdc_lifecycle_events_total",
            "Total lifecycle events",
        )
        .unwrap();
        let token_persists = IntCounter::new(
            "mongodb_cdc_token_persists_total",
            "Total resume token persist ops",
        )
        .unwrap();
        let reconnects = IntCounter::new(
            "mongodb_cdc_reconnects_total",
            "Total reconnection attempts",
        )
        .unwrap();
        let large_event_fragments = IntCounter::new(
            "mongodb_cdc_large_event_fragments_total",
            "Total large event fragments",
        )
        .unwrap();
        let large_events_reassembled = IntCounter::new(
            "mongodb_cdc_large_events_reassembled_total",
            "Total large events reassembled",
        )
        .unwrap();

        let _ = reg.register(Box::new(events_received.clone()));
        let _ = reg.register(Box::new(bytes_received.clone()));
        let _ = reg.register(Box::new(errors.clone()));
        let _ = reg.register(Box::new(batches_produced.clone()));
        let _ = reg.register(Box::new(inserts.clone()));
        let _ = reg.register(Box::new(updates.clone()));
        let _ = reg.register(Box::new(replaces.clone()));
        let _ = reg.register(Box::new(deletes.clone()));
        let _ = reg.register(Box::new(lifecycle_events.clone()));
        let _ = reg.register(Box::new(token_persists.clone()));
        let _ = reg.register(Box::new(reconnects.clone()));
        let _ = reg.register(Box::new(large_event_fragments.clone()));
        let _ = reg.register(Box::new(large_events_reassembled.clone()));

        Self {
            events_received,
            bytes_received,
            errors,
            batches_produced,
            inserts,
            updates,
            replaces,
            deletes,
            lifecycle_events,
            token_persists,
            reconnects,
            large_event_fragments,
            large_events_reassembled,
        }
    }

    /// Records a received change event by operation type.
    pub fn record_event(&self, op: &str) {
        self.events_received.inc();
        match op {
            "I" => self.inserts.inc(),
            "U" => self.updates.inc(),
            "R" => self.replaces.inc(),
            "D" => self.deletes.inc(),
            _ => self.lifecycle_events.inc(),
        }
    }

    /// Records bytes received from the change stream.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_received.inc_by(bytes);
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Records a batch produced for downstream.
    pub fn record_batch(&self) {
        self.batches_produced.inc();
    }

    /// Records a resume token persistence operation.
    pub fn record_token_persist(&self) {
        self.token_persists.inc();
    }

    /// Records a reconnection attempt.
    pub fn record_reconnect(&self) {
        self.reconnects.inc();
    }

    /// Records a large event fragment received.
    pub fn record_large_event_fragment(&self) {
        self.large_event_fragments.inc();
    }

    /// Records a large event reassembled.
    pub fn record_large_event_reassembled(&self) {
        self.large_events_reassembled.inc();
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.events_received.get();
        m.bytes_total = self.bytes_received.get();
        m.errors_total = self.errors.get();

        m.add_custom("inserts", self.inserts.get() as f64);
        m.add_custom("updates", self.updates.get() as f64);
        m.add_custom("replaces", self.replaces.get() as f64);
        m.add_custom("deletes", self.deletes.get() as f64);
        m.add_custom("lifecycle_events", self.lifecycle_events.get() as f64);
        m.add_custom("reconnects", self.reconnects.get() as f64);
        m.add_custom(
            "large_events_reassembled",
            self.large_events_reassembled.get() as f64,
        );
        m
    }
}

impl Default for MongoDbCdcMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Metrics for the `MongoDB` sink connector.
#[derive(Debug, Clone)]
pub struct MongoDbSinkMetrics {
    /// Total records written.
    pub records_written: IntCounter,
    /// Total bytes written.
    pub bytes_written: IntCounter,
    /// Total errors encountered.
    pub errors: IntCounter,
    /// Total batches flushed.
    pub batches_flushed: IntCounter,
    /// Total bulk write operations issued.
    pub bulk_writes: IntCounter,
    /// Total individual insert operations.
    pub inserts: IntCounter,
    /// Total upsert operations.
    pub upserts: IntCounter,
    /// Total delete operations.
    pub deletes: IntCounter,
}

impl MongoDbSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let local;
        let reg = if let Some(r) = registry {
            r
        } else {
            local = Registry::new();
            &local
        };

        let records_written = IntCounter::new(
            "mongodb_sink_records_written_total",
            "Total MongoDB sink records written",
        )
        .unwrap();
        let bytes_written = IntCounter::new(
            "mongodb_sink_bytes_written_total",
            "Total MongoDB sink bytes written",
        )
        .unwrap();
        let errors =
            IntCounter::new("mongodb_sink_errors_total", "Total MongoDB sink errors").unwrap();
        let batches_flushed = IntCounter::new(
            "mongodb_sink_batches_flushed_total",
            "Total batches flushed",
        )
        .unwrap();
        let bulk_writes = IntCounter::new(
            "mongodb_sink_bulk_writes_total",
            "Total bulk write operations",
        )
        .unwrap();
        let inserts =
            IntCounter::new("mongodb_sink_inserts_total", "Total insert operations").unwrap();
        let upserts =
            IntCounter::new("mongodb_sink_upserts_total", "Total upsert operations").unwrap();
        let deletes =
            IntCounter::new("mongodb_sink_deletes_total", "Total delete operations").unwrap();

        let _ = reg.register(Box::new(records_written.clone()));
        let _ = reg.register(Box::new(bytes_written.clone()));
        let _ = reg.register(Box::new(errors.clone()));
        let _ = reg.register(Box::new(batches_flushed.clone()));
        let _ = reg.register(Box::new(bulk_writes.clone()));
        let _ = reg.register(Box::new(inserts.clone()));
        let _ = reg.register(Box::new(upserts.clone()));
        let _ = reg.register(Box::new(deletes.clone()));

        Self {
            records_written,
            bytes_written,
            errors,
            batches_flushed,
            bulk_writes,
            inserts,
            upserts,
            deletes,
        }
    }

    /// Records a successful batch flush.
    pub fn record_flush(&self, records: u64, bytes: u64) {
        self.records_written.inc_by(records);
        self.bytes_written.inc_by(bytes);
        self.batches_flushed.inc();
    }

    /// Records a bulk write operation.
    pub fn record_bulk_write(&self) {
        self.bulk_writes.inc();
    }

    /// Records an error.
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Records insert operations.
    pub fn record_inserts(&self, count: u64) {
        self.inserts.inc_by(count);
    }

    /// Records upsert operations.
    pub fn record_upserts(&self, count: u64) {
        self.upserts.inc_by(count);
    }

    /// Records delete operations.
    pub fn record_deletes(&self, count: u64) {
        self.deletes.inc_by(count);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics::new();
        m.records_total = self.records_written.get();
        m.bytes_total = self.bytes_written.get();
        m.errors_total = self.errors.get();

        m.add_custom("inserts", self.inserts.get() as f64);
        m.add_custom("upserts", self.upserts.get() as f64);
        m.add_custom("deletes", self.deletes.get() as f64);
        m.add_custom("bulk_writes", self.bulk_writes.get() as f64);
        m.add_custom("batches_flushed", self.batches_flushed.get() as f64);
        m
    }
}

impl Default for MongoDbSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_metrics_record_events() {
        let m = MongoDbCdcMetrics::new(None);
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

        assert_eq!(m.events_received.get(), 5);
        assert_eq!(m.inserts.get(), 2);
        assert_eq!(m.updates.get(), 1);
        assert_eq!(m.deletes.get(), 1);
        assert_eq!(m.lifecycle_events.get(), 1);
        assert_eq!(m.bytes_received.get(), 1024);
        assert_eq!(m.errors.get(), 1);
        assert_eq!(m.batches_produced.get(), 1);
        assert_eq!(m.token_persists.get(), 1);
        assert_eq!(m.reconnects.get(), 1);
    }

    #[test]
    fn test_source_metrics_to_connector_metrics() {
        let m = MongoDbCdcMetrics::new(None);
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
        let m = MongoDbSinkMetrics::new(None);
        m.record_flush(100, 5000);
        m.record_bulk_write();
        m.record_inserts(80);
        m.record_upserts(15);
        m.record_deletes(5);
        m.record_error();

        assert_eq!(m.records_written.get(), 100);
        assert_eq!(m.bytes_written.get(), 5000);
        assert_eq!(m.batches_flushed.get(), 1);
        assert_eq!(m.bulk_writes.get(), 1);
        assert_eq!(m.inserts.get(), 80);
        assert_eq!(m.upserts.get(), 15);
        assert_eq!(m.deletes.get(), 5);
        assert_eq!(m.errors.get(), 1);
    }

    #[test]
    fn test_sink_metrics_to_connector_metrics() {
        let m = MongoDbSinkMetrics::new(None);
        m.record_flush(50, 2500);
        m.record_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 50);
        assert_eq!(cm.bytes_total, 2500);
        assert_eq!(cm.errors_total, 1);
    }
}
