//! `PostgreSQL` CDC source connector metrics.
//!
//! Prometheus-backed counters/gauges for tracking CDC replication performance.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

/// Metrics for the `PostgreSQL` CDC source connector.
///
/// All counters are prometheus-backed and will appear in the scrape
/// output when a shared registry is provided.
#[derive(Debug, Clone)]
pub struct PostgresCdcMetrics {
    /// Total change events received (insert + update + delete).
    pub events_received: IntCounter,

    /// Total bytes received from the WAL stream.
    pub bytes_received: IntCounter,

    /// Total batches produced for downstream.
    pub batches_produced: IntCounter,

    /// Total INSERT operations received.
    pub inserts: IntCounter,

    /// Total UPDATE operations received.
    pub updates: IntCounter,

    /// Total DELETE operations received.
    pub deletes: IntCounter,

    /// Total transactions (commit messages) received.
    pub transactions: IntCounter,

    /// Current confirmed flush LSN (as raw u64).
    pub confirmed_flush_lsn: IntGauge,

    /// Current replication lag in bytes (`write_lsn` - `confirmed_flush_lsn`).
    pub replication_lag_bytes: IntGauge,
}

impl PostgresCdcMetrics {
    /// Creates a new metrics instance with all counters at zero.
    ///
    /// # Panics
    ///
    /// Panics if a built-in metric descriptor is invalid.
    #[must_use]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            events_received: reg.counter(
                "postgres_cdc_events_received_total",
                "Total CDC change events received",
            ),
            bytes_received: reg.counter(
                "postgres_cdc_bytes_received_total",
                "Total bytes from WAL stream",
            ),
            batches_produced: reg.counter(
                "postgres_cdc_batches_produced_total",
                "Total batches produced",
            ),
            inserts: reg.counter("postgres_cdc_inserts_total", "Total INSERT events"),
            updates: reg.counter("postgres_cdc_updates_total", "Total UPDATE events"),
            deletes: reg.counter("postgres_cdc_deletes_total", "Total DELETE events"),
            transactions: reg.counter(
                "postgres_cdc_transactions_total",
                "Total transactions received",
            ),
            confirmed_flush_lsn: reg.gauge(
                "postgres_cdc_confirmed_flush_lsn",
                "Current confirmed flush LSN",
            ),
            replication_lag_bytes: reg.gauge(
                "postgres_cdc_replication_lag_bytes",
                "Replication lag in bytes",
            ),
        }
    }

    /// Records a received INSERT event.
    pub fn record_insert(&self) {
        self.inserts.inc();
        self.events_received.inc();
    }

    /// Records a received UPDATE event.
    pub fn record_update(&self) {
        self.updates.inc();
        self.events_received.inc();
    }

    /// Records a received DELETE event.
    pub fn record_delete(&self) {
        self.deletes.inc();
        self.events_received.inc();
    }

    /// Records a received transaction commit.
    pub fn record_transaction(&self) {
        self.transactions.inc();
    }

    /// Records bytes received from the WAL stream.
    pub fn record_bytes(&self, bytes: u64) {
        self.bytes_received.inc_by(bytes);
    }

    /// Records a batch produced for downstream.
    pub fn record_batch(&self) {
        self.batches_produced.inc();
    }

    /// Updates the confirmed flush LSN.
    pub fn set_confirmed_flush_lsn(&self, lsn: u64) {
        self.confirmed_flush_lsn
            .set(i64::from_ne_bytes(lsn.to_ne_bytes()));
    }

    /// Updates the replication lag in bytes.
    pub fn set_replication_lag_bytes(&self, lag: u64) {
        self.replication_lag_bytes
            .set(i64::from_ne_bytes(lag.to_ne_bytes()));
    }
}

impl Default for PostgresCdcMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests;
