//! WebSocket source connector metrics.
//!
//! [`WebSocketSourceMetrics`] provides lock-free atomic counters for
//! tracking WebSocket source statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for WebSocket source connector statistics.
///
/// All counters use `Relaxed` ordering for maximum throughput on the
/// hot path. Snapshot reads via [`to_connector_metrics`](Self::to_connector_metrics)
/// provide a consistent-enough view for monitoring purposes.
#[derive(Debug)]
pub struct WebSocketSourceMetrics {
    /// Total messages received from the WebSocket connection.
    pub messages_received: AtomicU64,
    /// Total messages dropped due to backpressure.
    pub messages_dropped_backpressure: AtomicU64,
    /// Total bytes received (raw payload, before parsing).
    pub bytes_received: AtomicU64,
    /// Total number of reconnection attempts.
    pub reconnect_count: AtomicU64,
    /// Total parse/deserialization errors.
    pub parse_errors: AtomicU64,
    /// Total detected sequence gaps (application-level).
    pub sequence_gaps: AtomicU64,
    /// Current number of connected clients (for server-mode source).
    pub connected_clients: AtomicU64,
}

impl WebSocketSourceMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            messages_dropped_backpressure: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            reconnect_count: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            sequence_gaps: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
        }
    }

    /// Records a successfully received message with the given payload size.
    pub fn record_message(&self, bytes: u64) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a message dropped due to backpressure.
    pub fn record_drop(&self) {
        self.messages_dropped_backpressure
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a reconnection attempt.
    pub fn record_reconnect(&self) {
        self.reconnect_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a parse/deserialization error.
    pub fn record_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a detected sequence gap.
    pub fn record_sequence_gap(&self) {
        self.sequence_gaps.fetch_add(1, Ordering::Relaxed);
    }

    /// Sets the current number of connected clients (server-mode source).
    pub fn set_connected_clients(&self, n: u64) {
        self.connected_clients.store(n, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.messages_received.load(Ordering::Relaxed),
            bytes_total: self.bytes_received.load(Ordering::Relaxed),
            errors_total: self.parse_errors.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "ws.messages_dropped_backpressure",
            self.messages_dropped_backpressure.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.reconnect_count",
            self.reconnect_count.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.sequence_gaps",
            self.sequence_gaps.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.connected_clients",
            self.connected_clients.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for WebSocketSourceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSourceMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_message() {
        let m = WebSocketSourceMetrics::new();
        m.record_message(1024);
        m.record_message(2048);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 2);
        assert_eq!(cm.bytes_total, 3072);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSourceMetrics::new();
        m.record_drop();
        m.record_drop();
        m.record_drop();

        let cm = m.to_connector_metrics();
        let dropped = cm
            .custom
            .iter()
            .find(|(k, _)| k == "ws.messages_dropped_backpressure");
        assert_eq!(dropped.unwrap().1, 3.0);
    }

    #[test]
    fn test_record_reconnect() {
        let m = WebSocketSourceMetrics::new();
        m.record_reconnect();
        m.record_reconnect();

        let cm = m.to_connector_metrics();
        let reconnects = cm.custom.iter().find(|(k, _)| k == "ws.reconnect_count");
        assert_eq!(reconnects.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_parse_error() {
        let m = WebSocketSourceMetrics::new();
        m.record_parse_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 1);
    }

    #[test]
    fn test_record_sequence_gap() {
        let m = WebSocketSourceMetrics::new();
        m.record_sequence_gap();
        m.record_sequence_gap();

        let cm = m.to_connector_metrics();
        let gaps = cm.custom.iter().find(|(k, _)| k == "ws.sequence_gaps");
        assert_eq!(gaps.unwrap().1, 2.0);
    }

    #[test]
    fn test_set_connected_clients() {
        let m = WebSocketSourceMetrics::new();
        m.set_connected_clients(5);

        let cm = m.to_connector_metrics();
        let clients = cm.custom.iter().find(|(k, _)| k == "ws.connected_clients");
        assert_eq!(clients.unwrap().1, 5.0);

        // Verify it overwrites (not accumulates)
        m.set_connected_clients(3);
        let cm2 = m.to_connector_metrics();
        let clients2 = cm2.custom.iter().find(|(k, _)| k == "ws.connected_clients");
        assert_eq!(clients2.unwrap().1, 3.0);
    }

    #[test]
    fn test_default() {
        let m = WebSocketSourceMetrics::default();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
        assert_eq!(cm.custom.len(), 4);
    }

    #[test]
    fn test_custom_metrics_count() {
        let m = WebSocketSourceMetrics::new();
        let cm = m.to_connector_metrics();
        // Should have exactly 4 custom metrics
        assert_eq!(cm.custom.len(), 4);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSourceMetrics::new();
        m.record_message(100);
        m.record_message(200);
        m.record_drop();
        m.record_reconnect();
        m.record_parse_error();
        m.record_sequence_gap();
        m.set_connected_clients(10);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 2);
        assert_eq!(cm.bytes_total, 300);
        assert_eq!(cm.errors_total, 1);
    }
}
