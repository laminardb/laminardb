//! `WebSocket` source connector metrics.
//!
//! [`WebSocketSourceMetrics`] provides prometheus-backed counters and gauges
//! for tracking WebSocket source statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

/// Prometheus-backed counters/gauges for WebSocket source connector statistics.
#[derive(Debug, Clone)]
pub struct WebSocketSourceMetrics {
    /// Total messages received from the WebSocket connection.
    pub messages_received: IntCounter,
    /// Total messages dropped due to backpressure.
    pub messages_dropped_backpressure: IntCounter,
    /// Total bytes received (raw payload, before parsing).
    pub bytes_received: IntCounter,
    /// Total number of reconnection attempts.
    pub reconnect_count: IntCounter,
    /// Total parse/deserialization errors.
    pub parse_errors: IntCounter,
    /// Total detected sequence gaps (application-level).
    pub sequence_gaps: IntCounter,
    /// Current number of connected clients (for server-mode source).
    pub connected_clients: IntGauge,
}

impl WebSocketSourceMetrics {
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

        let messages_received = IntCounter::new(
            "ws_source_messages_received_total",
            "Total WS messages received",
        )
        .unwrap();
        let messages_dropped_backpressure = IntCounter::new(
            "ws_source_messages_dropped_backpressure_total",
            "Total WS messages dropped (backpressure)",
        )
        .unwrap();
        let bytes_received =
            IntCounter::new("ws_source_bytes_received_total", "Total WS bytes received").unwrap();
        let reconnect_count = IntCounter::new(
            "ws_source_reconnect_total",
            "Total WS reconnection attempts",
        )
        .unwrap();
        let parse_errors = IntCounter::new(
            "ws_source_parse_errors_total",
            "Total WS parse/deserialization errors",
        )
        .unwrap();
        let sequence_gaps = IntCounter::new(
            "ws_source_sequence_gaps_total",
            "Total WS sequence gaps detected",
        )
        .unwrap();
        let connected_clients = IntGauge::new(
            "ws_source_connected_clients",
            "Current connected WS clients (server mode)",
        )
        .unwrap();

        let _ = reg.register(Box::new(messages_received.clone()));
        let _ = reg.register(Box::new(messages_dropped_backpressure.clone()));
        let _ = reg.register(Box::new(bytes_received.clone()));
        let _ = reg.register(Box::new(reconnect_count.clone()));
        let _ = reg.register(Box::new(parse_errors.clone()));
        let _ = reg.register(Box::new(sequence_gaps.clone()));
        let _ = reg.register(Box::new(connected_clients.clone()));

        Self {
            messages_received,
            messages_dropped_backpressure,
            bytes_received,
            reconnect_count,
            parse_errors,
            sequence_gaps,
            connected_clients,
        }
    }

    /// Records a successfully received message with the given payload size.
    pub fn record_message(&self, bytes: u64) {
        self.messages_received.inc();
        self.bytes_received.inc_by(bytes);
    }

    /// Records a message dropped due to backpressure.
    pub fn record_drop(&self) {
        self.messages_dropped_backpressure.inc();
    }

    /// Records a reconnection attempt.
    pub fn record_reconnect(&self) {
        self.reconnect_count.inc();
    }

    /// Records a parse/deserialization error.
    pub fn record_parse_error(&self) {
        self.parse_errors.inc();
    }

    /// Records a detected sequence gap.
    pub fn record_sequence_gap(&self) {
        self.sequence_gaps.inc();
    }

    /// Sets the current number of connected clients (server-mode source).
    #[allow(clippy::cast_possible_wrap)]
    pub fn set_connected_clients(&self, n: u64) {
        self.connected_clients.set(n as i64);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.messages_received.get(),
            bytes_total: self.bytes_received.get(),
            errors_total: self.parse_errors.get(),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "ws.messages_dropped_backpressure",
            self.messages_dropped_backpressure.get() as f64,
        );
        m.add_custom("ws.reconnect_count", self.reconnect_count.get() as f64);
        m.add_custom("ws.sequence_gaps", self.sequence_gaps.get() as f64);
        m.add_custom("ws.connected_clients", self.connected_clients.get() as f64);
        m
    }
}

impl Default for WebSocketSourceMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSourceMetrics::new(None);
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_message() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_message(1024);
        m.record_message(2048);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 2);
        assert_eq!(cm.bytes_total, 3072);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSourceMetrics::new(None);
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
        let m = WebSocketSourceMetrics::new(None);
        m.record_reconnect();
        m.record_reconnect();

        let cm = m.to_connector_metrics();
        let reconnects = cm.custom.iter().find(|(k, _)| k == "ws.reconnect_count");
        assert_eq!(reconnects.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_parse_error() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_parse_error();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 1);
    }

    #[test]
    fn test_record_sequence_gap() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_sequence_gap();
        m.record_sequence_gap();

        let cm = m.to_connector_metrics();
        let gaps = cm.custom.iter().find(|(k, _)| k == "ws.sequence_gaps");
        assert_eq!(gaps.unwrap().1, 2.0);
    }

    #[test]
    fn test_set_connected_clients() {
        let m = WebSocketSourceMetrics::new(None);
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
        let m = WebSocketSourceMetrics::new(None);
        let cm = m.to_connector_metrics();
        // Should have exactly 4 custom metrics
        assert_eq!(cm.custom.len(), 4);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSourceMetrics::new(None);
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
