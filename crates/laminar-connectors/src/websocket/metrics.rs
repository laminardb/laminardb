//! `WebSocket` source connector metrics.
//!
//! [`WebSocketSourceMetrics`] provides prometheus-backed counters and gauges
//! for tracking WebSocket source statistics.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

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
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            messages_received: reg.counter(
                "ws_source_messages_received_total",
                "Total WS messages received",
            ),
            messages_dropped_backpressure: reg.counter(
                "ws_source_messages_dropped_backpressure_total",
                "Total WS messages dropped (backpressure)",
            ),
            bytes_received: reg
                .counter("ws_source_bytes_received_total", "Total WS bytes received"),
            reconnect_count: reg.counter(
                "ws_source_reconnect_total",
                "Total WS reconnection attempts",
            ),
            parse_errors: reg.counter(
                "ws_source_parse_errors_total",
                "Total WS parse/deserialization errors",
            ),
            sequence_gaps: reg.counter(
                "ws_source_sequence_gaps_total",
                "Total WS sequence gaps detected",
            ),
            connected_clients: reg.gauge(
                "ws_source_connected_clients",
                "Current connected WS clients (server mode)",
            ),
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
        assert_eq!(m.messages_received.get(), 0);
        assert_eq!(m.bytes_received.get(), 0);
        assert_eq!(m.parse_errors.get(), 0);
    }

    #[test]
    fn test_record_message() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_message(1024);
        m.record_message(2048);

        assert_eq!(m.messages_received.get(), 2);
        assert_eq!(m.bytes_received.get(), 3072);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_drop();
        m.record_drop();
        m.record_drop();

        assert_eq!(m.messages_dropped_backpressure.get(), 3);
    }

    #[test]
    fn test_record_reconnect() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_reconnect();
        m.record_reconnect();

        assert_eq!(m.reconnect_count.get(), 2);
    }

    #[test]
    fn test_record_parse_error() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_parse_error();

        assert_eq!(m.parse_errors.get(), 1);
    }

    #[test]
    fn test_record_sequence_gap() {
        let m = WebSocketSourceMetrics::new(None);
        m.record_sequence_gap();
        m.record_sequence_gap();

        assert_eq!(m.sequence_gaps.get(), 2);
    }

    #[test]
    fn test_set_connected_clients() {
        let m = WebSocketSourceMetrics::new(None);
        m.set_connected_clients(5);

        assert_eq!(m.connected_clients.get(), 5);

        // Verify it overwrites (not accumulates)
        m.set_connected_clients(3);
        assert_eq!(m.connected_clients.get(), 3);
    }

    #[test]
    fn test_default() {
        let m = WebSocketSourceMetrics::default();
        assert_eq!(m.messages_received.get(), 0);
        assert_eq!(m.bytes_received.get(), 0);
        assert_eq!(m.parse_errors.get(), 0);
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

        assert_eq!(m.messages_received.get(), 2);
        assert_eq!(m.bytes_received.get(), 300);
        assert_eq!(m.parse_errors.get(), 1);
    }
}
