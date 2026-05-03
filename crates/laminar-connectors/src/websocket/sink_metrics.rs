//! WebSocket sink connector metrics.
//!
//! [`WebSocketSinkMetrics`] provides prometheus-backed counters and gauges
//! for tracking WebSocket sink statistics.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::prom::reg_or_local;

/// Prometheus-backed counters/gauges for WebSocket sink connector statistics.
#[derive(Debug, Clone)]
pub struct WebSocketSinkMetrics {
    /// Total messages sent to connected clients.
    pub messages_sent: IntCounter,
    /// Total messages dropped because a client was too slow.
    pub messages_dropped_slow_client: IntCounter,
    /// Total bytes sent (serialized payload).
    pub bytes_sent: IntCounter,
    /// Current number of connected clients.
    pub connected_clients: IntGauge,
    /// Total client disconnection events.
    pub client_disconnects: IntCounter,
    /// Total replay requests received from clients.
    pub replay_requests: IntCounter,
    /// Total clients disconnected due to ping timeout.
    pub ping_timeouts: IntCounter,
}

impl WebSocketSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn new(registry: Option<&Registry>) -> Self {
        let mut local = None;
        let reg = reg_or_local(registry, &mut local);

        Self {
            messages_sent: reg.counter("ws_sink_messages_sent_total", "Total WS messages sent"),
            messages_dropped_slow_client: reg.counter(
                "ws_sink_messages_dropped_slow_client_total",
                "Total WS messages dropped (slow client)",
            ),
            bytes_sent: reg.counter("ws_sink_bytes_sent_total", "Total WS bytes sent"),
            connected_clients: reg
                .gauge("ws_sink_connected_clients", "Current connected WS clients"),
            client_disconnects: reg.counter(
                "ws_sink_client_disconnects_total",
                "Total WS client disconnections",
            ),
            replay_requests: reg
                .counter("ws_sink_replay_requests_total", "Total WS replay requests"),
            ping_timeouts: reg.counter("ws_sink_ping_timeouts_total", "Total WS ping timeouts"),
        }
    }

    /// Records a successfully sent message with the given payload size.
    pub fn record_send(&self, bytes: u64) {
        self.messages_sent.inc();
        self.bytes_sent.inc_by(bytes);
    }

    /// Records a message dropped due to a slow client.
    pub fn record_drop(&self) {
        self.messages_dropped_slow_client.inc();
    }

    /// Records a new client connection.
    pub fn record_connect(&self) {
        self.connected_clients.inc();
    }

    /// Records a client disconnection.
    pub fn record_disconnect(&self) {
        self.client_disconnects.inc();
        // Saturating subtract to avoid underflow on spurious disconnect events.
        // IntGauge can go negative so we clamp manually.
        let current = self.connected_clients.get();
        if current > 0 {
            self.connected_clients.dec();
        }
    }

    /// Records a replay request from a client.
    pub fn record_replay(&self) {
        self.replay_requests.inc();
    }

    /// Records a client disconnected due to ping timeout.
    pub fn record_ping_timeout(&self) {
        self.ping_timeouts.inc();
    }
}

impl Default for WebSocketSinkMetrics {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSinkMetrics::new(None);
        assert_eq!(m.messages_sent.get(), 0);
        assert_eq!(m.bytes_sent.get(), 0);
        assert_eq!(m.messages_dropped_slow_client.get(), 0);
    }

    #[test]
    fn test_record_send() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_send(512);
        m.record_send(1024);

        assert_eq!(m.messages_sent.get(), 2);
        assert_eq!(m.bytes_sent.get(), 1536);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_drop();
        m.record_drop();

        assert_eq!(m.messages_dropped_slow_client.get(), 2);
    }

    #[test]
    fn test_record_connect_disconnect() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_connect();
        m.record_connect();
        m.record_connect();
        m.record_disconnect();

        assert_eq!(m.connected_clients.get(), 2);
        assert_eq!(m.client_disconnects.get(), 1);
    }

    #[test]
    fn test_disconnect_saturates_at_zero() {
        let m = WebSocketSinkMetrics::new(None);
        // Disconnect without any connect should not underflow
        m.record_disconnect();

        assert_eq!(m.connected_clients.get(), 0);
        assert_eq!(m.client_disconnects.get(), 1);
    }

    #[test]
    fn test_record_replay() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_replay();
        m.record_replay();
        m.record_replay();

        assert_eq!(m.replay_requests.get(), 3);
    }

    #[test]
    fn test_default() {
        let m = WebSocketSinkMetrics::default();
        assert_eq!(m.messages_sent.get(), 0);
        assert_eq!(m.bytes_sent.get(), 0);
        assert_eq!(m.messages_dropped_slow_client.get(), 0);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_send(100);
        m.record_send(200);
        m.record_send(300);
        m.record_drop();
        m.record_connect();
        m.record_connect();
        m.record_disconnect();
        m.record_replay();

        assert_eq!(m.messages_sent.get(), 3);
        assert_eq!(m.bytes_sent.get(), 600);
        assert_eq!(m.messages_dropped_slow_client.get(), 1);
        assert_eq!(m.connected_clients.get(), 1);
    }
}
