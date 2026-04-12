//! WebSocket sink connector metrics.
//!
//! [`WebSocketSinkMetrics`] provides prometheus-backed counters and gauges
//! for tracking WebSocket sink statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use prometheus::{IntCounter, IntGauge, Registry};

use crate::metrics::ConnectorMetrics;

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
        let local;
        let reg = if let Some(r) = registry {
            r
        } else {
            local = Registry::new();
            &local
        };

        let messages_sent =
            IntCounter::new("ws_sink_messages_sent_total", "Total WS messages sent").unwrap();
        let messages_dropped_slow_client = IntCounter::new(
            "ws_sink_messages_dropped_slow_client_total",
            "Total WS messages dropped (slow client)",
        )
        .unwrap();
        let bytes_sent =
            IntCounter::new("ws_sink_bytes_sent_total", "Total WS bytes sent").unwrap();
        let connected_clients =
            IntGauge::new("ws_sink_connected_clients", "Current connected WS clients").unwrap();
        let client_disconnects = IntCounter::new(
            "ws_sink_client_disconnects_total",
            "Total WS client disconnections",
        )
        .unwrap();
        let replay_requests =
            IntCounter::new("ws_sink_replay_requests_total", "Total WS replay requests").unwrap();
        let ping_timeouts =
            IntCounter::new("ws_sink_ping_timeouts_total", "Total WS ping timeouts").unwrap();

        let _ = reg.register(Box::new(messages_sent.clone()));
        let _ = reg.register(Box::new(messages_dropped_slow_client.clone()));
        let _ = reg.register(Box::new(bytes_sent.clone()));
        let _ = reg.register(Box::new(connected_clients.clone()));
        let _ = reg.register(Box::new(client_disconnects.clone()));
        let _ = reg.register(Box::new(replay_requests.clone()));
        let _ = reg.register(Box::new(ping_timeouts.clone()));

        Self {
            messages_sent,
            messages_dropped_slow_client,
            bytes_sent,
            connected_clients,
            client_disconnects,
            replay_requests,
            ping_timeouts,
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

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.messages_sent.get(),
            bytes_total: self.bytes_sent.get(),
            errors_total: self.messages_dropped_slow_client.get(),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom("ws.connected_clients", self.connected_clients.get() as f64);
        m.add_custom(
            "ws.client_disconnects",
            self.client_disconnects.get() as f64,
        );
        m.add_custom("ws.replay_requests", self.replay_requests.get() as f64);
        m.add_custom(
            "ws.messages_dropped_slow_client",
            self.messages_dropped_slow_client.get() as f64,
        );
        m.add_custom("ws.ping_timeouts", self.ping_timeouts.get() as f64);
        m
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
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_send() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_send(512);
        m.record_send(1024);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 2);
        assert_eq!(cm.bytes_total, 1536);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_drop();
        m.record_drop();

        let cm = m.to_connector_metrics();
        assert_eq!(cm.errors_total, 2);

        let dropped = cm
            .custom
            .iter()
            .find(|(k, _)| k == "ws.messages_dropped_slow_client");
        assert_eq!(dropped.unwrap().1, 2.0);
    }

    #[test]
    fn test_record_connect_disconnect() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_connect();
        m.record_connect();
        m.record_connect();
        m.record_disconnect();

        let cm = m.to_connector_metrics();
        let clients = cm.custom.iter().find(|(k, _)| k == "ws.connected_clients");
        assert_eq!(clients.unwrap().1, 2.0);

        let disconnects = cm.custom.iter().find(|(k, _)| k == "ws.client_disconnects");
        assert_eq!(disconnects.unwrap().1, 1.0);
    }

    #[test]
    fn test_disconnect_saturates_at_zero() {
        let m = WebSocketSinkMetrics::new(None);
        // Disconnect without any connect should not underflow
        m.record_disconnect();

        let cm = m.to_connector_metrics();
        let clients = cm.custom.iter().find(|(k, _)| k == "ws.connected_clients");
        assert_eq!(clients.unwrap().1, 0.0);

        let disconnects = cm.custom.iter().find(|(k, _)| k == "ws.client_disconnects");
        assert_eq!(disconnects.unwrap().1, 1.0);
    }

    #[test]
    fn test_record_replay() {
        let m = WebSocketSinkMetrics::new(None);
        m.record_replay();
        m.record_replay();
        m.record_replay();

        let cm = m.to_connector_metrics();
        let replays = cm.custom.iter().find(|(k, _)| k == "ws.replay_requests");
        assert_eq!(replays.unwrap().1, 3.0);
    }

    #[test]
    fn test_default() {
        let m = WebSocketSinkMetrics::default();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
        assert_eq!(cm.custom.len(), 5);
    }

    #[test]
    fn test_custom_metrics_count() {
        let m = WebSocketSinkMetrics::new(None);
        let cm = m.to_connector_metrics();
        // Should have exactly 5 custom metrics
        assert_eq!(cm.custom.len(), 5);
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

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 3);
        assert_eq!(cm.bytes_total, 600);
        assert_eq!(cm.errors_total, 1);

        let clients = cm.custom.iter().find(|(k, _)| k == "ws.connected_clients");
        assert_eq!(clients.unwrap().1, 1.0);
    }
}
