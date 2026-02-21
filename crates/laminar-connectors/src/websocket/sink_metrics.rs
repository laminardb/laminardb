//! WebSocket sink connector metrics.
//!
//! [`WebSocketSinkMetrics`] provides lock-free atomic counters for
//! tracking WebSocket sink statistics, convertible to the SDK's
//! [`ConnectorMetrics`] type.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::ConnectorMetrics;

/// Atomic counters for WebSocket sink connector statistics.
///
/// All counters use `Relaxed` ordering for maximum throughput on the
/// hot path. Snapshot reads via [`to_connector_metrics`](Self::to_connector_metrics)
/// provide a consistent-enough view for monitoring purposes.
#[derive(Debug)]
pub struct WebSocketSinkMetrics {
    /// Total messages sent to connected clients.
    pub messages_sent: AtomicU64,
    /// Total messages dropped because a client was too slow.
    pub messages_dropped_slow_client: AtomicU64,
    /// Total bytes sent (serialized payload).
    pub bytes_sent: AtomicU64,
    /// Current number of connected clients.
    pub connected_clients: AtomicU64,
    /// Total client disconnection events.
    pub client_disconnects: AtomicU64,
    /// Total replay requests received from clients.
    pub replay_requests: AtomicU64,
}

impl WebSocketSinkMetrics {
    /// Creates a new metrics instance with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            messages_sent: AtomicU64::new(0),
            messages_dropped_slow_client: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            client_disconnects: AtomicU64::new(0),
            replay_requests: AtomicU64::new(0),
        }
    }

    /// Records a successfully sent message with the given payload size.
    pub fn record_send(&self, bytes: u64) {
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records a message dropped due to a slow client.
    pub fn record_drop(&self) {
        self.messages_dropped_slow_client
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Records a new client connection.
    pub fn record_connect(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a client disconnection.
    pub fn record_disconnect(&self) {
        self.client_disconnects.fetch_add(1, Ordering::Relaxed);
        // Saturating subtract to avoid underflow on spurious disconnect events.
        self.connected_clients
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    /// Records a replay request from a client.
    pub fn record_replay(&self) {
        self.replay_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Converts to the SDK's [`ConnectorMetrics`].
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn to_connector_metrics(&self) -> ConnectorMetrics {
        let mut m = ConnectorMetrics {
            records_total: self.messages_sent.load(Ordering::Relaxed),
            bytes_total: self.bytes_sent.load(Ordering::Relaxed),
            errors_total: self.messages_dropped_slow_client.load(Ordering::Relaxed),
            lag: 0,
            custom: Vec::new(),
        };
        m.add_custom(
            "ws.connected_clients",
            self.connected_clients.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.client_disconnects",
            self.client_disconnects.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.replay_requests",
            self.replay_requests.load(Ordering::Relaxed) as f64,
        );
        m.add_custom(
            "ws.messages_dropped_slow_client",
            self.messages_dropped_slow_client.load(Ordering::Relaxed) as f64,
        );
        m
    }
}

impl Default for WebSocketSinkMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSinkMetrics::new();
        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 0);
        assert_eq!(cm.bytes_total, 0);
        assert_eq!(cm.errors_total, 0);
    }

    #[test]
    fn test_record_send() {
        let m = WebSocketSinkMetrics::new();
        m.record_send(512);
        m.record_send(1024);

        let cm = m.to_connector_metrics();
        assert_eq!(cm.records_total, 2);
        assert_eq!(cm.bytes_total, 1536);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSinkMetrics::new();
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
        let m = WebSocketSinkMetrics::new();
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
        let m = WebSocketSinkMetrics::new();
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
        let m = WebSocketSinkMetrics::new();
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
        assert_eq!(cm.custom.len(), 4);
    }

    #[test]
    fn test_custom_metrics_count() {
        let m = WebSocketSinkMetrics::new();
        let cm = m.to_connector_metrics();
        // Should have exactly 4 custom metrics
        assert_eq!(cm.custom.len(), 4);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSinkMetrics::new();
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
