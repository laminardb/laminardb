//! WebSocket sink connector metrics.
//!
//! [`WebSocketSinkMetrics`] provides prometheus-backed counters and gauges
//! for tracking WebSocket sink statistics.

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounter, IntGauge, Registry};

use crate::error::ConnectorError;

/// Prometheus-backed counters/gauges for WebSocket sink connector statistics.
#[derive(Debug, Clone)]
pub struct WebSocketSinkMetrics {
    /// Total messages sent to connected clients.
    pub messages_sent: IntCounter,
    /// Total messages dropped because a client was too slow.
    pub messages_dropped_slow_client: IntCounter,
    /// Total subscribed client frames that failed during socket delivery.
    pub delivery_failures: IntCounter,
    /// Total bytes sent (serialized payload).
    pub bytes_sent: IntCounter,
    /// Current number of connected clients.
    pub connected_clients: IntGauge,
    /// Total client disconnection events.
    pub client_disconnects: IntCounter,
    /// Total clients disconnected due to ping timeout.
    pub ping_timeouts: IntCounter,
}

impl WebSocketSinkMetrics {
    /// Creates an unregistered metrics family with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn local() -> Self {
        Self {
            messages_sent: IntCounter::new(
                "websocket_sink_messages_sent_total",
                "Total WebSocket messages sent",
            )
            .expect("static WebSocket sink metric must be valid"),
            messages_dropped_slow_client: IntCounter::new(
                "websocket_sink_messages_dropped_slow_client_total",
                "Total WebSocket messages dropped for slow clients",
            )
            .expect("static WebSocket sink metric must be valid"),
            delivery_failures: IntCounter::new(
                "websocket_sink_delivery_failures_total",
                "Total WebSocket client-frame socket delivery failures",
            )
            .expect("static WebSocket sink metric must be valid"),
            bytes_sent: IntCounter::new(
                "websocket_sink_bytes_sent_total",
                "Total WebSocket bytes sent",
            )
            .expect("static WebSocket sink metric must be valid"),
            connected_clients: IntGauge::new(
                "websocket_sink_connected_clients",
                "Current connected WebSocket clients",
            )
            .expect("static WebSocket sink metric must be valid"),
            client_disconnects: IntCounter::new(
                "websocket_sink_client_disconnects_total",
                "Total WebSocket client disconnections",
            )
            .expect("static WebSocket sink metric must be valid"),
            ping_timeouts: IntCounter::new(
                "websocket_sink_ping_timeouts_total",
                "Total WebSocket ping timeouts",
            )
            .expect("static WebSocket sink metric must be valid"),
        }
    }

    /// Creates and registers one sink metrics family.
    pub fn register(registry: &Registry) -> Result<Self, ConnectorError> {
        let metrics = Self::local();
        registry
            .register(Box::new(metrics.clone()))
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "failed to register WebSocket sink metrics: {error}"
                ))
            })?;
        Ok(metrics)
    }

    /// Records a successfully sent message with the given payload size.
    pub fn record_send(&self, bytes: u64) {
        self.record_sends(1, bytes);
    }

    /// Records a successfully flushed group of messages.
    pub fn record_sends(&self, messages: u64, bytes: u64) {
        self.messages_sent.inc_by(messages);
        self.bytes_sent.inc_by(bytes);
    }

    /// Records messages dropped due to slow clients.
    pub fn record_drops(&self, count: u64) {
        self.messages_dropped_slow_client.inc_by(count);
    }

    /// Records subscribed client frames that could not be written.
    pub fn record_delivery_failure(&self, count: u64) {
        self.delivery_failures.inc_by(count);
    }

    /// Records a new client connection.
    pub fn record_connect(&self) {
        self.connected_clients.inc();
    }

    /// Tracks one established server-side connection until its task exits.
    #[must_use]
    pub(super) fn connection_guard(&self) -> ConnectionGuard {
        self.record_connect();
        ConnectionGuard {
            metrics: self.clone(),
        }
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

    /// Records a client disconnected due to ping timeout.
    pub fn record_ping_timeout(&self) {
        self.ping_timeouts.inc();
    }
}

/// Balances the aggregate connection gauge even when a client task is aborted.
pub(super) struct ConnectionGuard {
    metrics: WebSocketSinkMetrics,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.record_disconnect();
    }
}

impl Collector for WebSocketSinkMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut descriptors = Vec::with_capacity(7);
        descriptors.extend(self.messages_sent.desc());
        descriptors.extend(self.messages_dropped_slow_client.desc());
        descriptors.extend(self.delivery_failures.desc());
        descriptors.extend(self.bytes_sent.desc());
        descriptors.extend(self.connected_clients.desc());
        descriptors.extend(self.client_disconnects.desc());
        descriptors.extend(self.ping_timeouts.desc());
        descriptors
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::with_capacity(7);
        families.extend(self.messages_sent.collect());
        families.extend(self.messages_dropped_slow_client.collect());
        families.extend(self.delivery_failures.collect());
        families.extend(self.bytes_sent.collect());
        families.extend(self.connected_clients.collect());
        families.extend(self.client_disconnects.collect());
        families.extend(self.ping_timeouts.collect());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSinkMetrics::local();
        assert_eq!(m.messages_sent.get(), 0);
        assert_eq!(m.bytes_sent.get(), 0);
        assert_eq!(m.messages_dropped_slow_client.get(), 0);
        assert_eq!(m.delivery_failures.get(), 0);
    }

    #[test]
    fn test_record_send() {
        let m = WebSocketSinkMetrics::local();
        m.record_send(512);
        m.record_send(1024);

        assert_eq!(m.messages_sent.get(), 2);
        assert_eq!(m.bytes_sent.get(), 1536);
    }

    #[test]
    fn test_record_drop() {
        let m = WebSocketSinkMetrics::local();
        m.record_drops(1);
        m.record_drops(1);
        m.record_delivery_failure(3);

        assert_eq!(m.messages_dropped_slow_client.get(), 2);
        assert_eq!(m.delivery_failures.get(), 3);
    }

    #[test]
    fn test_record_connect_disconnect() {
        let m = WebSocketSinkMetrics::local();
        m.record_connect();
        m.record_connect();
        m.record_connect();
        m.record_disconnect();

        assert_eq!(m.connected_clients.get(), 2);
        assert_eq!(m.client_disconnects.get(), 1);
    }

    #[test]
    fn test_disconnect_saturates_at_zero() {
        let m = WebSocketSinkMetrics::local();
        // Disconnect without any connect should not underflow
        m.record_disconnect();

        assert_eq!(m.connected_clients.get(), 0);
        assert_eq!(m.client_disconnects.get(), 1);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSinkMetrics::local();
        m.record_send(100);
        m.record_send(200);
        m.record_send(300);
        m.record_drops(1);
        m.record_connect();
        m.record_connect();
        m.record_disconnect();
        assert_eq!(m.messages_sent.get(), 3);
        assert_eq!(m.bytes_sent.get(), 600);
        assert_eq!(m.messages_dropped_slow_client.get(), 1);
        assert_eq!(m.connected_clients.get(), 1);
    }

    #[test]
    fn connection_guard_balances_shared_gauge() {
        let metrics = WebSocketSinkMetrics::local();
        let first = metrics.connection_guard();
        let second = metrics.connection_guard();
        assert_eq!(metrics.connected_clients.get(), 2);

        drop(first);
        assert_eq!(metrics.connected_clients.get(), 1);
        drop(second);
        assert_eq!(metrics.connected_clients.get(), 0);
        assert_eq!(metrics.client_disconnects.get(), 2);
    }

    #[test]
    fn duplicate_family_registration_fails_visibly() {
        let registry = Registry::new();
        WebSocketSinkMetrics::register(&registry).unwrap();

        let error = WebSocketSinkMetrics::register(&registry)
            .unwrap_err()
            .to_string();

        assert!(error.contains("register WebSocket sink metrics"), "{error}");
    }
}
