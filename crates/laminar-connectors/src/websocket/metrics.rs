//! `WebSocket` source connector metrics.
//!
//! [`WebSocketSourceMetrics`] provides Prometheus-backed counters
//! for tracking WebSocket source statistics.

use prometheus::core::{Collector, Desc};
use prometheus::proto::MetricFamily;
use prometheus::{IntCounter, Registry};

use crate::error::ConnectorError;

/// Prometheus-backed counters for WebSocket source connector statistics.
#[derive(Debug, Clone)]
pub struct WebSocketSourceMetrics {
    /// Total messages received from the WebSocket connection.
    pub messages_received: IntCounter,
    /// Total bytes received (raw payload, before parsing).
    pub bytes_received: IntCounter,
    /// Total number of reconnection attempts.
    pub reconnect_count: IntCounter,
    /// Total parse/deserialization errors.
    pub parse_errors: IntCounter,
    /// Total ingress messages dropped by the configured backpressure policy.
    pub backpressure_drops: IntCounter,
}

impl WebSocketSourceMetrics {
    /// Creates an unregistered metrics family with all counters at zero.
    #[must_use]
    #[allow(clippy::missing_panics_doc)]
    pub fn local() -> Self {
        Self {
            messages_received: IntCounter::new(
                "websocket_source_messages_received_total",
                "Total WebSocket messages received",
            )
            .expect("static WebSocket source metric must be valid"),
            bytes_received: IntCounter::new(
                "websocket_source_bytes_received_total",
                "Total WebSocket bytes received",
            )
            .expect("static WebSocket source metric must be valid"),
            reconnect_count: IntCounter::new(
                "websocket_source_reconnect_total",
                "Total WebSocket reconnection attempts",
            )
            .expect("static WebSocket source metric must be valid"),
            parse_errors: IntCounter::new(
                "websocket_source_parse_errors_total",
                "Total WebSocket parse and deserialization errors",
            )
            .expect("static WebSocket source metric must be valid"),
            backpressure_drops: IntCounter::new(
                "websocket_source_backpressure_drops_total",
                "Total WebSocket messages dropped by ingress backpressure",
            )
            .expect("static WebSocket source metric must be valid"),
        }
    }

    /// Creates and registers one source metrics family.
    pub fn register(registry: &Registry) -> Result<Self, ConnectorError> {
        let metrics = Self::local();
        registry
            .register(Box::new(metrics.clone()))
            .map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "failed to register WebSocket source metrics: {error}"
                ))
            })?;
        Ok(metrics)
    }

    /// Records a successfully received message with the given payload size.
    pub fn record_message(&self, bytes: usize) {
        self.messages_received.inc();
        self.bytes_received
            .inc_by(u64::try_from(bytes).unwrap_or(u64::MAX));
    }

    /// Records a reconnection attempt.
    pub fn record_reconnect(&self) {
        self.reconnect_count.inc();
    }

    /// Records a parse/deserialization error.
    pub fn record_parse_error(&self) {
        self.parse_errors.inc();
    }

    /// Records one message intentionally dropped at ingress.
    pub fn record_backpressure_drop(&self) {
        self.backpressure_drops.inc();
    }
}

impl Collector for WebSocketSourceMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut descriptors = Vec::with_capacity(5);
        descriptors.extend(self.messages_received.desc());
        descriptors.extend(self.bytes_received.desc());
        descriptors.extend(self.reconnect_count.desc());
        descriptors.extend(self.parse_errors.desc());
        descriptors.extend(self.backpressure_drops.desc());
        descriptors
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = Vec::with_capacity(5);
        families.extend(self.messages_received.collect());
        families.extend(self.bytes_received.collect());
        families.extend(self.reconnect_count.collect());
        families.extend(self.parse_errors.collect());
        families.extend(self.backpressure_drops.collect());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_zeros() {
        let m = WebSocketSourceMetrics::local();
        assert_eq!(m.messages_received.get(), 0);
        assert_eq!(m.bytes_received.get(), 0);
        assert_eq!(m.parse_errors.get(), 0);
        assert_eq!(m.backpressure_drops.get(), 0);
    }

    #[test]
    fn test_record_message() {
        let m = WebSocketSourceMetrics::local();
        m.record_message(1024);
        m.record_message(2048);

        assert_eq!(m.messages_received.get(), 2);
        assert_eq!(m.bytes_received.get(), 3072);
    }

    #[test]
    fn test_record_reconnect() {
        let m = WebSocketSourceMetrics::local();
        m.record_reconnect();
        m.record_reconnect();

        assert_eq!(m.reconnect_count.get(), 2);
    }

    #[test]
    fn test_record_parse_error() {
        let m = WebSocketSourceMetrics::local();
        m.record_parse_error();

        assert_eq!(m.parse_errors.get(), 1);
    }

    #[test]
    fn test_record_backpressure_drop() {
        let m = WebSocketSourceMetrics::local();
        m.record_backpressure_drop();
        assert_eq!(m.backpressure_drops.get(), 1);
    }

    #[test]
    fn test_combined_operations() {
        let m = WebSocketSourceMetrics::local();
        m.record_message(100);
        m.record_message(200);
        m.record_reconnect();
        m.record_parse_error();

        assert_eq!(m.messages_received.get(), 2);
        assert_eq!(m.bytes_received.get(), 300);
        assert_eq!(m.parse_errors.get(), 1);
    }

    #[test]
    fn duplicate_family_registration_fails_visibly() {
        let registry = Registry::new();
        WebSocketSourceMetrics::register(&registry).unwrap();

        let error = WebSocketSourceMetrics::register(&registry)
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("register WebSocket source metrics"),
            "{error}"
        );
    }
}
