//! Backpressure strategies for WebSocket connectors.
//!
//! When the internal Ring 0 bounded channel is full and cannot accept more
//! messages from a WebSocket source, a `WsBackpressure` strategy determines
//! what happens to incoming data.

use serde::{Deserialize, Serialize};

/// Strategy applied when the Ring 0 channel is full and cannot accept more messages.
///
/// WebSocket sources produce data at the rate of the remote sender. When the
/// downstream processing pipeline cannot keep up, one of these strategies
/// governs how the connector handles the overflow.
///
/// **Implementation status** (2026-03-07):
/// - `Block`: fully implemented (TCP backpressure propagation)
/// - `DropNewest`: implemented in `source.rs` via `try_send`
/// - `DropOldest`, `Buffer`, `Sample`: parsed from SQL WITH, fall back to
///   `DropNewest` behavior with a debug log. Full dispatch not yet implemented.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum WsBackpressure {
    /// Block WS read -- TCP backpressure propagates to sender.
    ///
    /// This is the safest option: the WebSocket read loop simply stops
    /// reading, which causes the TCP window to fill, eventually slowing
    /// the remote sender. No data is lost, but latency increases.
    #[default]
    Block,
    /// Drop oldest messages from bounded channel when full.
    ///
    /// The channel evicts the oldest buffered message to make room for
    /// each new arrival. Good for "latest value wins" use cases.
    DropOldest,
    /// Drop incoming message when channel full (don't enqueue).
    ///
    /// The newly arriving message is silently discarded. Useful when
    /// freshness of already-buffered data matters more than completeness.
    DropNewest,
    /// Buffer up to `max_bytes`, then drop oldest.
    ///
    /// An intermediate strategy: an additional byte-bounded buffer sits
    /// in front of the channel. Once the buffer exceeds `max_bytes`, the
    /// oldest buffered messages are evicted.
    Buffer {
        /// Maximum buffer size in bytes before eviction kicks in.
        max_bytes: usize,
    },
    /// Sample: keep every Nth message.
    ///
    /// Under sustained backpressure, only every `rate`-th message is
    /// forwarded. All others are dropped. Useful for high-frequency
    /// telemetry where statistical sampling is acceptable.
    Sample {
        /// Keep every Nth message (e.g., `rate = 10` keeps 10% of messages).
        rate: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_block() {
        let strategy = WsBackpressure::default();
        assert!(matches!(strategy, WsBackpressure::Block));
    }

    #[test]
    fn test_debug_format() {
        let strategy = WsBackpressure::Block;
        let debug = format!("{strategy:?}");
        assert_eq!(debug, "Block");
    }

    #[test]
    fn test_buffer_variant() {
        let strategy = WsBackpressure::Buffer {
            max_bytes: 1_048_576,
        };
        if let WsBackpressure::Buffer { max_bytes } = strategy {
            assert_eq!(max_bytes, 1_048_576);
        } else {
            panic!("expected Buffer variant");
        }
    }

    #[test]
    fn test_sample_variant() {
        let strategy = WsBackpressure::Sample { rate: 10 };
        if let WsBackpressure::Sample { rate } = strategy {
            assert_eq!(rate, 10);
        } else {
            panic!("expected Sample variant");
        }
    }

    #[test]
    fn test_clone() {
        let original = WsBackpressure::DropOldest;
        let cloned = original.clone();
        assert!(matches!(cloned, WsBackpressure::DropOldest));
    }

    #[test]
    fn test_serde_roundtrip_block() {
        let strategy = WsBackpressure::Block;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: WsBackpressure = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, WsBackpressure::Block));
    }

    #[test]
    fn test_serde_roundtrip_buffer() {
        let strategy = WsBackpressure::Buffer { max_bytes: 65_536 };
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: WsBackpressure = serde_json::from_str(&json).unwrap();
        if let WsBackpressure::Buffer { max_bytes } = deserialized {
            assert_eq!(max_bytes, 65_536);
        } else {
            panic!("expected Buffer variant after deserialization");
        }
    }

    #[test]
    fn test_serde_roundtrip_sample() {
        let strategy = WsBackpressure::Sample { rate: 5 };
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: WsBackpressure = serde_json::from_str(&json).unwrap();
        if let WsBackpressure::Sample { rate } = deserialized {
            assert_eq!(rate, 5);
        } else {
            panic!("expected Sample variant after deserialization");
        }
    }

    #[test]
    fn test_serde_roundtrip_drop_oldest() {
        let strategy = WsBackpressure::DropOldest;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: WsBackpressure = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, WsBackpressure::DropOldest));
    }

    #[test]
    fn test_serde_roundtrip_drop_newest() {
        let strategy = WsBackpressure::DropNewest;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: WsBackpressure = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, WsBackpressure::DropNewest));
    }
}
