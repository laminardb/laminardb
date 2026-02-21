//! Backpressure strategies for WebSocket connectors.
//!
//! When the internal Ring 0 bounded channel is full and cannot accept more
//! messages from a WebSocket source, a [`BackpressureStrategy`] determines
//! what happens to incoming data.

use serde::{Deserialize, Serialize};

/// Strategy applied when the Ring 0 channel is full and cannot accept more messages.
///
/// WebSocket sources produce data at the rate of the remote sender. When the
/// downstream processing pipeline cannot keep up, one of these strategies
/// governs how the connector handles the overflow.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum BackpressureStrategy {
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
        let strategy = BackpressureStrategy::default();
        assert!(matches!(strategy, BackpressureStrategy::Block));
    }

    #[test]
    fn test_debug_format() {
        let strategy = BackpressureStrategy::Block;
        let debug = format!("{strategy:?}");
        assert_eq!(debug, "Block");
    }

    #[test]
    fn test_buffer_variant() {
        let strategy = BackpressureStrategy::Buffer {
            max_bytes: 1_048_576,
        };
        if let BackpressureStrategy::Buffer { max_bytes } = strategy {
            assert_eq!(max_bytes, 1_048_576);
        } else {
            panic!("expected Buffer variant");
        }
    }

    #[test]
    fn test_sample_variant() {
        let strategy = BackpressureStrategy::Sample { rate: 10 };
        if let BackpressureStrategy::Sample { rate } = strategy {
            assert_eq!(rate, 10);
        } else {
            panic!("expected Sample variant");
        }
    }

    #[test]
    fn test_clone() {
        let original = BackpressureStrategy::DropOldest;
        let cloned = original.clone();
        assert!(matches!(cloned, BackpressureStrategy::DropOldest));
    }

    #[test]
    fn test_serde_roundtrip_block() {
        let strategy = BackpressureStrategy::Block;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: BackpressureStrategy = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, BackpressureStrategy::Block));
    }

    #[test]
    fn test_serde_roundtrip_buffer() {
        let strategy = BackpressureStrategy::Buffer { max_bytes: 65_536 };
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: BackpressureStrategy = serde_json::from_str(&json).unwrap();
        if let BackpressureStrategy::Buffer { max_bytes } = deserialized {
            assert_eq!(max_bytes, 65_536);
        } else {
            panic!("expected Buffer variant after deserialization");
        }
    }

    #[test]
    fn test_serde_roundtrip_sample() {
        let strategy = BackpressureStrategy::Sample { rate: 5 };
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: BackpressureStrategy = serde_json::from_str(&json).unwrap();
        if let BackpressureStrategy::Sample { rate } = deserialized {
            assert_eq!(rate, 5);
        } else {
            panic!("expected Sample variant after deserialization");
        }
    }

    #[test]
    fn test_serde_roundtrip_drop_oldest() {
        let strategy = BackpressureStrategy::DropOldest;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: BackpressureStrategy = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, BackpressureStrategy::DropOldest));
    }

    #[test]
    fn test_serde_roundtrip_drop_newest() {
        let strategy = BackpressureStrategy::DropNewest;
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: BackpressureStrategy = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized, BackpressureStrategy::DropNewest));
    }
}
