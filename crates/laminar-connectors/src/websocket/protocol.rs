//! Subscription wire protocol for WebSocket sink server mode.
//!
//! Defines the JSON message types exchanged between WebSocket sink server
//! and its connected clients. Messages are tagged with `"action"` (client)
//! or `"type"` (server) for serde dispatch.

use serde::{Deserialize, Serialize};

use super::sink_config::SinkFormat;

/// Messages sent from a client to the WebSocket sink server.
///
/// Clients use these messages to manage subscriptions and send
/// application-level pings. The `action` field in the JSON object
/// determines which variant is deserialized.
#[derive(Debug, Deserialize)]
#[serde(tag = "action")]
pub enum ClientMessage {
    /// Subscribe to streaming query results.
    #[serde(rename = "subscribe")]
    Subscribe {
        /// Optional filter expression to apply server-side.
        filter: Option<String>,
        /// If provided, server attempts to replay from this sequence.
        last_sequence: Option<u64>,
        /// Preferred serialization format for data messages.
        format: Option<SinkFormat>,
    },
    /// Unsubscribe from an active subscription.
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        /// The subscription ID returned in the `Subscribed` response.
        subscription_id: String,
    },
    /// Client ping (application-level keepalive).
    #[serde(rename = "ping")]
    Ping,
}

/// Messages sent from the WebSocket sink server to clients.
///
/// The `type` field in the JSON object determines which variant is
/// serialized. Data messages include monotonically increasing sequence
/// numbers for client-side gap detection.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    /// Data delivery containing query results.
    #[serde(rename = "data")]
    Data {
        /// Subscription this data belongs to.
        subscription_id: String,
        /// The payload (format depends on client's requested `SinkFormat`).
        data: serde_json::Value,
        /// Monotonically increasing sequence number for gap detection.
        sequence: u64,
        /// Current watermark (epoch millis), if available.
        watermark: Option<i64>,
    },
    /// Subscription confirmed -- sent after a successful `Subscribe` request.
    #[serde(rename = "subscribed")]
    Subscribed {
        /// Unique identifier for this subscription.
        subscription_id: String,
    },
    /// Error message sent to the client.
    #[serde(rename = "error")]
    Error {
        /// Human-readable error description.
        message: String,
    },
    /// Periodic heartbeat from the server.
    #[serde(rename = "heartbeat")]
    Heartbeat {
        /// Server timestamp (epoch millis).
        server_time: i64,
    },
    /// Backpressure warning indicating the server's send buffer is filling up.
    #[serde(rename = "backpressure")]
    BackpressureWarning {
        /// Percentage of the send buffer currently used (0-100).
        buffer_pct: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ClientMessage tests ──

    #[test]
    fn test_deserialize_subscribe_full() {
        let json = r#"{
            "action": "subscribe",
            "filter": "symbol = 'AAPL'",
            "last_sequence": 42,
            "format": "Json"
        }"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        if let ClientMessage::Subscribe {
            filter,
            last_sequence,
            format,
        } = msg
        {
            assert_eq!(filter.as_deref(), Some("symbol = 'AAPL'"));
            assert_eq!(last_sequence, Some(42));
            assert!(format.is_some());
        } else {
            panic!("expected Subscribe variant");
        }
    }

    #[test]
    fn test_deserialize_subscribe_minimal() {
        let json = r#"{"action": "subscribe"}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        if let ClientMessage::Subscribe {
            filter,
            last_sequence,
            format,
        } = msg
        {
            assert!(filter.is_none());
            assert!(last_sequence.is_none());
            assert!(format.is_none());
        } else {
            panic!("expected Subscribe variant");
        }
    }

    #[test]
    fn test_deserialize_unsubscribe() {
        let json = r#"{"action": "unsubscribe", "subscription_id": "sub-123"}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        if let ClientMessage::Unsubscribe { subscription_id } = msg {
            assert_eq!(subscription_id, "sub-123");
        } else {
            panic!("expected Unsubscribe variant");
        }
    }

    #[test]
    fn test_deserialize_ping() {
        let json = r#"{"action": "ping"}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, ClientMessage::Ping));
    }

    #[test]
    fn test_deserialize_unknown_action_fails() {
        let json = r#"{"action": "unknown_action"}"#;
        let result = serde_json::from_str::<ClientMessage>(json);
        assert!(result.is_err());
    }

    // ── ServerMessage tests ──

    #[test]
    fn test_serialize_data() {
        let msg = ServerMessage::Data {
            subscription_id: "sub-1".to_string(),
            data: serde_json::json!({"price": 150.5}),
            sequence: 100,
            watermark: Some(1_700_000_000_000),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "data");
        assert_eq!(json["subscription_id"], "sub-1");
        assert_eq!(json["sequence"], 100);
        assert_eq!(json["watermark"], 1_700_000_000_000_i64);
        assert_eq!(json["data"]["price"], 150.5);
    }

    #[test]
    fn test_serialize_data_no_watermark() {
        let msg = ServerMessage::Data {
            subscription_id: "sub-2".to_string(),
            data: serde_json::json!("hello"),
            sequence: 1,
            watermark: None,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "data");
        assert!(json["watermark"].is_null());
    }

    #[test]
    fn test_serialize_subscribed() {
        let msg = ServerMessage::Subscribed {
            subscription_id: "sub-abc".to_string(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "subscribed");
        assert_eq!(json["subscription_id"], "sub-abc");
    }

    #[test]
    fn test_serialize_error() {
        let msg = ServerMessage::Error {
            message: "invalid filter expression".to_string(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "error");
        assert_eq!(json["message"], "invalid filter expression");
    }

    #[test]
    fn test_serialize_heartbeat() {
        let msg = ServerMessage::Heartbeat {
            server_time: 1_700_000_000_000,
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "heartbeat");
        assert_eq!(json["server_time"], 1_700_000_000_000_i64);
    }

    #[test]
    fn test_serialize_backpressure_warning() {
        let msg = ServerMessage::BackpressureWarning { buffer_pct: 85 };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "backpressure");
        assert_eq!(json["buffer_pct"], 85);
    }

    #[test]
    fn test_server_message_clone() {
        let msg = ServerMessage::Heartbeat { server_time: 12345 };
        let cloned = msg.clone();
        let json = serde_json::to_value(&cloned).unwrap();
        assert_eq!(json["server_time"], 12345);
    }

    #[test]
    fn test_server_message_debug() {
        let msg = ServerMessage::Error {
            message: "test".to_string(),
        };
        let debug = format!("{msg:?}");
        assert!(debug.contains("Error"));
        assert!(debug.contains("test"));
    }
}
