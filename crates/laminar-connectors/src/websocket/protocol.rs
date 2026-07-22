//! Subscription wire protocol for WebSocket sink server mode.
//!
//! Defines the JSON message types exchanged between WebSocket sink server
//! and its connected clients. Messages are tagged with `"action"` (client)
//! or `"type"` (server) for serde dispatch.

use serde::{Deserialize, Serialize};

/// Messages sent from a client to the WebSocket sink server.
///
/// Clients use these messages to manage subscriptions. The `action` field in
/// the JSON object determines which variant is deserialized.
#[derive(Deserialize)]
#[serde(tag = "action", deny_unknown_fields)]
pub(super) enum ClientMessage {
    /// Subscribe to streaming query results.
    #[serde(rename = "subscribe")]
    Subscribe {},
    /// Unsubscribe from an active subscription.
    #[serde(rename = "unsubscribe")]
    Unsubscribe {
        /// The subscription ID returned in the `Subscribed` response.
        subscription_id: String,
    },
}

/// Messages sent from the WebSocket sink server to clients.
///
/// The `type` field in the JSON object identifies the response.
#[derive(Serialize)]
#[serde(tag = "type")]
pub(super) enum ServerMessage {
    /// Subscription confirmed -- sent after a successful `Subscribe` request.
    #[serde(rename = "subscribed")]
    Subscribed {
        /// Unique identifier for this subscription.
        subscription_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ClientMessage tests ──

    #[test]
    fn test_deserialize_subscribe_minimal() {
        let json = r#"{"action": "subscribe"}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();
        assert!(matches!(msg, ClientMessage::Subscribe {}));
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
    fn test_removed_subscription_options_are_rejected() {
        for json in [
            r#"{"action":"subscribe","filter":"id > 1"}"#,
            r#"{"action":"subscribe","format":"Json"}"#,
            r#"{"action":"subscribe","last_sequence":42}"#,
            r#"{"action":"ping"}"#,
        ] {
            assert!(serde_json::from_str::<ClientMessage>(json).is_err());
        }
    }

    #[test]
    fn test_deserialize_unknown_action_fails() {
        let json = r#"{"action": "unknown_action"}"#;
        let result = serde_json::from_str::<ClientMessage>(json);
        assert!(result.is_err());
    }

    // ── ServerMessage tests ──

    #[test]
    fn test_serialize_subscribed() {
        let msg = ServerMessage::Subscribed {
            subscription_id: "sub-abc".to_string(),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["type"], "subscribed");
        assert_eq!(json["subscription_id"], "sub-abc");
    }
}
