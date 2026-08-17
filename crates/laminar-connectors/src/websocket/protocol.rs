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
mod tests;
