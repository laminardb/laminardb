//! WebSocket source checkpoint types.
//!
//! WebSocket is non-replayable -- checkpoints capture state for best-effort recovery.
//! On recovery, data gaps should be expected and logged.

use serde::{Deserialize, Serialize};

use crate::checkpoint::SourceCheckpoint;

/// Checkpoint state for a WebSocket source connector.
///
/// Since WebSocket has no intrinsic offsets or replay capability,
/// this checkpoint captures application-level state for best-effort recovery.
/// On recovery, data gaps should be expected and logged.
///
/// The entire struct is serialized as JSON and stored under a single key
/// (`"websocket_state"`) in the [`SourceCheckpoint`] offsets map.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebSocketSourceCheckpoint {
    /// Application-level sequence number (if the WS server provides one).
    pub last_sequence: Option<u64>,
    /// Server-provided position token (if any -- some APIs support resume tokens).
    pub position_token: Option<String>,
    /// Last event timestamp processed (epoch millis).
    pub last_event_time: Option<i64>,
    /// Current watermark value.
    pub watermark: i64,
    /// Which URL was active at checkpoint time.
    pub active_url: Option<String>,
    /// Subscription messages to re-send on recovery.
    pub subscriptions: Vec<String>,
}

/// Key used to store the serialized checkpoint in [`SourceCheckpoint`] offsets.
const CHECKPOINT_KEY: &str = "websocket_state";

impl WebSocketSourceCheckpoint {
    /// Serializes this checkpoint into a [`SourceCheckpoint`].
    ///
    /// The entire struct is stored as a JSON string under the key
    /// `"websocket_state"` in the checkpoint's offsets map.
    ///
    /// # Arguments
    ///
    /// * `epoch` - The epoch number for the checkpoint.
    ///
    /// # Panics
    ///
    /// Panics if the struct cannot be serialized to JSON (should never
    /// happen for these field types).
    #[must_use]
    pub fn to_source_checkpoint(&self, epoch: u64) -> SourceCheckpoint {
        let json = serde_json::to_string(self)
            .expect("WebSocketSourceCheckpoint should always be JSON-serializable");
        let mut cp = SourceCheckpoint::new(epoch);
        cp.set_offset(CHECKPOINT_KEY, json);
        cp
    }

    /// Deserializes a [`WebSocketSourceCheckpoint`] from a [`SourceCheckpoint`].
    ///
    /// Looks for the `"websocket_state"` key in the checkpoint's offsets map
    /// and deserializes the JSON value. Returns a default (empty) checkpoint
    /// if the key is missing or the JSON is malformed.
    #[must_use]
    pub fn from_source_checkpoint(cp: &SourceCheckpoint) -> Self {
        cp.get_offset(CHECKPOINT_KEY)
            .and_then(|json| serde_json::from_str(json).ok())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_checkpoint() {
        let cp = WebSocketSourceCheckpoint::default();
        assert!(cp.last_sequence.is_none());
        assert!(cp.position_token.is_none());
        assert!(cp.last_event_time.is_none());
        assert_eq!(cp.watermark, 0);
        assert!(cp.active_url.is_none());
        assert!(cp.subscriptions.is_empty());
    }

    #[test]
    fn test_roundtrip_empty() {
        let ws_cp = WebSocketSourceCheckpoint::default();
        let source_cp = ws_cp.to_source_checkpoint(1);
        let restored = WebSocketSourceCheckpoint::from_source_checkpoint(&source_cp);

        assert_eq!(restored.last_sequence, None);
        assert_eq!(restored.position_token, None);
        assert_eq!(restored.last_event_time, None);
        assert_eq!(restored.watermark, 0);
        assert_eq!(restored.active_url, None);
        assert!(restored.subscriptions.is_empty());
    }

    #[test]
    fn test_roundtrip_full() {
        let ws_cp = WebSocketSourceCheckpoint {
            last_sequence: Some(42),
            position_token: Some("tok-abc-123".to_string()),
            last_event_time: Some(1_700_000_000_000),
            watermark: 1_699_999_999_000,
            active_url: Some("wss://feed.example.com/v1".to_string()),
            subscriptions: vec![
                r#"{"action":"subscribe","channel":"trades"}"#.to_string(),
                r#"{"action":"subscribe","channel":"orderbook"}"#.to_string(),
            ],
        };

        let source_cp = ws_cp.to_source_checkpoint(5);
        assert_eq!(source_cp.epoch(), 5);

        let restored = WebSocketSourceCheckpoint::from_source_checkpoint(&source_cp);
        assert_eq!(restored.last_sequence, Some(42));
        assert_eq!(restored.position_token.as_deref(), Some("tok-abc-123"));
        assert_eq!(restored.last_event_time, Some(1_700_000_000_000));
        assert_eq!(restored.watermark, 1_699_999_999_000);
        assert_eq!(
            restored.active_url.as_deref(),
            Some("wss://feed.example.com/v1")
        );
        assert_eq!(restored.subscriptions.len(), 2);
        assert!(restored.subscriptions[0].contains("trades"));
        assert!(restored.subscriptions[1].contains("orderbook"));
    }

    #[test]
    fn test_from_empty_source_checkpoint() {
        let source_cp = SourceCheckpoint::new(0);
        let restored = WebSocketSourceCheckpoint::from_source_checkpoint(&source_cp);
        assert!(restored.last_sequence.is_none());
        assert_eq!(restored.watermark, 0);
    }

    #[test]
    fn test_from_invalid_json() {
        let mut source_cp = SourceCheckpoint::new(1);
        source_cp.set_offset("websocket_state", "not valid json");
        let restored = WebSocketSourceCheckpoint::from_source_checkpoint(&source_cp);
        // Should fall back to default
        assert!(restored.last_sequence.is_none());
        assert_eq!(restored.watermark, 0);
    }

    #[test]
    fn test_epoch_preserved() {
        let ws_cp = WebSocketSourceCheckpoint {
            watermark: 100,
            ..Default::default()
        };
        let source_cp = ws_cp.to_source_checkpoint(42);
        assert_eq!(source_cp.epoch(), 42);
    }

    #[test]
    fn test_checkpoint_key_in_offsets() {
        let ws_cp = WebSocketSourceCheckpoint {
            last_sequence: Some(99),
            ..Default::default()
        };
        let source_cp = ws_cp.to_source_checkpoint(1);
        let raw_json = source_cp.get_offset("websocket_state");
        assert!(raw_json.is_some());
        assert!(raw_json.unwrap().contains("99"));
    }

    #[test]
    fn test_serde_json_roundtrip() {
        let ws_cp = WebSocketSourceCheckpoint {
            last_sequence: Some(7),
            position_token: None,
            last_event_time: Some(-1000),
            watermark: -500,
            active_url: Some("ws://localhost:8080".to_string()),
            subscriptions: vec!["sub1".to_string()],
        };

        let json = serde_json::to_string(&ws_cp).unwrap();
        let restored: WebSocketSourceCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.last_sequence, Some(7));
        assert_eq!(restored.last_event_time, Some(-1000));
        assert_eq!(restored.watermark, -500);
        assert_eq!(restored.subscriptions, vec!["sub1"]);
    }
}
