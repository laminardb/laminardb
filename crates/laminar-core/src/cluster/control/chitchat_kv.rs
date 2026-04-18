//! Chitchat-backed [`ClusterKv`]. Reads filter by `live_nodes()` so
//! suspected peers don't count toward quorum.

use std::sync::Arc;

use async_trait::async_trait;

use super::barrier::ClusterKv;
use crate::cluster::discovery::NodeId;

/// Chitchat-backed cluster KV.
pub struct ChitchatKv {
    chitchat: Arc<tokio::sync::Mutex<chitchat::Chitchat>>,
}

impl std::fmt::Debug for ChitchatKv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChitchatKv").finish_non_exhaustive()
    }
}

impl ChitchatKv {
    /// Construct from a chitchat handle.
    #[must_use]
    pub fn from_handle(handle: &chitchat::ChitchatHandle) -> Self {
        Self {
            chitchat: handle.chitchat(),
        }
    }

    /// Construct from a shared chitchat state.
    #[must_use]
    pub fn from_chitchat(chitchat: Arc<tokio::sync::Mutex<chitchat::Chitchat>>) -> Self {
        Self { chitchat }
    }
}

fn encode_node_id(node_id: NodeId) -> String {
    format!("node-{}", node_id.0)
}

fn decode_chitchat_id(id: &chitchat::ChitchatId) -> Option<NodeId> {
    id.node_id.strip_prefix("node-")?.parse::<u64>().ok().map(NodeId)
}

#[async_trait]
impl ClusterKv for ChitchatKv {
    async fn write(&self, key: &str, value: String) {
        let mut guard = self.chitchat.lock().await;
        guard.self_node_state().set(key, value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let target = encode_node_id(who);
        let guard = self.chitchat.lock().await;
        for (cc_id, state) in guard.node_states() {
            if cc_id.node_id == target {
                return state.get(key).map(str::to_string);
            }
        }
        None
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let guard = self.chitchat.lock().await;
        let live: Vec<&chitchat::ChitchatId> = guard.live_nodes().collect();
        let mut out = Vec::new();
        for (cc_id, state) in guard.node_states() {
            if !live.contains(&cc_id) {
                continue;
            }
            let Some(node_id) = decode_chitchat_id(cc_id) else {
                continue;
            };
            if let Some(value) = state.get(key) {
                out.push((node_id, value.to_string()));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_encoding_roundtrips() {
        for &v in &[1u64, 42, u64::MAX] {
            let s = encode_node_id(NodeId(v));
            assert!(s.starts_with("node-"), "got: {s}");
            let parsed: u64 = s.strip_prefix("node-").unwrap().parse().unwrap();
            assert_eq!(parsed, v);
        }
    }

    #[test]
    fn decode_rejects_unexpected_formats() {
        let bad = chitchat::ChitchatId::new(
            "foo".to_string(),
            0,
            "127.0.0.1:1".parse().unwrap(),
        );
        assert_eq!(decode_chitchat_id(&bad), None);
    }

    #[test]
    fn decode_accepts_valid_format() {
        let good = chitchat::ChitchatId::new(
            "node-42".to_string(),
            0,
            "127.0.0.1:1".parse().unwrap(),
        );
        assert_eq!(decode_chitchat_id(&good), Some(NodeId(42)));
    }
}
