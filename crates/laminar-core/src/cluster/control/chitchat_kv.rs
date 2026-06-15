//! Chitchat-backed [`ClusterKv`]. Reads filter by `live_nodes()` so
//! suspected peers don't count toward quorum.
#![allow(clippy::disallowed_types)] // cold control-plane path

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
    id.node_id
        .strip_prefix("node-")?
        .parse::<u64>()
        .ok()
        .map(NodeId)
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

        let mut best_val = None;
        let mut highest_gen = 0;

        for (cc_id, state) in guard.node_states() {
            if cc_id.node_id == target && cc_id.generation_id >= highest_gen {
                if let Some(val) = state.get(key) {
                    highest_gen = cc_id.generation_id;
                    best_val = Some(val.to_string());
                }
            }
        }
        best_val
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let guard = self.chitchat.lock().await;
        let live: Vec<&chitchat::ChitchatId> = guard.live_nodes().collect();
        let mut best: std::collections::HashMap<NodeId, (u64, String)> =
            std::collections::HashMap::new();
        for (cc_id, state) in guard.node_states() {
            if !live.contains(&cc_id) {
                continue;
            }
            let Some(node_id) = decode_chitchat_id(cc_id) else {
                continue;
            };
            if let Some(value) = state.get(key) {
                let entry = best
                    .entry(node_id)
                    .or_insert_with(|| (cc_id.generation_id, value.to_string()));
                if cc_id.generation_id > entry.0 {
                    entry.0 = cc_id.generation_id;
                    entry.1 = value.to_string();
                }
            }
        }
        best.into_iter().map(|(id, (_, val))| (id, val)).collect()
    }

    async fn scan_prefix(&self, prefix: &str) -> Vec<(NodeId, String, String)> {
        let guard = self.chitchat.lock().await;
        let live: Vec<&chitchat::ChitchatId> = guard.live_nodes().collect();
        let mut best_gen: std::collections::HashMap<NodeId, u64> = std::collections::HashMap::new();
        let mut best_states: std::collections::HashMap<NodeId, Vec<(String, String)>> =
            std::collections::HashMap::new();

        for (cc_id, state) in guard.node_states() {
            if !live.contains(&cc_id) {
                continue;
            }
            let Some(node_id) = decode_chitchat_id(cc_id) else {
                continue;
            };
            let current_best_gen = *best_gen.get(&node_id).unwrap_or(&0);
            if cc_id.generation_id >= current_best_gen {
                best_gen.insert(node_id, cc_id.generation_id);
                let mut node_kvs = Vec::new();
                for (key, val) in state.key_values() {
                    if key.starts_with(prefix) {
                        node_kvs.push((key.to_string(), val.to_string()));
                    }
                }
                best_states.insert(node_id, node_kvs);
            }
        }

        let mut out = Vec::new();
        for (node_id, kvs) in best_states {
            for (k, v) in kvs {
                out.push((node_id, k, v));
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
        let bad = chitchat::ChitchatId::new("foo".to_string(), 0, "127.0.0.1:1".parse().unwrap());
        assert_eq!(decode_chitchat_id(&bad), None);
    }

    #[test]
    fn decode_accepts_valid_format() {
        let good =
            chitchat::ChitchatId::new("node-42".to_string(), 0, "127.0.0.1:1".parse().unwrap());
        assert_eq!(decode_chitchat_id(&good), Some(NodeId(42)));
    }
}
