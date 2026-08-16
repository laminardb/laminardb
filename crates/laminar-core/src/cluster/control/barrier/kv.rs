//! Cluster key-value seam and deterministic in-memory implementation.

use super::{async_trait, FxHashMap, Mutex, NodeId};

/// Gossip-KV seam.
#[async_trait]
pub trait ClusterKv: Send + Sync + 'static {
    /// Write `value` to this instance's `key` slot (overwrites).
    async fn write(&self, key: &str, value: String);
    /// Write with transport failure reporting when the backend supports it.
    ///
    /// Fast gossip implementations may use the default because their write API has no result.
    /// Durable control implementations override this so recovery never treats a dropped write as
    /// successful.
    ///
    /// # Errors
    /// Durable implementations return a transport or storage error when the value was not
    /// accepted by their authority.
    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        self.write(key, value).await;
        Ok(())
    }
    /// Read `key` from `who`'s slot.
    async fn read_from(&self, who: NodeId, key: &str) -> Option<String>;
    /// Read with transport failure reporting when the backend supports it.
    ///
    /// # Errors
    /// Durable implementations return a transport or storage error. A genuinely absent key is
    /// `Ok(None)`.
    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        Ok(self.read_from(who, key).await)
    }
    /// Every visible instance's value for `key`.
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)>;
    /// Scan with transport failure reporting when the backend supports it.
    ///
    /// # Errors
    /// Durable implementations fail the whole scan when any visible participant cannot be read.
    async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        Ok(self.scan(key).await)
    }
}

/// In-memory KV for tests.
#[derive(Debug)]
pub struct InMemoryKv {
    local_id: NodeId,
    state: Mutex<FxHashMap<(NodeId, String), String>>,
}

impl InMemoryKv {
    /// Create a new in-memory KV identified as `local_id`.
    #[must_use]
    pub fn new(local_id: NodeId) -> Self {
        Self {
            local_id,
            state: Mutex::new(FxHashMap::default()),
        }
    }

    /// Seed a remote peer's state for tests.
    pub fn seed(&self, peer: NodeId, key: &str, value: String) {
        self.state.lock().insert((peer, key.to_string()), value);
    }
}

#[async_trait]
impl ClusterKv for InMemoryKv {
    async fn write(&self, key: &str, value: String) {
        self.state
            .lock()
            .insert((self.local_id, key.to_string()), value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.state.lock().get(&(who, key.to_string())).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.state
            .lock()
            .iter()
            .filter(|((_, k), _)| k == key)
            .map(|((n, _), v)| (*n, v.clone()))
            .collect()
    }
}
