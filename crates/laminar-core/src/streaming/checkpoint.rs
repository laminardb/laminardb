//! Streaming checkpoint configuration.

/// Configuration for streaming checkpoints.
#[derive(Debug, Clone, Default)]
pub struct StreamCheckpointConfig {
    /// Checkpoint interval in milliseconds. `None` = manual only.
    pub interval_ms: Option<u64>,
    /// One end-to-end attempt deadline in milliseconds, spanning sink fencing, alignment,
    /// capture, durable publication, and completion delivery. `None` = default (`120_000`).
    pub timeout_ms: Option<u64>,
    /// Directory for persisting checkpoints. `None` uses the database storage directory, then
    /// falls back to `./data`; it never silently selects volatile checkpoint storage.
    pub data_dir: Option<std::path::PathBuf>,
    /// Number of predecessor checkpoints retained alongside the current recovery cut.
    /// `None` = default (3); predecessors keep reference/delta chains resolvable.
    pub max_retained: Option<usize>,
    /// Max epochs admitted between capture and restorable. `None` = default (4).
    /// Exactly-once pipelines are capped at 1 regardless.
    pub max_in_flight_epochs: Option<u64>,
    /// Cap on captured-state bytes held by in-flight epochs; admission
    /// pauses at the cap. `None` = default (512 MiB).
    pub max_staged_bytes: Option<u64>,
    /// Enable incremental delta checkpoints (cluster-only), bounding the re-base chain length.
    /// `None` = off. When set, the per-vnode delta chain is the primary aggregate checkpoint,
    /// dropping per-cycle cost to O(dirty). Requires a durable backend and must be strictly less
    /// than `max_retained` so the chain base never ages out of the prune window.
    pub delta_chain_max: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = StreamCheckpointConfig::default();
        assert!(config.interval_ms.is_none());
        assert!(config.timeout_ms.is_none());
        assert!(config.data_dir.is_none());
        assert!(config.max_retained.is_none());
        assert!(config.max_in_flight_epochs.is_none());
        assert!(config.max_staged_bytes.is_none());
        assert!(config.delta_chain_max.is_none());
    }
}
