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
    /// Maximum bytes admitted for one checkpoint across in-flight capture and
    /// persisted/restored external state. `None` uses
    /// `DEFAULT_MAX_CHECKPOINT_STATE_BYTES`.
    pub max_staged_bytes: Option<u64>,
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
        assert!(config.max_staged_bytes.is_none());
    }
}
