//! Streaming checkpoint configuration.

/// Configuration for streaming checkpoints.
#[derive(Debug, Clone, Default)]
pub struct StreamCheckpointConfig {
    /// Checkpoint interval in milliseconds. `None` = manual only.
    pub interval_ms: Option<u64>,
    /// While a recent committed read waits for a newer checkpoint, the next periodic checkpoint
    /// starts this long after the previous one ended instead of `interval_ms`. Must be shorter
    /// than `interval_ms`. `None` = recent reads wait for the periodic cadence.
    pub recent_read_interval_ms: Option<u64>,
    /// One end-to-end attempt deadline in milliseconds, spanning sink fencing, alignment,
    /// capture, durable publication, and completion delivery. `None` = default (`120_000`).
    pub timeout_ms: Option<u64>,
    /// Directory for persisting checkpoints. `None` uses the database storage directory, then
    /// falls back to `./data`; it never silently selects volatile checkpoint storage.
    pub data_dir: Option<std::path::PathBuf>,
    /// Maximum bytes admitted for one participant's checkpoint node-data object.
    /// `None` uses `DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES`.
    pub max_node_data_bytes: Option<u64>,
}

#[cfg(test)]
mod tests;
