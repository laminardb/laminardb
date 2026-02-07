//! Connector checkpoint types.
//!
//! Checkpoints capture the position of a source connector so it can
//! resume from where it left off after a restart.

use std::collections::HashMap;

/// Checkpoint state for a source connector.
///
/// Captures the connector's position using string key-value pairs.
/// This is flexible enough to represent:
/// - Kafka: `{"partition-0": "1234", "partition-1": "5678"}`
/// - `PostgreSQL` CDC: `{"lsn": "0/1234ABCD"}`
/// - File: `{"path": "/data/file.csv", "offset": "4096"}`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SourceCheckpoint {
    /// Connector-specific offset data.
    offsets: HashMap<String, String>,

    /// Epoch number this checkpoint belongs to.
    epoch: u64,

    /// Optional metadata for the checkpoint.
    metadata: HashMap<String, String>,
}

impl SourceCheckpoint {
    /// Creates an empty checkpoint.
    #[must_use]
    pub fn new(epoch: u64) -> Self {
        Self {
            offsets: HashMap::new(),
            epoch,
            metadata: HashMap::new(),
        }
    }

    /// Creates a checkpoint with the given offsets.
    #[must_use]
    pub fn with_offsets(epoch: u64, offsets: HashMap<String, String>) -> Self {
        Self {
            offsets,
            epoch,
            metadata: HashMap::new(),
        }
    }

    /// Sets an offset value.
    pub fn set_offset(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.offsets.insert(key.into(), value.into());
    }

    /// Gets an offset value.
    #[must_use]
    pub fn get_offset(&self, key: &str) -> Option<&str> {
        self.offsets.get(key).map(String::as_str)
    }

    /// Returns all offsets.
    #[must_use]
    pub fn offsets(&self) -> &HashMap<String, String> {
        &self.offsets
    }

    /// Returns the epoch number.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Sets metadata on the checkpoint.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    /// Gets metadata from the checkpoint.
    #[must_use]
    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.metadata.get(key).map(String::as_str)
    }

    /// Returns all metadata.
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Returns `true` if the checkpoint has any offsets.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

/// Runtime-managed checkpoint that wraps a source checkpoint with
/// additional tracking information.
#[derive(Debug, Clone)]
pub struct RuntimeCheckpoint {
    /// The source connector's checkpoint.
    pub source: SourceCheckpoint,

    /// Timestamp when this checkpoint was created (millis since epoch).
    pub timestamp_ms: u64,

    /// Name of the connector instance.
    pub connector_name: String,
}

impl RuntimeCheckpoint {
    /// Creates a new runtime checkpoint.
    #[must_use]
    pub fn new(
        source: SourceCheckpoint,
        connector_name: impl Into<String>,
        timestamp_ms: u64,
    ) -> Self {
        Self {
            source,
            timestamp_ms,
            connector_name: connector_name.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_source_checkpoint_basic() {
        let mut cp = SourceCheckpoint::new(1);
        cp.set_offset("partition-0", "1234");
        cp.set_offset("partition-1", "5678");

        assert_eq!(cp.epoch(), 1);
        assert_eq!(cp.get_offset("partition-0"), Some("1234"));
        assert_eq!(cp.get_offset("partition-1"), Some("5678"));
        assert_eq!(cp.get_offset("partition-2"), None);
        assert!(!cp.is_empty());
    }

    #[test]
    fn test_source_checkpoint_with_offsets() {
        let mut offsets = HashMap::new();
        offsets.insert("lsn".to_string(), "0/1234ABCD".to_string());

        let cp = SourceCheckpoint::with_offsets(5, offsets);
        assert_eq!(cp.epoch(), 5);
        assert_eq!(cp.get_offset("lsn"), Some("0/1234ABCD"));
    }

    #[test]
    fn test_source_checkpoint_metadata() {
        let mut cp = SourceCheckpoint::new(1);
        cp.set_metadata("connector", "kafka");
        cp.set_metadata("topic", "events");

        assert_eq!(cp.get_metadata("connector"), Some("kafka"));
        assert_eq!(cp.get_metadata("topic"), Some("events"));
    }

    #[test]
    fn test_runtime_checkpoint() {
        let source = SourceCheckpoint::new(3);
        let rt = RuntimeCheckpoint::new(source, "my-kafka-source", 1_700_000_000_000);

        assert_eq!(rt.source.epoch(), 3);
        assert_eq!(rt.connector_name, "my-kafka-source");
        assert_eq!(rt.timestamp_ms, 1_700_000_000_000);
    }

    #[test]
    fn test_empty_checkpoint() {
        let cp = SourceCheckpoint::new(0);
        assert!(cp.is_empty());
    }
}
