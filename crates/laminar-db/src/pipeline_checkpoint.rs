//! Pipeline checkpoint data types for connector pipelines.
//!
//! Provides [`PipelineCheckpoint`] and [`SerializableSourceCheckpoint`] for
//! backward-compatible checkpoint deserialization. The checkpoint coordinator
//! ([`CheckpointCoordinator`](crate::checkpoint_coordinator::CheckpointCoordinator))
//! is the active persistence mechanism.

use std::collections::HashMap;

use laminar_connectors::checkpoint::SourceCheckpoint;

/// A serde-friendly mirror of [`SourceCheckpoint`].
///
/// The connector crate's `SourceCheckpoint` doesn't derive `Serialize`/`Deserialize`,
/// so we use this intermediate type for JSON persistence.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct SerializableSourceCheckpoint {
    /// Connector-specific offset data.
    pub offsets: HashMap<String, String>,
    /// Epoch number this checkpoint belongs to.
    pub epoch: u64,
    /// Optional metadata.
    pub metadata: HashMap<String, String>,
}

impl From<&SourceCheckpoint> for SerializableSourceCheckpoint {
    fn from(cp: &SourceCheckpoint) -> Self {
        Self {
            offsets: cp.offsets().clone(),
            epoch: cp.epoch(),
            metadata: cp.metadata().clone(),
        }
    }
}

impl SerializableSourceCheckpoint {
    /// Converts back to a [`SourceCheckpoint`].
    #[must_use]
    pub fn to_source_checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::with_offsets(self.epoch, self.offsets.clone());
        for (k, v) in &self.metadata {
            cp.set_metadata(k.clone(), v.clone());
        }
        cp
    }
}

/// A point-in-time snapshot of connector pipeline state.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PipelineCheckpoint {
    /// Monotonically increasing epoch number.
    pub epoch: u64,
    /// Timestamp when checkpoint was created (millis since Unix epoch).
    pub timestamp_ms: u64,
    /// Per-source connector offsets (key: source name).
    pub source_offsets: HashMap<String, SerializableSourceCheckpoint>,
    /// Per-sink last committed epoch (key: sink name).
    pub sink_epochs: HashMap<String, u64>,
    /// Per-table source offsets (key: table name). Absent in older checkpoints.
    #[serde(default)]
    pub table_offsets: HashMap<String, SerializableSourceCheckpoint>,
    /// Path to the table store checkpoint, if any.
    #[serde(default)]
    pub table_store_checkpoint_path: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- SerializableSourceCheckpoint tests --

    #[test]
    fn test_serializable_round_trip() {
        let mut cp = SourceCheckpoint::with_offsets(
            5,
            HashMap::from([
                ("partition-0".to_string(), "1234".to_string()),
                ("partition-1".to_string(), "5678".to_string()),
            ]),
        );
        cp.set_metadata("topic", "events");

        let ser = SerializableSourceCheckpoint::from(&cp);
        assert_eq!(ser.epoch, 5);
        assert_eq!(ser.offsets.get("partition-0"), Some(&"1234".to_string()));
        assert_eq!(ser.metadata.get("topic"), Some(&"events".to_string()));

        let restored = ser.to_source_checkpoint();
        assert_eq!(restored.epoch(), 5);
        assert_eq!(restored.get_offset("partition-0"), Some("1234"));
        assert_eq!(restored.get_offset("partition-1"), Some("5678"));
        assert_eq!(restored.get_metadata("topic"), Some("events"));
    }

    #[test]
    fn test_serializable_empty() {
        let cp = SourceCheckpoint::new(0);
        let ser = SerializableSourceCheckpoint::from(&cp);
        assert_eq!(ser.epoch, 0);
        assert!(ser.offsets.is_empty());
        assert!(ser.metadata.is_empty());

        let restored = ser.to_source_checkpoint();
        assert_eq!(restored.epoch(), 0);
        assert!(restored.is_empty());
    }

    // -- PipelineCheckpoint JSON tests --

    #[test]
    fn test_pipeline_checkpoint_json_round_trip() {
        let cp = PipelineCheckpoint {
            epoch: 42,
            timestamp_ms: 1_700_000_000_000,
            source_offsets: HashMap::from([(
                "kafka-source".to_string(),
                SerializableSourceCheckpoint {
                    offsets: HashMap::from([
                        ("partition-0".to_string(), "100".to_string()),
                        ("partition-1".to_string(), "200".to_string()),
                    ]),
                    epoch: 42,
                    metadata: HashMap::from([("topic".to_string(), "events".to_string())]),
                },
            )]),
            sink_epochs: HashMap::from([("pg-sink".to_string(), 41)]),
            table_offsets: HashMap::new(),
            table_store_checkpoint_path: None,
        };

        let json = serde_json::to_string(&cp).unwrap();
        let restored: PipelineCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.epoch, 42);
        assert_eq!(restored.timestamp_ms, 1_700_000_000_000);
        let src = restored.source_offsets.get("kafka-source").unwrap();
        assert_eq!(src.offsets.get("partition-0"), Some(&"100".to_string()));
        assert_eq!(src.metadata.get("topic"), Some(&"events".to_string()));
        assert_eq!(restored.sink_epochs.get("pg-sink"), Some(&41));
    }

    #[test]
    fn test_pipeline_checkpoint_empty() {
        let cp = PipelineCheckpoint {
            epoch: 1,
            timestamp_ms: 0,
            source_offsets: HashMap::new(),
            sink_epochs: HashMap::new(),
            table_offsets: HashMap::new(),
            table_store_checkpoint_path: None,
        };

        let json = serde_json::to_string(&cp).unwrap();
        let restored: PipelineCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.epoch, 1);
        assert!(restored.source_offsets.is_empty());
        assert!(restored.sink_epochs.is_empty());
    }

    // ── table_offsets backward compatibility tests ──

    #[test]
    fn test_checkpoint_with_table_offsets_round_trip() {
        let cp = PipelineCheckpoint {
            epoch: 10,
            timestamp_ms: 2_000,
            source_offsets: HashMap::new(),
            sink_epochs: HashMap::new(),
            table_offsets: HashMap::from([(
                "instruments".to_string(),
                SerializableSourceCheckpoint {
                    offsets: HashMap::from([("lsn".to_string(), "0/ABCD".to_string())]),
                    epoch: 10,
                    metadata: HashMap::new(),
                },
            )]),
            table_store_checkpoint_path: None,
        };

        let json = serde_json::to_string(&cp).unwrap();
        let restored: PipelineCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.table_offsets.len(), 1);
        let tbl = restored.table_offsets.get("instruments").unwrap();
        assert_eq!(tbl.offsets.get("lsn"), Some(&"0/ABCD".to_string()));
        assert_eq!(tbl.epoch, 10);
    }

    #[test]
    fn test_checkpoint_backward_compat_missing_table_offsets() {
        // Old format without table_offsets field should deserialize with empty map
        let json = r#"{
            "epoch": 5,
            "timestamp_ms": 1000,
            "source_offsets": {},
            "sink_epochs": {}
        }"#;

        let cp: PipelineCheckpoint = serde_json::from_str(json).unwrap();
        assert_eq!(cp.epoch, 5);
        assert!(cp.table_offsets.is_empty());
    }
}
