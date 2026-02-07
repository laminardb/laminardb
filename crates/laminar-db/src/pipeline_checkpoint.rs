//! Pipeline checkpoint persistence for connector pipelines.
//!
//! Provides [`PipelineCheckpoint`] and [`PipelineCheckpointManager`] to persist
//! source connector offsets and sink epoch state across restarts. Checkpoints
//! are stored as JSON files in a `pipeline_checkpoints/` subdirectory.
//!
//! ## File Layout
//!
//! ```text
//! {data_dir}/pipeline_checkpoints/
//!   checkpoint_000001.json
//!   checkpoint_000002.json
//!   latest.txt  → "checkpoint_000002.json"
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use laminar_connectors::checkpoint::SourceCheckpoint;

use crate::error::DbError;

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
    /// Path to the `RocksDB` table store checkpoint, if any.
    #[serde(default)]
    pub table_store_checkpoint_path: Option<String>,
}

/// Manages pipeline checkpoint persistence on disk.
///
/// Writes JSON checkpoint files to `{data_dir}/pipeline_checkpoints/` and
/// maintains a `latest.txt` pointer (not a symlink) for Windows compatibility.
pub(crate) struct PipelineCheckpointManager {
    checkpoint_dir: PathBuf,
    max_retained: usize,
    current_epoch: u64,
}

impl PipelineCheckpointManager {
    /// Creates a new manager. The checkpoint subdirectory is created lazily on
    /// the first save.
    pub(crate) fn new(data_dir: impl AsRef<Path>, max_retained: usize) -> Self {
        Self {
            checkpoint_dir: data_dir.as_ref().join("pipeline_checkpoints"),
            max_retained,
            current_epoch: 0,
        }
    }

    /// Increments and returns the next epoch number.
    pub(crate) fn next_epoch(&mut self) -> u64 {
        self.current_epoch += 1;
        self.current_epoch
    }

    /// Sets the current epoch (used after recovery).
    pub(crate) fn set_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    /// Returns the current epoch.
    #[cfg(test)]
    pub(crate) fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Persists a checkpoint to disk.
    ///
    /// Writes `checkpoint_{epoch:06}.json`, updates `latest.txt`, and prunes
    /// old files beyond `max_retained`.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` on I/O or serialization failure.
    pub(crate) fn save(&mut self, checkpoint: &PipelineCheckpoint) -> Result<u64, DbError> {
        // Ensure directory exists
        std::fs::create_dir_all(&self.checkpoint_dir).map_err(|e| {
            DbError::Checkpoint(format!(
                "cannot create checkpoint dir {}: {e}",
                self.checkpoint_dir.display()
            ))
        })?;

        let filename = format!("checkpoint_{:06}.json", checkpoint.epoch);
        let filepath = self.checkpoint_dir.join(&filename);

        let json = serde_json::to_string_pretty(checkpoint)
            .map_err(|e| DbError::Checkpoint(format!("checkpoint serialization failed: {e}")))?;

        std::fs::write(&filepath, json).map_err(|e| {
            DbError::Checkpoint(format!("cannot write {}: {e}", filepath.display()))
        })?;

        // Update latest.txt pointer
        let latest_path = self.checkpoint_dir.join("latest.txt");
        std::fs::write(&latest_path, &filename)
            .map_err(|e| DbError::Checkpoint(format!("cannot write latest.txt: {e}")))?;

        self.prune()?;

        Ok(checkpoint.epoch)
    }

    /// Loads the most recent checkpoint from disk.
    ///
    /// Returns `Ok(None)` if no checkpoint exists yet.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` on I/O or deserialization failure.
    pub(crate) fn load_latest(&self) -> Result<Option<PipelineCheckpoint>, DbError> {
        let latest_path = self.checkpoint_dir.join("latest.txt");
        if !latest_path.exists() {
            return Ok(None);
        }

        let filename = std::fs::read_to_string(&latest_path)
            .map_err(|e| DbError::Checkpoint(format!("cannot read latest.txt: {e}")))?;
        let filename = filename.trim();
        if filename.is_empty() {
            return Ok(None);
        }

        let filepath = self.checkpoint_dir.join(filename);
        if !filepath.exists() {
            return Ok(None);
        }

        let json = std::fs::read_to_string(&filepath)
            .map_err(|e| DbError::Checkpoint(format!("cannot read {}: {e}", filepath.display())))?;

        let checkpoint: PipelineCheckpoint = serde_json::from_str(&json)
            .map_err(|e| DbError::Checkpoint(format!("checkpoint deserialization failed: {e}")))?;

        Ok(Some(checkpoint))
    }

    /// Removes old checkpoint files beyond `max_retained`.
    fn prune(&self) -> Result<(), DbError> {
        if self.max_retained == 0 {
            return Ok(());
        }

        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.checkpoint_dir)
            .map_err(|e| DbError::Checkpoint(format!("cannot read checkpoint dir: {e}")))?
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name().and_then(|n| n.to_str()).is_some_and(|n| {
                    n.starts_with("checkpoint_")
                        && Path::new(n)
                            .extension()
                            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                })
            })
            .collect();

        // Sort ascending by filename (epoch-based names sort correctly)
        files.sort();

        if files.len() > self.max_retained {
            let to_remove = files.len() - self.max_retained;
            for path in &files[..to_remove] {
                let _ = std::fs::remove_file(path);
            }
        }

        Ok(())
    }
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

    // -- PipelineCheckpointManager tests --

    #[test]
    fn test_manager_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 3);

        let epoch = mgr.next_epoch();
        let cp = PipelineCheckpoint {
            epoch,
            timestamp_ms: 1_000,
            source_offsets: HashMap::from([(
                "src".to_string(),
                SerializableSourceCheckpoint {
                    offsets: HashMap::from([("p0".to_string(), "42".to_string())]),
                    epoch,
                    metadata: HashMap::new(),
                },
            )]),
            sink_epochs: HashMap::new(),
            table_offsets: HashMap::new(),
            table_store_checkpoint_path: None,
        };

        let saved_epoch = mgr.save(&cp).unwrap();
        assert_eq!(saved_epoch, 1);

        let loaded = mgr.load_latest().unwrap().unwrap();
        assert_eq!(loaded.epoch, 1);
        assert_eq!(
            loaded.source_offsets.get("src").unwrap().offsets.get("p0"),
            Some(&"42".to_string())
        );
    }

    #[test]
    fn test_manager_load_latest_returns_none_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = PipelineCheckpointManager::new(dir.path(), 3);
        assert!(mgr.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_manager_load_latest_returns_most_recent() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 5);

        for i in 1..=3 {
            let epoch = mgr.next_epoch();
            let cp = PipelineCheckpoint {
                epoch,
                timestamp_ms: i * 1_000,
                source_offsets: HashMap::new(),
                sink_epochs: HashMap::new(),
                table_offsets: HashMap::new(),
                table_store_checkpoint_path: None,
            };
            mgr.save(&cp).unwrap();
        }

        let latest = mgr.load_latest().unwrap().unwrap();
        assert_eq!(latest.epoch, 3);
    }

    #[test]
    fn test_manager_prune_respects_max_retained() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 2);

        for _ in 0..5 {
            let epoch = mgr.next_epoch();
            let cp = PipelineCheckpoint {
                epoch,
                timestamp_ms: epoch * 1_000,
                source_offsets: HashMap::new(),
                sink_epochs: HashMap::new(),
                table_offsets: HashMap::new(),
                table_store_checkpoint_path: None,
            };
            mgr.save(&cp).unwrap();
        }

        // Count checkpoint JSON files
        let cp_dir = dir.path().join("pipeline_checkpoints");
        let json_files: Vec<_> = std::fs::read_dir(&cp_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| {
                        n.starts_with("checkpoint_")
                            && std::path::Path::new(n)
                                .extension()
                                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                    })
            })
            .collect();

        assert_eq!(json_files.len(), 2);

        // Latest is still loadable
        let latest = mgr.load_latest().unwrap().unwrap();
        assert_eq!(latest.epoch, 5);
    }

    #[test]
    fn test_manager_epoch_sequencing() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 10);

        assert_eq!(mgr.current_epoch(), 0);
        assert_eq!(mgr.next_epoch(), 1);
        assert_eq!(mgr.next_epoch(), 2);
        assert_eq!(mgr.next_epoch(), 3);
        assert_eq!(mgr.current_epoch(), 3);
    }

    #[test]
    fn test_manager_set_epoch_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 3);

        mgr.set_epoch(10);
        assert_eq!(mgr.current_epoch(), 10);
        assert_eq!(mgr.next_epoch(), 11);
    }

    #[test]
    fn test_manager_multiple_saves_load_latest() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = PipelineCheckpointManager::new(dir.path(), 10);

        let offsets_a = HashMap::from([("p0".to_string(), "10".to_string())]);
        let offsets_b = HashMap::from([("p0".to_string(), "99".to_string())]);

        let e1 = mgr.next_epoch();
        mgr.save(&PipelineCheckpoint {
            epoch: e1,
            timestamp_ms: 1_000,
            source_offsets: HashMap::from([(
                "src".to_string(),
                SerializableSourceCheckpoint {
                    offsets: offsets_a,
                    epoch: e1,
                    metadata: HashMap::new(),
                },
            )]),
            sink_epochs: HashMap::new(),
            table_offsets: HashMap::new(),
            table_store_checkpoint_path: None,
        })
        .unwrap();

        let e2 = mgr.next_epoch();
        mgr.save(&PipelineCheckpoint {
            epoch: e2,
            timestamp_ms: 2_000,
            source_offsets: HashMap::from([(
                "src".to_string(),
                SerializableSourceCheckpoint {
                    offsets: offsets_b,
                    epoch: e2,
                    metadata: HashMap::new(),
                },
            )]),
            sink_epochs: HashMap::new(),
            table_offsets: HashMap::new(),
            table_store_checkpoint_path: None,
        })
        .unwrap();

        let latest = mgr.load_latest().unwrap().unwrap();
        assert_eq!(latest.epoch, 2);
        assert_eq!(
            latest.source_offsets.get("src").unwrap().offsets.get("p0"),
            Some(&"99".to_string())
        );
    }

    #[test]
    fn test_manager_corrupt_file_error() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join("pipeline_checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();

        // Write a corrupt checkpoint file
        std::fs::write(cp_dir.join("checkpoint_000001.json"), "not valid json").unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000001.json").unwrap();

        let mgr = PipelineCheckpointManager::new(dir.path(), 3);
        let result = mgr.load_latest();
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("deserialization"), "got: {err}");
    }

    #[test]
    fn test_manager_latest_points_to_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join("pipeline_checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();

        // latest.txt points to a file that doesn't exist
        std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000099.json").unwrap();

        let mgr = PipelineCheckpointManager::new(dir.path(), 3);
        assert!(mgr.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_manager_empty_latest_txt() {
        let dir = tempfile::tempdir().unwrap();
        let cp_dir = dir.path().join("pipeline_checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();

        std::fs::write(cp_dir.join("latest.txt"), "").unwrap();

        let mgr = PipelineCheckpointManager::new(dir.path(), 3);
        assert!(mgr.load_latest().unwrap().is_none());
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
