//! Unified checkpoint manifest types.
//!
//! The [`CheckpointManifest`] is the single source of truth for checkpoint state,
//! replacing the previously separate `PipelineCheckpoint`, `DagCheckpointSnapshot`,
//! and `CheckpointMetadata` types. One manifest captures ALL state at a point in
//! time: source offsets, sink epochs, operator state, WAL positions, and watermarks.
//!
//! ## Manifest Format
//!
//! Manifests are serialized as JSON for human readability and debuggability.
//! Large operator state (binary blobs) is stored in a separate `state.bin` file
//! referenced by the manifest.
//!
//! ## Connector Checkpoint
//!
//! The [`ConnectorCheckpoint`] type provides a connector-agnostic offset container
//! that supports all connector types (Kafka partitions, PostgreSQL LSNs, MySQL GTIDs)
//! through string key-value pairs.

use std::collections::HashMap;

/// Per-sink commit status tracked during the checkpoint commit phase.
///
/// After the manifest is persisted (Step 5), sinks start as [`Pending`](Self::Pending).
/// The coordinator updates each sink's status during commit (Step 6) and
/// saves the manifest again. Recovery uses these statuses to determine
/// which sinks need rollback.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum SinkCommitStatus {
    /// Sink has been pre-committed but not yet committed.
    Pending,
    /// Sink commit succeeded.
    Committed,
    /// Sink commit failed.
    Failed(String),
}

/// A point-in-time snapshot of all pipeline state.
///
/// This is the single source of truth for checkpoint persistence, replacing
/// the three previously disconnected checkpoint systems:
/// - `PipelineCheckpoint` (source offsets + sink epochs)
/// - `DagCheckpointSnapshot` (operator state — in-memory only)
/// - `CheckpointMetadata` (WAL position + watermark)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct CheckpointManifest {
    /// Manifest format version (for future evolution).
    pub version: u32,
    /// Unique, monotonically increasing checkpoint ID.
    pub checkpoint_id: u64,
    /// Epoch number for exactly-once coordination.
    pub epoch: u64,
    /// Timestamp when checkpoint was created (millis since Unix epoch).
    pub timestamp_ms: u64,

    // ── Connector State ──
    /// Per-source connector offsets (key: source name).
    #[serde(default)]
    pub source_offsets: HashMap<String, ConnectorCheckpoint>,
    /// Per-sink last committed epoch (key: sink name).
    #[serde(default)]
    pub sink_epochs: HashMap<String, u64>,
    /// Per-sink commit status (key: sink name).
    ///
    /// Populated during the commit phase (Step 6) and saved to the manifest
    /// afterward. Recovery uses this to decide which sinks need rollback
    /// (those with [`SinkCommitStatus::Pending`] or [`SinkCommitStatus::Failed`]).
    #[serde(default)]
    pub sink_commit_statuses: HashMap<String, SinkCommitStatus>,
    /// Per-table source offsets for reference tables (key: table name).
    #[serde(default)]
    pub table_offsets: HashMap<String, ConnectorCheckpoint>,

    // ── Operator State ──
    /// Per-operator checkpoint data (key: operator/node name).
    ///
    /// Small state is inlined as base64. Large state is stored in a separate
    /// `state.bin` file and this map holds only a reference marker.
    #[serde(default)]
    pub operator_states: HashMap<String, OperatorCheckpoint>,

    // ── Storage State ──
    /// Path to the table store checkpoint, if any.
    #[serde(default)]
    pub table_store_checkpoint_path: Option<String>,
    /// WAL position for single-writer mode.
    #[serde(default)]
    pub wal_position: u64,
    /// Per-core WAL positions for thread-per-core mode.
    #[serde(default)]
    pub per_core_wal_positions: Vec<u64>,

    // ── Time State ──
    /// Global watermark at checkpoint time.
    #[serde(default)]
    pub watermark: Option<i64>,
    /// Per-source watermarks (key: source name).
    #[serde(default)]
    pub source_watermarks: HashMap<String, i64>,

    // ── Topology ──
    /// Sorted names of all registered sources at checkpoint time.
    ///
    /// Used during recovery to detect topology changes (added/removed sources)
    /// and warn the operator.
    #[serde(default)]
    pub source_names: Vec<String>,
    /// Sorted names of all registered sinks at checkpoint time.
    #[serde(default)]
    pub sink_names: Vec<String>,

    // ── Pipeline Identity ──
    /// Hash of the pipeline configuration at checkpoint time.
    ///
    /// Computed from SQL queries, source/sink configuration, and connector
    /// options. Recovery logs a warning when this changes, indicating
    /// operator state may be incompatible with the new configuration.
    #[serde(default)]
    pub pipeline_hash: Option<u64>,

    // ── In-flight Data (unaligned checkpoints) ──
    /// In-flight channel data captured during unaligned checkpoints.
    ///
    /// Key: operator name. Value: list of in-flight records from input channels.
    /// Empty for aligned checkpoints. During recovery, these events are replayed
    /// before resuming normal processing.
    #[serde(default)]
    pub inflight_data: HashMap<String, Vec<InFlightRecord>>,

    // ── Metadata ──
    /// Total size of all checkpoint data in bytes (manifest + state.bin).
    #[serde(default)]
    pub size_bytes: u64,
    /// Whether this is an incremental checkpoint (only deltas since parent).
    #[serde(default)]
    pub is_incremental: bool,
    /// Parent checkpoint ID for incremental checkpoints.
    #[serde(default)]
    pub parent_id: Option<u64>,
}

/// Errors found during manifest validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestValidationError {
    /// Human-readable description of the issue.
    pub message: String,
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl CheckpointManifest {
    /// Validates manifest consistency before recovery.
    ///
    /// Returns a list of issues found. An empty list means the manifest is valid.
    /// Callers should treat non-empty results as warnings (recovery may still
    /// proceed) or errors depending on severity.
    #[must_use]
    pub fn validate(&self) -> Vec<ManifestValidationError> {
        let mut errors = Vec::new();

        if self.version == 0 {
            errors.push(ManifestValidationError {
                message: "manifest version is 0".into(),
            });
        }

        if self.checkpoint_id == 0 {
            errors.push(ManifestValidationError {
                message: "checkpoint_id is 0".into(),
            });
        }

        if self.epoch == 0 {
            errors.push(ManifestValidationError {
                message: "epoch is 0".into(),
            });
        }

        if self.timestamp_ms == 0 {
            errors.push(ManifestValidationError {
                message: "timestamp_ms is 0 (missing creation time)".into(),
            });
        }

        // Sink epochs should match sink commit statuses
        for sink_name in self.sink_epochs.keys() {
            if !self.sink_commit_statuses.is_empty()
                && !self.sink_commit_statuses.contains_key(sink_name)
            {
                errors.push(ManifestValidationError {
                    message: format!("sink '{sink_name}' has epoch but no commit status"),
                });
            }
        }

        // Source offsets should reference known sources (if topology is recorded)
        if !self.source_names.is_empty() {
            for name in self.source_offsets.keys() {
                if !self.source_names.contains(name) {
                    errors.push(ManifestValidationError {
                        message: format!("source_offsets contains '{name}' not in source_names"),
                    });
                }
            }
        }

        // Incremental checkpoints must have a parent
        if self.is_incremental && self.parent_id.is_none() {
            errors.push(ManifestValidationError {
                message: "incremental checkpoint has no parent_id".into(),
            });
        }

        errors
    }

    /// Creates a new manifest with the given ID and epoch.
    #[must_use]
    pub fn new(checkpoint_id: u64, epoch: u64) -> Self {
        #[allow(clippy::cast_possible_truncation)] // u64 millis won't overflow until year 584M
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            version: 1,
            checkpoint_id,
            epoch,
            timestamp_ms,
            source_offsets: HashMap::new(),
            sink_epochs: HashMap::new(),
            sink_commit_statuses: HashMap::new(),
            table_offsets: HashMap::new(),
            operator_states: HashMap::new(),
            table_store_checkpoint_path: None,
            wal_position: 0,
            per_core_wal_positions: Vec::new(),
            watermark: None,
            source_watermarks: HashMap::new(),
            source_names: Vec::new(),
            sink_names: Vec::new(),
            pipeline_hash: None,
            inflight_data: HashMap::new(),
            size_bytes: 0,
            is_incremental: false,
            parent_id: None,
        }
    }
}

/// Connector-agnostic offset container.
///
/// Uses string key-value pairs to support all connector types:
/// - **Kafka**: `{"partition-0": "1234", "partition-1": "5678"}`
/// - **`PostgreSQL` CDC**: `{"lsn": "0/1234ABCD"}`
/// - **`MySQL` CDC**: `{"gtid_set": "uuid:1-5", "binlog_file": "mysql-bin.000003"}`
/// - **Delta Lake**: `{"version": "42"}`
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ConnectorCheckpoint {
    /// Connector-specific offset data.
    pub offsets: HashMap<String, String>,
    /// Epoch this checkpoint belongs to.
    pub epoch: u64,
    /// Optional metadata (connector type, topic name, etc.).
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl ConnectorCheckpoint {
    /// Creates a new connector checkpoint with the given epoch.
    #[must_use]
    pub fn new(epoch: u64) -> Self {
        Self {
            offsets: HashMap::new(),
            epoch,
            metadata: HashMap::new(),
        }
    }

    /// Creates a connector checkpoint with pre-populated offsets.
    #[must_use]
    pub fn with_offsets(epoch: u64, offsets: HashMap<String, String>) -> Self {
        Self {
            offsets,
            epoch,
            metadata: HashMap::new(),
        }
    }
}

/// In-flight data record from an unaligned checkpoint.
///
/// Each record represents serialized events buffered in a single input
/// channel at the time of the unaligned snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InFlightRecord {
    /// Input channel index.
    pub input_id: usize,
    /// Base64-encoded serialized event data.
    pub data_b64: String,
}

/// Serialized operator state stored in the manifest.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct OperatorCheckpoint {
    /// Base64-encoded binary state (for small payloads inlined in JSON).
    #[serde(default)]
    pub state_b64: Option<String>,
    /// If true, state is stored externally in the state.bin sidecar file.
    #[serde(default)]
    pub external: bool,
    /// Byte offset into the state.bin file (if external).
    #[serde(default)]
    pub external_offset: u64,
    /// Byte length of the state in the state.bin file (if external).
    #[serde(default)]
    pub external_length: u64,
}

impl OperatorCheckpoint {
    /// Creates an inline operator checkpoint from raw bytes.
    ///
    /// The bytes are base64-encoded for JSON storage.
    #[must_use]
    pub fn inline(data: &[u8]) -> Self {
        use base64::Engine;
        Self {
            state_b64: Some(base64::engine::general_purpose::STANDARD.encode(data)),
            external: false,
            external_offset: 0,
            external_length: 0,
        }
    }

    /// Creates an external reference to state in the sidecar file.
    #[must_use]
    pub fn external(offset: u64, length: u64) -> Self {
        Self {
            state_b64: None,
            external: true,
            external_offset: offset,
            external_length: length,
        }
    }

    /// Decodes the inline state, returning the raw bytes.
    ///
    /// Returns `None` if the state is external or no inline data is present.
    #[must_use]
    pub fn decode_inline(&self) -> Option<Vec<u8>> {
        use base64::Engine;
        self.state_b64
            .as_ref()
            .and_then(|b64| base64::engine::general_purpose::STANDARD.decode(b64).ok())
    }

    /// Creates an `OperatorCheckpoint` from raw bytes using a size threshold.
    ///
    /// If `data.len() <= threshold`, the state is inlined as base64.
    /// If `data.len() > threshold`, the state is marked as external with the
    /// given offset and length, and the raw data is returned for sidecar storage.
    ///
    /// # Arguments
    ///
    /// * `data` — Raw operator state bytes
    /// * `threshold` — Maximum size in bytes for inline storage
    /// * `current_offset` — Byte offset into the sidecar file for this blob
    ///
    /// # Returns
    ///
    /// A tuple of the checkpoint entry and optional raw data for the sidecar.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_bytes(
        data: &[u8],
        threshold: usize,
        current_offset: u64,
    ) -> (Self, Option<Vec<u8>>) {
        if data.len() <= threshold {
            (Self::inline(data), None)
        } else {
            let length = data.len() as u64;
            (Self::external(current_offset, length), Some(data.to_vec()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_new() {
        let m = CheckpointManifest::new(1, 5);
        assert_eq!(m.version, 1);
        assert_eq!(m.checkpoint_id, 1);
        assert_eq!(m.epoch, 5);
        assert!(m.timestamp_ms > 0);
        assert!(m.source_offsets.is_empty());
        assert!(m.sink_epochs.is_empty());
        assert!(m.operator_states.is_empty());
        assert!(!m.is_incremental);
        assert!(m.parent_id.is_none());
    }

    #[test]
    fn test_manifest_json_round_trip() {
        let mut m = CheckpointManifest::new(42, 10);
        m.source_offsets.insert(
            "kafka-src".into(),
            ConnectorCheckpoint::with_offsets(
                10,
                HashMap::from([
                    ("partition-0".into(), "1234".into()),
                    ("partition-1".into(), "5678".into()),
                ]),
            ),
        );
        m.sink_epochs.insert("pg-sink".into(), 9);
        m.watermark = Some(999_000);
        m.wal_position = 4096;
        m.operator_states
            .insert("window-agg".into(), OperatorCheckpoint::inline(b"hello"));

        let json = serde_json::to_string_pretty(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.checkpoint_id, 42);
        assert_eq!(restored.epoch, 10);
        assert_eq!(restored.watermark, Some(999_000));
        assert_eq!(restored.wal_position, 4096);

        let src = restored.source_offsets.get("kafka-src").unwrap();
        assert_eq!(src.offsets.get("partition-0"), Some(&"1234".into()));
        assert_eq!(restored.sink_epochs.get("pg-sink"), Some(&9));

        let op = restored.operator_states.get("window-agg").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"hello");
    }

    #[test]
    fn test_manifest_backward_compat_missing_fields() {
        // Simulate an older manifest with only mandatory fields
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;

        let m: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert_eq!(m.version, 1);
        assert!(m.source_offsets.is_empty());
        assert!(m.sink_epochs.is_empty());
        assert!(m.operator_states.is_empty());
        assert!(m.per_core_wal_positions.is_empty());
        assert!(m.watermark.is_none());
        assert!(!m.is_incremental);
    }

    #[test]
    fn test_connector_checkpoint_new() {
        let cp = ConnectorCheckpoint::new(5);
        assert_eq!(cp.epoch, 5);
        assert!(cp.offsets.is_empty());
        assert!(cp.metadata.is_empty());
    }

    #[test]
    fn test_connector_checkpoint_with_offsets() {
        let offsets = HashMap::from([("lsn".into(), "0/ABCD".into())]);
        let cp = ConnectorCheckpoint::with_offsets(3, offsets);
        assert_eq!(cp.epoch, 3);
        assert_eq!(cp.offsets.get("lsn"), Some(&"0/ABCD".into()));
    }

    #[test]
    fn test_operator_checkpoint_inline() {
        let op = OperatorCheckpoint::inline(b"state-data");
        assert!(!op.external);
        assert!(op.state_b64.is_some());
        assert_eq!(op.decode_inline().unwrap(), b"state-data");
    }

    #[test]
    fn test_operator_checkpoint_external() {
        let op = OperatorCheckpoint::external(1024, 256);
        assert!(op.external);
        assert_eq!(op.external_offset, 1024);
        assert_eq!(op.external_length, 256);
        assert!(op.decode_inline().is_none());
    }

    #[test]
    fn test_operator_checkpoint_empty_inline() {
        let op = OperatorCheckpoint::inline(b"");
        assert_eq!(op.decode_inline().unwrap(), b"");
    }

    #[test]
    fn test_manifest_with_incremental() {
        let mut m = CheckpointManifest::new(5, 5);
        m.is_incremental = true;
        m.parent_id = Some(4);

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert!(restored.is_incremental);
        assert_eq!(restored.parent_id, Some(4));
    }

    #[test]
    fn test_manifest_per_core_wal_positions() {
        let mut m = CheckpointManifest::new(1, 1);
        m.per_core_wal_positions = vec![100, 200, 300, 400];

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.per_core_wal_positions, vec![100, 200, 300, 400]);
    }

    #[test]
    fn test_manifest_table_offsets() {
        let mut m = CheckpointManifest::new(1, 1);
        m.table_offsets.insert(
            "instruments".into(),
            ConnectorCheckpoint::with_offsets(1, HashMap::from([("lsn".into(), "0/ABCD".into())])),
        );
        m.table_store_checkpoint_path = Some("/tmp/rocksdb_cp".into());

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.table_offsets.len(), 1);
        assert_eq!(
            restored.table_store_checkpoint_path.as_deref(),
            Some("/tmp/rocksdb_cp")
        );
    }

    #[test]
    fn test_manifest_topology_fields_round_trip() {
        let mut m = CheckpointManifest::new(1, 1);
        m.source_names = vec!["kafka-clicks".into(), "ws-prices".into()];
        m.sink_names = vec!["pg-sink".into()];

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.source_names, vec!["kafka-clicks", "ws-prices"]);
        assert_eq!(restored.sink_names, vec!["pg-sink"]);
    }

    #[test]
    fn test_manifest_topology_backward_compat() {
        // Older manifests without topology fields should deserialize fine.
        let json = r#"{
            "version": 1,
            "checkpoint_id": 5,
            "epoch": 3,
            "timestamp_ms": 1000
        }"#;
        let m: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert!(m.source_names.is_empty());
        assert!(m.sink_names.is_empty());
    }

    #[test]
    fn test_validate_orphaned_source_offset() {
        let mut m = CheckpointManifest::new(1, 1);
        m.source_names = vec!["a".into(), "b".into()];
        m.source_offsets
            .insert("c".into(), ConnectorCheckpoint::new(1));

        let errors = m.validate();
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("'c' not in source_names")),
            "expected orphaned source offset error: {errors:?}"
        );
    }

    #[test]
    fn test_manifest_pipeline_hash_round_trip() {
        let mut m = CheckpointManifest::new(1, 1);
        m.pipeline_hash = Some(0xDEAD_BEEF_CAFE_1234);

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.pipeline_hash, Some(0xDEAD_BEEF_CAFE_1234));
    }

    #[test]
    fn test_from_bytes_inline() {
        let data = b"small-state";
        let (op, sidecar) = OperatorCheckpoint::from_bytes(data, 1024, 0);
        assert!(!op.external);
        assert!(sidecar.is_none());
        assert_eq!(op.decode_inline().unwrap(), data);
    }

    #[test]
    fn test_from_bytes_external() {
        let data = vec![0xAB; 2048];
        let (op, sidecar) = OperatorCheckpoint::from_bytes(&data, 1024, 512);
        assert!(op.external);
        assert_eq!(op.external_offset, 512);
        assert_eq!(op.external_length, 2048);
        assert!(op.decode_inline().is_none());
        assert_eq!(sidecar.unwrap(), data);
    }

    #[test]
    fn test_from_bytes_at_threshold_boundary() {
        // Exactly at threshold → inline
        let data = vec![0xFF; 100];
        let (op, sidecar) = OperatorCheckpoint::from_bytes(&data, 100, 0);
        assert!(!op.external);
        assert!(sidecar.is_none());
        assert_eq!(op.decode_inline().unwrap(), data);

        // One byte over threshold → external
        let data_over = vec![0xFF; 101];
        let (op2, sidecar2) = OperatorCheckpoint::from_bytes(&data_over, 100, 0);
        assert!(op2.external);
        assert!(sidecar2.is_some());
    }

    #[test]
    fn test_from_bytes_empty_data() {
        let (op, sidecar) = OperatorCheckpoint::from_bytes(b"", 1024, 0);
        assert!(!op.external);
        assert!(sidecar.is_none());
        assert_eq!(op.decode_inline().unwrap(), b"");
    }

    #[test]
    fn test_manifest_inflight_round_trip() {
        use base64::Engine;

        let mut m = CheckpointManifest::new(1, 1);
        let record = InFlightRecord {
            input_id: 2,
            data_b64: base64::engine::general_purpose::STANDARD.encode(b"buffered-event"),
        };
        m.inflight_data.insert("join-op".into(), vec![record]);

        let json = serde_json::to_string_pretty(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.inflight_data.len(), 1);
        let records = restored.inflight_data.get("join-op").unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].input_id, 2);

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&records[0].data_b64)
            .unwrap();
        assert_eq!(decoded, b"buffered-event");
    }

    #[test]
    fn test_manifest_inflight_backward_compat() {
        // Older manifests without inflight_data should deserialize fine.
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;
        let m: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert!(m.inflight_data.is_empty());
    }

    #[test]
    fn test_manifest_pipeline_hash_backward_compat() {
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;
        let m: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert!(m.pipeline_hash.is_none());
    }
}
