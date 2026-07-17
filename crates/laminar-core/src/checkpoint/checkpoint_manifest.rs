//! Checkpoint manifest types.
//!
//! Manifests are JSON for debuggability. Large operator state goes into a
//! separate `state.bin` sidecar referenced by offset/length in the manifest.

#![allow(clippy::disallowed_types)] // cold path: manifest serialization

use std::collections::HashMap;

use crate::state::{KeyGroupCount, LOCAL_KEY_GROUP_COUNT, PARTITIONING_ABI_VERSION};

/// Current checkpoint manifest format. Older manifests are rejected rather
/// than guessed at recovery time.
/// Version 6 binds recovery to the durable key-partitioning ABI.
pub const CHECKPOINT_MANIFEST_VERSION: u32 = 6;

/// Canonical pipeline-identity payload version.
pub const PIPELINE_IDENTITY_VERSION: u16 = 3;

/// SHA-256 identity of the logical pipeline and recovery-state ABI.
///
/// The canonical version is part of the persisted contract: changing canonicalization or state
/// compatibility requires a new version rather than silently comparing unlike digests.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub struct PipelineIdentity {
    /// Version of the canonical payload format.
    pub canonical_version: u16,
    /// Exactly 64 lowercase hexadecimal characters.
    pub sha256: String,
}

impl PipelineIdentity {
    /// Identity of an empty canonical payload, used by manifest-only tests and empty runtimes.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        }
    }

    /// Validate the persisted identity format.
    pub(crate) fn validation_error(&self) -> Option<String> {
        if self.canonical_version != PIPELINE_IDENTITY_VERSION {
            return Some(format!(
                "unsupported pipeline identity version {}; expected {PIPELINE_IDENTITY_VERSION}",
                self.canonical_version
            ));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Some("pipeline identity must be 64 lowercase hexadecimal characters".into());
        }
        None
    }

    /// Whether this identity uses the current canonical version and digest encoding.
    #[must_use]
    pub(crate) fn is_canonical(&self) -> bool {
        self.validation_error().is_none()
    }
}

/// Durable publication state of a checkpoint manifest.
///
/// `Prepared` records are inventory, not an independent commit signal. Recovery may promote one
/// only when the exact durable checkpoint decision exists. `Finalized` records are published
/// recovery candidates, still subject to that decision whenever a decision store is configured.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DurableCheckpointPhase {
    /// State has been captured but the checkpoint has not completed.
    Prepared,
    /// The checkpoint completed and is eligible for recovery.
    Finalized,
}

/// A point-in-time snapshot of all pipeline state.
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
    /// Durable publication state. This field is intentionally required.
    pub durable_phase: DurableCheckpointPhase,
    /// Writer identity (`0` outside cluster mode).
    pub participant_id: u64,

    // ── Connector State ──
    /// Per-source connector offsets (key: source name).
    #[serde(default)]
    pub source_offsets: HashMap<String, ConnectorCheckpoint>,
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
    /// Required, deterministic identity of the logical topology and state ABI.
    pub pipeline_identity: PipelineIdentity,
    /// Create-once checkpoint/decision-store incarnation. A storage reset rotates this value so
    /// surviving external sink cursors cannot be reused by a fresh checkpoint-id sequence.
    pub deployment_id: String,

    // ── Metadata ──
    /// Durable key encoding, hashing, and key-group mapping contract.
    pub partitioning_abi_version: u16,
    /// Virtual partition count for state key distribution.
    #[serde(default)]
    pub vnode_count: u16,

    // ── Integrity ──
    /// SHA-256 hex digest of the sidecar `state.bin` file (if any).
    ///
    /// Written during checkpoint commit so that recovery can verify the
    /// sidecar hasn't been corrupted or truncated on disk/S3.
    #[serde(default)]
    pub state_checksum: Option<String>,
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
    /// `expected_key_group_count` is the runtime's configured key-group count;
    /// a manifest written with a different count can't be safely restored
    /// because state keys won't map to the same shards.
    ///
    /// Returns a list of issues found. An empty list means the manifest is valid.
    /// Every returned issue makes the manifest ineligible for recovery.
    #[must_use]
    pub fn validate(
        &self,
        expected_key_group_count: KeyGroupCount,
    ) -> Vec<ManifestValidationError> {
        let mut errors = Vec::new();

        if self.version != CHECKPOINT_MANIFEST_VERSION {
            errors.push(ManifestValidationError {
                message: format!(
                    "unsupported manifest version {}; expected {CHECKPOINT_MANIFEST_VERSION}",
                    self.version
                ),
            });
        }

        if self.partitioning_abi_version != PARTITIONING_ABI_VERSION {
            errors.push(ManifestValidationError {
                message: format!(
                    "partitioning ABI mismatch: checkpoint has {}, runtime expects {PARTITIONING_ABI_VERSION}",
                    self.partitioning_abi_version
                ),
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

        if let Some(message) = self.pipeline_identity.validation_error() {
            errors.push(ManifestValidationError { message });
        }
        if !self.deployment_id.is_empty() {
            let valid = uuid::Uuid::parse_str(&self.deployment_id)
                .is_ok_and(|id| !id.is_nil() && id.to_string() == self.deployment_id);
            if !valid {
                errors.push(ManifestValidationError {
                    message: "deployment_id must be a canonical non-nil UUID".into(),
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

        if self.vnode_count == 0 {
            errors.push(ManifestValidationError {
                message: "vnode_count is 0 (missing or legacy checkpoint)".into(),
            });
        } else if self.vnode_count != expected_key_group_count.get() {
            errors.push(ManifestValidationError {
                message: format!(
                    "vnode_count mismatch: checkpoint has {}, runtime expects {expected_key_group_count}",
                    self.vnode_count,
                ),
            });
        }

        if !self.operator_states.is_empty() && self.state_checksum.is_none() {
            errors.push(ManifestValidationError {
                message: "operator state is missing its integrity checksum".into(),
            });
        }

        errors
    }

    /// Creates a new manifest for an embedded or single-node runtime.
    #[must_use]
    pub fn new(checkpoint_id: u64, epoch: u64) -> Self {
        Self::new_with_key_group_count(checkpoint_id, epoch, LOCAL_KEY_GROUP_COUNT)
    }

    /// Creates a new manifest with an explicit stable key-group count.
    #[must_use]
    pub fn new_with_key_group_count(
        checkpoint_id: u64,
        epoch: u64,
        key_group_count: KeyGroupCount,
    ) -> Self {
        #[allow(clippy::cast_possible_truncation)] // u64 millis won't overflow until year 584M
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            version: CHECKPOINT_MANIFEST_VERSION,
            checkpoint_id,
            epoch,
            timestamp_ms,
            durable_phase: DurableCheckpointPhase::Prepared,
            participant_id: 0,
            source_offsets: HashMap::new(),
            table_offsets: HashMap::new(),
            operator_states: HashMap::new(),
            table_store_checkpoint_path: None,
            watermark: None,
            source_watermarks: HashMap::new(),
            source_names: Vec::new(),
            sink_names: Vec::new(),
            pipeline_identity: PipelineIdentity::empty(),
            deployment_id: String::new(),
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnode_count: key_group_count.get(),
            state_checksum: None,
        }
    }
}

/// Connector-agnostic offset container.
///
/// Uses string key-value pairs to support all connector types:
/// - **Kafka**: `{"events:0": "1234", "events:1": "5678"}`
/// - **`PostgreSQL` CDC**: `{"lsn": "0/1234ABCD"}`
/// - **Delta Lake**: `{"version": "42"}`
///
/// The containing [`CheckpointManifest`] supplies the exact attempt identity;
/// duplicating its epoch in each connector payload would create conflicting
/// authorities during recovery.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ConnectorCheckpoint {
    /// Connector-specific offset data.
    pub offsets: HashMap<String, String>,
    /// Optional metadata (connector type, topic name, etc.).
    pub metadata: HashMap<String, String>,
    /// Provider-neutral source-assignment version that owns this offset cut.
    ///
    /// `None` is valid for sources that do not participate in partition assignment. Cluster
    /// recovery validates populated versions against the checkpoint assignment fence.
    pub source_assignment_version: Option<std::num::NonZeroU64>,
}

impl ConnectorCheckpoint {
    /// Creates an empty connector checkpoint.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a connector checkpoint with pre-populated offsets.
    #[must_use]
    pub fn with_offsets(offsets: HashMap<String, String>) -> Self {
        Self {
            offsets,
            metadata: HashMap::new(),
            source_assignment_version: None,
        }
    }
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
    /// Returns `None` if the state is external, no inline data is present,
    /// or if the base64 data is corrupted (logs a warning in that case).
    #[must_use]
    pub fn decode_inline(&self) -> Option<Vec<u8>> {
        use base64::Engine;
        self.state_b64.as_ref().and_then(|b64| {
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(data) => Some(data),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        b64_len = b64.len(),
                        "[LDB-4004] Failed to decode inline operator state from base64 — \
                         operator will start from scratch"
                    );
                    None
                }
            }
        })
    }

    /// Decodes the inline state, returning a `Result` for callers that need
    /// to distinguish between "no inline state" and "corrupted state".
    ///
    /// Returns `Ok(None)` if no inline data is present (external or absent).
    /// Returns `Ok(Some(bytes))` on successful decode.
    ///
    /// # Errors
    ///
    /// Returns `Err` if base64 data is present but corrupted.
    pub fn try_decode_inline(&self) -> Result<Option<Vec<u8>>, String> {
        use base64::Engine;
        match &self.state_b64 {
            None => Ok(None),
            Some(b64) => base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map(Some)
                .map_err(|e| format!("[LDB-4004] base64 decode failed: {e}")),
        }
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

    /// Shared-buffer variant of [`Self::from_bytes`].
    ///
    /// Takes an owned [`bytes::Bytes`] and returns the same type on the
    /// external path, avoiding the `data.to_vec()` copy the `&[u8]`
    /// version has to make. The checkpoint pipeline passes rkyv output
    /// through as `Bytes`, so per-operator state no longer doubles in
    /// memory when crossing this boundary.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn from_bytes_shared(
        data: bytes::Bytes,
        threshold: usize,
        current_offset: u64,
    ) -> (Self, Option<bytes::Bytes>) {
        if data.len() <= threshold {
            (Self::inline(&data), None)
        } else {
            let length = data.len() as u64;
            (Self::external(current_offset, length), Some(data))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_new() {
        let m = CheckpointManifest::new(1, 5);
        assert_eq!(m.version, CHECKPOINT_MANIFEST_VERSION);
        assert_eq!(m.partitioning_abi_version, PARTITIONING_ABI_VERSION);
        assert_eq!(m.durable_phase, DurableCheckpointPhase::Prepared);
        assert_eq!(m.checkpoint_id, 1);
        assert_eq!(m.epoch, 5);
        assert_eq!(m.vnode_count, LOCAL_KEY_GROUP_COUNT.get());
        assert!(m.timestamp_ms > 0);
        assert!(m.source_offsets.is_empty());
        assert!(m.operator_states.is_empty());
    }

    #[test]
    fn test_manifest_new_with_explicit_key_group_count() {
        let key_group_count = KeyGroupCount::try_from(256_u16).unwrap();
        let manifest = CheckpointManifest::new_with_key_group_count(1, 5, key_group_count);

        assert_eq!(manifest.vnode_count, key_group_count.get());
        assert!(manifest.validate(key_group_count).is_empty());
        assert!(!manifest.validate(LOCAL_KEY_GROUP_COUNT).is_empty());
    }

    #[test]
    fn test_manifest_json_round_trip() {
        let mut m = CheckpointManifest::new(42, 10);
        let mut source_checkpoint = ConnectorCheckpoint::with_offsets(HashMap::from([
            ("events:0".into(), "1234".into()),
            ("events:1".into(), "5678".into()),
        ]));
        source_checkpoint.source_assignment_version = std::num::NonZeroU64::new(12);
        m.source_offsets
            .insert("kafka-src".into(), source_checkpoint);
        m.watermark = Some(999_000);
        m.operator_states
            .insert("window-agg".into(), OperatorCheckpoint::inline(b"hello"));

        let json = serde_json::to_string_pretty(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.checkpoint_id, 42);
        assert_eq!(restored.epoch, 10);
        assert_eq!(restored.watermark, Some(999_000));
        let src = restored.source_offsets.get("kafka-src").unwrap();
        assert_eq!(src.offsets.get("events:0"), Some(&"1234".into()));
        assert_eq!(src.source_assignment_version, std::num::NonZeroU64::new(12));

        let op = restored.operator_states.get("window-agg").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"hello");
    }

    #[test]
    fn test_manifest_rejects_previous_version() {
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest.version = CHECKPOINT_MANIFEST_VERSION - 1;
        let restored: CheckpointManifest =
            serde_json::from_str(&serde_json::to_string(&manifest).unwrap()).unwrap();
        let errors = restored.validate(LOCAL_KEY_GROUP_COUNT);
        let previous = CHECKPOINT_MANIFEST_VERSION - 1;
        assert!(
            errors.iter().any(|error| error
                .message
                .contains(&format!("unsupported manifest version {previous}"))),
            "{errors:?}"
        );
    }

    #[test]
    fn test_connector_checkpoint_new() {
        let cp = ConnectorCheckpoint::new();
        assert!(cp.offsets.is_empty());
        assert!(cp.metadata.is_empty());
        assert_eq!(cp.source_assignment_version, None);
    }

    #[test]
    fn test_connector_checkpoint_with_offsets() {
        let offsets = HashMap::from([("lsn".into(), "0/ABCD".into())]);
        let cp = ConnectorCheckpoint::with_offsets(offsets);
        assert_eq!(cp.offsets.get("lsn"), Some(&"0/ABCD".into()));
        assert_eq!(cp.source_assignment_version, None);
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
    fn test_manifest_table_offsets() {
        let mut m = CheckpointManifest::new(1, 1);
        m.table_offsets.insert(
            "instruments".into(),
            ConnectorCheckpoint::with_offsets(HashMap::from([("lsn".into(), "0/ABCD".into())])),
        );
        m.table_store_checkpoint_path = Some("/tmp/table_store_cp".into());

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.table_offsets.len(), 1);
        assert_eq!(
            restored.table_store_checkpoint_path.as_deref(),
            Some("/tmp/table_store_cp")
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
    fn test_manifest_requires_durable_phase() {
        let json = r#"{
            "version": 1,
            "checkpoint_id": 5,
            "epoch": 3,
            "timestamp_ms": 1000
        }"#;
        assert!(serde_json::from_str::<CheckpointManifest>(json).is_err());
    }

    #[test]
    fn test_manifest_requires_pipeline_identity() {
        let manifest = CheckpointManifest::new(5, 3);
        let mut value = serde_json::to_value(manifest).unwrap();
        value.as_object_mut().unwrap().remove("pipeline_identity");
        assert!(serde_json::from_value::<CheckpointManifest>(value).is_err());
    }

    #[test]
    fn test_manifest_requires_partitioning_abi() {
        let manifest = CheckpointManifest::new(5, 3);
        let mut value = serde_json::to_value(manifest).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("partitioning_abi_version");
        assert!(serde_json::from_value::<CheckpointManifest>(value).is_err());
    }

    #[test]
    fn test_manifest_rejects_wrong_partitioning_abi() {
        let mut manifest = CheckpointManifest::new(5, 3);
        manifest.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;

        let errors = manifest.validate(LOCAL_KEY_GROUP_COUNT);
        assert!(
            errors
                .iter()
                .any(|error| error.message.contains("partitioning ABI mismatch")),
            "{errors:?}"
        );
    }

    #[test]
    fn test_validate_orphaned_source_offset() {
        let mut m = CheckpointManifest::new(1, 1);
        m.source_names = vec!["a".into(), "b".into()];
        m.source_offsets
            .insert("c".into(), ConnectorCheckpoint::new());

        let errors = m.validate(LOCAL_KEY_GROUP_COUNT);
        assert!(
            errors
                .iter()
                .any(|e| e.message.contains("'c' not in source_names")),
            "expected orphaned source offset error: {errors:?}"
        );
    }

    #[test]
    fn test_manifest_pipeline_identity_round_trip() {
        let mut m = CheckpointManifest::new(1, 1);
        m.pipeline_identity = PipelineIdentity {
            canonical_version: PIPELINE_IDENTITY_VERSION,
            sha256: "ab".repeat(32),
        };

        let json = serde_json::to_string(&m).unwrap();
        let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.pipeline_identity, m.pipeline_identity);
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
    fn test_manifest_rejects_missing_v2_fields() {
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;
        assert!(serde_json::from_str::<CheckpointManifest>(json).is_err());
    }
}
