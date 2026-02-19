//! Distributed recovery manager for Phase 6a.
//!
//! Complements the single-node `RecoveryManager` in `laminar-db` with
//! distributed checkpoint discovery, incremental delta application,
//! source seeking, and integrity verification.
//!
//! ## Recovery sequence
//!
//! 1. **Discover**: Try `read_latest()`, fall back to `list_checkpoints()`
//!    (C9: dual-source discovery catches partial Raft commit failures)
//! 2. **Load manifest**: `load_manifest(id)`
//! 3. **Validate**: Check operator names match the current deployment
//! 4. **Restore state**: For each operator partition — full snapshot or
//!    incremental delta (C1: incremental support)
//! 5. **Seek sources**: Reset source offsets to checkpoint positions
//! 6. **Return** [`RecoveryResult`]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use super::checkpointer::{Checkpointer, CheckpointerError, verify_integrity};
use super::layout::CheckpointId;

/// Trait for operator state that can be restored from a checkpoint.
pub trait Restorable: Send + Sync {
    /// Restore from a full snapshot blob.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::RestoreFailed`] if the data cannot be
    /// deserialized or applied.
    fn restore(&mut self, data: &[u8]) -> Result<(), RecoveryError>;

    /// Apply an incremental delta on top of the current state.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::RestoreFailed`] if the delta cannot be
    /// applied (e.g., base state missing).
    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), RecoveryError>;

    /// Unique operator identifier (must match the key in the manifest).
    fn operator_id(&self) -> &str;
}

/// Trait for source connectors that can seek to a checkpoint offset.
pub trait Seekable: Send + Sync {
    /// Seek the source to a previously checkpointed offset.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::SeekFailed`] if the source cannot seek
    /// to the given offsets (e.g., expired Kafka offsets).
    fn seek(&mut self, offsets: &HashMap<String, String>) -> Result<(), RecoveryError>;

    /// Source identifier (must match the key in the manifest).
    fn source_id(&self) -> &str;
}

/// Trait for source connectors that can seek using a typed [`SourcePosition`].
///
/// This extends the raw string-based [`Seekable`] trait with type-safe
/// position tracking for exactly-once semantics (F-E2E-001).
pub trait TypedSeekable: Send + Sync {
    /// Seek the source to a typed checkpoint position.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError::SeekFailed`] if the source cannot seek
    /// to the given position.
    fn seek_typed(
        &mut self,
        position: &crate::checkpoint::source_offsets::SourcePosition,
    ) -> Result<(), RecoveryError>;

    /// Validate that the source can seek to the given position.
    ///
    /// Returns `false` if the position is unreachable (e.g., Kafka
    /// retention expired, replication slot dropped).
    fn can_seek_to(
        &self,
        position: &crate::checkpoint::source_offsets::SourcePosition,
    ) -> bool {
        let _ = position;
        true
    }

    /// Source identifier (must match the key in the manifest).
    fn source_id(&self) -> &str;
}

/// Configuration for the distributed recovery manager.
#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    /// Maximum number of fallback checkpoint attempts.
    pub max_fallback_attempts: usize,
    /// Whether to verify SHA-256 integrity of loaded artifacts.
    pub verify_integrity: bool,
    /// Overall timeout for the recovery process.
    pub recovery_timeout: Duration,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            max_fallback_attempts: 3,
            verify_integrity: true,
            recovery_timeout: Duration::from_secs(300),
        }
    }
}

/// Successful recovery result.
#[derive(Debug)]
pub struct RecoveryResult {
    /// The checkpoint that was restored.
    pub checkpoint_id: CheckpointId,
    /// The epoch of the restored checkpoint.
    pub epoch: u64,
    /// Global watermark at checkpoint time.
    pub watermark: Option<i64>,
    /// Number of operators whose state was restored.
    pub operators_restored: usize,
    /// Number of sources that were seeked.
    pub sources_seeked: usize,
}

/// Errors from the recovery process.
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    /// No checkpoint exists.
    #[error("no checkpoint available")]
    NoCheckpointAvailable,

    /// All attempted checkpoints were corrupt or invalid.
    #[error("all checkpoints corrupt after {0} attempts")]
    AllCheckpointsCorrupt(usize),

    /// The operator set in the manifest doesn't match the current deployment.
    #[error("operator mismatch: expected {expected:?}, found {found:?}")]
    OperatorMismatch {
        /// Operators expected by the current deployment.
        expected: Vec<String>,
        /// Operators found in the checkpoint manifest.
        found: Vec<String>,
    },

    /// Restoring an operator's state failed.
    #[error("restore failed for operator `{operator}`: {reason}")]
    RestoreFailed {
        /// Operator that failed.
        operator: String,
        /// Cause of the failure.
        reason: String,
    },

    /// Seeking a source to its checkpointed offset failed.
    #[error("seek failed for source `{source_id}`: {reason}")]
    SeekFailed {
        /// Source that failed.
        source_id: String,
        /// Cause of the failure.
        reason: String,
    },

    /// SHA-256 integrity check failed.
    #[error("integrity check failed: {0}")]
    IntegrityFailed(#[from] CheckpointerError),

    /// The recovery process timed out.
    #[error("recovery timed out after {0:?}")]
    Timeout(Duration),
}

/// Distributed recovery manager.
///
/// Discovers the latest valid checkpoint, restores operator state,
/// and seeks source connectors to the checkpoint offsets.
pub struct RecoveryManager {
    checkpointer: Arc<dyn Checkpointer>,
    config: RecoveryConfig,
}

impl RecoveryManager {
    /// Create a new recovery manager.
    pub fn new(checkpointer: Arc<dyn Checkpointer>, config: RecoveryConfig) -> Self {
        Self {
            checkpointer,
            config,
        }
    }

    /// Run the full recovery sequence.
    ///
    /// # Arguments
    ///
    /// * `restorables` — mutable references to operator state objects
    /// * `seekables` — mutable references to source connectors
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError`] if recovery fails (no checkpoint,
    /// corrupt data, operator mismatch, etc.).
    pub async fn recover(
        &self,
        restorables: &mut [&mut dyn Restorable],
        seekables: &mut [&mut dyn Seekable],
    ) -> Result<RecoveryResult, RecoveryError> {
        // Step 1: Discover checkpoint candidates
        let candidates = self.discover_candidates().await?;

        // Step 2: Try each candidate until one succeeds
        let max_attempts = self.config.max_fallback_attempts.min(candidates.len());
        let mut last_error = None;

        for (attempt, id) in candidates.iter().take(max_attempts).enumerate() {
            info!(
                checkpoint_id = %id,
                attempt = attempt + 1,
                "attempting recovery from checkpoint"
            );

            match self
                .try_recover_from(id, restorables, seekables)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!(
                        checkpoint_id = %id,
                        error = %e,
                        "checkpoint recovery failed, trying fallback"
                    );
                    last_error = Some(e);
                }
            }
        }

        // All candidates exhausted
        match last_error {
            Some(e) => {
                warn!(
                    attempts = max_attempts,
                    last_error = %e,
                    "all checkpoint recovery attempts failed"
                );
                Err(RecoveryError::AllCheckpointsCorrupt(max_attempts))
            }
            None => Err(RecoveryError::NoCheckpointAvailable),
        }
    }

    /// Run the full recovery sequence with typed source seeking (F-E2E-001).
    ///
    /// This is the same as [`recover()`](Self::recover) but uses
    /// [`TypedSeekable`] for type-safe source position restoration.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryError`] if recovery fails.
    pub async fn recover_typed(
        &self,
        restorables: &mut [&mut dyn Restorable],
        typed_seekables: &mut [&mut dyn TypedSeekable],
    ) -> Result<RecoveryResult, RecoveryError> {
        let candidates = self.discover_candidates().await?;

        let max_attempts = self.config.max_fallback_attempts.min(candidates.len());
        let mut last_error = None;

        for (attempt, id) in candidates.iter().take(max_attempts).enumerate() {
            info!(
                checkpoint_id = %id,
                attempt = attempt + 1,
                "attempting typed recovery from checkpoint"
            );

            match self
                .try_recover_typed_from(id, restorables, typed_seekables)
                .await
            {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!(
                        checkpoint_id = %id,
                        error = %e,
                        "typed checkpoint recovery failed, trying fallback"
                    );
                    last_error = Some(e);
                }
            }
        }

        match last_error {
            Some(e) => {
                warn!(
                    attempts = max_attempts,
                    last_error = %e,
                    "all typed checkpoint recovery attempts failed"
                );
                Err(RecoveryError::AllCheckpointsCorrupt(max_attempts))
            }
            None => Err(RecoveryError::NoCheckpointAvailable),
        }
    }

    /// Discover checkpoint candidates in priority order (latest first).
    ///
    /// C9: dual-source discovery — tries `read_latest()` first, then
    /// falls back to `list_checkpoints()` to catch partial Raft commit
    /// failures where `_latest` wasn't updated.
    async fn discover_candidates(&self) -> Result<Vec<CheckpointId>, RecoveryError> {
        let mut candidates = Vec::new();

        // Primary: latest pointer
        match self.checkpointer.read_latest().await {
            Ok(Some(id)) => {
                info!(checkpoint_id = %id, "found latest checkpoint pointer");
                candidates.push(id);
            }
            Ok(None) => {
                info!("no latest checkpoint pointer found");
            }
            Err(e) => {
                warn!(error = %e, "failed to read latest pointer, trying list");
            }
        }

        // Secondary: list all checkpoints (reverse chronological)
        match self.checkpointer.list_checkpoints().await {
            Ok(mut ids) => {
                ids.sort();
                ids.reverse(); // Newest first
                let mut seen: std::collections::HashSet<CheckpointId> =
                    candidates.iter().copied().collect();
                for id in ids {
                    if seen.insert(id) {
                        candidates.push(id);
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "failed to list checkpoints");
            }
        }

        if candidates.is_empty() {
            return Err(RecoveryError::NoCheckpointAvailable);
        }

        Ok(candidates)
    }

    /// Attempt recovery from a single checkpoint.
    async fn try_recover_from(
        &self,
        id: &CheckpointId,
        restorables: &mut [&mut dyn Restorable],
        seekables: &mut [&mut dyn Seekable],
    ) -> Result<RecoveryResult, RecoveryError> {
        // Load manifest
        let manifest = self.checkpointer.load_manifest(id).await?;

        // Validate operator set
        let expected: Vec<String> = restorables
            .iter()
            .map(|r| r.operator_id().to_string())
            .collect();
        let found: Vec<String> = manifest.operators.keys().cloned().collect();

        // Check that every restorable has a matching manifest entry.
        // Extra operators in the manifest are tolerable (removed operators),
        // but missing ones are not.
        for op_id in &expected {
            if !manifest.operators.contains_key(op_id) {
                return Err(RecoveryError::OperatorMismatch {
                    expected: expected.clone(),
                    found,
                });
            }
        }

        // Restore operator state
        let mut operators_restored = 0;
        for restorable in restorables.iter_mut() {
            let op_id = restorable.operator_id().to_string();
            if let Some(op_entry) = manifest.operators.get(&op_id) {
                // Sort: full snapshots first, then deltas, to prevent applying
                // deltas to empty state when partition order is non-deterministic.
                let mut partitions = op_entry.partitions.clone();
                partitions.sort_by_key(|p| p.is_delta);
                for partition in &partitions {
                    // Load artifact
                    let data = self
                        .checkpointer
                        .load_artifact(&partition.path)
                        .await?;

                    // Optional integrity check
                    if self.config.verify_integrity {
                        if let Some(expected_sha) = &partition.sha256 {
                            verify_integrity(&partition.path, &data, expected_sha)?;
                        }
                    }

                    // Apply snapshot or delta
                    if partition.is_delta {
                        restorable.apply_delta(&data).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator: op_id.clone(),
                                reason: e.to_string(),
                            }
                        })?;
                    } else {
                        restorable.restore(&data).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator: op_id.clone(),
                                reason: e.to_string(),
                            }
                        })?;
                    }
                }
                operators_restored += 1;
            }
        }

        // Seek sources
        let mut sources_seeked = 0;
        for seekable in seekables.iter_mut() {
            let src_id = seekable.source_id().to_string();
            if let Some(offset_entry) = manifest.source_offsets.get(&src_id) {
                seekable.seek(&offset_entry.offsets).map_err(|e| {
                    RecoveryError::SeekFailed {
                        source_id: src_id,
                        reason: e.to_string(),
                    }
                })?;
                sources_seeked += 1;
            }
        }

        Ok(RecoveryResult {
            checkpoint_id: manifest.checkpoint_id,
            epoch: manifest.epoch,
            watermark: manifest.watermark,
            operators_restored,
            sources_seeked,
        })
    }

    /// Attempt typed recovery from a single checkpoint (F-E2E-001).
    ///
    /// Same as [`try_recover_from`] but converts manifest offset entries
    /// to [`SourcePosition`] and calls [`TypedSeekable::seek_typed`].
    async fn try_recover_typed_from(
        &self,
        id: &CheckpointId,
        restorables: &mut [&mut dyn Restorable],
        typed_seekables: &mut [&mut dyn TypedSeekable],
    ) -> Result<RecoveryResult, RecoveryError> {
        use crate::checkpoint::source_offsets::SourcePosition;

        let manifest = self.checkpointer.load_manifest(id).await?;

        // Validate operator set
        let expected: Vec<String> = restorables
            .iter()
            .map(|r| r.operator_id().to_string())
            .collect();
        let found: Vec<String> = manifest.operators.keys().cloned().collect();
        for op_id in &expected {
            if !manifest.operators.contains_key(op_id) {
                return Err(RecoveryError::OperatorMismatch {
                    expected: expected.clone(),
                    found,
                });
            }
        }

        // Restore operator state (same as untyped path)
        let mut operators_restored = 0;
        for restorable in restorables.iter_mut() {
            let op_id = restorable.operator_id().to_string();
            if let Some(op_entry) = manifest.operators.get(&op_id) {
                // Sort: full snapshots first, then deltas.
                let mut partitions = op_entry.partitions.clone();
                partitions.sort_by_key(|p| p.is_delta);
                for partition in &partitions {
                    let data = self
                        .checkpointer
                        .load_artifact(&partition.path)
                        .await?;

                    if self.config.verify_integrity {
                        if let Some(expected_sha) = &partition.sha256 {
                            verify_integrity(&partition.path, &data, expected_sha)?;
                        }
                    }

                    if partition.is_delta {
                        restorable.apply_delta(&data).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator: op_id.clone(),
                                reason: e.to_string(),
                            }
                        })?;
                    } else {
                        restorable.restore(&data).map_err(|e| {
                            RecoveryError::RestoreFailed {
                                operator: op_id.clone(),
                                reason: e.to_string(),
                            }
                        })?;
                    }
                }
                operators_restored += 1;
            }
        }

        // Typed source seeking
        let mut sources_seeked = 0;
        for seekable in typed_seekables.iter_mut() {
            let src_id = seekable.source_id().to_string();
            if let Some(offset_entry) = manifest.source_offsets.get(&src_id) {
                if let Some(position) = SourcePosition::from_offset_entry(offset_entry) {
                    // Validate seekability
                    if !seekable.can_seek_to(&position) {
                        return Err(RecoveryError::SeekFailed {
                            source_id: src_id,
                            reason: "source reports position is unreachable".into(),
                        });
                    }
                    seekable.seek_typed(&position).map_err(|e| {
                        RecoveryError::SeekFailed {
                            source_id: src_id,
                            reason: e.to_string(),
                        }
                    })?;
                    sources_seeked += 1;
                } else {
                    // Fall back to raw string-based seek via Seekable if
                    // typed parsing fails. If that's not available either,
                    // fail recovery to prevent exactly-once violations.
                    warn!(
                        source_id = %src_id,
                        source_type = %offset_entry.source_type,
                        "could not parse typed position from offset entry"
                    );
                    return Err(RecoveryError::SeekFailed {
                        source_id: src_id,
                        reason: format!(
                            "could not parse typed position for source_type '{}'",
                            offset_entry.source_type
                        ),
                    });
                }
            }
        }

        Ok(RecoveryResult {
            checkpoint_id: manifest.checkpoint_id,
            epoch: manifest.epoch,
            watermark: manifest.watermark,
            operators_restored,
            sources_seeked,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::checkpointer::ObjectStoreCheckpointer;
    use crate::checkpoint::layout::{
        CheckpointManifestV2, CheckpointPaths, OperatorSnapshotEntry,
        PartitionSnapshotEntry, SourceOffsetEntry,
    };
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use sha2::{Digest, Sha256};

    // ---- Test helpers ----

    struct TestRestorable {
        id: String,
        state: Vec<u8>,
        deltas: Vec<Vec<u8>>,
    }

    impl TestRestorable {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                state: Vec::new(),
                deltas: Vec::new(),
            }
        }
    }

    impl Restorable for TestRestorable {
        fn restore(&mut self, data: &[u8]) -> Result<(), RecoveryError> {
            self.state = data.to_vec();
            Ok(())
        }

        fn apply_delta(&mut self, delta: &[u8]) -> Result<(), RecoveryError> {
            self.deltas.push(delta.to_vec());
            self.state.extend_from_slice(delta);
            Ok(())
        }

        fn operator_id(&self) -> &str {
            &self.id
        }
    }

    struct TestSeekable {
        id: String,
        offsets: Option<HashMap<String, String>>,
    }

    impl TestSeekable {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                offsets: None,
            }
        }
    }

    impl Seekable for TestSeekable {
        fn seek(&mut self, offsets: &HashMap<String, String>) -> Result<(), RecoveryError> {
            self.offsets = Some(offsets.clone());
            Ok(())
        }

        fn source_id(&self) -> &str {
            &self.id
        }
    }

    fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    async fn setup_checkpointer() -> Arc<ObjectStoreCheckpointer> {
        let store = Arc::new(InMemory::new());
        let paths = CheckpointPaths::default();
        Arc::new(ObjectStoreCheckpointer::new(store, paths, 4))
    }

    fn make_paths() -> CheckpointPaths {
        CheckpointPaths::default()
    }

    async fn save_checkpoint(
        ckpt: &ObjectStoreCheckpointer,
        id: &CheckpointId,
        epoch: u64,
        operators: Vec<(&str, &[u8], bool)>,
        sources: Vec<(&str, HashMap<String, String>)>,
        watermark: Option<i64>,
    ) {
        let paths = make_paths();
        let mut manifest = CheckpointManifestV2::new(*id, epoch);
        manifest.watermark = watermark;

        for (op_name, data, is_delta) in &operators {
            // save_snapshot/save_delta return the SHA-256 digest, not
            // the path. Compute the path from CheckpointPaths.
            let (artifact_path, digest) = if *is_delta {
                let d = ckpt
                    .save_delta(id, op_name, 0, Bytes::from(data.to_vec()))
                    .await
                    .unwrap();
                (paths.delta(id, op_name, 0), d)
            } else {
                let d = ckpt
                    .save_snapshot(id, op_name, 0, Bytes::from(data.to_vec()))
                    .await
                    .unwrap();
                (paths.snapshot(id, op_name, 0), d)
            };

            manifest.operators.insert(
                op_name.to_string(),
                OperatorSnapshotEntry {
                    partitions: vec![PartitionSnapshotEntry {
                        partition_id: 0,
                        is_delta: *is_delta,
                        path: artifact_path,
                        size_bytes: data.len() as u64,
                        sha256: Some(digest),
                    }],
                    total_bytes: data.len() as u64,
                },
            );
        }

        for (src_name, offsets) in sources {
            manifest.source_offsets.insert(
                src_name.to_string(),
                SourceOffsetEntry {
                    source_type: "test".into(),
                    offsets,
                    epoch,
                },
            );
        }

        ckpt.save_manifest(&manifest).await.unwrap();
        ckpt.update_latest(id).await.unwrap();
    }

    // ---- Tests ----

    #[tokio::test]
    async fn test_recovery_fresh_start() {
        let ckpt = setup_checkpointer().await;
        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());

        let mut op = TestRestorable::new("op1");
        let result = rm
            .recover(&mut [&mut op], &mut [])
            .await;

        assert!(matches!(
            result.unwrap_err(),
            RecoveryError::NoCheckpointAvailable
        ));
    }

    #[tokio::test]
    async fn test_recovery_full_snapshot() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        save_checkpoint(
            &ckpt,
            &id,
            5,
            vec![("op1", b"full_state", false)],
            vec![("kafka", HashMap::from([("p0".into(), "100".into())]))],
            Some(9999),
        )
        .await;

        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());
        let mut op = TestRestorable::new("op1");
        let mut src = TestSeekable::new("kafka");

        let result = rm
            .recover(&mut [&mut op], &mut [&mut src])
            .await
            .unwrap();

        assert_eq!(result.checkpoint_id, id);
        assert_eq!(result.epoch, 5);
        assert_eq!(result.watermark, Some(9999));
        assert_eq!(result.operators_restored, 1);
        assert_eq!(result.sources_seeked, 1);
        assert_eq!(op.state, b"full_state");
        assert_eq!(
            src.offsets.as_ref().unwrap().get("p0"),
            Some(&"100".to_string())
        );
    }

    #[tokio::test]
    async fn test_recovery_incremental() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();
        let paths = make_paths();

        // Full snapshot
        let full_data = b"base_state";
        let full_digest = ckpt
            .save_snapshot(&id, "op1", 0, Bytes::from_static(full_data))
            .await
            .unwrap();

        // Delta
        let delta_data = b"_delta";
        let delta_digest = ckpt
            .save_delta(&id, "op1", 1, Bytes::from_static(delta_data))
            .await
            .unwrap();

        let mut manifest = CheckpointManifestV2::new(id, 10);
        manifest.operators.insert(
            "op1".into(),
            OperatorSnapshotEntry {
                partitions: vec![
                    PartitionSnapshotEntry {
                        partition_id: 0,
                        is_delta: false,
                        path: paths.snapshot(&id, "op1", 0),
                        size_bytes: full_data.len() as u64,
                        sha256: Some(full_digest),
                    },
                    PartitionSnapshotEntry {
                        partition_id: 1,
                        is_delta: true,
                        path: paths.delta(&id, "op1", 1),
                        size_bytes: delta_data.len() as u64,
                        sha256: Some(delta_digest),
                    },
                ],
                total_bytes: (full_data.len() + delta_data.len()) as u64,
            },
        );
        ckpt.save_manifest(&manifest).await.unwrap();
        ckpt.update_latest(&id).await.unwrap();

        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());
        let mut op = TestRestorable::new("op1");

        let result = rm.recover(&mut [&mut op], &mut []).await.unwrap();
        assert_eq!(result.epoch, 10);
        // Full snapshot applied first, then delta appended
        assert_eq!(op.state, b"base_state_delta");
        assert_eq!(op.deltas.len(), 1);
    }

    #[tokio::test]
    async fn test_recovery_integrity_check() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        // Save with wrong digest
        let data = b"real_data";
        let path_str = {
            let paths = CheckpointPaths::default();
            paths.snapshot(&id, "op1", 0)
        };
        ckpt.save_snapshot(&id, "op1", 0, Bytes::from_static(data))
            .await
            .unwrap();

        let mut manifest = CheckpointManifestV2::new(id, 1);
        manifest.operators.insert(
            "op1".into(),
            OperatorSnapshotEntry {
                partitions: vec![PartitionSnapshotEntry {
                    partition_id: 0,
                    is_delta: false,
                    path: path_str,
                    size_bytes: data.len() as u64,
                    sha256: Some("bad_hash_value".into()), // Corrupt digest
                }],
                total_bytes: data.len() as u64,
            },
        );
        ckpt.save_manifest(&manifest).await.unwrap();
        ckpt.update_latest(&id).await.unwrap();

        let rm = RecoveryManager::new(
            ckpt,
            RecoveryConfig {
                max_fallback_attempts: 1,
                ..Default::default()
            },
        );
        let mut op = TestRestorable::new("op1");

        let result = rm.recover(&mut [&mut op], &mut []).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RecoveryError::AllCheckpointsCorrupt(1)
        ));
    }

    #[tokio::test]
    async fn test_recovery_fallback() {
        let ckpt = setup_checkpointer().await;

        // First checkpoint: bad digest (will fail)
        let id1 = CheckpointId::now();
        {
            let path_str = {
                let paths = CheckpointPaths::default();
                paths.snapshot(&id1, "op1", 0)
            };
            ckpt.save_snapshot(&id1, "op1", 0, Bytes::from_static(b"data1"))
                .await
                .unwrap();
            let mut manifest = CheckpointManifestV2::new(id1, 1);
            manifest.operators.insert(
                "op1".into(),
                OperatorSnapshotEntry {
                    partitions: vec![PartitionSnapshotEntry {
                        partition_id: 0,
                        is_delta: false,
                        path: path_str,
                        size_bytes: 5,
                        sha256: Some("corrupted".into()),
                    }],
                    total_bytes: 5,
                },
            );
            ckpt.save_manifest(&manifest).await.unwrap();
        }

        // Small delay so UUID v7 ordering works
        tokio::time::sleep(Duration::from_millis(2)).await;

        // Second checkpoint: correct (will succeed)
        let id2 = CheckpointId::now();
        save_checkpoint(&ckpt, &id2, 2, vec![("op1", b"good_state", false)], vec![], None).await;

        // Set latest to the bad one to test fallback
        ckpt.update_latest(&id1).await.unwrap();

        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());
        let mut op = TestRestorable::new("op1");

        let result = rm.recover(&mut [&mut op], &mut []).await.unwrap();
        // Should have fallen back to id2
        assert_eq!(result.checkpoint_id, id2);
        assert_eq!(op.state, b"good_state");
    }

    #[tokio::test]
    async fn test_recovery_dual_source_discovery() {
        // _latest is missing, but list_checkpoints() finds the checkpoint
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        // Save checkpoint but do NOT update_latest
        let data = b"discovered";
        let digest = sha256_hex(data);
        let path_str = {
            let paths = CheckpointPaths::default();
            paths.snapshot(&id, "op1", 0)
        };
        ckpt.save_snapshot(&id, "op1", 0, Bytes::from_static(data))
            .await
            .unwrap();

        let mut manifest = CheckpointManifestV2::new(id, 7);
        manifest.operators.insert(
            "op1".into(),
            OperatorSnapshotEntry {
                partitions: vec![PartitionSnapshotEntry {
                    partition_id: 0,
                    is_delta: false,
                    path: path_str,
                    size_bytes: data.len() as u64,
                    sha256: Some(digest),
                }],
                total_bytes: data.len() as u64,
            },
        );
        ckpt.save_manifest(&manifest).await.unwrap();
        // Deliberately NOT calling update_latest()

        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());
        let mut op = TestRestorable::new("op1");

        let result = rm.recover(&mut [&mut op], &mut []).await.unwrap();
        assert_eq!(result.checkpoint_id, id);
        assert_eq!(op.state, b"discovered");
    }

    #[tokio::test]
    async fn test_recovery_operator_mismatch() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        // Checkpoint has "op1", but we try to restore "op2"
        save_checkpoint(
            &ckpt,
            &id,
            1,
            vec![("op1", b"state", false)],
            vec![],
            None,
        )
        .await;

        let rm = RecoveryManager::new(
            ckpt,
            RecoveryConfig {
                max_fallback_attempts: 1,
                ..Default::default()
            },
        );
        let mut op = TestRestorable::new("op2");

        let result = rm.recover(&mut [&mut op], &mut []).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            RecoveryError::AllCheckpointsCorrupt(1)
        ));
    }

    // ---- TypedSeekable tests ----

    struct TestTypedSeekable {
        id: String,
        position: Option<crate::checkpoint::source_offsets::SourcePosition>,
        reachable: bool,
    }

    impl TestTypedSeekable {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                position: None,
                reachable: true,
            }
        }

        fn unreachable(id: &str) -> Self {
            Self {
                id: id.to_string(),
                position: None,
                reachable: false,
            }
        }
    }

    impl TypedSeekable for TestTypedSeekable {
        fn seek_typed(
            &mut self,
            position: &crate::checkpoint::source_offsets::SourcePosition,
        ) -> Result<(), RecoveryError> {
            self.position = Some(position.clone());
            Ok(())
        }

        fn can_seek_to(
            &self,
            _position: &crate::checkpoint::source_offsets::SourcePosition,
        ) -> bool {
            self.reachable
        }

        fn source_id(&self) -> &str {
            &self.id
        }
    }

    #[tokio::test]
    async fn test_typed_recovery() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        save_checkpoint(
            &ckpt,
            &id,
            5,
            vec![("op1", b"state", false)],
            vec![("kafka-src", HashMap::from([
                ("group_id".into(), "g1".into()),
                ("events-0".into(), "100".into()),
            ]))],
            Some(8000),
        )
        .await;

        // Update the source_type to "kafka" so typed parsing works
        {
            let mut manifest = ckpt.load_manifest(&id).await.unwrap();
            manifest.source_offsets.get_mut("kafka-src").unwrap().source_type = "kafka".into();
            ckpt.save_manifest(&manifest).await.unwrap();
        }

        let rm = RecoveryManager::new(ckpt, RecoveryConfig::default());
        let mut op = TestRestorable::new("op1");
        let mut src = TestTypedSeekable::new("kafka-src");

        let result = rm
            .recover_typed(&mut [&mut op], &mut [&mut src])
            .await
            .unwrap();

        assert_eq!(result.epoch, 5);
        assert_eq!(result.operators_restored, 1);
        assert_eq!(result.sources_seeked, 1);
        assert!(src.position.is_some());
    }

    #[tokio::test]
    async fn test_typed_recovery_unreachable_position() {
        let ckpt = setup_checkpointer().await;
        let id = CheckpointId::now();

        save_checkpoint(
            &ckpt,
            &id,
            5,
            vec![("op1", b"state", false)],
            vec![("kafka-src", HashMap::from([
                ("group_id".into(), "g1".into()),
                ("events-0".into(), "100".into()),
            ]))],
            None,
        )
        .await;

        // Set source_type to kafka
        {
            let mut manifest = ckpt.load_manifest(&id).await.unwrap();
            manifest.source_offsets.get_mut("kafka-src").unwrap().source_type = "kafka".into();
            ckpt.save_manifest(&manifest).await.unwrap();
        }

        let rm = RecoveryManager::new(
            ckpt,
            RecoveryConfig {
                max_fallback_attempts: 1,
                ..Default::default()
            },
        );
        let mut op = TestRestorable::new("op1");
        let mut src = TestTypedSeekable::unreachable("kafka-src");

        let result = rm
            .recover_typed(&mut [&mut op], &mut [&mut src])
            .await;
        assert!(result.is_err());
    }
}
