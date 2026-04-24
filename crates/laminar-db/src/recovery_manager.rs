//! Unified recovery manager.
//!
//! Single recovery path that loads a
//! [`CheckpointManifest`](laminar_storage::checkpoint_manifest::CheckpointManifest) and restores
//! ALL state: source offsets, sink epochs, operator states, table offsets,
//! and watermarks.
//!
//! ## Recovery Protocol
//!
//! 1. `store.load_latest()` → `Option<CheckpointManifest>`
//! 2. If `None` → fresh start (no recovery needed)
//! 3. For each source: `source.restore(manifest.source_offsets[name])`
//! 4. For each table source: `source.restore(manifest.table_offsets[name])`
//! 5. For each exactly-once sink: `sink.rollback_epoch(manifest.epoch)`
//! 6. Return recovered state (watermark, epoch, operator states)
//!    — caller is responsible for restoring DAG/TPC operators from `operator_states`
//!
//! ## Fallback Recovery
//!
//! If the latest checkpoint is corrupt or fails to restore, the manager
//! iterates through all available checkpoints in reverse chronological order
//! until one succeeds. This prevents a single corrupt checkpoint from causing
//! total data loss.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;

use laminar_storage::checkpoint_manifest::{CheckpointManifest, SinkCommitStatus};
use laminar_storage::checkpoint_store::CheckpointStore;
use laminar_storage::ValidationResult;
use tracing::{debug, error, info, warn};

use crate::checkpoint_coordinator::{
    connector_to_source_checkpoint, RegisteredSink, RegisteredSource,
};
use crate::error::DbError;

/// Result of a successful recovery from a checkpoint.
#[derive(Debug)]
pub struct RecoveredState {
    /// The manifest that was loaded and restored from.
    pub manifest: CheckpointManifest,
    /// Number of sources successfully restored.
    pub sources_restored: usize,
    /// Number of table sources successfully restored.
    pub tables_restored: usize,
    /// Number of sinks rolled back.
    pub sinks_rolled_back: usize,
    /// Sources that failed to restore (name → error message).
    pub source_errors: HashMap<String, String>,
    /// Sinks that failed to roll back (name → error message).
    pub sink_errors: HashMap<String, String>,
}

impl RecoveredState {
    /// Returns the recovered epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    /// Returns the recovered watermark.
    #[must_use]
    pub fn watermark(&self) -> Option<i64> {
        self.manifest.watermark
    }

    /// Returns whether there were any errors during recovery.
    #[must_use]
    pub fn has_errors(&self) -> bool {
        !self.source_errors.is_empty() || !self.sink_errors.is_empty()
    }

    /// Returns the recovered operator states (for DAG restoration).
    #[must_use]
    pub fn operator_states(
        &self,
    ) -> &HashMap<String, laminar_storage::checkpoint_manifest::OperatorCheckpoint> {
        &self.manifest.operator_states
    }

    /// Returns the table store checkpoint path, if any.
    #[must_use]
    pub fn table_store_checkpoint_path(&self) -> Option<&str> {
        self.manifest.table_store_checkpoint_path.as_deref()
    }
}

/// Recovery manager.
///
/// Loads the latest [`CheckpointManifest`] from a [`CheckpointStore`] and
/// restores all registered sources, sinks, and tables to their checkpointed
/// state.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
    /// When true, any source/sink restore failure aborts the entire recovery.
    /// When false (default), failures are logged and recorded in `RecoveredState`
    /// but recovery continues — the pipeline resumes with potentially
    /// mismatched offsets.
    strict: bool,
}

impl<'a> RecoveryManager<'a> {
    /// Creates a new recovery manager using the given checkpoint store.
    ///
    /// Defaults to strict mode: any source/sink restore failure aborts
    /// the entire recovery. Use [`Self::lenient`] to allow partial recovery.
    #[must_use]
    pub fn new(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            strict: true,
        }
    }

    /// Creates a lenient recovery manager.
    ///
    /// In lenient mode, source/sink restore failures are logged and
    /// recorded in `RecoveredState` but do not abort recovery. The
    /// pipeline resumes with potentially mismatched offsets.
    #[must_use]
    pub fn lenient(store: &'a dyn CheckpointStore) -> Self {
        Self {
            store,
            strict: false,
        }
    }

    /// Attempts to recover from the latest checkpoint, with fallback to older
    /// checkpoints if the latest is corrupt or fails to restore.
    ///
    /// Returns `Ok(None)` if no checkpoint exists (fresh start).
    /// Returns `Ok(Some(RecoveredState))` on successful recovery.
    ///
    /// Recovery is best-effort: individual source/sink failures are recorded
    /// in `RecoveredState` but do not abort the entire recovery. This allows
    /// partial recovery (e.g., one source fails to seek but others succeed).
    ///
    /// ## Fallback Behavior
    ///
    /// If the latest checkpoint fails (corrupt manifest, deserialization error),
    /// the manager iterates through all available checkpoints in reverse
    /// chronological order until one succeeds or all are exhausted. This
    /// prevents a single corrupt checkpoint from causing total data loss.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the checkpoint store fails, or
    /// in strict mode, if any source/sink restore fails.
    pub(crate) async fn recover(
        &self,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
    ) -> Result<Option<RecoveredState>, DbError> {
        // Fast path: try load_latest() first.
        match self.store.load_latest().await {
            Ok(Some(manifest)) => {
                if self.is_checkpoint_corrupt(&manifest).await {
                    warn!(
                        checkpoint_id = manifest.checkpoint_id,
                        "[LDB-6010] latest checkpoint corrupt, trying fallback"
                    );
                } else if Self::has_pending_sinks(&manifest) {
                    warn!(
                        checkpoint_id = manifest.checkpoint_id,
                        epoch = manifest.epoch,
                        "[LDB-6015] checkpoint has uncommitted sinks — source offsets \
                         may be past uncommitted data, falling back to previous checkpoint"
                    );
                } else {
                    let state = self
                        .restore_from(manifest, sources, sinks, table_sources)
                        .await;
                    if let Err(e) = self.check_strict(&state) {
                        warn!(
                            checkpoint_id = state.manifest.checkpoint_id,
                            error = %e,
                            "latest checkpoint restore had strict errors, trying fallback"
                        );
                    } else {
                        return Ok(Some(state));
                    }
                }
            }
            Ok(None) => {
                info!("no checkpoint found, starting fresh");
                return Ok(None);
            }
            Err(e) => {
                warn!(error = %e, "latest checkpoint load failed, trying fallback");
            }
        }

        // Fallback: iterate through all checkpoints in reverse order.
        let checkpoints = self.store.list().await.map_err(DbError::from)?;

        if checkpoints.is_empty() {
            warn!("no checkpoints available for fallback, starting fresh");
            return Ok(None);
        }

        for &(checkpoint_id, _epoch) in checkpoints.iter().rev() {
            match self.store.load_by_id(checkpoint_id).await {
                Ok(Some(manifest)) => {
                    if self.is_checkpoint_corrupt(&manifest).await {
                        warn!(
                            checkpoint_id,
                            "[LDB-6010] fallback checkpoint corrupt, skipping"
                        );
                        continue;
                    }
                    if Self::has_pending_sinks(&manifest) {
                        warn!(
                            checkpoint_id,
                            "[LDB-6015] fallback checkpoint has uncommitted sinks, skipping"
                        );
                        continue;
                    }
                    info!(checkpoint_id, "recovering from fallback checkpoint");
                    let state = self
                        .restore_from(manifest, sources, sinks, table_sources)
                        .await;
                    if let Err(e) = self.check_strict(&state) {
                        warn!(
                            checkpoint_id,
                            error = %e,
                            "fallback checkpoint restore had strict errors, trying next"
                        );
                        continue;
                    }
                    return Ok(Some(state));
                }
                Ok(None) => {
                    debug!(checkpoint_id, "fallback checkpoint not found, skipping");
                }
                Err(e) => {
                    warn!(
                        checkpoint_id,
                        error = %e,
                        "fallback checkpoint load failed, trying next"
                    );
                }
            }
        }

        warn!("all checkpoints failed to load, starting fresh");
        Ok(None)
    }

    /// Resolves external operator states by loading the sidecar file.
    ///
    /// For any operator state marked as `external`, loads the corresponding
    /// bytes from `state.bin` and replaces it with an inline entry. This
    /// makes the rest of recovery code work uniformly with inline state.
    ///
    /// Returns `true` if all external states were resolved successfully.
    /// Returns `false` if any state could not be resolved (missing sidecar,
    /// truncated sidecar, or I/O error). In strict mode, the caller should
    /// treat a `false` return as a corrupt checkpoint and try fallback.
    async fn resolve_external_states(&self, manifest: &mut CheckpointManifest) -> bool {
        let external_ops: Vec<String> = manifest
            .operator_states
            .iter()
            .filter(|(_, op)| op.external)
            .map(|(name, _)| name.clone())
            .collect();

        if external_ops.is_empty() {
            return true;
        }

        let state_data = match self.store.load_state_data(manifest.checkpoint_id).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    operators = ?external_ops,
                    "[LDB-6010] sidecar state.bin missing — external operator states \
                     cannot be resolved; operators will start with empty state"
                );
                // Clear external flag so recovery doesn't attempt to
                // dereference invalid offsets later
                for name in &external_ops {
                    if let Some(op) = manifest.operator_states.get_mut(name) {
                        *op = laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(&[]);
                    }
                }
                return false;
            }
            Err(e) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %e,
                    operators = ?external_ops,
                    "[LDB-6010] failed to load sidecar state.bin — external operator states \
                     cannot be resolved; operators will start with empty state"
                );
                for name in &external_ops {
                    if let Some(op) = manifest.operator_states.get_mut(name) {
                        *op = laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(&[]);
                    }
                }
                return false;
            }
        };

        let mut all_resolved = true;
        for (name, op) in &mut manifest.operator_states {
            if op.external {
                #[allow(clippy::cast_possible_truncation)] // Sidecar files are always < 4 GB
                let start = op.external_offset as usize;
                #[allow(clippy::cast_possible_truncation)]
                let end = start + op.external_length as usize;
                if end <= state_data.len() {
                    let external_offset = op.external_offset;
                    let external_length = op.external_length;
                    let data = &state_data[start..end];
                    *op = laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(data);
                    debug!(
                        operator = %name,
                        offset = external_offset,
                        length = external_length,
                        "resolved external operator state from sidecar"
                    );
                } else {
                    error!(
                        operator = %name,
                        offset = start,
                        length = op.external_length,
                        sidecar_len = state_data.len(),
                        "[LDB-6010] sidecar too small for external operator state — \
                         operator will start with empty state"
                    );
                    *op = laminar_storage::checkpoint_manifest::OperatorCheckpoint::inline(&[]);
                    all_resolved = false;
                }
            }
        }
        all_resolved
    }

    /// Restores pipeline state from a loaded manifest.
    ///
    /// This is the inner restore logic shared by both the fast path
    /// (latest checkpoint) and fallback path (older checkpoints).
    #[allow(clippy::too_many_lines)]
    async fn restore_from(
        &self,
        mut manifest: CheckpointManifest,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
    ) -> RecoveredState {
        // Resolve external operator states from sidecar before recovery.
        // In strict mode, unresolved sidecar state is recorded as a source
        // error so check_strict() will reject this checkpoint.
        let sidecar_ok = self.resolve_external_states(&mut manifest).await;
        if !sidecar_ok && self.strict {
            warn!(
                checkpoint_id = manifest.checkpoint_id,
                "[LDB-6010] sidecar resolution failed in strict mode — \
                 checkpoint will be rejected"
            );
        }

        // Validate manifest consistency before restoring state.
        // `DEFAULT_VNODE_COUNT` is a placeholder; the runtime
        // `VnodeRegistry` value is not threaded through recovery yet.
        let validation_errors =
            manifest.validate(laminar_storage::checkpoint_manifest::DEFAULT_VNODE_COUNT);
        if !validation_errors.is_empty() {
            for err in &validation_errors {
                warn!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %err,
                    "manifest validation warning"
                );
            }
        }

        // Topology drift detection: compare current sources/sinks against
        // the checkpoint to warn the operator about changes.
        if !manifest.source_names.is_empty() {
            let mut current_sources: Vec<&str> = sources.iter().map(|s| s.name.as_str()).collect();
            current_sources.sort_unstable();
            let checkpoint_sources: Vec<&str> =
                manifest.source_names.iter().map(String::as_str).collect();
            let added: Vec<&&str> = current_sources
                .iter()
                .filter(|n| !checkpoint_sources.contains(n))
                .collect();
            let removed: Vec<&&str> = checkpoint_sources
                .iter()
                .filter(|n| !current_sources.contains(n))
                .collect();
            if !added.is_empty() {
                warn!(
                    sources = ?added,
                    "new sources added since checkpoint — no saved offsets"
                );
            }
            if !removed.is_empty() {
                warn!(
                    sources = ?removed,
                    "sources removed since checkpoint — orphaned offsets"
                );
            }
        }
        if !manifest.sink_names.is_empty() {
            let mut current_sinks: Vec<&str> = sinks.iter().map(|s| s.name.as_str()).collect();
            current_sinks.sort_unstable();
            let checkpoint_sinks: Vec<&str> =
                manifest.sink_names.iter().map(String::as_str).collect();
            let added: Vec<&&str> = current_sinks
                .iter()
                .filter(|n| !checkpoint_sinks.contains(n))
                .collect();
            let removed: Vec<&&str> = checkpoint_sinks
                .iter()
                .filter(|n| !current_sinks.contains(n))
                .collect();
            if !added.is_empty() {
                warn!(
                    sinks = ?added,
                    "new sinks added since checkpoint — no saved epoch"
                );
            }
            if !removed.is_empty() {
                warn!(
                    sinks = ?removed,
                    "sinks removed since checkpoint — orphaned epochs"
                );
            }
        }

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            validation_warnings = validation_errors.len(),
            "recovering from checkpoint"
        );

        let mut result = RecoveredState {
            manifest: manifest.clone(),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::new(),
            sink_errors: HashMap::new(),
        };

        // Record sidecar failure so check_strict() rejects this checkpoint.
        if !sidecar_ok {
            result.source_errors.insert(
                "__sidecar__".into(),
                "[LDB-6010] sidecar state.bin missing or truncated — \
                 operator state cannot be fully restored"
                    .into(),
            );
        }

        // Step 3: Restore source offsets
        for source in sources {
            if !source.supports_replay {
                info!(
                    source = %source.name,
                    "skipping restore for non-replayable source (at-most-once)"
                );
                continue;
            }
            if let Some(cp) = manifest.source_offsets.get(&source.name) {
                let source_cp = connector_to_source_checkpoint(cp);
                let mut last_err = None;
                for attempt in 0..3u32 {
                    let mut connector = source.connector.lock().await;
                    match connector.restore(&source_cp).await {
                        Ok(()) => {
                            last_err = None;
                            break;
                        }
                        Err(e) => {
                            warn!(
                                source = %source.name, attempt,
                                error = %e, "source restore failed, retrying"
                            );
                            last_err = Some(e);
                            drop(connector);
                            if attempt < 2 {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                }
                if let Some(e) = last_err {
                    let msg = format!("source restore failed after 3 attempts: {e}");
                    result.source_errors.insert(source.name.clone(), msg);
                } else {
                    result.sources_restored += 1;
                    debug!(source = %source.name, epoch = cp.epoch, "source restored");
                }
            }
        }

        // Step 4: Restore table source offsets
        for table_source in table_sources {
            if let Some(cp) = manifest.table_offsets.get(&table_source.name) {
                let source_cp = connector_to_source_checkpoint(cp);
                let mut connector = table_source.connector.lock().await;
                match connector.restore(&source_cp).await {
                    Ok(()) => {
                        result.tables_restored += 1;
                        debug!(table = %table_source.name, epoch = cp.epoch, "table source restored");
                    }
                    Err(e) => {
                        let msg = format!("table source restore failed: {e}");
                        warn!(table = %table_source.name, error = %e, "table source restore failed");
                        result.source_errors.insert(table_source.name.clone(), msg);
                    }
                }
            }
        }

        // Step 5: Rollback sinks for exactly-once semantics.
        // Only roll back sinks that did NOT successfully commit (Pending or Failed).
        // Sinks with SinkCommitStatus::Committed already completed their commit
        // and should not be rolled back.
        for sink in sinks {
            if sink.exactly_once {
                // Check per-sink commit status — if the sink committed, skip rollback.
                let already_committed = manifest
                    .sink_commit_statuses
                    .get(&sink.name)
                    .is_some_and(|s| matches!(s, SinkCommitStatus::Committed));

                if already_committed {
                    debug!(
                        sink = %sink.name,
                        epoch = manifest.epoch,
                        "sink already committed, skipping rollback"
                    );
                    continue;
                }

                match sink.handle.rollback_epoch(manifest.epoch).await {
                    Ok(()) => {
                        result.sinks_rolled_back += 1;
                        debug!(sink = %sink.name, epoch = manifest.epoch, "sink rolled back");
                    }
                    Err(e) => {
                        result
                            .sink_errors
                            .insert(sink.name.clone(), format!("rollback failed: {e}"));
                        warn!(
                            sink = %sink.name,
                            epoch = manifest.epoch,
                            error = %e,
                            "[LDB-6016] sink rollback failed during recovery"
                        );
                    }
                }
            }
        }

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
            sources_restored = result.sources_restored,
            tables_restored = result.tables_restored,
            sinks_rolled_back = result.sinks_rolled_back,
            errors = result.source_errors.len() + result.sink_errors.len(),
            "recovery complete"
        );

        result
    }

    /// Checks whether a checkpoint's sidecar data is corrupt.
    ///
    /// Returns `true` if the checkpoint has a `state_checksum` and
    /// [`CheckpointStore::validate_checkpoint`] reports a checksum mismatch
    /// or missing sidecar. Returns `false` if there is no sidecar, or
    /// if the sidecar is valid, or if validation I/O fails (we proceed
    /// with caution rather than blocking recovery).
    /// Returns `true` if the checkpoint fails integrity validation.
    ///
    /// Checks both sidecar checksum and manifest validity. Any validation
    /// failure (sidecar corruption, missing data, or manifest issues like
    /// epoch=0) causes the checkpoint to be rejected so fallback can try
    /// an older one.
    ///
    /// Only returns `false` (proceed) when validation passes OR when the
    /// checkpoint has no sidecar to validate.
    async fn is_checkpoint_corrupt(&self, manifest: &CheckpointManifest) -> bool {
        // No sidecar and no state_checksum → nothing to validate beyond
        // manifest parsing (which already succeeded if we got here).
        if manifest.state_checksum.is_none() && manifest.operator_states.is_empty() {
            return false;
        }
        match self.store.validate_checkpoint(manifest.checkpoint_id).await {
            Ok(ValidationResult {
                valid: false,
                ref issues,
                ..
            }) => {
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    issues = ?issues,
                    "[LDB-6010] checkpoint integrity check failed"
                );
                true
            }
            Ok(_) => false, // valid
            Err(e) => {
                // I/O errors during validation are treated as corruption —
                // if we can't verify the checkpoint, don't trust it.
                error!(
                    checkpoint_id = manifest.checkpoint_id,
                    error = %e,
                    "[LDB-6010] checkpoint validation I/O error — \
                     treating as corrupt for safety"
                );
                true
            }
        }
    }

    /// In strict mode, returns an error if any source or sink had restore failures.
    /// Returns true if any exactly-once sink has Pending commit status.
    ///
    /// A checkpoint with Pending sinks was persisted before sink commit
    /// completed. Recovering from it would advance source offsets past
    /// data the sinks never received — causing silent data loss.
    fn has_pending_sinks(manifest: &CheckpointManifest) -> bool {
        manifest
            .sink_commit_statuses
            .values()
            .any(|s| matches!(s, SinkCommitStatus::Pending))
    }

    fn check_strict(&self, state: &RecoveredState) -> Result<(), DbError> {
        if !self.strict || !state.has_errors() {
            return Ok(());
        }
        let mut msgs: Vec<String> = state
            .source_errors
            .iter()
            .map(|(k, v)| format!("source '{k}': {v}"))
            .collect();
        for (k, v) in &state.sink_errors {
            msgs.push(format!("sink '{k}': {v}"));
        }
        Err(DbError::Checkpoint(format!(
            "strict recovery failed — {} restore error(s): {}",
            msgs.len(),
            msgs.join("; ")
        )))
    }

    /// Loads the latest manifest without performing recovery.
    ///
    /// Useful for inspecting checkpoint state or building a recovery plan.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub async fn load_latest(&self) -> Result<Option<CheckpointManifest>, DbError> {
        self.store.load_latest().await.map_err(DbError::from)
    }

    /// Loads a specific checkpoint by ID.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub async fn load_by_id(
        &self,
        checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, DbError> {
        self.store
            .load_by_id(checkpoint_id)
            .await
            .map_err(DbError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_storage::checkpoint_manifest::OperatorCheckpoint;
    use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

    fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir, 3)
    }

    #[tokio::test]
    async fn test_recover_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        let result = mgr.recover(&[], &[], &[]).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recover_empty_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Save a basic checkpoint
        let manifest = CheckpointManifest::new(1, 5);
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(result.epoch(), 5);
        assert_eq!(result.sources_restored, 0);
        assert_eq!(result.tables_restored, 0);
        assert_eq!(result.sinks_rolled_back, 0);
        assert!(!result.has_errors());
    }

    #[tokio::test]
    async fn test_recover_with_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 3);
        manifest.watermark = Some(42_000);
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(result.watermark(), Some(42_000));
    }

    #[tokio::test]
    async fn test_recover_with_operator_states() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 7);
        manifest
            .operator_states
            .insert("0".to_string(), OperatorCheckpoint::inline(b"window-state"));
        manifest
            .operator_states
            .insert("3".to_string(), OperatorCheckpoint::inline(b"filter-state"));
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(result.operator_states().len(), 2);
        let op0 = result.operator_states().get("0").unwrap();
        assert_eq!(op0.decode_inline().unwrap(), b"window-state");
    }

    #[tokio::test]
    async fn test_recover_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 1);
        manifest.table_store_checkpoint_path = Some("/data/rocksdb_cp_001".into());
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(
            result.table_store_checkpoint_path(),
            Some("/data/rocksdb_cp_001")
        );
    }

    #[tokio::test]
    async fn test_load_latest_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        assert!(mgr.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&CheckpointManifest::new(1, 1)).await.unwrap();
        store.save(&CheckpointManifest::new(2, 2)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let m = mgr.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.checkpoint_id, 1);

        let m2 = mgr.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m2.checkpoint_id, 2);

        assert!(mgr.load_by_id(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_recover_fallback_to_previous_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save two valid checkpoints
        let mut m1 = CheckpointManifest::new(1, 10);
        m1.watermark = Some(1000);
        store.save(&m1).await.unwrap();

        let mut m2 = CheckpointManifest::new(2, 20);
        m2.watermark = Some(2000);
        store.save(&m2).await.unwrap();

        // Corrupt the latest checkpoint by writing invalid JSON
        let latest_manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000002")
            .join("manifest.json");
        std::fs::write(&latest_manifest_path, "not valid json!!!").unwrap();

        // Also corrupt latest.txt to point to the corrupt checkpoint
        // (it already does from the save, but the manifest file is now corrupt)

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap();

        // Should fall back to checkpoint 1
        let recovered = result.expect("should recover from fallback checkpoint");
        assert_eq!(recovered.manifest.checkpoint_id, 1);
        assert_eq!(recovered.epoch(), 10);
        assert_eq!(recovered.watermark(), Some(1000));
    }

    #[tokio::test]
    async fn test_recover_all_checkpoints_corrupt_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save a checkpoint then corrupt it
        store.save(&CheckpointManifest::new(1, 5)).await.unwrap();

        let manifest_path = dir
            .path()
            .join("checkpoints")
            .join("checkpoint_000001")
            .join("manifest.json");
        std::fs::write(&manifest_path, "corrupt").unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap();

        // All checkpoints corrupt → fresh start
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_recover_latest_ok_no_fallback_needed() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&CheckpointManifest::new(1, 10)).await.unwrap();
        store.save(&CheckpointManifest::new(2, 20)).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        // Should use the latest (no fallback needed)
        assert_eq!(result.manifest.checkpoint_id, 2);
        assert_eq!(result.epoch(), 20);
    }

    #[tokio::test]
    async fn test_recover_with_sidecar_state() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Build a manifest with an external operator state
        let mut manifest = CheckpointManifest::new(1, 5);
        let large_data = vec![0xAB; 2048];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 2048));

        // Write sidecar first, then manifest
        store
            .save_state_data(1, &[bytes::Bytes::copy_from_slice(&large_data)])
            .await
            .unwrap();
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        // External state should have been resolved to inline
        let op = result.operator_states().get("big-op").unwrap();
        assert!(!op.external, "external state should be resolved to inline");
        assert_eq!(op.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_mixed_inline_and_external() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 3);
        // Small inline state
        manifest
            .operator_states
            .insert("small-op".into(), OperatorCheckpoint::inline(b"tiny"));
        // Large external state
        let large_data = vec![0xCD; 4096];
        manifest
            .operator_states
            .insert("big-op".into(), OperatorCheckpoint::external(0, 4096));

        store
            .save_state_data(1, &[bytes::Bytes::copy_from_slice(&large_data)])
            .await
            .unwrap();
        store.save(&manifest).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        let small = result.operator_states().get("small-op").unwrap();
        assert_eq!(small.decode_inline().unwrap(), b"tiny");

        let big = result.operator_states().get("big-op").unwrap();
        assert_eq!(big.decode_inline().unwrap(), large_data);
    }

    #[tokio::test]
    async fn test_recover_missing_sidecar_graceful() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manifest references external state but sidecar is missing
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest
            .operator_states
            .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
        store.save(&manifest).await.unwrap();

        // Use lenient mode — graceful degradation replaces missing
        // sidecar state with empty inline. Strict mode rejects this
        // checkpoint entirely (see test_recover_missing_sidecar_strict).
        let mgr = RecoveryManager::lenient(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        // Should still recover (gracefully) — external state replaced with
        // empty inline to avoid dangling offset references
        let op = result.operator_states().get("orphan").unwrap();
        assert!(
            !op.external,
            "unresolved external state replaced with inline empty"
        );
        assert!(
            op.state_b64.as_ref().is_none_or(String::is_empty),
            "replaced state should be empty"
        );
    }

    #[tokio::test]
    async fn test_recovered_state_has_errors() {
        let state = RecoveredState {
            manifest: CheckpointManifest::new(1, 1),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::new(),
            sink_errors: HashMap::new(),
        };
        assert!(!state.has_errors());

        let state_with_errors = RecoveredState {
            manifest: CheckpointManifest::new(1, 1),
            sources_restored: 0,
            tables_restored: 0,
            sinks_rolled_back: 0,
            source_errors: HashMap::from([("source1".into(), "failed".into())]),
            sink_errors: HashMap::new(),
        };
        assert!(state_with_errors.has_errors());
    }

    #[tokio::test]
    async fn test_recover_missing_sidecar_strict_rejects() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manifest references external state but sidecar is missing
        let mut manifest = CheckpointManifest::new(1, 1);
        manifest
            .operator_states
            .insert("orphan".into(), OperatorCheckpoint::external(0, 100));
        store.save(&manifest).await.unwrap();

        // Strict mode: missing sidecar causes the checkpoint to be rejected
        // and recovery falls back. With only one checkpoint, this means
        // fresh start.
        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap();
        assert!(
            result.is_none(),
            "strict mode should reject checkpoint with missing sidecar"
        );
    }

    #[tokio::test]
    async fn test_recover_skips_pending_sinks_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Epoch 1: fully committed checkpoint (good).
        let mut m1 = CheckpointManifest::new(1, 1);
        m1.sink_commit_statuses
            .insert("delta_sink".into(), SinkCommitStatus::Committed);
        store.save(&m1).await.unwrap();

        // Epoch 2: crashed between manifest persist and sink commit (Pending).
        let mut m2 = CheckpointManifest::new(2, 2);
        m2.sink_commit_statuses
            .insert("delta_sink".into(), SinkCommitStatus::Pending);
        store.save(&m2).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap();
        let state = result.expect("should recover from epoch 1 fallback");

        // Must fall back to epoch 1 (the last fully committed checkpoint),
        // not epoch 2 (which has uncommitted sink data).
        assert_eq!(
            state.epoch(),
            1,
            "recovery must skip checkpoint with Pending sinks"
        );
    }

    #[tokio::test]
    async fn test_recover_all_pending_starts_fresh() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Only checkpoint has Pending sinks — no safe fallback.
        let mut m = CheckpointManifest::new(1, 1);
        m.sink_commit_statuses
            .insert("sink".into(), SinkCommitStatus::Pending);
        store.save(&m).await.unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap();
        assert!(
            result.is_none(),
            "should start fresh when all checkpoints have pending sinks"
        );
    }
}
