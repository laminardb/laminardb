//! Unified recovery manager (F-CKP-007).
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
//! 6. If DAG: `dag_executor.restore(manifest.operator_states)` via conversion
//! 7. Return recovered state (watermark, epoch, operator states)

use std::collections::HashMap;

use laminar_storage::checkpoint_manifest::CheckpointManifest;
use laminar_storage::checkpoint_store::CheckpointStore;
use tracing::{debug, info, warn};

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

    /// Returns the WAL position from the manifest.
    #[must_use]
    pub fn wal_position(&self) -> u64 {
        self.manifest.wal_position
    }

    /// Returns the per-core WAL positions from the manifest.
    #[must_use]
    pub fn per_core_wal_positions(&self) -> &[u64] {
        &self.manifest.per_core_wal_positions
    }

    /// Returns the table store checkpoint path, if any.
    #[must_use]
    pub fn table_store_checkpoint_path(&self) -> Option<&str> {
        self.manifest.table_store_checkpoint_path.as_deref()
    }
}

/// Unified recovery manager.
///
/// Loads the latest [`CheckpointManifest`] from a [`CheckpointStore`] and
/// restores all registered sources, sinks, and tables to their checkpointed
/// state.
pub struct RecoveryManager<'a> {
    store: &'a dyn CheckpointStore,
}

#[allow(dead_code)] // Methods used in tests; wired into db.rs in future
impl<'a> RecoveryManager<'a> {
    /// Creates a new recovery manager using the given checkpoint store.
    #[must_use]
    pub fn new(store: &'a dyn CheckpointStore) -> Self {
        Self { store }
    }

    /// Attempts to recover from the latest checkpoint.
    ///
    /// Returns `Ok(None)` if no checkpoint exists (fresh start).
    /// Returns `Ok(Some(RecoveredState))` on successful recovery.
    ///
    /// Recovery is best-effort: individual source/sink failures are recorded
    /// in `RecoveredState` but do not abort the entire recovery. This allows
    /// partial recovery (e.g., one source fails to seek but others succeed).
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` only if the checkpoint store itself fails.
    pub(crate) async fn recover(
        &self,
        sources: &[RegisteredSource],
        sinks: &[RegisteredSink],
        table_sources: &[RegisteredSource],
    ) -> Result<Option<RecoveredState>, DbError> {
        // Step 1: Load latest manifest
        let manifest = self
            .store
            .load_latest()
            .map_err(|e| DbError::Checkpoint(format!("failed to load checkpoint: {e}")))?;

        let Some(manifest) = manifest else {
            info!("no checkpoint found, starting fresh");
            return Ok(None);
        };

        info!(
            checkpoint_id = manifest.checkpoint_id,
            epoch = manifest.epoch,
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

        // Step 3: Restore source offsets
        for source in sources {
            if let Some(cp) = manifest.source_offsets.get(&source.name) {
                let source_cp = connector_to_source_checkpoint(cp);
                let mut connector = source.connector.lock().await;
                match connector.restore(&source_cp).await {
                    Ok(()) => {
                        result.sources_restored += 1;
                        debug!(source = %source.name, epoch = cp.epoch, "source restored");
                    }
                    Err(e) => {
                        let msg = format!("source restore failed: {e}");
                        warn!(source = %source.name, error = %e, "source restore failed");
                        result.source_errors.insert(source.name.clone(), msg);
                    }
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

        // Step 5: Rollback sinks for exactly-once semantics
        for sink in sinks {
            if sink.exactly_once {
                let mut connector = sink.connector.lock().await;
                match connector.rollback_epoch(manifest.epoch).await {
                    Ok(()) => {
                        result.sinks_rolled_back += 1;
                        debug!(sink = %sink.name, epoch = manifest.epoch, "sink rolled back");
                    }
                    Err(e) => {
                        let msg = format!("sink rollback failed: {e}");
                        warn!(sink = %sink.name, error = %e, "sink rollback failed");
                        result.sink_errors.insert(sink.name.clone(), msg);
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

        Ok(Some(result))
    }

    /// Loads the latest manifest without performing recovery.
    ///
    /// Useful for inspecting checkpoint state or building a recovery plan.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub fn load_latest(&self) -> Result<Option<CheckpointManifest>, DbError> {
        self.store
            .load_latest()
            .map_err(|e| DbError::Checkpoint(format!("failed to load checkpoint: {e}")))
    }

    /// Loads a specific checkpoint by ID.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if the store fails.
    pub fn load_by_id(&self, checkpoint_id: u64) -> Result<Option<CheckpointManifest>, DbError> {
        self.store.load_by_id(checkpoint_id).map_err(|e| {
            DbError::Checkpoint(format!("failed to load checkpoint {checkpoint_id}: {e}"))
        })
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
        store.save(&manifest).unwrap();

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
        store.save(&manifest).unwrap();

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
        store.save(&manifest).unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(result.operator_states().len(), 2);
        let op0 = result.operator_states().get("0").unwrap();
        assert_eq!(op0.decode_inline().unwrap(), b"window-state");
    }

    #[tokio::test]
    async fn test_recover_wal_positions() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 2);
        manifest.wal_position = 4096;
        manifest.per_core_wal_positions = vec![100, 200, 300];
        store.save(&manifest).unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(result.wal_position(), 4096);
        assert_eq!(result.per_core_wal_positions(), &[100, 200, 300]);
    }

    #[tokio::test]
    async fn test_recover_table_store_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut manifest = CheckpointManifest::new(1, 1);
        manifest.table_store_checkpoint_path = Some("/data/rocksdb_cp_001".into());
        store.save(&manifest).unwrap();

        let mgr = RecoveryManager::new(&store);
        let result = mgr.recover(&[], &[], &[]).await.unwrap().unwrap();

        assert_eq!(
            result.table_store_checkpoint_path(),
            Some("/data/rocksdb_cp_001")
        );
    }

    #[test]
    fn test_load_latest_no_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        assert!(mgr.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&CheckpointManifest::new(1, 1)).unwrap();
        store.save(&CheckpointManifest::new(2, 2)).unwrap();

        let mgr = RecoveryManager::new(&store);
        let m = mgr.load_by_id(1).unwrap().unwrap();
        assert_eq!(m.checkpoint_id, 1);

        let m2 = mgr.load_by_id(2).unwrap().unwrap();
        assert_eq!(m2.checkpoint_id, 2);

        assert!(mgr.load_by_id(999).unwrap().is_none());
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
}
