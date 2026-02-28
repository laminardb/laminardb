//! Checkpoint persistence via the [`CheckpointStore`] trait.
//!
//! Provides a filesystem-backed implementation ([`FileSystemCheckpointStore`])
//! that writes manifests as atomic JSON files with a `latest.txt` pointer
//! for crash-safe recovery.
//!
//! ## Disk Layout
//!
//! ```text
//! {base_dir}/checkpoints/
//!   checkpoint_000001/
//!     manifest.json     # CheckpointManifest as pretty-printed JSON
//!     state.bin         # Optional: large operator state sidecar
//!   checkpoint_000002/
//!     manifest.json
//!   latest.txt          # "checkpoint_000002" — pointer to latest good checkpoint
//! ```

use std::path::{Path, PathBuf};

use crate::checkpoint_manifest::CheckpointManifest;

/// Fsync a file to ensure its contents are durable on disk.
fn sync_file(path: &Path) -> Result<(), std::io::Error> {
    // Must open with write access — Windows requires it for FlushFileBuffers.
    let f = std::fs::OpenOptions::new().write(true).open(path)?;
    f.sync_all()
}

/// Fsync a directory to make rename operations durable.
///
/// On Unix, this flushes directory metadata (new/renamed entries).
/// On Windows, directory sync is not supported; the OS handles durability.
#[allow(clippy::unnecessary_wraps)] // Returns Result on Unix, no-op on Windows
fn sync_dir(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let f = std::fs::File::open(path)?;
        f.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Errors from checkpoint store operations.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointStoreError {
    /// I/O error during checkpoint persistence.
    #[error("checkpoint I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization error.
    #[error("checkpoint serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// Checkpoint not found.
    #[error("checkpoint {0} not found")]
    NotFound(u64),
}

/// Trait for checkpoint persistence backends.
///
/// Implementations must guarantee atomic manifest writes (readers never see
/// a partial manifest). The `latest.txt` pointer is updated only after the
/// manifest is fully written and synced.
pub trait CheckpointStore: Send + Sync {
    /// Persists a checkpoint manifest atomically.
    ///
    /// The implementation writes to a temporary file and renames on success
    /// to prevent partial writes from being visible.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError>;

    /// Loads the most recent checkpoint manifest.
    ///
    /// Returns `Ok(None)` if no checkpoint exists yet.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O or deserialization failure.
    fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Loads a specific checkpoint manifest by ID.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError::NotFound`] if the checkpoint does not exist.
    fn load_by_id(&self, id: u64) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Lists all available checkpoints as `(checkpoint_id, epoch)` pairs.
    ///
    /// Results are sorted by checkpoint ID ascending.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError>;

    /// Prunes old checkpoints, keeping at most `keep_count` recent ones.
    ///
    /// Returns the number of checkpoints removed.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError>;

    /// Writes large operator state data to a sidecar file for a given checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn save_state_data(&self, id: u64, data: &[u8]) -> Result<(), CheckpointStoreError>;

    /// Loads large operator state data from a sidecar file.
    ///
    /// Returns `Ok(None)` if no sidecar file exists.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError>;

    /// Atomically saves a checkpoint manifest with optional sidecar state data.
    ///
    /// When `state_data` is provided, the sidecar (`state.bin`) is written and
    /// fsynced **before** the manifest. This ensures that if the sidecar write
    /// fails, the manifest is never persisted and `latest.txt` still points to
    /// the previous valid checkpoint.
    ///
    /// Orphaned `state.bin` files (written but no manifest) are harmless and
    /// cleaned up by [`prune()`](Self::prune).
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[u8]>,
    ) -> Result<(), CheckpointStoreError> {
        if let Some(data) = state_data {
            self.save_state_data(manifest.checkpoint_id, data)?;
        }
        self.save(manifest)
    }
}

/// Filesystem-backed checkpoint store.
///
/// Writes checkpoint manifests as JSON files with atomic rename semantics.
/// A `latest.txt` pointer (not a symlink) tracks the most recent checkpoint
/// for Windows compatibility.
pub struct FileSystemCheckpointStore {
    base_dir: PathBuf,
    max_retained: usize,
}

impl FileSystemCheckpointStore {
    /// Creates a new filesystem checkpoint store.
    ///
    /// The `base_dir` is the parent directory; checkpoints are stored under
    /// `{base_dir}/checkpoints/`. The directory is created lazily on first save.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>, max_retained: usize) -> Self {
        Self {
            base_dir: base_dir.into(),
            max_retained,
        }
    }

    /// Returns the checkpoints directory path.
    fn checkpoints_dir(&self) -> PathBuf {
        self.base_dir.join("checkpoints")
    }

    /// Returns the directory path for a specific checkpoint.
    fn checkpoint_dir(&self, id: u64) -> PathBuf {
        self.checkpoints_dir().join(format!("checkpoint_{id:06}"))
    }

    /// Returns the manifest file path for a specific checkpoint.
    fn manifest_path(&self, id: u64) -> PathBuf {
        self.checkpoint_dir(id).join("manifest.json")
    }

    /// Returns the state sidecar file path for a specific checkpoint.
    fn state_path(&self, id: u64) -> PathBuf {
        self.checkpoint_dir(id).join("state.bin")
    }

    /// Returns the latest.txt pointer path.
    fn latest_path(&self) -> PathBuf {
        self.checkpoints_dir().join("latest.txt")
    }

    /// Parses a checkpoint ID from a directory name like `checkpoint_000042`.
    fn parse_checkpoint_id(name: &str) -> Option<u64> {
        name.strip_prefix("checkpoint_")
            .and_then(|s| s.parse().ok())
    }

    /// Collects and sorts all checkpoint directory entries.
    fn sorted_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        let dir = self.checkpoints_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut ids: Vec<u64> = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().to_str().and_then(Self::parse_checkpoint_id))
            .collect();

        ids.sort_unstable();
        Ok(ids)
    }
}

impl CheckpointStore for FileSystemCheckpointStore {
    fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        let cp_dir = self.checkpoint_dir(manifest.checkpoint_id);
        std::fs::create_dir_all(&cp_dir)?;

        let manifest_path = self.manifest_path(manifest.checkpoint_id);
        let json = serde_json::to_string_pretty(manifest)?;

        // Write to a temp file, fsync, then rename for atomic durability
        let tmp_path = manifest_path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &json)?;
        sync_file(&tmp_path)?;
        std::fs::rename(&tmp_path, &manifest_path)?;
        sync_dir(&cp_dir)?;

        // Update latest.txt pointer — only after manifest is durable
        let latest = self.latest_path();
        let latest_dir = latest.parent().unwrap_or(Path::new("."));
        std::fs::create_dir_all(latest_dir)?;
        let latest_content = format!("checkpoint_{:06}", manifest.checkpoint_id);
        let tmp_latest = latest.with_extension("txt.tmp");
        std::fs::write(&tmp_latest, &latest_content)?;
        sync_file(&tmp_latest)?;
        std::fs::rename(&tmp_latest, &latest)?;
        sync_dir(latest_dir)?;

        // Auto-prune if configured
        if self.max_retained > 0 {
            let _ = self.prune(self.max_retained);
        }

        Ok(())
    }

    fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let latest = self.latest_path();
        if !latest.exists() {
            return Ok(None);
        }

        let content = std::fs::read_to_string(&latest)?;
        let dir_name = content.trim();
        if dir_name.is_empty() {
            return Ok(None);
        }

        let id = Self::parse_checkpoint_id(dir_name);
        match id {
            Some(id) => self.load_by_id(id),
            None => Ok(None),
        }
    }

    fn load_by_id(&self, id: u64) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let path = self.manifest_path(id);
        if !path.exists() {
            return Ok(None);
        }

        let json = std::fs::read_to_string(&path)?;
        let manifest: CheckpointManifest = serde_json::from_str(&json)?;
        Ok(Some(manifest))
    }

    fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.sorted_checkpoint_ids()?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            // Skip missing/corrupt manifests — list() is best-effort.
            if let Ok(Some(manifest)) = self.load_by_id(id) {
                result.push((manifest.checkpoint_id, manifest.epoch));
            }
        }

        Ok(result)
    }

    fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError> {
        let ids = self.sorted_checkpoint_ids()?;
        if ids.len() <= keep_count {
            return Ok(0);
        }

        let to_remove = ids.len() - keep_count;
        let mut removed = 0;

        for &id in &ids[..to_remove] {
            let dir = self.checkpoint_dir(id);
            if std::fs::remove_dir_all(&dir).is_ok() {
                removed += 1;
            }
        }

        Ok(removed)
    }

    fn save_state_data(&self, id: u64, data: &[u8]) -> Result<(), CheckpointStoreError> {
        let cp_dir = self.checkpoint_dir(id);
        std::fs::create_dir_all(&cp_dir)?;

        let path = self.state_path(id);
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, data)?;
        sync_file(&tmp)?;
        std::fs::rename(&tmp, &path)?;
        sync_dir(&cp_dir)?;

        Ok(())
    }

    fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[u8]>,
    ) -> Result<(), CheckpointStoreError> {
        // Write sidecar FIRST — if this fails, manifest is never written
        // and latest.txt still points to the previous valid checkpoint.
        if let Some(data) = state_data {
            self.save_state_data(manifest.checkpoint_id, data)?;
        }
        self.save(manifest)
    }

    fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let path = self.state_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let data = std::fs::read(&path)?;
        Ok(Some(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_manifest::{ConnectorCheckpoint, OperatorCheckpoint};
    use std::collections::HashMap;

    fn make_store(dir: &Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir, 3)
    }

    fn make_manifest(id: u64, epoch: u64) -> CheckpointManifest {
        CheckpointManifest::new(id, epoch)
    }

    #[test]
    fn test_save_and_load_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save(&m).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[test]
    fn test_load_latest_returns_none_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        assert!(store.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_load_latest_returns_most_recent() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).unwrap();
        }

        let latest = store.load_latest().unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 5);
        assert_eq!(latest.epoch, 5);
    }

    #[test]
    fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        let m = store.load_by_id(1).unwrap().unwrap();
        assert_eq!(m.epoch, 10);

        let m = store.load_by_id(2).unwrap().unwrap();
        assert_eq!(m.epoch, 20);

        assert!(store.load_by_id(99).unwrap().is_none());
    }

    #[test]
    fn test_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(3, 30)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        let list = store.list().unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn test_prune_keeps_max() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10); // no auto-prune

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).unwrap();
        }

        let removed = store.prune(2).unwrap();
        assert_eq!(removed, 3);

        let list = store.list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[test]
    fn test_auto_prune_on_save() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 2);

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).unwrap();
        }

        let list = store.list().unwrap();
        assert_eq!(list.len(), 2);
        // Should keep the two most recent
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[test]
    fn test_save_and_load_state_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&make_manifest(1, 1)).unwrap();

        let data = b"large operator state binary blob";
        store.save_state_data(1, data).unwrap();

        let loaded = store.load_state_data(1).unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_load_state_data_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        assert!(store.load_state_data(99).unwrap().is_none());
    }

    #[test]
    fn test_full_manifest_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut m = make_manifest(1, 5);
        m.source_offsets.insert(
            "kafka-src".into(),
            ConnectorCheckpoint::with_offsets(
                5,
                HashMap::from([("0".into(), "1000".into()), ("1".into(), "2000".into())]),
            ),
        );
        m.sink_epochs.insert("pg-sink".into(), 4);
        m.table_offsets.insert(
            "instruments".into(),
            ConnectorCheckpoint::with_offsets(5, HashMap::from([("lsn".into(), "0/AB".into())])),
        );
        m.operator_states
            .insert("window".into(), OperatorCheckpoint::inline(b"data"));
        m.watermark = Some(999_000);
        m.wal_position = 4096;
        m.per_core_wal_positions = vec![100, 200];

        store.save(&m).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 5);
        assert_eq!(loaded.watermark, Some(999_000));
        assert_eq!(loaded.wal_position, 4096);
        assert_eq!(loaded.per_core_wal_positions, vec![100, 200]);

        let src = loaded.source_offsets.get("kafka-src").unwrap();
        assert_eq!(src.offsets.get("0"), Some(&"1000".into()));

        assert_eq!(loaded.sink_epochs.get("pg-sink"), Some(&4));

        let tbl = loaded.table_offsets.get("instruments").unwrap();
        assert_eq!(tbl.offsets.get("lsn"), Some(&"0/AB".into()));

        let op = loaded.operator_states.get("window").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"data");
    }

    #[test]
    fn test_empty_latest_txt() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "").unwrap();

        assert!(store.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_latest_points_to_missing_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000099").unwrap();

        assert!(store.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_prune_no_op_when_under_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&make_manifest(1, 1)).unwrap();
        let removed = store.prune(5).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_save_with_state_writes_sidecar_before_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        let state = b"large-operator-state-blob";
        store.save_with_state(&m, Some(state)).unwrap();

        // Both manifest and state should be present.
        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);

        let loaded_state = store.load_state_data(1).unwrap().unwrap();
        assert_eq!(loaded_state, state);
    }

    #[test]
    fn test_save_with_state_none_is_same_as_save() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save_with_state(&m, None).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert!(store.load_state_data(1).unwrap().is_none());
    }

    #[test]
    fn test_orphaned_state_without_manifest_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Write only sidecar state, no manifest (simulates crash after
        // state write but before manifest write).
        store.save_state_data(1, b"orphaned").unwrap();

        // load_latest should return None — the orphan is not visible.
        assert!(store.load_latest().unwrap().is_none());

        // list should not include the orphan (no manifest.json).
        assert!(store.list().unwrap().is_empty());
    }
}
