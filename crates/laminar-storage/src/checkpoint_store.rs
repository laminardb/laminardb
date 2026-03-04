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
use std::sync::Arc;

use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, PutPayload};
use sha2::{Digest, Sha256};
use tracing::warn;

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

    /// Object store error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),
}

// ---------------------------------------------------------------------------
// Checkpoint validation types
// ---------------------------------------------------------------------------

/// Result of validating a single checkpoint.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Checkpoint ID that was validated.
    pub checkpoint_id: u64,
    /// Whether the checkpoint is valid for recovery.
    pub valid: bool,
    /// Issues found during validation.
    pub issues: Vec<String>,
}

/// Report from a crash-safe recovery walk.
///
/// Captures which checkpoints were tried, which were skipped (and why),
/// and which was ultimately chosen for recovery.
#[derive(Debug, Clone)]
pub struct RecoveryReport {
    /// The checkpoint that was selected for recovery (`None` if fresh start).
    pub chosen_id: Option<u64>,
    /// Checkpoints that were tried and skipped (id, reason).
    pub skipped: Vec<(u64, String)>,
    /// Total number of checkpoints examined.
    pub examined: usize,
    /// Elapsed time for the recovery walk.
    pub elapsed: std::time::Duration,
}

/// Compute SHA-256 hex digest of data.
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
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

    /// Lists all checkpoint IDs without loading manifests.
    ///
    /// This is used by crash recovery to enumerate candidates including
    /// those with corrupt manifests. Results are sorted ascending.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        // Default implementation: extract IDs from list().
        Ok(self.list()?.iter().map(|(id, _)| *id).collect())
    }

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

    /// Validate a specific checkpoint's integrity.
    ///
    /// Checks that the manifest is parseable and, if a `state_checksum` is
    /// present, verifies the sidecar data matches.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn validate_checkpoint(&self, id: u64) -> Result<ValidationResult, CheckpointStoreError> {
        let mut issues = Vec::new();

        // Load manifest — corrupt JSON is a validation failure, not an I/O error.
        let manifest = match self.load_by_id(id) {
            Ok(Some(m)) => m,
            Ok(None) => {
                return Ok(ValidationResult {
                    checkpoint_id: id,
                    valid: false,
                    issues: vec![format!("manifest not found for checkpoint {id}")],
                });
            }
            Err(CheckpointStoreError::Serde(e)) => {
                return Ok(ValidationResult {
                    checkpoint_id: id,
                    valid: false,
                    issues: vec![format!("corrupt manifest: {e}")],
                });
            }
            Err(e) => return Err(e),
        };

        // Basic manifest validation
        for err in manifest.validate() {
            issues.push(format!("manifest validation: {err}"));
        }

        // Verify state sidecar checksum
        if let Some(expected) = &manifest.state_checksum {
            match self.load_state_data(id)? {
                Some(data) => {
                    let actual = sha256_hex(&data);
                    if actual != *expected {
                        issues.push(format!(
                            "state.bin checksum mismatch: expected {expected}, got {actual}"
                        ));
                    }
                }
                None => {
                    issues.push("state.bin referenced by checksum but not found".into());
                }
            }
        }

        let valid =
            issues.is_empty() || issues.iter().all(|i| i.starts_with("manifest validation:"));
        Ok(ValidationResult {
            checkpoint_id: id,
            valid,
            issues,
        })
    }

    /// Walk backward from latest to find the first valid checkpoint.
    ///
    /// Returns a [`RecoveryReport`] describing the walk. If no valid
    /// checkpoint is found, `chosen_id` is `None` (fresh start).
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn recover_latest_validated(&self) -> Result<RecoveryReport, CheckpointStoreError> {
        let start = std::time::Instant::now();
        let mut skipped = Vec::new();

        // Get all checkpoint IDs sorted descending (newest first).
        // Uses list_ids() instead of list() so corrupt manifests are still
        // enumerated (list() silently skips them).
        let mut ids = self.list_ids()?;
        ids.sort_unstable();
        ids.reverse();

        let examined = ids.len();

        for id in &ids {
            let result = self.validate_checkpoint(*id)?;
            if result.valid {
                return Ok(RecoveryReport {
                    chosen_id: Some(*id),
                    skipped,
                    examined,
                    elapsed: start.elapsed(),
                });
            }
            let reason = result.issues.join("; ");
            warn!(
                checkpoint_id = id,
                reason = %reason,
                "skipping invalid checkpoint"
            );
            skipped.push((*id, reason));
        }

        Ok(RecoveryReport {
            chosen_id: None,
            skipped,
            examined,
            elapsed: start.elapsed(),
        })
    }

    /// Delete orphaned state files that have no matching manifest.
    ///
    /// Returns the number of orphans cleaned up.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
        // Default: no-op. Overridden by implementations that can detect orphans.
        Ok(0)
    }

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
        let mut manifest = manifest.clone();
        if let Some(data) = state_data {
            // Compute checksum before writing for crash-safe verification.
            manifest.state_checksum = Some(sha256_hex(data));
            self.save_state_data(manifest.checkpoint_id, data)?;
        }
        self.save(&manifest)
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

impl FileSystemCheckpointStore {
    /// Find checkpoint directories that have state.bin but no manifest.json
    /// (orphaned from a crash after sidecar write but before manifest commit).
    fn find_orphan_dirs(&self) -> Result<Vec<PathBuf>, CheckpointStoreError> {
        let dir = self.checkpoints_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut orphans = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let has_state = path.join("state.bin").exists();
            let has_manifest = path.join("manifest.json").exists();
            if has_state && !has_manifest {
                orphans.push(path);
            }
        }
        Ok(orphans)
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
            if let Err(e) = self.prune(self.max_retained) {
                tracing::warn!(
                    max_retained = self.max_retained,
                    error = %e,
                    "[LDB-6009] Checkpoint prune failed — old checkpoints may accumulate on disk"
                );
            }
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

    fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.sorted_checkpoint_ids()
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
        let mut manifest = manifest.clone();
        // Write sidecar FIRST — if this fails, manifest is never written
        // and latest.txt still points to the previous valid checkpoint.
        if let Some(data) = state_data {
            manifest.state_checksum = Some(sha256_hex(data));
            self.save_state_data(manifest.checkpoint_id, data)?;
        }
        self.save(&manifest)
    }

    fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
        let orphans = self.find_orphan_dirs()?;
        let mut cleaned = 0;
        for dir in &orphans {
            if std::fs::remove_dir_all(dir).is_ok() {
                tracing::info!(
                    path = %dir.display(),
                    "cleaned up orphaned checkpoint directory"
                );
                cleaned += 1;
            }
        }
        Ok(cleaned)
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

// ---------------------------------------------------------------------------
// ObjectStoreCheckpointStore — sync wrapper around any ObjectStore backend
// ---------------------------------------------------------------------------

/// JSON pointer stored in `manifests/latest.json`.
#[derive(serde::Serialize, serde::Deserialize)]
struct LatestPointer {
    checkpoint_id: u64,
}

/// Object-store-backed checkpoint store with hierarchical layout.
///
/// Bridges the sync [`CheckpointStore`] trait to the async [`ObjectStore`] API
/// via a dedicated single-threaded Tokio runtime. Checkpoints run infrequently
/// (every ~10s) and are not on the hot path.
///
/// ## Object Layout (v2 — hierarchical)
///
/// ```text
/// {prefix}/
///   manifests/
///     manifest-000001.json    # Checkpoint manifest (JSON)
///     manifest-000002.json
///     latest.json             # {"checkpoint_id": 2}
///   checkpoints/
///     state-000001.bin        # Optional sidecar state
///     state-000002.bin
/// ```
///
/// ## Legacy Layout (v1 — flat, read-only)
///
/// ```text
/// {prefix}/checkpoints/
///   checkpoint_000001/
///     manifest.json
///     state.bin
///   latest.txt                # "checkpoint_000002"
/// ```
///
/// Reads check v2 paths first, then fall back to v1 for backward compatibility.
/// Writes always use v2 layout. Manifest writes use [`PutMode::Create`] for
/// split-brain prevention (conditional PUT).
pub struct ObjectStoreCheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    max_retained: usize,
    /// Dedicated single-threaded runtime for async→sync bridging.
    /// Separate from the application runtime to avoid `block_on` reentrancy.
    rt: tokio::runtime::Runtime,
}

impl ObjectStoreCheckpointStore {
    /// Create a new object-store-backed checkpoint store.
    ///
    /// `prefix` is prepended to all object paths (e.g., `"nodes/abc123/"`).
    /// It should end with `/` or be empty.
    ///
    /// # Errors
    ///
    /// Returns `std::io::Error` if the internal Tokio runtime cannot be created.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        max_retained: usize,
    ) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            store,
            prefix,
            max_retained,
            rt,
        })
    }

    // ── v2 (hierarchical) paths ──

    fn manifest_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}manifests/manifest-{id:06}.json", self.prefix))
    }

    fn latest_pointer_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}manifests/latest.json", self.prefix))
    }

    fn state_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}checkpoints/state-{id:06}.bin", self.prefix))
    }

    // ── v1 (legacy) paths — for backward-compat reads only ──

    fn legacy_manifest_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}checkpoints/checkpoint_{id:06}/manifest.json",
            self.prefix
        ))
    }

    fn legacy_state_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}checkpoints/checkpoint_{id:06}/state.bin",
            self.prefix
        ))
    }

    fn legacy_latest_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}checkpoints/latest.txt", self.prefix))
    }

    // ── Helpers ──

    /// GET an object, returning `Ok(None)` for `NotFound`.
    fn get_bytes(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Option<bytes::Bytes>, CheckpointStoreError> {
        let result = self
            .rt
            .block_on(async { self.store.get_opts(path, GetOptions::default()).await });

        match result {
            Ok(get_result) => {
                let data = self.rt.block_on(async { get_result.bytes().await })?;
                Ok(Some(data))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(CheckpointStoreError::ObjectStore(e)),
        }
    }

    /// Load a manifest from a specific path, returning `Ok(None)` for `NotFound`.
    fn load_manifest_at(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        match self.get_bytes(path)? {
            Some(data) => {
                let manifest: CheckpointManifest = serde_json::from_slice(&data)?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    /// List checkpoint IDs by scanning both v2 and v1 layouts.
    fn list_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        let mut ids = std::collections::BTreeSet::new();

        // v2 layout: manifests/manifest-NNNNNN.json
        let manifests_prefix = object_store::path::Path::from(format!("{}manifests/", self.prefix));
        let entries: Vec<_> = self.rt.block_on(async {
            use futures::TryStreamExt;
            self.store
                .list(Some(&manifests_prefix))
                .try_collect::<Vec<_>>()
                .await
        })?;
        for entry in &entries {
            let path_str = entry.location.as_ref();
            for segment in path_str.split('/') {
                if let Some(rest) = segment.strip_prefix("manifest-") {
                    if let Some(id_str) = rest.strip_suffix(".json") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            ids.insert(id);
                        }
                    }
                }
            }
        }

        // v1 layout: checkpoints/checkpoint_NNNNNN/manifest.json
        let checkpoints_prefix =
            object_store::path::Path::from(format!("{}checkpoints/", self.prefix));
        let entries: Vec<_> = self.rt.block_on(async {
            use futures::TryStreamExt;
            self.store
                .list(Some(&checkpoints_prefix))
                .try_collect::<Vec<_>>()
                .await
        })?;
        for entry in &entries {
            let path_str = entry.location.as_ref();
            if !path_str.ends_with("manifest.json") {
                continue;
            }
            for segment in path_str.split('/') {
                if let Some(id_str) = segment.strip_prefix("checkpoint_") {
                    if let Ok(id) = id_str.parse::<u64>() {
                        ids.insert(id);
                    }
                }
            }
        }

        Ok(ids.into_iter().collect())
    }
}

impl CheckpointStore for ObjectStoreCheckpointStore {
    fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        let json = serde_json::to_string_pretty(manifest)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        let json_bytes = bytes::Bytes::from(json);

        // Conditional PUT — prevents duplicate manifest writes (split-brain safety).
        let create_opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let result = self.rt.block_on(async {
            self.store
                .put_opts(
                    &path,
                    PutPayload::from_bytes(json_bytes.clone()),
                    create_opts,
                )
                .await
        });

        match result {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                tracing::warn!(
                    checkpoint_id = manifest.checkpoint_id,
                    "[LDB-6010] Manifest already exists — skipping write"
                );
            }
            Err(object_store::Error::NotImplemented { .. }) => {
                // Backend doesn't support conditional PUT — fall back to overwrite.
                self.rt.block_on(async {
                    self.store
                        .put_opts(
                            &path,
                            PutPayload::from_bytes(json_bytes),
                            PutOptions::default(),
                        )
                        .await
                })?;
            }
            Err(e) => return Err(CheckpointStoreError::ObjectStore(e)),
        }

        // Update latest.json pointer (always overwrite).
        let latest = self.latest_pointer_path();
        let pointer = serde_json::to_string(&LatestPointer {
            checkpoint_id: manifest.checkpoint_id,
        })?;
        let payload = PutPayload::from_bytes(bytes::Bytes::from(pointer));
        self.rt.block_on(async {
            self.store
                .put_opts(&latest, payload, PutOptions::default())
                .await
        })?;

        // Auto-prune
        if self.max_retained > 0 {
            if let Err(e) = self.prune(self.max_retained) {
                tracing::warn!(
                    max_retained = self.max_retained,
                    error = %e,
                    "[LDB-6009] Object store checkpoint prune failed"
                );
            }
        }

        Ok(())
    }

    fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        // Try v2 layout: manifests/latest.json
        if let Some(data) = self.get_bytes(&self.latest_pointer_path())? {
            let pointer: LatestPointer = serde_json::from_slice(&data)?;
            return self.load_by_id(pointer.checkpoint_id);
        }

        // Fall back to v1 layout: checkpoints/latest.txt
        if let Some(data) = self.get_bytes(&self.legacy_latest_path())? {
            let content = String::from_utf8_lossy(&data);
            let dir_name = content.trim();
            if dir_name.is_empty() {
                return Ok(None);
            }
            let id = dir_name
                .strip_prefix("checkpoint_")
                .and_then(|s| s.parse::<u64>().ok());
            return match id {
                Some(id) => self.load_by_id(id),
                None => Ok(None),
            };
        }

        Ok(None)
    }

    fn load_by_id(&self, id: u64) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        // Try v2 layout first
        if let Some(m) = self.load_manifest_at(&self.manifest_path(id))? {
            return Ok(Some(m));
        }
        // Fall back to v1 layout
        self.load_manifest_at(&self.legacy_manifest_path(id))
    }

    fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.list_checkpoint_ids()
    }

    fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.list_checkpoint_ids()?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            if let Ok(Some(manifest)) = self.load_by_id(id) {
                result.push((manifest.checkpoint_id, manifest.epoch));
            }
        }

        Ok(result)
    }

    fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError> {
        let ids = self.list_checkpoint_ids()?;
        if ids.len() <= keep_count {
            return Ok(0);
        }

        let to_remove = ids.len() - keep_count;
        let mut removed = 0;

        for &id in &ids[..to_remove] {
            // Delete from both v2 and v1 layouts (ignore NotFound).
            let paths = vec![
                Ok(self.manifest_path(id)),
                Ok(self.state_path(id)),
                Ok(self.legacy_manifest_path(id)),
                Ok(self.legacy_state_path(id)),
            ];

            self.rt.block_on(async {
                use futures::StreamExt;
                let stream = futures::stream::iter(paths).boxed();
                let mut results = self.store.delete_stream(stream);
                while let Some(_result) = results.next().await {
                    // Ignore individual delete errors (file may not exist)
                }
            });
            removed += 1;
        }

        Ok(removed)
    }

    fn save_state_data(&self, id: u64, data: &[u8]) -> Result<(), CheckpointStoreError> {
        let path = self.state_path(id);
        let payload = PutPayload::from_bytes(bytes::Bytes::copy_from_slice(data));
        self.rt.block_on(async {
            self.store
                .put_opts(&path, payload, PutOptions::default())
                .await
        })?;
        Ok(())
    }

    fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        // Try v2 layout first
        if let Some(data) = self.get_bytes(&self.state_path(id))? {
            return Ok(Some(data.to_vec()));
        }
        // Fall back to v1 layout
        Ok(self
            .get_bytes(&self.legacy_state_path(id))?
            .map(|d| d.to_vec()))
    }

    fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
        // Collect state IDs that have a state.bin but no matching manifest.
        let manifest_ids: std::collections::BTreeSet<u64> =
            self.list_checkpoint_ids()?.into_iter().collect();

        // List state files in v2 layout: checkpoints/state-NNNNNN.bin
        let state_prefix = object_store::path::Path::from(format!("{}checkpoints/", self.prefix));
        let entries: Vec<_> = self.rt.block_on(async {
            use futures::TryStreamExt;
            self.store
                .list(Some(&state_prefix))
                .try_collect::<Vec<_>>()
                .await
        })?;

        let mut orphan_paths = Vec::new();
        for entry in &entries {
            let path_str = entry.location.as_ref();
            for segment in path_str.split('/') {
                if let Some(rest) = segment.strip_prefix("state-") {
                    if let Some(id_str) = rest.strip_suffix(".bin") {
                        if let Ok(id) = id_str.parse::<u64>() {
                            if !manifest_ids.contains(&id) {
                                orphan_paths.push(entry.location.clone());
                            }
                        }
                    }
                }
            }
        }

        let count = orphan_paths.len();
        if !orphan_paths.is_empty() {
            self.rt.block_on(async {
                use futures::StreamExt;
                let stream = futures::stream::iter(orphan_paths.into_iter().map(Ok)).boxed();
                let mut results = self.store.delete_stream(stream);
                while let Some(result) = results.next().await {
                    if let Err(e) = result {
                        tracing::warn!(error = %e, "failed to delete orphan state file");
                    }
                }
            });
        }

        Ok(count)
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

    // -----------------------------------------------------------------------
    // ObjectStoreCheckpointStore tests (using InMemory backend)
    // -----------------------------------------------------------------------

    fn make_obj_store() -> ObjectStoreCheckpointStore {
        let store = Arc::new(object_store::memory::InMemory::new());
        ObjectStoreCheckpointStore::new(store, String::new(), 3).unwrap()
    }

    fn make_obj_store_shared(
        store: Arc<object_store::memory::InMemory>,
    ) -> ObjectStoreCheckpointStore {
        ObjectStoreCheckpointStore::new(store, String::new(), 10).unwrap()
    }

    /// Write a manifest to the legacy (v1) layout for backward-compat testing.
    fn write_legacy_manifest(
        store: &Arc<object_store::memory::InMemory>,
        prefix: &str,
        manifest: &CheckpointManifest,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let json = serde_json::to_string_pretty(manifest).unwrap();

        let path = object_store::path::Path::from(format!(
            "{}checkpoints/checkpoint_{:06}/manifest.json",
            prefix, manifest.checkpoint_id
        ));
        rt.block_on(async {
            store
                .put_opts(
                    &path,
                    PutPayload::from_bytes(bytes::Bytes::from(json)),
                    PutOptions::default(),
                )
                .await
                .unwrap();
        });

        let latest = object_store::path::Path::from(format!("{prefix}checkpoints/latest.txt"));
        let content = format!("checkpoint_{:06}", manifest.checkpoint_id);
        rt.block_on(async {
            store
                .put_opts(
                    &latest,
                    PutPayload::from_bytes(bytes::Bytes::from(content)),
                    PutOptions::default(),
                )
                .await
                .unwrap();
        });
    }

    #[test]
    fn test_obj_save_and_load_latest() {
        let store = make_obj_store();
        let m = make_manifest(1, 1);
        store.save(&m).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[test]
    fn test_obj_load_latest_returns_none_when_empty() {
        let store = make_obj_store();
        assert!(store.load_latest().unwrap().is_none());
    }

    #[test]
    fn test_obj_load_by_id() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        let m = store.load_by_id(1).unwrap().unwrap();
        assert_eq!(m.epoch, 10);
        let m = store.load_by_id(2).unwrap().unwrap();
        assert_eq!(m.epoch, 20);
        assert!(store.load_by_id(99).unwrap().is_none());
    }

    #[test]
    fn test_obj_list() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(3, 30)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        let list = store.list().unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[test]
    fn test_obj_prune() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

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
    fn test_obj_auto_prune_on_save() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            2,
        )
        .unwrap();

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).unwrap();
        }

        let list = store.list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[test]
    fn test_obj_save_and_load_state_data() {
        let store = make_obj_store();
        store.save(&make_manifest(1, 1)).unwrap();

        let data = b"large operator state binary blob";
        store.save_state_data(1, data).unwrap();

        let loaded = store.load_state_data(1).unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_obj_load_state_data_returns_none() {
        let store = make_obj_store();
        assert!(store.load_state_data(99).unwrap().is_none());
    }

    #[test]
    fn test_obj_with_prefix() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner, "nodes/abc123/".to_string(), 10).unwrap();

        store.save(&make_manifest(1, 42)).unwrap();
        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 42);
    }

    // -----------------------------------------------------------------------
    // v2 layout verification + backward compatibility tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_obj_v2_layout_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10).unwrap();

        store.save(&make_manifest(1, 10)).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Manifest should be at v2 path
        let result = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("manifests/manifest-000001.json"),
                    GetOptions::default(),
                )
                .await
        });
        assert!(result.is_ok(), "v2 manifest path should exist");

        // latest.json should exist
        let result = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("manifests/latest.json"),
                    GetOptions::default(),
                )
                .await
        });
        assert!(result.is_ok(), "v2 latest.json should exist");

        // v1 path should NOT exist
        let result = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("checkpoints/checkpoint_000001/manifest.json"),
                    GetOptions::default(),
                )
                .await
        });
        assert!(result.is_err(), "v1 manifest path should NOT exist");
    }

    #[test]
    fn test_obj_v1_backward_compat_load() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        write_legacy_manifest(&inner, "", &make_manifest(1, 42));

        let store = make_obj_store_shared(inner);

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 42);

        let loaded = store.load_by_id(1).unwrap().unwrap();
        assert_eq!(loaded.epoch, 42);
    }

    #[test]
    fn test_obj_v1_backward_compat_list() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        write_legacy_manifest(&inner, "", &make_manifest(1, 10));
        write_legacy_manifest(&inner, "", &make_manifest(2, 20));

        let store = make_obj_store_shared(inner);
        let list = store.list().unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn test_obj_mixed_layout_list() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        // Checkpoint 1 in v1 layout
        write_legacy_manifest(&inner, "", &make_manifest(1, 10));
        // Checkpoint 2 in v2 layout
        let store = make_obj_store_shared(inner);
        store.save(&make_manifest(2, 20)).unwrap();

        let list = store.list().unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20)]);
    }

    #[test]
    fn test_obj_conditional_put_idempotent() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

        let m = make_manifest(1, 10);
        store.save(&m).unwrap();

        // Second save with same ID should succeed (logs warning, skips write)
        store.save(&m).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 10);
    }

    #[test]
    fn test_obj_v1_state_backward_compat() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Write state to v1 path
        let path = object_store::path::Path::from("checkpoints/checkpoint_000001/state.bin");
        let data = b"legacy-state-blob";
        rt.block_on(async {
            inner
                .put_opts(
                    &path,
                    PutPayload::from_bytes(bytes::Bytes::from_static(data)),
                    PutOptions::default(),
                )
                .await
                .unwrap();
        });

        let store = make_obj_store_shared(inner);
        let loaded = store.load_state_data(1).unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[test]
    fn test_obj_v2_state_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10).unwrap();

        store.save(&make_manifest(1, 1)).unwrap();
        store.save_state_data(1, b"v2-state").unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // State should be at v2 path
        let result = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("checkpoints/state-000001.bin"),
                    GetOptions::default(),
                )
                .await
        });
        assert!(result.is_ok(), "v2 state path should exist");

        // v1 state path should NOT exist
        let result = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("checkpoints/checkpoint_000001/state.bin"),
                    GetOptions::default(),
                )
                .await
        });
        assert!(result.is_err(), "v1 state path should NOT exist");
    }

    #[test]
    fn test_obj_prune_cleans_both_layouts() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        // Checkpoint 1 in v1 layout
        write_legacy_manifest(&inner, "", &make_manifest(1, 10));
        // Checkpoints 2-4 in v2 layout
        let store = ObjectStoreCheckpointStore::new(inner, String::new(), 10).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();
        store.save(&make_manifest(3, 30)).unwrap();
        store.save(&make_manifest(4, 40)).unwrap();

        let removed = store.prune(2).unwrap();
        assert_eq!(removed, 2);

        let list = store.list().unwrap();
        assert_eq!(list, vec![(3, 30), (4, 40)]);
    }

    #[test]
    fn test_obj_latest_json_format() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10).unwrap();

        store.save(&make_manifest(5, 50)).unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let data = rt.block_on(async {
            inner
                .get_opts(
                    &object_store::path::Path::from("manifests/latest.json"),
                    GetOptions::default(),
                )
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        });

        let pointer: super::LatestPointer = serde_json::from_slice(&data).unwrap();
        assert_eq!(pointer.checkpoint_id, 5);
    }

    #[test]
    fn test_validate_checkpoint_valid() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save(&m).unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(result.valid, "valid checkpoint: {:?}", result.issues);
        assert!(result.issues.is_empty());
    }

    #[test]
    fn test_validate_checkpoint_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let result = store.validate_checkpoint(99).unwrap();
        assert!(!result.valid);
        assert!(result.issues[0].contains("not found"));
    }

    #[test]
    fn test_validate_checkpoint_corrupt_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Create a checkpoint dir with corrupt manifest JSON.
        let cp_dir = dir.path().join("checkpoints/checkpoint_000001");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("manifest.json"), "not valid json").unwrap();

        // Corrupt manifest is a validation failure, not an I/O error.
        let result = store.validate_checkpoint(1).unwrap();
        assert!(!result.valid);
        assert!(
            result.issues[0].contains("corrupt manifest"),
            "expected corrupt manifest issue: {:?}",
            result.issues
        );
    }

    #[test]
    fn test_validate_checkpoint_state_checksum_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        let state = b"important operator state";
        let m = make_manifest(1, 1);
        store.save_with_state(&m, Some(state)).unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(result.valid, "checksum should match: {:?}", result.issues);
    }

    #[test]
    fn test_validate_checkpoint_state_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save with state to get a checksum.
        let state = b"original state";
        let m = make_manifest(1, 1);
        store.save_with_state(&m, Some(state)).unwrap();

        // Now corrupt the state.bin on disk.
        let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
        std::fs::write(&state_path, b"corrupted data!!").unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(!result.valid, "corrupted state should be invalid");
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.contains("checksum mismatch")),
            "should report checksum mismatch: {:?}",
            result.issues
        );
    }

    #[test]
    fn test_validate_checkpoint_state_missing_when_expected() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save with state.
        let m = make_manifest(1, 1);
        store.save_with_state(&m, Some(b"state")).unwrap();

        // Delete the state.bin file to simulate partial crash.
        let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
        std::fs::remove_file(&state_path).unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(!result.valid);
        assert!(
            result.issues.iter().any(|i| i.contains("not found")),
            "should report missing state: {:?}",
            result.issues
        );
    }

    #[test]
    fn test_recover_latest_validated_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save two checkpoints.
        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        // Corrupt the latest checkpoint's manifest.
        let cp2_manifest = dir
            .path()
            .join("checkpoints/checkpoint_000002/manifest.json");
        std::fs::write(cp2_manifest, "<<<corrupt>>>").unwrap();

        // Recovery should skip checkpoint 2 and pick checkpoint 1.
        let report = store.recover_latest_validated().unwrap();
        assert_eq!(report.chosen_id, Some(1));
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, 2);
        assert_eq!(report.examined, 2);
    }

    #[test]
    fn test_recover_latest_validated_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let report = store.recover_latest_validated().unwrap();
        assert!(report.chosen_id.is_none());
        assert_eq!(report.examined, 0);
    }

    #[test]
    fn test_recover_latest_validated_all_corrupt_is_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save a checkpoint, then corrupt it.
        store.save(&make_manifest(1, 1)).unwrap();
        let cp_manifest = dir
            .path()
            .join("checkpoints/checkpoint_000001/manifest.json");
        std::fs::write(cp_manifest, "corrupt").unwrap();

        // The corrupt manifest will cause load_by_id (via list()) to fail,
        // so it may not appear in the list at all. Either way, recovery
        // should not select it.
        let report = store.recover_latest_validated().unwrap();
        assert!(report.chosen_id.is_none());
    }

    #[test]
    fn test_cleanup_orphans_removes_stateless_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Create an orphan: state.bin exists but no manifest.json.
        let orphan_dir = dir.path().join("checkpoints/checkpoint_000099");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        std::fs::write(orphan_dir.join("state.bin"), b"orphaned").unwrap();

        // Normal checkpoint (has manifest).
        store.save(&make_manifest(1, 1)).unwrap();

        let cleaned = store.cleanup_orphans().unwrap();
        assert_eq!(cleaned, 1);

        // Orphan dir should be gone.
        assert!(!orphan_dir.exists());
        // Normal checkpoint should still be there.
        assert!(store.load_by_id(1).unwrap().is_some());
    }

    #[test]
    fn test_cleanup_orphans_noop_when_clean() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 1)).unwrap();
        let cleaned = store.cleanup_orphans().unwrap();
        assert_eq!(cleaned, 0);
    }

    #[test]
    fn test_save_with_state_writes_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        let state = b"state-data-for-checksum";
        let m = make_manifest(1, 1);
        store.save_with_state(&m, Some(state)).unwrap();

        let loaded = store.load_latest().unwrap().unwrap();
        assert!(
            loaded.state_checksum.is_some(),
            "state_checksum should be set"
        );
        let expected = sha256_hex(state);
        assert_eq!(loaded.state_checksum.unwrap(), expected);
    }

    #[test]
    fn test_state_checksum_backward_compat() {
        // Older manifests without state_checksum should deserialize fine.
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;
        let m: CheckpointManifest = serde_json::from_str(json).unwrap();
        assert!(m.state_checksum.is_none());
    }

    // ObjectStore variants

    #[test]
    fn test_obj_validate_checkpoint_valid() {
        let store = make_obj_store();
        store.save(&make_manifest(1, 1)).unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(result.valid, "valid checkpoint: {:?}", result.issues);
    }

    #[test]
    fn test_obj_validate_checkpoint_missing() {
        let store = make_obj_store();
        let result = store.validate_checkpoint(99).unwrap();
        assert!(!result.valid);
    }

    #[test]
    fn test_obj_validate_state_checksum() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

        let state = b"obj-store-state-data";
        let m = make_manifest(1, 1);
        store.save_with_state(&m, Some(state)).unwrap();

        let result = store.validate_checkpoint(1).unwrap();
        assert!(result.valid, "checksum should match: {:?}", result.issues);
    }

    #[test]
    fn test_obj_recover_latest_validated() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        )
        .unwrap();

        store.save(&make_manifest(1, 10)).unwrap();
        store.save(&make_manifest(2, 20)).unwrap();

        let report = store.recover_latest_validated().unwrap();
        assert_eq!(report.chosen_id, Some(2));
        assert!(report.skipped.is_empty());
    }

    #[test]
    fn test_obj_cleanup_orphans() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10).unwrap();

        // Save a checkpoint (creates manifest + state).
        let state = b"state-with-manifest";
        store
            .save_with_state(&make_manifest(1, 1), Some(state))
            .unwrap();

        // Write an orphan state file (no manifest).
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let orphan_path = object_store::path::Path::from("checkpoints/state-000099.bin");
        rt.block_on(async {
            inner
                .put_opts(
                    &orphan_path,
                    PutPayload::from_bytes(bytes::Bytes::from_static(b"orphan")),
                    PutOptions::default(),
                )
                .await
                .unwrap();
        });

        let cleaned = store.cleanup_orphans().unwrap();
        assert_eq!(cleaned, 1);

        // Verify orphan is gone but real state is intact.
        let real_state = store.load_state_data(1).unwrap();
        assert!(real_state.is_some());
    }
}
