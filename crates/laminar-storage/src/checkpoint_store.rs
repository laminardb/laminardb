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

use async_trait::async_trait;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::checkpoint_manifest::CheckpointManifest;

/// Fsync a file to ensure its contents are durable on disk.
async fn sync_file(path: &Path) -> Result<(), std::io::Error> {
    // Must open with write access — Windows requires it for FlushFileBuffers.
    let f = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await?;
    f.sync_all().await
}

/// Fsync a directory to make rename operations durable.
///
/// On Unix, this flushes directory metadata (new/renamed entries).
/// On Windows, directory sync is not supported; the OS handles durability.
#[allow(clippy::unnecessary_wraps, clippy::unused_async)] // no-op on Windows
async fn sync_dir(path: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        let f = tokio::fs::File::open(path).await?;
        f.sync_all().await?;
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

/// Classification of a single validation finding.
///
/// `ManifestWarning` is non-fatal — the checkpoint is still usable.
/// `IntegrityFailure` is fatal — recovery must skip this checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationIssue {
    /// Non-fatal manifest-level warning (e.g. `vnode_count` mismatch,
    /// orphaned source offset).
    ManifestWarning(String),
    /// Fatal: manifest is missing/corrupt, or the sidecar integrity
    /// check (checksum, presence) failed.
    IntegrityFailure(String),
}

impl ValidationIssue {
    /// True if this issue renders the checkpoint unusable for recovery.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::IntegrityFailure(_))
    }

    /// Underlying human-readable message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::ManifestWarning(s) | Self::IntegrityFailure(s) => s,
        }
    }
}

impl std::fmt::Display for ValidationIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

/// Result of validating a single checkpoint.
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Checkpoint ID that was validated.
    pub checkpoint_id: u64,
    /// Whether the checkpoint is valid for recovery. A checkpoint is
    /// valid iff it has no [`ValidationIssue::IntegrityFailure`] issues.
    pub valid: bool,
    /// Issues found during validation.
    pub issues: Vec<ValidationIssue>,
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

/// Parse a checkpoint id out of an object-store path segment shaped like
/// `"{prefix}NNNNNN{suffix}"` (e.g. `"manifest-000042.json"`). Scans all
/// '/'-separated segments so the helper works on prefixed stores. A
/// segment with the right affixes but a non-numeric middle is logged at
/// warn — operators need to notice manually-renamed files rather than
/// see silent gaps in `prune`/`list_ids`.
fn parse_checkpoint_id_from_path(path: &str, prefix: &str, suffix: &str) -> Option<u64> {
    for segment in path.split('/') {
        let Some(rest) = segment.strip_prefix(prefix) else {
            continue;
        };
        let Some(id_str) = rest.strip_suffix(suffix) else {
            continue;
        };
        if let Ok(id) = id_str.parse::<u64>() {
            return Some(id);
        }
        warn!(
            path,
            prefix, suffix, "malformed checkpoint id in object path — skipped"
        );
        return None;
    }
    None
}

/// Compute SHA-256 hex digest of data.
fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

/// Compute SHA-256 hex digest across a chain of `Bytes` chunks.
///
/// Equivalent to hashing the concatenation of the chunks, but without
/// materializing that concatenation in memory. Used by
/// [`CheckpointStore::save_with_state`] to checksum the sidecar before
/// the multi-chunk write.
fn sha256_hex_chunks(chunks: &[bytes::Bytes]) -> String {
    let mut hasher = Sha256::new();
    for chunk in chunks {
        hasher.update(chunk);
    }
    format!("{:x}", hasher.finalize())
}

/// Trait for checkpoint persistence backends.
///
/// Implementations must guarantee atomic manifest writes (readers never see
/// a partial manifest). The `latest.txt` pointer is updated only after the
/// manifest is fully written and synced.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Runtime vnode count that manifests written by this store are
    /// expected to use. Consulted when validating loaded manifests —
    /// a mismatch is reported as a manifest warning. Defaults to
    /// [`crate::checkpoint_manifest::DEFAULT_VNODE_COUNT`] when the
    /// implementation has no configured value.
    fn vnode_count(&self) -> u16 {
        crate::checkpoint_manifest::DEFAULT_VNODE_COUNT
    }

    /// Atomically persists a checkpoint manifest. Implementations must
    /// guarantee readers never observe a partial manifest.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError>;

    /// Loads the most recent checkpoint manifest, or `Ok(None)` on a
    /// fresh store.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or deserialization failure.
    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Loads a specific manifest, or `Ok(None)` if absent.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or deserialization failure.
    async fn load_by_id(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Lists all available checkpoints as `(id, epoch)` pairs, sorted
    /// ascending by ID. May read every manifest; callers that only
    /// need IDs should use [`Self::list_ids`].
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError>;

    /// Lists all checkpoint IDs, **sorted ascending**. Unlike
    /// [`Self::list`] this enumerates corrupt manifests too (used by
    /// crash recovery). Callers rely on the ascending invariant.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        // Default: O(N) manifest reads via list(). Production backends
        // should override; list() already sorts ascending.
        Ok(self.list().await?.iter().map(|(id, _)| *id).collect())
    }

    /// Prunes old checkpoints, keeping at most `keep_count` recent
    /// ones. Returns the number of checkpoints removed.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError>;

    /// Overwrites an existing manifest, bypassing the conditional-PUT
    /// fence used by [`Self::save`]. Used after a successful sink
    /// commit to record per-sink status transitions.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    async fn update_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointStoreError> {
        self.save(manifest).await
    }

    /// Writes operator state sidecar bytes for a checkpoint.
    ///
    /// Accepts a chain of `Bytes` chunks (one per operator) rather than
    /// a single concatenated slice. Backends that support native
    /// multi-chunk writes (object-store `PutPayload`) avoid copying the
    /// chunks into a contiguous buffer; backends without such support
    /// write sequentially.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn save_state_data(
        &self,
        id: u64,
        chunks: &[bytes::Bytes],
    ) -> Result<(), CheckpointStoreError>;

    /// Loads operator state sidecar bytes for a checkpoint, or `Ok(None)`
    /// if no sidecar was written.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn load_state_data(&self, id: u64)
        -> Result<Option<Vec<u8>>, CheckpointStoreError>;

    /// Validate a specific checkpoint's integrity.
    ///
    /// Checks that the manifest is parseable and, if a `state_checksum` is
    /// present, verifies the sidecar data matches.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn validate_checkpoint(
        &self,
        id: u64,
    ) -> Result<ValidationResult, CheckpointStoreError> {
        let mut issues = Vec::new();

        // Load manifest — corrupt JSON is a validation failure, not an I/O error.
        let manifest = match self.load_by_id(id).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                return Ok(ValidationResult {
                    checkpoint_id: id,
                    valid: false,
                    issues: vec![ValidationIssue::IntegrityFailure(format!(
                        "manifest not found for checkpoint {id}"
                    ))],
                });
            }
            Err(CheckpointStoreError::Serde(e)) => {
                return Ok(ValidationResult {
                    checkpoint_id: id,
                    valid: false,
                    issues: vec![ValidationIssue::IntegrityFailure(format!(
                        "corrupt manifest: {e}"
                    ))],
                });
            }
            Err(e) => return Err(e),
        };

        for err in manifest.validate(self.vnode_count()) {
            issues.push(ValidationIssue::ManifestWarning(format!(
                "manifest validation: {err}"
            )));
        }

        // Verify state sidecar checksum.
        if let Some(expected) = &manifest.state_checksum {
            match self.load_state_data(id).await? {
                Some(data) => {
                    let actual = sha256_hex(&data);
                    if actual != *expected {
                        issues.push(ValidationIssue::IntegrityFailure(format!(
                            "state.bin checksum mismatch: expected {expected}, got {actual}"
                        )));
                    }
                }
                None => {
                    issues.push(ValidationIssue::IntegrityFailure(
                        "state.bin referenced by checksum but not found".into(),
                    ));
                }
            }
        }

        // epoch=0 or checkpoint_id=0 indicates a corrupted or nonsensical
        // manifest — reject as invalid regardless of other issues.
        if manifest.epoch == 0 || manifest.checkpoint_id == 0 {
            issues.push(ValidationIssue::IntegrityFailure(
                "epoch or checkpoint_id is 0 — likely corrupted".into(),
            ));
        }

        let valid = issues.iter().all(|i| !i.is_fatal());
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
    async fn recover_latest_validated(
        &self,
    ) -> Result<RecoveryReport, CheckpointStoreError> {
        let start = std::time::Instant::now();
        let mut skipped = Vec::new();

        // list_ids returns ascending per the trait contract; we iterate
        // newest-first so the first valid checkpoint wins.
        let mut ids = self.list_ids().await?;
        ids.reverse();

        let examined = ids.len();

        for id in &ids {
            let result = self.validate_checkpoint(*id).await?;
            if result.valid {
                return Ok(RecoveryReport {
                    chosen_id: Some(*id),
                    skipped,
                    examined,
                    elapsed: start.elapsed(),
                });
            }
            let reason = result
                .issues
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
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
    async fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
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
    async fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[bytes::Bytes]>,
    ) -> Result<(), CheckpointStoreError> {
        let mut manifest = manifest.clone();
        if let Some(chunks) = state_data {
            // Compute checksum across the chunks before writing. This is
            // safe because: (1) save_state_data writes to a temp then
            // renames atomically, so the on-disk bytes match the
            // in-memory chain exactly; (2) if the sidecar write fails,
            // save() is never called, so the manifest with the checksum
            // is never persisted.
            manifest.state_checksum = Some(sha256_hex_chunks(chunks));
            self.save_state_data(manifest.checkpoint_id, chunks).await?;
        }
        self.save(&manifest).await
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
    vnode_count: u16,
}

impl FileSystemCheckpointStore {
    /// Creates a new filesystem checkpoint store.
    ///
    /// The `base_dir` is the parent directory; checkpoints are stored under
    /// `{base_dir}/checkpoints/`. The directory is created lazily on first save.
    ///
    /// The store's `vnode_count` defaults to
    /// [`crate::checkpoint_manifest::DEFAULT_VNODE_COUNT`]. Hosts that run
    /// with a non-default value should chain [`Self::with_vnode_count`] so
    /// manifest validation checks the right invariant.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>, max_retained: usize) -> Self {
        Self {
            base_dir: base_dir.into(),
            max_retained,
            vnode_count: crate::checkpoint_manifest::DEFAULT_VNODE_COUNT,
        }
    }

    /// Override the `vnode_count` used during manifest validation.
    #[must_use]
    pub fn with_vnode_count(mut self, vnode_count: u16) -> Self {
        self.vnode_count = vnode_count;
        self
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
    async fn sorted_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        let dir = self.checkpoints_dir();
        let mut reader = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut ids: Vec<u64> = Vec::new();
        while let Some(entry) = reader.next_entry().await? {
            let ft = entry.file_type().await?;
            if !ft.is_dir() {
                continue;
            }
            if let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(Self::parse_checkpoint_id)
            {
                ids.push(id);
            }
        }

        ids.sort_unstable();
        Ok(ids)
    }
}

impl FileSystemCheckpointStore {
    /// Find checkpoint directories that have state.bin but no manifest.json
    /// (orphaned from a crash after sidecar write but before manifest commit).
    async fn find_orphan_dirs(&self) -> Result<Vec<PathBuf>, CheckpointStoreError> {
        let dir = self.checkpoints_dir();
        let mut reader = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut orphans = Vec::new();
        while let Some(entry) = reader.next_entry().await? {
            let ft = entry.file_type().await?;
            if !ft.is_dir() {
                continue;
            }
            let path = entry.path();
            let has_state = tokio::fs::metadata(path.join("state.bin")).await.is_ok();
            let has_manifest =
                tokio::fs::metadata(path.join("manifest.json")).await.is_ok();
            if has_state && !has_manifest {
                orphans.push(path);
            }
        }
        Ok(orphans)
    }
}

#[async_trait]
impl CheckpointStore for FileSystemCheckpointStore {
    fn vnode_count(&self) -> u16 {
        self.vnode_count
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        let cp_dir = self.checkpoint_dir(manifest.checkpoint_id);
        tokio::fs::create_dir_all(&cp_dir).await?;

        let manifest_path = self.manifest_path(manifest.checkpoint_id);
        let json = serde_json::to_string_pretty(manifest)?;

        // Write to a temp file, fsync, then rename for atomic durability.
        let tmp_path = manifest_path.with_extension("json.tmp");
        let write_res = async {
            tokio::fs::write(&tmp_path, &json).await?;
            sync_file(&tmp_path).await?;
            tokio::fs::rename(&tmp_path, &manifest_path).await?;
            sync_dir(&cp_dir).await
        }
        .await;
        if let Err(e) = write_res {
            // Clean up temp file to avoid orphans on disk-full.
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(e.into());
        }

        // Update latest.txt pointer — only after manifest is durable.
        let latest = self.latest_path();
        let latest_dir = latest.parent().unwrap_or(Path::new(".")).to_path_buf();
        tokio::fs::create_dir_all(&latest_dir).await?;
        let latest_content = format!("checkpoint_{:06}", manifest.checkpoint_id);
        let tmp_latest = latest.with_extension("txt.tmp");
        tokio::fs::write(&tmp_latest, &latest_content).await?;
        sync_file(&tmp_latest).await?;
        tokio::fs::rename(&tmp_latest, &latest).await?;
        sync_dir(&latest_dir).await?;

        // Auto-prune if configured.
        if self.max_retained > 0 {
            if let Err(e) = self.prune(self.max_retained).await {
                tracing::warn!(
                    max_retained = self.max_retained,
                    error = %e,
                    "[LDB-6009] Checkpoint prune failed — old checkpoints may accumulate on disk"
                );
            }
        }

        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let latest = self.latest_path();
        let content = match tokio::fs::read_to_string(&latest).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let dir_name = content.trim();
        if dir_name.is_empty() {
            return Ok(None);
        }

        match Self::parse_checkpoint_id(dir_name) {
            Some(id) => self.load_by_id(id).await,
            None => Ok(None),
        }
    }

    async fn load_by_id(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let path = self.manifest_path(id);
        let json = match tokio::fs::read_to_string(&path).await {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let manifest: CheckpointManifest = serde_json::from_str(&json)?;

        let errors = manifest.validate(self.vnode_count());
        if !errors.is_empty() {
            tracing::warn!(
                checkpoint_id = id,
                error_count = errors.len(),
                first_error = %errors[0],
                "loaded checkpoint manifest has validation warnings"
            );
        }

        Ok(Some(manifest))
    }

    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.sorted_checkpoint_ids().await
    }

    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.sorted_checkpoint_ids().await?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            // Skip missing/corrupt manifests — list() is best-effort.
            if let Ok(Some(manifest)) = self.load_by_id(id).await {
                result.push((manifest.checkpoint_id, manifest.epoch));
            }
        }

        Ok(result)
    }

    async fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError> {
        let ids = self.sorted_checkpoint_ids().await?;
        if ids.len() <= keep_count {
            return Ok(0);
        }

        let to_remove = ids.len() - keep_count;
        let mut removed = 0;

        for &id in &ids[..to_remove] {
            let dir = self.checkpoint_dir(id);
            if tokio::fs::remove_dir_all(&dir).await.is_ok() {
                removed += 1;
            }
        }

        Ok(removed)
    }

    async fn save_state_data(
        &self,
        id: u64,
        chunks: &[bytes::Bytes],
    ) -> Result<(), CheckpointStoreError> {
        use tokio::io::AsyncWriteExt;

        let cp_dir = self.checkpoint_dir(id);
        tokio::fs::create_dir_all(&cp_dir).await?;

        let path = self.state_path(id);
        let tmp = path.with_extension("bin.tmp");

        // Write chunks sequentially to the temp file — no concatenation
        // into a contiguous buffer. Each chunk is already an owned Bytes;
        // write_all borrows it.
        let mut file = tokio::fs::File::create(&tmp).await?;
        for chunk in chunks {
            file.write_all(chunk).await?;
        }
        file.sync_all().await?;
        drop(file);

        tokio::fs::rename(&tmp, &path).await?;
        sync_dir(&cp_dir).await?;

        Ok(())
    }

    async fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[bytes::Bytes]>,
    ) -> Result<(), CheckpointStoreError> {
        let mut manifest = manifest.clone();
        // Write sidecar FIRST — if this fails, manifest is never written
        // and latest.txt still points to the previous valid checkpoint.
        if let Some(chunks) = state_data {
            manifest.state_checksum = Some(sha256_hex_chunks(chunks));
            self.save_state_data(manifest.checkpoint_id, chunks).await?;
        }
        self.save(&manifest).await
    }

    async fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
        let orphans = self.find_orphan_dirs().await?;
        let mut cleaned = 0;
        for dir in &orphans {
            if tokio::fs::remove_dir_all(dir).await.is_ok() {
                tracing::info!(
                    path = %dir.display(),
                    "cleaned up orphaned checkpoint directory"
                );
                cleaned += 1;
            }
        }
        Ok(cleaned)
    }

    async fn load_state_data(
        &self,
        id: u64,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let path = self.state_path(id);
        match tokio::fs::read(&path).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
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

/// Object-store-backed checkpoint store.
///
/// Drives any `object_store::ObjectStore` backend (S3, GCS, Azure,
/// local FS) directly over `.await`; no dedicated runtime. The app
/// runtime's HTTP connection pool is reused.
///
/// ## Object Layout
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
/// Manifest writes use [`PutMode::Create`] for split-brain prevention
/// (conditional PUT).
pub struct ObjectStoreCheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    max_retained: usize,
    vnode_count: u16,
}

impl ObjectStoreCheckpointStore {
    /// Create a new object-store-backed checkpoint store.
    ///
    /// `prefix` is prepended to all object paths (e.g., `"nodes/abc123/"`).
    /// It should end with `/` or be empty.
    ///
    /// The store's `vnode_count` defaults to
    /// [`crate::checkpoint_manifest::DEFAULT_VNODE_COUNT`]. Hosts that run
    /// with a non-default value should chain [`Self::with_vnode_count`].
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, max_retained: usize) -> Self {
        Self {
            store,
            prefix,
            max_retained,
            vnode_count: crate::checkpoint_manifest::DEFAULT_VNODE_COUNT,
        }
    }

    /// Override the `vnode_count` used during manifest validation.
    #[must_use]
    pub fn with_vnode_count(mut self, vnode_count: u16) -> Self {
        self.vnode_count = vnode_count;
        self
    }

    fn manifest_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}manifests/manifest-{id:06}.json", self.prefix))
    }

    fn latest_pointer_path(&self) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}manifests/latest.json", self.prefix))
    }

    fn state_path(&self, id: u64) -> object_store::path::Path {
        object_store::path::Path::from(format!("{}checkpoints/state-{id:06}.bin", self.prefix))
    }

    // ── Helpers ──

    /// Put a payload with bounded retry + jittered backoff for idempotent
    /// writes (sidecar state, pointer update). Retries on
    /// `object_store::Error::Generic` — which covers most transient
    /// 5xx / connection failures across backends — and bubbles every
    /// other error immediately. Non-idempotent writes (conditional
    /// creates on the manifest path) MUST NOT use this helper.
    ///
    /// `payload` is consumed on the happy path and cloned on retry.
    /// `PutPayload::clone` is cheap (Arc-bump on each underlying
    /// `Bytes` chunk), so multi-chunk payloads cost nothing extra to
    /// retry.
    async fn put_with_retry(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
        opts: &PutOptions,
    ) -> Result<(), CheckpointStoreError> {
        const BACKOFFS_MS: &[u64] = &[100, 500, 2000];
        let mut attempt = 0usize;
        loop {
            let result = self
                .store
                .put_opts(path, payload.clone(), opts.clone())
                .await;
            match result {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Generic { .. })
                    if attempt < BACKOFFS_MS.len() =>
                {
                    let delay = std::time::Duration::from_millis(BACKOFFS_MS[attempt]);
                    tracing::warn!(
                        path = %path,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis(),
                        "transient put error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(e) => return Err(CheckpointStoreError::ObjectStore(e)),
            }
        }
    }

    /// GET an object, returning `Ok(None)` for `NotFound`.
    async fn get_bytes(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Option<bytes::Bytes>, CheckpointStoreError> {
        match self.store.get_opts(path, GetOptions::default()).await {
            Ok(get_result) => {
                let data = get_result.bytes().await?;
                Ok(Some(data))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(CheckpointStoreError::ObjectStore(e)),
        }
    }

    /// Load a manifest from a specific path, returning `Ok(None)` for `NotFound`.
    async fn load_manifest_at(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        match self.get_bytes(path).await? {
            Some(data) => {
                let manifest: CheckpointManifest = serde_json::from_slice(&data)?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    /// List checkpoint IDs by scanning `manifests/manifest-NNNNNN.json`.
    async fn list_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        use futures::TryStreamExt;

        let mut ids = std::collections::BTreeSet::new();

        let manifests_prefix = object_store::path::Path::from(format!("{}manifests/", self.prefix));
        let entries: Vec<_> = self
            .store
            .list(Some(&manifests_prefix))
            .try_collect()
            .await?;
        for entry in &entries {
            if let Some(id) = parse_checkpoint_id_from_path(
                entry.location.as_ref(),
                "manifest-",
                ".json",
            ) {
                ids.insert(id);
            }
        }

        Ok(ids.into_iter().collect())
    }
}

#[async_trait]
impl CheckpointStore for ObjectStoreCheckpointStore {
    fn vnode_count(&self) -> u16 {
        self.vnode_count
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        let json = serde_json::to_string_pretty(manifest)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        let json_bytes = bytes::Bytes::from(json);

        // Conditional PUT — prevents duplicate manifest writes (split-brain safety).
        let create_opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(
                &path,
                PutPayload::from_bytes(json_bytes.clone()),
                create_opts,
            )
            .await;

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
                self.store
                    .put_opts(
                        &path,
                        PutPayload::from_bytes(json_bytes),
                        PutOptions::default(),
                    )
                    .await?;
            }
            Err(e) => return Err(CheckpointStoreError::ObjectStore(e)),
        }

        // Monotonic pointer update. A stale writer must not regress the
        // pointer from id N+1 back to id N. Read the current pointer; if
        // it already references a newer id, skip the write. Same-writer
        // races (not expected — one coordinator per store instance) can
        // still race past this check, but cross-leader stomps from a
        // delayed ex-leader are caught.
        let latest = self.latest_pointer_path();
        if let Some(current) = self.get_bytes(&latest).await? {
            if let Ok(existing) = serde_json::from_slice::<LatestPointer>(&current) {
                if existing.checkpoint_id > manifest.checkpoint_id {
                    tracing::warn!(
                        current = existing.checkpoint_id,
                        ours = manifest.checkpoint_id,
                        "[LDB-6010] latest.json already points at a newer checkpoint — \
                         skipping pointer update (possible split-brain or delayed writer)"
                    );
                    return Ok(());
                }
            }
        }
        let pointer = serde_json::to_string(&LatestPointer {
            checkpoint_id: manifest.checkpoint_id,
        })?;
        self.put_with_retry(
            &latest,
            PutPayload::from_bytes(bytes::Bytes::from(pointer)),
            &PutOptions::default(),
        )
        .await?;

        // Auto-prune
        if self.max_retained > 0 {
            if let Err(e) = self.prune(self.max_retained).await {
                tracing::warn!(
                    max_retained = self.max_retained,
                    error = %e,
                    "[LDB-6009] Object store checkpoint prune failed"
                );
            }
        }

        Ok(())
    }

    async fn update_manifest(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointStoreError> {
        let json = serde_json::to_string_pretty(manifest)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        let payload = PutPayload::from_bytes(bytes::Bytes::from(json));

        // Unconditional PUT — overwrites the existing manifest.
        self.store
            .put_opts(&path, payload, PutOptions::default())
            .await?;

        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        if let Some(data) = self.get_bytes(&self.latest_pointer_path()).await? {
            let pointer: LatestPointer = serde_json::from_slice(&data)?;
            return self.load_by_id(pointer.checkpoint_id).await;
        }

        Ok(None)
    }

    async fn load_by_id(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        self.load_manifest_at(&self.manifest_path(id)).await
    }

    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.list_checkpoint_ids().await
    }

    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.list_checkpoint_ids().await?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            if let Ok(Some(manifest)) = self.load_by_id(id).await {
                result.push((manifest.checkpoint_id, manifest.epoch));
            }
        }

        Ok(result)
    }

    async fn prune(&self, keep_count: usize) -> Result<usize, CheckpointStoreError> {
        let ids = self.list_checkpoint_ids().await?;
        if ids.len() <= keep_count {
            return Ok(0);
        }

        let to_remove = ids.len() - keep_count;
        let mut removed = 0;
        let mut logged_error = false;

        for &id in &ids[..to_remove] {
            let manifest = self.manifest_path(id);
            let state = self.state_path(id);
            let manifest_res = self.store.delete(&manifest).await;
            let state_res = self.store.delete(&state).await;

            // Count the id as removed only if the manifest is gone
            // (state.bin is optional — its absence is fine).
            let manifest_ok = matches!(
                manifest_res,
                Ok(()) | Err(object_store::Error::NotFound { .. })
            );
            if manifest_ok {
                removed += 1;
            }

            // Surface the first real error so operators can notice.
            // Permission errors silently leak old checkpoints forever
            // otherwise.
            for err in [manifest_res, state_res]
                .into_iter()
                .filter_map(Result::err)
            {
                if matches!(err, object_store::Error::NotFound { .. }) {
                    continue;
                }
                if !logged_error {
                    tracing::warn!(
                        checkpoint_id = id,
                        error = %err,
                        "[LDB-6027] checkpoint prune: delete failed — \
                         retained objects may accumulate"
                    );
                    logged_error = true;
                }
            }
        }

        Ok(removed)
    }

    async fn save_state_data(
        &self,
        id: u64,
        chunks: &[bytes::Bytes],
    ) -> Result<(), CheckpointStoreError> {
        let path = self.state_path(id);
        // PutPayload is a chain of Bytes — no concatenation into a
        // contiguous buffer. Each Arc bump is ~nothing; the underlying
        // bytes reach the object-store client untouched.
        let payload: PutPayload = chunks.iter().cloned().collect();
        // Sidecar writes are idempotent — retry transients.
        self.put_with_retry(&path, payload, &PutOptions::default()).await
    }

    async fn load_state_data(
        &self,
        id: u64,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        Ok(self
            .get_bytes(&self.state_path(id))
            .await?
            .map(|d| d.to_vec()))
    }

    async fn cleanup_orphans(&self) -> Result<usize, CheckpointStoreError> {
        use futures::{StreamExt, TryStreamExt};

        // Collect state IDs that have a state.bin but no matching manifest.
        let manifest_ids: std::collections::BTreeSet<u64> =
            self.list_checkpoint_ids().await?.into_iter().collect();

        // List state files: checkpoints/state-NNNNNN.bin
        let state_prefix = object_store::path::Path::from(format!("{}checkpoints/", self.prefix));
        let entries: Vec<_> = self.store.list(Some(&state_prefix)).try_collect().await?;

        let mut orphan_paths = Vec::new();
        for entry in &entries {
            if let Some(id) =
                parse_checkpoint_id_from_path(entry.location.as_ref(), "state-", ".bin")
            {
                if !manifest_ids.contains(&id) {
                    orphan_paths.push(entry.location.clone());
                }
            }
        }

        let count = orphan_paths.len();
        if !orphan_paths.is_empty() {
            let stream = futures::stream::iter(orphan_paths.into_iter().map(Ok)).boxed();
            let mut results = self.store.delete_stream(stream);
            while let Some(result) = results.next().await {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "failed to delete orphan state file");
                }
            }
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint_manifest::{ConnectorCheckpoint, OperatorCheckpoint};
    #[allow(clippy::disallowed_types)] // cold path: checkpoint store
    use std::collections::HashMap;

    fn make_store(dir: &Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir, 3)
    }

    fn make_manifest(id: u64, epoch: u64) -> CheckpointManifest {
        CheckpointManifest::new(id, epoch)
    }

    #[tokio::test]
    async fn test_save_and_load_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save(&m).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[tokio::test]
    async fn test_load_latest_returns_none_when_empty() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        assert!(store.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_load_latest_returns_most_recent() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let latest = store.load_latest().await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 5);
        assert_eq!(latest.epoch, 5);
    }

    #[tokio::test]
    async fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let m = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.epoch, 10);

        let m = store.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m.epoch, 20);

        assert!(store.load_by_id(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(3, 30)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[tokio::test]
    async fn test_prune_keeps_max() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10); // no auto-prune

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let removed = store.prune(2).await.unwrap();
        assert_eq!(removed, 3);

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[tokio::test]
    async fn test_auto_prune_on_save() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 2);

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 2);
        // Should keep the two most recent
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[tokio::test]
    async fn test_save_and_load_state_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&make_manifest(1, 1)).await.unwrap();

        let data = b"large operator state binary blob";
        store
            .save_state_data(1, &[bytes::Bytes::from_static(data)])
            .await
            .unwrap();

        let loaded = store.load_state_data(1).await.unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[tokio::test]
    async fn test_load_state_data_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        assert!(store.load_state_data(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_full_manifest_round_trip() {
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

        store.save(&m).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 5);
        assert_eq!(loaded.watermark, Some(999_000));

        let src = loaded.source_offsets.get("kafka-src").unwrap();
        assert_eq!(src.offsets.get("0"), Some(&"1000".into()));

        assert_eq!(loaded.sink_epochs.get("pg-sink"), Some(&4));

        let tbl = loaded.table_offsets.get("instruments").unwrap();
        assert_eq!(tbl.offsets.get("lsn"), Some(&"0/AB".into()));

        let op = loaded.operator_states.get("window").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"data");
    }

    #[tokio::test]
    async fn test_empty_latest_txt() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "").unwrap();

        assert!(store.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_latest_points_to_missing_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000099").unwrap();

        assert!(store.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_prune_no_op_when_under_limit() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        store.save(&make_manifest(1, 1)).await.unwrap();
        let removed = store.prune(5).await.unwrap();
        assert_eq!(removed, 0);
    }

    #[tokio::test]
    async fn test_save_with_state_writes_sidecar_before_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        let state = b"large-operator-state-blob";
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        // Both manifest and state should be present.
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);

        let loaded_state = store.load_state_data(1).await.unwrap().unwrap();
        assert_eq!(loaded_state, state);
    }

    #[tokio::test]
    async fn test_save_with_state_none_is_same_as_save() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save_with_state(&m, None).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert!(store.load_state_data(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_orphaned_state_without_manifest_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Write only sidecar state, no manifest (simulates crash after
        // state write but before manifest write).
        store
            .save_state_data(1, &[bytes::Bytes::from_static(b"orphaned")])
            .await
            .unwrap();

        // load_latest should return None — the orphan is not visible.
        assert!(store.load_latest().await.unwrap().is_none());

        // list should not include the orphan (no manifest.json).
        assert!(store.list().await.unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // ObjectStoreCheckpointStore tests (using InMemory backend)
    // -----------------------------------------------------------------------

    fn make_obj_store() -> ObjectStoreCheckpointStore {
        let store = Arc::new(object_store::memory::InMemory::new());
        ObjectStoreCheckpointStore::new(store, String::new(), 3)
    }

    #[tokio::test]
    async fn test_obj_save_and_load_latest() {
        let store = make_obj_store();
        let m = make_manifest(1, 1);
        store.save(&m).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[tokio::test]
    async fn test_obj_load_latest_returns_none_when_empty() {
        let store = make_obj_store();
        assert!(store.load_latest().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_obj_load_by_id() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let m = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.epoch, 10);
        let m = store.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m.epoch, 20);
        assert!(store.load_by_id(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_obj_list() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(3, 30)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[tokio::test]
    async fn test_obj_prune() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let removed = store.prune(2).await.unwrap();
        assert_eq!(removed, 3);

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[tokio::test]
    async fn test_obj_auto_prune_on_save() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            2,
        );

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, 4);
        assert_eq!(list[1].0, 5);
    }

    #[tokio::test]
    async fn test_obj_save_and_load_state_data() {
        let store = make_obj_store();
        store.save(&make_manifest(1, 1)).await.unwrap();

        let data = b"large operator state binary blob";
        store
            .save_state_data(1, &[bytes::Bytes::from_static(data)])
            .await
            .unwrap();

        let loaded = store.load_state_data(1).await.unwrap().unwrap();
        assert_eq!(loaded, data);
    }

    #[tokio::test]
    async fn test_obj_load_state_data_returns_none() {
        let store = make_obj_store();
        assert!(store.load_state_data(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_obj_with_prefix() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store =
            ObjectStoreCheckpointStore::new(inner, "nodes/abc123/".to_string(), 10);

        store.save(&make_manifest(1, 42)).await.unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 42);
    }

    // -----------------------------------------------------------------------
    // v2 layout verification + backward compatibility tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_obj_layout_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10);

        store.save(&make_manifest(1, 10)).await.unwrap();

        let result = inner
            .get_opts(
                &object_store::path::Path::from("manifests/manifest-000001.json"),
                GetOptions::default(),
            )
            .await;
        assert!(result.is_ok(), "manifest path should exist");

        let result = inner
            .get_opts(
                &object_store::path::Path::from("manifests/latest.json"),
                GetOptions::default(),
            )
            .await;
        assert!(result.is_ok(), "latest.json should exist");
    }

    #[tokio::test]
    async fn test_obj_conditional_put_idempotent() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        let m = make_manifest(1, 10);
        store.save(&m).await.unwrap();

        // Second save with same ID should succeed (logs warning, skips write)
        store.save(&m).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 10);
    }

    #[tokio::test]
    async fn test_obj_update_manifest_overwrites() {
        use crate::checkpoint_manifest::SinkCommitStatus;

        let store = make_obj_store();

        // Step 5: initial save with Pending sink status
        let mut m = make_manifest(1, 10);
        m.sink_commit_statuses
            .insert("pg-sink".into(), SinkCommitStatus::Pending);
        store.save(&m).await.unwrap();

        // Verify Pending persisted
        let loaded = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(
            loaded.sink_commit_statuses.get("pg-sink"),
            Some(&SinkCommitStatus::Pending)
        );

        // Step 6b: update manifest with Committed status
        m.sink_commit_statuses
            .insert("pg-sink".into(), SinkCommitStatus::Committed);
        store.update_manifest(&m).await.unwrap();

        // Verify Committed persisted (the bug: save() would skip this)
        let loaded = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(
            loaded.sink_commit_statuses.get("pg-sink"),
            Some(&SinkCommitStatus::Committed)
        );
    }

    #[tokio::test]
    async fn test_obj_save_still_uses_conditional_put() {
        let store = make_obj_store();

        let m = make_manifest(1, 10);
        store.save(&m).await.unwrap();

        // Second save() with same ID skips (conditional PUT)
        // but does not error
        store.save(&m).await.unwrap();

        // update_manifest() with same ID overwrites
        let mut m2 = make_manifest(1, 10);
        m2.watermark = Some(42);
        store.update_manifest(&m2).await.unwrap();

        let loaded = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(loaded.watermark, Some(42));
    }

    #[tokio::test]
    async fn test_fs_update_manifest_overwrites() {
        use crate::checkpoint_manifest::SinkCommitStatus;

        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut m = make_manifest(1, 10);
        m.sink_commit_statuses
            .insert("sink-a".into(), SinkCommitStatus::Pending);
        store.save(&m).await.unwrap();

        m.sink_commit_statuses
            .insert("sink-a".into(), SinkCommitStatus::Committed);
        store.update_manifest(&m).await.unwrap();

        let loaded = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(
            loaded.sink_commit_statuses.get("sink-a"),
            Some(&SinkCommitStatus::Committed)
        );
    }

    #[tokio::test]
    async fn test_obj_state_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10);

        store.save(&make_manifest(1, 1)).await.unwrap();
        store
            .save_state_data(1, &[bytes::Bytes::from_static(b"state-blob")])
            .await
            .unwrap();

        let result = inner
            .get_opts(
                &object_store::path::Path::from("checkpoints/state-000001.bin"),
                GetOptions::default(),
            )
            .await;
        assert!(result.is_ok(), "state path should exist");
    }

    #[tokio::test]
    async fn test_obj_latest_json_format() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10);

        store.save(&make_manifest(5, 50)).await.unwrap();

        let data = inner
            .get_opts(
                &object_store::path::Path::from("manifests/latest.json"),
                GetOptions::default(),
            )
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        let pointer: super::LatestPointer = serde_json::from_slice(&data).unwrap();
        assert_eq!(pointer.checkpoint_id, 5);
    }

    #[tokio::test]
    async fn test_obj_latest_monotonic_guard_skips_regression() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10);

        store.save(&make_manifest(10, 10)).await.unwrap();
        // A delayed writer (e.g., paused ex-leader) tries to write id=5
        // after the current leader already advanced to id=10. The pointer
        // must not regress.
        store.save(&make_manifest(5, 5)).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(
            loaded.checkpoint_id, 10,
            "latest pointer should not regress to an older id"
        );
    }

    #[tokio::test]
    async fn test_validate_checkpoint_valid() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1, 1);
        store.save(&m).await.unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(result.valid, "valid checkpoint: {:?}", result.issues);
        assert!(result.issues.is_empty());
    }

    #[tokio::test]
    async fn test_validate_checkpoint_epoch_zero_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Manually save a manifest with epoch=0 (bypassing normal creation)
        let m = make_manifest(1, 0);
        store.save(&m).await.unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(!result.valid, "epoch=0 should be invalid");
        assert!(
            result.issues.iter().any(|i| i.message().contains("epoch")),
            "should mention epoch: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_validate_checkpoint_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let result = store.validate_checkpoint(99).await.unwrap();
        assert!(!result.valid);
        assert!(result.issues[0].message().contains("not found"));
    }

    #[tokio::test]
    async fn test_validate_checkpoint_corrupt_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Create a checkpoint dir with corrupt manifest JSON.
        let cp_dir = dir.path().join("checkpoints/checkpoint_000001");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("manifest.json"), "not valid json").unwrap();

        // Corrupt manifest is a validation failure, not an I/O error.
        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(!result.valid);
        assert!(
            result.issues[0].message().contains("corrupt manifest"),
            "expected corrupt manifest issue: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_validate_checkpoint_state_checksum_ok() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        let state = b"important operator state";
        let m = make_manifest(1, 1);
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(result.valid, "checksum should match: {:?}", result.issues);
    }

    #[tokio::test]
    async fn test_validate_checkpoint_state_checksum_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save with state to get a checksum.
        let state = b"original state";
        let m = make_manifest(1, 1);
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        // Now corrupt the state.bin on disk.
        let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
        std::fs::write(&state_path, b"corrupted data!!").unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(!result.valid, "corrupted state should be invalid");
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.message().contains("checksum mismatch")),
            "should report checksum mismatch: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_validate_checkpoint_state_missing_when_expected() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save with state.
        let m = make_manifest(1, 1);
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(b"state")]))
            .await
            .unwrap();

        // Delete the state.bin file to simulate partial crash.
        let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
        std::fs::remove_file(&state_path).unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(!result.valid);
        assert!(
            result.issues.iter().any(|i| i.message().contains("not found")),
            "should report missing state: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_recover_latest_validated_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save two checkpoints.
        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        // Corrupt the latest checkpoint's manifest.
        let cp2_manifest = dir
            .path()
            .join("checkpoints/checkpoint_000002/manifest.json");
        std::fs::write(cp2_manifest, "<<<corrupt>>>").unwrap();

        // Recovery should skip checkpoint 2 and pick checkpoint 1.
        let report = store.recover_latest_validated().await.unwrap();
        assert_eq!(report.chosen_id, Some(1));
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, 2);
        assert_eq!(report.examined, 2);
    }

    #[tokio::test]
    async fn test_recover_latest_validated_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let report = store.recover_latest_validated().await.unwrap();
        assert!(report.chosen_id.is_none());
        assert_eq!(report.examined, 0);
    }

    #[tokio::test]
    async fn test_recover_latest_validated_all_corrupt_is_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Save a checkpoint, then corrupt it.
        store.save(&make_manifest(1, 1)).await.unwrap();
        let cp_manifest = dir
            .path()
            .join("checkpoints/checkpoint_000001/manifest.json");
        std::fs::write(cp_manifest, "corrupt").unwrap();

        // The corrupt manifest will cause load_by_id (via list()) to fail,
        // so it may not appear in the list at all. Either way, recovery
        // should not select it.
        let report = store.recover_latest_validated().await.unwrap();
        assert!(report.chosen_id.is_none());
    }

    #[tokio::test]
    async fn test_cleanup_orphans_removes_stateless_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        // Create an orphan: state.bin exists but no manifest.json.
        let orphan_dir = dir.path().join("checkpoints/checkpoint_000099");
        std::fs::create_dir_all(&orphan_dir).unwrap();
        std::fs::write(orphan_dir.join("state.bin"), b"orphaned").unwrap();

        // Normal checkpoint (has manifest).
        store.save(&make_manifest(1, 1)).await.unwrap();

        let cleaned = store.cleanup_orphans().await.unwrap();
        assert_eq!(cleaned, 1);

        // Orphan dir should be gone.
        assert!(!orphan_dir.exists());
        // Normal checkpoint should still be there.
        assert!(store.load_by_id(1).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_cleanup_orphans_noop_when_clean() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        store.save(&make_manifest(1, 1)).await.unwrap();
        let cleaned = store.cleanup_orphans().await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn test_save_with_state_writes_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path(), 10);

        let state = b"state-data-for-checksum";
        let m = make_manifest(1, 1);
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert!(
            loaded.state_checksum.is_some(),
            "state_checksum should be set"
        );
        let expected = sha256_hex(state);
        assert_eq!(loaded.state_checksum.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_state_checksum_backward_compat() {
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

    #[tokio::test]
    async fn test_obj_validate_checkpoint_valid() {
        let store = make_obj_store();
        store.save(&make_manifest(1, 1)).await.unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(result.valid, "valid checkpoint: {:?}", result.issues);
    }

    #[tokio::test]
    async fn test_obj_validate_checkpoint_missing() {
        let store = make_obj_store();
        let result = store.validate_checkpoint(99).await.unwrap();
        assert!(!result.valid);
    }

    #[tokio::test]
    async fn test_obj_validate_state_checksum() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        let state = b"obj-store-state-data";
        let m = make_manifest(1, 1);
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(result.valid, "checksum should match: {:?}", result.issues);
    }

    #[tokio::test]
    async fn test_obj_recover_latest_validated() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
            10,
        );

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let report = store.recover_latest_validated().await.unwrap();
        assert_eq!(report.chosen_id, Some(2));
        assert!(report.skipped.is_empty());
    }

    #[tokio::test]
    async fn test_obj_cleanup_orphans() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new(), 10);

        // Save a checkpoint (creates manifest + state).
        let state = b"state-with-manifest";
        store
            .save_with_state(
                &make_manifest(1, 1),
                Some(&[bytes::Bytes::from_static(state)]),
            )
            .await
            .unwrap();

        // Write an orphan state file (no manifest).
        let orphan_path = object_store::path::Path::from("checkpoints/state-000099.bin");
        inner
            .put_opts(
                &orphan_path,
                PutPayload::from_bytes(bytes::Bytes::from_static(b"orphan")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let cleaned = store.cleanup_orphans().await.unwrap();
        assert_eq!(cleaned, 1);

        // Verify orphan is gone but real state is intact.
        let real_state = store.load_state_data(1).await.unwrap();
        assert!(real_state.is_some());
    }
}
