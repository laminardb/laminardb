//! Checkpoint persistence via the [`CheckpointStore`] trait.
//!
//! Provides a filesystem-backed implementation ([`FileSystemCheckpointStore`])
//! that writes manifests as atomic JSON files with a `latest.txt` pointer
//! for crash-safe recovery.
//!

#![allow(clippy::disallowed_types)] // cold path: checkpoint metadata operations
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

use crate::checkpoint::checkpoint_manifest::{CheckpointManifest, DurableCheckpointPhase};
use crate::durable_fs::{durable_rename, DurableRenameMode};

/// Fsync a file to ensure its contents are durable on disk.
async fn sync_file(path: &Path) -> Result<(), std::io::Error> {
    // Must open with write access — Windows requires it for FlushFileBuffers.
    let f = tokio::fs::OpenOptions::new().write(true).open(path).await?;
    f.sync_all().await
}

/// Move a synced temporary file into place without blocking the async runtime.
async fn durable_replace(source: &Path, destination: &Path) -> Result<(), std::io::Error> {
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    tokio::task::spawn_blocking(move || {
        durable_rename(&source, &destination, DurableRenameMode::Replace)
    })
    .await
    .map_err(std::io::Error::other)?
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

    /// Persisted checkpoint metadata violates the store contract.
    #[error("invalid checkpoint: {0}")]
    Invalid(String),
}

// ---------------------------------------------------------------------------
// Checkpoint validation types
// ---------------------------------------------------------------------------

/// Classification of a single validation finding.
///
/// Both variants are fatal. They remain distinct so diagnostics can separate
/// an incompatible runtime contract from corrupt persisted bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidationIssue {
    /// Fatal manifest-level incompatibility (for example a vnode-count mismatch).
    ManifestIncompatibility(String),
    /// Fatal: manifest is missing/corrupt, or the sidecar integrity
    /// check (checksum, presence) failed.
    IntegrityFailure(String),
}

impl ValidationIssue {
    /// True if this issue renders the checkpoint unusable for recovery.
    #[must_use]
    pub fn is_fatal(&self) -> bool {
        true
    }

    /// Underlying human-readable message.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::ManifestIncompatibility(s) | Self::IntegrityFailure(s) => s,
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
    /// valid iff it has no validation issues.
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

/// Combined checksum for a mixed (inline + external) manifest:
/// `sha256(inline_hash_hex || concat(sidecar_chunks))`.
fn sha256_hex_mixed<'a, I>(
    states: &std::collections::HashMap<
        String,
        crate::checkpoint::checkpoint_manifest::OperatorCheckpoint,
    >,
    sidecar_chunks: I,
) -> String
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let inline = sha256_hex_inline_states(states);
    let mut hasher = Sha256::new();
    hasher.update(inline.as_bytes());
    for chunk in sidecar_chunks {
        hasher.update(chunk);
    }
    format!("{:x}", hasher.finalize())
}

/// SHA-256 over inline operator-state entries in sorted-name order,
/// used as the `state_checksum` when no sidecar exists.
fn sha256_hex_inline_states(
    states: &std::collections::HashMap<
        String,
        crate::checkpoint::checkpoint_manifest::OperatorCheckpoint,
    >,
) -> String {
    let mut names: Vec<&String> = states.keys().collect();
    names.sort_unstable();
    let mut hasher = Sha256::new();
    for n in names {
        if let Some(op) = states.get(n) {
            if op.external {
                continue;
            }
            hasher.update(n.as_bytes());
            hasher.update([0u8]);
            if let Some(b64) = &op.state_b64 {
                hasher.update(b64.as_bytes());
            }
            hasher.update([0u8]);
        }
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
    /// [`crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT`] when the
    /// implementation has no configured value.
    fn vnode_count(&self) -> u16 {
        crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT
    }

    /// Participant whose manifests belong in this store namespace.
    ///
    /// Embedded and standalone stores use participant `0`; cluster stores use their stable
    /// numeric instance id and a matching participant-specific namespace.
    fn participant_id(&self) -> u64 {
        0
    }

    /// Reject a manifest that was routed to another participant's namespace.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError::Invalid`] when the manifest participant does not match
    /// this store's participant.
    fn ensure_manifest_participant(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointStoreError> {
        if manifest.participant_id == self.participant_id() {
            Ok(())
        } else {
            Err(CheckpointStoreError::Invalid(format!(
                "manifest participant {} does not match store participant {}",
                manifest.participant_id,
                self.participant_id()
            )))
        }
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
    async fn load_by_id(&self, id: u64)
        -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Load an exact checkpoint from a participant namespace.
    ///
    /// Cluster recovery uses this only when the durable decision proves that the local
    /// participant was not part of the committed cut. Implementations that do not expose a
    /// shared participant namespace reject non-local reads.
    async fn load_manifest_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        if participant_id != self.participant_id() {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint store participant {} cannot read participant {participant_id}",
                self.participant_id()
            )));
        }
        self.load_by_id(id).await
    }

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
    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError>;

    /// Prunes manifests whose checkpoint epoch is strictly below `before_epoch`.
    ///
    /// This is the production retention primitive. The coordinator supplies the
    /// same externally-committed/recovery-safe horizon used for state and decision
    /// retention, so all three durable inventories advance together. The manifest
    /// referenced by the latest recovery pointer is always retained.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or deserialization failure. A
    /// malformed latest pointer or manifest fails closed without deleting it.
    async fn prune_before(&self, before_epoch: u64) -> Result<usize, CheckpointStoreError>;

    /// Atomically publish a prepared checkpoint as the latest recoverable cut.
    ///
    /// The transition is idempotent and preserves the checksum stamped by
    /// [`Self::save_with_state`].
    async fn finalize(
        &self,
        checkpoint_id: u64,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        let mut manifest = self
            .load_by_id(checkpoint_id)
            .await?
            .ok_or(CheckpointStoreError::NotFound(checkpoint_id))?;
        if manifest.checkpoint_id != checkpoint_id {
            return Err(CheckpointStoreError::Invalid(format!(
                "storage id {checkpoint_id} contains manifest id {}",
                manifest.checkpoint_id
            )));
        }
        self.ensure_manifest_participant(&manifest)?;
        let errors = manifest.validate(self.vnode_count());
        if !errors.is_empty() {
            return Err(CheckpointStoreError::Invalid(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            manifest.durable_phase = DurableCheckpointPhase::Finalized;
            self.save(&manifest).await?;
        }
        Ok(manifest)
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
    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError>;

    /// Validate a specific checkpoint's integrity.
    ///
    /// Checks that the manifest is parseable and, if a `state_checksum` is
    /// present, verifies the sidecar data matches.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn validate_checkpoint(&self, id: u64) -> Result<ValidationResult, CheckpointStoreError> {
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
            issues.push(ValidationIssue::ManifestIncompatibility(format!(
                "manifest validation: {err}"
            )));
        }
        if manifest.participant_id != self.participant_id() {
            issues.push(ValidationIssue::ManifestIncompatibility(format!(
                "manifest participant {} does not match store participant {}",
                manifest.participant_id,
                self.participant_id()
            )));
        }

        // `state_checksum` covers, depending on shape: the sidecar bytes
        // (purely-external), the inline operator_states (purely-inline),
        // or both (mixed) — see `sha256_hex_mixed`.
        if let Some(expected) = &manifest.state_checksum {
            let any_inline = manifest.operator_states.values().any(|o| !o.external);
            let any_external = manifest.operator_states.values().any(|o| o.external);
            let needs_sidecar = any_external || !any_inline;
            let sidecar = if needs_sidecar {
                self.load_state_data(id).await?
            } else {
                None
            };
            let actual = match (any_inline, &sidecar) {
                (true, Some(data)) => {
                    sha256_hex_mixed(&manifest.operator_states, std::iter::once(data.as_slice()))
                }
                (true, None) if !any_external => {
                    sha256_hex_inline_states(&manifest.operator_states)
                }
                (_, Some(data)) => sha256_hex(data),
                (_, None) => {
                    issues.push(ValidationIssue::IntegrityFailure(
                        "state.bin referenced by checksum but not found".into(),
                    ));
                    String::new()
                }
            };
            if !actual.is_empty() && actual != *expected {
                let label = if any_inline && any_external {
                    "mixed state checksum mismatch"
                } else if any_inline {
                    "inline state checksum mismatch"
                } else {
                    "state.bin checksum mismatch"
                };
                issues.push(ValidationIssue::IntegrityFailure(format!(
                    "{label}: expected {expected}, got {actual}"
                )));
            }
        }

        // epoch=0 or checkpoint_id=0 indicates a corrupted or nonsensical
        // manifest — reject as invalid regardless of other issues.
        if manifest.epoch == 0 || manifest.checkpoint_id == 0 {
            issues.push(ValidationIssue::IntegrityFailure(
                "epoch or checkpoint_id is 0 — likely corrupted".into(),
            ));
        }

        let valid = issues.is_empty();
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
    async fn recover_latest_validated(&self) -> Result<RecoveryReport, CheckpointStoreError> {
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
                match self.load_by_id(*id).await? {
                    Some(manifest)
                        if manifest.durable_phase == DurableCheckpointPhase::Finalized =>
                    {
                        return Ok(RecoveryReport {
                            chosen_id: Some(*id),
                            skipped,
                            examined,
                            elapsed: start.elapsed(),
                        });
                    }
                    Some(_) => {
                        skipped.push((*id, "checkpoint is prepared but not finalized".into()));
                        continue;
                    }
                    None => {
                        skipped.push((*id, "manifest disappeared during validation".into()));
                        continue;
                    }
                }
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
    /// cleaned up by [`Self::cleanup_orphans`].
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    async fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[bytes::Bytes]>,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        let mut manifest = manifest.clone();
        if let Some(chunks) = state_data {
            // Compute checksum across the chunks before writing. This is
            // safe because: (1) save_state_data writes to a temp then
            // renames atomically, so the on-disk bytes match the
            // in-memory chain exactly; (2) if the sidecar write fails,
            // save() is never called, so the manifest with the checksum
            // is never persisted.
            manifest.state_checksum = Some(stamp_checksum(&manifest.operator_states, Some(chunks)));
            self.save_state_data(manifest.checkpoint_id, chunks).await?;
        } else if !manifest.operator_states.is_empty()
            && manifest.operator_states.values().all(|o| !o.external)
            && manifest.state_checksum.is_none()
        {
            // Inline-only: checksum guards against a torn manifest.json write.
            manifest.state_checksum = Some(sha256_hex_inline_states(&manifest.operator_states));
        }
        self.save(&manifest).await?;
        Ok(manifest)
    }
}

/// Stamp `state_checksum` for a save: mixed manifests hash inline+sidecar
/// together, purely-external hash the sidecar alone.
fn stamp_checksum(
    states: &std::collections::HashMap<
        String,
        crate::checkpoint::checkpoint_manifest::OperatorCheckpoint,
    >,
    chunks: Option<&[bytes::Bytes]>,
) -> String {
    let chunks = chunks.unwrap_or_default();
    let any_inline = states.values().any(|o| !o.external);
    if any_inline {
        sha256_hex_mixed(states, chunks.iter().map(AsRef::as_ref))
    } else {
        sha256_hex_chunks(chunks)
    }
}

/// Filesystem-backed checkpoint store.
///
/// Writes checkpoint manifests as JSON files with atomic rename semantics.
/// A `latest.txt` pointer (not a symlink) tracks the most recent checkpoint
/// for Windows compatibility.
pub struct FileSystemCheckpointStore {
    base_dir: PathBuf,
    vnode_count: u16,
    participant_id: u64,
}

impl FileSystemCheckpointStore {
    /// Creates a new filesystem checkpoint store.
    ///
    /// The `base_dir` is the parent directory; checkpoints are stored under
    /// `{base_dir}/checkpoints/`. The directory is created lazily on first save.
    ///
    /// The store's `vnode_count` defaults to
    /// [`crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT`]. Hosts that run
    /// with a non-default value should chain [`Self::with_vnode_count`] so
    /// manifest validation checks the right invariant.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            vnode_count: crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT,
            participant_id: 0,
        }
    }

    /// Override the `vnode_count` used during manifest validation.
    #[must_use]
    pub fn with_vnode_count(mut self, vnode_count: u16) -> Self {
        self.vnode_count = vnode_count;
        self
    }

    /// Bind this store to one runtime participant.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
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

    /// Read the recovery pointer without loading its manifest. Retention must
    /// preserve this exact ID even if newer prepared attempts sort after it.
    async fn latest_checkpoint_id(&self) -> Result<Option<u64>, CheckpointStoreError> {
        let content = match tokio::fs::read_to_string(self.latest_path()).await {
            Ok(content) => content,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let id = Self::parse_checkpoint_id(content.trim()).ok_or_else(|| {
            CheckpointStoreError::Invalid(format!(
                "invalid checkpoint recovery pointer {:?}",
                content.trim()
            ))
        })?;
        Ok(Some(id))
    }

    /// Parses a checkpoint ID from a directory name like `checkpoint_000042`.
    fn parse_checkpoint_id(name: &str) -> Option<u64> {
        name.strip_prefix("checkpoint_")
            .and_then(|s| s.parse().ok())
    }

    /// Collects and sorts checkpoint directories that contain a manifest.
    ///
    /// A sidecar-only directory is an expected crash orphan: it was written
    /// before manifest publication and must not turn an otherwise fresh store
    /// into unusable checkpoint history.
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
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(Self::parse_checkpoint_id)
            else {
                continue;
            };
            match tokio::fs::metadata(entry.path().join("manifest.json")).await {
                Ok(meta) if meta.is_file() => ids.push(id),
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
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
            let has_manifest = tokio::fs::metadata(path.join("manifest.json"))
                .await
                .is_ok();
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

    fn participant_id(&self) -> u64 {
        self.participant_id
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        let cp_dir = self.checkpoint_dir(manifest.checkpoint_id);
        tokio::fs::create_dir_all(&cp_dir).await?;

        let manifest_path = self.manifest_path(manifest.checkpoint_id);
        let json = serde_json::to_string_pretty(manifest)?;

        // Write to a temp file, fsync, then rename for atomic durability.
        let tmp_path = manifest_path.with_extension("json.tmp");
        let write_res = async {
            tokio::fs::write(&tmp_path, &json).await?;
            sync_file(&tmp_path).await?;
            durable_replace(&tmp_path, &manifest_path).await
        }
        .await;
        if let Err(e) = write_res {
            // Clean up temp file to avoid orphans on disk-full.
            let _ = tokio::fs::remove_file(&tmp_path).await;
            return Err(e.into());
        }

        // Prepared attempts are inventory, never the published recovery cut.
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            return Ok(());
        }

        // Update latest.txt pointer only after a finalized manifest is durable.
        let latest = self.latest_path();
        let latest_dir = latest.parent().unwrap_or(Path::new(".")).to_path_buf();
        tokio::fs::create_dir_all(&latest_dir).await?;
        let latest_content = format!("checkpoint_{:06}", manifest.checkpoint_id);
        let tmp_latest = latest.with_extension("txt.tmp");
        tokio::fs::write(&tmp_latest, &latest_content).await?;
        sync_file(&tmp_latest).await?;
        durable_replace(&tmp_latest, &latest).await?;

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
        self.ensure_manifest_participant(&manifest)?;

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

    async fn prune_before(&self, before_epoch: u64) -> Result<usize, CheckpointStoreError> {
        let latest_id = self.latest_checkpoint_id().await?;
        let mut candidates = Vec::new();

        // Resolve the complete candidate set before deleting anything. A corrupt
        // manifest therefore fails this pass closed instead of partially advancing
        // retention past an inventory we could not classify.
        for id in self.sorted_checkpoint_ids().await? {
            if Some(id) == latest_id {
                continue;
            }
            let manifest = self
                .load_by_id(id)
                .await?
                .ok_or(CheckpointStoreError::NotFound(id))?;
            if manifest.epoch < before_epoch {
                candidates.push(id);
            }
        }

        let mut removed = 0;
        for id in candidates {
            match tokio::fs::remove_dir_all(self.checkpoint_dir(id)).await {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
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

        durable_replace(&tmp, &path).await?;

        Ok(())
    }

    async fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[bytes::Bytes]>,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        let mut manifest = manifest.clone();
        // Write sidecar FIRST — if this fails, manifest is never written
        // and latest.txt still points to the previous valid checkpoint.
        if let Some(chunks) = state_data {
            manifest.state_checksum = Some(stamp_checksum(&manifest.operator_states, Some(chunks)));
            self.save_state_data(manifest.checkpoint_id, chunks).await?;
        } else if !manifest.operator_states.is_empty()
            && manifest.operator_states.values().all(|o| !o.external)
            && manifest.state_checksum.is_none()
        {
            // Inline-only: checksum guards against a torn manifest.json write.
            manifest.state_checksum = Some(sha256_hex_inline_states(&manifest.operator_states));
        }
        self.save(&manifest).await?;
        Ok(manifest)
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

    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
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
    vnode_count: u16,
    participant_id: u64,
}

impl ObjectStoreCheckpointStore {
    /// Create a new object-store-backed checkpoint store.
    ///
    /// `prefix` is prepended to all object paths (e.g., `"nodes/abc123/"`).
    /// It should end with `/` or be empty.
    ///
    /// The store's `vnode_count` defaults to
    /// [`crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT`]. Hosts that run
    /// with a non-default value should chain [`Self::with_vnode_count`].
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            vnode_count: crate::checkpoint::checkpoint_manifest::DEFAULT_VNODE_COUNT,
            participant_id: 0,
        }
    }

    /// Override the `vnode_count` used during manifest validation.
    #[must_use]
    pub fn with_vnode_count(mut self, vnode_count: u16) -> Self {
        self.vnode_count = vnode_count;
        self
    }

    /// Bind this store to one runtime participant.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
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

    fn prefix_for_participant(&self, participant_id: u64) -> Result<String, CheckpointStoreError> {
        if participant_id == self.participant_id {
            return Ok(self.prefix.clone());
        }
        let expected = format!("nodes/{}/", self.participant_id);
        if self.prefix != expected {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint prefix '{}' is not the shared {expected} namespace",
                self.prefix
            )));
        }
        Ok(format!("nodes/{participant_id}/"))
    }

    fn manifest_path_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<object_store::path::Path, CheckpointStoreError> {
        let prefix = self.prefix_for_participant(participant_id)?;
        Ok(object_store::path::Path::from(format!(
            "{prefix}manifests/manifest-{id:06}.json"
        )))
    }

    /// Read the recovery pointer without loading its manifest. Retention must
    /// preserve this exact ID even if newer prepared attempts sort after it.
    async fn latest_checkpoint_id(&self) -> Result<Option<u64>, CheckpointStoreError> {
        let Some(data) = self.get_bytes(&self.latest_pointer_path()).await? else {
            return Ok(None);
        };
        let pointer: LatestPointer = serde_json::from_slice(&data)?;
        Ok(Some(pointer.checkpoint_id))
    }

    // ── Helpers ──

    /// Create an immutable object with bounded retry and report whether this call created it.
    ///
    /// An ambiguous transient failure is retried with the same conditional create. If the first
    /// request actually succeeded, the retry returns `AlreadyExists`; callers then compare the
    /// durable bytes with their intended content before treating the operation as idempotent.
    async fn create_with_retry(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
    ) -> Result<bool, CheckpointStoreError> {
        const BACKOFFS_MS: &[u64] = &[100, 500, 2000];
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let mut attempt = 0usize;
        loop {
            match self
                .store
                .put_opts(path, payload.clone(), options.clone())
                .await
            {
                Ok(_) => return Ok(true),
                Err(object_store::Error::AlreadyExists { .. }) => return Ok(false),
                Err(object_store::Error::Generic { .. }) if attempt < BACKOFFS_MS.len() => {
                    use rand::RngExt;
                    let base_ms = BACKOFFS_MS[attempt];
                    let jitter_ms = rand::rng().random_range(0..=base_ms / 2);
                    let delay = std::time::Duration::from_millis(base_ms + jitter_ms);
                    tracing::warn!(
                        path = %path,
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis(),
                        "transient immutable create error, retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(object_store::Error::NotImplemented { .. }) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "object store does not support conditional create required for immutable checkpoint object '{path}'"
                    )));
                }
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
        }
    }

    /// Put a payload with bounded retry + jittered backoff for replaceable
    /// writes (latest-pointer updates). Retries on
    /// `object_store::Error::Generic` — which covers most transient
    /// 5xx / connection failures across backends — and bubbles every
    /// other error immediately. Immutable checkpoint objects use
    /// [`Self::create_with_retry`] instead.
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
                Err(object_store::Error::Generic { .. }) if attempt < BACKOFFS_MS.len() => {
                    use rand::RngExt;
                    let base_ms = BACKOFFS_MS[attempt];
                    let jitter_ms = rand::rng().random_range(0..=base_ms / 2);
                    let delay = std::time::Duration::from_millis(base_ms + jitter_ms);
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
                self.ensure_manifest_participant(&manifest)?;
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
            if let Some(id) =
                parse_checkpoint_id_from_path(entry.location.as_ref(), "manifest-", ".json")
            {
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

    fn participant_id(&self) -> u64 {
        self.participant_id
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        let json = serde_json::to_string_pretty(manifest)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        let json_bytes = bytes::Bytes::from(json);

        // Conditional PUT — prevents duplicate manifest writes (split-brain safety).
        if !self
            .create_with_retry(&path, PutPayload::from_bytes(json_bytes))
            .await?
        {
            let existing = self.get_bytes(&path).await?.ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "checkpoint {} manifest create reported AlreadyExists but the object is missing",
                    manifest.checkpoint_id
                ))
            })?;
            let existing: CheckpointManifest = serde_json::from_slice(&existing)?;
            self.ensure_manifest_participant(&existing)?;
            if existing != *manifest {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint {} manifest already exists with different immutable content",
                    manifest.checkpoint_id
                )));
            }
        }

        // A prepared manifest is deliberately invisible to the normal latest
        // pointer. Recovery inventory still discovers it through list_ids().
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            return Ok(());
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

        Ok(())
    }

    async fn finalize(
        &self,
        checkpoint_id: u64,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        let mut manifest = self
            .load_by_id(checkpoint_id)
            .await?
            .ok_or(CheckpointStoreError::NotFound(checkpoint_id))?;
        if manifest.checkpoint_id != checkpoint_id {
            return Err(CheckpointStoreError::Invalid(format!(
                "storage id {checkpoint_id} contains manifest id {}",
                manifest.checkpoint_id
            )));
        }
        self.ensure_manifest_participant(&manifest)?;
        let errors = manifest.validate(self.vnode_count());
        if !errors.is_empty() {
            return Err(CheckpointStoreError::Invalid(
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; "),
            ));
        }
        if manifest.durable_phase == DurableCheckpointPhase::Finalized {
            return Ok(manifest);
        }

        manifest.durable_phase = DurableCheckpointPhase::Finalized;
        let json = serde_json::to_string_pretty(&manifest)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        let payload = PutPayload::from_bytes(bytes::Bytes::from(json));

        // Unconditional PUT — overwrites the existing manifest.
        self.store
            .put_opts(&path, payload, PutOptions::default())
            .await?;

        let latest = self.latest_pointer_path();
        if let Some(current) = self.get_bytes(&latest).await? {
            if let Ok(existing) = serde_json::from_slice::<LatestPointer>(&current) {
                if existing.checkpoint_id > manifest.checkpoint_id {
                    return Ok(manifest);
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

        Ok(manifest)
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

    async fn load_manifest_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let path = self.manifest_path_for_participant(participant_id, id)?;
        let Some(data) = self.get_bytes(&path).await? else {
            return Ok(None);
        };
        let manifest: CheckpointManifest = serde_json::from_slice(&data)?;
        if manifest.participant_id != participant_id {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest participant {} does not match storage participant {participant_id}",
                manifest.participant_id
            )));
        }
        Ok(Some(manifest))
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

    async fn prune_before(&self, before_epoch: u64) -> Result<usize, CheckpointStoreError> {
        let latest_id = self.latest_checkpoint_id().await?;
        let mut candidates = Vec::new();

        // Resolve the complete candidate set before deleting anything. A corrupt
        // manifest therefore fails this pass closed instead of partially advancing
        // retention past an inventory we could not classify.
        for id in self.list_checkpoint_ids().await? {
            if Some(id) == latest_id {
                continue;
            }
            let manifest = self
                .load_by_id(id)
                .await?
                .ok_or(CheckpointStoreError::NotFound(id))?;
            if manifest.epoch < before_epoch {
                candidates.push(id);
            }
        }

        let mut removed = 0;
        for id in candidates {
            let manifest = self.manifest_path(id);
            let state = self.state_path(id);
            let manifest_result = self.store.delete(&manifest).await;
            let state_result = self.store.delete(&state).await;

            match manifest_result {
                Ok(()) => removed += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
            if let Err(error) = state_result {
                if !matches!(error, object_store::Error::NotFound { .. }) {
                    return Err(CheckpointStoreError::ObjectStore(error));
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
        if self.create_with_retry(&path, payload).await? {
            Ok(())
        } else {
            let existing = self.get_bytes(&path).await?.ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "checkpoint {id} state create reported AlreadyExists but the object is missing"
                ))
            })?;
            let expected = sha256_hex_chunks(chunks);
            let actual = sha256_hex(&existing);
            if actual == expected {
                Ok(())
            } else {
                Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint {id} state already exists with different immutable content"
                )))
            }
        }
    }

    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
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
    use crate::checkpoint::checkpoint_manifest::{ConnectorCheckpoint, OperatorCheckpoint};
    #[allow(clippy::disallowed_types)] // cold path: checkpoint store
    use std::collections::HashMap;

    fn make_store(dir: &Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir)
    }

    fn make_manifest(id: u64, epoch: u64) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(id, epoch);
        manifest.durable_phase = DurableCheckpointPhase::Finalized;
        manifest
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
    async fn prepared_is_invisible_until_finalize() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let prepared = CheckpointManifest::new(1, 1);

        let persisted = store.save_with_state(&prepared, None).await.unwrap();
        assert_eq!(persisted.durable_phase, DurableCheckpointPhase::Prepared);
        assert!(store.load_latest().await.unwrap().is_none());
        assert_eq!(store.list_ids().await.unwrap(), vec![1]);

        let finalized = store.finalize(1).await.unwrap();
        assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
        assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
        assert_eq!(store.finalize(1).await.unwrap(), finalized);
    }

    #[tokio::test]
    async fn validated_recovery_skips_newer_prepared_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&make_manifest(1, 1)).await.unwrap();
        store
            .save_with_state(&CheckpointManifest::new(2, 2), None)
            .await
            .unwrap();

        let report = store.recover_latest_validated().await.unwrap();
        assert_eq!(report.chosen_id, Some(1));
        assert_eq!(
            report.skipped,
            vec![(2, "checkpoint is prepared but not finalized".into())]
        );
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
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(3, 30)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[tokio::test]
    async fn test_save_does_not_run_retention_inline() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 5);
    }

    #[tokio::test]
    async fn epoch_prune_preserves_latest_recovery_cut() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&make_manifest(1, 1)).await.unwrap();
        for id in 2..=5 {
            store.save(&CheckpointManifest::new(id, id)).await.unwrap();
        }

        assert_eq!(store.prune_before(10).await.unwrap(), 4);
        assert_eq!(store.list_ids().await.unwrap(), vec![1]);
        assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
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
            ConnectorCheckpoint::with_offsets(HashMap::from([
                ("events:0".into(), "1000".into()),
                ("events:1".into(), "2000".into()),
            ])),
        );
        m.table_offsets.insert(
            "instruments".into(),
            ConnectorCheckpoint::with_offsets(HashMap::from([("lsn".into(), "0/AB".into())])),
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
        assert_eq!(src.offsets.get("events:0"), Some(&"1000".into()));

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
        ObjectStoreCheckpointStore::new(store, String::new())
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
    async fn object_store_manifest_is_immutable_and_idempotent() {
        let store = make_obj_store();
        let manifest = make_manifest(7, 5);

        store.save(&manifest).await.unwrap();
        store.save(&manifest).await.unwrap();

        let mut conflicting = manifest.clone();
        conflicting.epoch = 6;
        let error = store
            .save(&conflicting)
            .await
            .expect_err("one checkpoint ID cannot name two manifests");
        assert!(error.to_string().contains("different immutable content"));
        assert_eq!(store.load_by_id(7).await.unwrap().unwrap(), manifest);
    }

    #[tokio::test]
    async fn test_obj_list() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );

        store.save(&make_manifest(1, 10)).await.unwrap();
        store.save(&make_manifest(3, 30)).await.unwrap();
        store.save(&make_manifest(2, 20)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 10), (2, 20), (3, 30)]);
    }

    #[tokio::test]
    async fn test_obj_save_does_not_run_retention_inline() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );

        for i in 1..=5 {
            store.save(&make_manifest(i, i)).await.unwrap();
        }

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 5);
    }

    #[tokio::test]
    async fn obj_epoch_prune_preserves_latest_recovery_cut() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );
        store.save(&make_manifest(1, 1)).await.unwrap();
        for id in 2..=5 {
            store.save(&CheckpointManifest::new(id, id)).await.unwrap();
        }

        assert_eq!(store.prune_before(10).await.unwrap(), 4);
        assert_eq!(store.list_ids().await.unwrap(), vec![1]);
        assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
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
    async fn object_store_state_sidecar_is_immutable_and_idempotent() {
        let store = make_obj_store();
        let original = [bytes::Bytes::from_static(b"state-blob")];

        store.save_state_data(1, &original).await.unwrap();
        store.save_state_data(1, &original).await.unwrap();

        let error = store
            .save_state_data(1, &[bytes::Bytes::from_static(b"different")])
            .await
            .expect_err("one checkpoint ID cannot overwrite its state sidecar");
        assert!(error.to_string().contains("different immutable content"));
        assert_eq!(
            store.load_state_data(1).await.unwrap().unwrap(),
            b"state-blob"
        );
    }

    #[tokio::test]
    async fn test_obj_load_state_data_returns_none() {
        let store = make_obj_store();
        assert!(store.load_state_data(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_obj_with_prefix() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner, "nodes/abc123/".to_string());

        store.save(&make_manifest(1, 42)).await.unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 42);
    }

    #[tokio::test]
    async fn test_obj_participant_namespaces_are_isolated() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let participant_11 =
            ObjectStoreCheckpointStore::new(inner.clone(), "participants/11/".to_string())
                .with_participant_id(11);
        let participant_22 = ObjectStoreCheckpointStore::new(inner, "participants/22/".to_string())
            .with_participant_id(22);

        let mut manifest_11 = make_manifest(7, 101);
        manifest_11.participant_id = 11;
        let mut manifest_22 = make_manifest(7, 202);
        manifest_22.participant_id = 22;

        participant_11.save(&manifest_11).await.unwrap();
        participant_22.save(&manifest_22).await.unwrap();

        let loaded_11 = participant_11.load_by_id(7).await.unwrap().unwrap();
        let loaded_22 = participant_22.load_by_id(7).await.unwrap().unwrap();
        assert_eq!((loaded_11.participant_id, loaded_11.epoch), (11, 101));
        assert_eq!((loaded_22.participant_id, loaded_22.epoch), (22, 202));
        assert_eq!(participant_11.list().await.unwrap(), vec![(7, 101)]);
        assert_eq!(participant_22.list().await.unwrap(), vec![(7, 202)]);
    }

    #[tokio::test]
    async fn test_obj_rejects_manifest_for_wrong_participant() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "participants/11/".to_string(),
        )
        .with_participant_id(11);
        let mut manifest = make_manifest(7, 101);
        manifest.participant_id = 22;

        let error = store.save(&manifest).await.unwrap_err();
        assert!(matches!(error, CheckpointStoreError::Invalid(_)));
        assert_eq!(
            error.to_string(),
            "invalid checkpoint: manifest participant 22 does not match store participant 11"
        );
        assert!(store.list_ids().await.unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // Object-store layout and conditional-publication tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_obj_layout_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

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
    async fn object_store_finalize_only_publishes_the_stored_prepared_manifest() {
        let store = make_obj_store();
        let prepared = CheckpointManifest::new(7, 70);
        store.save_with_state(&prepared, None).await.unwrap();
        assert!(store.load_latest().await.unwrap().is_none());

        let finalized = store.finalize(7).await.unwrap();

        assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
        assert_eq!(store.load_latest().await.unwrap(), Some(finalized));
    }

    #[tokio::test]
    async fn test_obj_state_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

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
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

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
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

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
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

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
            result
                .issues
                .iter()
                .any(|i| i.message().contains("not found")),
            "should report missing state: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_recover_latest_validated_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

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
    async fn test_recover_latest_validated_all_corrupt_reports_unusable_history() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        // Save a checkpoint, then corrupt it.
        store.save(&make_manifest(1, 1)).await.unwrap();
        let cp_manifest = dir
            .path()
            .join("checkpoints/checkpoint_000001/manifest.json");
        std::fs::write(cp_manifest, "corrupt").unwrap();

        let report = store.recover_latest_validated().await.unwrap();
        assert!(report.chosen_id.is_none());
        assert_eq!(report.examined, 1);
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].0, 1);
    }

    #[tokio::test]
    async fn test_cleanup_orphans_removes_stateless_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

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
        let store = FileSystemCheckpointStore::new(dir.path());

        store.save(&make_manifest(1, 1)).await.unwrap();
        let cleaned = store.cleanup_orphans().await.unwrap();
        assert_eq!(cleaned, 0);
    }

    #[tokio::test]
    async fn test_save_with_state_writes_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

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
    async fn test_legacy_manifest_is_rejected() {
        let json = r#"{
            "version": 1,
            "checkpoint_id": 1,
            "epoch": 1,
            "timestamp_ms": 1000
        }"#;
        assert!(serde_json::from_str::<CheckpointManifest>(json).is_err());
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
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

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
