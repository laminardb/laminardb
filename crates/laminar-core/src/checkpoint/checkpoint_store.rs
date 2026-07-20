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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};
use rand::RngExt;
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::checkpoint::checkpoint_manifest::{
    CheckpointManifest, DurableCheckpointPhase, OperatorCheckpoint,
};
use crate::durable_fs::{durable_rename, ensure_durable_directory, DurableRenameMode};
use crate::state::{KeyGroupCount, LOCAL_KEY_GROUP_COUNT};

const MAX_LATEST_POINTER_BYTES: u64 = 1_024;
const MAX_MANIFEST_BYTES: u64 = 16 * 1_024 * 1_024;
/// Default upper bound for both one checkpoint's aggregate raw logical operator state and its
/// external sidecar.
pub const DEFAULT_MAX_CHECKPOINT_STATE_BYTES: u64 = 512 * 1024 * 1024;
/// Maximum number of checkpoint entries one bounded inventory operation may materialize.
pub const MAX_CHECKPOINT_INVENTORY_ENTRIES: usize = 65_536;
const MAX_CAS_ATTEMPTS: usize = 16;

/// Validate a configured per-checkpoint operator-state byte budget.
///
/// # Errors
///
/// Returns [`CheckpointStoreError::Invalid`] when the budget is zero or cannot
/// be safely represented and overflow-probed by this process.
pub fn validate_max_checkpoint_state_bytes(limit: u64) -> Result<(), CheckpointStoreError> {
    if limit == 0 {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint state sidecar safety limit must be greater than zero".into(),
        ));
    }
    if limit.checked_add(1).is_none() {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint state sidecar safety limit must leave room for a one-byte overflow probe"
                .into(),
        ));
    }
    if usize::try_from(limit).is_err() || isize::try_from(limit).is_err() {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint state sidecar safety limit {limit} exceeds this process address space"
        )));
    }
    Ok(())
}

async fn checkpoint_cas_backoff(attempt: usize) {
    if attempt == 0 {
        tokio::task::yield_now().await;
        return;
    }
    let base_ms = 1_u64 << attempt.saturating_sub(1).min(5);
    let jitter_ms = rand::rng().random_range(0..=base_ms);
    tokio::time::sleep(std::time::Duration::from_millis(base_ms + jitter_ms)).await;
}

fn exact_phase_successor(prepared: &CheckpointManifest, finalized: &CheckpointManifest) -> bool {
    if prepared.durable_phase != DurableCheckpointPhase::Prepared
        || finalized.durable_phase != DurableCheckpointPhase::Finalized
    {
        return false;
    }
    let mut expected = prepared.clone();
    expected.durable_phase = DurableCheckpointPhase::Finalized;
    expected == *finalized
}

struct TemporaryFile(PathBuf);

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn unique_temporary_path(destination: &Path) -> Result<PathBuf, std::io::Error> {
    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    let name = destination
        .file_name()
        .ok_or_else(|| std::io::Error::other("checkpoint destination has no file name"))?
        .to_string_lossy();
    Ok(parent.join(format!(".{name}#{}", uuid::Uuid::new_v4().as_u128())))
}

fn write_synced_file(path: &Path, chunks: &[bytes::Bytes]) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    for chunk in chunks {
        file.write_all(chunk)?;
    }
    file.sync_all()
}

fn oversized_metadata(kind: &str, size: u64, limit: u64) -> CheckpointStoreError {
    CheckpointStoreError::Invalid(format!(
        "{kind} is {size} bytes, exceeding the {limit}-byte safety limit"
    ))
}

fn read_bounded_file(
    path: &Path,
    limit: u64,
    kind: &str,
) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => {
            return Err(CheckpointStoreError::Invalid(format!(
                "{kind} '{}' is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if metadata.len() > limit {
        return Err(oversized_metadata(kind, metadata.len(), limit));
    }

    let mut file = File::open(path)?;
    let capacity = usize::try_from(metadata.len()).map_err(|_| {
        CheckpointStoreError::Invalid(format!(
            "{kind} size {} cannot be represented by this process",
            metadata.len()
        ))
    })?;
    let mut bytes = Vec::with_capacity(capacity);
    std::io::Read::by_ref(&mut file)
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(oversized_metadata(kind, bytes.len() as u64, limit));
    }
    Ok(Some(bytes))
}

fn open_bounded_regular_file(
    path: &Path,
    limit: u64,
    kind: &str,
) -> Result<Option<(File, u64)>, CheckpointStoreError> {
    let path_metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata,
        Ok(_) => {
            return Err(CheckpointStoreError::Invalid(format!(
                "{kind} '{}' is not a regular file",
                path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if path_metadata.len() > limit {
        return Err(oversized_metadata(kind, path_metadata.len(), limit));
    }

    let file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.file_type().is_file() {
        return Err(CheckpointStoreError::Invalid(format!(
            "{kind} '{}' is not a regular file",
            path.display()
        )));
    }
    if opened_metadata.len() > limit {
        return Err(oversized_metadata(kind, opened_metadata.len(), limit));
    }
    if opened_metadata.len() != path_metadata.len() {
        return Err(CheckpointStoreError::Invalid(format!(
            "{kind} '{}' changed length while it was opened ({} to {} bytes)",
            path.display(),
            path_metadata.len(),
            opened_metadata.len()
        )));
    }
    Ok(Some((file, opened_metadata.len())))
}

fn read_bounded_open_file(
    file: &mut File,
    expected_len: u64,
    limit: u64,
    kind: &str,
) -> Result<Vec<u8>, CheckpointStoreError> {
    if expected_len > limit {
        return Err(oversized_metadata(kind, expected_len, limit));
    }
    let capacity = usize::try_from(expected_len).map_err(|_| {
        CheckpointStoreError::Invalid(format!(
            "{kind} length {expected_len} exceeds this process address space"
        ))
    })?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(capacity).map_err(|error| {
        CheckpointStoreError::Invalid(format!(
            "{kind} cannot reserve its advertised {expected_len}-byte length: {error}"
        ))
    })?;

    // Read through a fixed scratch buffer so neither EOF detection nor a concurrent one-byte
    // growth can make the destination Vec reserve beyond the configured checkpoint budget.
    let mut scratch = [0_u8; 64 * 1024];
    while bytes.len() < capacity {
        let remaining = capacity - bytes.len();
        let read_len = remaining.min(scratch.len());
        let read = file.read(&mut scratch[..read_len])?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&scratch[..read]);
    }
    let actual_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if actual_len != expected_len {
        return Err(CheckpointStoreError::Invalid(format!(
            "{kind} body length changed from {expected_len} to {actual_len} bytes while reading"
        )));
    }

    let mut overflow_probe = [0_u8; 1];
    let grew = file.read(&mut overflow_probe)? != 0;
    let final_len = file.metadata()?.len();
    if final_len > limit {
        return Err(oversized_metadata(kind, final_len, limit));
    }
    if grew {
        let observed_len = expected_len.saturating_add(1);
        return Err(CheckpointStoreError::Invalid(format!(
            "{kind} body grew beyond its advertised {expected_len}-byte length (observed at least {observed_len} bytes)"
        )));
    }
    if final_len != expected_len {
        return Err(CheckpointStoreError::Invalid(format!(
            "{kind} length changed from {expected_len} to {final_len} bytes while reading"
        )));
    }
    Ok(bytes)
}

fn read_bounded_regular_file(
    path: &Path,
    limit: u64,
    kind: &str,
) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
    let Some((mut file, expected_len)) = open_bounded_regular_file(path, limit, kind)? else {
        return Ok(None);
    };
    read_bounded_open_file(&mut file, expected_len, limit, kind).map(Some)
}

fn ensure_serialized_size(kind: &str, size: usize, limit: u64) -> Result<(), CheckpointStoreError> {
    let size = u64::try_from(size).unwrap_or(u64::MAX);
    if size > limit {
        Err(oversized_metadata(kind, size, limit))
    } else {
        Ok(())
    }
}

fn ensure_loaded_manifest(
    manifest: &CheckpointManifest,
    storage_checkpoint_id: u64,
    storage_participant_id: u64,
    key_group_count: KeyGroupCount,
) -> Result<(), CheckpointStoreError> {
    if manifest.checkpoint_id != storage_checkpoint_id {
        return Err(CheckpointStoreError::Invalid(format!(
            "storage checkpoint {storage_checkpoint_id} contains manifest checkpoint {}",
            manifest.checkpoint_id
        )));
    }
    if manifest.participant_id != storage_participant_id {
        return Err(CheckpointStoreError::Invalid(format!(
            "storage participant {storage_participant_id} contains manifest participant {}",
            manifest.participant_id
        )));
    }
    let errors = manifest.validate(key_group_count);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(CheckpointStoreError::Invalid(format!(
            "stored checkpoint {storage_checkpoint_id} manifest validation: {}",
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        )))
    }
}

fn open_and_lock(path: &Path) -> Result<File, std::io::Error> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.lock()?;
    Ok(file)
}

fn file_matches_chunks(path: &Path, chunks: &[bytes::Bytes]) -> Result<bool, std::io::Error> {
    let expected_len = chunks.iter().try_fold(0_u64, |total, chunk| {
        total.checked_add(chunk.len() as u64).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "checkpoint state is too large",
            )
        })
    })?;
    let mut file = File::open(path)?;
    if file.metadata()?.len() != expected_len {
        return Ok(false);
    }
    let mut scratch = [0_u8; 64 * 1024];
    for chunk in chunks {
        let mut offset = 0;
        while offset < chunk.len() {
            let length = scratch.len().min(chunk.len() - offset);
            file.read_exact(&mut scratch[..length])?;
            if scratch[..length] != chunk[offset..offset + length] {
                return Ok(false);
            }
            offset += length;
        }
    }
    Ok(true)
}

fn publish_filesystem_manifest(
    checkpoint_dir: &Path,
    manifest_path: &Path,
    manifest: &CheckpointManifest,
    encoded: bytes::Bytes,
) -> Result<(), CheckpointStoreError> {
    ensure_durable_directory(checkpoint_dir)?;
    let _lock = open_and_lock(&checkpoint_dir.join(".manifest.lock"))?;

    ensure_serialized_size("checkpoint manifest", encoded.len(), MAX_MANIFEST_BYTES)?;
    if let Some(existing) =
        read_bounded_file(manifest_path, MAX_MANIFEST_BYTES, "checkpoint manifest")?
    {
        let existing: CheckpointManifest = serde_json::from_slice(&existing)?;
        if existing == *manifest || exact_phase_successor(manifest, &existing) {
            return Ok(());
        }
        if !exact_phase_successor(&existing, manifest) {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint {} manifest already exists with different immutable content",
                manifest.checkpoint_id
            )));
        }
    }

    let temporary = unique_temporary_path(manifest_path)?;
    let cleanup = TemporaryFile(temporary.clone());
    write_synced_file(&temporary, &[encoded])?;
    let mode = if manifest_path.exists() {
        DurableRenameMode::Replace
    } else {
        DurableRenameMode::NoReplace
    };
    durable_rename(&temporary, manifest_path, mode)?;
    drop(cleanup);
    Ok(())
}

fn publish_filesystem_state(
    checkpoint_dir: &Path,
    state_path: &Path,
    chunks: &[bytes::Bytes],
) -> Result<(), CheckpointStoreError> {
    ensure_durable_directory(checkpoint_dir)?;
    let _lock = open_and_lock(&checkpoint_dir.join(".state.lock"))?;
    match std::fs::symlink_metadata(state_path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint state path '{}' is not a regular file",
                state_path.display()
            )));
        }
        Ok(_) if file_matches_chunks(state_path, chunks)? => return Ok(()),
        Ok(_) => {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint state '{}' already exists with different immutable content",
                state_path.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let temporary = unique_temporary_path(state_path)?;
    let cleanup = TemporaryFile(temporary.clone());
    write_synced_file(&temporary, chunks)?;
    durable_rename(&temporary, state_path, DurableRenameMode::NoReplace)?;
    drop(cleanup);
    Ok(())
}

fn parse_filesystem_latest(bytes: &[u8]) -> Result<u64, CheckpointStoreError> {
    let content = std::str::from_utf8(bytes).map_err(|error| {
        CheckpointStoreError::Invalid(format!("checkpoint recovery pointer is not UTF-8: {error}"))
    })?;
    let name = content.trim();
    FileSystemCheckpointStore::parse_checkpoint_id(name).ok_or_else(|| {
        CheckpointStoreError::Invalid(format!("invalid checkpoint recovery pointer {name:?}"))
    })
}

fn load_filesystem_manifest(
    path: &Path,
    checkpoint_id: u64,
    participant_id: u64,
    key_group_count: KeyGroupCount,
) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
    let Some(bytes) = read_bounded_file(path, MAX_MANIFEST_BYTES, "checkpoint manifest")? else {
        return Ok(None);
    };
    let manifest: CheckpointManifest = serde_json::from_slice(&bytes)?;
    ensure_loaded_manifest(&manifest, checkpoint_id, participant_id, key_group_count)?;
    Ok(Some(manifest))
}

fn ensure_filesystem_latest_target(
    checkpoints_dir: &Path,
    checkpoint_id: u64,
    participant_id: u64,
    key_group_count: KeyGroupCount,
) -> Result<(), CheckpointStoreError> {
    let manifest_path = checkpoints_dir
        .join(format!("checkpoint_{checkpoint_id:06}"))
        .join("manifest.json");
    let manifest = load_filesystem_manifest(
        &manifest_path,
        checkpoint_id,
        participant_id,
        key_group_count,
    )?
    .ok_or_else(|| {
        CheckpointStoreError::Invalid(format!(
            "checkpoint recovery pointer references missing checkpoint {checkpoint_id}"
        ))
    })?;
    if manifest.durable_phase != DurableCheckpointPhase::Finalized {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint recovery pointer references non-finalized checkpoint {checkpoint_id}"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[derive(Debug, Default)]
struct FilesystemLatestPublicationGate {
    state: std::sync::Mutex<(bool, bool)>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl FilesystemLatestPublicationGate {
    fn block(&self) -> Result<(), std::io::Error> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| std::io::Error::other("latest publication gate is poisoned"))?;
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self
                .changed
                .wait(state)
                .map_err(|_| std::io::Error::other("latest publication gate is poisoned"))?;
        }
        Ok(())
    }

    fn wait_until_entered(&self, timeout: std::time::Duration) -> bool {
        let Ok(state) = self.state.lock() else {
            return false;
        };
        self.changed
            .wait_timeout_while(state, timeout, |state| !state.0)
            .is_ok_and(|(state, _)| state.0)
    }

    fn release(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.1 = true;
            self.changed.notify_all();
        }
    }
}

fn publish_filesystem_latest(
    checkpoints_dir: &Path,
    latest_path: &Path,
    checkpoint_id: u64,
    participant_id: u64,
    key_group_count: KeyGroupCount,
    #[cfg(test)] gate: Option<Arc<FilesystemLatestPublicationGate>>,
) -> Result<(), CheckpointStoreError> {
    ensure_durable_directory(checkpoints_dir)?;
    let _lock = open_and_lock(&checkpoints_dir.join(".latest.lock"))?;
    #[cfg(test)]
    if let Some(gate) = gate {
        gate.block()?;
    }

    if let Some(existing) = read_bounded_file(
        latest_path,
        MAX_LATEST_POINTER_BYTES,
        "checkpoint recovery pointer",
    )? {
        let current = parse_filesystem_latest(&existing)?;
        ensure_filesystem_latest_target(checkpoints_dir, current, participant_id, key_group_count)?;
        if current >= checkpoint_id {
            return Ok(());
        }
    }

    let temporary = unique_temporary_path(latest_path)?;
    let cleanup = TemporaryFile(temporary.clone());
    let content = bytes::Bytes::from(format!("checkpoint_{checkpoint_id:06}"));
    write_synced_file(&temporary, &[content])?;
    durable_rename(&temporary, latest_path, DurableRenameMode::Replace)?;
    drop(cleanup);
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
    /// Fatal manifest-level incompatibility (for example a key-group-count mismatch).
    ManifestIncompatibility(String),
    /// Fatal: manifest is missing/corrupt, or the sidecar integrity
    /// check (checksum, presence) failed.
    IntegrityFailure(String),
}

impl ValidationIssue {
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

/// Manifest and optional operator-state sidecar loaded for one exact checkpoint.
///
/// The sidecar is present only when the manifest's checksum shape requires it. Keeping both
/// artifacts together lets recovery validate and restore the same bytes without a second read.
#[derive(Debug, Clone, PartialEq)]
pub struct CheckpointArtifacts {
    /// Parsed checkpoint manifest.
    pub manifest: CheckpointManifest,
    /// Exact checksum-required `state.bin` bytes, if the manifest uses a sidecar.
    pub state_data: Option<Vec<u8>>,
}

impl CheckpointArtifacts {
    /// Validate these already-loaded bytes without blocking an async runtime worker.
    ///
    /// Ownership is returned with the result so recovery can restore the same sidecar allocation
    /// that was checksummed instead of cloning or re-reading it.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError::Invalid`] for an unusable state budget, or
    /// [`CheckpointStoreError::Io`] if the blocking validation task cannot be joined.
    pub async fn validate(
        self,
        checkpoint_id: u64,
        participant_id: u64,
        key_group_count: KeyGroupCount,
        max_state_data_bytes: u64,
    ) -> Result<(Self, ValidationResult), CheckpointStoreError> {
        validate_max_checkpoint_state_bytes(max_state_data_bytes)?;
        tokio::task::spawn_blocking(move || {
            let result = validate_checkpoint_artifacts(
                &self,
                checkpoint_id,
                participant_id,
                key_group_count,
                max_state_data_bytes,
            );
            (self, result)
        })
        .await
        .map_err(|error| {
            std::io::Error::other(format!("checkpoint validation task failed: {error}"))
        })
        .map_err(CheckpointStoreError::Io)
    }
}

fn exact_inline_state_len(encoded: &str) -> Result<u64, String> {
    let mut decoder = base64::read::DecoderReader::new(
        encoded.as_bytes(),
        &base64::engine::general_purpose::STANDARD,
    );
    let mut decoded_len = 0_u64;
    let mut scratch = [0_u8; 8 * 1024];
    loop {
        let read = decoder
            .read(&mut scratch)
            .map_err(|error| format!("invalid base64: {error}"))?;
        if read == 0 {
            return Ok(decoded_len);
        }
        decoded_len = decoded_len
            .checked_add(u64::try_from(read).unwrap_or(u64::MAX))
            .ok_or_else(|| "decoded inline state length overflow".to_string())?;
    }
}

type ExternalStateRange<'a> = (u64, u64, &'a str);

fn operator_raw_state_len<'a>(
    name: &'a str,
    state: &OperatorCheckpoint,
    external_ranges: &mut Vec<ExternalStateRange<'a>>,
    issues: &mut Vec<ValidationIssue>,
) -> Option<u64> {
    if state.external {
        if state.state_b64.is_some() {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "operator '{name}' external state also contains inline base64"
            )));
        }
        if state.external_length == 0 {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "operator '{name}' has an empty external sidecar range"
            )));
        }
        match state.external_offset.checked_add(state.external_length) {
            Some(end) => external_ranges.push((state.external_offset, end, name)),
            None => issues.push(ValidationIssue::IntegrityFailure(format!(
                "operator '{name}' external sidecar range overflows"
            ))),
        }
        return Some(state.external_length);
    }

    if state.external_offset != 0 || state.external_length != 0 {
        issues.push(ValidationIssue::IntegrityFailure(format!(
            "operator '{name}' inline state has nonzero external offset or length"
        )));
    }
    let Some(encoded) = state.state_b64.as_deref() else {
        issues.push(ValidationIssue::IntegrityFailure(format!(
            "operator '{name}' inline state is missing base64 data"
        )));
        return None;
    };
    match exact_inline_state_len(encoded) {
        Ok(length) => Some(length),
        Err(error) => {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "operator '{name}' inline state {error}"
            )));
            None
        }
    }
}

fn validate_external_state_ranges(
    mut external_ranges: Vec<ExternalStateRange<'_>>,
    state_data_len: Option<u64>,
    issues: &mut Vec<ValidationIssue>,
) {
    external_ranges.sort_unstable();
    if external_ranges.is_empty() {
        if state_data_len.is_some() {
            issues.push(ValidationIssue::IntegrityFailure(
                "checkpoint state sidecar has no external operator ranges".into(),
            ));
        }
        return;
    }

    let mut expected_offset = 0_u64;
    for (start, end, name) in external_ranges {
        if start != expected_offset {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "operator '{name}' external sidecar range starts at {start}, expected {expected_offset}"
            )));
        }
        expected_offset = end;
    }
    match state_data_len {
        Some(actual) if actual != expected_offset => {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "checkpoint state sidecar is {actual} bytes but external operator ranges cover {expected_offset} bytes"
            )));
        }
        None => issues.push(ValidationIssue::IntegrityFailure(
            "external operator state references a missing checkpoint state sidecar".into(),
        )),
        Some(_) => {}
    }
}

fn operator_state_validation_issues(
    manifest: &CheckpointManifest,
    state_data_len: Option<u64>,
    max_state_data_bytes: u64,
) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    if let Some(length) = state_data_len {
        if length > max_state_data_bytes {
            issues.push(ValidationIssue::IntegrityFailure(format!(
                "checkpoint state sidecar is {length} bytes, exceeding the {max_state_data_bytes}-byte safety limit"
            )));
        }
    }

    let mut names = manifest.operator_states.keys().collect::<Vec<_>>();
    names.sort_unstable();
    let mut logical_bytes = 0_u64;
    let mut logical_overflow = false;
    let mut external_ranges = Vec::new();

    for name in names {
        let state = &manifest.operator_states[name];
        let raw_length =
            operator_raw_state_len(name.as_str(), state, &mut external_ranges, &mut issues);

        if let Some(raw_length) = raw_length {
            if let Some(total) = logical_bytes.checked_add(raw_length) {
                logical_bytes = total;
            } else if !logical_overflow {
                logical_overflow = true;
                issues.push(ValidationIssue::IntegrityFailure(
                    "aggregate logical operator state length overflows".into(),
                ));
            }
        }
    }

    if !logical_overflow && logical_bytes > max_state_data_bytes {
        issues.push(ValidationIssue::IntegrityFailure(format!(
            "aggregate logical operator state is {logical_bytes} bytes, exceeding the {max_state_data_bytes}-byte safety limit"
        )));
    }

    validate_external_state_ranges(external_ranges, state_data_len, &mut issues);

    if manifest.operator_states.is_empty() && manifest.state_checksum.is_some() {
        issues.push(ValidationIssue::IntegrityFailure(
            "state checksum has no operator state".into(),
        ));
    }
    issues
}

fn validate_checkpoint_artifacts(
    artifacts: &CheckpointArtifacts,
    checkpoint_id: u64,
    participant_id: u64,
    key_group_count: KeyGroupCount,
    max_state_data_bytes: u64,
) -> ValidationResult {
    let manifest = &artifacts.manifest;
    let mut issues = manifest
        .validate(key_group_count)
        .into_iter()
        .map(|error| {
            ValidationIssue::ManifestIncompatibility(format!("manifest validation: {error}"))
        })
        .collect::<Vec<_>>();

    if manifest.participant_id != participant_id {
        issues.push(ValidationIssue::ManifestIncompatibility(format!(
            "manifest participant {} does not match store participant {participant_id}",
            manifest.participant_id
        )));
    }
    if manifest.checkpoint_id != checkpoint_id {
        issues.push(ValidationIssue::IntegrityFailure(format!(
            "storage checkpoint {checkpoint_id} contains manifest checkpoint {}",
            manifest.checkpoint_id
        )));
    }

    let state_data_len = artifacts
        .state_data
        .as_ref()
        .map(|data| u64::try_from(data.len()).unwrap_or(u64::MAX));
    issues.extend(operator_state_validation_issues(
        manifest,
        state_data_len,
        max_state_data_bytes,
    ));

    // Retain this integrity classification in addition to the manifest compatibility finding.
    if manifest.epoch == 0 || manifest.checkpoint_id == 0 {
        issues.push(ValidationIssue::IntegrityFailure(
            "epoch or checkpoint_id is 0 — likely corrupted".into(),
        ));
    }

    // Invalid shape, namespace, or budget is already fatal; avoid hashing a large sidecar that
    // recovery cannot use.
    if issues.is_empty() {
        if let Some(expected) = &manifest.state_checksum {
            let (any_inline, any_external) = operator_state_shape(manifest);
            let actual = match (any_inline, any_external, artifacts.state_data.as_deref()) {
                (true, true, Some(data)) => {
                    sha256_hex_mixed(&manifest.operator_states, std::iter::once(data))
                }
                (true, false, _) => sha256_hex_inline_states(&manifest.operator_states),
                (false, true, Some(data)) => sha256_hex(data),
                _ => unreachable!("validated operator state shape must select checksum bytes"),
            };
            if actual != *expected {
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
    }

    ValidationResult {
        checkpoint_id,
        valid: issues.is_empty(),
        issues,
    }
}

fn artifact_load_failure(checkpoint_id: u64, message: String) -> ValidationResult {
    ValidationResult {
        checkpoint_id,
        valid: false,
        issues: vec![ValidationIssue::IntegrityFailure(message)],
    }
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
            let canonical = format!("{prefix}{id:06}{suffix}");
            if segment == canonical {
                return Some(id);
            }
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

fn checked_state_data_len(chunks: &[bytes::Bytes]) -> Result<u64, CheckpointStoreError> {
    chunks.iter().try_fold(0_u64, |total, chunk| {
        let chunk_len = u64::try_from(chunk.len()).map_err(|_| {
            CheckpointStoreError::Invalid("checkpoint state sidecar length overflow".into())
        })?;
        total.checked_add(chunk_len).ok_or_else(|| {
            CheckpointStoreError::Invalid("checkpoint state sidecar length overflow".into())
        })
    })
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

fn operator_state_shape(manifest: &CheckpointManifest) -> (bool, bool) {
    manifest
        .operator_states
        .values()
        .fold((false, false), |(inline, external), operator| {
            (inline || !operator.external, external || operator.external)
        })
}

fn requires_state_data(manifest: &CheckpointManifest) -> bool {
    manifest.state_checksum.is_some()
        && manifest
            .operator_states
            .values()
            .any(|state| state.external)
}

/// Trait for checkpoint persistence backends.
///
/// Implementations must guarantee atomic manifest writes (readers never see
/// a partial manifest). The `latest.txt` pointer is updated only after the
/// manifest is fully written and synced.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Maximum aggregate raw logical operator-state bytes admitted for one checkpoint.
    ///
    /// The same bound independently limits the physical sidecar. Implementations must enforce it
    /// on writes, body reads, metadata-only length reads, and artifact validation.
    fn max_state_data_bytes(&self) -> u64;

    /// Runtime key-group count that manifests written by this store are
    /// expected to use. Consulted when validating loaded manifests. Embedded
    /// and single-node stores default to one key group.
    fn key_group_count(&self) -> KeyGroupCount {
        LOCAL_KEY_GROUP_COUNT
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

    /// Reject an invalid manifest before any sidecar, directory, or object is created.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError::Invalid`] when the manifest violates an invariant.
    fn ensure_manifest_valid(
        &self,
        manifest: &CheckpointManifest,
    ) -> Result<(), CheckpointStoreError> {
        let errors = manifest.validate(self.key_group_count());
        if errors.is_empty() {
            Ok(())
        } else {
            Err(CheckpointStoreError::Invalid(format!(
                "manifest validation: {}",
                errors
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("; ")
            )))
        }
    }

    /// Atomically persists a checkpoint manifest. Implementations must
    /// guarantee readers never observe a partial manifest.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or serialization failure.
    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError>;

    /// Loads the most recent checkpoint manifest, or `Ok(None)` when the
    /// recovery pointer does not exist in a fresh store.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O or deserialization failure, or
    /// when an existing recovery pointer is malformed, dangling, or references
    /// anything other than the matching Finalized manifest.
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
    /// Returns [`CheckpointStoreError`] on I/O, deserialization, or manifest validation failure.
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
        let errors = manifest.validate(self.key_group_count());
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
        }
        // Save even when the manifest was already Finalized: a crash may have landed that phase
        // transition but not its recovery pointer, and finalize is the repair operation.
        self.save(&manifest).await?;
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
    /// Returns [`CheckpointStoreError`] on I/O failure or when the sidecar
    /// exceeds [`Self::max_state_data_bytes`].
    async fn save_state_data(
        &self,
        id: u64,
        chunks: &[bytes::Bytes],
    ) -> Result<(), CheckpointStoreError>;

    /// Loads operator state sidecar bytes for a checkpoint, or `Ok(None)`
    /// if no sidecar was written.
    ///
    /// # Errors
    /// Returns [`CheckpointStoreError`] on I/O failure, malformed storage
    /// metadata/body, or a sidecar above [`Self::max_state_data_bytes`].
    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError>;

    /// Load operator state sidecar bytes from a participant namespace.
    ///
    /// Stores without a shared participant namespace reject non-local reads.
    async fn load_state_data_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        if participant_id != self.participant_id() {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint store participant {} cannot read participant {participant_id}",
                self.participant_id()
            )));
        }
        self.load_state_data(id).await
    }

    /// Read only the durable sidecar object's length from storage metadata.
    ///
    /// Cluster retention uses this to prove that a manifest's immutable sidecar still exists
    /// without downloading its body. Custom stores fail closed until they provide an equivalent
    /// metadata-only lookup.
    async fn state_data_len_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<u64>, CheckpointStoreError> {
        Err(CheckpointStoreError::Invalid(format!(
            "checkpoint store participant {} cannot provide metadata-only sidecar evidence for participant {participant_id} checkpoint {id}",
            self.participant_id()
        )))
    }

    /// Load the manifest and checksum-required sidecar for a local checkpoint exactly once.
    async fn load_checkpoint_artifacts(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointArtifacts>, CheckpointStoreError> {
        self.load_checkpoint_artifacts_for_participant(self.participant_id(), id)
            .await
    }

    /// Load the manifest and checksum-required sidecar from a participant namespace exactly once.
    ///
    /// Inline-only checkpoints do not read `state.bin`. External and mixed shapes perform one
    /// sidecar read after the manifest has selected that shape.
    async fn load_checkpoint_artifacts_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<CheckpointArtifacts>, CheckpointStoreError> {
        let Some(manifest) = self
            .load_manifest_for_participant(participant_id, id)
            .await?
        else {
            return Ok(None);
        };
        let state_data = if requires_state_data(&manifest) {
            self.load_state_data_for_participant(participant_id, id)
                .await?
        } else {
            None
        };
        Ok(Some(CheckpointArtifacts {
            manifest,
            state_data,
        }))
    }

    /// Validate a specific checkpoint's integrity.
    ///
    /// Checks that the manifest is parseable and, if a `state_checksum` is
    /// present, verifies the sidecar data matches.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O failure.
    async fn validate_checkpoint(&self, id: u64) -> Result<ValidationResult, CheckpointStoreError> {
        let artifacts = match self.load_checkpoint_artifacts(id).await {
            Ok(Some(artifacts)) => artifacts,
            Ok(None) => {
                return Ok(artifact_load_failure(
                    id,
                    format!("manifest not found for checkpoint {id}"),
                ));
            }
            Err(CheckpointStoreError::Serde(error)) => {
                return Ok(artifact_load_failure(
                    id,
                    format!("corrupt manifest: {error}"),
                ));
            }
            Err(CheckpointStoreError::Invalid(error)) => {
                return Ok(artifact_load_failure(id, error));
            }
            Err(error) => return Err(error),
        };
        let (_, validation) = artifacts
            .validate(
                id,
                self.participant_id(),
                self.key_group_count(),
                self.max_state_data_bytes(),
            )
            .await?;
        Ok(validation)
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
            let (result, durable_phase) = match self.load_checkpoint_artifacts(*id).await {
                Ok(Some(artifacts)) => {
                    let durable_phase = artifacts.manifest.durable_phase;
                    let (_, validation) = artifacts
                        .validate(
                            *id,
                            self.participant_id(),
                            self.key_group_count(),
                            self.max_state_data_bytes(),
                        )
                        .await?;
                    (validation, Some(durable_phase))
                }
                Ok(None) => (
                    artifact_load_failure(*id, format!("manifest not found for checkpoint {id}")),
                    None,
                ),
                Err(CheckpointStoreError::Serde(error)) => (
                    artifact_load_failure(*id, format!("corrupt manifest: {error}")),
                    None,
                ),
                Err(CheckpointStoreError::Invalid(error)) => {
                    (artifact_load_failure(*id, error), None)
                }
                Err(error) => return Err(error),
            };
            if result.valid {
                if durable_phase == Some(DurableCheckpointPhase::Finalized) {
                    return Ok(RecoveryReport {
                        chosen_id: Some(*id),
                        skipped,
                        examined,
                        elapsed: start.elapsed(),
                    });
                }
                skipped.push((*id, "checkpoint is prepared but not finalized".into()));
                continue;
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

    /// Atomically saves a checkpoint manifest with optional sidecar state data.
    ///
    /// When `state_data` is provided, the sidecar (`state.bin`) is written and
    /// fsynced **before** the manifest. This ensures that if the sidecar write
    /// fails, the manifest is never persisted and `latest.txt` still points to
    /// the previous valid checkpoint.
    ///
    /// A `state.bin` written without a manifest is not visible to checkpoint
    /// inventory or recovery.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError`] on I/O, serialization, validation, or operator state
    /// above [`Self::max_state_data_bytes`].
    async fn save_with_state(
        &self,
        manifest: &CheckpointManifest,
        state_data: Option<&[bytes::Bytes]>,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        let max_state_data_bytes = self.max_state_data_bytes();
        validate_max_checkpoint_state_bytes(max_state_data_bytes)?;

        // Validate the cheap manifest invariants before scheduling a potentially large hash. The
        // checksum is always restamped below, so a temporary value satisfies its presence rule.
        let mut manifest = manifest.clone();
        if !manifest.operator_states.is_empty() && manifest.state_checksum.is_none() {
            manifest.state_checksum = Some("pending".into());
        }
        self.ensure_manifest_valid(&manifest)?;

        // Bytes clones retain the capture buffers without copying their contents. Hashing and
        // exact base64/layout validation are CPU work and must not occupy a Tokio worker.
        let state_data = state_data.map(<[bytes::Bytes]>::to_vec);
        let (manifest, state_data) = tokio::task::spawn_blocking(move || {
            let state_data_len = state_data
                .as_deref()
                .map(checked_state_data_len)
                .transpose()?;
            let issues =
                operator_state_validation_issues(&manifest, state_data_len, max_state_data_bytes);
            if !issues.is_empty() {
                return Err(CheckpointStoreError::Invalid(format!(
                    "operator state validation: {}",
                    issues
                        .iter()
                        .map(ValidationIssue::message)
                        .collect::<Vec<_>>()
                        .join("; ")
                )));
            }

            let mut manifest = manifest;
            manifest.state_checksum = if let Some(chunks) = state_data.as_deref() {
                Some(stamp_checksum(&manifest.operator_states, Some(chunks)))
            } else if manifest.operator_states.is_empty() {
                None
            } else {
                Some(sha256_hex_inline_states(&manifest.operator_states))
            };
            Ok((manifest, state_data))
        })
        .await
        .map_err(|error| {
            CheckpointStoreError::Io(std::io::Error::other(format!(
                "checkpoint checksum task failed: {error}"
            )))
        })??;

        self.ensure_manifest_valid(&manifest)?;
        if let Some(chunks) = state_data.as_deref() {
            self.save_state_data(manifest.checkpoint_id, chunks).await?;
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
    key_group_count: KeyGroupCount,
    participant_id: u64,
    max_state_data_bytes: u64,
    #[cfg(test)]
    latest_publication_gate: Option<Arc<FilesystemLatestPublicationGate>>,
}

impl FileSystemCheckpointStore {
    /// Creates a new filesystem checkpoint store.
    ///
    /// The `base_dir` is the parent directory; checkpoints are stored under
    /// `{base_dir}/checkpoints/`. The directory is created lazily on first save.
    ///
    /// Embedded and single-node stores default to one key group. Cluster hosts
    /// must chain [`Self::with_key_group_count`] with their durable topology.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            key_group_count: LOCAL_KEY_GROUP_COUNT,
            participant_id: 0,
            max_state_data_bytes: DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
            #[cfg(test)]
            latest_publication_gate: None,
        }
    }

    #[cfg(test)]
    fn with_latest_publication_gate(mut self, gate: Arc<FilesystemLatestPublicationGate>) -> Self {
        self.latest_publication_gate = Some(gate);
        self
    }

    /// Override the stable key-group count used during manifest validation.
    #[must_use]
    pub fn with_key_group_count(mut self, key_group_count: KeyGroupCount) -> Self {
        self.key_group_count = key_group_count;
        self
    }

    /// Bind this store to one runtime participant.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
        self
    }

    /// Override the aggregate raw logical-state and physical-sidecar bytes admitted per checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError::Invalid`] when the limit cannot safely
    /// bound an in-memory restore allocation and its one-byte overflow probe.
    pub fn with_max_state_data_bytes(
        mut self,
        max_state_data_bytes: u64,
    ) -> Result<Self, CheckpointStoreError> {
        validate_max_checkpoint_state_bytes(max_state_data_bytes)?;
        self.max_state_data_bytes = max_state_data_bytes;
        Ok(self)
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
        Ok(self
            .load_latest()
            .await?
            .map(|manifest| manifest.checkpoint_id))
    }

    /// Parses a checkpoint ID from a directory name like `checkpoint_000042`.
    fn parse_checkpoint_id(name: &str) -> Option<u64> {
        let id = name.strip_prefix("checkpoint_")?.parse::<u64>().ok()?;
        (name == format!("checkpoint_{id:06}")).then_some(id)
    }

    /// Collects and sorts checkpoint directories that contain a manifest.
    ///
    /// A sidecar-only directory is an expected crash orphan: it was written
    /// before manifest publication and must not turn an otherwise fresh store
    /// into unusable checkpoint history.
    async fn sorted_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.sorted_checkpoint_ids_with_limit(MAX_CHECKPOINT_INVENTORY_ENTRIES)
            .await
    }

    async fn sorted_checkpoint_ids_with_limit(
        &self,
        max_entries: usize,
    ) -> Result<Vec<u64>, CheckpointStoreError> {
        let dir = self.checkpoints_dir();
        let mut reader = match tokio::fs::read_dir(&dir).await {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut ids: Vec<u64> = Vec::new();
        let mut scanned = 0usize;
        while let Some(entry) = reader.next_entry().await? {
            if scanned >= max_entries {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint inventory exceeds the {max_entries}-entry safety limit"
                )));
            }
            scanned += 1;
            let ft = entry.file_type().await?;
            if !ft.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(id) = Self::parse_checkpoint_id(name) else {
                if name.starts_with("checkpoint_") {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "non-canonical checkpoint inventory entry '{name}'"
                    )));
                }
                continue;
            };
            let manifest_path = entry.path().join("manifest.json");
            match tokio::fs::symlink_metadata(&manifest_path).await {
                Ok(meta) if meta.file_type().is_file() => {
                    ids.push(id);
                }
                Ok(_) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "checkpoint manifest '{}' is not a regular file",
                        manifest_path.display()
                    )));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        ids.sort_unstable();
        Ok(ids)
    }
}

#[async_trait]
impl CheckpointStore for FileSystemCheckpointStore {
    fn max_state_data_bytes(&self) -> u64 {
        self.max_state_data_bytes
    }

    fn key_group_count(&self) -> KeyGroupCount {
        self.key_group_count
    }

    fn participant_id(&self) -> u64 {
        self.participant_id
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        self.ensure_manifest_valid(manifest)?;
        let cp_dir = self.checkpoint_dir(manifest.checkpoint_id);
        let manifest_path = self.manifest_path(manifest.checkpoint_id);
        let encoded = bytes::Bytes::from(serde_json::to_vec_pretty(manifest)?);
        let owned_manifest = manifest.clone();
        tokio::task::spawn_blocking(move || {
            publish_filesystem_manifest(&cp_dir, &manifest_path, &owned_manifest, encoded)
        })
        .await
        .map_err(std::io::Error::other)??;

        // Prepared attempts are inventory, never the published recovery cut.
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            return Ok(());
        }

        // The blocking task owns the lock and the complete compare/publish transaction. Dropping
        // this future cannot leave a detached stale writer that later regresses the pointer.
        let latest = self.latest_path();
        let checkpoints = self.checkpoints_dir();
        let checkpoint_id = manifest.checkpoint_id;
        let participant_id = self.participant_id;
        let key_group_count = self.key_group_count;
        #[cfg(test)]
        let gate = self.latest_publication_gate.clone();
        tokio::task::spawn_blocking(move || {
            publish_filesystem_latest(
                &checkpoints,
                &latest,
                checkpoint_id,
                participant_id,
                key_group_count,
                #[cfg(test)]
                gate,
            )
        })
        .await
        .map_err(std::io::Error::other)??;

        Ok(())
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let latest = self.latest_path();
        let content = tokio::task::spawn_blocking(move || {
            read_bounded_file(
                &latest,
                MAX_LATEST_POINTER_BYTES,
                "checkpoint recovery pointer",
            )
        })
        .await
        .map_err(std::io::Error::other)??;
        let Some(content) = content else {
            return Ok(None);
        };
        let checkpoint_id = parse_filesystem_latest(&content)?;
        let manifest = self.load_by_id(checkpoint_id).await?.ok_or_else(|| {
            CheckpointStoreError::Invalid(format!(
                "checkpoint recovery pointer references missing checkpoint {checkpoint_id}"
            ))
        })?;
        if manifest.durable_phase != DurableCheckpointPhase::Finalized {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint recovery pointer references non-finalized checkpoint {checkpoint_id}"
            )));
        }
        Ok(Some(manifest))
    }

    async fn load_by_id(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let path = self.manifest_path(id);
        let participant_id = self.participant_id;
        let key_group_count = self.key_group_count;
        tokio::task::spawn_blocking(move || {
            load_filesystem_manifest(&path, id, participant_id, key_group_count)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.sorted_checkpoint_ids().await
    }

    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.sorted_checkpoint_ids().await?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            let manifest = self
                .load_by_id(id)
                .await?
                .ok_or(CheckpointStoreError::NotFound(id))?;
            result.push((manifest.checkpoint_id, manifest.epoch));
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
        let state_len = checked_state_data_len(chunks)?;
        if state_len > self.max_state_data_bytes {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                state_len,
                self.max_state_data_bytes,
            ));
        }
        let cp_dir = self.checkpoint_dir(id);
        let path = self.state_path(id);
        let chunks = chunks.to_vec();
        tokio::task::spawn_blocking(move || publish_filesystem_state(&cp_dir, &path, &chunks))
            .await
            .map_err(std::io::Error::other)?
    }

    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let path = self.state_path(id);
        let limit = self.max_state_data_bytes;
        tokio::task::spawn_blocking(move || {
            read_bounded_regular_file(&path, limit, "checkpoint state sidecar")
        })
        .await
        .map_err(std::io::Error::other)?
    }

    async fn state_data_len_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<u64>, CheckpointStoreError> {
        if participant_id != self.participant_id {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint store participant {} cannot read participant {participant_id}",
                self.participant_id
            )));
        }
        let path = self.state_path(id);
        let limit = self.max_state_data_bytes;
        tokio::task::spawn_blocking(move || {
            Ok(
                open_bounded_regular_file(&path, limit, "checkpoint state sidecar")?
                    .map(|(_, len)| len),
            )
        })
        .await
        .map_err(std::io::Error::other)?
    }
}

// ---------------------------------------------------------------------------
// ObjectStoreCheckpointStore — adapter for any ObjectStore backend
// ---------------------------------------------------------------------------

/// JSON pointer stored in `manifests/latest.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct LatestPointer {
    checkpoint_id: u64,
}

struct VersionedBytes {
    bytes: bytes::Bytes,
    version: UpdateVersion,
}

struct VersionedManifest {
    manifest: CheckpointManifest,
    version: UpdateVersion,
}

async fn collect_bounded_state_data(
    result: object_store::GetResult,
    expected_size: u64,
    expected_len: usize,
    limit: u64,
) -> Result<Vec<u8>, CheckpointStoreError> {
    use futures::TryStreamExt;

    if result.meta.size > limit {
        return Err(oversized_metadata(
            "checkpoint state sidecar",
            result.meta.size,
            limit,
        ));
    }
    if result.meta.size != expected_size {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint state sidecar length changed from {expected_size} to {} bytes after metadata preflight",
            result.meta.size
        )));
    }
    if result.range.start != 0 || result.range.end != expected_size {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint state sidecar response range {}..{} does not match its advertised {expected_size}-byte length",
            result.range.start, result.range.end
        )));
    }

    let mut bytes = Vec::new();
    bytes.try_reserve_exact(expected_len).map_err(|error| {
        CheckpointStoreError::Invalid(format!(
            "checkpoint state sidecar cannot reserve its advertised {expected_size}-byte length: {error}"
        ))
    })?;
    let mut stream = result.into_stream();
    while let Some(chunk) = stream.try_next().await? {
        let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
            CheckpointStoreError::Invalid("checkpoint state sidecar body length overflow".into())
        })?;
        let next_len_u64 = u64::try_from(next_len).unwrap_or(u64::MAX);
        if next_len_u64 > limit {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                next_len_u64,
                limit,
            ));
        }
        if next_len > expected_len {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint state sidecar body exceeded its advertised {expected_size}-byte length"
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != expected_len {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint state sidecar body length {} does not match its advertised {expected_size}-byte length",
            bytes.len()
        )));
    }
    Ok(bytes)
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
/// Manifest creation, finalization, and recovery-pointer publication are all
/// conditional writes. The only mutable manifest transition is the exact
/// `Prepared` to `Finalized` phase change.
pub struct ObjectStoreCheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    key_group_count: KeyGroupCount,
    participant_id: u64,
    max_state_data_bytes: u64,
}

impl ObjectStoreCheckpointStore {
    /// Create a new object-store-backed checkpoint store.
    ///
    /// `prefix` is prepended to all object paths (e.g., `"nodes/abc123/"`).
    /// It should end with `/` or be empty.
    ///
    /// Embedded and single-node stores default to one key group. Cluster hosts
    /// must chain [`Self::with_key_group_count`] with their durable topology.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            key_group_count: LOCAL_KEY_GROUP_COUNT,
            participant_id: 0,
            max_state_data_bytes: DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
        }
    }

    /// Override the stable key-group count used during manifest validation.
    #[must_use]
    pub fn with_key_group_count(mut self, key_group_count: KeyGroupCount) -> Self {
        self.key_group_count = key_group_count;
        self
    }

    /// Bind this store to one runtime participant.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
        self
    }

    /// Override the aggregate raw logical-state and physical-sidecar bytes admitted per checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointStoreError::Invalid`] when the limit cannot safely
    /// bound an in-memory restore allocation and its one-byte overflow probe.
    pub fn with_max_state_data_bytes(
        mut self,
        max_state_data_bytes: u64,
    ) -> Result<Self, CheckpointStoreError> {
        validate_max_checkpoint_state_bytes(max_state_data_bytes)?;
        self.max_state_data_bytes = max_state_data_bytes;
        Ok(self)
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

    fn state_path_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<object_store::path::Path, CheckpointStoreError> {
        let prefix = self.prefix_for_participant(participant_id)?;
        Ok(object_store::path::Path::from(format!(
            "{prefix}checkpoints/state-{id:06}.bin"
        )))
    }

    /// Read the recovery pointer without loading its manifest. Retention must
    /// preserve this exact ID even if newer prepared attempts sort after it.
    async fn latest_checkpoint_id(&self) -> Result<Option<u64>, CheckpointStoreError> {
        Ok(self
            .load_latest()
            .await?
            .map(|manifest| manifest.checkpoint_id))
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
                Err(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                ) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "object store does not support conditional create required for immutable checkpoint object '{path}'"
                    )));
                }
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
        }
    }

    /// Load a sidecar only after a metadata preflight has proved its allocation bound.
    async fn get_bounded_state_data(
        &self,
        path: &object_store::path::Path,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let limit = self.max_state_data_bytes;
        let preflight = match self.store.head(path).await {
            Ok(metadata) => metadata,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
        };
        if preflight.size > limit {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                preflight.size,
                limit,
            ));
        }

        let expected_len = usize::try_from(preflight.size).map_err(|_| {
            CheckpointStoreError::Invalid(format!(
                "checkpoint state sidecar size {} exceeds this process address space",
                preflight.size
            ))
        })?;
        let conditional_version = preflight.version.clone();
        let conditional_etag = conditional_version
            .is_none()
            .then(|| preflight.e_tag.clone())
            .flatten();

        // An empty immutable object needs no body allocation. A conditional second HEAD still
        // proves that the exact preflight version remains empty without issuing an invalid 0..0
        // range request on backends that reject ranges against empty objects.
        if expected_len == 0 {
            let result = self
                .store
                .get_opts(
                    path,
                    GetOptions {
                        if_match: conditional_etag,
                        version: conditional_version,
                        head: true,
                        ..GetOptions::default()
                    },
                )
                .await?;
            if result.meta.size != 0 {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint state sidecar length changed from 0 to {} bytes after metadata preflight",
                    result.meta.size
                )));
            }
            return Ok(Some(Vec::new()));
        }

        let request_end = preflight.size.checked_add(1).ok_or_else(|| {
            CheckpointStoreError::Invalid(
                "checkpoint state sidecar safety limit cannot be range-bounded".into(),
            )
        })?;
        let result = self
            .store
            .get_opts(
                path,
                GetOptions {
                    if_match: conditional_etag,
                    range: Some(GetRange::Bounded(0..request_end)),
                    version: conditional_version,
                    ..GetOptions::default()
                },
            )
            .await?;
        collect_bounded_state_data(result, preflight.size, expected_len, limit)
            .await
            .map(Some)
    }

    async fn get_bounded_versioned(
        &self,
        path: &object_store::path::Path,
        limit: u64,
        kind: &str,
    ) -> Result<Option<VersionedBytes>, CheckpointStoreError> {
        let request_end = limit.checked_add(1).ok_or_else(|| {
            CheckpointStoreError::Invalid(format!("{kind} limit cannot be range-bounded"))
        })?;
        match self
            .store
            .get_opts(
                path,
                GetOptions {
                    range: Some(GetRange::Bounded(0..request_end)),
                    ..GetOptions::default()
                },
            )
            .await
        {
            Ok(result) => {
                use futures::TryStreamExt;

                let size = result.meta.size;
                if size > limit {
                    return Err(oversized_metadata(kind, size, limit));
                }
                let range_size = result
                    .range
                    .end
                    .checked_sub(result.range.start)
                    .unwrap_or(u64::MAX);
                if range_size > limit {
                    return Err(oversized_metadata(kind, range_size, limit));
                }
                if result.range.start != 0 || result.range.end != size {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "{kind} response range {}..{} does not match object metadata size {size}",
                        result.range.start, result.range.end
                    )));
                }
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let capacity = usize::try_from(size).map_err(|_| {
                    CheckpointStoreError::Invalid(format!(
                        "{kind} size {size} exceeds this process address space"
                    ))
                })?;
                let mut bytes = BytesMut::with_capacity(capacity);
                let mut stream = result.into_stream();
                while let Some(chunk) = stream.try_next().await? {
                    let next_len = bytes.len().checked_add(chunk.len()).ok_or_else(|| {
                        CheckpointStoreError::Invalid(format!("{kind} body length overflow"))
                    })?;
                    if next_len > capacity {
                        return Err(CheckpointStoreError::Invalid(format!(
                            "{kind} body exceeded its advertised {size}-byte length"
                        )));
                    }
                    bytes.extend_from_slice(&chunk);
                }
                if bytes.len() != capacity {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "{kind} body length {} does not match object metadata size {size}",
                        bytes.len()
                    )));
                }
                Ok(Some(VersionedBytes {
                    bytes: bytes.freeze(),
                    version,
                }))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(CheckpointStoreError::ObjectStore(error)),
        }
    }

    fn require_update_version(
        path: &object_store::path::Path,
        version: UpdateVersion,
    ) -> Result<UpdateVersion, CheckpointStoreError> {
        if version.e_tag.is_none() && version.version.is_none() {
            Err(CheckpointStoreError::Invalid(format!(
                "object store returned no ETag or version required for conditional update of '{path}'"
            )))
        } else {
            Ok(version)
        }
    }

    async fn load_manifest_versioned_at(
        &self,
        path: &object_store::path::Path,
        checkpoint_id: u64,
        participant_id: u64,
    ) -> Result<Option<VersionedManifest>, CheckpointStoreError> {
        let Some(versioned) = self
            .get_bounded_versioned(path, MAX_MANIFEST_BYTES, "checkpoint manifest")
            .await?
        else {
            return Ok(None);
        };
        let manifest: CheckpointManifest = serde_json::from_slice(&versioned.bytes)?;
        ensure_loaded_manifest(
            &manifest,
            checkpoint_id,
            participant_id,
            self.key_group_count,
        )?;
        Ok(Some(VersionedManifest {
            manifest,
            version: versioned.version,
        }))
    }

    async fn load_manifest_at(
        &self,
        path: &object_store::path::Path,
        checkpoint_id: u64,
        participant_id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        Ok(self
            .load_manifest_versioned_at(path, checkpoint_id, participant_id)
            .await?
            .map(|versioned| versioned.manifest))
    }

    async fn load_latest_pointer_versioned(
        &self,
    ) -> Result<Option<(LatestPointer, UpdateVersion)>, CheckpointStoreError> {
        let Some(versioned) = self
            .get_bounded_versioned(
                &self.latest_pointer_path(),
                MAX_LATEST_POINTER_BYTES,
                "checkpoint recovery pointer",
            )
            .await?
        else {
            return Ok(None);
        };
        let pointer: LatestPointer = serde_json::from_slice(&versioned.bytes)?;
        Ok(Some((pointer, versioned.version)))
    }

    async fn load_finalized_manifest(
        &self,
        checkpoint_id: u64,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        let manifest = self
            .load_manifest_at(
                &self.manifest_path(checkpoint_id),
                checkpoint_id,
                self.participant_id,
            )
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "checkpoint recovery pointer references missing checkpoint {checkpoint_id}"
                ))
            })?;
        if manifest.durable_phase != DurableCheckpointPhase::Finalized {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint recovery pointer references non-finalized checkpoint {checkpoint_id}"
            )));
        }
        Ok(manifest)
    }

    async fn publish_latest(&self, checkpoint_id: u64) -> Result<(), CheckpointStoreError> {
        let path = self.latest_pointer_path();
        let encoded = bytes::Bytes::from(serde_json::to_vec(&LatestPointer { checkpoint_id })?);
        ensure_serialized_size(
            "checkpoint recovery pointer",
            encoded.len(),
            MAX_LATEST_POINTER_BYTES,
        )?;

        let mut last_storage_error = None;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let observed = self.load_latest_pointer_versioned().await?;
            let mode = match observed {
                Some((pointer, version)) => {
                    self.load_finalized_manifest(pointer.checkpoint_id).await?;
                    if pointer.checkpoint_id >= checkpoint_id {
                        return Ok(());
                    }
                    PutMode::Update(Self::require_update_version(&path, version)?)
                }
                None => PutMode::Create,
            };
            let options = PutOptions {
                mode,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(&path, PutPayload::from_bytes(encoded.clone()), options)
                .await
            {
                Ok(_) => return Ok(()),
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => {
                    // This also reconciles a successful write whose response was lost.
                    checkpoint_cas_backoff(attempt).await;
                }
                Err(error @ object_store::Error::Generic { .. }) => {
                    last_storage_error = Some(error.to_string());
                    checkpoint_cas_backoff(attempt).await;
                }
                Err(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                ) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "object store does not support conditional latest-pointer update for '{path}'"
                    )));
                }
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
        }

        if let Some((pointer, _)) = self.load_latest_pointer_versioned().await? {
            self.load_finalized_manifest(pointer.checkpoint_id).await?;
            if pointer.checkpoint_id >= checkpoint_id {
                return Ok(());
            }
        }
        let detail = last_storage_error
            .map(|error| format!("; last storage error: {error}"))
            .unwrap_or_default();
        Err(CheckpointStoreError::Invalid(format!(
            "latest-pointer update for checkpoint {checkpoint_id} exceeded the bounded contention budget{detail}"
        )))
    }

    async fn replace_manifest_exactly(
        &self,
        path: &object_store::path::Path,
        intended: &CheckpointManifest,
        encoded: bytes::Bytes,
    ) -> Result<(), CheckpointStoreError> {
        let mut last_storage_error = None;
        for attempt in 0..MAX_CAS_ATTEMPTS {
            let observed = self
                .load_manifest_versioned_at(path, intended.checkpoint_id, intended.participant_id)
                .await?
                .ok_or_else(|| {
                    CheckpointStoreError::Invalid(format!(
                        "checkpoint {} manifest disappeared during finalization",
                        intended.checkpoint_id
                    ))
                })?;
            if observed.manifest == *intended || exact_phase_successor(intended, &observed.manifest)
            {
                return Ok(());
            }
            if !exact_phase_successor(&observed.manifest, intended) {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint {} manifest already exists with different immutable content",
                    intended.checkpoint_id
                )));
            }
            let options = PutOptions {
                mode: PutMode::Update(Self::require_update_version(path, observed.version)?),
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(path, PutPayload::from_bytes(encoded.clone()), options)
                .await
            {
                Ok(_) => return Ok(()),
                Err(object_store::Error::Precondition { .. }) => {
                    // Re-read to distinguish contention from a successful write with a lost ACK.
                    checkpoint_cas_backoff(attempt).await;
                }
                Err(error @ object_store::Error::Generic { .. }) => {
                    last_storage_error = Some(error.to_string());
                    checkpoint_cas_backoff(attempt).await;
                }
                Err(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                ) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "object store does not support conditional manifest finalization for '{path}'"
                    )));
                }
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
        }

        let observed = self
            .load_manifest_at(path, intended.checkpoint_id, intended.participant_id)
            .await?;
        if observed.as_ref().is_some_and(|manifest| {
            manifest == intended || exact_phase_successor(intended, manifest)
        }) {
            return Ok(());
        }
        let detail = last_storage_error
            .map(|error| format!("; last storage error: {error}"))
            .unwrap_or_default();
        Err(CheckpointStoreError::Invalid(format!(
            "checkpoint {} manifest finalization exceeded the bounded contention budget{detail}",
            intended.checkpoint_id
        )))
    }

    async fn publish_manifest(
        &self,
        manifest: &CheckpointManifest,
        encoded: bytes::Bytes,
    ) -> Result<(), CheckpointStoreError> {
        ensure_serialized_size("checkpoint manifest", encoded.len(), MAX_MANIFEST_BYTES)?;
        let path = self.manifest_path(manifest.checkpoint_id);
        if self
            .create_with_retry(&path, PutPayload::from_bytes(encoded.clone()))
            .await?
        {
            return Ok(());
        }

        let existing = self
            .load_manifest_at(&path, manifest.checkpoint_id, manifest.participant_id)
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "checkpoint {} manifest create reported AlreadyExists but the object is missing",
                    manifest.checkpoint_id
                ))
            })?;
        if existing == *manifest || exact_phase_successor(manifest, &existing) {
            return Ok(());
        }
        if exact_phase_successor(&existing, manifest) {
            return self
                .replace_manifest_exactly(&path, manifest, encoded)
                .await;
        }
        Err(CheckpointStoreError::Invalid(format!(
            "checkpoint {} manifest already exists with different immutable content",
            manifest.checkpoint_id
        )))
    }

    async fn state_matches_chunks(
        &self,
        path: &object_store::path::Path,
        chunks: &[bytes::Bytes],
    ) -> Result<Option<bool>, CheckpointStoreError> {
        use futures::TryStreamExt;

        let expected_len = checked_state_data_len(chunks)?;
        if expected_len > self.max_state_data_bytes {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                expected_len,
                self.max_state_data_bytes,
            ));
        }
        let options = if expected_len == 0 {
            GetOptions {
                head: true,
                ..GetOptions::default()
            }
        } else {
            GetOptions {
                range: Some(GetRange::Bounded(
                    0..expected_len.checked_add(1).ok_or_else(|| {
                        CheckpointStoreError::Invalid(
                            "checkpoint state sidecar length cannot be range-bounded".into(),
                        )
                    })?,
                )),
                ..GetOptions::default()
            }
        };
        let result = match self.store.get_opts(path, options).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
        };
        if result.meta.size > self.max_state_data_bytes {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                result.meta.size,
                self.max_state_data_bytes,
            ));
        }
        if result.meta.size != expected_len {
            return Ok(Some(false));
        }
        if expected_len == 0 {
            return Ok(Some(true));
        }
        if result.range.start != 0 || result.range.end != expected_len {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint state sidecar response range {}..{} does not match its advertised {expected_len}-byte length",
                result.range.start, result.range.end
            )));
        }

        let mut actual = Sha256::new();
        let mut actual_len = 0_u64;
        let mut stream = result.into_stream();
        while let Some(chunk) = stream.try_next().await? {
            actual_len = actual_len.checked_add(chunk.len() as u64).ok_or_else(|| {
                CheckpointStoreError::Invalid(
                    "checkpoint state sidecar body length overflow".into(),
                )
            })?;
            if actual_len > self.max_state_data_bytes {
                return Err(oversized_metadata(
                    "checkpoint state sidecar",
                    actual_len,
                    self.max_state_data_bytes,
                ));
            }
            if actual_len > expected_len {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint state sidecar body exceeded its advertised {expected_len}-byte length"
                )));
            }
            actual.update(&chunk);
        }
        if actual_len != expected_len {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint state sidecar body length {actual_len} does not match its advertised {expected_len}-byte length"
            )));
        }
        let expected = sha256_hex_chunks(chunks);
        Ok(Some(format!("{:x}", actual.finalize()) == expected))
    }

    /// List checkpoint IDs by scanning `manifests/manifest-NNNNNN.json`.
    async fn list_checkpoint_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.list_checkpoint_ids_with_limit(MAX_CHECKPOINT_INVENTORY_ENTRIES)
            .await
    }

    async fn list_checkpoint_ids_with_limit(
        &self,
        max_entries: usize,
    ) -> Result<Vec<u64>, CheckpointStoreError> {
        use futures::TryStreamExt;

        let mut ids = std::collections::BTreeSet::new();

        let manifest_namespace = format!("{}manifests/", self.prefix);
        let manifests_prefix = object_store::path::Path::from(manifest_namespace.clone());
        let mut entries = self.store.list(Some(&manifests_prefix));
        let mut scanned = 0usize;
        while let Some(entry) = entries.try_next().await? {
            if scanned >= max_entries {
                return Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint inventory exceeds the {max_entries}-entry safety limit"
                )));
            }
            scanned += 1;
            let path = entry.location.as_ref();
            if let Some(id) = parse_checkpoint_id_from_path(path, "manifest-", ".json") {
                if entry.location != self.manifest_path(id) {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "non-canonical checkpoint manifest path '{path}'"
                    )));
                }
                ids.insert(id);
            } else if path
                .strip_prefix(&manifest_namespace)
                .is_some_and(|name| name.starts_with("manifest-"))
            {
                return Err(CheckpointStoreError::Invalid(format!(
                    "non-canonical checkpoint manifest path '{path}'"
                )));
            }
        }

        Ok(ids.into_iter().collect())
    }
}

#[async_trait]
impl CheckpointStore for ObjectStoreCheckpointStore {
    fn max_state_data_bytes(&self) -> u64 {
        self.max_state_data_bytes
    }

    fn key_group_count(&self) -> KeyGroupCount {
        self.key_group_count
    }

    fn participant_id(&self) -> u64 {
        self.participant_id
    }

    async fn save(&self, manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
        self.ensure_manifest_participant(manifest)?;
        self.ensure_manifest_valid(manifest)?;
        let encoded = bytes::Bytes::from(serde_json::to_vec_pretty(manifest)?);
        self.publish_manifest(manifest, encoded).await?;

        // A prepared manifest is deliberately invisible to the normal latest
        // pointer. Recovery inventory still discovers it through list_ids().
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            return Ok(());
        }

        self.publish_latest(manifest.checkpoint_id).await
    }

    async fn finalize(
        &self,
        checkpoint_id: u64,
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        let path = self.manifest_path(checkpoint_id);
        let mut manifest = self
            .load_manifest_at(&path, checkpoint_id, self.participant_id)
            .await?
            .ok_or(CheckpointStoreError::NotFound(checkpoint_id))?;
        if manifest.durable_phase == DurableCheckpointPhase::Prepared {
            manifest.durable_phase = DurableCheckpointPhase::Finalized;
            let encoded = bytes::Bytes::from(serde_json::to_vec_pretty(&manifest)?);
            ensure_serialized_size("checkpoint manifest", encoded.len(), MAX_MANIFEST_BYTES)?;
            self.replace_manifest_exactly(&path, &manifest, encoded)
                .await?;
        }
        self.publish_latest(manifest.checkpoint_id).await?;
        Ok(manifest)
    }

    async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let Some((pointer, _)) = self.load_latest_pointer_versioned().await? else {
            return Ok(None);
        };
        Ok(Some(
            self.load_finalized_manifest(pointer.checkpoint_id).await?,
        ))
    }

    async fn load_by_id(
        &self,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        self.load_manifest_at(&self.manifest_path(id), id, self.participant_id)
            .await
    }

    async fn load_manifest_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let path = self.manifest_path_for_participant(participant_id, id)?;
        self.load_manifest_at(&path, id, participant_id).await
    }

    async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
        self.list_checkpoint_ids().await
    }

    async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
        let ids = self.list_checkpoint_ids().await?;
        let mut result = Vec::with_capacity(ids.len());

        for id in ids {
            let manifest = self
                .load_by_id(id)
                .await?
                .ok_or(CheckpointStoreError::NotFound(id))?;
            result.push((manifest.checkpoint_id, manifest.epoch));
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
            // The manifest is the inventory record. Delete the sidecar first so
            // a failed or ambiguously acknowledged sidecar delete remains
            // discoverable and can be completed by a later retention pass.
            if let Err(error) = self.store.delete(&state).await {
                if !matches!(error, object_store::Error::NotFound { .. }) {
                    return Err(CheckpointStoreError::ObjectStore(error));
                }
            }
            match self.store.delete(&manifest).await {
                Ok(()) => removed += 1,
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(CheckpointStoreError::ObjectStore(error)),
            }
        }
        Ok(removed)
    }

    async fn save_state_data(
        &self,
        id: u64,
        chunks: &[bytes::Bytes],
    ) -> Result<(), CheckpointStoreError> {
        let state_len = checked_state_data_len(chunks)?;
        if state_len > self.max_state_data_bytes {
            return Err(oversized_metadata(
                "checkpoint state sidecar",
                state_len,
                self.max_state_data_bytes,
            ));
        }
        let path = self.state_path(id);
        // PutPayload is a chain of Bytes — no concatenation into a
        // contiguous buffer. Each Arc bump is ~nothing; the underlying
        // bytes reach the object-store client untouched.
        let payload: PutPayload = chunks.iter().cloned().collect();
        if self.create_with_retry(&path, payload).await? {
            Ok(())
        } else {
            let matches = self
                .state_matches_chunks(&path, chunks)
                .await?
                .ok_or_else(|| {
                    CheckpointStoreError::Invalid(format!(
                    "checkpoint {id} state create reported AlreadyExists but the object is missing"
                ))
                })?;
            if matches {
                Ok(())
            } else {
                Err(CheckpointStoreError::Invalid(format!(
                    "checkpoint {id} state already exists with different immutable content"
                )))
            }
        }
    }

    async fn load_state_data(&self, id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        self.get_bounded_state_data(&self.state_path(id)).await
    }

    async fn load_state_data_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
        let path = self.state_path_for_participant(participant_id, id)?;
        self.get_bounded_state_data(&path).await
    }

    async fn state_data_len_for_participant(
        &self,
        participant_id: u64,
        id: u64,
    ) -> Result<Option<u64>, CheckpointStoreError> {
        let path = self.state_path_for_participant(participant_id, id)?;
        match self.store.head(&path).await {
            Ok(metadata) if metadata.size <= self.max_state_data_bytes => Ok(Some(metadata.size)),
            Ok(metadata) => Err(oversized_metadata(
                "checkpoint state sidecar",
                metadata.size,
                self.max_state_data_bytes,
            )),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(CheckpointStoreError::ObjectStore(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::checkpoint_manifest::{ConnectorCheckpoint, OperatorCheckpoint};
    #[allow(clippy::disallowed_types)] // cold path: checkpoint store
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    fn make_store(dir: &Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir)
    }

    fn make_manifest(id: u64) -> CheckpointManifest {
        let mut manifest = CheckpointManifest::new(id, id);
        manifest.durable_phase = DurableCheckpointPhase::Finalized;
        manifest
    }

    fn declare_external_state(manifest: &mut CheckpointManifest, length: usize) {
        manifest.operator_states.insert(
            "external".into(),
            OperatorCheckpoint::external(0, u64::try_from(length).unwrap()),
        );
    }

    fn make_external_manifest(id: u64, length: usize) -> CheckpointManifest {
        let mut manifest = make_manifest(id);
        declare_external_state(&mut manifest, length);
        manifest
    }

    #[test]
    fn checkpoint_stores_default_to_local_key_group_count() {
        let dir = tempfile::tempdir().unwrap();
        let filesystem = FileSystemCheckpointStore::new(dir.path());
        let object_store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );

        assert_eq!(filesystem.key_group_count(), LOCAL_KEY_GROUP_COUNT);
        assert_eq!(object_store.key_group_count(), LOCAL_KEY_GROUP_COUNT);
        assert_eq!(
            filesystem.max_state_data_bytes(),
            DEFAULT_MAX_CHECKPOINT_STATE_BYTES
        );
        assert_eq!(
            object_store.max_state_data_bytes(),
            DEFAULT_MAX_CHECKPOINT_STATE_BYTES
        );
    }

    #[test]
    fn checkpoint_state_budget_rejects_unusable_limits() {
        assert!(matches!(
            validate_max_checkpoint_state_bytes(0),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("must be greater than zero")
        ));
        assert!(matches!(
            validate_max_checkpoint_state_bytes(u64::MAX),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("one-byte overflow probe")
        ));
        if let Some(above_isize) = u64::try_from(isize::MAX)
            .ok()
            .and_then(|maximum| maximum.checked_add(1))
        {
            assert!(matches!(
                validate_max_checkpoint_state_bytes(above_isize),
                Err(CheckpointStoreError::Invalid(error)) if error.contains("process address space")
            ));
        }

        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            FileSystemCheckpointStore::new(dir.path()).with_max_state_data_bytes(0),
            Err(CheckpointStoreError::Invalid(_))
        ));
        assert!(matches!(
            ObjectStoreCheckpointStore::new(
                Arc::new(object_store::memory::InMemory::new()),
                String::new(),
            )
            .with_max_state_data_bytes(0),
            Err(CheckpointStoreError::Invalid(_))
        ));
    }

    #[test]
    fn checkpoint_stores_accept_explicit_key_group_count() {
        let key_group_count = KeyGroupCount::try_from(256_u16).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let filesystem =
            FileSystemCheckpointStore::new(dir.path()).with_key_group_count(key_group_count);
        let object_store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        )
        .with_key_group_count(key_group_count);

        assert_eq!(filesystem.key_group_count(), key_group_count);
        assert_eq!(object_store.key_group_count(), key_group_count);
    }

    #[tokio::test]
    async fn checkpoint_stores_reject_noncanonical_manifest_before_sidecar_write() {
        let invalid = CheckpointManifest::new(7, 8);
        let chunks = [bytes::Bytes::from_static(b"must-not-persist")];

        let dir = tempfile::tempdir().unwrap();
        let filesystem = FileSystemCheckpointStore::new(dir.path());
        let error = filesystem
            .save_with_state(&invalid, Some(&chunks))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("canonical checkpoint ID"));
        assert!(!filesystem.state_path(7).exists());
        assert!(filesystem.list_ids().await.unwrap().is_empty());

        let object_store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );
        let error = object_store
            .save_with_state(&invalid, Some(&chunks))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("canonical checkpoint ID"));
        assert!(object_store.load_state_data(7).await.unwrap().is_none());
        assert!(object_store.list_ids().await.unwrap().is_empty());
    }

    #[derive(Debug)]
    struct GetCountingStore {
        inner: Arc<dyn ObjectStore>,
        manifest_gets: AtomicUsize,
        state_gets: AtomicUsize,
        lost_ack: Option<LostAckTarget>,
        lost_ack_remaining: AtomicUsize,
        fail_manifest_delete: bool,
        state_delete_fault: Option<StateDeleteFault>,
        state_delete_fault_remaining: Arc<AtomicUsize>,
        get_fault: Option<GetFault>,
        bounded_latest_get_seen: AtomicBool,
    }

    #[derive(Debug, Clone, Copy)]
    enum LostAckTarget {
        LatestCreate,
        LatestUpdate,
        ManifestUpdate,
    }

    #[derive(Debug, Clone, Copy)]
    enum StateDeleteFault {
        HardFailure,
        LostAck,
    }

    #[derive(Debug, Clone, Copy)]
    enum GetFault {
        MisreportedRange,
        MisreportedSize,
        ShortBody,
        LongBody,
        MissingVersion,
    }

    impl GetCountingStore {
        fn new(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                manifest_gets: AtomicUsize::new(0),
                state_gets: AtomicUsize::new(0),
                lost_ack: None,
                lost_ack_remaining: AtomicUsize::new(0),
                fail_manifest_delete: false,
                state_delete_fault: None,
                state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
                get_fault: None,
                bounded_latest_get_seen: AtomicBool::new(false),
            }
        }

        fn with_lost_ack(inner: Arc<dyn ObjectStore>, target: LostAckTarget) -> Self {
            Self {
                inner,
                manifest_gets: AtomicUsize::new(0),
                state_gets: AtomicUsize::new(0),
                lost_ack: Some(target),
                lost_ack_remaining: AtomicUsize::new(1),
                fail_manifest_delete: false,
                state_delete_fault: None,
                state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
                get_fault: None,
                bounded_latest_get_seen: AtomicBool::new(false),
            }
        }

        fn with_manifest_delete_failure(inner: Arc<dyn ObjectStore>) -> Self {
            Self {
                inner,
                manifest_gets: AtomicUsize::new(0),
                state_gets: AtomicUsize::new(0),
                lost_ack: None,
                lost_ack_remaining: AtomicUsize::new(0),
                fail_manifest_delete: true,
                state_delete_fault: None,
                state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
                get_fault: None,
                bounded_latest_get_seen: AtomicBool::new(false),
            }
        }

        fn with_state_delete_fault(
            inner: Arc<dyn ObjectStore>,
            state_delete_fault: StateDeleteFault,
        ) -> Self {
            Self {
                inner,
                manifest_gets: AtomicUsize::new(0),
                state_gets: AtomicUsize::new(0),
                lost_ack: None,
                lost_ack_remaining: AtomicUsize::new(0),
                fail_manifest_delete: false,
                state_delete_fault: Some(state_delete_fault),
                state_delete_fault_remaining: Arc::new(AtomicUsize::new(1)),
                get_fault: None,
                bounded_latest_get_seen: AtomicBool::new(false),
            }
        }

        fn with_get_fault(inner: Arc<dyn ObjectStore>, get_fault: GetFault) -> Self {
            Self {
                inner,
                manifest_gets: AtomicUsize::new(0),
                state_gets: AtomicUsize::new(0),
                lost_ack: None,
                lost_ack_remaining: AtomicUsize::new(0),
                fail_manifest_delete: false,
                state_delete_fault: None,
                state_delete_fault_remaining: Arc::new(AtomicUsize::new(0)),
                get_fault: Some(get_fault),
                bounded_latest_get_seen: AtomicBool::new(false),
            }
        }

        fn should_lose_ack(&self, location: &object_store::path::Path, mode: &PutMode) -> bool {
            let matches = match self.lost_ack {
                Some(LostAckTarget::LatestCreate) => {
                    location.as_ref().ends_with("manifests/latest.json")
                        && matches!(mode, PutMode::Create)
                }
                Some(LostAckTarget::LatestUpdate) => {
                    location.as_ref().ends_with("manifests/latest.json")
                        && matches!(mode, PutMode::Update(_))
                }
                Some(LostAckTarget::ManifestUpdate) => {
                    location.as_ref().contains("manifests/manifest-")
                        && matches!(mode, PutMode::Update(_))
                }
                None => false,
            };
            matches
                && self
                    .lost_ack_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
        }

        fn reset(&self) {
            self.manifest_gets.store(0, Ordering::Relaxed);
            self.state_gets.store(0, Ordering::Relaxed);
        }

        fn counts(&self) -> (usize, usize) {
            (
                self.manifest_gets.load(Ordering::Relaxed),
                self.state_gets.load(Ordering::Relaxed),
            )
        }
    }

    impl std::fmt::Display for GetCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("GetCountingStore")
        }
    }

    #[async_trait]
    impl ObjectStore for GetCountingStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let lose_ack = self.should_lose_ack(location, &options.mode);
            let result = self.inner.put_opts(location, payload, options).await?;
            if lose_ack {
                return Err(object_store::Error::Generic {
                    store: "lost-ack-test",
                    source: Box::new(std::io::Error::other(
                        "injected response loss after successful conditional write",
                    )),
                });
            }
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let path = location.as_ref();
            if path.contains("manifests/manifest-") {
                self.manifest_gets.fetch_add(1, Ordering::Relaxed);
            } else if path.contains("checkpoints/state-") {
                self.state_gets.fetch_add(1, Ordering::Relaxed);
            }
            let is_latest = path.ends_with("manifests/latest.json");
            let is_state = path.contains("checkpoints/state-");
            if is_latest
                && matches!(
                    options.range.as_ref(),
                    Some(GetRange::Bounded(range))
                        if range.start == 0 && range.end == MAX_LATEST_POINTER_BYTES + 1
                )
            {
                self.bounded_latest_get_seen.store(true, Ordering::Relaxed);
            }
            let is_head = options.head;
            let result = self.inner.get_opts(location, options).await?;
            let Some(fault) = self
                .get_fault
                .filter(|_| is_latest || (is_state && !is_head))
            else {
                return Ok(result);
            };

            use futures::StreamExt;
            let mut meta = result.meta.clone();
            let attributes = result.attributes.clone();
            let bytes = result.bytes().await?;
            let size = bytes.len() as u64;
            let (range, body) = match fault {
                GetFault::MisreportedRange => (1..size + 1, bytes),
                GetFault::MisreportedSize => {
                    meta.size = size.saturating_add(1);
                    (0..size, bytes)
                }
                GetFault::ShortBody => (0..size, bytes.slice(..bytes.len().saturating_sub(1))),
                GetFault::LongBody => {
                    let mut body = BytesMut::from(bytes.as_ref());
                    body.extend_from_slice(b"x");
                    (0..size, body.freeze())
                }
                GetFault::MissingVersion => {
                    meta.e_tag = None;
                    meta.version = None;
                    (0..size, bytes)
                }
            };
            Ok(object_store::GetResult {
                payload: object_store::GetResultPayload::Stream(
                    futures::stream::once(async move { Ok(body) }).boxed(),
                ),
                meta,
                range,
                attributes,
            })
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            use futures::StreamExt;

            let fail_manifest_delete = self.fail_manifest_delete;
            let state_delete_fault = self.state_delete_fault;
            let state_delete_fault_remaining = Arc::clone(&self.state_delete_fault_remaining);
            let inner = Arc::clone(&self.inner);
            locations
                .then(move |result| {
                    let inner = Arc::clone(&inner);
                    let state_delete_fault_remaining = Arc::clone(&state_delete_fault_remaining);
                    async move {
                        let location = result?;
                        if fail_manifest_delete && location.as_ref().contains("manifests/manifest-")
                        {
                            return Err(object_store::Error::Generic {
                                store: "manifest-delete-test",
                                source: Box::new(std::io::Error::other(
                                    "injected manifest deletion failure",
                                )),
                            });
                        }

                        let inject_state_fault = state_delete_fault.is_some()
                            && location.as_ref().contains("checkpoints/state-")
                            && state_delete_fault_remaining
                                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                                    remaining.checked_sub(1)
                                })
                                .is_ok();
                        if inject_state_fault
                            && matches!(state_delete_fault, Some(StateDeleteFault::HardFailure))
                        {
                            return Err(object_store::Error::Generic {
                                store: "state-delete-test",
                                source: Box::new(std::io::Error::other(
                                    "injected state sidecar deletion failure",
                                )),
                            });
                        }

                        inner.delete(&location).await?;
                        if inject_state_fault
                            && matches!(state_delete_fault, Some(StateDeleteFault::LostAck))
                        {
                            return Err(object_store::Error::Generic {
                                store: "state-delete-test",
                                source: Box::new(std::io::Error::other(
                                    "injected response loss after state sidecar deletion",
                                )),
                            });
                        }
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[tokio::test]
    async fn test_save_and_load_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1);
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
        std::fs::remove_file(dir.path().join("checkpoints/latest.txt")).unwrap();
        assert_eq!(store.finalize(1).await.unwrap(), finalized);
        assert_eq!(store.load_latest().await.unwrap(), Some(finalized));
    }

    #[tokio::test]
    async fn only_the_exact_prepared_to_finalized_manifest_transition_is_accepted() {
        let prepared = CheckpointManifest::new(7, 7);
        let mut changed = make_manifest(7);
        changed.watermark = Some(42);

        let dir = tempfile::tempdir().unwrap();
        let filesystem = FileSystemCheckpointStore::new(dir.path());
        filesystem.save(&prepared).await.unwrap();
        assert!(filesystem.save(&changed).await.is_err());
        assert_eq!(filesystem.load_by_id(7).await.unwrap().unwrap(), prepared);
        filesystem.finalize(7).await.unwrap();

        let object_store = make_obj_store();
        object_store.save(&prepared).await.unwrap();
        assert!(object_store.save(&changed).await.is_err());
        assert_eq!(object_store.load_by_id(7).await.unwrap().unwrap(), prepared);
        object_store.finalize(7).await.unwrap();
    }

    #[tokio::test]
    async fn validated_recovery_skips_newer_prepared_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&make_manifest(1)).await.unwrap();
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
            store.save(&make_manifest(i)).await.unwrap();
        }

        let latest = store.load_latest().await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 5);
        assert_eq!(latest.epoch, 5);
    }

    #[tokio::test]
    async fn test_load_by_id() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let m = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.epoch, 1);

        let m = store.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m.epoch, 2);

        assert!(store.load_by_id(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(3)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 1), (2, 2), (3, 3)]);
    }

    #[tokio::test]
    async fn filesystem_inventory_bounds_all_entries_and_rejects_aliases() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&make_manifest(1)).await.unwrap();

        let existing_entries = std::fs::read_dir(store.checkpoints_dir()).unwrap().count();
        assert_eq!(
            store
                .sorted_checkpoint_ids_with_limit(existing_entries)
                .await
                .unwrap(),
            vec![1]
        );
        std::fs::write(store.checkpoints_dir().join("unrelated"), b"noise").unwrap();
        let error = store
            .sorted_checkpoint_ids_with_limit(existing_entries)
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains(&format!("{existing_entries}-entry safety limit")));

        let alias_dir = tempfile::tempdir().unwrap();
        let alias_store = FileSystemCheckpointStore::new(alias_dir.path());
        let alias = alias_store.checkpoints_dir().join("checkpoint_01");
        std::fs::create_dir_all(&alias).unwrap();
        std::fs::write(alias.join("manifest.json"), b"{}").unwrap();
        let error = alias_store
            .sorted_checkpoint_ids_with_limit(10)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-canonical"));
    }

    #[tokio::test]
    async fn filesystem_inventory_rejects_a_non_regular_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        let manifest_path = store.manifest_path(1);
        std::fs::create_dir_all(&manifest_path).unwrap();

        let error = store.list_ids().await.unwrap_err();
        assert!(error.to_string().contains("is not a regular file"));
    }

    #[tokio::test]
    async fn test_save_does_not_run_retention_inline() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        for i in 1..=5 {
            store.save(&make_manifest(i)).await.unwrap();
        }

        let list = store.list().await.unwrap();
        assert_eq!(list.len(), 5);
    }

    #[tokio::test]
    async fn epoch_prune_preserves_latest_recovery_cut() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        store.save(&make_manifest(1)).await.unwrap();
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

        store.save(&make_manifest(1)).await.unwrap();

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
    async fn filesystem_state_budget_is_exact_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path())
            .with_max_state_data_bytes(8)
            .unwrap();
        let exact = [
            bytes::Bytes::from_static(b"1234"),
            bytes::Bytes::from_static(b"5678"),
        ];

        store.save_state_data(1, &exact).await.unwrap();
        assert_eq!(
            store.load_state_data(1).await.unwrap().unwrap(),
            b"12345678"
        );
        assert_eq!(
            store.state_data_len_for_participant(0, 1).await.unwrap(),
            Some(8)
        );
        assert!(matches!(
            store
                .save_state_data(2, &[bytes::Bytes::from_static(b"123456789")])
                .await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));
        assert!(!store.state_path(2).exists());

        let lowered = FileSystemCheckpointStore::new(dir.path())
            .with_max_state_data_bytes(7)
            .unwrap();
        assert!(matches!(
            lowered.load_state_data(1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
        ));
        assert!(matches!(
            lowered.state_data_len_for_participant(0, 1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
        ));
    }

    #[tokio::test]
    async fn filesystem_state_read_rejects_non_regular_and_oversized_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path()).with_max_state_data_bytes(8).unwrap();

        let non_regular = store.state_path(1);
        assert!(matches!(
            store
                .save_state_data(1, &[bytes::Bytes::from_static(b"123456789")])
                .await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));
        assert!(!non_regular.exists());

        std::fs::create_dir_all(&non_regular).unwrap();
        assert!(matches!(
            store.load_state_data(1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
        ));
        assert!(matches!(
            store.state_data_len_for_participant(0, 1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
        ));

        std::fs::remove_dir(&non_regular).unwrap();
        std::fs::write(&non_regular, b"123456789").unwrap();
        assert!(matches!(
            store.load_state_data(1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));
        assert!(matches!(
            store.state_data_len_for_participant(0, 1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn filesystem_state_read_rejects_a_symlink_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let state_path = store.state_path(1);
        std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        let target = dir.path().join("outside-state.bin");
        std::fs::write(&target, b"outside").unwrap();
        std::os::unix::fs::symlink(&target, &state_path).unwrap();

        assert!(matches!(
            store.load_state_data(1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
        ));
        assert!(matches!(
            store.state_data_len_for_participant(0, 1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("not a regular file")
        ));
    }

    #[test]
    fn bounded_state_reader_rejects_body_growth_and_truncation() {
        use std::io::{Seek, SeekFrom};

        let dir = tempfile::tempdir().unwrap();
        let growing = dir.path().join("growing.bin");
        std::fs::write(&growing, b"1234").unwrap();
        let mut growing_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&growing)
            .unwrap();
        growing_file.seek(SeekFrom::End(0)).unwrap();
        growing_file.write_all(b"56789012").unwrap();
        growing_file.seek(SeekFrom::Start(0)).unwrap();
        assert!(matches!(
            read_bounded_open_file(&mut growing_file, 4, 8, "test sidecar"),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));

        let exact_limit = dir.path().join("exact-limit.bin");
        std::fs::write(&exact_limit, b"12345678").unwrap();
        let mut exact_limit_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&exact_limit)
            .unwrap();
        exact_limit_file.seek(SeekFrom::End(0)).unwrap();
        exact_limit_file.write_all(b"9").unwrap();
        exact_limit_file.seek(SeekFrom::Start(0)).unwrap();
        assert!(matches!(
            read_bounded_open_file(&mut exact_limit_file, 8, 8, "test sidecar"),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("is 9 bytes, exceeding the 8-byte safety limit")
        ));

        let truncating = dir.path().join("truncating.bin");
        std::fs::write(&truncating, b"12345678").unwrap();
        let mut truncating_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&truncating)
            .unwrap();
        truncating_file.set_len(3).unwrap();
        truncating_file.seek(SeekFrom::Start(0)).unwrap();
        assert!(matches!(
            read_bounded_open_file(&mut truncating_file, 8, 8, "test sidecar"),
            Err(CheckpointStoreError::Invalid(error)) if error.contains("body length changed from 8 to 3 bytes")
        ));
    }

    #[tokio::test]
    async fn participant_state_read_delegates_to_local_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path()).with_participant_id(11);
        let mut manifest = make_manifest(7);
        manifest.participant_id = 11;
        declare_external_state(&mut manifest, b"local-state".len());
        store
            .save_with_state(
                &manifest,
                Some(&[bytes::Bytes::from_static(b"local-state")]),
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .load_state_data_for_participant(11, 7)
                .await
                .unwrap()
                .unwrap(),
            b"local-state"
        );
        let artifacts = store
            .load_checkpoint_artifacts_for_participant(11, 7)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(7, 11, store.key_group_count(), store.max_state_data_bytes())
            .await
            .unwrap();
        assert!(validation.valid);
    }

    #[tokio::test]
    async fn filesystem_rejects_foreign_participant_state_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path()).with_participant_id(11);

        let error = store
            .load_state_data_for_participant(22, 7)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid checkpoint: checkpoint store participant 11 cannot read participant 22"
        );

        let error = store
            .load_checkpoint_artifacts_for_participant(22, 7)
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "invalid checkpoint: checkpoint store participant 11 cannot read participant 22"
        );
    }

    #[tokio::test]
    async fn test_full_manifest_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let mut m = make_manifest(1);
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

        store.save_with_state(&m, None).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
        assert_eq!(loaded.watermark, Some(999_000));

        let src = loaded.source_offsets.get("kafka-src").unwrap();
        assert_eq!(src.offsets.get("events:0"), Some(&"1000".into()));

        let tbl = loaded.table_offsets.get("instruments").unwrap();
        assert_eq!(tbl.offsets.get("lsn"), Some(&"0/AB".into()));

        let op = loaded.operator_states.get("window").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"data");
    }

    #[tokio::test]
    async fn test_empty_latest_txt_is_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "").unwrap();

        let error = store.load_latest().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("invalid checkpoint recovery pointer"));
    }

    #[tokio::test]
    async fn test_latest_points_to_missing_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let cp_dir = dir.path().join("checkpoints");
        std::fs::create_dir_all(&cp_dir).unwrap();
        std::fs::write(cp_dir.join("latest.txt"), "checkpoint_000099").unwrap();

        let error = store.load_latest().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("recovery pointer references missing checkpoint 99"));
    }

    #[tokio::test]
    async fn test_save_with_state_writes_sidecar_before_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let state = b"large-operator-state-blob";
        let m = make_external_manifest(1, state.len());
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
    async fn filesystem_save_establishes_nested_publication_directories() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("missing").join("checkpoint-root");
        let store = make_store(&base);
        let state = b"durable-state";

        store
            .save_with_state(
                &make_external_manifest(1, state.len()),
                Some(&[bytes::Bytes::from_static(state)]),
            )
            .await
            .unwrap();

        let checkpoint = base.join("checkpoints").join("checkpoint_000001");
        assert_eq!(std::fs::read(checkpoint.join("state.bin")).unwrap(), state);
        assert!(checkpoint.join("manifest.json").is_file());
        assert_eq!(
            std::fs::read_to_string(base.join("checkpoints").join("latest.txt")).unwrap(),
            "checkpoint_000001"
        );
        assert!(!checkpoint.join("state.bin.tmp").exists());
        assert!(!checkpoint.join("manifest.json.tmp").exists());
        assert!(!base.join("checkpoints").join("latest.txt.tmp").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_filesystem_save_cannot_regress_a_later_pointer_update() {
        let dir = tempfile::tempdir().unwrap();
        let gate = Arc::new(FilesystemLatestPublicationGate::default());
        let delayed = FileSystemCheckpointStore::new(dir.path())
            .with_latest_publication_gate(Arc::clone(&gate));
        let delayed_task = tokio::spawn(async move { delayed.save(&make_manifest(5)).await });

        assert!(gate.wait_until_entered(std::time::Duration::from_secs(5)));
        delayed_task.abort();
        let _ = delayed_task.await;

        let base = dir.path().to_path_buf();
        let later = tokio::spawn(async move {
            FileSystemCheckpointStore::new(base)
                .save(&make_manifest(10))
                .await
        });
        gate.release();
        later.await.unwrap().unwrap();

        let loaded = FileSystemCheckpointStore::new(dir.path())
            .load_latest()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.checkpoint_id, 10);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_filesystem_state_publication_has_one_immutable_winner() {
        let dir = tempfile::tempdir().unwrap();
        let base_a = dir.path().to_path_buf();
        let base_b = base_a.clone();
        let first = tokio::spawn(async move {
            FileSystemCheckpointStore::new(base_a)
                .save_with_state(
                    &make_external_manifest(7, b"first".len()),
                    Some(&[bytes::Bytes::from_static(b"first")]),
                )
                .await
        });
        let second = tokio::spawn(async move {
            FileSystemCheckpointStore::new(base_b)
                .save_with_state(
                    &make_external_manifest(7, b"second".len()),
                    Some(&[bytes::Bytes::from_static(b"second")]),
                )
                .await
        });
        let (first, second) = tokio::join!(first, second);
        let results = [first.unwrap(), second.unwrap()];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);

        let store = FileSystemCheckpointStore::new(dir.path());
        let latest = store.load_latest().await.unwrap().unwrap();
        let state = store.load_state_data(7).await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 7);
        assert!(state == b"first" || state == b"second");
        assert!(store.validate_checkpoint(7).await.unwrap().valid);
    }

    #[tokio::test]
    async fn filesystem_public_loads_reject_oversize_and_misrouted_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());
        let checkpoint_dir = store.checkpoint_dir(7);
        std::fs::create_dir_all(&checkpoint_dir).unwrap();

        let manifest_path = store.manifest_path(7);
        let wrong = make_manifest(8);
        std::fs::write(&manifest_path, serde_json::to_vec(&wrong).unwrap()).unwrap();
        let error = store.load_by_id(7).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("storage checkpoint 7 contains manifest checkpoint 8"));
        assert!(store.list().await.is_err());

        let oversized = File::create(&manifest_path).unwrap();
        oversized.set_len(MAX_MANIFEST_BYTES + 1).unwrap();
        let error = store.load_by_id(7).await.unwrap_err();
        assert!(error.to_string().contains("exceeding the"));

        std::fs::create_dir_all(store.checkpoints_dir()).unwrap();
        let pointer = File::create(store.latest_path()).unwrap();
        pointer.set_len(MAX_LATEST_POINTER_BYTES + 1).unwrap();
        let error = store.load_latest().await.unwrap_err();
        assert!(error.to_string().contains("exceeding the"));
    }

    #[tokio::test]
    async fn test_save_with_state_none_is_same_as_save() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1);
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
        let m = make_manifest(1);
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

        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let m = store.load_by_id(1).await.unwrap().unwrap();
        assert_eq!(m.epoch, 1);
        let m = store.load_by_id(2).await.unwrap().unwrap();
        assert_eq!(m.epoch, 2);
        assert!(store.load_by_id(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn object_store_manifest_is_immutable_and_idempotent() {
        let store = make_obj_store();
        let manifest = make_manifest(7);

        store.save(&manifest).await.unwrap();
        store.save(&manifest).await.unwrap();

        let mut conflicting = manifest.clone();
        conflicting.watermark = Some(6);
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

        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(3)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let list = store.list().await.unwrap();
        assert_eq!(list, vec![(1, 1), (2, 2), (3, 3)]);
    }

    #[tokio::test]
    async fn object_inventory_bounds_all_entries_and_rejects_aliases() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
        store.save(&make_manifest(1)).await.unwrap();

        assert_eq!(
            store.list_checkpoint_ids_with_limit(2).await.unwrap(),
            vec![1]
        );
        inner
            .put_opts(
                &object_store::path::Path::from("manifests/unrelated"),
                PutPayload::from_bytes(bytes::Bytes::from_static(b"noise")),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let error = store.list_checkpoint_ids_with_limit(2).await.unwrap_err();
        assert!(error.to_string().contains("2-entry safety limit"));

        let alias_inner = Arc::new(object_store::memory::InMemory::new());
        let alias_store = ObjectStoreCheckpointStore::new(alias_inner.clone(), String::new());
        alias_inner
            .put_opts(
                &object_store::path::Path::from("manifests/manifest-01.json"),
                PutPayload::from_bytes(bytes::Bytes::from_static(b"{}")),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let error = alias_store
            .list_checkpoint_ids_with_limit(10)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("non-canonical"));
    }

    #[tokio::test]
    async fn test_obj_save_does_not_run_retention_inline() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            String::new(),
        );

        for i in 1..=5 {
            store.save(&make_manifest(i)).await.unwrap();
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
        store.save(&make_manifest(1)).await.unwrap();
        for id in 2..=5 {
            store.save(&CheckpointManifest::new(id, id)).await.unwrap();
        }

        assert_eq!(store.prune_before(10).await.unwrap(), 4);
        assert_eq!(store.list_ids().await.unwrap(), vec![1]);
        assert_eq!(store.load_latest().await.unwrap().unwrap().checkpoint_id, 1);
    }

    #[tokio::test]
    async fn object_prune_keeps_inventory_when_manifest_delete_fails() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let faulting = Arc::new(GetCountingStore::with_manifest_delete_failure(
            inner.clone(),
        ));
        let store = ObjectStoreCheckpointStore::new(faulting, String::new());

        let state = [bytes::Bytes::from_static(b"state-must-survive")];
        store
            .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
            .await
            .unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let error = store.prune_before(2).await.unwrap_err();
        assert!(error.to_string().contains("manifest deletion failure"));
        assert!(store.load_by_id(1).await.unwrap().is_some());
        assert!(store.load_state_data(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn object_prune_retries_state_delete_failure_from_manifest_inventory() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let faulting = Arc::new(GetCountingStore::with_state_delete_fault(
            inner,
            StateDeleteFault::HardFailure,
        ));
        let store = ObjectStoreCheckpointStore::new(faulting, String::new());

        let state = [bytes::Bytes::from_static(b"state-must-survive")];
        store
            .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
            .await
            .unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let error = store.prune_before(2).await.unwrap_err();
        assert!(error.to_string().contains("state sidecar deletion failure"));
        assert!(store.load_by_id(1).await.unwrap().is_some());
        assert_eq!(
            store.load_state_data(1).await.unwrap().as_deref(),
            Some(b"state-must-survive".as_slice())
        );

        assert_eq!(store.prune_before(2).await.unwrap(), 1);
        assert!(store.load_by_id(1).await.unwrap().is_none());
        assert!(store.load_state_data(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn object_prune_retries_lost_state_delete_ack_without_orphaning_state() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let faulting = Arc::new(GetCountingStore::with_state_delete_fault(
            inner,
            StateDeleteFault::LostAck,
        ));
        let store = ObjectStoreCheckpointStore::new(faulting, String::new());

        let state = [bytes::Bytes::from_static(b"state-deleted-before-ack-loss")];
        store
            .save_with_state(&make_external_manifest(1, state[0].len()), Some(&state))
            .await
            .unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let error = store.prune_before(2).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("response loss after state sidecar deletion"));
        assert!(store.load_by_id(1).await.unwrap().is_some());
        assert!(store.load_state_data(1).await.unwrap().is_none());

        assert_eq!(store.prune_before(2).await.unwrap(), 1);
        assert!(store.load_by_id(1).await.unwrap().is_none());
        assert!(store.load_state_data(1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_obj_save_and_load_state_data() {
        let store = make_obj_store();
        store.save(&make_manifest(1)).await.unwrap();

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
    async fn object_state_budget_is_exact_and_survives_restart() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new())
            .with_max_state_data_bytes(8)
            .unwrap();
        let exact = [
            bytes::Bytes::from_static(b"1234"),
            bytes::Bytes::from_static(b"5678"),
        ];

        store.save_state_data(1, &exact).await.unwrap();
        assert_eq!(
            store.load_state_data(1).await.unwrap().unwrap(),
            b"12345678"
        );
        assert_eq!(
            store.state_data_len_for_participant(0, 1).await.unwrap(),
            Some(8)
        );
        assert!(matches!(
            store
                .save_state_data(2, &[bytes::Bytes::from_static(b"123456789")])
                .await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 8-byte safety limit")
        ));
        assert!(inner.head(&store.state_path(2)).await.is_err());

        let lowered = ObjectStoreCheckpointStore::new(inner, String::new())
            .with_max_state_data_bytes(7)
            .unwrap();
        assert!(matches!(
            lowered.load_state_data(1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
        ));
        assert!(matches!(
            lowered.state_data_len_for_participant(0, 1).await,
            Err(CheckpointStoreError::Invalid(error)) if error.contains("exceeding the 7-byte safety limit")
        ));
    }

    #[tokio::test]
    async fn bounded_object_state_reader_rejects_metadata_and_body_faults() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let path = object_store::path::Path::from("checkpoints/state-000001.bin");
        inner
            .put_opts(
                &path,
                PutPayload::from_bytes(bytes::Bytes::from_static(b"12345678")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        for (fault, limit, expected) in [
            (GetFault::MisreportedSize, 16, "length changed"),
            (GetFault::ShortBody, 16, "body length"),
            (GetFault::LongBody, 16, "body exceeded"),
            (GetFault::LongBody, 8, "exceeding the 8-byte safety limit"),
        ] {
            let faulting = Arc::new(GetCountingStore::with_get_fault(inner.clone(), fault));
            let store = ObjectStoreCheckpointStore::new(faulting, String::new())
                .with_max_state_data_bytes(limit)
                .unwrap();
            let error = store.load_state_data(1).await.unwrap_err();
            assert!(
                error.to_string().contains(expected),
                "fault {fault:?}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn test_obj_with_prefix() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner, "nodes/abc123/".to_string());

        store.save(&make_manifest(1)).await.unwrap();
        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[tokio::test]
    async fn test_obj_participant_namespaces_are_isolated() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let participant_11 =
            ObjectStoreCheckpointStore::new(inner.clone(), "participants/11/".to_string())
                .with_participant_id(11);
        let participant_22 = ObjectStoreCheckpointStore::new(inner, "participants/22/".to_string())
            .with_participant_id(22);

        let mut manifest_11 = make_manifest(7);
        manifest_11.participant_id = 11;
        let mut manifest_22 = make_manifest(7);
        manifest_22.participant_id = 22;

        participant_11.save(&manifest_11).await.unwrap();
        participant_22.save(&manifest_22).await.unwrap();

        let loaded_11 = participant_11.load_by_id(7).await.unwrap().unwrap();
        let loaded_22 = participant_22.load_by_id(7).await.unwrap().unwrap();
        assert_eq!((loaded_11.participant_id, loaded_11.epoch), (11, 7));
        assert_eq!((loaded_22.participant_id, loaded_22.epoch), (22, 7));
        assert_eq!(participant_11.list().await.unwrap(), vec![(7, 7)]);
        assert_eq!(participant_22.list().await.unwrap(), vec![(7, 7)]);
    }

    #[tokio::test]
    async fn object_store_reads_and_validates_peer_participant_state() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let participant_11 =
            ObjectStoreCheckpointStore::new(inner.clone(), "nodes/11/".to_string())
                .with_participant_id(11);
        let participant_22 =
            ObjectStoreCheckpointStore::new(inner.clone(), "nodes/22/".to_string())
                .with_participant_id(22);
        let mut manifest = make_manifest(7);
        manifest.participant_id = 22;
        declare_external_state(&mut manifest, b"peer-state".len());
        participant_22
            .save_with_state(&manifest, Some(&[bytes::Bytes::from_static(b"peer-state")]))
            .await
            .unwrap();

        assert_eq!(
            participant_11
                .load_state_data_for_participant(22, 7)
                .await
                .unwrap()
                .unwrap(),
            b"peer-state"
        );
        assert!(participant_11
            .load_state_data_for_participant(22, 8)
            .await
            .unwrap()
            .is_none());
        let artifacts = participant_11
            .load_checkpoint_artifacts_for_participant(22, 7)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(
                7,
                22,
                participant_11.key_group_count(),
                participant_11.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(validation.valid);

        inner
            .put_opts(
                &participant_22.state_path(7),
                PutPayload::from_bytes(bytes::Bytes::from_static(b"other-data")),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let artifacts = participant_11
            .load_checkpoint_artifacts_for_participant(22, 7)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(
                7,
                22,
                participant_11.key_group_count(),
                participant_11.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(!validation.valid);
        assert!(validation
            .issues
            .iter()
            .any(|issue| issue.message().contains("state.bin checksum mismatch")));
    }

    #[tokio::test]
    async fn external_artifact_load_reads_manifest_and_sidecar_once_and_rejects_tamper() {
        let raw = Arc::new(object_store::memory::InMemory::new());
        let raw_store: Arc<dyn ObjectStore> = raw.clone();
        let counting = Arc::new(GetCountingStore::new(raw_store));
        let counted_store: Arc<dyn ObjectStore> = counting.clone();
        let reader =
            ObjectStoreCheckpointStore::new(Arc::clone(&counted_store), "nodes/11/".to_string())
                .with_participant_id(11);
        let writer = ObjectStoreCheckpointStore::new(counted_store, "nodes/22/".to_string())
            .with_participant_id(22);
        let state = b"peer-external-state";
        let mut manifest = make_manifest(7);
        manifest.participant_id = 22;
        manifest.operator_states.insert(
            "external".into(),
            OperatorCheckpoint::external(0, state.len() as u64),
        );
        writer
            .save_with_state(&manifest, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        counting.reset();
        let artifacts = reader
            .load_checkpoint_artifacts_for_participant(22, 7)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(artifacts.state_data.as_deref(), Some(state.as_slice()));
        let (_, validation) = artifacts
            .validate(
                7,
                22,
                reader.key_group_count(),
                reader.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(validation.valid);
        assert_eq!(counting.counts(), (1, 2));

        raw.put_opts(
            &writer.state_path(7),
            PutPayload::from_bytes(bytes::Bytes::from_static(b"evil-external-state")),
            PutOptions::default(),
        )
        .await
        .unwrap();
        counting.reset();
        let artifacts = reader
            .load_checkpoint_artifacts_for_participant(22, 7)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(
                7,
                22,
                reader.key_group_count(),
                reader.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(!validation.valid);
        assert!(validation
            .issues
            .iter()
            .any(|issue| issue.message().contains("state.bin checksum mismatch")));
        assert_eq!(counting.counts(), (1, 2));
    }

    #[tokio::test]
    async fn inline_validation_reads_manifest_once_and_never_reads_sidecar() {
        let raw = Arc::new(object_store::memory::InMemory::new());
        let raw_store: Arc<dyn ObjectStore> = raw.clone();
        let counting = Arc::new(GetCountingStore::new(raw_store));
        let counted_store: Arc<dyn ObjectStore> = counting.clone();
        let reader =
            ObjectStoreCheckpointStore::new(Arc::clone(&counted_store), "nodes/11/".to_string())
                .with_participant_id(11);
        let writer = ObjectStoreCheckpointStore::new(counted_store, "nodes/22/".to_string())
            .with_participant_id(22);
        let mut manifest = make_manifest(8);
        manifest.participant_id = 22;
        manifest
            .operator_states
            .insert("inline".into(), OperatorCheckpoint::inline(b"inline-state"));
        let mut persisted = writer.save_with_state(&manifest, None).await.unwrap();

        counting.reset();
        let artifacts = reader
            .load_checkpoint_artifacts_for_participant(22, 8)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(
                8,
                22,
                reader.key_group_count(),
                reader.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(validation.valid, "inline artifact: {:?}", validation.issues);
        assert_eq!(counting.counts(), (1, 0));

        persisted.operator_states.insert(
            "inline".into(),
            OperatorCheckpoint::inline(b"tampered-inline-state"),
        );
        raw.put_opts(
            &writer.manifest_path(8),
            PutPayload::from_bytes(bytes::Bytes::from(
                serde_json::to_vec_pretty(&persisted).unwrap(),
            )),
            PutOptions::default(),
        )
        .await
        .unwrap();
        counting.reset();
        let artifacts = reader
            .load_checkpoint_artifacts_for_participant(22, 8)
            .await
            .unwrap()
            .unwrap();
        let (_, validation) = artifacts
            .validate(
                8,
                22,
                reader.key_group_count(),
                reader.max_state_data_bytes(),
            )
            .await
            .unwrap();
        assert!(!validation.valid);
        assert!(validation
            .issues
            .iter()
            .any(|issue| issue.message().contains("inline state checksum mismatch")));
        assert_eq!(counting.counts(), (1, 0));
    }

    #[tokio::test]
    async fn test_obj_rejects_manifest_for_wrong_participant() {
        let store = ObjectStoreCheckpointStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            "participants/11/".to_string(),
        )
        .with_participant_id(11);
        let mut manifest = make_manifest(7);
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

        store.save(&make_manifest(1)).await.unwrap();

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

        let m = make_manifest(1);
        store.save(&m).await.unwrap();

        // Second save with same ID should succeed (logs warning, skips write)
        store.save(&m).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(loaded.checkpoint_id, 1);
        assert_eq!(loaded.epoch, 1);
    }

    #[tokio::test]
    async fn object_store_finalize_only_publishes_the_stored_prepared_manifest() {
        let store = make_obj_store();
        let prepared = CheckpointManifest::new(7, 7);
        store.save_with_state(&prepared, None).await.unwrap();
        assert!(store.load_latest().await.unwrap().is_none());

        let finalized = store.finalize(7).await.unwrap();

        assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
        assert_eq!(store.load_latest().await.unwrap(), Some(finalized.clone()));
        store
            .store
            .delete(&store.latest_pointer_path())
            .await
            .unwrap();
        assert!(store.load_latest().await.unwrap().is_none());
        assert_eq!(store.finalize(7).await.unwrap(), finalized);
        assert_eq!(store.load_latest().await.unwrap(), Some(finalized));
    }

    #[tokio::test]
    async fn test_obj_state_paths() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

        store.save(&make_manifest(1)).await.unwrap();
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

        store.save(&make_manifest(5)).await.unwrap();

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
    async fn test_obj_latest_pointing_to_missing_checkpoint_is_invalid() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
        let pointer = serde_json::to_vec(&LatestPointer { checkpoint_id: 99 }).unwrap();
        inner
            .put_opts(
                &object_store::path::Path::from("manifests/latest.json"),
                PutPayload::from_bytes(bytes::Bytes::from(pointer)),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let error = store.load_latest().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("recovery pointer references missing checkpoint 99"));
    }

    #[tokio::test]
    async fn test_obj_latest_pointing_to_prepared_checkpoint_is_invalid() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());
        store.save(&CheckpointManifest::new(7, 7)).await.unwrap();
        let pointer = serde_json::to_vec(&LatestPointer { checkpoint_id: 7 }).unwrap();
        inner
            .put_opts(
                &object_store::path::Path::from("manifests/latest.json"),
                PutPayload::from_bytes(bytes::Bytes::from(pointer)),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let error = store.load_latest().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("recovery pointer references non-finalized checkpoint 7"));
    }

    #[tokio::test]
    async fn test_obj_latest_monotonic_guard_skips_regression() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

        store.save(&make_manifest(10)).await.unwrap();
        // A delayed writer (e.g., paused ex-leader) tries to write id=5
        // after the current leader already advanced to id=10. The pointer
        // must not regress.
        store.save(&make_manifest(5)).await.unwrap();

        let loaded = store.load_latest().await.unwrap().unwrap();
        assert_eq!(
            loaded.checkpoint_id, 10,
            "latest pointer should not regress to an older id"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_object_store_publication_keeps_the_highest_checkpoint() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let mut tasks = Vec::new();
        for checkpoint_id in 1..=32 {
            let inner = Arc::clone(&inner);
            tasks.push(tokio::spawn(async move {
                ObjectStoreCheckpointStore::new(inner, String::new())
                    .save(&make_manifest(checkpoint_id))
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        let store = ObjectStoreCheckpointStore::new(inner, String::new());
        assert_eq!(
            store.load_latest().await.unwrap().unwrap().checkpoint_id,
            32
        );
    }

    #[tokio::test]
    async fn object_store_reconciles_lost_ack_for_finalize_and_latest_cas() {
        let create_raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let create_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
            Arc::clone(&create_raw),
            LostAckTarget::LatestCreate,
        ));
        let create_store = ObjectStoreCheckpointStore::new(create_fault, String::new());
        create_store.save(&make_manifest(1)).await.unwrap();
        assert_eq!(
            create_store
                .load_latest()
                .await
                .unwrap()
                .unwrap()
                .checkpoint_id,
            1
        );

        let raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let finalize_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
            Arc::clone(&raw),
            LostAckTarget::ManifestUpdate,
        ));
        let finalize_store = ObjectStoreCheckpointStore::new(finalize_fault, String::new());
        finalize_store
            .save(&CheckpointManifest::new(1, 1))
            .await
            .unwrap();
        let finalized = finalize_store.finalize(1).await.unwrap();
        assert_eq!(finalized.durable_phase, DurableCheckpointPhase::Finalized);
        assert_eq!(
            finalize_store
                .load_latest()
                .await
                .unwrap()
                .unwrap()
                .checkpoint_id,
            1
        );

        let latest_fault: Arc<dyn ObjectStore> = Arc::new(GetCountingStore::with_lost_ack(
            Arc::clone(&raw),
            LostAckTarget::LatestUpdate,
        ));
        let latest_store = ObjectStoreCheckpointStore::new(latest_fault, String::new());
        latest_store.save(&make_manifest(2)).await.unwrap();
        assert_eq!(
            latest_store
                .load_latest()
                .await
                .unwrap()
                .unwrap()
                .checkpoint_id,
            2
        );
    }

    #[tokio::test]
    async fn object_store_public_loads_reject_oversize_and_misrouted_metadata() {
        let inner = Arc::new(object_store::memory::InMemory::new());
        let store = ObjectStoreCheckpointStore::new(inner.clone(), String::new());

        inner
            .put_opts(
                &store.manifest_path(7),
                PutPayload::from_bytes(bytes::Bytes::from(
                    serde_json::to_vec(&make_manifest(8)).unwrap(),
                )),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let error = store.load_by_id(7).await.unwrap_err();
        assert!(error
            .to_string()
            .contains("storage checkpoint 7 contains manifest checkpoint 8"));
        assert!(store.list().await.is_err());

        inner
            .put_opts(
                &store.latest_pointer_path(),
                PutPayload::from_bytes(bytes::Bytes::from(vec![
                    b'x';
                    (MAX_LATEST_POINTER_BYTES + 1)
                        as usize
                ])),
                PutOptions::default(),
            )
            .await
            .unwrap();
        let error = store.load_latest().await.unwrap_err();
        assert!(error.to_string().contains("exceeding the"));
    }

    #[tokio::test]
    async fn object_metadata_reads_bound_requests_and_reject_malformed_responses() {
        let raw: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let raw_store = ObjectStoreCheckpointStore::new(Arc::clone(&raw), String::new());
        raw_store.save(&make_manifest(1)).await.unwrap();

        for (fault, expected) in [
            (GetFault::MisreportedRange, "response range"),
            (GetFault::ShortBody, "body length"),
            (GetFault::LongBody, "body exceeded"),
        ] {
            let faulting = Arc::new(GetCountingStore::with_get_fault(Arc::clone(&raw), fault));
            let store = ObjectStoreCheckpointStore::new(faulting.clone(), String::new());
            let error = store.load_latest().await.unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert!(
                faulting.bounded_latest_get_seen.load(Ordering::Relaxed),
                "latest metadata was not requested with the configured hard range"
            );
        }

        let versionless = Arc::new(GetCountingStore::with_get_fault(
            Arc::clone(&raw),
            GetFault::MissingVersion,
        ));
        let store = ObjectStoreCheckpointStore::new(versionless, String::new());
        let error = store.save(&make_manifest(2)).await.unwrap_err();
        assert!(error.to_string().contains("no ETag or version"), "{error}");
        assert_eq!(
            raw_store
                .load_latest()
                .await
                .unwrap()
                .unwrap()
                .checkpoint_id,
            1
        );
    }

    #[tokio::test]
    async fn test_validate_checkpoint_valid() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        let m = make_manifest(1);
        store.save(&m).await.unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(result.valid, "valid checkpoint: {:?}", result.issues);
        assert!(result.issues.is_empty());
    }

    #[tokio::test]
    async fn loaded_artifact_validation_binds_manifest_to_storage_checkpoint_id() {
        let artifacts = CheckpointArtifacts {
            manifest: make_manifest(8),
            state_data: None,
        };
        let participant_id = artifacts.manifest.participant_id;
        let key_group_count = KeyGroupCount::try_from(artifacts.manifest.vnode_count).unwrap();

        let (_, validation) = artifacts
            .validate(
                7,
                participant_id,
                key_group_count,
                DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
            )
            .await
            .unwrap();

        assert!(!validation.valid);
        assert!(validation.issues.iter().any(|issue| {
            issue
                .message()
                .contains("storage checkpoint 7 contains manifest checkpoint 8")
        }));
    }

    #[tokio::test]
    async fn loaded_artifact_validation_preserves_mixed_checksum_rule() {
        let state = bytes::Bytes::from_static(b"external-state");
        let mut manifest = make_manifest(9);
        manifest
            .operator_states
            .insert("inline".into(), OperatorCheckpoint::inline(b"inline-state"));
        manifest.operator_states.insert(
            "external".into(),
            OperatorCheckpoint::external(0, state.len() as u64),
        );
        manifest.state_checksum = Some(stamp_checksum(
            &manifest.operator_states,
            Some(std::slice::from_ref(&state)),
        ));
        let artifacts = CheckpointArtifacts {
            manifest,
            state_data: Some(state.to_vec()),
        };
        let key_group_count = KeyGroupCount::try_from(artifacts.manifest.vnode_count).unwrap();

        let (mut artifacts, validation) = artifacts
            .validate(9, 0, key_group_count, DEFAULT_MAX_CHECKPOINT_STATE_BYTES)
            .await
            .unwrap();
        assert!(validation.valid);
        artifacts.state_data = Some(b"tampered-state".to_vec());
        let (_, validation) = artifacts
            .validate(9, 0, key_group_count, DEFAULT_MAX_CHECKPOINT_STATE_BYTES)
            .await
            .unwrap();
        assert!(!validation.valid);
        assert!(validation
            .issues
            .iter()
            .any(|issue| issue.message().contains("mixed state checksum mismatch")));
    }

    #[tokio::test]
    async fn artifact_validation_enforces_aggregate_raw_logical_state_budget() {
        let state = bytes::Bytes::from_static(b"5678");
        let mut manifest = make_manifest(10);
        manifest
            .operator_states
            .insert("inline".into(), OperatorCheckpoint::inline(b"1234"));
        manifest.operator_states.insert(
            "external".into(),
            OperatorCheckpoint::external(0, state.len() as u64),
        );
        manifest.state_checksum = Some(stamp_checksum(
            &manifest.operator_states,
            Some(std::slice::from_ref(&state)),
        ));
        let key_group_count = KeyGroupCount::try_from(manifest.vnode_count).unwrap();
        let artifacts = CheckpointArtifacts {
            manifest,
            state_data: Some(state.to_vec()),
        };

        let (artifacts, exact) = artifacts.validate(10, 0, key_group_count, 8).await.unwrap();
        assert!(exact.valid, "exact aggregate budget: {:?}", exact.issues);
        assert_eq!(artifacts.state_data.as_deref(), Some(state.as_ref()));

        let (_, over) = artifacts.validate(10, 0, key_group_count, 7).await.unwrap();
        assert!(!over.valid);
        assert!(over.issues.iter().any(|issue| issue
            .message()
            .contains("aggregate logical operator state is 8 bytes")));
    }

    #[tokio::test]
    async fn artifact_validation_rejects_malformed_operator_shapes_and_base64() {
        let mut malformed_inline = make_manifest(11);
        malformed_inline.operator_states.insert(
            "bad-inline".into(),
            OperatorCheckpoint {
                state_b64: Some("%%%".into()),
                external: false,
                external_offset: 0,
                external_length: 0,
            },
        );
        malformed_inline.state_checksum = Some("untrusted".into());
        let key_group_count = KeyGroupCount::try_from(malformed_inline.vnode_count).unwrap();
        let (_, validation) = CheckpointArtifacts {
            manifest: malformed_inline,
            state_data: None,
        }
        .validate(11, 0, key_group_count, 64)
        .await
        .unwrap();
        assert!(validation
            .issues
            .iter()
            .any(|issue| issue.message().contains("invalid base64")));

        let mut malformed_external = make_manifest(12);
        malformed_external.operator_states.insert(
            "bad-external".into(),
            OperatorCheckpoint {
                state_b64: Some("eA==".into()),
                external: true,
                external_offset: 1,
                external_length: 1,
            },
        );
        malformed_external.state_checksum = Some("untrusted".into());
        let key_group_count = KeyGroupCount::try_from(malformed_external.vnode_count).unwrap();
        let (_, validation) = CheckpointArtifacts {
            manifest: malformed_external,
            state_data: Some(vec![0, 1]),
        }
        .validate(12, 0, key_group_count, 64)
        .await
        .unwrap();
        assert!(validation.issues.iter().any(|issue| issue
            .message()
            .contains("external state also contains inline base64")));
        assert!(validation.issues.iter().any(|issue| issue
            .message()
            .contains("sidecar range starts at 1, expected 0")));
    }

    #[tokio::test]
    async fn save_rejects_aggregate_state_over_budget_before_writing_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path())
            .with_max_state_data_bytes(8)
            .unwrap();
        let sidecar = [bytes::Bytes::from_static(b"6789")];
        let mut manifest = make_external_manifest(13, sidecar[0].len());
        manifest
            .operator_states
            .insert("inline".into(), OperatorCheckpoint::inline(b"12345"));

        let error = store
            .save_with_state(&manifest, Some(&sidecar))
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("aggregate logical operator state is 9 bytes"));
        assert!(!store.state_path(13).exists());
        assert!(store.list_ids().await.unwrap().is_empty());
    }

    #[test]
    fn logical_state_length_overflow_is_rejected() {
        let mut manifest = make_manifest(14);
        manifest
            .operator_states
            .insert("first".into(), OperatorCheckpoint::external(0, u64::MAX));
        manifest
            .operator_states
            .insert("second".into(), OperatorCheckpoint::external(u64::MAX, 1));
        manifest.state_checksum = Some("untrusted".into());

        let issues = operator_state_validation_issues(&manifest, None, u64::MAX - 1);
        assert!(issues.iter().any(|issue| issue
            .message()
            .contains("aggregate logical operator state length overflows")));
        assert!(issues
            .iter()
            .any(|issue| issue.message().contains("sidecar range overflows")));
    }

    #[tokio::test]
    async fn test_validate_checkpoint_rejects_zero_noncanonical_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        // Inject malformed storage directly; the production save boundary rejects it.
        let mut m = CheckpointManifest::new(1, 0);
        m.durable_phase = DurableCheckpointPhase::Finalized;
        let checkpoint_dir = store.checkpoint_dir(1);
        std::fs::create_dir_all(&checkpoint_dir).unwrap();
        std::fs::write(
            store.manifest_path(1),
            serde_json::to_vec_pretty(&m).unwrap(),
        )
        .unwrap();

        let result = store.validate_checkpoint(1).await.unwrap();
        assert!(!result.valid, "epoch=0 should be invalid");
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.message().contains("canonical checkpoint ID")),
            "should identify the canonical-attempt violation: {:?}",
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
        let m = make_external_manifest(1, state.len());
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
        let m = make_external_manifest(1, state.len());
        store
            .save_with_state(&m, Some(&[bytes::Bytes::from_static(state)]))
            .await
            .unwrap();

        // Now corrupt the state.bin on disk.
        let state_path = dir.path().join("checkpoints/checkpoint_000001/state.bin");
        std::fs::write(&state_path, b"tampered state").unwrap();

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
        let m = make_external_manifest(1, b"state".len());
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
                .any(|i| i.message().contains("missing")),
            "should report missing state: {:?}",
            result.issues
        );
    }

    #[tokio::test]
    async fn test_recover_latest_validated_skips_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        // Save two checkpoints.
        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

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
        store.save(&make_manifest(1)).await.unwrap();
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
    async fn test_save_with_state_writes_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileSystemCheckpointStore::new(dir.path());

        let state = b"state-data-for-checksum";
        let m = make_external_manifest(1, state.len());
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
        store.save(&make_manifest(1)).await.unwrap();

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
        let m = make_external_manifest(1, state.len());
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

        store.save(&make_manifest(1)).await.unwrap();
        store.save(&make_manifest(2)).await.unwrap();

        let report = store.recover_latest_validated().await.unwrap();
        assert_eq!(report.chosen_id, Some(2));
        assert!(report.skipped.is_empty());
    }
}
