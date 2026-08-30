//! Persisted manifest, abort-seal, digest, and node-data validation.

use bytes::Bytes;
use sha2::{Digest, Sha256};

use super::{
    CheckpointArtifactAbortSeal, CheckpointStoreError, CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION,
    CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION_V1, MAX_ABORT_SEAL_BYTES,
};
use crate::checkpoint::canonical_json_bytes;
use crate::checkpoint::checkpoint_manifest::{CheckpointManifest, StateChunkId};
use crate::state::KeyGroupCount;

pub(super) enum ManifestAbortState {
    Sealed {
        original_manifest: Option<(CheckpointManifest, Bytes)>,
        sink_cleanup_complete: bool,
    },
    Manifest(CheckpointManifest, Bytes),
}

pub(super) fn normalize_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

/// Validate the configured per-node checkpoint byte budget.
///
/// # Errors
/// Returns an error for zero or unrepresentable limits.
pub fn validate_max_checkpoint_node_data_bytes(limit: u64) -> Result<(), CheckpointStoreError> {
    if limit == 0 {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint node-data limit must be greater than zero".into(),
        ));
    }
    if limit > isize::MAX as u64 {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint node-data limit {limit} exceeds this process address space"
        )));
    }
    Ok(())
}

pub(super) fn validate_abort_seal_request(
    chunk: StateChunkId,
    expected_artifact_identity_sha256: &str,
) -> Result<(), CheckpointStoreError> {
    if chunk.participant_id == 0
        || chunk.checkpoint_id == 0
        || !is_canonical_sha256(expected_artifact_identity_sha256)
    {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint artifact abort seal identity is not canonical".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_abort_seal(
    seal: &CheckpointArtifactAbortSeal,
    expected_chunk: StateChunkId,
    expected_artifact_identity_sha256: &str,
) -> Result<(), CheckpointStoreError> {
    validate_abort_seal_request(expected_chunk, expected_artifact_identity_sha256)?;
    if !matches!(
        seal.version,
        CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION_V1 | CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION
    ) || seal.chunk != expected_chunk
        || seal.artifact_identity_sha256 != expected_artifact_identity_sha256
        || (seal.version == CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION_V1 && seal.sink_cleanup_complete)
    {
        return Err(CheckpointStoreError::Invalid(format!(
            "participant {} checkpoint {} has a different abort seal",
            expected_chunk.participant_id, expected_chunk.checkpoint_id
        )));
    }
    Ok(())
}

pub(super) fn checkpoint_artifact_abort_seal_bytes(
    seal: &CheckpointArtifactAbortSeal,
) -> Result<Bytes, CheckpointStoreError> {
    let encoded = Bytes::from(canonical_json_bytes(seal)?);
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_ABORT_SEAL_BYTES {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint artifact abort seal exceeds the {MAX_ABORT_SEAL_BYTES}-byte limit"
        )));
    }
    Ok(encoded)
}

pub(super) fn ensure_manifest_valid(
    manifest: &CheckpointManifest,
    participant_id: u64,
    key_group_count: KeyGroupCount,
    max_node_data_bytes: u64,
) -> Result<(), CheckpointStoreError> {
    validate_max_checkpoint_node_data_bytes(max_node_data_bytes)?;
    if participant_id == 0 || manifest.participant_id != participant_id {
        return Err(CheckpointStoreError::Invalid(format!(
            "manifest participant {} does not match store participant {participant_id}",
            manifest.participant_id
        )));
    }
    let errors = manifest.validate(key_group_count);
    if !errors.is_empty() {
        return Err(CheckpointStoreError::Invalid(format!(
            "manifest validation: {}",
            errors
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
    if manifest.node_data.object_length > max_node_data_bytes {
        return Err(CheckpointStoreError::Invalid(format!(
            "node data object is {} bytes, exceeding the {max_node_data_bytes}-byte limit",
            manifest.node_data.object_length
        )));
    }
    Ok(())
}

pub(super) fn validate_node_data_layout(
    manifest: &CheckpointManifest,
    chunks: &[Bytes],
    limit: u64,
) -> Result<(), CheckpointStoreError> {
    let length = checked_chunks_len(chunks)?;
    if length > limit {
        return Err(CheckpointStoreError::Invalid(format!(
            "node data object is {length} bytes, exceeding the {limit}-byte limit"
        )));
    }
    if length != manifest.node_data.object_length {
        return Err(CheckpointStoreError::Invalid(format!(
            "node data is {length} bytes; manifest declares {}",
            manifest.node_data.object_length
        )));
    }
    Ok(())
}

fn checked_chunks_len(chunks: &[Bytes]) -> Result<u64, CheckpointStoreError> {
    chunks.iter().try_fold(0_u64, |total, chunk| {
        total
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| CheckpointStoreError::Invalid("node data length overflow".into()))
    })
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn missing_node_data(chunk: StateChunkId) -> CheckpointStoreError {
    CheckpointStoreError::Invalid(format!(
        "node data object for participant {} checkpoint {} is missing",
        chunk.participant_id, chunk.checkpoint_id
    ))
}
