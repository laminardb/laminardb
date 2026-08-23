//! Durable binding between checkpoint cleanup authority and object identity.

use crate::checkpoint::{canonical_json_sha256, StateChunkId};
use crate::checkpoint_decision::CheckpointArtifactInventory;
use crate::state::LOCAL_NODE_ID;

use super::CheckpointStoreError;

const CHECKPOINT_ARTIFACT_IDENTITY_VERSION: u32 = 1;

#[derive(serde::Serialize)]
struct CheckpointArtifactIdentityPayload<'a> {
    version: u32,
    inventory: &'a CheckpointArtifactInventory,
    chunk: StateChunkId,
}

/// SHA-256 binding one active artifact inventory to one exact participant object namespace.
///
/// # Errors
/// Returns an error when the inventory or object identity is not canonical.
pub fn checkpoint_artifact_identity_sha256(
    inventory: &CheckpointArtifactInventory,
    chunk: StateChunkId,
) -> Result<String, CheckpointStoreError> {
    inventory.validate().map_err(|error| {
        CheckpointStoreError::Invalid(format!("checkpoint artifact inventory: {error}"))
    })?;
    if chunk.participant_id == 0 || chunk.checkpoint_id != inventory.attempt.checkpoint_id {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint artifact chunk does not match its active inventory".into(),
        ));
    }
    match inventory.assignment_fence.as_ref() {
        Some(fence) if !fence.contains(chunk.participant_id) => {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint artifact participant {} is outside its assignment fence",
                chunk.participant_id
            )));
        }
        None if chunk.participant_id != LOCAL_NODE_ID.0 => {
            return Err(CheckpointStoreError::Invalid(format!(
                "local checkpoint artifact participant must be {}",
                LOCAL_NODE_ID.0
            )));
        }
        Some(_) | None => {}
    }
    canonical_json_sha256(&CheckpointArtifactIdentityPayload {
        version: CHECKPOINT_ARTIFACT_IDENTITY_VERSION,
        inventory,
        chunk,
    })
    .map_err(CheckpointStoreError::Serde)
}
