//! Immutable view-log objects, one per participant per checkpoint, beside `node-data.bin`.

use bytes::Bytes;
use object_store::PutPayload;
use sha2::{Digest, Sha256};

use super::{CheckpointStoreError, NodeDataObject, ObjectStoreCheckpointStore, StateChunkId};

pub(super) fn unsupported_store_error() -> CheckpointStoreError {
    CheckpointStoreError::Invalid("checkpoint store does not support view-log objects".into())
}

impl ObjectStoreCheckpointStore {
    pub(super) fn view_log_path(&self, chunk: StateChunkId) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}nodes/{}/checkpoints/{:020}/view-log.bin",
            self.prefix, chunk.participant_id, chunk.checkpoint_id
        ))
    }
}

pub(super) async fn save(
    store: &ObjectStoreCheckpointStore,
    object: &NodeDataObject,
    payload: &[Bytes],
) -> Result<(), CheckpointStoreError> {
    if object.chunk.participant_id != store.participant_id {
        return Err(CheckpointStoreError::Invalid(format!(
            "view-log object belongs to participant {}, not {}",
            object.chunk.participant_id, store.participant_id
        )));
    }
    let mut digest = Sha256::new();
    let mut length = 0_u64;
    for part in payload {
        digest.update(part);
        length = length
            .checked_add(u64::try_from(part.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| CheckpointStoreError::Invalid("view-log length overflow".into()))?;
    }
    if length == 0 || length != object.object_length || length > store.max_node_data_bytes {
        return Err(CheckpointStoreError::Invalid(format!(
            "view-log payload is {length} bytes; the object declares {} within a {}-byte limit",
            object.object_length, store.max_node_data_bytes
        )));
    }
    if format!("{:x}", digest.finalize()) != object.sha256 {
        return Err(CheckpointStoreError::Invalid(
            "view-log payload does not match its declared digest".into(),
        ));
    }
    let path = store.view_log_path(object.chunk);
    let payload: PutPayload = payload.iter().cloned().collect();
    if !store.create_immutable(&path, payload).await?
        && !store.existing_object_matches(&path, object).await?
    {
        return Err(CheckpointStoreError::Invalid(format!(
            "view log for participant {} checkpoint {} already exists with different immutable content",
            object.chunk.participant_id, object.chunk.checkpoint_id
        )));
    }
    Ok(())
}
