//! Durable checkpoint abort-seal validation and reconciliation.

use bytes::Bytes;
use object_store::ObjectStoreExt;

use super::validation::{
    checkpoint_artifact_abort_seal_bytes, ensure_manifest_valid, validate_abort_seal,
    ManifestAbortState,
};
use super::{
    checkpoint_artifact_identity_sha256, checkpoint_manifest_bytes, CheckpointArtifactAbortSeal,
    CheckpointStoreError, ObjectStoreCheckpointStore, MAX_ABORT_SEAL_BYTES, MAX_MANIFEST_BYTES,
};
use crate::checkpoint::checkpoint_manifest::{CheckpointManifest, StateChunkId};
use crate::checkpoint_decision::CheckpointArtifactInventory;

impl ObjectStoreCheckpointStore {
    pub(super) async fn load_manifest_or_abort_seal_bytes(
        &self,
        chunk: StateChunkId,
    ) -> Result<Option<Bytes>, CheckpointStoreError> {
        let path = self.manifest_path(chunk);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size > MAX_ABORT_SEAL_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest or abort seal is {} bytes, exceeding the {MAX_ABORT_SEAL_BYTES}-byte limit",
                result.meta.size
            )));
        }
        let expected_size = result.meta.size;
        let bytes = result.bytes().await?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_size {
            return Err(CheckpointStoreError::Invalid(
                "manifest or abort seal length changed while being read".into(),
            ));
        }
        Ok(Some(bytes))
    }

    fn validate_abort_seal_manifest(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
        manifest: &CheckpointManifest,
    ) -> Result<Bytes, CheckpointStoreError> {
        ensure_manifest_valid(
            manifest,
            chunk.participant_id,
            self.key_group_count,
            self.max_node_data_bytes,
        )?;
        if manifest.checkpoint_id != chunk.checkpoint_id || manifest.node_data.chunk != chunk {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest does not match aborted artifact path for participant {} checkpoint {}",
                chunk.participant_id, chunk.checkpoint_id
            )));
        }
        let inventory = CheckpointArtifactInventory {
            deployment_id: manifest.deployment_id.clone(),
            pipeline_identity: manifest.pipeline_identity.clone(),
            attempt: crate::checkpoint::CheckpointAttempt::new(
                manifest.epoch,
                manifest.checkpoint_id,
            ),
            assignment_fence: manifest.assignment_fence.clone(),
            sink_artifact_intent_protocol: !manifest.sink_artifact_intents.is_empty(),
        };
        let actual = checkpoint_artifact_identity_sha256(&inventory, chunk)?;
        if actual != expected_artifact_identity_sha256 {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest for participant {} checkpoint {} has a different artifact identity",
                chunk.participant_id, chunk.checkpoint_id
            )));
        }
        let canonical = Bytes::from(checkpoint_manifest_bytes(manifest)?);
        if u64::try_from(canonical.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        Ok(canonical)
    }

    pub(super) fn decode_manifest_abort_state(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
        bytes: &Bytes,
    ) -> Result<ManifestAbortState, CheckpointStoreError> {
        if let Ok(seal) = serde_json::from_slice::<CheckpointArtifactAbortSeal>(bytes) {
            validate_abort_seal(&seal, chunk, expected_artifact_identity_sha256)?;
            let canonical_seal = checkpoint_artifact_abort_seal_bytes(&seal)?;
            if canonical_seal != *bytes {
                return Err(CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} abort seal is not canonical",
                    chunk.participant_id, chunk.checkpoint_id
                )));
            }
            let original_manifest = seal
                .original_manifest
                .map(|manifest| {
                    let canonical = self.validate_abort_seal_manifest(
                        chunk,
                        expected_artifact_identity_sha256,
                        &manifest,
                    )?;
                    Ok::<_, CheckpointStoreError>((manifest, canonical))
                })
                .transpose()?;
            return Ok(ManifestAbortState::Sealed {
                original_manifest,
                sink_artifact_intent_protocol: seal.sink_artifact_intent_protocol,
                open_sink_artifact_intents: seal.open_sink_artifact_intents,
                sink_cleanup_complete: seal.sink_cleanup_complete,
            });
        }
        if let Some(record) =
            Self::decode_artifact_intent_record(bytes, chunk, expected_artifact_identity_sha256)?
        {
            return Ok(ManifestAbortState::Intent(record));
        }
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }
        let manifest = self.decode_manifest(chunk, bytes)?;
        let canonical =
            self.validate_abort_seal_manifest(chunk, expected_artifact_identity_sha256, &manifest)?;
        if canonical != *bytes {
            return Err(CheckpointStoreError::Invalid(format!(
                "participant {} checkpoint {} manifest is not canonical",
                chunk.participant_id, chunk.checkpoint_id
            )));
        }
        Ok(ManifestAbortState::Manifest(manifest, canonical))
    }

    pub(super) async fn node_data_is_exact_abort_seal(
        &self,
        chunk: StateChunkId,
        expected: &Bytes,
    ) -> Result<bool, CheckpointStoreError> {
        let path = self.node_data_path(chunk);
        let expected_len = u64::try_from(expected.len()).unwrap_or(u64::MAX);
        let meta = match self.store.head(&path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if meta.size != expected_len {
            return Ok(false);
        }
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size != expected_len {
            return Ok(false);
        }
        Ok(result.bytes().await? == *expected)
    }
}
