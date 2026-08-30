//! Durable sink artifact evidence written before an epoch can create files.

use bytes::Bytes;
use object_store::PutPayload;

use super::validation::validate_abort_seal_request;
use super::{CheckpointStoreError, ObjectStoreCheckpointStore};
use crate::checkpoint::canonical_json_bytes;
use crate::checkpoint::checkpoint_manifest::{
    checkpoint_artifact_intent_sha256, CheckpointManifest, StateChunkId,
    PREPARED_SINK_DESCRIPTOR_VERSION,
};

/// Maximum connector artifact-intent payload accepted for one sink.
pub const MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES: usize = 64 * 1024;
const MAX_CHECKPOINT_SINK_ARTIFACT_INTENTS: usize = 1_024;
/// Maximum aggregate connector intent payload retained by one participant.
pub const MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES: usize = 1024 * 1024;
pub(super) const MAX_CHECKPOINT_ARTIFACT_INTENT_RECORD_BYTES: u64 = 5 * 1024 * 1024;
const CHECKPOINT_ARTIFACT_INTENT_VERSION: u32 = 1;

/// One connector's bounded cleanup evidence persisted before `begin_epoch`.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSinkArtifactIntent {
    /// Stable registered sink name.
    pub sink_name: String,
    /// Runtime intent-envelope version.
    pub format_version: u16,
    /// Connector-specific cleanup evidence, or `None` when cleanup needs no payload.
    pub payload: Option<Vec<u8>>,
    /// Presence-domain-separated digest of `payload`.
    pub sha256: String,
}

impl std::fmt::Debug for CheckpointSinkArtifactIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointSinkArtifactIntent")
            .field("sink_name", &self.sink_name)
            .field("format_version", &self.format_version)
            .field("payload_bytes", &self.payload.as_ref().map_or(0, Vec::len))
            .field("sha256", &self.sha256)
            .finish()
    }
}

impl CheckpointSinkArtifactIntent {
    /// Construct one canonical runtime envelope.
    ///
    /// # Errors
    /// Returns an error for an empty name or an oversized payload.
    pub fn try_new(
        sink_name: String,
        payload: Option<Vec<u8>>,
    ) -> Result<Self, CheckpointStoreError> {
        if sink_name.is_empty() {
            return Err(CheckpointStoreError::Invalid(
                "sink artifact intent name must not be empty".into(),
            ));
        }
        if payload
            .as_ref()
            .is_some_and(|payload| payload.len() > MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES)
        {
            return Err(CheckpointStoreError::Invalid(format!(
                "sink artifact intent exceeds {MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES} bytes"
            )));
        }
        let sha256 = checkpoint_artifact_intent_sha256(payload.as_deref());
        Ok(Self {
            sink_name,
            format_version: PREPARED_SINK_DESCRIPTOR_VERSION,
            payload,
            sha256,
        })
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct CheckpointArtifactIntentRecord {
    version: u32,
    artifact_identity_sha256: String,
    chunk: StateChunkId,
    sink_intents: Vec<CheckpointSinkArtifactIntent>,
}

impl CheckpointArtifactIntentRecord {
    pub(super) fn sink_intents(&self) -> &[CheckpointSinkArtifactIntent] {
        &self.sink_intents
    }
}

pub(super) fn validate_artifact_intent_record(
    record: &CheckpointArtifactIntentRecord,
    expected_chunk: StateChunkId,
    expected_artifact_identity_sha256: &str,
) -> Result<(), CheckpointStoreError> {
    validate_abort_seal_request(expected_chunk, expected_artifact_identity_sha256)?;
    if record.version != CHECKPOINT_ARTIFACT_INTENT_VERSION
        || record.chunk != expected_chunk
        || record.artifact_identity_sha256 != expected_artifact_identity_sha256
    {
        return Err(CheckpointStoreError::Invalid(format!(
            "participant {} checkpoint {} has a different sink artifact intent",
            expected_chunk.participant_id, expected_chunk.checkpoint_id
        )));
    }
    validate_sink_intents(&record.sink_intents)
}

pub(super) fn validate_sink_intents(
    intents: &[CheckpointSinkArtifactIntent],
) -> Result<(), CheckpointStoreError> {
    if intents.is_empty() || intents.len() > MAX_CHECKPOINT_SINK_ARTIFACT_INTENTS {
        return Err(CheckpointStoreError::Invalid(format!(
            "sink artifact intent count must be in 1..={MAX_CHECKPOINT_SINK_ARTIFACT_INTENTS}"
        )));
    }
    if !intents
        .windows(2)
        .all(|pair| pair[0].sink_name < pair[1].sink_name)
    {
        return Err(CheckpointStoreError::Invalid(
            "sink artifact intents must be strictly ordered and unique".into(),
        ));
    }
    let mut aggregate = 0_usize;
    for intent in intents {
        if intent.sink_name.is_empty()
            || intent.format_version != PREPARED_SINK_DESCRIPTOR_VERSION
            || intent.sha256 != checkpoint_artifact_intent_sha256(intent.payload.as_deref())
        {
            return Err(CheckpointStoreError::Invalid(format!(
                "sink artifact intent '{}' has an invalid envelope",
                intent.sink_name
            )));
        }
        let bytes = intent.payload.as_ref().map_or(0, Vec::len);
        if bytes > MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "sink artifact intent '{}' exceeds {MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_BYTES} bytes",
                intent.sink_name
            )));
        }
        aggregate = aggregate
            .checked_add(bytes)
            .ok_or_else(|| CheckpointStoreError::Invalid("sink intent byte overflow".into()))?;
        if aggregate > MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "sink artifact intents exceed {MAX_CHECKPOINT_SINK_ARTIFACT_INTENT_AGGREGATE_BYTES} aggregate bytes"
            )));
        }
    }
    Ok(())
}

fn artifact_intent_bytes(
    record: &CheckpointArtifactIntentRecord,
) -> Result<Bytes, CheckpointStoreError> {
    let encoded = Bytes::from(canonical_json_bytes(record)?);
    if u64::try_from(encoded.len()).unwrap_or(u64::MAX)
        > MAX_CHECKPOINT_ARTIFACT_INTENT_RECORD_BYTES
    {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint sink artifact intent record exceeds {MAX_CHECKPOINT_ARTIFACT_INTENT_RECORD_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

impl ObjectStoreCheckpointStore {
    pub(super) fn decode_pending_artifact_intent_record(
        bytes: &Bytes,
        chunk: StateChunkId,
    ) -> Result<Option<CheckpointArtifactIntentRecord>, CheckpointStoreError> {
        let Ok(record) = serde_json::from_slice::<CheckpointArtifactIntentRecord>(bytes) else {
            return Ok(None);
        };
        validate_artifact_intent_record(&record, chunk, &record.artifact_identity_sha256)?;
        if artifact_intent_bytes(&record)? != *bytes {
            return Err(CheckpointStoreError::Invalid(format!(
                "participant {} checkpoint {} sink artifact intent is not canonical",
                chunk.participant_id, chunk.checkpoint_id
            )));
        }
        Ok(Some(record))
    }

    pub(super) fn decode_artifact_intent_record(
        bytes: &Bytes,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<Option<CheckpointArtifactIntentRecord>, CheckpointStoreError> {
        let Some(record) = Self::decode_pending_artifact_intent_record(bytes, chunk)? else {
            return Ok(None);
        };
        if record.artifact_identity_sha256 != expected_artifact_identity_sha256 {
            return Err(CheckpointStoreError::Invalid(format!(
                "participant {} checkpoint {} has a different sink artifact intent",
                chunk.participant_id, chunk.checkpoint_id
            )));
        }
        Ok(Some(record))
    }

    pub(super) async fn save_artifact_intent_record(
        &self,
        chunk: StateChunkId,
        artifact_identity_sha256: &str,
        sink_intents: Vec<CheckpointSinkArtifactIntent>,
    ) -> Result<(), CheckpointStoreError> {
        validate_abort_seal_request(chunk, artifact_identity_sha256)?;
        validate_sink_intents(&sink_intents)?;
        let record = CheckpointArtifactIntentRecord {
            version: CHECKPOINT_ARTIFACT_INTENT_VERSION,
            artifact_identity_sha256: artifact_identity_sha256.to_owned(),
            chunk,
            sink_intents,
        };
        let encoded = artifact_intent_bytes(&record)?;
        let path = self.manifest_path(chunk);
        if self
            .create_immutable(&path, PutPayload::from_bytes(encoded.clone()))
            .await?
        {
            return Ok(());
        }
        let existing = self
            .load_manifest_or_abort_seal_bytes(chunk)
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} sink intent create conflicted but no object exists",
                    chunk.participant_id, chunk.checkpoint_id
                ))
            })?;
        if existing == encoded {
            Ok(())
        } else {
            Err(CheckpointStoreError::Invalid(format!(
                "participant {} checkpoint {} already has different durable artifact state",
                chunk.participant_id, chunk.checkpoint_id
            )))
        }
    }

    pub(super) async fn promote_artifact_intent_to_manifest(
        &self,
        manifest: &CheckpointManifest,
        encoded_manifest: Bytes,
        current: &Bytes,
    ) -> Result<bool, CheckpointStoreError> {
        let chunk = manifest.node_data.chunk;
        let inventory = crate::checkpoint_decision::CheckpointArtifactInventory {
            deployment_id: manifest.deployment_id.clone(),
            pipeline_identity: manifest.pipeline_identity.clone(),
            attempt: crate::checkpoint::CheckpointAttempt::new(
                manifest.epoch,
                manifest.checkpoint_id,
            ),
            assignment_fence: manifest.assignment_fence.clone(),
            sink_artifact_intent_protocol: !manifest.sink_artifact_intents.is_empty(),
        };
        let identity = super::checkpoint_artifact_identity_sha256(&inventory, chunk)?;
        let Some(record) = Self::decode_artifact_intent_record(current, chunk, &identity)? else {
            return Ok(false);
        };
        validate_manifest_intents(manifest, record.sink_intents())?;
        self.replace_exact(&self.manifest_path(chunk), current, encoded_manifest)
            .await?;
        Ok(true)
    }
}

fn validate_manifest_intents(
    manifest: &CheckpointManifest,
    intents: &[CheckpointSinkArtifactIntent],
) -> Result<(), CheckpointStoreError> {
    if manifest.sink_artifact_intents.len() != intents.len() {
        return Err(CheckpointStoreError::Invalid(
            "checkpoint manifest does not retain its admitted sink artifact intents".into(),
        ));
    }
    for (descriptor, intent) in manifest.sink_artifact_intents.iter().zip(intents) {
        if descriptor.sink_name != intent.sink_name
            || descriptor.format_version != intent.format_version
            || descriptor.payload.is_some() != intent.payload.is_some()
            || descriptor.sha256 != intent.sha256
        {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint manifest changed sink artifact intent '{}'",
                intent.sink_name
            )));
        }
    }
    Ok(())
}
