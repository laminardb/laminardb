//! Provider-neutral checkpoint persistence over [`object_store`].

#![allow(clippy::disallowed_types)] // cold path: checkpoint metadata

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use sha2::{Digest, Sha256};

use crate::checkpoint::checkpoint_manifest::{
    checkpoint_descriptor_sha256, ByteRange, CheckpointManifest, NodeDataObject,
    PreparedSinkDescriptor, StateChunkId,
};
use crate::checkpoint::{canonical_json_bytes, OutputSegmentRef};
use crate::state::{KeyGroupCount, DEFAULT_KEY_GROUP_COUNT, LOCAL_NODE_ID};

mod abort_seal;
mod artifact_identity;
mod conditional_probe;
mod subscription_segments;
mod validation;

pub use artifact_identity::checkpoint_artifact_identity_sha256;
pub use conditional_probe::{
    probe_object_store_conditional_create, probe_object_store_conditional_update,
};
pub use subscription_segments::SubscriptionOrphanCleanup;
pub use validation::validate_max_checkpoint_node_data_bytes;
use validation::{
    checkpoint_artifact_abort_seal_bytes, ensure_manifest_valid, missing_node_data,
    normalize_prefix, sha256, validate_abort_seal_request, validate_node_data_layout,
    ManifestAbortState,
};

const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ABORT_SEAL_BYTES: u64 = MAX_MANIFEST_BYTES + 64 * 1024;
const CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION: u32 = 2;
const CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION_V1: u32 = 1;

/// Default maximum size of one participant's checkpoint data object.
pub const DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES: u64 = 512 * 1024 * 1024;

/// Checkpoint persistence error.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointStoreError {
    /// Object-store operation failed.
    #[error("object store: {0}")]
    ObjectStore(#[from] object_store::Error),
    /// Manifest serialization failed.
    #[error("manifest serialization: {0}")]
    Serde(#[from] serde_json::Error),
    /// Persisted data violates the checkpoint contract.
    #[error("invalid checkpoint: {0}")]
    Invalid(String),
    /// An exact checkpoint manifest was not found.
    #[error("checkpoint {0} not found")]
    NotFound(u64),
}

/// Exact canonical bytes persisted for a checkpoint manifest.
///
/// # Errors
/// Returns an error when the manifest cannot be represented as JSON.
pub fn checkpoint_manifest_bytes(
    manifest: &CheckpointManifest,
) -> Result<Vec<u8>, serde_json::Error> {
    canonical_json_bytes(manifest)
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct CheckpointArtifactAbortSeal {
    version: u32,
    artifact_identity_sha256: String,
    chunk: StateChunkId,
    original_manifest: Option<CheckpointManifest>,
    #[serde(default, skip_serializing_if = "is_false")]
    sink_cleanup_complete: bool,
}

const fn is_false(value: &bool) -> bool {
    !*value
}

/// Durable state returned after sealing one aborted participant manifest.
#[derive(Clone, PartialEq)]
pub struct CheckpointManifestAbortSeal {
    /// Original prepared manifest, when that participant completed persistence.
    pub original_manifest: Option<(CheckpointManifest, Bytes)>,
    /// Whether exact connector cleanup completed before node data may be sealed.
    pub sink_cleanup_complete: bool,
}

impl std::fmt::Debug for CheckpointManifestAbortSeal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointManifestAbortSeal")
            .field("has_original_manifest", &self.original_manifest.is_some())
            .field("sink_cleanup_complete", &self.sink_cleanup_complete)
            .finish()
    }
}

/// Immutable checkpoint storage contract.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Maximum bytes admitted for one node data object.
    fn max_node_data_bytes(&self) -> u64;

    /// Stable vnode count expected in manifests.
    fn key_group_count(&self) -> KeyGroupCount {
        DEFAULT_KEY_GROUP_COUNT
    }

    /// Node whose manifests this store writes.
    fn participant_id(&self) -> u64 {
        LOCAL_NODE_ID.0
    }

    /// Validate and conditional-create the immutable node data object, then its manifest. Active
    /// inventory and exact Abort seals reconcile creates left ambiguous by caller cancellation.
    async fn save_checkpoint(
        &self,
        manifest: &CheckpointManifest,
        node_data: &[Bytes],
    ) -> Result<Bytes, CheckpointStoreError>;

    /// Replace this exact aborted manifest path with a monotone seal. If a valid manifest was
    /// already durable, preserve it inside the seal and return it with its canonical bytes.
    async fn seal_aborted_manifest(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError>;

    /// Mark exact prepared-sink cleanup complete on an already sealed manifest.
    async fn complete_aborted_sink_cleanup(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError>;

    /// Replace this exact aborted node-data path with a monotone seal after sink cleanup is durable.
    async fn seal_aborted_node_data(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<(), CheckpointStoreError>;

    /// Load this node's exact manifest.
    async fn load_manifest(
        &self,
        checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        self.load_manifest_for_participant(self.participant_id(), checkpoint_id)
            .await
    }

    /// Load an exact manifest from a known participant namespace.
    async fn load_manifest_for_participant(
        &self,
        participant_id: u64,
        checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Load a manifest only when its exact persisted length and digest match a committed index.
    async fn load_manifest_verified(
        &self,
        participant_id: u64,
        checkpoint_id: u64,
        expected_len: u64,
        expected_sha256: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError>;

    /// Read exact ranges after verifying the immutable object's declared length.
    async fn load_node_data_ranges(
        &self,
        chunk: StateChunkId,
        expected_object_length: u64,
        ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, CheckpointStoreError>;

    /// Delete an explicitly identified manifest. GC must supply the identity from durable
    /// checkpoint metadata, never from object listing.
    async fn delete_manifest(&self, chunk: StateChunkId) -> Result<(), CheckpointStoreError>;

    /// Delete an explicitly identified node object after its durable reference count reaches zero.
    async fn delete_node_data(&self, chunk: StateChunkId) -> Result<(), CheckpointStoreError>;

    /// Read and verify one prepared sink descriptor.
    async fn load_prepared_sink_descriptor(
        &self,
        manifest: &CheckpointManifest,
        descriptor: &PreparedSinkDescriptor,
    ) -> Result<Option<Bytes>, CheckpointStoreError> {
        let Some(range) = descriptor.payload else {
            let expected = checkpoint_descriptor_sha256(None);
            if descriptor.sha256 != expected {
                return Err(CheckpointStoreError::Invalid(format!(
                    "prepared sink '{}' absence digest mismatch",
                    descriptor.sink_name
                )));
            }
            return Ok(None);
        };
        let mut payloads = self
            .load_node_data_ranges(
                manifest.node_data.chunk,
                manifest.node_data.object_length,
                &[range],
            )
            .await?
            .ok_or_else(|| missing_node_data(manifest.node_data.chunk))?;
        if payloads.len() != 1 {
            return Err(CheckpointStoreError::Invalid(format!(
                "one descriptor range produced {} payloads",
                payloads.len()
            )));
        }
        let bytes = payloads
            .pop()
            .expect("one descriptor payload was validated");
        let actual = checkpoint_descriptor_sha256(Some(&bytes));
        if actual != descriptor.sha256 {
            return Err(CheckpointStoreError::Invalid(format!(
                "prepared sink '{}' descriptor checksum mismatch",
                descriptor.sink_name
            )));
        }
        Ok(Some(bytes))
    }

    /// Create an immutable subscription segment, accepting an identical retry only.
    async fn save_subscription_segment(
        &self,
        _segment: &OutputSegmentRef,
        _payload: Bytes,
    ) -> Result<(), CheckpointStoreError> {
        Err(subscription_segments::unsupported_store_error())
    }

    /// Load and verify an exact subscription segment without consulting object listing.
    async fn load_subscription_segment(
        &self,
        _segment: &OutputSegmentRef,
    ) -> Result<Option<Bytes>, CheckpointStoreError> {
        Err(subscription_segments::unsupported_store_error())
    }

    /// Delete an explicitly unreachable subscription segment.
    async fn delete_subscription_segment(
        &self,
        _object_key: &str,
    ) -> Result<(), CheckpointStoreError> {
        Err(subscription_segments::unsupported_store_error())
    }

    /// Delete grace-expired segment objects not present in an authoritative reachable set.
    /// Object listing supplies candidates only; `reachable` and `through_checkpoint_id` are the
    /// caller's committed-state authority.
    async fn delete_subscription_orphans(
        &self,
        _reachable: &std::collections::BTreeSet<String>,
        _through_checkpoint_id: u64,
        _grace_before_ms: i64,
    ) -> Result<SubscriptionOrphanCleanup, CheckpointStoreError> {
        Err(subscription_segments::unsupported_store_error())
    }
}

/// Checkpoint store backed by any [`ObjectStore`] implementation.
pub struct ObjectStoreCheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    key_group_count: KeyGroupCount,
    participant_id: u64,
    max_node_data_bytes: u64,
}

impl ObjectStoreCheckpointStore {
    /// Create a store beneath a deployment-relative prefix.
    ///
    /// Node namespaces are derived as `{prefix}/nodes/{participant_id}/`; callers must not include
    /// a node id in `prefix`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: normalize_prefix(prefix),
            key_group_count: DEFAULT_KEY_GROUP_COUNT,
            participant_id: LOCAL_NODE_ID.0,
            max_node_data_bytes: DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
        }
    }

    /// Bind the store to one nonzero node id.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
        self
    }

    /// Set the exact vnode topology.
    #[must_use]
    pub fn with_key_group_count(mut self, key_group_count: KeyGroupCount) -> Self {
        self.key_group_count = key_group_count;
        self
    }

    /// Set the node-object byte limit.
    ///
    /// # Errors
    /// Returns an error for zero or unrepresentable limits.
    pub fn with_max_node_data_bytes(mut self, limit: u64) -> Result<Self, CheckpointStoreError> {
        validate_max_checkpoint_node_data_bytes(limit)?;
        self.max_node_data_bytes = limit;
        Ok(self)
    }

    fn manifest_path(&self, chunk: StateChunkId) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}nodes/{}/checkpoints/{:020}/manifest.json",
            self.prefix, chunk.participant_id, chunk.checkpoint_id
        ))
    }

    fn node_data_path(&self, chunk: StateChunkId) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}nodes/{}/checkpoints/{:020}/node-data.bin",
            self.prefix, chunk.participant_id, chunk.checkpoint_id
        ))
    }

    async fn load_bounded_manifest_bytes(
        &self,
        chunk: StateChunkId,
    ) -> Result<Option<Bytes>, CheckpointStoreError> {
        let path = self.manifest_path(chunk);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if result.meta.size > MAX_MANIFEST_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest is {} bytes, exceeding the {MAX_MANIFEST_BYTES}-byte limit",
                result.meta.size
            )));
        }
        let expected_size = result.meta.size;
        let bytes = result.bytes().await?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_size {
            return Err(CheckpointStoreError::Invalid(
                "manifest length changed while being read".into(),
            ));
        }
        Ok(Some(bytes))
    }

    fn decode_manifest(
        &self,
        chunk: StateChunkId,
        bytes: &[u8],
    ) -> Result<CheckpointManifest, CheckpointStoreError> {
        let manifest: CheckpointManifest = serde_json::from_slice(bytes)?;
        ensure_manifest_valid(
            &manifest,
            chunk.participant_id,
            self.key_group_count,
            self.max_node_data_bytes,
        )?;
        if manifest.checkpoint_id != chunk.checkpoint_id {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest path for checkpoint {} contains checkpoint {}",
                chunk.checkpoint_id, manifest.checkpoint_id
            )));
        }
        Ok(manifest)
    }

    async fn load_bounded_manifest(
        &self,
        chunk: StateChunkId,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let Some(bytes) = self.load_bounded_manifest_bytes(chunk).await? else {
            return Ok(None);
        };
        self.decode_manifest(chunk, &bytes).map(Some)
    }

    async fn overwrite(
        &self,
        path: &object_store::path::Path,
        bytes: Bytes,
    ) -> Result<(), CheckpointStoreError> {
        self.store
            .put_opts(
                path,
                PutPayload::from_bytes(bytes),
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    async fn create_immutable(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
    ) -> Result<bool, CheckpointStoreError> {
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self.store.put_opts(path, payload, options).await {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(
                object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. },
            ) => Err(CheckpointStoreError::Invalid(format!(
                "object store does not support conditional create for '{path}'"
            ))),
            Err(error) => Err(error.into()),
        }
    }

    async fn existing_node_data_matches(
        &self,
        object: &NodeDataObject,
    ) -> Result<bool, CheckpointStoreError> {
        use futures::TryStreamExt;

        let path = self.node_data_path(object.chunk);
        let meta = match self.store.head(&path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if meta.size != object.object_length || meta.size > self.max_node_data_bytes {
            return Ok(false);
        }
        let result = self.store.get(&path).await?;
        if result.meta.size != object.object_length {
            return Ok(false);
        }
        let mut length = 0_u64;
        let mut digest = Sha256::new();
        let mut stream = result.into_stream();
        while let Some(bytes) = stream.try_next().await? {
            length = length
                .checked_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
                .ok_or_else(|| CheckpointStoreError::Invalid("node data length overflow".into()))?;
            if length > object.object_length {
                return Ok(false);
            }
            digest.update(bytes);
        }
        Ok(length == object.object_length && format!("{:x}", digest.finalize()) == object.sha256)
    }

    async fn delete_exact(
        &self,
        path: &object_store::path::Path,
    ) -> Result<(), CheckpointStoreError> {
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    async fn load_node_data_ranges_inner(
        &self,
        chunk: StateChunkId,
        expected_object_length: u64,
        ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, CheckpointStoreError> {
        let path = self.node_data_path(chunk);
        let meta = match self.store.head(&path).await {
            Ok(meta) => meta,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if meta.size > self.max_node_data_bytes {
            return Err(CheckpointStoreError::Invalid(format!(
                "node data object is {} bytes, exceeding the {}-byte limit",
                meta.size, self.max_node_data_bytes
            )));
        }
        if meta.size != expected_object_length {
            return Err(CheckpointStoreError::Invalid(format!(
                "node data object is {} bytes, expected {expected_object_length}",
                meta.size
            )));
        }

        let mut nonempty = Vec::new();
        for range in ranges {
            let Some(end) = range.end() else {
                return Err(CheckpointStoreError::Invalid(
                    "node data range overflows".into(),
                ));
            };
            if end > meta.size {
                return Err(CheckpointStoreError::Invalid(format!(
                    "node data range {}..{end} exceeds {} bytes",
                    range.offset, meta.size
                )));
            }
            if range.length != 0 {
                nonempty.push(range.offset..end);
            }
        }
        let loaded = if nonempty.is_empty() {
            Vec::new()
        } else {
            object_store::coalesce_ranges(
                &nonempty,
                |range| self.store.get_range(&path, range),
                0, // non-adjacent reads must not pin unaccounted gap bytes
            )
            .await?
        };
        let mut loaded = loaded.into_iter();
        let mut result = Vec::with_capacity(ranges.len());
        for range in ranges {
            if range.length == 0 {
                result.push(Bytes::new());
            } else {
                let bytes = loaded.next().ok_or_else(|| {
                    CheckpointStoreError::Invalid(
                        "object store returned too few range payloads".into(),
                    )
                })?;
                if u64::try_from(bytes.len()).ok() != Some(range.length) {
                    return Err(CheckpointStoreError::Invalid(
                        "object store returned a range with the wrong length".into(),
                    ));
                }
                result.push(bytes);
            }
        }
        if loaded.next().is_some() {
            return Err(CheckpointStoreError::Invalid(
                "object store returned too many range payloads".into(),
            ));
        }
        Ok(Some(result))
    }
}

#[async_trait]
impl CheckpointStore for ObjectStoreCheckpointStore {
    fn max_node_data_bytes(&self) -> u64 {
        self.max_node_data_bytes
    }

    fn key_group_count(&self) -> KeyGroupCount {
        self.key_group_count
    }

    fn participant_id(&self) -> u64 {
        self.participant_id
    }

    async fn save_checkpoint(
        &self,
        manifest: &CheckpointManifest,
        node_data: &[Bytes],
    ) -> Result<Bytes, CheckpointStoreError> {
        ensure_manifest_valid(
            manifest,
            self.participant_id,
            self.key_group_count,
            self.max_node_data_bytes,
        )?;
        validate_node_data_layout(manifest, node_data, self.max_node_data_bytes)?;

        let encoded = Bytes::from(checkpoint_manifest_bytes(manifest)?);
        if u64::try_from(encoded.len()).unwrap_or(u64::MAX) > MAX_MANIFEST_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "manifest exceeds the {MAX_MANIFEST_BYTES}-byte limit"
            )));
        }

        let payload: PutPayload = node_data.iter().cloned().collect();
        let path = self.node_data_path(manifest.node_data.chunk);
        if !self.create_immutable(&path, payload).await?
            && !self.existing_node_data_matches(&manifest.node_data).await?
        {
            return Err(CheckpointStoreError::Invalid(format!(
                "node data for participant {} checkpoint {} already exists with different immutable content",
                manifest.node_data.chunk.participant_id, manifest.node_data.chunk.checkpoint_id
            )));
        }

        // The manifest is the readiness marker. Publishing it last prevents readers from polling
        // an incomplete checkpoint while the larger node-data object is still being uploaded.
        let path = self.manifest_path(manifest.node_data.chunk);
        if !self
            .create_immutable(&path, PutPayload::from_bytes(encoded.clone()))
            .await?
        {
            match self
                .load_bounded_manifest_bytes(manifest.node_data.chunk)
                .await?
            {
                Some(existing) if existing == encoded => {}
                Some(_) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "checkpoint {} manifest already exists with different immutable content",
                        manifest.checkpoint_id
                    )));
                }
                None => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "checkpoint {} manifest create reported a conflict but no object exists",
                        manifest.checkpoint_id
                    )));
                }
            }
        }
        Ok(encoded)
    }

    async fn seal_aborted_manifest(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError> {
        validate_abort_seal_request(chunk, expected_artifact_identity_sha256)?;
        let empty_seal = CheckpointArtifactAbortSeal {
            version: CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION,
            artifact_identity_sha256: expected_artifact_identity_sha256.to_owned(),
            chunk,
            original_manifest: None,
            sink_cleanup_complete: false,
        };
        let empty_seal_bytes = checkpoint_artifact_abort_seal_bytes(&empty_seal)?;
        let path = self.manifest_path(chunk);
        if self
            .create_immutable(&path, PutPayload::from_bytes(empty_seal_bytes.clone()))
            .await?
        {
            return Ok(CheckpointManifestAbortSeal {
                original_manifest: None,
                sink_cleanup_complete: false,
            });
        }

        let current = self
            .load_manifest_or_abort_seal_bytes(chunk)
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} manifest seal create conflicted but no object exists",
                    chunk.participant_id, chunk.checkpoint_id
                ))
            })?;
        match self.decode_manifest_abort_state(
            chunk,
            expected_artifact_identity_sha256,
            &current,
        )? {
            ManifestAbortState::Sealed {
                original_manifest,
                sink_cleanup_complete,
            } => Ok(CheckpointManifestAbortSeal {
                original_manifest,
                sink_cleanup_complete,
            }),
            ManifestAbortState::Manifest(manifest, canonical) => {
                let seal = CheckpointArtifactAbortSeal {
                    version: CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION,
                    artifact_identity_sha256: expected_artifact_identity_sha256.to_owned(),
                    chunk,
                    original_manifest: Some(manifest.clone()),
                    sink_cleanup_complete: false,
                };
                self.overwrite(&path, checkpoint_artifact_abort_seal_bytes(&seal)?)
                    .await?;
                Ok(CheckpointManifestAbortSeal {
                    original_manifest: Some((manifest, canonical)),
                    sink_cleanup_complete: false,
                })
            }
        }
    }

    async fn complete_aborted_sink_cleanup(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<CheckpointManifestAbortSeal, CheckpointStoreError> {
        validate_abort_seal_request(chunk, expected_artifact_identity_sha256)?;
        let path = self.manifest_path(chunk);
        let current = self
            .load_manifest_or_abort_seal_bytes(chunk)
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} has no manifest abort seal",
                    chunk.participant_id, chunk.checkpoint_id
                ))
            })?;
        let ManifestAbortState::Sealed {
            original_manifest,
            sink_cleanup_complete,
        } = self.decode_manifest_abort_state(chunk, expected_artifact_identity_sha256, &current)?
        else {
            return Err(CheckpointStoreError::Invalid(format!(
                "participant {} checkpoint {} manifest is not sealed before sink cleanup",
                chunk.participant_id, chunk.checkpoint_id
            )));
        };
        if sink_cleanup_complete {
            return Ok(CheckpointManifestAbortSeal {
                original_manifest,
                sink_cleanup_complete,
            });
        }
        let seal = CheckpointArtifactAbortSeal {
            version: CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION,
            artifact_identity_sha256: expected_artifact_identity_sha256.to_owned(),
            chunk,
            original_manifest: original_manifest
                .as_ref()
                .map(|(manifest, _)| manifest.clone()),
            sink_cleanup_complete: true,
        };
        self.overwrite(&path, checkpoint_artifact_abort_seal_bytes(&seal)?)
            .await?;
        Ok(CheckpointManifestAbortSeal {
            original_manifest,
            sink_cleanup_complete: true,
        })
    }

    async fn seal_aborted_node_data(
        &self,
        chunk: StateChunkId,
        expected_artifact_identity_sha256: &str,
    ) -> Result<(), CheckpointStoreError> {
        validate_abort_seal_request(chunk, expected_artifact_identity_sha256)?;
        let manifest_bytes = self
            .load_manifest_or_abort_seal_bytes(chunk)
            .await?
            .ok_or_else(|| {
                CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} has no manifest abort seal",
                    chunk.participant_id, chunk.checkpoint_id
                ))
            })?;
        match self.decode_manifest_abort_state(
            chunk,
            expected_artifact_identity_sha256,
            &manifest_bytes,
        )? {
            ManifestAbortState::Sealed {
                sink_cleanup_complete: true,
                ..
            } => {}
            ManifestAbortState::Sealed { .. } => {
                return Err(CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} sink cleanup is incomplete",
                    chunk.participant_id, chunk.checkpoint_id
                )));
            }
            ManifestAbortState::Manifest(..) => {
                return Err(CheckpointStoreError::Invalid(format!(
                    "participant {} checkpoint {} manifest is not sealed",
                    chunk.participant_id, chunk.checkpoint_id
                )));
            }
        }
        let seal = CheckpointArtifactAbortSeal {
            version: CHECKPOINT_ARTIFACT_ABORT_SEAL_VERSION,
            artifact_identity_sha256: expected_artifact_identity_sha256.to_owned(),
            chunk,
            original_manifest: None,
            sink_cleanup_complete: false,
        };
        let encoded = checkpoint_artifact_abort_seal_bytes(&seal)?;
        let path = self.node_data_path(chunk);
        if self
            .create_immutable(&path, PutPayload::from_bytes(encoded.clone()))
            .await?
            || self.node_data_is_exact_abort_seal(chunk, &encoded).await?
        {
            return Ok(());
        }
        self.overwrite(&path, encoded).await
    }

    async fn load_manifest_for_participant(
        &self,
        participant_id: u64,
        checkpoint_id: u64,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        if participant_id == 0 || checkpoint_id == 0 {
            return Err(CheckpointStoreError::Invalid(
                "manifest identity must use nonzero participant and checkpoint ids".into(),
            ));
        }
        self.load_bounded_manifest(StateChunkId {
            participant_id,
            checkpoint_id,
        })
        .await
    }

    async fn load_manifest_verified(
        &self,
        participant_id: u64,
        checkpoint_id: u64,
        expected_len: u64,
        expected_sha256: &str,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        if participant_id == 0 || checkpoint_id == 0 {
            return Err(CheckpointStoreError::Invalid(
                "manifest identity must use nonzero participant and checkpoint ids".into(),
            ));
        }
        let chunk = StateChunkId {
            participant_id,
            checkpoint_id,
        };
        let Some(bytes) = self.load_bounded_manifest_bytes(chunk).await? else {
            return Ok(None);
        };
        let actual_len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let actual_sha256 = sha256(&bytes);
        if actual_len != expected_len || actual_sha256 != expected_sha256 {
            return Err(CheckpointStoreError::Invalid(format!(
                "participant {participant_id} checkpoint {checkpoint_id} manifest differs from the committed reference"
            )));
        }
        self.decode_manifest(chunk, &bytes).map(Some)
    }

    async fn load_node_data_ranges(
        &self,
        chunk: StateChunkId,
        expected_object_length: u64,
        ranges: &[ByteRange],
    ) -> Result<Option<Vec<Bytes>>, CheckpointStoreError> {
        self.load_node_data_ranges_inner(chunk, expected_object_length, ranges)
            .await
    }

    async fn delete_manifest(&self, chunk: StateChunkId) -> Result<(), CheckpointStoreError> {
        self.delete_exact(&self.manifest_path(chunk)).await
    }

    async fn delete_node_data(&self, chunk: StateChunkId) -> Result<(), CheckpointStoreError> {
        self.delete_exact(&self.node_data_path(chunk)).await
    }

    async fn save_subscription_segment(
        &self,
        segment: &OutputSegmentRef,
        payload: Bytes,
    ) -> Result<(), CheckpointStoreError> {
        subscription_segments::save(self, segment, payload).await
    }

    async fn load_subscription_segment(
        &self,
        segment: &OutputSegmentRef,
    ) -> Result<Option<Bytes>, CheckpointStoreError> {
        subscription_segments::load(self, segment).await
    }

    async fn delete_subscription_segment(
        &self,
        object_key: &str,
    ) -> Result<(), CheckpointStoreError> {
        subscription_segments::delete(self, object_key).await
    }

    async fn delete_subscription_orphans(
        &self,
        reachable: &std::collections::BTreeSet<String>,
        through_checkpoint_id: u64,
        grace_before_ms: i64,
    ) -> Result<SubscriptionOrphanCleanup, CheckpointStoreError> {
        subscription_segments::delete_orphans(
            self,
            reachable,
            through_checkpoint_id,
            grace_before_ms,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
