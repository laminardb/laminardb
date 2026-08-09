//! Provider-neutral checkpoint persistence over [`object_store`].

#![allow(clippy::disallowed_types)] // cold path: checkpoint metadata

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use sha2::{Digest, Sha256};

use crate::checkpoint::canonical_json_bytes;
use crate::checkpoint::checkpoint_manifest::{
    checkpoint_descriptor_sha256, ByteRange, CheckpointManifest, NodeDataObject,
    PreparedSinkDescriptor, StateChunkId,
};
use crate::state::{KeyGroupCount, DEFAULT_KEY_GROUP_COUNT, LOCAL_NODE_ID};

const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

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
    if usize::try_from(limit).is_err() {
        return Err(CheckpointStoreError::Invalid(format!(
            "checkpoint node-data limit {limit} exceeds this process address space"
        )));
    }
    Ok(())
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

    /// Validate and conditional-create the immutable node data object, then its manifest.
    async fn save_checkpoint(
        &self,
        manifest: &CheckpointManifest,
        node_data: &[Bytes],
    ) -> Result<Bytes, CheckpointStoreError>;

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
}

/// Prove that conditional create rejects an existing object.
///
/// # Errors
/// Returns an error when conditional create is absent, times out, or cannot be cleaned up.
pub async fn probe_object_store_conditional_create(
    store: &dyn ObjectStore,
    prefix: &str,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    run_conditional_probe(store, prefix, false, timeout).await
}

/// Prove conditional create/update and stale-update rejection.
///
/// # Errors
/// Returns an error when any conditional operation is absent, times out, or cannot be cleaned up.
pub async fn probe_object_store_conditional_update(
    store: &dyn ObjectStore,
    prefix: &str,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    run_conditional_probe(store, prefix, true, timeout).await
}

async fn run_conditional_probe(
    store: &dyn ObjectStore,
    prefix: &str,
    require_update: bool,
    timeout: Duration,
) -> Result<(), CheckpointStoreError> {
    let path = object_store::path::Path::from(format!(
        "{}capability-probes/{}.bin",
        normalize_prefix(prefix),
        uuid::Uuid::new_v4()
    ));
    let probe = async {
        let create = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        store
            .put_opts(
                &path,
                PutPayload::from_static(b"laminardb-conditional-create-v1"),
                create.clone(),
            )
            .await?;
        match store
            .put_opts(
                &path,
                PutPayload::from_static(b"must-not-overwrite"),
                create,
            )
            .await
        {
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {}
            Ok(_) => {
                return Err(CheckpointStoreError::Invalid(
                    "object store overwrote an existing conditional-create probe".into(),
                ));
            }
            Err(error) => return Err(error.into()),
        }
        if require_update {
            let observed = store.get(&path).await?;
            let stale = UpdateVersion {
                e_tag: observed.meta.e_tag.clone(),
                version: observed.meta.version.clone(),
            };
            if stale.e_tag.is_none() && stale.version.is_none() {
                return Err(CheckpointStoreError::Invalid(
                    "object store GET returned neither ETag nor version for conditional update"
                        .into(),
                ));
            }
            store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"laminardb-conditional-update-v2"),
                    PutOptions {
                        mode: PutMode::Update(stale.clone()),
                        ..PutOptions::default()
                    },
                )
                .await?;
            match store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"must-not-stale-update"),
                    PutOptions {
                        mode: PutMode::Update(stale),
                        ..PutOptions::default()
                    },
                )
                .await
            {
                Err(object_store::Error::Precondition { .. }) => {}
                Ok(_) => {
                    return Err(CheckpointStoreError::Invalid(
                        "object store accepted a stale conditional update".into(),
                    ));
                }
                Err(error) => return Err(error.into()),
            }
            let observed = store.get(&path).await?;
            let current = UpdateVersion {
                e_tag: observed.meta.e_tag.clone(),
                version: observed.meta.version.clone(),
            };
            if current.e_tag.is_none() && current.version.is_none() {
                return Err(CheckpointStoreError::Invalid(
                    "object store GET returned neither ETag nor version after conditional update"
                        .into(),
                ));
            }
            store
                .put_opts(
                    &path,
                    PutPayload::from_static(b"laminardb-conditional-update-v3"),
                    PutOptions {
                        mode: PutMode::Update(current),
                        ..PutOptions::default()
                    },
                )
                .await?;
        }
        Ok::<(), CheckpointStoreError>(())
    };

    let result = match tokio::time::timeout(timeout, probe).await {
        Ok(result) => result,
        Err(_) => Err(CheckpointStoreError::Invalid(
            "conditional-put probe timed out".into(),
        )),
    };
    let cleanup = tokio::time::timeout(timeout, store.delete(&path)).await;
    match cleanup {
        Ok(Ok(()) | Err(object_store::Error::NotFound { .. })) => result,
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(CheckpointStoreError::Invalid(
            "conditional-put probe cleanup timed out".into(),
        )),
    }
}

fn ensure_manifest_valid(
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

fn validate_node_data_layout(
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

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn missing_node_data(chunk: StateChunkId) -> CheckpointStoreError {
    CheckpointStoreError::Invalid(format!(
        "node data object for participant {} checkpoint {} is missing",
        chunk.participant_id, chunk.checkpoint_id
    ))
}

fn normalize_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}/")
    }
}

#[cfg(test)]
mod tests;
