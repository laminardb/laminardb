//! Bounded object-store reads and conditional state transitions.

use bytes::Bytes;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use sha2::{Digest, Sha256};

use super::{
    ensure_manifest_valid, normalize_prefix, validate_max_checkpoint_node_data_bytes, ByteRange,
    CheckpointManifest, CheckpointStoreError, ObjectStoreCheckpointStore, StateChunkId,
    DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES, MAX_ABORT_SEAL_BYTES, MAX_MANIFEST_BYTES,
};
use crate::checkpoint::checkpoint_manifest::NodeDataObject;
use crate::state::{DEFAULT_KEY_GROUP_COUNT, LOCAL_NODE_ID};

const CREATE_IMMUTABLE_ATTEMPTS: u32 = 3;
/// Longer than any healthy checkpoint write observed under load, and short enough that a
/// retry still fits inside a 30-second checkpoint deadline.
const CREATE_ATTEMPT_MIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const CREATE_ATTEMPT_MIN_BYTES_PER_SECOND: u64 = 4 * 1024 * 1024;

pub(super) fn create_attempt_timeout(payload_bytes: usize) -> std::time::Duration {
    let bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
    let transfer = std::time::Duration::from_millis(
        bytes.saturating_mul(1_000) / CREATE_ATTEMPT_MIN_BYTES_PER_SECOND,
    );
    transfer.max(CREATE_ATTEMPT_MIN_TIMEOUT)
}

impl ObjectStoreCheckpointStore {
    /// Create a store beneath a deployment-relative prefix.
    ///
    /// Node namespaces are derived as `{prefix}/nodes/{participant_id}/`; callers must not include
    /// a node id in `prefix`.
    #[must_use]
    pub fn new(store: std::sync::Arc<dyn object_store::ObjectStore>, prefix: &str) -> Self {
        Self {
            store,
            prefix: normalize_prefix(prefix),
            key_group_count: DEFAULT_KEY_GROUP_COUNT,
            participant_id: LOCAL_NODE_ID.0,
            max_node_data_bytes: DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
            exclusive_writer: false,
        }
    }

    /// Permit exact overwrite transitions while an external lock excludes every other writer.
    ///
    /// The caller must retain exclusive ownership of this checkpoint namespace for the complete
    /// store lifetime. Shared object-store deployments must use conditional updates instead.
    #[must_use]
    pub fn with_exclusive_writer(mut self) -> Self {
        self.exclusive_writer = true;
        self
    }

    /// Bind the store to one nonzero node id.
    #[must_use]
    pub fn with_participant_id(mut self, participant_id: u64) -> Self {
        self.participant_id = participant_id;
        self
    }

    /// Set the exact vnode topology.
    #[must_use]
    pub fn with_key_group_count(mut self, key_group_count: crate::state::KeyGroupCount) -> Self {
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

    pub(super) fn manifest_path(&self, chunk: StateChunkId) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}nodes/{}/checkpoints/{:020}/manifest.json",
            self.prefix, chunk.participant_id, chunk.checkpoint_id
        ))
    }

    pub(super) fn node_data_path(&self, chunk: StateChunkId) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "{}nodes/{}/checkpoints/{:020}/node-data.bin",
            self.prefix, chunk.participant_id, chunk.checkpoint_id
        ))
    }

    pub(super) async fn load_bounded_manifest_bytes(
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

    pub(super) fn decode_manifest(
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

    pub(super) async fn load_bounded_manifest(
        &self,
        chunk: StateChunkId,
    ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
        let Some(bytes) = self.load_bounded_manifest_bytes(chunk).await? else {
            return Ok(None);
        };
        let manifest_error = match self.decode_manifest(chunk, &bytes) {
            Ok(manifest) => return Ok(Some(manifest)),
            Err(CheckpointStoreError::Serde(error)) => error,
            Err(error) => return Err(error),
        };
        // A pre-begin intent owns this key until its conditional promotion; it is not readiness.
        if Self::decode_pending_artifact_intent_record(&bytes, chunk)?.is_some() {
            return Ok(None);
        }
        Err(CheckpointStoreError::Serde(manifest_error))
    }

    pub(super) async fn overwrite(
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

    pub(super) async fn replace_exact(
        &self,
        path: &object_store::path::Path,
        expected: &Bytes,
        replacement: Bytes,
    ) -> Result<(), CheckpointStoreError> {
        let current = self.store.get(path).await?;
        if current.meta.size > MAX_ABORT_SEAL_BYTES {
            return Err(CheckpointStoreError::Invalid(format!(
                "checkpoint control object is {} bytes, exceeding the {MAX_ABORT_SEAL_BYTES}-byte limit",
                current.meta.size
            )));
        }
        let update = UpdateVersion {
            e_tag: current.meta.e_tag.clone(),
            version: current.meta.version.clone(),
        };
        if current.bytes().await? != *expected {
            return Err(CheckpointStoreError::Invalid(
                "checkpoint artifact state changed before its durable transition".into(),
            ));
        }
        if self.exclusive_writer {
            return self.overwrite(path, replacement).await;
        }
        if update.e_tag.is_none() && update.version.is_none() {
            return Err(CheckpointStoreError::Invalid(
                "checkpoint store cannot conditionally replace durable artifact state".into(),
            ));
        }
        match self
            .store
            .put_opts(
                path,
                PutPayload::from_bytes(replacement),
                PutOptions {
                    mode: PutMode::Update(update),
                    ..PutOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::NotFound { .. }
                | object_store::Error::Precondition { .. },
            ) => Err(CheckpointStoreError::Invalid(
                "checkpoint artifact state changed during its durable transition".into(),
            )),
            Err(
                object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. },
            ) => Err(CheckpointStoreError::Invalid(
                "checkpoint store does not support conditional artifact-state replacement".into(),
            )),
            Err(error) => Err(error.into()),
        }
    }

    /// Returns `false` when the object already exists; callers read it back and compare bytes.
    ///
    /// A request that stalls or fails in transport may still have been applied, and the store
    /// client does not retry a non-idempotent create. The caller's content comparison makes a
    /// repeated create safe, so each attempt is bounded and retried within the caller's deadline.
    pub(super) async fn create_immutable(
        &self,
        path: &object_store::path::Path,
        payload: PutPayload,
    ) -> Result<bool, CheckpointStoreError> {
        let mut attempt_timeout = create_attempt_timeout(payload.content_length());
        let mut attempt = 1;
        loop {
            let options = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            let created = tokio::time::timeout(
                attempt_timeout,
                self.store.put_opts(path, payload.clone(), options),
            )
            .await;
            let retry_reason = match created {
                Ok(Ok(_)) => return Ok(true),
                Ok(Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                )) => return Ok(false),
                Ok(Err(
                    object_store::Error::NotImplemented { .. }
                    | object_store::Error::NotSupported { .. },
                )) => {
                    return Err(CheckpointStoreError::Invalid(format!(
                        "object store does not support conditional create for '{path}'"
                    )));
                }
                Ok(Err(error @ object_store::Error::Generic { .. }))
                    if attempt < CREATE_IMMUTABLE_ATTEMPTS =>
                {
                    error.to_string()
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) if attempt < CREATE_IMMUTABLE_ATTEMPTS => {
                    format!("no response within {attempt_timeout:?}")
                }
                Err(_) => {
                    return Err(CheckpointStoreError::ObjectStore(
                        object_store::Error::Generic {
                            store: "checkpoint store",
                            source: format!(
                                "conditional create for '{path}' had no response after \
                                 {CREATE_IMMUTABLE_ATTEMPTS} attempts"
                            )
                            .into(),
                        },
                    ));
                }
            };
            tracing::warn!(
                %path,
                attempt,
                reason = %retry_reason,
                "checkpoint object create did not complete; retrying"
            );
            attempt += 1;
            attempt_timeout = attempt_timeout.saturating_mul(2);
        }
    }

    pub(super) async fn existing_node_data_matches(
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

    pub(super) async fn delete_exact(
        &self,
        path: &object_store::path::Path,
    ) -> Result<(), CheckpointStoreError> {
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) async fn load_node_data_ranges_inner(
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
            object_store::coalesce_ranges(&nonempty, |range| self.store.get_range(&path, range), 0)
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
