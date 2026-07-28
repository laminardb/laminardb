//! [`ObjectStoreBackend`] — durable partial-state storage backed by any
//! `object_store` implementation (S3, GCS, Azure, `LocalFileSystem`).
//!
//! `seal_checkpoint` performs a CAS seal: if every vnode's `partial.bin`
//! and every required commit descriptor is present, `put(_SEAL, Create)`
//! seals the exact checkpoint attempt. The `_SEAL` marker is the durability boundary the
//! checkpoint coordinator consults before releasing sinks. Retention advances one durable,
//! monotonic prune floor before deleting artifacts, preventing concurrent or restarted writers
//! from republishing a retired attempt without accumulating one tombstone per checkpoint.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload,
    UpdateVersion,
};

use crate::checkpoint::{
    CheckpointAssignmentFence, LeaderProof, LeaderProofOwner, PipelineIdentity,
};

use super::backend::{
    digest_hex, sha256, CheckpointAttempt, CheckpointSeal, CheckpointSealInventory,
    SealedCommitDescriptor, SealedCommitDescriptorWriter, SealedVnodePartial, SealedVnodeWriter,
    StateBackend, StateBackendDurability, StateBackendError, StateNamespaceBinding,
    VnodePartialLineage, CHECKPOINT_SEAL_VERSION, STATE_NAMESPACE_RESOURCE,
};

const VNODE_PARTIAL_MAGIC: &[u8; 8] = b"LDBVP3\0\0";
const VNODE_PARTIAL_VERSION: u32 = 3;
const VNODE_PARTIAL_HEADER_LEN: usize = 164;
const PARTIAL_ATTESTATION_READ_CONCURRENCY: usize = 32;
const COMMIT_DESCRIPTOR_MAGIC: &[u8; 8] = b"LDBCD2\0\0";
const COMMIT_DESCRIPTOR_VERSION: u32 = 2;
const COMMIT_DESCRIPTOR_HEADER_LEN: usize = 204;
const DESCRIPTOR_ATTESTATION_READ_CONCURRENCY: usize = 32;
const STATE_PRUNE_FLOOR_VERSION: u32 = 1;
const STATE_PRUNE_FLOOR_MAX_BYTES: u64 = 512;
const STATE_PRUNE_DELETE_BATCH_SIZE: usize = 256;
const STATE_NAMESPACE_VERSION: u32 = 1;
const STATE_NAMESPACE_MAX_BYTES: u64 = 512;
// MAX_KEY_GROUP_COUNT (65,535) at a conservative 768 encoded bytes of provenance per
// vnode is under 48 MiB; 64 MiB leaves over 16 MiB for the assignment and descriptors.
const MAX_CHECKPOINT_SEAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct StateNamespaceMarker {
    version: u32,
    deployment_id: String,
    pipeline_identity: PipelineIdentity,
}

/// Monotonic publication fence for every state attempt below `before_epoch`.
///
/// `swept_before_epoch` is only a repair cursor. Readers and writers fence on
/// `before_epoch`, so a crash between publishing the floor and deleting old objects cannot make a
/// pruned checkpoint visible again.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct StatePruneFloor {
    version: u32,
    before_epoch: u64,
    swept_before_epoch: u64,
}

#[derive(Debug)]
struct VersionedStatePruneFloor {
    floor: StatePruneFloor,
    update_version: UpdateVersion,
}

/// Object-store-backed [`StateBackend`].
pub struct ObjectStoreBackend {
    store: Arc<dyn ObjectStore>,
    empty_prefix_cleanup: Option<Arc<dyn crate::durable_local_store::EmptyPrefixCleanup>>,
    durability_scope: StateBackendDurability,
    instance_id: String,
    /// Fresh for each backend construction, even when `instance_id` is stable across restarts.
    execution_id: uuid::Uuid,
    vnode_capacity: u32,
    /// Split-brain fence: writes must match this exact `assignment_version`.
    /// `0` disables the fence, accepting unconfigured single-instance callers.
    authoritative_version: Arc<AtomicU64>,
    /// Serializes the node-local fallback for stores that cannot perform a conditional update.
    prune_floor_update_lock: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for ObjectStoreBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreBackend")
            .field("durability_scope", &self.durability_scope)
            .field("instance_id", &self.instance_id)
            .field("execution_id", &self.execution_id)
            .field("vnode_capacity", &self.vnode_capacity)
            .finish_non_exhaustive()
    }
}

impl ObjectStoreBackend {
    /// Wrap an existing [`ObjectStore`] without certifying persistence.
    ///
    /// The opaque trait object does not reveal whether it is an in-memory,
    /// node-local, or shared implementation, so this conservative constructor
    /// reports [`StateBackendDurability::Volatile`]. Production hosts should use
    /// [`Self::node_durable`] or [`Self::cluster_shared`] after establishing the
    /// storage topology.
    #[must_use]
    pub fn new(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::Volatile,
        )
    }

    /// Wrap storage that survives restart on this node but is not guaranteed
    /// to be reachable by cluster peers.
    #[must_use]
    pub fn node_durable(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::NodeDurable,
        )
    }

    pub(crate) fn node_durable_with_empty_prefix_cleanup<T>(
        store: Arc<T>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self
    where
        T: ObjectStore + crate::durable_local_store::EmptyPrefixCleanup + 'static,
    {
        let object_store: Arc<dyn ObjectStore> = store.clone();
        let cleanup: Arc<dyn crate::durable_local_store::EmptyPrefixCleanup> = store;
        let mut backend = Self::node_durable(object_store, instance_id, vnode_capacity);
        backend.empty_prefix_cleanup = Some(cleanup);
        backend
    }

    /// Wrap durable storage whose namespace is reachable by every cluster node.
    #[must_use]
    pub fn cluster_shared(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
    ) -> Self {
        Self::with_durability_scope(
            store,
            instance_id,
            vnode_capacity,
            StateBackendDurability::ClusterShared,
        )
    }

    fn with_durability_scope(
        store: Arc<dyn ObjectStore>,
        instance_id: impl Into<String>,
        vnode_capacity: u32,
        durability_scope: StateBackendDurability,
    ) -> Self {
        let instance_id = instance_id.into();
        Self {
            store,
            empty_prefix_cleanup: None,
            durability_scope,
            instance_id,
            execution_id: uuid::Uuid::new_v4(),
            vnode_capacity,
            authoritative_version: Arc::new(AtomicU64::new(0)),
            prune_floor_update_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Shared handle to the authoritative version counter, cloneable by a
    /// single owner that drives it without relaying through the trait method.
    #[must_use]
    pub fn authoritative_version_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.authoritative_version)
    }

    #[cfg(test)]
    fn execution_id(&self) -> uuid::Uuid {
        self.execution_id
    }

    fn check_vnode(&self, v: u32) -> Result<(), StateBackendError> {
        if v >= self.vnode_capacity {
            Err(StateBackendError::Io(format!(
                "vnode {v} out of range (capacity {})",
                self.vnode_capacity
            )))
        } else {
            Ok(())
        }
    }

    fn attempt_prefix(attempt: CheckpointAttempt) -> String {
        format!(
            "state-v2/epoch={}/checkpoint={}/",
            attempt.epoch, attempt.checkpoint_id
        )
    }

    fn ensure_canonical_attempt(attempt: CheckpointAttempt) -> Result<(), StateBackendError> {
        if attempt.is_canonical() {
            Ok(())
        } else {
            Err(StateBackendError::Conflict {
                resource: Self::attempt_prefix(attempt),
                message: "state attempt must use one nonzero canonical checkpoint ID".into(),
            })
        }
    }

    fn partial_path(attempt: CheckpointAttempt, vnode: u32) -> OsPath {
        OsPath::from(format!(
            "{}vnode={vnode}/partial.bin",
            Self::attempt_prefix(attempt)
        ))
    }

    fn seal_path(attempt: CheckpointAttempt) -> OsPath {
        OsPath::from(format!("{}_SEAL", Self::attempt_prefix(attempt)))
    }

    fn descriptor_path(attempt: CheckpointAttempt, key: &str) -> OsPath {
        OsPath::from(format!("{}commit/{key}", Self::attempt_prefix(attempt)))
    }

    fn prune_floor_path() -> OsPath {
        OsPath::from("state-v2/_PRUNE_FLOOR")
    }

    fn namespace_path() -> OsPath {
        OsPath::from(STATE_NAMESPACE_RESOURCE)
    }

    fn check_namespace_marker_size(path: &OsPath, size: u64) -> Result<(), StateBackendError> {
        if size == 0 || size > STATE_NAMESPACE_MAX_BYTES {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "state namespace marker is {size} bytes; expected 1..={STATE_NAMESPACE_MAX_BYTES}"
                ),
            });
        }
        Ok(())
    }

    fn encode_namespace_marker(
        binding: &StateNamespaceBinding,
    ) -> Result<Bytes, StateBackendError> {
        let marker = StateNamespaceMarker {
            version: STATE_NAMESPACE_VERSION,
            deployment_id: binding.deployment_id.clone(),
            pipeline_identity: binding.pipeline_identity.clone(),
        };
        let bytes = serde_json::to_vec(&marker)
            .map(Bytes::from)
            .map_err(|error| StateBackendError::Serialization(error.to_string()))?;
        Self::check_namespace_marker_size(&Self::namespace_path(), bytes.len() as u64)?;
        Ok(bytes)
    }

    fn decode_namespace_marker(
        path: &OsPath,
        bytes: &[u8],
    ) -> Result<StateNamespaceBinding, StateBackendError> {
        Self::check_namespace_marker_size(path, bytes.len() as u64)?;
        let marker: StateNamespaceMarker =
            serde_json::from_slice(bytes).map_err(|error| StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!("state namespace marker is malformed: {error}"),
            })?;
        if marker.version != STATE_NAMESPACE_VERSION {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "state namespace marker version {} is unsupported; expected {STATE_NAMESPACE_VERSION}",
                    marker.version
                ),
            });
        }
        let canonical = serde_json::to_vec(&marker)
            .map_err(|error| StateBackendError::Serialization(error.to_string()))?;
        if canonical.as_slice() != bytes {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: "state namespace marker is not canonical".into(),
            });
        }
        StateNamespaceBinding::try_new(&marker.deployment_id, &marker.pipeline_identity)
    }

    async fn read_namespace_binding(
        &self,
        path: &OsPath,
    ) -> Result<StateNamespaceBinding, StateBackendError> {
        let result = self
            .store
            .get(path)
            .await
            .map_err(|error| StateBackendError::Io(error.to_string()))?;
        Self::check_namespace_marker_size(path, result.meta.size)?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| StateBackendError::Io(error.to_string()))?;
        Self::decode_namespace_marker(path, &bytes)
    }

    fn verify_namespace_binding(
        path: &OsPath,
        existing: &StateNamespaceBinding,
        requested: &StateNamespaceBinding,
    ) -> Result<(), StateBackendError> {
        if existing.deployment_id != requested.deployment_id {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "state root belongs to deployment {}; requested {}",
                    existing.deployment_id, requested.deployment_id
                ),
            });
        }
        if existing.pipeline_identity != requested.pipeline_identity {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "state root pipeline identity {} does not match requested {}",
                    existing.pipeline_identity.sha256, requested.pipeline_identity.sha256
                ),
            });
        }
        Ok(())
    }

    async fn preflight_unbound_state_root(
        &self,
        namespace_path: &OsPath,
        requested: &StateNamespaceBinding,
    ) -> Result<bool, StateBackendError> {
        use futures::StreamExt as _;

        let prefix = OsPath::from("state-v2");
        let mut objects = self.store.list(Some(&prefix));
        if let Some(result) = objects.next().await {
            let object = result.map_err(|error| StateBackendError::Io(error.to_string()))?;
            if object.location == *namespace_path {
                let existing = self.read_namespace_binding(namespace_path).await?;
                Self::verify_namespace_binding(namespace_path, &existing, requested)?;
                return Ok(true);
            }

            // A concurrent first binder may have published the marker and begun state I/O after
            // this caller's initial marker lookup. Prefer its immutable binding over a false
            // legacy-artifact rejection.
            match self.store.get(namespace_path).await {
                Ok(marker) => {
                    Self::check_namespace_marker_size(namespace_path, marker.meta.size)?;
                    let bytes = marker
                        .bytes()
                        .await
                        .map_err(|error| StateBackendError::Io(error.to_string()))?;
                    let existing = Self::decode_namespace_marker(namespace_path, &bytes)?;
                    Self::verify_namespace_binding(namespace_path, &existing, requested)?;
                    return Ok(true);
                }
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(StateBackendError::Conflict {
                        resource: namespace_path.to_string(),
                        message: format!(
                            "state root contains unbound artifact {}; remove the old state root before reuse",
                            object.location
                        ),
                    });
                }
                Err(error) => return Err(StateBackendError::Io(error.to_string())),
            }
        }
        Ok(false)
    }

    async fn verify_object_size_from_metadata(
        &self,
        path: &OsPath,
        listed_size: Option<u64>,
        expected_size: u64,
    ) -> Result<(), StateBackendError> {
        if listed_size == Some(expected_size) {
            return Ok(());
        }
        match self.store.head(path).await {
            Ok(metadata) if metadata.size == expected_size => Ok(()),
            Ok(metadata) => Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "sealed artifact is {} bytes in storage metadata; expected {expected_size}",
                    metadata.size
                ),
            }),
            Err(object_store::Error::NotFound { .. }) => Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: "sealed artifact is missing from storage metadata".into(),
            }),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    /// Parse one immediate `state-v2/epoch=N` delimiter prefix.
    fn epoch_from_prefix(prefix: &OsPath) -> Option<u64> {
        let encoded = prefix.as_ref().strip_prefix("state-v2/epoch=")?;
        if encoded.is_empty() || encoded.contains('/') {
            return None;
        }
        let epoch = encoded.parse::<u64>().ok()?;
        (epoch != 0 && epoch.to_string() == encoded).then_some(epoch)
    }

    /// Wrap raw operator state in a fixed-width provenance header. The fixed width lets the
    /// durability gate validate hundreds of vnode generations with small concurrent range GETs
    /// instead of downloading every state blob again.
    fn encode_partial(
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        writer: Option<&SealedVnodeWriter>,
        lineage: VnodePartialLineage,
        payload: &Bytes,
    ) -> Bytes {
        let payload_digest = sha256(payload);
        let mut encoded = Vec::with_capacity(VNODE_PARTIAL_HEADER_LEN + payload.len());
        encoded.extend_from_slice(VNODE_PARTIAL_MAGIC);
        encoded.extend_from_slice(&VNODE_PARTIAL_VERSION.to_be_bytes());
        encoded.extend_from_slice(&attempt.epoch.to_be_bytes());
        encoded.extend_from_slice(&attempt.checkpoint_id.to_be_bytes());
        encoded.extend_from_slice(&vnode.to_be_bytes());
        encoded.extend_from_slice(&assignment_version.to_be_bytes());
        if let Some(writer) = writer {
            encoded.extend_from_slice(&writer.node_id.to_be_bytes());
            encoded.extend_from_slice(writer.boot_incarnation.as_bytes());
            encoded.extend_from_slice(&writer.assignment_certificate_digest);
        } else {
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(uuid::Uuid::nil().as_bytes());
            encoded.extend_from_slice(&[0; 32]);
        }
        encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&payload_digest);
        if let Some(parent) = lineage.parent() {
            encoded.extend_from_slice(&parent.epoch.to_be_bytes());
            encoded.extend_from_slice(&parent.checkpoint_id.to_be_bytes());
        } else {
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(&0_u64.to_be_bytes());
        }
        encoded.extend_from_slice(&lineage.total_payload_bytes().to_be_bytes());
        encoded.extend_from_slice(&lineage.artifact_count().to_be_bytes());
        debug_assert_eq!(encoded.len(), VNODE_PARTIAL_HEADER_LEN);
        encoded.extend_from_slice(payload);
        Bytes::from(encoded)
    }

    fn parse_partial_header(
        header: &[u8],
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<SealedVnodePartial, StateBackendError> {
        fn field<const N: usize>(
            header: &[u8],
            start: usize,
        ) -> Result<[u8; N], StateBackendError> {
            header
                .get(start..start + N)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    StateBackendError::Serialization(
                        "truncated vnode partial provenance header".into(),
                    )
                })
        }

        if header.len() < VNODE_PARTIAL_HEADER_LEN
            || &header[..VNODE_PARTIAL_MAGIC.len()] != VNODE_PARTIAL_MAGIC
        {
            return Err(StateBackendError::Serialization(
                "invalid vnode partial provenance header".into(),
            ));
        }
        let version = u32::from_be_bytes(field(header, 8)?);
        if version != VNODE_PARTIAL_VERSION {
            return Err(StateBackendError::Serialization(format!(
                "unsupported vnode partial version {version}; expected {VNODE_PARTIAL_VERSION}"
            )));
        }
        let attempt = CheckpointAttempt::new(
            u64::from_be_bytes(field(header, 12)?),
            u64::from_be_bytes(field(header, 20)?),
        );
        let vnode = u32::from_be_bytes(field(header, 28)?);
        if attempt != expected_attempt || vnode != expected_vnode {
            return Err(StateBackendError::Conflict {
                resource: Self::partial_path(expected_attempt, expected_vnode).to_string(),
                message: format!(
                    "partial header names attempt {attempt:?} vnode {vnode}, expected attempt \
                     {expected_attempt:?} vnode {expected_vnode}"
                ),
            });
        }
        let assignment_version = u64::from_be_bytes(field(header, 32)?);
        let writer_node_id = u64::from_be_bytes(field(header, 40)?);
        let writer_boot_incarnation = uuid::Uuid::from_bytes(field(header, 48)?);
        let assignment_certificate_digest = field::<32>(header, 64)?;
        let writer = if writer_node_id == 0
            && writer_boot_incarnation.is_nil()
            && assignment_certificate_digest == [0; 32]
        {
            None
        } else if writer_node_id != 0
            && !writer_boot_incarnation.is_nil()
            && assignment_certificate_digest != [0; 32]
        {
            Some(SealedVnodeWriter {
                node_id: writer_node_id,
                boot_incarnation: writer_boot_incarnation,
                assignment_certificate_digest,
            })
        } else {
            return Err(StateBackendError::Serialization(
                "incomplete vnode partial writer certificate".into(),
            ));
        };
        let payload_len = u64::from_be_bytes(field(header, 96)?);
        let payload_digest = field::<32>(header, 104)?;
        let parent_epoch = u64::from_be_bytes(field(header, 136)?);
        let parent_checkpoint_id = u64::from_be_bytes(field(header, 144)?);
        let parent = match (parent_epoch, parent_checkpoint_id) {
            (0, 0) => None,
            _ => Some(CheckpointAttempt::new(parent_epoch, parent_checkpoint_id)),
        };
        let lineage = VnodePartialLineage::from_persisted(
            parent,
            u64::from_be_bytes(field(header, 152)?),
            u32::from_be_bytes(field(header, 160)?),
        );
        lineage.validate(attempt, payload_len).map_err(|message| {
            StateBackendError::Serialization(format!(
                "invalid vnode partial lineage metadata: {message}"
            ))
        })?;
        Ok(SealedVnodePartial {
            vnode,
            assignment_version,
            writer,
            payload_len,
            payload_sha256: digest_hex(&payload_digest),
            lineage,
        })
    }

    fn decode_partial(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<Bytes, StateBackendError> {
        Self::decode_partial_with_attestation(bytes, expected_attempt, expected_vnode)
            .map(|(_, payload)| payload)
    }

    fn decode_partial_with_attestation(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<(SealedVnodePartial, Bytes), StateBackendError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        let metadata = Self::parse_partial_header(bytes, expected_attempt, expected_vnode)?;
        let payload_len = usize::try_from(metadata.payload_len).map_err(|_| {
            StateBackendError::Serialization("vnode partial payload length overflows usize".into())
        })?;
        if bytes.len() != VNODE_PARTIAL_HEADER_LEN.saturating_add(payload_len) {
            return Err(StateBackendError::Serialization(format!(
                "vnode partial payload length mismatch: header={} actual={}",
                metadata.payload_len,
                bytes.len().saturating_sub(VNODE_PARTIAL_HEADER_LEN)
            )));
        }
        let payload = bytes.slice(VNODE_PARTIAL_HEADER_LEN..);
        if metadata.payload_sha256 != digest_hex(&sha256(&payload)) {
            return Err(StateBackendError::Serialization(
                "vnode partial payload checksum mismatch".into(),
            ));
        }
        let payload = if payload.is_empty() || payload.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0
        {
            payload
        } else {
            // Object-store clients may expose a view into an arbitrarily aligned network
            // buffer. Normalize it once here so recovery-chain consumers can validate and
            // decode the rkyv payload repeatedly without making a fresh aligned copy on every
            // pass.
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
            aligned.extend_from_slice(&payload);
            Bytes::from_owner(aligned)
        };
        Ok((metadata, payload))
    }

    async fn read_partial_attestation(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<SealedVnodePartial>, StateBackendError> {
        let path = Self::partial_path(attempt, vnode);
        match self
            .store
            .get_range(&path, 0..VNODE_PARTIAL_HEADER_LEN as u64)
            .await
        {
            Ok(header) => Self::parse_partial_header(&header, attempt, vnode).map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    /// Wrap a coordinated-commit descriptor in a fixed-width identity and content header. Seal
    /// publication reads only this header; recovery validates the complete payload before use.
    fn encode_commit_descriptor(
        attempt: CheckpointAttempt,
        key: &str,
        assignment_version: u64,
        writer: Option<&SealedCommitDescriptorWriter>,
        payload: &Bytes,
    ) -> Bytes {
        let key_digest = sha256(key.as_bytes());
        let payload_digest = sha256(payload);
        let mut encoded = Vec::with_capacity(COMMIT_DESCRIPTOR_HEADER_LEN + payload.len());
        encoded.extend_from_slice(COMMIT_DESCRIPTOR_MAGIC);
        encoded.extend_from_slice(&COMMIT_DESCRIPTOR_VERSION.to_be_bytes());
        encoded.extend_from_slice(&attempt.epoch.to_be_bytes());
        encoded.extend_from_slice(&attempt.checkpoint_id.to_be_bytes());
        encoded.extend_from_slice(&key_digest);
        encoded.extend_from_slice(&assignment_version.to_be_bytes());
        if let Some(writer) = writer {
            encoded.extend_from_slice(&writer.assignment_certificate_digest);
            encoded.extend_from_slice(&writer.participant.node_id.to_be_bytes());
            encoded.extend_from_slice(writer.participant.boot_incarnation.as_bytes());
            encoded.extend_from_slice(&writer.leader_proof.owner.node_id.to_be_bytes());
            encoded.extend_from_slice(writer.leader_proof.owner.boot_id.as_bytes());
            encoded.extend_from_slice(&writer.leader_proof.owner.process_term.to_be_bytes());
            encoded.extend_from_slice(&writer.leader_proof.fencing_token.to_be_bytes());
        } else {
            encoded.extend_from_slice(&[0; 32]);
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(uuid::Uuid::nil().as_bytes());
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(uuid::Uuid::nil().as_bytes());
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(&0_u64.to_be_bytes());
        }
        encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&payload_digest);
        debug_assert_eq!(encoded.len(), COMMIT_DESCRIPTOR_HEADER_LEN);
        encoded.extend_from_slice(payload);
        Bytes::from(encoded)
    }

    fn parse_commit_descriptor_header(
        header: &[u8],
        expected_attempt: CheckpointAttempt,
        expected_key: &str,
    ) -> Result<SealedCommitDescriptor, StateBackendError> {
        fn field<const N: usize>(
            header: &[u8],
            start: usize,
        ) -> Result<[u8; N], StateBackendError> {
            header
                .get(start..start + N)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    StateBackendError::Serialization(
                        "truncated commit descriptor provenance header".into(),
                    )
                })
        }

        if header.len() < COMMIT_DESCRIPTOR_HEADER_LEN
            || &header[..COMMIT_DESCRIPTOR_MAGIC.len()] != COMMIT_DESCRIPTOR_MAGIC
        {
            return Err(StateBackendError::Serialization(
                "invalid commit descriptor provenance header".into(),
            ));
        }
        let version = u32::from_be_bytes(field(header, 8)?);
        if version != COMMIT_DESCRIPTOR_VERSION {
            return Err(StateBackendError::Serialization(format!(
                "unsupported commit descriptor version {version}; expected \
                 {COMMIT_DESCRIPTOR_VERSION}"
            )));
        }
        let attempt = CheckpointAttempt::new(
            u64::from_be_bytes(field(header, 12)?),
            u64::from_be_bytes(field(header, 20)?),
        );
        let key_digest = field::<32>(header, 28)?;
        if attempt != expected_attempt || key_digest != sha256(expected_key.as_bytes()) {
            return Err(StateBackendError::Conflict {
                resource: Self::descriptor_path(expected_attempt, expected_key).to_string(),
                message: format!(
                    "descriptor header names attempt {attempt:?} key digest {}, expected attempt \
                     {expected_attempt:?} key digest {}",
                    digest_hex(&key_digest),
                    digest_hex(&sha256(expected_key.as_bytes()))
                ),
            });
        }

        let assignment_version = u64::from_be_bytes(field(header, 60)?);
        let assignment_certificate_digest = field::<32>(header, 68)?;
        let writer_node_id = u64::from_be_bytes(field(header, 100)?);
        let writer_boot_incarnation = uuid::Uuid::from_bytes(field(header, 108)?);
        let leader_node_id = u64::from_be_bytes(field(header, 124)?);
        let leader_boot_id = uuid::Uuid::from_bytes(field(header, 132)?);
        let leader_process_term = u64::from_be_bytes(field(header, 148)?);
        let leader_fencing_token = u64::from_be_bytes(field(header, 156)?);
        let local_provenance = assignment_version == 0
            && assignment_certificate_digest == [0; 32]
            && writer_node_id == 0
            && writer_boot_incarnation.is_nil()
            && leader_node_id == 0
            && leader_boot_id.is_nil()
            && leader_process_term == 0
            && leader_fencing_token == 0;
        let writer = if local_provenance {
            None
        } else {
            let leader_proof = LeaderProof {
                owner: LeaderProofOwner {
                    node_id: leader_node_id,
                    boot_id: leader_boot_id,
                    process_term: leader_process_term,
                },
                fencing_token: leader_fencing_token,
            };
            if assignment_version == 0
                || assignment_certificate_digest == [0; 32]
                || writer_node_id == 0
                || writer_boot_incarnation.is_nil()
                || !leader_proof.is_canonical()
            {
                return Err(StateBackendError::Serialization(
                    "incomplete commit descriptor writer certificate".into(),
                ));
            }
            Some(SealedCommitDescriptorWriter {
                participant: crate::checkpoint::CheckpointParticipant {
                    node_id: writer_node_id,
                    boot_incarnation: writer_boot_incarnation,
                },
                assignment_certificate_digest,
                leader_proof,
            })
        };
        let payload_len = u64::from_be_bytes(field(header, 164)?);
        let payload_digest = field::<32>(header, 172)?;
        Ok(SealedCommitDescriptor {
            key: expected_key.to_owned(),
            assignment_version,
            writer,
            payload_len,
            payload_sha256: digest_hex(&payload_digest),
        })
    }

    fn decode_commit_descriptor(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_key: &str,
    ) -> Result<Bytes, StateBackendError> {
        Self::decode_commit_descriptor_with_attestation(bytes, expected_attempt, expected_key)
            .map(|(_, payload)| payload)
    }

    fn decode_commit_descriptor_with_attestation(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_key: &str,
    ) -> Result<(SealedCommitDescriptor, Bytes), StateBackendError> {
        let metadata = Self::parse_commit_descriptor_header(bytes, expected_attempt, expected_key)?;
        let payload_len = usize::try_from(metadata.payload_len).map_err(|_| {
            StateBackendError::Serialization(
                "commit descriptor payload length overflows usize".into(),
            )
        })?;
        if bytes.len() != COMMIT_DESCRIPTOR_HEADER_LEN.saturating_add(payload_len) {
            return Err(StateBackendError::Serialization(format!(
                "commit descriptor payload length mismatch: header={} actual={}",
                metadata.payload_len,
                bytes.len().saturating_sub(COMMIT_DESCRIPTOR_HEADER_LEN)
            )));
        }
        let payload = bytes.slice(COMMIT_DESCRIPTOR_HEADER_LEN..);
        if metadata.payload_sha256 != digest_hex(&sha256(&payload)) {
            return Err(StateBackendError::Serialization(
                "commit descriptor payload checksum mismatch".into(),
            ));
        }
        Ok((metadata, payload))
    }

    async fn read_commit_descriptor_attestation(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<SealedCommitDescriptor>, StateBackendError> {
        let path = Self::descriptor_path(attempt, key);
        let options = GetOptions {
            range: Some(GetRange::Bounded(0..COMMIT_DESCRIPTOR_HEADER_LEN as u64)),
            ..GetOptions::default()
        };
        match self.store.get_opts(&path, options).await {
            Ok(result) => {
                let object_size = result.meta.size;
                let header = result
                    .bytes()
                    .await
                    .map_err(|error| StateBackendError::Io(error.to_string()))?;
                let attestation = Self::parse_commit_descriptor_header(&header, attempt, key)?;
                let expected_size = (COMMIT_DESCRIPTOR_HEADER_LEN as u64)
                    .checked_add(attestation.payload_len)
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "commit descriptor declared length overflows object size".into(),
                    })?;
                if object_size != expected_size {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "commit descriptor declared {} payload bytes but its stored object is \
                             {object_size} bytes",
                            attestation.payload_len
                        ),
                    });
                }
                Ok(Some(attestation))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn read_prune_floor(
        &self,
    ) -> Result<Option<VersionedStatePruneFloor>, StateBackendError> {
        let path = Self::prune_floor_path();
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(StateBackendError::Io(error.to_string())),
        };
        if result.meta.size == 0 || result.meta.size > STATE_PRUNE_FLOOR_MAX_BYTES {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "state prune floor is {} bytes; expected 1..={STATE_PRUNE_FLOOR_MAX_BYTES}",
                    result.meta.size
                ),
            });
        }
        let update_version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| StateBackendError::Io(error.to_string()))?;
        let floor: StatePruneFloor =
            serde_json::from_slice(&bytes).map_err(|error| StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!("invalid state prune floor: {error}"),
            })?;
        if floor.version != STATE_PRUNE_FLOOR_VERSION
            || floor.before_epoch == 0
            || floor.swept_before_epoch > floor.before_epoch
        {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: "state prune floor has a non-canonical version or horizon".into(),
            });
        }
        let canonical = serde_json::to_vec(&floor)
            .map_err(|error| StateBackendError::Serialization(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: "state prune floor does not use its canonical body".into(),
            });
        }
        Ok(Some(VersionedStatePruneFloor {
            floor,
            update_version,
        }))
    }

    async fn attempt_is_pruned(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<bool, StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        Ok(self
            .read_prune_floor()
            .await?
            .is_some_and(|versioned| attempt.epoch < versioned.floor.before_epoch))
    }

    async fn ensure_attempt_live(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<(), StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        if let Some(versioned) = self.read_prune_floor().await? {
            if attempt.epoch < versioned.floor.before_epoch {
                return Err(StateBackendError::Conflict {
                    resource: Self::attempt_prefix(attempt),
                    message: format!(
                        "checkpoint epoch {} is below durable state prune floor {}",
                        attempt.epoch, versioned.floor.before_epoch
                    ),
                });
            }
        }
        Ok(())
    }

    async fn put_live_immutable(
        &self,
        attempt: CheckpointAttempt,
        path: &OsPath,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.ensure_attempt_live(attempt).await?;
        self.put_immutable(path, bytes).await?;
        let Some(floor) = self.read_prune_floor().await? else {
            return Ok(());
        };
        if attempt.epoch >= floor.floor.before_epoch {
            return Ok(());
        }
        match self.store.delete(path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(delete_error) => tracing::warn!(
                %delete_error,
                path = %path,
                "state prune: failed to remove a late immutable artifact"
            ),
        }
        Err(StateBackendError::Conflict {
            resource: Self::attempt_prefix(attempt),
            message: format!(
                "checkpoint epoch {} fell below durable state prune floor {} during publication",
                attempt.epoch, floor.floor.before_epoch
            ),
        })
    }

    async fn compare_and_swap_prune_floor(
        &self,
        floor: &StatePruneFloor,
        expected: Option<UpdateVersion>,
    ) -> Result<bool, StateBackendError> {
        let path = Self::prune_floor_path();
        let bytes = serde_json::to_vec(floor)
            .map(Bytes::from)
            .map_err(|error| StateBackendError::Serialization(error.to_string()))?;
        let options = PutOptions {
            mode: expected.clone().map_or(PutMode::Create, PutMode::Update),
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(bytes.clone()), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. }
                | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(object_store::Error::NotImplemented { .. })
                if expected.is_some()
                    && self.durability_scope != StateBackendDurability::ClusterShared =>
            {
                // `LocalFileSystem` provides atomic overwrite but no conditional update. Local
                // runtimes have one process owner for a state namespace, so serialize the
                // read/compare/overwrite within that owner. Cluster-shared storage must provide
                // native compare-and-swap and never takes this weaker path.
                let _guard = self.prune_floor_update_lock.lock().await;
                let current = self.read_prune_floor().await?;
                if current.as_ref().map(|value| &value.update_version) != expected.as_ref() {
                    return Ok(false);
                }
                let overwrite = PutOptions {
                    mode: PutMode::Overwrite,
                    ..PutOptions::default()
                };
                self.store
                    .put_opts(&path, PutPayload::from(bytes), overwrite)
                    .await
                    .map(|_| true)
                    .map_err(|error| StateBackendError::Io(error.to_string()))
            }
            Err(error) => match self.read_prune_floor().await? {
                Some(current)
                    if current.floor.before_epoch >= floor.before_epoch
                        && current.floor.swept_before_epoch >= floor.swept_before_epoch =>
                {
                    Ok(true)
                }
                _ => Err(StateBackendError::Io(error.to_string())),
            },
        }
    }

    async fn delete_retired_prefix(&self, prefix: &OsPath) -> Result<(), StateBackendError> {
        use futures::StreamExt;

        loop {
            // Materialize one bounded batch before mutating the prefix. LocalFileSystem resumes
            // WalkDir in chunks, and deleting from that live iterator can skip entries. Re-listing
            // from the prefix root also bounds memory for the maximum key-group topology.
            let mut entries = self.store.list(Some(prefix));
            let mut locations = Vec::with_capacity(STATE_PRUNE_DELETE_BATCH_SIZE);
            while locations.len() < STATE_PRUNE_DELETE_BATCH_SIZE {
                let Some(entry) = entries.next().await else {
                    break;
                };
                locations.push(
                    entry
                        .map_err(|error| StateBackendError::Io(error.to_string()))?
                        .location,
                );
            }
            drop(entries);
            if locations.is_empty() {
                if let Some(cleanup) = &self.empty_prefix_cleanup {
                    cleanup
                        .cleanup_empty_prefix(prefix)
                        .await
                        .map_err(|error| StateBackendError::Io(error.to_string()))?;
                }
                return Ok(());
            }

            let expected = locations.len();
            let input =
                futures::stream::iter(locations.into_iter().map(Ok::<_, object_store::Error>))
                    .boxed();
            let mut deletes = self.store.delete_stream(input);
            let mut completed = 0_usize;
            while let Some(result) = deletes.next().await {
                match result {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => {
                        completed += 1;
                    }
                    Err(error) => {
                        return Err(StateBackendError::Io(format!(
                            "state backend prune failed to delete an artifact: {error}"
                        )));
                    }
                }
            }
            if completed != expected {
                return Err(StateBackendError::Io(format!(
                    "state backend prune delete stream ended after {completed} of {expected} artifacts"
                )));
            }
            tokio::task::yield_now().await;
        }
    }
}

#[async_trait]
impl StateBackend for ObjectStoreBackend {
    fn key_group_capacity(&self) -> u32 {
        self.vnode_capacity
    }

    async fn bind_state_namespace(
        &self,
        deployment_id: &str,
        pipeline_identity: &PipelineIdentity,
    ) -> Result<(), StateBackendError> {
        let requested = StateNamespaceBinding::try_new(deployment_id, pipeline_identity)?;
        let path = Self::namespace_path();
        match self.store.get(&path).await {
            Ok(result) => {
                Self::check_namespace_marker_size(&path, result.meta.size)?;
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| StateBackendError::Io(error.to_string()))?;
                let existing = Self::decode_namespace_marker(&path, &bytes)?;
                return Self::verify_namespace_binding(&path, &existing, &requested);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(StateBackendError::Io(error.to_string())),
        }

        // No marker may claim an already-populated root. All conforming writers bind before state
        // I/O, so concurrent first binders can both observe an empty root; create-only publication
        // below chooses the winner and the loser compares that immutable winner.
        if self.preflight_unbound_state_root(&path, &requested).await? {
            return Ok(());
        }
        let bytes = Self::encode_namespace_marker(&requested)?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(bytes), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.read_namespace_binding(&path).await?;
                Self::verify_namespace_binding(&path, &existing, &requested)
            }
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    fn durability_scope(&self) -> StateBackendDurability {
        self.durability_scope
    }

    fn uses_exact_object_store(&self, expected: &Arc<dyn ObjectStore>) -> bool {
        Arc::ptr_eq(&self.store, expected)
    }

    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        lineage: VnodePartialLineage,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        self.check_vnode(vnode)?;
        self.check_assignment_version(assignment_version)?;
        let path = Self::partial_path(attempt, vnode);
        lineage
            .validate(attempt, bytes.len() as u64)
            .map_err(|message| StateBackendError::Conflict {
                resource: path.to_string(),
                message,
            })?;
        let bytes = Self::encode_partial(attempt, vnode, assignment_version, None, lineage, &bytes);
        self.put_live_immutable(attempt, &path, bytes).await
    }

    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        lineage: VnodePartialLineage,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        self.check_vnode(vnode)?;
        if !assignment_fence.is_canonical() {
            return Err(StateBackendError::Conflict {
                resource: Self::partial_path(attempt, vnode).to_string(),
                message: "assignment certificate is not canonical".into(),
            });
        }
        lineage
            .validate(attempt, bytes.len() as u64)
            .map_err(|message| StateBackendError::Conflict {
                resource: Self::partial_path(attempt, vnode).to_string(),
                message,
            })?;
        self.check_assignment_version(assignment_fence.assignment_version)?;
        let writer =
            SealedVnodeWriter::from_fence(assignment_fence, writer_node_id).ok_or_else(|| {
                StateBackendError::Conflict {
                    resource: Self::partial_path(attempt, vnode).to_string(),
                    message: "partial writer is absent from the canonical assignment certificate"
                        .into(),
                }
            })?;
        let encoded = Self::encode_partial(
            attempt,
            vnode,
            assignment_fence.assignment_version,
            Some(&writer),
            lineage,
            &bytes,
        );
        let path = Self::partial_path(attempt, vnode);
        self.put_live_immutable(attempt, &path, encoded).await
    }

    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.check_vnode(vnode)?;
        if self.attempt_is_pruned(attempt).await? {
            return Ok(None);
        }
        let path = Self::partial_path(attempt, vnode);
        match self.store.get(&path).await {
            Ok(res) => {
                let b = res
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                if self.attempt_is_pruned(attempt).await? {
                    Ok(None)
                } else {
                    Self::decode_partial(&b, attempt, vnode).map(Some)
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn read_sealed_partial_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &SealedVnodePartial,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        Self::ensure_canonical_attempt(attempt)?;
        self.check_vnode(sealed.vnode)?;
        let path = Self::partial_path(attempt, sealed.vnode);
        if sealed.payload_len > max_bytes {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "sealed vnode partial declares {} bytes; read bound is {max_bytes}",
                    sealed.payload_len
                ),
            });
        }
        if self.attempt_is_pruned(attempt).await? {
            return Ok(None);
        }
        match self.store.get(&path).await {
            Ok(result) => {
                let expected_object_bytes = (VNODE_PARTIAL_HEADER_LEN as u64)
                    .checked_add(sealed.payload_len)
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "sealed vnode partial length overflows object size".into(),
                    })?;
                if result.meta.size != expected_object_bytes {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "stored vnode partial is {} bytes; checkpoint seal requires \
                             {expected_object_bytes} bytes including its header",
                            result.meta.size
                        ),
                    });
                }
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| StateBackendError::Io(error.to_string()))?;
                if self.attempt_is_pruned(attempt).await? {
                    return Ok(None);
                }
                let (current, payload) =
                    Self::decode_partial_with_attestation(&bytes, attempt, sealed.vnode)?;
                if &current != sealed {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message:
                            "stored vnode partial attestation does not match the checkpoint seal"
                                .into(),
                    });
                }
                Ok(Some(payload))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let path = Self::descriptor_path(attempt, key);
        let authoritative = self.authoritative_version();
        if authoritative != 0 {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "uncertified commit descriptor write is disabled while assignment version \
                     {authoritative} is authoritative"
                ),
            });
        }
        let encoded = Self::encode_commit_descriptor(attempt, key, 0, None, &bytes);
        self.put_live_immutable(attempt, &path, encoded).await
    }

    async fn write_certified_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        leader_proof: &LeaderProof,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let path = Self::descriptor_path(attempt, key);
        if !assignment_fence.is_canonical() {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: "assignment certificate is not canonical".into(),
            });
        }
        self.check_assignment_version(assignment_fence.assignment_version)?;
        let writer = SealedCommitDescriptorWriter::from_fence(
            assignment_fence,
            writer_node_id,
            leader_proof,
        )
        .ok_or_else(|| StateBackendError::Conflict {
            resource: path.to_string(),
            message: "descriptor writer or leader is absent from the canonical assignment \
                      certificate"
                .into(),
        })?;
        let encoded = Self::encode_commit_descriptor(
            attempt,
            key,
            assignment_fence.assignment_version,
            Some(&writer),
            &bytes,
        );
        self.put_live_immutable(attempt, &path, encoded).await
    }

    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError> {
        self.read_commit_descriptor_bounded(attempt, key, u64::MAX)
            .await
    }

    async fn read_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        if self.attempt_is_pruned(attempt).await? {
            return Ok(None);
        }
        let path = Self::descriptor_path(attempt, key);
        match self.store.get(&path).await {
            Ok(result) => {
                let max_object_bytes =
                    (COMMIT_DESCRIPTOR_HEADER_LEN as u64).saturating_add(max_bytes);
                if result.meta.size > max_object_bytes {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "commit descriptor payload exceeds its read bound; read bound is \
                             {max_bytes} bytes (stored object is {} bytes including its {}-byte \
                             header)",
                            result.meta.size, COMMIT_DESCRIPTOR_HEADER_LEN
                        ),
                    });
                }
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| StateBackendError::Io(error.to_string()))?;
                if self.attempt_is_pruned(attempt).await? {
                    Ok(None)
                } else {
                    let payload = Self::decode_commit_descriptor(&bytes, attempt, key)?;
                    if payload.len() as u64 > max_bytes {
                        return Err(StateBackendError::Conflict {
                            resource: path.to_string(),
                            message: format!(
                                "commit descriptor payload is {} bytes; read bound is {max_bytes}",
                                payload.len()
                            ),
                        });
                    }
                    Ok(Some(payload))
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let path = Self::descriptor_path(attempt, &sealed.key);
        if sealed.payload_len > max_bytes {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "sealed commit descriptor declares {} bytes; read bound is {max_bytes}",
                    sealed.payload_len
                ),
            });
        }
        if self.attempt_is_pruned(attempt).await? {
            return Ok(None);
        }
        match self.store.get(&path).await {
            Ok(result) => {
                let expected_object_bytes = (COMMIT_DESCRIPTOR_HEADER_LEN as u64)
                    .checked_add(sealed.payload_len)
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "sealed commit descriptor length overflows object size".into(),
                    })?;
                if result.meta.size != expected_object_bytes {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "stored commit descriptor is {} bytes; checkpoint seal requires \
                             {expected_object_bytes} bytes including its header",
                            result.meta.size
                        ),
                    });
                }
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| StateBackendError::Io(error.to_string()))?;
                if self.attempt_is_pruned(attempt).await? {
                    return Ok(None);
                }
                let (current, payload) =
                    Self::decode_commit_descriptor_with_attestation(&bytes, attempt, &sealed.key)?;
                if &current != sealed {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "stored commit descriptor attestation does not match the checkpoint seal"
                            .into(),
                    });
                }
                Ok(Some(payload))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError> {
        use rustc_hash::FxHashSet;
        use tokio_stream::StreamExt;

        Self::ensure_canonical_attempt(attempt)?;
        let assignment_version = self.seal_assignment_version(attempt, assignment_fence)?;
        self.check_assignment_version(assignment_version)?;
        self.ensure_attempt_live(attempt).await?;
        let mut required_vnodes = vnodes.to_vec();
        required_vnodes.sort_unstable();
        required_vnodes.dedup();
        let mut required_descriptors = required_descriptors.to_vec();
        required_descriptors.sort_unstable();
        required_descriptors.dedup();
        if required_descriptors.iter().any(String::is_empty) {
            return Err(StateBackendError::Conflict {
                resource: Self::seal_path(attempt).to_string(),
                message: "checkpoint seal descriptor key cannot be empty".into(),
            });
        }
        let seal_path = Self::seal_path(attempt);
        match self.store.head(&seal_path).await {
            Ok(_) => {
                let existing = self.read_seal(&seal_path).await?;
                let expected = CheckpointSeal::new(
                    self.instance_id.clone(),
                    self.execution_id,
                    CheckpointSealInventory {
                        attempt,
                        assignment_fence: assignment_fence.cloned(),
                        assignment_version,
                        required_vnodes,
                        sealed_partials: existing.sealed_partials.clone(),
                        required_descriptors,
                        sealed_descriptors: existing.sealed_descriptors.clone(),
                    },
                );
                let result = if existing == expected {
                    Ok(true)
                } else {
                    Err(StateBackendError::Conflict {
                        resource: seal_path.to_string(),
                        message: "existing seal does not match this execution, assignment, or artifact inventory".into(),
                    })
                };
                self.ensure_attempt_live(attempt).await?;
                return result;
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(StateBackendError::Io(e.to_string())),
        }

        for &v in &required_vnodes {
            self.check_vnode(v)?;
        }

        // List once for presence only. Some providers do not populate object size in LIST;
        // descriptor length is checked from ranged-GET metadata below.
        let prefix = OsPath::from(Self::attempt_prefix(attempt));
        let mut entries = self.store.list(Some(&prefix));
        let mut found_objects: FxHashSet<OsPath> = FxHashSet::default();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| StateBackendError::Io(e.to_string()))?;
            found_objects.insert(entry.location);
        }

        for &v in &required_vnodes {
            let path = Self::partial_path(attempt, v);
            if !found_objects.contains(&path) {
                return Ok(false);
            }
        }
        // Commit descriptors live under this attempt's `commit/` prefix.
        for key in &required_descriptors {
            if !found_objects.contains(&Self::descriptor_path(attempt, key)) {
                return Ok(false);
            }
        }

        let Some(sealed_partials) = self
            .read_sealed_partials(
                attempt,
                &required_vnodes,
                assignment_version,
                assignment_fence,
            )
            .await?
        else {
            return Ok(false);
        };
        let Some(sealed_descriptors) = self
            .read_sealed_descriptors(attempt, &required_descriptors, assignment_fence)
            .await?
        else {
            return Ok(false);
        };

        let expected_seal = CheckpointSeal::new(
            self.instance_id.clone(),
            self.execution_id,
            CheckpointSealInventory {
                attempt,
                assignment_fence: assignment_fence.cloned(),
                assignment_version,
                required_vnodes,
                sealed_partials,
                required_descriptors,
                sealed_descriptors,
            },
        );
        expected_seal
            .validate()
            .map_err(|message| StateBackendError::Conflict {
                resource: seal_path.to_string(),
                message,
            })?;

        let encoded = serde_json::to_vec(&expected_seal)
            .map_err(|e| StateBackendError::Serialization(e.to_string()))?;
        Self::check_seal_encoded_size(&seal_path, encoded.len() as u64)?;
        let bytes = Bytes::from(encoded);
        self.put_live_immutable(attempt, &seal_path, bytes).await?;
        Ok(true)
    }

    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError> {
        if self.attempt_is_pruned(attempt).await? {
            return Ok(None);
        }
        let path = Self::seal_path(attempt);
        match self.store.get(&path).await {
            Ok(result) => {
                Self::check_seal_encoded_size(&path, result.meta.size)?;
                let bytes = match result.bytes().await {
                    Ok(bytes) => bytes,
                    Err(object_store::Error::NotFound { .. }) => return Ok(None),
                    Err(error) => return Err(StateBackendError::Io(error.to_string())),
                };
                if self.attempt_is_pruned(attempt).await? {
                    return Ok(None);
                }
                let seal = Self::decode_seal(&bytes)?;
                if seal.attempt != attempt {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "seal body names {:?}, requested {attempt:?}",
                            seal.attempt
                        ),
                    });
                }
                Ok(Some(seal.inventory()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(StateBackendError::Io(error.to_string())),
        }
    }

    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &CheckpointSealInventory,
    ) -> Result<(), StateBackendError> {
        use futures::StreamExt as _;

        let attempt = inventory.attempt;
        if self.attempt_is_pruned(attempt).await? {
            return Err(StateBackendError::Conflict {
                resource: Self::attempt_prefix(attempt),
                message: "sealed attempt is below the durable state prune floor".into(),
            });
        }

        let prefix = OsPath::from(Self::attempt_prefix(attempt));
        let mut objects = self.store.list(Some(&prefix));
        let mut listed_sizes = rustc_hash::FxHashMap::default();
        while let Some(entry) = objects.next().await {
            let entry = entry.map_err(|error| StateBackendError::Io(error.to_string()))?;
            listed_sizes.insert(entry.location, entry.size);
        }

        for partial in &inventory.sealed_partials {
            let path = Self::partial_path(attempt, partial.vnode);
            let header_len = u64::try_from(VNODE_PARTIAL_HEADER_LEN).map_err(|_| {
                StateBackendError::Conflict {
                    resource: path.to_string(),
                    message: "vnode storage header length is not representable".into(),
                }
            })?;
            let expected_size = header_len.checked_add(partial.payload_len).ok_or_else(|| {
                StateBackendError::Conflict {
                    resource: path.to_string(),
                    message: "sealed vnode partial length overflows storage size".into(),
                }
            })?;
            self.verify_object_size_from_metadata(
                &path,
                listed_sizes.get(&path).copied(),
                expected_size,
            )
            .await?;
        }
        for descriptor in &inventory.sealed_descriptors {
            let path = Self::descriptor_path(attempt, &descriptor.key);
            let header_len = u64::try_from(COMMIT_DESCRIPTOR_HEADER_LEN).map_err(|_| {
                StateBackendError::Conflict {
                    resource: path.to_string(),
                    message: "descriptor storage header length is not representable".into(),
                }
            })?;
            let expected_size =
                header_len
                    .checked_add(descriptor.payload_len)
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "sealed commit descriptor length overflows storage size".into(),
                    })?;
            self.verify_object_size_from_metadata(
                &path,
                listed_sizes.get(&path).copied(),
                expected_size,
            )
            .await?;
        }

        if self.attempt_is_pruned(attempt).await? {
            return Err(StateBackendError::Conflict {
                resource: Self::attempt_prefix(attempt),
                message: "sealed attempt was pruned during metadata verification".into(),
            });
        }
        Ok(())
    }

    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError> {
        if before == 0 {
            return Ok(());
        }

        // Publish the correctness boundary first. A failed or interrupted sweep leaves garbage,
        // never a readable checkpoint, and the durable sweep cursor makes the next caller repair
        // the exact unfinished range.
        loop {
            let observed = self.read_prune_floor().await?;
            if observed
                .as_ref()
                .is_some_and(|current| current.floor.before_epoch >= before)
            {
                break;
            }
            let floor = StatePruneFloor {
                version: STATE_PRUNE_FLOOR_VERSION,
                before_epoch: before,
                swept_before_epoch: observed
                    .as_ref()
                    .map_or(0, |current| current.floor.swept_before_epoch),
            };
            let expected = observed.map(|current| current.update_version);
            if self.compare_and_swap_prune_floor(&floor, expected).await? {
                break;
            }
            tokio::task::yield_now().await;
        }

        'sweep: loop {
            let mut current =
                self.read_prune_floor()
                    .await?
                    .ok_or_else(|| StateBackendError::Conflict {
                        resource: Self::prune_floor_path().to_string(),
                        message: "state prune floor disappeared after publication".into(),
                    })?;
            let target = current.floor.before_epoch;

            // Discover materialized epochs rather than issuing one LIST for every numeric ID in
            // the retired range. Sparse checkpoint IDs are normal after allocation failures and
            // can otherwise turn one retention pass into tens of thousands of remote requests.
            // Revisit prefixes below the durable cursor too: a writer whose publication raced the
            // floor can leave garbage after an ambiguously failed cleanup, even though readers
            // already reject it.
            let state_root = OsPath::from("state-v2");
            let discovered = self
                .store
                .list_with_delimiter(Some(&state_root))
                .await
                .map_err(|error| StateBackendError::Io(error.to_string()))?;
            let mut retired_prefixes = discovered
                .common_prefixes
                .into_iter()
                .filter_map(|prefix| {
                    let epoch = Self::epoch_from_prefix(&prefix)?;
                    (epoch < target).then_some((epoch, prefix))
                })
                .collect::<Vec<_>>();
            retired_prefixes.sort_unstable_by_key(|(epoch, _)| *epoch);

            for (epoch, prefix) in retired_prefixes {
                self.delete_retired_prefix(&prefix).await?;

                // The numeric cursor can represent completion of a sparse materialized prefix:
                // every lower prefix was absent from the same delimiter snapshot or was already
                // swept. Publishing after deletion makes a crash resume at this prefix unless the
                // entire prefix was removed.
                let next = epoch.saturating_add(1).min(target);
                if current.floor.swept_before_epoch < next {
                    let swept = StatePruneFloor {
                        swept_before_epoch: next,
                        ..current.floor.clone()
                    };
                    if !self
                        .compare_and_swap_prune_floor(&swept, Some(current.update_version.clone()))
                        .await?
                    {
                        tokio::task::yield_now().await;
                        continue 'sweep;
                    }
                    current = self.read_prune_floor().await?.ok_or_else(|| {
                        StateBackendError::Conflict {
                            resource: Self::prune_floor_path().to_string(),
                            message: "state prune floor disappeared after sweep progress".into(),
                        }
                    })?;
                    if current.floor.before_epoch != target {
                        continue 'sweep;
                    }
                }
            }

            if current.floor.swept_before_epoch >= target {
                return Ok(());
            }
            let swept = StatePruneFloor {
                swept_before_epoch: target,
                ..current.floor.clone()
            };
            if self
                .compare_and_swap_prune_floor(&swept, Some(current.update_version.clone()))
                .await?
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    fn set_authoritative_version(&self, version: u64) {
        // CAS loop avoids lowering the version on a late call.
        let mut cur = self.authoritative_version.load(Ordering::Acquire);
        while version > cur {
            match self.authoritative_version.compare_exchange(
                cur,
                version,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => cur = observed,
            }
        }
    }

    fn authoritative_version(&self) -> u64 {
        self.authoritative_version.load(Ordering::Acquire)
    }
}

impl ObjectStoreBackend {
    async fn read_sealed_partials(
        &self,
        attempt: CheckpointAttempt,
        required_vnodes: &[u32],
        assignment_version: u64,
        assignment_fence: Option<&CheckpointAssignmentFence>,
    ) -> Result<Option<Vec<SealedVnodePartial>>, StateBackendError> {
        let mut sealed_partials = Vec::with_capacity(required_vnodes.len());
        for chunk in required_vnodes.chunks(PARTIAL_ATTESTATION_READ_CONCURRENCY) {
            let attestations = futures::future::try_join_all(
                chunk
                    .iter()
                    .map(|&vnode| self.read_partial_attestation(attempt, vnode)),
            )
            .await?;
            for attestation in attestations {
                let Some(attestation) = attestation else {
                    return Ok(None);
                };
                if attestation.assignment_version != assignment_version {
                    return Err(StateBackendError::Conflict {
                        resource: Self::partial_path(attempt, attestation.vnode).to_string(),
                        message: format!(
                            "partial assignment version {} cannot satisfy seal version {assignment_version}",
                            attestation.assignment_version
                        ),
                    });
                }
                match (assignment_fence, &attestation.writer) {
                    (Some(fence), Some(writer)) if writer.matches_fence(fence) => {}
                    (None, None) => {}
                    _ => {
                        return Err(StateBackendError::Conflict {
                            resource: Self::partial_path(attempt, attestation.vnode).to_string(),
                            message: "partial writer certificate does not match the exact seal assignment"
                                .into(),
                        });
                    }
                }
                sealed_partials.push(attestation);
            }
        }
        Ok(Some(sealed_partials))
    }

    async fn read_sealed_descriptors(
        &self,
        attempt: CheckpointAttempt,
        required_descriptors: &[String],
        assignment_fence: Option<&CheckpointAssignmentFence>,
    ) -> Result<Option<Vec<SealedCommitDescriptor>>, StateBackendError> {
        let mut sealed_descriptors = Vec::with_capacity(required_descriptors.len());
        for chunk in required_descriptors.chunks(DESCRIPTOR_ATTESTATION_READ_CONCURRENCY) {
            let attestations = futures::future::try_join_all(
                chunk
                    .iter()
                    .map(|key| self.read_commit_descriptor_attestation(attempt, key)),
            )
            .await?;
            for (key, attestation) in chunk.iter().zip(attestations) {
                let Some(attestation) = attestation else {
                    return Ok(None);
                };
                let path = Self::descriptor_path(attempt, key);
                match (assignment_fence, &attestation.writer) {
                    (Some(fence), Some(writer))
                        if attestation.assignment_version == fence.assignment_version
                            && writer.matches_fence(fence) => {}
                    (None, None) if attestation.assignment_version == 0 => {}
                    _ => {
                        return Err(StateBackendError::Conflict {
                            resource: path.to_string(),
                            message: "descriptor writer certificate does not match the exact seal \
                                      assignment"
                                .into(),
                        });
                    }
                }
                sealed_descriptors.push(attestation);
            }
        }

        Ok(Some(sealed_descriptors))
    }

    fn seal_assignment_version(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&CheckpointAssignmentFence>,
    ) -> Result<u64, StateBackendError> {
        if assignment_fence.is_some_and(|fence| !fence.is_canonical()) {
            return Err(StateBackendError::Conflict {
                resource: Self::seal_path(attempt).to_string(),
                message: "assignment certificate is not canonical".into(),
            });
        }
        Ok(assignment_fence.map_or_else(
            || self.authoritative_version(),
            |fence| fence.assignment_version,
        ))
    }

    fn check_assignment_version(&self, caller: u64) -> Result<(), StateBackendError> {
        let authoritative = self.authoritative_version.load(Ordering::Acquire);
        if authoritative == 0 || caller == authoritative {
            return Ok(());
        }
        if caller < authoritative {
            return Err(StateBackendError::StaleVersion {
                caller,
                authoritative,
            });
        }
        Err(StateBackendError::FutureVersion {
            caller,
            authoritative,
        })
    }

    fn check_seal_encoded_size(path: &OsPath, size: u64) -> Result<(), StateBackendError> {
        if size > MAX_CHECKPOINT_SEAL_BYTES {
            return Err(StateBackendError::Conflict {
                resource: path.to_string(),
                message: format!(
                    "checkpoint seal is {size} bytes; maximum is {MAX_CHECKPOINT_SEAL_BYTES}"
                ),
            });
        }
        Ok(())
    }

    /// CAS-create immutable bytes. A retry of the exact bytes succeeds; a
    /// different payload at the same key is a hard conflict.
    async fn put_immutable(&self, path: &OsPath, bytes: Bytes) -> Result<(), StateBackendError> {
        let intended_size = bytes.len() as u64;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(path, PutPayload::from(bytes.clone()), opts)
            .await
        {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let result = self
                    .store
                    .get(path)
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                if result.meta.size != intended_size {
                    return Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: format!(
                            "existing immutable artifact is {} bytes; retry is {intended_size} bytes",
                            result.meta.size
                        ),
                    });
                }
                let existing = result
                    .bytes()
                    .await
                    .map_err(|e| StateBackendError::Io(e.to_string()))?;
                if existing == bytes {
                    Ok(())
                } else {
                    Err(StateBackendError::Conflict {
                        resource: path.to_string(),
                        message: "existing immutable artifact has different bytes".into(),
                    })
                }
            }
            Err(e) => Err(StateBackendError::Io(e.to_string())),
        }
    }

    async fn read_seal_if_present(
        &self,
        path: &OsPath,
    ) -> Result<Option<CheckpointSeal>, StateBackendError> {
        let result = match self.store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(StateBackendError::Io(error.to_string())),
        };
        Self::check_seal_encoded_size(path, result.meta.size)?;
        let bytes = match result.bytes().await {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(StateBackendError::Io(error.to_string())),
        };
        Self::decode_seal(&bytes).map(Some)
    }

    async fn read_seal(&self, path: &OsPath) -> Result<CheckpointSeal, StateBackendError> {
        self.read_seal_if_present(path).await?.ok_or_else(|| {
            StateBackendError::Io(format!("checkpoint seal '{}' is absent", path.as_ref()))
        })
    }

    fn decode_seal(bytes: &[u8]) -> Result<CheckpointSeal, StateBackendError> {
        let seal: CheckpointSeal = serde_json::from_slice(bytes).map_err(|e| {
            StateBackendError::Serialization(format!("invalid checkpoint seal: {e}"))
        })?;
        if seal.version != CHECKPOINT_SEAL_VERSION {
            return Err(StateBackendError::Serialization(format!(
                "unsupported checkpoint seal version {}; expected {CHECKPOINT_SEAL_VERSION}",
                seal.version
            )));
        }
        seal.validate().map_err(|error| {
            StateBackendError::Serialization(format!("invalid checkpoint seal: {error}"))
        })?;
        Ok(seal)
    }
}

#[cfg(test)]
mod tests;
