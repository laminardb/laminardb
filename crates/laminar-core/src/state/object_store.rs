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
    CHECKPOINT_SEAL_VERSION, STATE_NAMESPACE_RESOURCE,
};

const VNODE_PARTIAL_MAGIC: &[u8; 8] = b"LDBVP2\0\0";
const VNODE_PARTIAL_VERSION: u32 = 2;
const VNODE_PARTIAL_HEADER_LEN: usize = 136;
const PARTIAL_ATTESTATION_READ_CONCURRENCY: usize = 32;
const COMMIT_DESCRIPTOR_MAGIC: &[u8; 8] = b"LDBCD2\0\0";
const COMMIT_DESCRIPTOR_VERSION: u32 = 2;
const COMMIT_DESCRIPTOR_HEADER_LEN: usize = 204;
const DESCRIPTOR_ATTESTATION_READ_CONCURRENCY: usize = 32;
const STATE_PRUNE_FLOOR_VERSION: u32 = 1;
const STATE_PRUNE_FLOOR_MAX_BYTES: u64 = 512;
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
        while let Some(result) = objects.next().await {
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
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        writer: Option<&SealedVnodeWriter>,
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
        Ok(SealedVnodePartial {
            vnode,
            assignment_version,
            writer,
            payload_len,
            payload_sha256: digest_hex(&payload_digest),
        })
    }

    fn decode_partial(
        bytes: &Bytes,
        expected_attempt: CheckpointAttempt,
        expected_vnode: u32,
    ) -> Result<Bytes, StateBackendError> {
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
        if payload.is_empty() || payload.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return Ok(payload);
        }

        // Object-store clients may expose a view into an arbitrarily aligned network buffer.
        // Normalize it once here so recovery-chain consumers can validate and decode the rkyv
        // payload repeatedly without making a fresh aligned copy on every pass.
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(payload.len());
        aligned.extend_from_slice(&payload);
        Ok(Bytes::from_owner(aligned))
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
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        self.check_assignment_version(assignment_version)?;
        let path = Self::partial_path(attempt, vnode);
        let bytes = self.encode_partial(attempt, vnode, assignment_version, None, &bytes);
        self.put_live_immutable(attempt, &path, bytes).await
    }

    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        self.check_vnode(vnode)?;
        if !assignment_fence.is_canonical() {
            return Err(StateBackendError::Conflict {
                resource: Self::partial_path(attempt, vnode).to_string(),
                message: "assignment certificate is not canonical".into(),
            });
        }
        self.check_assignment_version(assignment_fence.assignment_version)?;
        let writer =
            SealedVnodeWriter::from_fence(assignment_fence, writer_node_id).ok_or_else(|| {
                StateBackendError::Conflict {
                    resource: Self::partial_path(attempt, vnode).to_string(),
                    message: "partial writer is absent from the canonical assignment certificate"
                        .into(),
                }
            })?;
        let encoded = self.encode_partial(
            attempt,
            vnode,
            assignment_fence.assignment_version,
            Some(&writer),
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
                let max_object_bytes = (COMMIT_DESCRIPTOR_HEADER_LEN as u64)
                    .checked_add(max_bytes)
                    .unwrap_or(u64::MAX);
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
        use futures::StreamExt;

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
                // Feed one prefix directly into bounded object-store deletion. Never retain every
                // artifact location for the whole sweep in memory.
                let locations = self
                    .store
                    .list(Some(&prefix))
                    .map(|entry| entry.map(|metadata| metadata.location))
                    .boxed();
                let mut deletes = self.store.delete_stream(locations);
                while let Some(result) = deletes.next().await {
                    match result {
                        Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                        Err(error) => {
                            return Err(StateBackendError::Io(format!(
                                "state backend prune failed to delete an artifact: {error}"
                            )));
                        }
                    }
                }

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
mod tests {
    use super::*;
    use futures::StreamExt as _;

    /// Records retention publication and deletion order while delegating storage to the real
    /// local backend.
    struct RetentionLogStore {
        inner: Arc<dyn ObjectStore>,
        operations: Arc<parking_lot::Mutex<Vec<String>>>,
        delete_calls: Arc<AtomicU64>,
        fail_delete_call: u64,
    }

    impl std::fmt::Debug for RetentionLogStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RetentionLogStore").finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for RetentionLogStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RetentionLogStore")
        }
    }

    #[async_trait]
    impl ObjectStore for RetentionLogStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location == &ObjectStoreBackend::prune_floor_path() {
                self.operations
                    .lock()
                    .push(format!("floor:{:?}", opts.mode));
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            let inner = Arc::clone(&self.inner);
            let operations = Arc::clone(&self.operations);
            let delete_calls = Arc::clone(&self.delete_calls);
            let fail_delete_call = self.fail_delete_call;
            locations
                .then(move |location| {
                    let inner = Arc::clone(&inner);
                    let operations = Arc::clone(&operations);
                    let delete_calls = Arc::clone(&delete_calls);
                    async move {
                        let location = location?;
                        operations.lock().push(format!("delete:{location}"));
                        let delete_call = delete_calls.fetch_add(1, Ordering::AcqRel) + 1;
                        if delete_call == fail_delete_call {
                            return Err(object_store::Error::Generic {
                                store: "retention-test",
                                source: Box::new(std::io::Error::other("injected delete failure")),
                            });
                        }
                        inner.delete(&location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.operations.lock().push(format!(
                "list:{}",
                prefix.map_or("<root>", |path| path.as_ref())
            ));
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.operations.lock().push(format!(
                "delimiter:{}",
                prefix.map_or("<root>", |path| path.as_ref())
            ));
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Pauses the first create of one seal immediately before it reaches shared storage.
    struct SealPublishGateStore {
        inner: Arc<dyn ObjectStore>,
        seal_path: OsPath,
        gated: std::sync::atomic::AtomicBool,
        reached: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl std::fmt::Debug for SealPublishGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SealPublishGateStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for SealPublishGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("SealPublishGateStore")
        }
    }

    #[async_trait]
    impl ObjectStore for SealPublishGateStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location == &self.seal_path
                && matches!(&opts.mode, PutMode::Create)
                && self
                    .gated
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.reached.add_permits(1);
                self.release
                    .acquire()
                    .await
                    .expect("test gate remains open")
                    .forget();
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn attempt(checkpoint_id: u64) -> CheckpointAttempt {
        CheckpointAttempt::canonical(checkpoint_id)
    }

    fn assignment_fence(version: u64, vnode_count: usize) -> CheckpointAssignmentFence {
        CheckpointAssignmentFence::from_owner_map(
            version,
            &vec![1; vnode_count],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            }],
        )
        .unwrap()
    }

    fn leader_proof(node_id: u64, boot_id: uuid::Uuid, token: u64) -> LeaderProof {
        LeaderProof {
            owner: LeaderProofOwner {
                node_id,
                boot_id,
                process_term: token,
            },
            fencing_token: token,
        }
    }
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;

    fn make_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    fn pipeline_identity(byte: u8) -> PipelineIdentity {
        PipelineIdentity {
            canonical_version: crate::checkpoint::PIPELINE_IDENTITY_VERSION,
            sha256: format!("{byte:02x}").repeat(32),
        }
    }

    #[tokio::test]
    async fn namespace_binding_is_atomic_and_idempotent() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let deployment = uuid::Uuid::from_u128(1).to_string();
        let identity = pipeline_identity(0x11);
        let first = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);
        first
            .bind_state_namespace(&deployment, &identity)
            .await
            .unwrap();

        let restarted = ObjectStoreBackend::node_durable(store, "node-0", 1);
        restarted
            .bind_state_namespace(&deployment, &identity)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_first_namespace_binders_accept_the_same_identity() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let deployment = uuid::Uuid::from_u128(1).to_string();
        let identity = pipeline_identity(0x11);
        let first = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);
        let second = ObjectStoreBackend::node_durable(store, "node-1", 1);

        let (first_result, second_result) = tokio::join!(
            first.bind_state_namespace(&deployment, &identity),
            second.bind_state_namespace(&deployment, &identity),
        );

        first_result.unwrap();
        second_result.unwrap();
    }

    #[tokio::test]
    async fn namespace_binding_rejects_an_unbound_nonempty_state_root() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let legacy_path = OsPath::from("state-v2/epoch=1/checkpoint=1/vnode=0/partial.bin");
        store
            .put(
                &legacy_path,
                PutPayload::from(Bytes::from_static(b"legacy")),
            )
            .await
            .unwrap();
        let backend = ObjectStoreBackend::node_durable(Arc::clone(&store), "node-0", 1);

        let error = backend
            .bind_state_namespace(
                &uuid::Uuid::from_u128(1).to_string(),
                &pipeline_identity(0x11),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("contains unbound artifact"));
        assert!(matches!(
            store.get(&ObjectStoreBackend::namespace_path()).await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    #[tokio::test]
    async fn namespace_binding_rejects_deployment_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
        let identity = pipeline_identity(0x11);
        backend
            .bind_state_namespace(&uuid::Uuid::from_u128(1).to_string(), &identity)
            .await
            .unwrap();

        let error = backend
            .bind_state_namespace(&uuid::Uuid::from_u128(2).to_string(), &identity)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("belongs to deployment"));
    }

    #[tokio::test]
    async fn namespace_binding_rejects_pipeline_mismatch() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
        let deployment = uuid::Uuid::from_u128(1).to_string();
        backend
            .bind_state_namespace(&deployment, &pipeline_identity(0x11))
            .await
            .unwrap();

        let error = backend
            .bind_state_namespace(&deployment, &pipeline_identity(0x22))
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("pipeline identity"));
    }

    #[tokio::test]
    async fn namespace_binding_rejects_malformed_and_oversized_markers() {
        for (poison, expected) in [
            (Bytes::from_static(b"{"), "malformed"),
            (
                Bytes::from(vec![b'x'; STATE_NAMESPACE_MAX_BYTES as usize + 1]),
                "expected 1..=",
            ),
        ] {
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            store
                .put(
                    &ObjectStoreBackend::namespace_path(),
                    PutPayload::from(poison),
                )
                .await
                .unwrap();
            let backend = ObjectStoreBackend::node_durable(store, "node-0", 1);
            let error = backend
                .bind_state_namespace(
                    &uuid::Uuid::from_u128(1).to_string(),
                    &pipeline_identity(0x11),
                )
                .await
                .unwrap_err();
            assert!(matches!(error, StateBackendError::Conflict { .. }));
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"hello"))
            .await
            .unwrap();
        let got = backend.read_partial(attempt(1), 0).await.unwrap().unwrap();
        assert_eq!(&got[..], b"hello");
    }

    #[tokio::test]
    async fn noncanonical_attempt_is_rejected_before_object_creation() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);
        let invalid = CheckpointAttempt::new(1, 2);

        let error = backend
            .write_partial(invalid, 0, 0, Bytes::from_static(b"invalid"))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("canonical checkpoint ID"));
        assert!(matches!(
            store
                .head(&ObjectStoreBackend::partial_path(invalid, 0))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(backend.read_partial(invalid, 0).await.is_err());
    }

    #[test]
    fn decode_partial_realigns_an_unaligned_transport_buffer() {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        let backend =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-0", 4);
        let checkpoint = attempt(1);
        let payload = Bytes::from_static(b"archived vnode state");
        let encoded = backend.encode_partial(checkpoint, 0, 0, None, &payload);

        let mut transport = bytes::BytesMut::zeroed(encoded.len() + ARCHIVE_ALIGNMENT);
        let offset = (0..ARCHIVE_ALIGNMENT)
            .find(|offset| {
                !(transport.as_ptr() as usize + offset + VNODE_PARTIAL_HEADER_LEN)
                    .is_multiple_of(ARCHIVE_ALIGNMENT)
            })
            .expect("an unaligned offset exists");
        transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let transport = transport.freeze().slice(offset..offset + encoded.len());
        assert!(!(transport[VNODE_PARTIAL_HEADER_LEN..].as_ptr() as usize)
            .is_multiple_of(ARCHIVE_ALIGNMENT));

        let decoded = ObjectStoreBackend::decode_partial(&transport, checkpoint, 0).unwrap();
        assert_eq!(decoded, payload);
        assert!((decoded.as_ptr() as usize).is_multiple_of(ARCHIVE_ALIGNMENT));
    }

    #[tokio::test]
    async fn immutable_artifact_accepts_identical_retry_and_rejects_conflict() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let checkpoint = attempt(1);
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"first"))
            .await
            .unwrap();
        assert!(matches!(
            backend
                .write_partial(checkpoint, 0, 0, Bytes::from_static(b"different"))
                .await,
            Err(StateBackendError::Conflict { .. })
        ));
        assert_eq!(
            backend.read_partial(checkpoint, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"first")
        );
    }

    #[tokio::test]
    async fn immutable_retry_rejects_size_poison_from_metadata() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let path = OsPath::from("immutable-poison");
        store
            .put(
                &path,
                PutPayload::from(Bytes::from_static(b"oversized-poison")),
            )
            .await
            .unwrap();
        let backend = ObjectStoreBackend::new(store, "node-0", 1);

        let error = backend
            .put_immutable(&path, Bytes::from_static(b"retry"))
            .await
            .unwrap_err();

        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error
            .to_string()
            .contains("existing immutable artifact is 16 bytes; retry is 5 bytes"));
    }

    #[test]
    fn checkpoint_seal_size_ceiling_covers_the_maximum_vnode_inventory() {
        assert!(MAX_CHECKPOINT_SEAL_BYTES >= u64::from(crate::state::MAX_KEY_GROUP_COUNT) * 768);
        let path = ObjectStoreBackend::seal_path(attempt(1));
        ObjectStoreBackend::check_seal_encoded_size(&path, MAX_CHECKPOINT_SEAL_BYTES).unwrap();

        let error =
            ObjectStoreBackend::check_seal_encoded_size(&path, MAX_CHECKPOINT_SEAL_BYTES + 1)
                .unwrap_err();

        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("checkpoint seal is"));
    }

    #[tokio::test]
    async fn oversized_seal_poison_is_rejected_from_metadata_on_read_and_retry() {
        let dir = tempdir().unwrap();
        let checkpoint = attempt(1);
        let object_path = ObjectStoreBackend::seal_path(checkpoint);
        let filesystem_path = dir.path().join(object_path.as_ref());
        std::fs::create_dir_all(filesystem_path.parent().unwrap()).unwrap();
        std::fs::File::create(&filesystem_path)
            .unwrap()
            .set_len(MAX_CHECKPOINT_SEAL_BYTES + 1)
            .unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 1);

        let read_error = backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap_err();
        let retry_error = backend
            .seal_checkpoint(checkpoint, None, &[], &[])
            .await
            .unwrap_err();

        for error in [read_error, retry_error] {
            assert!(matches!(error, StateBackendError::Conflict { .. }));
            assert!(error.to_string().contains("checkpoint seal is"));
        }
    }

    #[tokio::test]
    async fn checkpoint_attempts_are_isolated() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let old = CheckpointAttempt::canonical(5);
        let new = CheckpointAttempt::canonical(99);
        backend
            .write_partial(old, 0, 0, Bytes::from_static(b"old"))
            .await
            .unwrap();
        backend
            .write_partial(new, 0, 0, Bytes::from_static(b"new"))
            .await
            .unwrap();
        assert_eq!(
            backend.read_partial(old, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"old")
        );
        assert_eq!(
            backend.read_partial(new, 0).await.unwrap().unwrap(),
            Bytes::from_static(b"new")
        );
    }

    #[tokio::test]
    async fn seal_checkpoint_cas_is_idempotent_for_same_execution() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32, 1, 2];

        assert!(!backend
            .seal_checkpoint(attempt(1), None, &vnodes, &[])
            .await
            .unwrap());
        for v in &vnodes {
            backend
                .write_partial(attempt(1), *v, 0, Bytes::from_static(b"y"))
                .await
                .unwrap();
        }
        assert!(backend
            .seal_checkpoint(attempt(1), None, &vnodes, &[])
            .await
            .unwrap());
        // Idempotent — same committer id in the audit body.
        assert!(backend
            .seal_checkpoint(attempt(1), None, &vnodes, &[])
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn sealed_artifact_metadata_rejects_a_missing_or_wrong_sized_vnode_object() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 1);
        let checkpoint = attempt(1);
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, None, &[0], &[])
            .await
            .unwrap());
        let inventory = backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap();
        backend
            .verify_checkpoint_artifact_metadata(&inventory)
            .await
            .unwrap();

        let path = ObjectStoreBackend::partial_path(checkpoint, 0);
        store.delete(&path).await.unwrap();
        let missing = backend
            .verify_checkpoint_artifact_metadata(&inventory)
            .await
            .unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("sealed artifact is missing from storage metadata"),
            "{missing}"
        );

        store
            .put(&path, PutPayload::from(Bytes::from_static(b"wrong")))
            .await
            .unwrap();
        let wrong_size = backend
            .verify_checkpoint_artifact_metadata(&inventory)
            .await
            .unwrap_err();
        assert!(
            wrong_size
                .to_string()
                .contains("sealed artifact is 5 bytes in storage metadata"),
            "{wrong_size}"
        );
    }

    #[tokio::test]
    async fn node_durable_seal_uses_local_authoritative_assignment_without_cluster_fence() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::node_durable(make_store(dir.path()), "node-0", 2);
        let checkpoint = attempt(1);
        backend.set_authoritative_version(2);
        backend
            .write_partial(checkpoint, 0, 2, Bytes::from_static(b"state"))
            .await
            .unwrap();

        assert!(backend
            .seal_checkpoint(checkpoint, None, &[0], &[])
            .await
            .unwrap());
        let inventory = backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inventory.sealed_partials[0].assignment_version, 2);
    }

    #[tokio::test]
    async fn seal_body_binds_attempt_writer_fence_and_artifact_inventory() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "stable-node", 2);
        let checkpoint = CheckpointAttempt::canonical(401);
        let fence = assignment_fence(7, 2);
        backend.set_authoritative_version(7);
        backend
            .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
            .await
            .unwrap();
        let descriptors = ["participant=7/sink=orders".to_string()];
        let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
        backend
            .write_certified_commit_descriptor(
                checkpoint,
                &descriptors[0],
                &fence,
                1,
                &authority,
                Bytes::from_static(b"marker"),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, Some(&fence), &[0], &descriptors)
            .await
            .unwrap());

        let bytes = store
            .get(&ObjectStoreBackend::seal_path(checkpoint))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let seal: CheckpointSeal = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(seal.version, CHECKPOINT_SEAL_VERSION);
        assert_eq!(seal.attempt, checkpoint);
        assert_eq!(seal.instance_id, "stable-node");
        assert_eq!(seal.execution_id, backend.execution_id());
        assert_eq!(seal.assignment_fence.as_ref(), Some(&fence));
        assert_eq!(seal.required_vnodes, vec![0]);
        assert_eq!(seal.sealed_partials.len(), 1);
        assert_eq!(seal.sealed_partials[0].vnode, 0);
        assert_eq!(seal.sealed_partials[0].assignment_version, 7);
        assert_eq!(
            seal.sealed_partials[0]
                .writer
                .as_ref()
                .map(|writer| (writer.node_id, writer.boot_incarnation)),
            Some((1, uuid::Uuid::from_u128(1)))
        );
        assert_eq!(
            seal.sealed_partials[0]
                .writer
                .as_ref()
                .map(|writer| writer.assignment_certificate_digest),
            Some(fence.digest())
        );
        assert_eq!(seal.sealed_partials[0].payload_len, 5);
        assert_eq!(seal.required_descriptors, descriptors);
        assert_eq!(seal.sealed_descriptors.len(), 1);
        assert_eq!(seal.sealed_descriptors[0].key, descriptors[0]);
        assert_eq!(seal.sealed_descriptors[0].assignment_version, 7);
        assert_eq!(seal.sealed_descriptors[0].payload_len, 6);
        assert_eq!(
            seal.sealed_descriptors[0]
                .writer
                .as_ref()
                .map(|writer| &writer.leader_proof),
            Some(&authority)
        );
        assert_eq!(
            backend
                .checkpoint_seal_inventory(checkpoint)
                .await
                .unwrap()
                .unwrap(),
            seal.inventory()
        );

        let error = backend
            .seal_checkpoint(checkpoint, Some(&fence), &[0, 1], &descriptors)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
    }

    #[tokio::test]
    async fn seal_checkpoint_requires_commit_descriptors() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);
        let vnodes = [0u32];
        let key = "node=node-0/sink=ice";
        let need = [key.to_string()];

        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"s"))
            .await
            .unwrap();
        // Partial present but the descriptor is missing → epoch not sealed.
        assert!(!backend
            .seal_checkpoint(attempt(1), None, &vnodes, &need)
            .await
            .unwrap());

        backend
            .write_commit_descriptor(attempt(1), key, Bytes::from_static(b"df"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt(1), None, &vnodes, &need)
            .await
            .unwrap());

        let inventory = backend
            .checkpoint_seal_inventory(attempt(1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inventory.required_descriptors, need);
        assert_eq!(inventory.sealed_descriptors.len(), 1);
        assert_eq!(inventory.sealed_descriptors[0].key, key);
        assert_eq!(inventory.sealed_descriptors[0].assignment_version, 0);
        assert_eq!(inventory.sealed_descriptors[0].writer, None);
        assert_eq!(inventory.sealed_descriptors[0].payload_len, 2);
        assert_eq!(
            inventory.sealed_descriptors[0].payload_sha256,
            digest_hex(&sha256(b"df"))
        );
        assert_eq!(
            backend
                .read_commit_descriptor(attempt(1), key)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"df"))
        );
    }

    #[tokio::test]
    async fn bounded_descriptor_read_rejects_from_object_metadata() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(store, "node-0", 1);
        let checkpoint = attempt(1);
        backend
            .write_commit_descriptor(
                checkpoint,
                "ready",
                Bytes::from_static(b"oversized-control-record"),
            )
            .await
            .unwrap();

        let error = backend
            .read_commit_descriptor_bounded(checkpoint, "ready", 8)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("read bound is 8"));
    }

    #[tokio::test]
    async fn sealed_descriptor_read_rejects_a_valid_replacement_envelope() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
        let checkpoint = attempt(1);
        let fence = assignment_fence(7, 1);
        let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
        let key = "participant=1/ready";
        backend.set_authoritative_version(7);
        backend
            .write_certified_commit_descriptor(
                checkpoint,
                key,
                &fence,
                1,
                &authority,
                Bytes::from_static(b"ready"),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, Some(&fence), &[], &[key.to_owned()])
            .await
            .unwrap());
        let inventory = backend
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .unwrap();
        let sealed = inventory.sealed_descriptor(key).unwrap();

        let replacement = ObjectStoreBackend::encode_commit_descriptor(
            checkpoint,
            key,
            fence.assignment_version,
            sealed.writer.as_ref(),
            &Bytes::from_static(b"evil!"),
        );
        store
            .put(
                &ObjectStoreBackend::descriptor_path(checkpoint, key),
                PutPayload::from(replacement),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .read_commit_descriptor(checkpoint, key)
                .await
                .unwrap(),
            Some(Bytes::from_static(b"evil!")),
            "the replacement must be a self-consistent descriptor envelope"
        );

        let error = backend
            .read_sealed_commit_descriptor_bounded(checkpoint, sealed, 5)
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error
            .to_string()
            .contains("does not match the checkpoint seal"));
    }

    #[tokio::test]
    async fn cluster_shared_local_runtime_descriptor_is_valid_without_assignment_authority() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::cluster_shared(store, "single-node", 1);
        let checkpoint = attempt(1);

        backend
            .write_commit_descriptor(checkpoint, "ready", Bytes::from_static(b"local"))
            .await
            .unwrap();

        assert_eq!(backend.authoritative_version(), 0);
        assert_eq!(
            backend
                .read_commit_descriptor(checkpoint, "ready")
                .await
                .unwrap(),
            Some(Bytes::from_static(b"local"))
        );
    }

    #[tokio::test]
    async fn installed_assignment_rejects_uncertified_descriptor_before_publication() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(store, "node-1", 1);
        let checkpoint = attempt(1);
        let fence = assignment_fence(7, 1);
        let authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
        let key = "participant=1/ready";
        backend.set_authoritative_version(7);
        backend
            .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
            .await
            .unwrap();
        let error = backend
            .write_commit_descriptor(checkpoint, key, Bytes::from_static(b"uncertified"))
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("uncertified commit descriptor"));
        assert_eq!(
            backend
                .read_commit_descriptor(checkpoint, key)
                .await
                .unwrap(),
            None
        );

        backend
            .write_certified_commit_descriptor(
                checkpoint,
                key,
                &fence,
                1,
                &authority,
                Bytes::from_static(b"certified"),
            )
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(checkpoint, Some(&fence), &[0], &[key.to_string()])
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn cluster_seal_rejects_stale_boot_descriptor_poison() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(store, "node-1", 1);
        let checkpoint = attempt(1);
        let stale = assignment_fence(7, 1);
        let current = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
        let stale_authority = leader_proof(1, uuid::Uuid::from_u128(1), 11);
        let current_authority = leader_proof(1, uuid::Uuid::from_u128(2), 12);
        let key = "participant=1/ready";
        backend.set_authoritative_version(7);
        backend
            .write_certified_partial(checkpoint, 0, &current, 1, Bytes::from_static(b"state"))
            .await
            .unwrap();
        backend
            .write_certified_commit_descriptor(
                checkpoint,
                key,
                &stale,
                1,
                &stale_authority,
                Bytes::from_static(b"stale"),
            )
            .await
            .unwrap();

        let poison = backend
            .write_certified_commit_descriptor(
                checkpoint,
                key,
                &current,
                1,
                &current_authority,
                Bytes::from_static(b"current"),
            )
            .await
            .unwrap_err();
        assert!(matches!(poison, StateBackendError::Conflict { .. }));
        let error = backend
            .seal_checkpoint(checkpoint, Some(&current), &[0], &[key.to_string()])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("writer certificate"));
    }

    #[tokio::test]
    async fn cluster_seal_rejects_descriptors_from_different_leader_terms() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(store, "node-1", 1);
        let checkpoint = attempt(1);
        let fence = assignment_fence(7, 1);
        backend.set_authoritative_version(7);
        backend
            .write_certified_partial(checkpoint, 0, &fence, 1, Bytes::from_static(b"state"))
            .await
            .unwrap();
        for (key, token) in [("participant=1/ready", 11), ("coordinator", 12)] {
            backend
                .write_certified_commit_descriptor(
                    checkpoint,
                    key,
                    &fence,
                    1,
                    &leader_proof(1, uuid::Uuid::from_u128(1), token),
                    Bytes::from_static(b"ready"),
                )
                .await
                .unwrap();
        }

        let error = backend
            .seal_checkpoint(
                checkpoint,
                Some(&fence),
                &[0],
                &["participant=1/ready".into(), "coordinator".into()],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("different leader terms"));
    }

    #[tokio::test]
    async fn descriptor_read_rejects_key_mismatch_and_payload_corruption() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
        let checkpoint = attempt(1);
        let encoded = ObjectStoreBackend::encode_commit_descriptor(
            checkpoint,
            "expected-key",
            0,
            None,
            &Bytes::from_static(b"payload"),
        );
        store
            .put(
                &ObjectStoreBackend::descriptor_path(checkpoint, "wrong-key"),
                PutPayload::from(encoded),
            )
            .await
            .unwrap();
        let mismatch = backend
            .read_commit_descriptor(checkpoint, "wrong-key")
            .await
            .unwrap_err();
        assert!(matches!(mismatch, StateBackendError::Conflict { .. }));
        assert!(mismatch.to_string().contains("key digest"));

        let other_attempt = attempt(2);
        let encoded = ObjectStoreBackend::encode_commit_descriptor(
            checkpoint,
            "attempt-bound",
            0,
            None,
            &Bytes::from_static(b"payload"),
        );
        store
            .put(
                &ObjectStoreBackend::descriptor_path(other_attempt, "attempt-bound"),
                PutPayload::from(encoded),
            )
            .await
            .unwrap();
        let mismatch = backend
            .read_commit_descriptor(other_attempt, "attempt-bound")
            .await
            .unwrap_err();
        assert!(matches!(mismatch, StateBackendError::Conflict { .. }));
        assert!(mismatch.to_string().contains("names attempt"));

        let mut corrupted = ObjectStoreBackend::encode_commit_descriptor(
            checkpoint,
            "corrupted",
            0,
            None,
            &Bytes::from_static(b"payload"),
        )
        .to_vec();
        *corrupted.last_mut().unwrap() ^= 0xff;
        store
            .put(
                &ObjectStoreBackend::descriptor_path(checkpoint, "corrupted"),
                PutPayload::from(Bytes::from(corrupted)),
            )
            .await
            .unwrap();
        let corruption = backend
            .read_commit_descriptor(checkpoint, "corrupted")
            .await
            .unwrap_err();
        assert!(matches!(corruption, StateBackendError::Serialization(_)));
        assert!(corruption.to_string().contains("checksum mismatch"));
    }

    #[tokio::test]
    async fn seal_rejects_descriptor_with_truncated_payload() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-1", 1);
        let checkpoint = attempt(1);
        let key = "ready";
        backend
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();
        let mut encoded = ObjectStoreBackend::encode_commit_descriptor(
            checkpoint,
            key,
            0,
            None,
            &Bytes::from_static(b"payload"),
        )
        .to_vec();
        encoded.pop();
        store
            .put(
                &ObjectStoreBackend::descriptor_path(checkpoint, key),
                PutPayload::from(Bytes::from(encoded)),
            )
            .await
            .unwrap();

        let error = backend
            .seal_checkpoint(checkpoint, None, &[0], &[key.to_string()])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("declared 7 payload bytes"));
    }

    /// The CAS-create `AlreadyExists` branch must not silently agree it committed:
    /// the loser reads the marker, sees a mismatched audit body, and fails loud.
    #[tokio::test]
    async fn seal_rejects_different_execution_incarnation() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        // Both "nodes" wrote partials for the epoch.
        for v in &vnodes {
            winner
                .write_partial(attempt(7), *v, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }

        // Winner CAS-creates the state seal first.
        assert!(winner
            .seal_checkpoint(attempt(7), None, &vnodes, &[])
            .await
            .unwrap());

        // Loser finds a seal created by a different execution incarnation.
        let err = loser
            .seal_checkpoint(attempt(7), None, &vnodes, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, StateBackendError::Conflict { .. }));

        // And the winner's repeated call is still idempotent Ok(true).
        assert!(winner
            .seal_checkpoint(attempt(7), None, &vnodes, &[])
            .await
            .unwrap());
    }

    /// Same contract on the CAS-loser path: if the marker doesn't exist
    /// at HEAD time but a peer sneaks in between our vnode-presence
    /// check and our own PUT, our `put_opts` fails with `AlreadyExists`.
    /// That branch must also compare committers, not silently succeed.
    #[tokio::test]
    async fn seal_cas_loser_rejects_different_execution() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let winner = ObjectStoreBackend::new(Arc::clone(&store), "winner", 4);
        let loser = ObjectStoreBackend::new(Arc::clone(&store), "loser", 4);

        let vnodes = [0u32, 1];
        for v in &vnodes {
            winner
                .write_partial(attempt(3), *v, 0, Bytes::from_static(b"w"))
                .await
                .unwrap();
        }
        // Manually pre-seed a structured seal under "winner" to
        // simulate the TOCTOU race deterministically — the loser's
        // put_opts will hit AlreadyExists on its own PUT attempt.
        let commit = ObjectStoreBackend::seal_path(attempt(3));
        let mut sealed_partials = Vec::new();
        for &vnode in &vnodes {
            sealed_partials.push(
                winner
                    .read_partial_attestation(attempt(3), vnode)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let seal = CheckpointSeal::new(
            "winner".into(),
            winner.execution_id(),
            CheckpointSealInventory {
                attempt: attempt(3),
                assignment_fence: None,
                assignment_version: 0,
                required_vnodes: vnodes.to_vec(),
                sealed_partials,
                required_descriptors: Vec::new(),
                sealed_descriptors: Vec::new(),
            },
        );
        store
            .put(
                &commit,
                PutPayload::from(Bytes::from(serde_json::to_vec(&seal).unwrap())),
            )
            .await
            .unwrap();

        let err = loser
            .seal_checkpoint(attempt(3), None, &vnodes, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, StateBackendError::Conflict { .. }));
    }

    #[tokio::test]
    async fn stale_version_rejected() {
        // Force two "nodes" (backend instances wrapping the same store)
        // to claim the same vnode at different generations. The stale
        // writer must be rejected.
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
        let fresh = ObjectStoreBackend::new(Arc::clone(&store), "node-fresh", 4);

        // Fresh learns about a new assignment generation — e.g. a new
        // snapshot rotated in after a leader election.
        fresh.set_authoritative_version(2);

        // Fresh writes at the current version: accepted.
        fresh
            .write_partial(attempt(1), 0, 2, Bytes::from_static(b"fresh"))
            .await
            .unwrap();

        // Stale tries to write at version 1 — but only IF it's also
        // learned of the rotation. Model that by promoting stale's
        // view too; the check is intra-backend here because the
        // durable version-broadcast channel is out of scope for this test.
        stale.set_authoritative_version(2);
        let err = stale
            .write_partial(attempt(1), 0, 1, Bytes::from_static(b"stale"))
            .await
            .unwrap_err();
        match err {
            StateBackendError::StaleVersion {
                caller,
                authoritative,
            } => {
                assert_eq!(caller, 1);
                assert_eq!(authoritative, 2);
            }
            other => panic!("expected StaleVersion, got {other:?}"),
        }

        // Fence-disabled backend (authoritative stays at 0) accepts
        // any version — preserves legacy single-instance behavior.
        let unfenced = ObjectStoreBackend::new(Arc::clone(&store), "node-unfenced", 4);
        unfenced
            .write_partial(attempt(1), 1, 0, Bytes::from_static(b"ok"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn future_assignment_version_is_rejected_before_publication() {
        let backend =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
        backend.set_authoritative_version(7);
        let future = assignment_fence(8, 1);

        let error = backend
            .write_certified_partial(attempt(1), 0, &future, 1, Bytes::from_static(b"future"))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StateBackendError::FutureVersion {
                caller: 8,
                authoritative: 7
            }
        ));
        assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());

        // A future partial that landed before fencing was configured must still fail the seal
        // after this backend adopts the current generation.
        let bypass =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
        bypass
            .write_certified_partial(attempt(2), 0, &future, 1, Bytes::from_static(b"future"))
            .await
            .unwrap();
        bypass.set_authoritative_version(7);
        let current = assignment_fence(7, 1);
        let error = bypass
            .seal_checkpoint(attempt(2), Some(&current), &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("cannot satisfy seal version 7"));
    }

    #[tokio::test]
    async fn seal_rejects_stale_boot_writer_certificate() {
        let backend =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 1);
        backend.set_authoritative_version(7);
        let stale = assignment_fence(7, 1);
        let current = CheckpointAssignmentFence::from_owner_map(
            7,
            &[1],
            vec![crate::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
        backend
            .write_certified_partial(attempt(1), 0, &stale, 1, Bytes::from_static(b"stale-boot"))
            .await
            .unwrap();

        let error = backend
            .seal_checkpoint(attempt(1), Some(&current), &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("writer certificate"));
    }

    #[tokio::test]
    async fn seal_rejects_assignment_certificate_digest_mismatch() {
        use crate::checkpoint::CheckpointParticipant;

        let backend =
            ObjectStoreBackend::new(Arc::new(object_store::memory::InMemory::new()), "node-1", 2);
        backend.set_authoritative_version(7);
        let participants = vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ];
        let written =
            CheckpointAssignmentFence::from_owner_map(7, &[1, 2], participants.clone()).unwrap();
        let sealing = CheckpointAssignmentFence::from_owner_map(7, &[2, 1], participants).unwrap();
        backend
            .write_certified_partial(attempt(1), 0, &written, 1, Bytes::from_static(b"wrong-map"))
            .await
            .unwrap();

        let error = backend
            .seal_checkpoint(attempt(1), Some(&sealing), &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("writer certificate"));
    }

    #[tokio::test]
    async fn stale_generation_partial_cannot_satisfy_fresh_seal() {
        let dir = tempdir().unwrap();
        let store = make_store(dir.path());
        let stale = ObjectStoreBackend::new(Arc::clone(&store), "node-stale", 4);
        let fresh = ObjectStoreBackend::new(store, "node-fresh", 4);
        let checkpoint = CheckpointAttempt::canonical(901);

        // The stale process has not learned generation 2 and wins the create-once path first.
        stale
            .write_partial(checkpoint, 0, 1, Bytes::from_static(b"stale-state"))
            .await
            .unwrap();
        fresh.set_authoritative_version(2);
        let fence = assignment_fence(2, 4);

        let error = fresh
            .seal_checkpoint(checkpoint, Some(&fence), &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(error.to_string().contains("cannot satisfy seal version 2"));
        assert!(fresh
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .is_none());
    }

    #[test]
    fn authoritative_version_is_monotonic() {
        let dir = tempdir().unwrap();
        let b = ObjectStoreBackend::new(make_store(dir.path()), "node", 2);
        assert_eq!(b.authoritative_version(), 0);
        b.set_authoritative_version(3);
        assert_eq!(b.authoritative_version(), 3);
        // Attempts to lower the version are no-ops.
        b.set_authoritative_version(1);
        assert_eq!(b.authoritative_version(), 3);
        b.set_authoritative_version(4);
        assert_eq!(b.authoritative_version(), 4);
    }

    #[test]
    fn durability_scope_requires_explicit_storage_topology() {
        let dir = tempdir().unwrap();
        assert_eq!(
            ObjectStoreBackend::new(make_store(dir.path()), "uncertified", 2).durability_scope(),
            StateBackendDurability::Volatile
        );
        assert_eq!(
            ObjectStoreBackend::node_durable(make_store(dir.path()), "local", 2).durability_scope(),
            StateBackendDurability::NodeDurable
        );
        assert_eq!(
            ObjectStoreBackend::cluster_shared(make_store(dir.path()), "shared", 2)
                .durability_scope(),
            StateBackendDurability::ClusterShared
        );
    }

    #[tokio::test]
    async fn object_safe_behind_arc() {
        let dir = tempdir().unwrap();
        let _: Arc<dyn StateBackend> =
            Arc::new(ObjectStoreBackend::new(make_store(dir.path()), "node-0", 2));
    }

    #[tokio::test]
    async fn prune_before_deletes_old_epochs() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Seed epochs 1..=5 with one vnode each.
        for epoch in 1..=5u64 {
            backend
                .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"x"))
                .await
                .unwrap();
        }

        backend.prune_before(4).await.unwrap();

        for epoch in 1..=3 {
            assert!(
                backend
                    .read_partial(attempt(epoch), 0)
                    .await
                    .unwrap()
                    .is_none(),
                "epoch {epoch} should be pruned",
            );
        }
        for epoch in 4..=5 {
            assert!(
                backend
                    .read_partial(attempt(epoch), 0)
                    .await
                    .unwrap()
                    .is_some(),
                "epoch {epoch} should be retained",
            );
        }
    }

    #[tokio::test]
    async fn prune_before_discovers_sparse_epochs_without_scanning_the_id_gap() {
        let dir = tempdir().unwrap();
        let operations = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let physical_store = make_store(dir.path());
        let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
            inner: Arc::clone(&physical_store),
            operations: Arc::clone(&operations),
            delete_calls: Arc::new(AtomicU64::new(0)),
            fail_delete_call: 0,
        });
        let backend = ObjectStoreBackend::new(store, "node-0", 1);
        let retired = attempt(1);
        let retained = attempt(65_537);

        backend
            .write_partial(retired, 0, 0, Bytes::from_static(b"retired"))
            .await
            .unwrap();
        backend
            .write_partial(retained, 0, 0, Bytes::from_static(b"retained"))
            .await
            .unwrap();

        operations.lock().clear();
        backend.prune_before(65_537).await.unwrap();

        assert!(matches!(
            physical_store
                .head(&ObjectStoreBackend::partial_path(retired, 0))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(physical_store
            .head(&ObjectStoreBackend::partial_path(retained, 0))
            .await
            .is_ok());

        let listings = operations
            .lock()
            .iter()
            .filter(|operation| {
                operation.starts_with("delimiter:") || operation.starts_with("list:")
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            listings,
            vec![
                "delimiter:state-v2".to_string(),
                "list:state-v2/epoch=1".to_string(),
            ],
            "retention must list only materialized retired epoch prefixes"
        );
    }

    #[tokio::test]
    async fn cluster_shared_prune_requires_native_conditional_update() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::cluster_shared(make_store(dir.path()), "cluster", 2);
        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();

        let error = backend.prune_before(2).await.unwrap_err();
        assert!(error.to_string().contains("PutMode::Update"), "{error}");
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 2);
        assert_eq!(floor.swept_before_epoch, 0);
        assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn retention_publishes_one_floor_before_deleting_attested_artifacts() {
        let dir = tempdir().unwrap();
        let operations = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
            inner: make_store(dir.path()),
            operations: Arc::clone(&operations),
            delete_calls: Arc::new(AtomicU64::new(0)),
            fail_delete_call: 0,
        });
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);
        let ready_key = "participant-ready/0.json";

        for epoch in 1..=3u64 {
            backend
                .write_partial(attempt(epoch), 0, 0, Bytes::from_static(b"state"))
                .await
                .unwrap();
            backend
                .write_commit_descriptor(attempt(epoch), ready_key, Bytes::from_static(b"ready"))
                .await
                .unwrap();
            assert!(backend
                .seal_checkpoint(attempt(epoch), None, &[0], &[ready_key.to_string()])
                .await
                .unwrap());
        }

        operations.lock().clear();
        backend.prune_before(2).await.unwrap();

        let recorded = operations.lock().clone();
        let first_floor = recorded
            .iter()
            .position(|operation| operation.starts_with("floor:"))
            .unwrap_or_else(|| panic!("prune_before did not publish its floor: {recorded:?}"));
        let first_artifact = recorded
            .iter()
            .position(|operation| operation.starts_with("delete:"))
            .unwrap_or_else(|| panic!("prune_before did not delete artifacts: {recorded:?}"));
        assert!(
            first_floor < first_artifact,
            "prune_before deleted artifacts before publishing its floor: {recorded:?}"
        );

        let seal_path = ObjectStoreBackend::seal_path(attempt(1));
        assert!(matches!(
            store.get(&seal_path).await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(backend
            .checkpoint_seal_inventory(attempt(1))
            .await
            .unwrap()
            .is_none());
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 2);
        assert_eq!(floor.swept_before_epoch, 2);
    }

    #[tokio::test]
    async fn prune_wins_after_sealer_verifies_but_before_seal_publication() {
        let dir = tempdir().unwrap();
        let checkpoint = attempt(1);
        let reached = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let store: Arc<dyn ObjectStore> = Arc::new(SealPublishGateStore {
            inner: make_store(dir.path()),
            seal_path: ObjectStoreBackend::seal_path(checkpoint),
            gated: std::sync::atomic::AtomicBool::new(false),
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
        });
        let sealer = Arc::new(ObjectStoreBackend::new(Arc::clone(&store), "sealer", 2));
        let collector = Arc::new(ObjectStoreBackend::new(store, "collector", 2));

        sealer
            .write_partial(checkpoint, 0, 0, Bytes::from_static(b"state"))
            .await
            .unwrap();

        let seal_task = tokio::spawn({
            let sealer = Arc::clone(&sealer);
            async move { sealer.seal_checkpoint(checkpoint, None, &[0], &[]).await }
        });
        reached
            .acquire()
            .await
            .expect("test gate remains open")
            .forget();

        // The sealer has listed and attested the partial, but its create has not reached shared
        // storage. Retention publishes the global floor while that create is suspended.
        collector.prune_before(2).await.unwrap();
        assert!(collector
            .read_partial(checkpoint, 0)
            .await
            .unwrap()
            .is_none());
        assert!(collector
            .checkpoint_seal_inventory(checkpoint)
            .await
            .unwrap()
            .is_none());

        release.add_permits(1);
        let error = seal_task.await.unwrap().unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));

        // The durable floor rejects a retry without retaining one tombstone per attempt.
        let error = sealer
            .seal_checkpoint(checkpoint, None, &[0], &[])
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
    }

    #[tokio::test]
    async fn prune_failure_preserves_completed_prefix_progress_and_repairs_the_rest() {
        let dir = tempdir().unwrap();
        let physical_store = make_store(dir.path());
        let store: Arc<dyn ObjectStore> = Arc::new(RetentionLogStore {
            inner: Arc::clone(&physical_store),
            operations: Arc::new(parking_lot::Mutex::new(Vec::new())),
            delete_calls: Arc::new(AtomicU64::new(0)),
            fail_delete_call: 2,
        });
        let backend = ObjectStoreBackend::new(store, "node-0", 2);
        for checkpoint_id in 1..=2 {
            backend
                .write_partial(attempt(checkpoint_id), 0, 0, Bytes::from_static(b"state"))
                .await
                .unwrap();
        }

        let error = backend.prune_before(3).await.unwrap_err();
        assert!(matches!(error, StateBackendError::Io(_)));
        assert!(error.to_string().contains("injected delete failure"));
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 3);
        assert_eq!(floor.swept_before_epoch, 2);
        assert!(matches!(
            physical_store
                .head(&ObjectStoreBackend::partial_path(attempt(1), 0))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(physical_store
            .head(&ObjectStoreBackend::partial_path(attempt(2), 0))
            .await
            .is_ok());

        backend.prune_before(3).await.unwrap();
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 3);
        assert_eq!(floor.swept_before_epoch, 3);
        assert!(matches!(
            physical_store
                .head(&ObjectStoreBackend::partial_path(attempt(2), 0))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
    }

    /// The durable sweep cursor advances so later retention lists only newly retired epochs.
    #[tokio::test]
    async fn prune_before_is_incremental_and_advances_horizon() {
        let dir = tempdir().unwrap();
        let backend = ObjectStoreBackend::new(make_store(dir.path()), "node-0", 4);

        // Seed epochs 1..=6, two vnodes each so deletes touch >1 object.
        for epoch in 1..=6u64 {
            for v in 0..2u32 {
                backend
                    .write_partial(attempt(epoch), v, 0, Bytes::from_static(b"x"))
                    .await
                    .unwrap();
            }
        }

        backend.prune_before(3).await.unwrap();
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 3);
        assert_eq!(floor.swept_before_epoch, 3);

        backend.prune_before(5).await.unwrap();
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 5);
        assert_eq!(floor.swept_before_epoch, 5);

        for epoch in 1..=4u64 {
            for v in 0..2u32 {
                assert!(
                    backend
                        .read_partial(attempt(epoch), v)
                        .await
                        .unwrap()
                        .is_none(),
                    "epoch {epoch} vnode {v} should be pruned",
                );
            }
        }
        for epoch in 5..=6u64 {
            for v in 0..2u32 {
                assert!(
                    backend
                        .read_partial(attempt(epoch), v)
                        .await
                        .unwrap()
                        .is_some(),
                    "epoch {epoch} vnode {v} should be retained",
                );
            }
        }

        // Idempotent re-prune at the same horizon is a no-op.
        backend.prune_before(5).await.unwrap();
        let floor = backend.read_prune_floor().await.unwrap().unwrap().floor;
        assert_eq!(floor.before_epoch, 5);
        assert_eq!(floor.swept_before_epoch, 5);
        assert!(backend.read_partial(attempt(5), 0).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn durable_floor_rejects_late_writes_after_restart() {
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let backend = ObjectStoreBackend::new(Arc::clone(&store), "node-0", 4);

        backend
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"x"))
            .await
            .unwrap();
        backend.prune_before(3).await.unwrap();
        assert!(backend.read_partial(attempt(1), 0).await.unwrap().is_none());

        let restarted = ObjectStoreBackend::new(store, "node-0", 4);
        let error = restarted
            .write_partial(attempt(1), 0, 0, Bytes::from_static(b"late"))
            .await
            .unwrap_err();
        assert!(matches!(error, StateBackendError::Conflict { .. }));
        assert!(restarted
            .read_partial(attempt(1), 0)
            .await
            .unwrap()
            .is_none());
        assert!(restarted
            .write_partial(attempt(3), 0, 0, Bytes::from_static(b"live"))
            .await
            .is_ok());
    }
}
