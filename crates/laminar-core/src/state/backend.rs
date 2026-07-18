//! The `StateBackend` trait persists per-checkpoint-attempt vnode artifacts
//! and exposes an exact-attempt durability seal. It is not the hot keyed-state
//! implementation used while operators process records.

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::checkpoint::{
    CheckpointAssignmentFence, CheckpointParticipant, LeaderProof, PipelineIdentity,
    PIPELINE_IDENTITY_VERSION,
};

pub(crate) const STATE_NAMESPACE_RESOURCE: &str = "state-v2/_NAMESPACE";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateNamespaceBinding {
    pub(crate) deployment_id: String,
    pub(crate) pipeline_identity: PipelineIdentity,
}

impl StateNamespaceBinding {
    pub(crate) fn try_new(
        deployment_id: &str,
        pipeline_identity: &PipelineIdentity,
    ) -> Result<Self, StateBackendError> {
        let deployment =
            uuid::Uuid::parse_str(deployment_id).map_err(|error| StateBackendError::Conflict {
                resource: STATE_NAMESPACE_RESOURCE.into(),
                message: format!("deployment ID is not a canonical UUID: {error}"),
            })?;
        if deployment.is_nil() || deployment.to_string() != deployment_id {
            return Err(StateBackendError::Conflict {
                resource: STATE_NAMESPACE_RESOURCE.into(),
                message: "deployment ID must be a canonical non-nil UUID".into(),
            });
        }
        if pipeline_identity.canonical_version != PIPELINE_IDENTITY_VERSION
            || pipeline_identity.sha256.len() != 64
            || !pipeline_identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(StateBackendError::Conflict {
                resource: STATE_NAMESPACE_RESOURCE.into(),
                message: "pipeline identity is not canonical".into(),
            });
        }
        Ok(Self {
            deployment_id: deployment_id.to_owned(),
            pipeline_identity: pipeline_identity.clone(),
        })
    }
}

/// Exact identity of one checkpoint attempt.
///
/// `epoch` is logical pipeline progress and `checkpoint_id` is the never-reused attempt identity.
/// Neither dimension alone orders attempts: valid progress requires both to move together.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct CheckpointAttempt {
    /// Logical pipeline epoch represented by this checkpoint.
    pub epoch: u64,
    /// Globally unique, never-reused checkpoint attempt ID.
    pub checkpoint_id: u64,
}

/// Explicit relation between two checkpoint attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointAttemptRelation {
    /// Both epoch and checkpoint ID are identical.
    Exact,
    /// Both epoch and checkpoint ID are lower than the compared attempt.
    Older,
    /// Both epoch and checkpoint ID are higher than the compared attempt.
    Newer,
    /// The dimensions move in different directions, or only one dimension changes.
    Conflict,
}

impl CheckpointAttempt {
    /// Construct an exact checkpoint attempt identity.
    #[must_use]
    pub const fn new(epoch: u64, checkpoint_id: u64) -> Self {
        Self {
            epoch,
            checkpoint_id,
        }
    }

    /// Relate this attempt to `other` without inventing a lexicographic order.
    #[must_use]
    pub const fn relation_to(self, other: Self) -> CheckpointAttemptRelation {
        if self.epoch == other.epoch && self.checkpoint_id == other.checkpoint_id {
            CheckpointAttemptRelation::Exact
        } else if self.epoch < other.epoch && self.checkpoint_id < other.checkpoint_id {
            CheckpointAttemptRelation::Older
        } else if self.epoch > other.epoch && self.checkpoint_id > other.checkpoint_id {
            CheckpointAttemptRelation::Newer
        } else {
            CheckpointAttemptRelation::Conflict
        }
    }
}

/// Current on-storage checkpoint-seal payload format.
pub(crate) const CHECKPOINT_SEAL_VERSION: u32 = 7;

/// Cluster identity that produced one immutable vnode partial.
///
/// The certificate digest binds the writer to the exact assignment version, ordered owner map,
/// and boot-incarnation roster. Local-runtime partials do not carry this structure.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealedVnodeWriter {
    /// Stable logical node identifier.
    pub node_id: u64,
    /// Boot-unique process identity certified for `node_id`.
    pub boot_incarnation: uuid::Uuid,
    /// SHA-256 of the exact checkpoint assignment certificate.
    #[serde(with = "hex_digest_serde")]
    pub assignment_certificate_digest: [u8; 32],
}

impl SealedVnodeWriter {
    #[must_use]
    pub(crate) fn from_fence(fence: &CheckpointAssignmentFence, node_id: u64) -> Option<Self> {
        let boot_incarnation = fence.participant_incarnation(node_id)?;
        Some(Self {
            node_id,
            boot_incarnation,
            assignment_certificate_digest: fence.digest(),
        })
    }

    /// Whether this writer is the exact process certified for its node by `fence`.
    #[must_use]
    pub fn matches_fence(&self, fence: &CheckpointAssignmentFence) -> bool {
        self.node_id != 0
            && !self.boot_incarnation.is_nil()
            && fence.participant_incarnation(self.node_id) == Some(self.boot_incarnation)
            && self.assignment_certificate_digest == fence.digest()
    }
}

/// Immutable provenance and content attestation for one vnode partial admitted by a seal.
///
/// The assignment generation and exact writer certificate are the durable split-brain fence,
/// while the payload digest lets recovery detect corruption without placing large state blobs in
/// the seal itself.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealedVnodePartial {
    /// Vnode whose partial was sealed.
    pub vnode: u32,
    /// Assignment generation observed by the writer.
    pub assignment_version: u64,
    /// Exact cluster writer, or `None` for a fence-free local-runtime partial.
    pub writer: Option<SealedVnodeWriter>,
    /// Raw partial payload length, excluding the storage envelope.
    pub payload_len: u64,
    /// SHA-256 of the raw partial payload.
    pub payload_sha256: String,
}

impl SealedVnodePartial {
    pub(crate) fn new(
        vnode: u32,
        assignment_version: u64,
        writer: Option<SealedVnodeWriter>,
        payload: &[u8],
    ) -> Self {
        Self {
            vnode,
            assignment_version,
            writer,
            payload_len: payload.len() as u64,
            payload_sha256: digest_hex(&sha256(payload)),
        }
    }
}

/// Exact process and leader term that produced one immutable commit descriptor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealedCommitDescriptorWriter {
    /// Boot-exact process that wrote the descriptor.
    pub participant: CheckpointParticipant,
    /// SHA-256 of the exact checkpoint assignment certificate.
    #[serde(with = "hex_digest_serde")]
    pub assignment_certificate_digest: [u8; 32],
    /// Durable leader term that initiated the checkpoint attempt.
    pub leader_proof: LeaderProof,
}

impl SealedCommitDescriptorWriter {
    #[must_use]
    pub(crate) fn from_fence(
        fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        leader_proof: &LeaderProof,
    ) -> Option<Self> {
        let boot_incarnation = fence.participant_incarnation(writer_node_id)?;
        if !fence.is_canonical()
            || !leader_proof.is_canonical()
            || fence.participant_incarnation(leader_proof.owner.node_id)
                != Some(leader_proof.owner.boot_id)
        {
            return None;
        }
        Some(Self {
            participant: CheckpointParticipant {
                node_id: writer_node_id,
                boot_incarnation,
            },
            assignment_certificate_digest: fence.digest(),
            leader_proof: leader_proof.clone(),
        })
    }

    /// Whether this writer and checkpoint authority exactly match `fence`.
    #[must_use]
    pub fn matches_fence(&self, fence: &CheckpointAssignmentFence) -> bool {
        self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && fence.participant_incarnation(self.participant.node_id)
                == Some(self.participant.boot_incarnation)
            && self.assignment_certificate_digest == fence.digest()
            && self.leader_proof.is_canonical()
            && fence.participant_incarnation(self.leader_proof.owner.node_id)
                == Some(self.leader_proof.owner.boot_id)
    }
}

/// Immutable provenance and content attestation for one commit descriptor admitted by a seal.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealedCommitDescriptor {
    /// Exact descriptor key within the checkpoint attempt.
    pub key: String,
    /// Assignment generation observed by the writer, or zero for a local-runtime descriptor.
    pub assignment_version: u64,
    /// Exact cluster writer and authority, or `None` for a local-runtime descriptor.
    pub writer: Option<SealedCommitDescriptorWriter>,
    /// Raw descriptor payload length, excluding its storage envelope.
    pub payload_len: u64,
    /// SHA-256 of the raw descriptor payload.
    pub payload_sha256: String,
}

impl SealedCommitDescriptor {
    pub(crate) fn new(
        key: String,
        assignment_version: u64,
        writer: Option<SealedCommitDescriptorWriter>,
        payload: &[u8],
    ) -> Self {
        Self {
            key,
            assignment_version,
            writer,
            payload_len: payload.len() as u64,
            payload_sha256: digest_hex(&sha256(payload)),
        }
    }
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn digest_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

mod hex_digest_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(digest: &[u8; 32], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&super::digest_hex(digest))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        if encoded.len() != 64
            || !encoded
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(serde::de::Error::custom(
                "SHA-256 digest must be 64 lowercase hexadecimal characters",
            ));
        }
        let mut digest = [0_u8; 32];
        for (index, byte) in digest.iter_mut().enumerate() {
            let offset = index * 2;
            *byte = u8::from_str_radix(&encoded[offset..offset + 2], 16)
                .map_err(serde::de::Error::custom)?;
        }
        Ok(digest)
    }
}

/// Canonical artifact inventory bound into an immutable checkpoint seal.
///
/// A seal is meaningful only for the exact set that was proven durable. The
/// inventory is exposed so recovery-side protocols (notably the designated
/// external-sink committer) can independently validate the participant markers
/// that the seal admitted.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct CheckpointSealInventory {
    /// Exact attempt named by the seal.
    pub attempt: CheckpointAttempt,
    /// Exact cluster assignment certificate, or `None` for a local-runtime seal.
    pub assignment_fence: Option<CheckpointAssignmentFence>,
    /// Assignment generation attested by every vnode partial, including local-runtime seals.
    pub assignment_version: u64,
    /// Sorted, duplicate-free vnode partials required by the seal.
    pub required_vnodes: Vec<u32>,
    /// Per-vnode writer, assignment-generation, length, and digest attestations.
    pub sealed_partials: Vec<SealedVnodePartial>,
    /// Sorted, duplicate-free commit-descriptor keys required by the seal.
    pub required_descriptors: Vec<String>,
    /// Per-descriptor writer, authority, length, and digest attestations.
    pub sealed_descriptors: Vec<SealedCommitDescriptor>,
}

impl CheckpointSealInventory {
    /// Attestation for one exact descriptor key.
    #[must_use]
    pub fn sealed_descriptor(&self, key: &str) -> Option<&SealedCommitDescriptor> {
        self.sealed_descriptors
            .binary_search_by(|descriptor| descriptor.key.as_str().cmp(key))
            .ok()
            .map(|index| &self.sealed_descriptors[index])
    }

    /// Exact leader term certified by every cluster descriptor in this seal.
    ///
    /// An empty descriptor inventory has no descriptor authority. Local descriptors return
    /// `None`; a mixed, incomplete, or assignment-mismatched inventory is rejected.
    pub fn descriptor_leader_proof(&self) -> Result<Option<&LeaderProof>, String> {
        if self.sealed_descriptors.len() != self.required_descriptors.len() {
            return Err(
                "checkpoint seal descriptor attestations do not exactly cover its keys".into(),
            );
        }
        let descriptor_keys: Vec<&str> = self
            .sealed_descriptors
            .iter()
            .map(|descriptor| descriptor.key.as_str())
            .collect();
        let required_keys: Vec<&str> = self
            .required_descriptors
            .iter()
            .map(String::as_str)
            .collect();
        if descriptor_keys != required_keys {
            return Err(
                "checkpoint seal descriptor attestations do not exactly cover its keys".into(),
            );
        }

        let mut authority = None;
        for descriptor in &self.sealed_descriptors {
            let valid_digest = descriptor.payload_sha256.len() == 64
                && descriptor
                    .payload_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
            if !valid_digest {
                return Err(format!(
                    "checkpoint seal has invalid descriptor digest for '{}'",
                    descriptor.key
                ));
            }
            match (&self.assignment_fence, &descriptor.writer) {
                (Some(fence), Some(writer))
                    if descriptor.assignment_version == fence.assignment_version
                        && writer.matches_fence(fence) =>
                {
                    if authority.is_some_and(|proof| proof != &writer.leader_proof) {
                        return Err(
                            "checkpoint seal descriptors name different leader terms".into()
                        );
                    }
                    authority = Some(&writer.leader_proof);
                }
                (None, None) if descriptor.assignment_version == 0 => {}
                _ => {
                    return Err(format!(
                        "checkpoint seal has invalid writer certificate for descriptor '{}'",
                        descriptor.key
                    ));
                }
            }
        }
        Ok(authority)
    }
}

/// Structured body of an exact-attempt state seal.
///
/// This remains crate-private because callers consume only the canonical
/// [`CheckpointSealInventory`]; the additional fields are durability audit and
/// fencing metadata owned by backend implementations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CheckpointSeal {
    pub version: u32,
    pub attempt: CheckpointAttempt,
    pub instance_id: String,
    pub execution_id: uuid::Uuid,
    pub assignment_fence: Option<CheckpointAssignmentFence>,
    pub assignment_version: u64,
    pub required_vnodes: Vec<u32>,
    pub sealed_partials: Vec<SealedVnodePartial>,
    pub required_descriptors: Vec<String>,
    pub sealed_descriptors: Vec<SealedCommitDescriptor>,
}

impl CheckpointSeal {
    pub(crate) fn new(
        instance_id: String,
        execution_id: uuid::Uuid,
        mut inventory: CheckpointSealInventory,
    ) -> Self {
        inventory.required_vnodes.sort_unstable();
        inventory.required_vnodes.dedup();
        inventory.required_descriptors.sort_unstable();
        inventory.required_descriptors.dedup();
        inventory
            .sealed_descriptors
            .sort_unstable_by(|left, right| left.key.cmp(&right.key));
        inventory
            .sealed_descriptors
            .dedup_by(|left, right| left.key == right.key);
        inventory
            .sealed_partials
            .sort_unstable_by_key(|partial| partial.vnode);
        inventory
            .sealed_partials
            .dedup_by_key(|partial| partial.vnode);
        Self {
            version: CHECKPOINT_SEAL_VERSION,
            attempt: inventory.attempt,
            instance_id,
            execution_id,
            assignment_fence: inventory.assignment_fence,
            assignment_version: inventory.assignment_version,
            required_vnodes: inventory.required_vnodes,
            sealed_partials: inventory.sealed_partials,
            required_descriptors: inventory.required_descriptors,
            sealed_descriptors: inventory.sealed_descriptors,
        }
    }

    pub(crate) fn inventory(&self) -> CheckpointSealInventory {
        CheckpointSealInventory {
            attempt: self.attempt,
            assignment_fence: self.assignment_fence.clone(),
            assignment_version: self.assignment_version,
            required_vnodes: self.required_vnodes.clone(),
            sealed_partials: self.sealed_partials.clone(),
            required_descriptors: self.required_descriptors.clone(),
            sealed_descriptors: self.sealed_descriptors.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.version != CHECKPOINT_SEAL_VERSION {
            return Err(format!(
                "unsupported checkpoint seal version {}; expected {CHECKPOINT_SEAL_VERSION}",
                self.version
            ));
        }
        if self.instance_id.is_empty() || self.execution_id.is_nil() {
            return Err("checkpoint seal has an empty writer identity".into());
        }
        if self
            .assignment_fence
            .as_ref()
            .is_some_and(|fence| !fence.is_canonical())
        {
            return Err("checkpoint seal has a non-canonical assignment certificate".into());
        }
        if self
            .assignment_fence
            .as_ref()
            .is_some_and(|fence| fence.assignment_version != self.assignment_version)
        {
            return Err("checkpoint seal assignment version does not match its certificate".into());
        }

        let mut canonical_vnodes = self.required_vnodes.clone();
        canonical_vnodes.sort_unstable();
        canonical_vnodes.dedup();
        if canonical_vnodes != self.required_vnodes {
            return Err("checkpoint seal vnode inventory is not canonical".into());
        }
        let partial_vnodes: Vec<u32> = self
            .sealed_partials
            .iter()
            .map(|partial| partial.vnode)
            .collect();
        if partial_vnodes != canonical_vnodes {
            return Err(
                "checkpoint seal partial attestations do not exactly cover its vnodes".into(),
            );
        }
        for partial in &self.sealed_partials {
            let valid_digest = |digest: &str| {
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            };
            if partial.assignment_version != self.assignment_version
                || !valid_digest(&partial.payload_sha256)
            {
                return Err(format!(
                    "checkpoint seal has invalid provenance for vnode {}",
                    partial.vnode
                ));
            }
            match (&self.assignment_fence, &partial.writer) {
                (Some(fence), Some(writer)) if writer.matches_fence(fence) => {}
                (None, None) => {}
                _ => {
                    return Err(format!(
                        "checkpoint seal has invalid writer certificate for vnode {}",
                        partial.vnode
                    ));
                }
            }
        }

        let mut canonical_descriptors = self.required_descriptors.clone();
        canonical_descriptors.sort_unstable();
        canonical_descriptors.dedup();
        if canonical_descriptors != self.required_descriptors
            || canonical_descriptors.iter().any(String::is_empty)
        {
            return Err("checkpoint seal descriptor inventory is not canonical".into());
        }
        self.inventory().descriptor_leader_proof()?;
        Ok(())
    }
}

/// Failure scope survived by a state backend.
///
/// The variants are ordered by strength so admission can require a minimum
/// scope without collapsing node-local persistence and cluster-shared storage
/// into the same boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StateBackendDurability {
    /// State is lost when the current process exits.
    Volatile,
    /// State survives a process restart on the same node, but peers are not
    /// guaranteed to be able to read it after that node fails.
    NodeDurable,
    /// State is durably stored in a namespace reachable by every cluster node.
    ClusterShared,
}

impl StateBackendDurability {
    /// Whether this scope is at least as strong as `required`.
    #[must_use]
    pub fn satisfies(self, required: Self) -> bool {
        self >= required
    }

    /// Classify a filesystem/object-store URL by the failure domain it can
    /// survive. Unknown and process-local schemes fail closed as volatile.
    #[must_use]
    pub fn for_storage_url(url: &str) -> Self {
        match url.split_once("://").map(|(scheme, _)| scheme) {
            Some("file") => Self::NodeDurable,
            Some("s3" | "gs" | "az" | "abfs" | "abfss") => Self::ClusterShared,
            _ => Self::Volatile,
        }
    }
}

/// Errors a [`StateBackend`] can raise.
#[derive(Debug, thiserror::Error)]
pub enum StateBackendError {
    /// Underlying I/O failure (filesystem, `object_store`, network).
    #[error("I/O error: {0}")]
    Io(String),

    /// Serialization or framing error in the stored partial bytes.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The partial for `(vnode, epoch)` is not present.
    #[error("not found: vnode={vnode} epoch={epoch}")]
    NotFound {
        /// Virtual node ID.
        vnode: u32,
        /// Epoch number.
        epoch: u64,
    },

    /// The caller's assignment version is older than the backend's
    /// authoritative version. Thrown by [`StateBackend::write_partial`]
    /// when a stale writer (for example, a deposed vnode owner)
    /// attempts to persist state at a version that has since been
    /// superseded. The caller should abandon the write, refresh its
    /// assignment snapshot, and retry at the new version.
    #[error("stale assignment version: caller={caller} < authoritative={authoritative}")]
    StaleVersion {
        /// Version the writer believes is current.
        caller: u64,
        /// Authoritative version seen by the backend.
        authoritative: u64,
    },

    /// The caller claims an assignment generation this backend has not adopted. Accepting it
    /// would let an unverified future owner bypass the local assignment fence.
    #[error("future assignment version: caller={caller} > authoritative={authoritative}")]
    FutureVersion {
        /// Version the writer claims is current.
        caller: u64,
        /// Exact authoritative version adopted by this backend.
        authoritative: u64,
    },

    /// A create-once artifact or seal already exists with different bytes or
    /// fencing metadata. Retrying the identical write is accepted; conflicting
    /// content is never overwritten.
    #[error("immutable state conflict at {resource}: {message}")]
    Conflict {
        /// Object path or in-process key that conflicted.
        resource: String,
        /// Human-readable conflict detail.
        message: String,
    },
}

/// A pluggable state store used by streaming operators for partial
/// aggregates and watermarks.
///
/// ## Object Safety
///
/// The trait is deliberately object-safe:
///
/// - No generic methods.
/// - No `Self`-returning methods.
/// - All async methods are `async_trait` boxed futures.
///
/// This lets the engine hold `Arc<dyn StateBackend>` and swap
/// implementations at construction time without touching call sites.
///
/// ## Concurrency
///
/// Implementations must be `Send + Sync + 'static`. The engine expects
/// to share a single backend across many worker tasks concurrently.
///
/// ## Idempotence
///
/// [`write_partial`](Self::write_partial) must be idempotent for a given
/// `(attempt, vnode)` pair. An identical retry succeeds; conflicting bytes
/// return [`StateBackendError::Conflict`] and must never overwrite the winner.
#[async_trait]
pub trait StateBackend: Send + Sync + 'static {
    /// Number of stable key groups this backend can address.
    ///
    /// Hosts must validate this value against the runtime's [`VnodeRegistry`] before installing
    /// the backend. The raw representation is intentional: custom backends can report an invalid
    /// value and be rejected at admission instead of forcing construction-time panics.
    ///
    /// [`VnodeRegistry`]: crate::state::VnodeRegistry
    fn key_group_capacity(&self) -> u32;

    /// Bind this storage root to one deployment and logical pipeline before recovery or writes.
    ///
    /// Durable custom backends must override this with an atomic create-once binding. Volatile
    /// custom backends have no restart-visible namespace, so the default only validates the
    /// supplied identity for them and fails closed for every durable scope.
    async fn bind_state_namespace(
        &self,
        deployment_id: &str,
        pipeline_identity: &PipelineIdentity,
    ) -> Result<(), StateBackendError> {
        StateNamespaceBinding::try_new(deployment_id, pipeline_identity)?;
        if self.durability_scope() == StateBackendDurability::Volatile {
            return Ok(());
        }
        Err(StateBackendError::Conflict {
            resource: STATE_NAMESPACE_RESOURCE.into(),
            message: "durable backend does not implement immutable namespace binding".into(),
        })
    }

    /// Persist a partial aggregate for `(vnode, epoch)`.
    ///
    /// `assignment_version` is the [`VnodeRegistry::assignment_version`]
    /// the writer observed when it started this write. Backends that
    /// implement the assignment fence compare it against their exact
    /// authoritative version and reject both stale and future writers.
    /// Backends that opt out of fencing accept any version.
    ///
    /// [`VnodeRegistry::assignment_version`]: crate::state::VnodeRegistry::assignment_version
    async fn write_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

    /// Persist a cluster partial certified by the exact active assignment and writer process.
    ///
    /// Cluster coordinators must use this method. The default fails closed so a custom backend
    /// cannot silently discard provenance and later publish an apparently certified seal.
    async fn write_certified_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        _bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        Err(StateBackendError::Conflict {
            resource: format!(
                "state-v2/epoch={}/checkpoint={}/vnode={vnode}/partial.bin",
                attempt.epoch, attempt.checkpoint_id
            ),
            message: format!(
                "backend cannot persist certified vnode partials for assignment {} writer {}",
                assignment_fence.assignment_version, writer_node_id
            ),
        })
    }

    /// Read the partial aggregate for `(attempt, vnode)`, if any.
    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Persist a local-runtime coordinated-commit descriptor for `attempt` under `key`.
    ///
    /// `key` is opaque and unique within the attempt. Backends with an installed assignment
    /// authority must reject this uncertified path; cluster writers use
    /// [`Self::write_certified_commit_descriptor`].
    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

    /// Persist a cluster descriptor certified by the exact assignment, writer process, and
    /// checkpoint-initiating leader term.
    ///
    /// Cluster coordinators must use this method. The default fails closed so a custom backend
    /// cannot silently discard provenance and later publish an apparently certified seal.
    async fn write_certified_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_fence: &CheckpointAssignmentFence,
        writer_node_id: u64,
        leader_proof: &LeaderProof,
        _bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        Err(StateBackendError::Conflict {
            resource: format!(
                "state-v2/epoch={}/checkpoint={}/commit/{key}",
                attempt.epoch, attempt.checkpoint_id
            ),
            message: format!(
                "backend cannot persist certified commit descriptors for assignment {} writer {} leader token {}",
                assignment_fence.assignment_version,
                writer_node_id,
                leader_proof.fencing_token
            ),
        })
    }

    /// Read one exact coordinated-commit descriptor, if it exists.
    ///
    /// Recovery and external commit paths already know the immutable keys from
    /// the checkpoint seal. Implementations must use a direct lookup here: a
    /// prefix listing per sink turns recovery into O(sinks x descriptors) I/O
    /// and unnecessarily materializes other sinks' potentially large payloads.
    async fn read_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Read one descriptor while enforcing a private control-plane allocation bound.
    ///
    /// Durable object-store implementations should reject from object metadata before loading the
    /// body. The default preserves custom backend compatibility while still validating the result;
    /// cluster deployments fail closed unless their backend supports certified partial writes.
    async fn read_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError> {
        let bytes = self.read_commit_descriptor(attempt, key).await?;
        if bytes
            .as_ref()
            .is_some_and(|bytes| bytes.len() as u64 > max_bytes)
        {
            return Err(StateBackendError::Conflict {
                resource: format!(
                    "state-v2/epoch={}/checkpoint={}/commit/{key}",
                    attempt.epoch, attempt.checkpoint_id
                ),
                message: format!("commit descriptor exceeds the {max_bytes}-byte read bound"),
            });
        }
        Ok(bytes)
    }

    /// Read a descriptor admitted by an exact checkpoint seal.
    ///
    /// Implementations must compare the currently stored provenance, length, and payload digest
    /// with `sealed` before returning the payload. Validating only the stored object's own envelope
    /// is insufficient: a self-consistent replacement after `_SEAL` must not change the recovery
    /// cut or an external sink commit.
    async fn read_sealed_commit_descriptor_bounded(
        &self,
        attempt: CheckpointAttempt,
        sealed: &SealedCommitDescriptor,
        max_bytes: u64,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Durability barrier: true once every `vnode` partial and every
    /// `required_descriptors` key for `attempt` is persisted, sealing that exact attempt.
    /// Sinks do not commit until it returns `Ok(true)`. In cluster mode,
    /// `required_descriptors` also binds every participant's final readiness attestation,
    /// including participants with no vnodes or coordinated sinks.
    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_fence: Option<&CheckpointAssignmentFence>,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError>;

    /// Read the canonical artifact inventory for an exact sealed attempt.
    /// Returns `None` when that attempt has no live seal or is below the durable prune floor.
    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError>;

    /// Prove from storage metadata that every artifact named by an exact seal still exists with
    /// its sealed length, without reading artifact payloads.
    ///
    /// Cluster retention invokes this only while advancing the shared GC floor. Durable custom
    /// backends must override the fail-closed default with equivalent metadata evidence.
    async fn verify_checkpoint_artifact_metadata(
        &self,
        inventory: &CheckpointSealInventory,
    ) -> Result<(), StateBackendError> {
        Err(StateBackendError::Conflict {
            resource: format!(
                "state-v2/epoch={}/checkpoint={}",
                inventory.attempt.epoch, inventory.attempt.checkpoint_id
            ),
            message: "backend does not implement metadata-only sealed-artifact verification".into(),
        })
    }

    /// Garbage-collect every partial and state seal whose epoch is
    /// strictly less than `before`. Called by the checkpoint
    /// coordinator after a successful checkpoint commit so the backend
    /// does not retain state for epochs that can never be recovered.
    ///
    /// Required — there is intentionally no default. Without it an
    /// in-memory backend leaks a `Bytes` per vnode per checkpoint
    /// forever, and an object-store backend leaves `state-v2/epoch=N/…` objects
    /// forever. Implementations must durably make the retired attempt unreadable before deleting
    /// any artifact it attests. Test backends that truly do not accumulate state should
    /// implement `Ok(())` explicitly so the choice is visible.
    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError>;

    /// Failure scope survived by this backend.
    ///
    /// Implementations default to [`StateBackendDurability::Volatile`]. A
    /// backend must report [`StateBackendDurability::ClusterShared`] only when
    /// every runtime node can reach the same durable namespace; merely using
    /// the `object_store` API or a `file://` URL is not sufficient.
    fn durability_scope(&self) -> StateBackendDurability {
        StateBackendDurability::Volatile
    }

    /// Raise the backend's authoritative assignment version — the
    /// exact [`VnodeRegistry::assignment_version`] it will accept on
    /// partial and descriptor writes. Hosts call this on boot
    /// after adopting an `AssignmentSnapshot` and on each subsequent
    /// rotation so stale writers from a deposed leader are fenced out.
    ///
    /// Default is a no-op — backends that opt out of fencing (e.g. the
    /// in-process backend used for single-node deployments) inherit it
    /// unchanged. Monotonic on implementations that do fence: a call
    /// with `version <= current` is a no-op.
    ///
    /// [`VnodeRegistry::assignment_version`]: crate::state::VnodeRegistry::assignment_version
    fn set_authoritative_version(&self, _version: u64) {}

    /// Current authoritative assignment version. `0` means the fence is
    /// disabled — every caller version is accepted. Backends that do
    /// not fence return `0` unconditionally.
    fn authoritative_version(&self) -> u64 {
        0
    }
}

const _: Option<&dyn StateBackend> = None;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durability_scopes_are_ordered_by_failure_domain() {
        assert!(
            StateBackendDurability::ClusterShared.satisfies(StateBackendDurability::NodeDurable)
        );
        assert!(StateBackendDurability::NodeDurable.satisfies(StateBackendDurability::NodeDurable));
        assert!(
            !StateBackendDurability::NodeDurable.satisfies(StateBackendDurability::ClusterShared)
        );
        assert!(!StateBackendDurability::Volatile.satisfies(StateBackendDurability::NodeDurable));
    }

    #[test]
    fn checkpoint_attempt_supports_serde_rkyv_hash_and_explicit_relation() {
        let attempt = CheckpointAttempt::new(7, 42);
        let json = serde_json::to_vec(&attempt).unwrap();
        assert_eq!(
            serde_json::from_slice::<CheckpointAttempt>(&json).unwrap(),
            attempt
        );

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&attempt).unwrap();
        assert_eq!(
            rkyv::from_bytes::<CheckpointAttempt, rkyv::rancor::Error>(&bytes).unwrap(),
            attempt
        );

        let mut hashed = rustc_hash::FxHashSet::default();
        hashed.insert(attempt);
        assert!(hashed.contains(&attempt));

        use CheckpointAttemptRelation::{Conflict, Exact, Newer, Older};
        assert_eq!(attempt.relation_to(attempt), Exact);
        assert_eq!(CheckpointAttempt::new(6, 41).relation_to(attempt), Older);
        assert_eq!(CheckpointAttempt::new(8, 43).relation_to(attempt), Newer);
        assert_eq!(CheckpointAttempt::new(6, 43).relation_to(attempt), Conflict);
        assert_eq!(CheckpointAttempt::new(8, 41).relation_to(attempt), Conflict);
        assert_eq!(CheckpointAttempt::new(7, 41).relation_to(attempt), Conflict);
        assert_eq!(CheckpointAttempt::new(7, 43).relation_to(attempt), Conflict);
    }
}
