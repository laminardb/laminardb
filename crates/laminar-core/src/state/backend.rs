//! The `StateBackend` trait: the single contract between streaming
//! operators and the storage tier. Backends persist per-checkpoint-attempt
//! artifacts and expose an exact-attempt durability seal.

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Exact identity of one checkpoint attempt.
///
/// `epoch` is logical pipeline progress and may be reused after a coordinated
/// rewind. `checkpoint_id` is the never-reused attempt identity that prevents
/// artifacts from an abandoned timeline satisfying a later checkpoint.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
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

impl CheckpointAttempt {
    /// Construct an exact checkpoint attempt identity.
    #[must_use]
    pub const fn new(epoch: u64, checkpoint_id: u64) -> Self {
        Self {
            epoch,
            checkpoint_id,
        }
    }
}

/// Current on-storage checkpoint-seal payload format.
pub(crate) const CHECKPOINT_SEAL_VERSION: u32 = 3;

/// Immutable provenance and content attestation for one vnode partial admitted by a seal.
///
/// The assignment generation is the durable split-brain fence. Writer and execution digests
/// make the winning producer auditable, while the payload digest lets recovery detect corruption
/// without placing large state blobs in the seal itself.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SealedVnodePartial {
    /// Vnode whose partial was sealed.
    pub vnode: u32,
    /// Assignment generation observed by the writer.
    pub assignment_version: u64,
    /// SHA-256 of the stable writer identity (bounded to avoid untrusted seal growth).
    pub writer_id_sha256: String,
    /// Process incarnation that created the immutable partial.
    pub execution_id: uuid::Uuid,
    /// Raw partial payload length, excluding the storage envelope.
    pub payload_len: u64,
    /// SHA-256 of the raw partial payload.
    pub payload_sha256: String,
}

impl SealedVnodePartial {
    pub(crate) fn new(
        vnode: u32,
        assignment_version: u64,
        writer_id: &str,
        execution_id: uuid::Uuid,
        payload: &[u8],
    ) -> Self {
        Self {
            vnode,
            assignment_version,
            writer_id_sha256: digest_hex(&sha256(writer_id.as_bytes())),
            execution_id,
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

/// Canonical artifact inventory bound into an immutable checkpoint seal.
///
/// A seal is meaningful only for the exact set that was proven durable. The
/// inventory is exposed so recovery-side protocols (notably the designated
/// external-sink committer) can independently validate the participant markers
/// that the seal admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointSealInventory {
    /// Exact attempt named by the seal.
    pub attempt: CheckpointAttempt,
    /// Sorted, duplicate-free vnode partials required by the seal.
    pub required_vnodes: Vec<u32>,
    /// Per-vnode writer, assignment-generation, length, and digest attestations.
    pub sealed_partials: Vec<SealedVnodePartial>,
    /// Sorted, duplicate-free commit-descriptor keys required by the seal.
    pub required_descriptors: Vec<String>,
}

/// Structured body of an exact-attempt state seal.
///
/// This remains crate-private because callers consume exact attempts through
/// [`StateBackend::sealed_checkpoints`]; the additional fields are durability
/// audit and fencing metadata owned by backend implementations.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CheckpointSeal {
    pub version: u32,
    pub attempt: CheckpointAttempt,
    pub instance_id: String,
    pub execution_id: uuid::Uuid,
    pub assignment_version: u64,
    pub required_vnodes: Vec<u32>,
    pub sealed_partials: Vec<SealedVnodePartial>,
    pub required_descriptors: Vec<String>,
}

impl CheckpointSeal {
    pub(crate) fn new(
        attempt: CheckpointAttempt,
        instance_id: String,
        execution_id: uuid::Uuid,
        assignment_version: u64,
        required_vnodes: &[u32],
        sealed_partials: &[SealedVnodePartial],
        required_descriptors: &[String],
    ) -> Self {
        let mut required_vnodes = required_vnodes.to_vec();
        required_vnodes.sort_unstable();
        required_vnodes.dedup();
        let mut required_descriptors = required_descriptors.to_vec();
        required_descriptors.sort_unstable();
        required_descriptors.dedup();
        let mut sealed_partials = sealed_partials.to_vec();
        sealed_partials.sort_unstable_by_key(|partial| partial.vnode);
        sealed_partials.dedup_by_key(|partial| partial.vnode);
        Self {
            version: CHECKPOINT_SEAL_VERSION,
            attempt,
            instance_id,
            execution_id,
            assignment_version,
            required_vnodes,
            sealed_partials,
            required_descriptors,
        }
    }

    pub(crate) fn inventory(&self) -> CheckpointSealInventory {
        CheckpointSealInventory {
            attempt: self.attempt,
            required_vnodes: self.required_vnodes.clone(),
            sealed_partials: self.sealed_partials.clone(),
            required_descriptors: self.required_descriptors.clone(),
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
                || partial.execution_id.is_nil()
                || !valid_digest(&partial.writer_id_sha256)
                || !valid_digest(&partial.payload_sha256)
            {
                return Err(format!(
                    "checkpoint seal has invalid provenance for vnode {}",
                    partial.vnode
                ));
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
    /// Persist a partial aggregate for `(vnode, epoch)`.
    ///
    /// `assignment_version` is the [`VnodeRegistry::assignment_version`]
    /// the writer observed when it started this write. Backends that
    /// implement the assignment fence compare it against their own
    /// authoritative version and return
    /// [`StateBackendError::StaleVersion`] if the writer is behind.
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

    /// Read the partial aggregate for `(attempt, vnode)`, if any.
    async fn read_partial(
        &self,
        attempt: CheckpointAttempt,
        vnode: u32,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Persist a coordinated-commit descriptor for `attempt` under `key`.
    ///
    /// `key` is opaque and unique within the attempt (the coordinator uses
    /// `node={id}/sink={name}`). Same fence and idempotence as `write_partial`.
    async fn write_commit_descriptor(
        &self,
        attempt: CheckpointAttempt,
        key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

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

    /// Every coordinated-commit descriptor written for `attempt`, as `(key, bytes)`.
    ///
    /// This bulk inventory API is intended for diagnostics and validation. The
    /// production committer uses [`read_commit_descriptor`](Self::read_commit_descriptor)
    /// with the exact keys recorded in the checkpoint seal.
    async fn read_commit_descriptors(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<(String, Bytes)>, StateBackendError>;

    /// Persist this node's source-checkpoint offsets for `attempt` (opaque
    /// connector key/value bytes), keyed by a per-node `node_key` so writers
    /// don't collide. A node acquiring a partition on a later rotation unions
    /// every node's blob (see [`read_source_offsets`](Self::read_source_offsets))
    /// to resume from the committed cut instead of `auto.offset.reset`. Same
    /// fence as `write_partial`. Default no-op: handoff degrades to the source's
    /// configured startup offset.
    async fn write_source_offsets(
        &self,
        attempt: CheckpointAttempt,
        node_key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let _ = (attempt, node_key, assignment_version, bytes);
        Ok(())
    }

    /// Every node's source-offset blob for `attempt` (see
    /// [`write_source_offsets`](Self::write_source_offsets)). The caller unions
    /// them into the global offset map. Default empty.
    async fn read_source_offsets(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<Bytes>, StateBackendError> {
        let _ = attempt;
        Ok(Vec::new())
    }

    /// Durability barrier: true once every `vnode` partial and every
    /// `required_descriptors` key for `attempt` is persisted, sealing that exact attempt.
    /// Sinks do not commit until it returns `Ok(true)`. `required_descriptors`
    /// is empty unless coordinated-commit sinks are present.
    async fn seal_checkpoint(
        &self,
        attempt: CheckpointAttempt,
        assignment_version: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError>;

    /// Sealed checkpoint attempts whose `checkpoint_id` is strictly greater
    /// than `after_checkpoint_id`, ascending by checkpoint ID.
    async fn sealed_checkpoints(
        &self,
        after_checkpoint_id: u64,
    ) -> Result<Vec<CheckpointAttempt>, StateBackendError>;

    /// Read the canonical artifact inventory for an exact sealed attempt.
    /// Returns `None` only when that attempt has no seal.
    async fn checkpoint_seal_inventory(
        &self,
        attempt: CheckpointAttempt,
    ) -> Result<Option<CheckpointSealInventory>, StateBackendError>;

    /// Garbage-collect every partial and state seal whose epoch is
    /// strictly less than `before`. Called by the checkpoint
    /// coordinator after a successful checkpoint commit so the backend
    /// does not retain state for epochs that can never be recovered.
    ///
    /// Required — there is intentionally no default. Without it an
    /// in-memory backend leaks a `Bytes` per vnode per checkpoint
    /// forever, and an object-store backend leaves `state-v2/epoch=N/…` objects
    /// forever. Test backends that truly do not accumulate state should
    /// implement `Ok(())` explicitly so the choice is visible.
    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError>;

    /// Delete every artifact (partials, seals, descriptors, source-offset blobs) whose epoch
    /// is strictly greater than `after`. A coordinated rewind to epoch `after` calls this
    /// while the cluster is stopped. Exact attempt namespaces prevent overwrite collisions,
    /// while truncation removes abandoned seals so `latest_sealed_checkpoint` reflects the
    /// rewound timeline. Default no-op for backends that never serve a coordinated rewind.
    async fn truncate_after(&self, after: u64) -> Result<(), StateBackendError> {
        let _ = after;
        Ok(())
    }

    /// Exact attempt with the highest checkpoint ID sealed by a durable state marker, or `None`.
    ///
    /// This is the epoch a node rehydrates from when it acquires a
    /// vnode during a rebalance: every owner agreed the epoch's
    /// per-vnode partials were durable before [`seal_checkpoint`] sealed
    /// it, so reading `partial.bin` at this epoch restores the last
    /// cluster-consistent state rather than starting empty.
    ///
    /// Default is `None` — backends that cannot enumerate committed
    /// epochs report "no committed state" and the caller treats every
    /// affected vnode as a fresh start.
    ///
    /// [`seal_checkpoint`]: Self::seal_checkpoint
    async fn latest_sealed_checkpoint(
        &self,
    ) -> Result<Option<CheckpointAttempt>, StateBackendError> {
        Ok(None)
    }

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
    /// minimum [`VnodeRegistry::assignment_version`] it will accept on
    /// [`write_partial`](Self::write_partial). Hosts call this on boot
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
    fn checkpoint_attempt_supports_serde_rkyv_hash_and_order() {
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
        assert!(CheckpointAttempt::new(6, 41) < attempt);
    }
}
