//! The `StateBackend` trait: the single contract between streaming
//! operators and the storage tier. Backends persist per-(vnode, epoch)
//! partial-state blobs and expose an `epoch_complete` durability gate.

use async_trait::async_trait;
use bytes::Bytes;

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
    /// when a stale writer (e.g. the losing side of a split-brain)
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

    /// Raised by [`StateBackend::epoch_complete`] when the caller
    /// observes an existing commit marker whose audit body names a
    /// different instance. Two leaders raced on the same epoch and
    /// the other one won. The caller (losing leader) must NOT proceed
    /// with its own sink-commit phase — its view of the state is not
    /// the one durably sealed.
    #[error(
        "split-brain commit detected: epoch already committed by {committer:?}, we are {self_id:?}"
    )]
    SplitBrainCommit {
        /// Instance id recorded in the existing commit marker.
        committer: String,
        /// This backend's own instance id.
        self_id: String,
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
/// [`write_partial`](Self::write_partial) must be idempotent for a
/// given `(vnode, epoch)` pair — recovery may replay the same write.
/// Overwriting the same bytes is acceptable; conflicting bytes for the
/// same key indicate a bug upstream.
#[async_trait]
pub trait StateBackend: Send + Sync + 'static {
    /// Persist a partial aggregate for `(vnode, epoch)`.
    ///
    /// `assignment_version` is the [`VnodeRegistry::assignment_version`]
    /// the writer observed when it started this write. Backends that
    /// implement the split-brain fence compare it against their own
    /// authoritative version and return
    /// [`StateBackendError::StaleVersion`] if the writer is behind.
    /// Backends that opt out of fencing accept any version.
    ///
    /// [`VnodeRegistry::assignment_version`]: crate::state::VnodeRegistry::assignment_version
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

    /// Read the partial aggregate for `(vnode, epoch)`, if any.
    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Persist a coordinated-commit descriptor for `epoch` under `key`.
    ///
    /// `key` is opaque and unique within the epoch (the coordinator uses
    /// `node={id}/sink={name}`). Same fence and idempotence as `write_partial`.
    async fn write_commit_descriptor(
        &self,
        epoch: u64,
        key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

    /// Every coordinated-commit descriptor written for `epoch`, as `(key, bytes)`.
    async fn read_commit_descriptors(
        &self,
        epoch: u64,
    ) -> Result<Vec<(String, Bytes)>, StateBackendError>;

    /// Persist this node's source-checkpoint offsets for `epoch` (opaque
    /// connector key/value bytes), keyed by a per-node `node_key` so writers
    /// don't collide. A node acquiring a partition on a later rotation unions
    /// every node's blob (see [`read_source_offsets`](Self::read_source_offsets))
    /// to resume from the committed cut instead of `auto.offset.reset`. Same
    /// fence as `write_partial`. Default no-op: handoff degrades to the source's
    /// configured startup offset.
    async fn write_source_offsets(
        &self,
        epoch: u64,
        node_key: &str,
        assignment_version: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError> {
        let _ = (epoch, node_key, assignment_version, bytes);
        Ok(())
    }

    /// Every node's source-offset blob for `epoch` (see
    /// [`write_source_offsets`](Self::write_source_offsets)). The caller unions
    /// them into the global offset map. Default empty.
    async fn read_source_offsets(&self, epoch: u64) -> Result<Vec<Bytes>, StateBackendError> {
        let _ = epoch;
        Ok(Vec::new())
    }

    /// Durability barrier: true once every `vnode` partial and every
    /// `required_descriptors` key for `epoch` is persisted, sealing the epoch.
    /// Sinks do not commit until it returns `Ok(true)`. `required_descriptors`
    /// is empty unless coordinated-commit sinks are present.
    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, StateBackendError>;

    /// Sealed epochs strictly greater than `after`, ascending. The designated
    /// committer uses this to commit exactly the sealed epochs in one pass and
    /// skip abandoned ones (which left partial descriptors but no seal), in a
    /// single listing rather than a probe per epoch.
    async fn sealed_epochs(&self, after: u64) -> Result<Vec<u64>, StateBackendError>;

    /// Garbage-collect every partial and commit marker whose epoch is
    /// strictly less than `before`. Called by the checkpoint
    /// coordinator after a successful checkpoint commit so the backend
    /// does not retain state for epochs that can never be recovered.
    ///
    /// Required — there is intentionally no default. Without it an
    /// in-memory backend leaks a `Bytes` per vnode per checkpoint
    /// forever, and an object-store backend leaves `epoch=N/…` objects
    /// forever. Test backends that truly do not accumulate state should
    /// implement `Ok(())` explicitly so the choice is visible.
    async fn prune_before(&self, before: u64) -> Result<(), StateBackendError>;

    /// Highest epoch sealed by a durable commit marker, or `None` when
    /// the store holds no committed epoch yet.
    ///
    /// This is the epoch a node rehydrates from when it acquires a
    /// vnode during a rebalance: every owner agreed the epoch's
    /// per-vnode partials were durable before [`epoch_complete`] sealed
    /// it, so reading `partial.bin` at this epoch restores the last
    /// cluster-consistent state rather than starting empty.
    ///
    /// Default is `None` — backends that cannot enumerate committed
    /// epochs report "no committed state" and the caller treats every
    /// affected vnode as a fresh start.
    ///
    /// [`epoch_complete`]: Self::epoch_complete
    async fn latest_committed_epoch(&self) -> Result<Option<u64>, StateBackendError> {
        Ok(None)
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
