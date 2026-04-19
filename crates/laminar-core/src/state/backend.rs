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
    #[error(
        "stale assignment version: caller={caller} < authoritative={authoritative}"
    )]
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
    /// implement the split-brain fence (Phase 1.4) compare it against
    /// their own authoritative version and return
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

    /// Durability barrier: returns true iff every `vnode` in the set
    /// has a partial persisted for `epoch`.
    ///
    /// The checkpoint coordinator calls this after sinks precommit
    /// and before they commit. Sinks do not commit until this returns
    /// `Ok(true)`.
    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
    ) -> Result<bool, StateBackendError>;

    /// Garbage-collect every partial and commit marker whose epoch is
    /// strictly less than `before`. Called by the checkpoint
    /// coordinator after a successful checkpoint commit so that the
    /// backend does not retain state for epochs that can never be
    /// recovered again.
    ///
    /// Without this, in-memory backends leak indefinitely (every
    /// checkpoint adds one entry per vnode) and object-store backends
    /// leave `epoch=N/…` artifacts forever.
    ///
    /// Default is a no-op so existing test backends compile. Production
    /// backends should override it.
    async fn prune_before(&self, _before: u64) -> Result<(), StateBackendError> {
        Ok(())
    }
}

const _: Option<&dyn StateBackend> = None;
