//! The `StateBackend` trait: the single contract between streaming
//! operators and the storage tier.
//!
//! Backends own two things per vnode:
//!
//! 1. **Partials** — opaque `Bytes` blobs written by operators at the
//!    end of each epoch. The backend persists them under
//!    `(vnode, epoch)` keys. Operators deserialize via
//!    [`PartialAggregate`](super::PartialAggregate).
//! 2. **Watermarks** — a per-vnode event-time frontier. Backends expose
//!    a global watermark as the min over a caller-supplied vnode set.
//!
//! Each epoch is "complete" iff every participating vnode has committed
//! its partial. [`StateBackend::epoch_complete`] is the durability
//! barrier the checkpoint coordinator consults before releasing sinks.
//!
//! The trait is deliberately narrow. Backends are pluggable via
//! [`StateBackendConfig`](super::config) so that `embedded`,
//! `standalone`, `distributed-embedded`, and `constellation` modes
//! differ only in which `Arc<dyn StateBackend>` the engine is holding.

use async_trait::async_trait;
use bytes::Bytes;

/// Errors a [`StateBackend`] can raise.
#[derive(Debug, thiserror::Error)]
pub enum StateBackendError {
    /// The write was rejected because the caller's assignment version
    /// is older than the one the backend currently recognizes.
    ///
    /// Split-brain guard: a paused node that wakes up after a
    /// rebalance must not overwrite state owned by the new assignee.
    #[error("stale assignment: caller v{caller}, backend v{backend}")]
    StaleAssignment {
        /// Version the caller used.
        caller: u64,
        /// Version the backend has recorded.
        backend: u64,
    },

    /// Underlying I/O failure (filesystem, `object_store`, network).
    #[error("I/O error: {0}")]
    Io(String),

    /// Serialization or framing error in the stored partial bytes.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// The partial or watermark for `(vnode, epoch)` is not present.
    #[error("not found: vnode={vnode} epoch={epoch}")]
    NotFound {
        /// Virtual node ID.
        vnode: u32,
        /// Epoch number.
        epoch: u64,
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
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        bytes: Bytes,
    ) -> Result<(), StateBackendError>;

    /// Read the partial aggregate for `(vnode, epoch)`, if any.
    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<Bytes>, StateBackendError>;

    /// Publish this node's event-time watermark for `vnode`.
    ///
    /// Idempotent and monotone: backends retain the max of observed
    /// values per vnode.
    async fn publish_watermark(
        &self,
        vnode: u32,
        ts_ms: i64,
    ) -> Result<(), StateBackendError>;

    /// Read the global watermark — the minimum over the supplied vnode
    /// set. Operators that consume across the full ring pass `&[0..N)`.
    async fn global_watermark(&self, vnodes: &[u32]) -> Result<i64, StateBackendError>;

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
}

const _: Option<&dyn StateBackend> = None;
